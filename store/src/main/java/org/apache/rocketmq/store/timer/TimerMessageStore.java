/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.timer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimerMessageStore {
    public static final String TIMER_TOPIC = "%SYS_TIMER_TOPIC%";
    public static final String TIMER_DELAY_KEY = MessageConst.PROPERTY_TIMER_DELAY_MS;
    public static final String TIMER_ENQUEUE_KEY = MessageConst.PROPERTY_TIMER_ENQUEUE_MS;
    public static final String TIMER_DEQUEUE_KEY = MessageConst.PROPERTY_TIMER_DEQUEUE_MS;
    public static final String TIMER_ROLL_TIMES_KEY = MessageConst.PROPERTY_TIMER_ROLL_TIMES;
    public static final int DAY_SECS = 24 * 3600;
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    //currently only use the queue 0
    private final ConcurrentMap<Integer /* queue */, Long/* offset */> offsetTable =
        new ConcurrentHashMap<Integer, Long>(32);

    private final ByteBuffer timerLogBuffer = ByteBuffer.allocate(1024);

    private final MessageStore messageStore;
    private final TimerWheel timerWheel;
    private final TimerLog timerLog;
    private final TimerCheckpoint timerCheckpoint;

    private volatile long currReadTimeMs;
    private volatile long currWriteTimeMs;
    private long currTimerLogFlushPos;


    public TimerMessageStore(final MessageStore messageStore, final MessageStoreConfig storeConfig) throws IOException {
        this.messageStore = messageStore;
        this.timerWheel = new TimerWheel(storeConfig.getStorePathRootDir() + File.separator + "timerwheel", 2 * DAY_SECS );
        this.timerLog = new TimerLog(storeConfig);
        this.timerCheckpoint = new TimerCheckpoint(storeConfig.getStorePathRootDir() + File.separator + "timercheck");

    }

    public void updateCurrReadTimeMs(long readTimeMs) {
        this.currReadTimeMs = readTimeMs;
    }
    public void updateOffset(int queue, long offset) {
        offsetTable.put(queue, offset);
    }

    private void maybeMoveWriteTime() {
        if (currWriteTimeMs < (System.currentTimeMillis()/1000) * 1000) {
            currWriteTimeMs =  (System.currentTimeMillis()/1000) * 1000;
        }
    }
    private void moveReadTime() {
        currReadTimeMs = currReadTimeMs + 1000;
    }

    public boolean enqueue(int queueId) {
        maybeMoveWriteTime();
        ConsumeQueue cq = this.messageStore.getConsumeQueue(TIMER_TOPIC, queueId);
        if (null == cq) {
            return false;
        }
        long offset =  offsetTable.get(queueId);
        SelectMappedBufferResult bufferCQ = cq.getIndexBuffer(offset);
        if (null == bufferCQ) {
            return false;
        }
        try {
            int i = 0;
            for ( ; i < bufferCQ.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                try {
                    long offsetPy = bufferCQ.getByteBuffer().getLong();
                    int sizePy = bufferCQ.getByteBuffer().getInt();
                    long tagsCode = bufferCQ.getByteBuffer().getLong();
                    MessageExt msgExt = getMessageByCommitOffset(offsetPy, sizePy);
                    if (null == msgExt) {
                        //TODO
                        log.warn("Get message failed in enqueuing offsetPy:{} sizePy:{}", offsetPy, sizePy);
                        continue;
                    }
                    //TODO TIMER_DELAY_KEY dose not exist
                    long delayedTime = Long.valueOf(msgExt.getProperty(TIMER_DELAY_KEY));
                    doEnqueue(offsetPy, sizePy, delayedTime);
                } catch (Exception e) {
                    //TODO retry
                    e.printStackTrace();
                }
            }
            offsetTable.put(queueId, offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE));
            return  i > 0;
        } catch (Exception e) {
           log.error("Unknown exception in enqueuing", e);
        } finally {
            bufferCQ.release();
        }
        return false;
    }


    public void doEnqueue(long offsetPy, int sizePy, long delayedTime) {
        if (delayedTime < currWriteTimeMs) {
            //TODO
        }
        int size = 4  //size
            +  8 //prev pos
            +  4 //magic value
            +  8 //curr write time, for trace
            +  4 //delayed time, for check
            +  8 //offsetPy
            +  4 //sizePy
            ;
        boolean needRoll =  delayedTime - currWriteTimeMs >= timerWheel.TTL_SECS * 1000;
        int magic = needRoll ?  1 : 0;
        if (needRoll) {
            delayedTime =  currWriteTimeMs +  timerWheel.TTL_SECS * 1000 - 1;
        }
        Slot slot = timerWheel.getSlot(delayedTime/1000);
        ByteBuffer tmpBuffer = timerLogBuffer;
        tmpBuffer.clear();
        tmpBuffer.putInt(size);
        tmpBuffer.putLong(slot.LAST_POS);
        tmpBuffer.putInt(magic);
        tmpBuffer.putLong(currWriteTimeMs);
        tmpBuffer.putInt((int) (delayedTime -  currWriteTimeMs));
        tmpBuffer.putLong(offsetPy);
        tmpBuffer.putInt(sizePy);
        tmpBuffer.flip();
        long ret = timerLog.append(tmpBuffer);
        if (ret == -1) {
            //TODO
        }
        timerWheel.putSlot(delayedTime/1000, slot.FIRST_POS == -1 ? ret : slot.FIRST_POS, ret);
        maybeMoveWriteTime();
    }


    public int dequeue() {
        if (currReadTimeMs >= currWriteTimeMs) {
            return -1;
        }
        Slot slot = timerWheel.getSlot(currReadTimeMs/1000);
        if (-1 == slot.TIME_SECS) {
            moveReadTime();
            return 0;
        }

        long currOffsetPy = slot.LAST_POS;
        LinkedList<SelectMappedBufferResult> stack =  new LinkedList<>();
        //read the msg one by one
        while (currOffsetPy != -1) {
            SelectMappedBufferResult selectRes = timerLog.getTimerMessage(currOffsetPy);
            if (null == selectRes) break;
            selectRes.getByteBuffer().mark();
            selectRes.getByteBuffer().getInt(); //size
            long prevPos = selectRes.getByteBuffer().getLong();
            selectRes.getByteBuffer().reset();
            stack.push(selectRes);
            currOffsetPy = prevPos;
            if (slot.FIRST_POS != -1 && currOffsetPy <= slot.FIRST_POS) {
                break;
            }
        }
        int pollNum = 0;
        SelectMappedBufferResult sbr = stack.pollFirst();
        while (sbr != null) {
            try {
                ByteBuffer bf = sbr.getByteBuffer();
                bf.getInt(); //size
                bf.getLong(); //prev pos
                int magic = bf.getInt();
                long enqueueTime = bf.getLong();
                long delayedTime = bf.getInt() + enqueueTime;
                long offsetPy = bf.getLong();
                int sizePy = bf.getInt();
                MessageExt msgExt = getMessageByCommitOffset(offsetPy, sizePy);
                if (null == msgExt) {
                    log.warn("Get message failed in dequeuing offsetPy:{} sizePy:{}", offsetPy, sizePy);
                    //TODO
                }
                doReput(msgExt, enqueueTime, magic > 0);
            } catch (Exception e) {
                //TODO
                log.error("Unknown exception in dequeuing", e);
            } finally {
                sbr.release();
            }
            if (++pollNum % 10 == 0) {
                timerWheel.putSlot(currReadTimeMs/1000, sbr.getStartOffset(), slot.LAST_POS);
            }
            sbr =  stack.pollFirst();
        }
        moveReadTime();
        return 1;
    }

    private MessageExt getMessageByCommitOffset(long offsetPy, int sizePy) {
        SelectMappedBufferResult sbr = messageStore.selectOneMessageByOffset(offsetPy, sizePy);
        if (null != sbr) {
            try {
                return MessageDecoder.decode(sbr.getByteBuffer(), true, false);
            } finally {
                sbr.release();
            }
        }
        return null;
    }

    private void doReput(MessageExt messageExt, long enqueueTime, boolean needRoll) {
        if (enqueueTime != -1) {
            MessageAccessor.putProperty(messageExt, TIMER_ENQUEUE_KEY, enqueueTime + "");
        }
        if (needRoll) {
            if (messageExt.getProperty(TIMER_ROLL_TIMES_KEY) != null) {
                MessageAccessor.putProperty(messageExt, TIMER_ROLL_TIMES_KEY,Integer.parseInt(messageExt.getProperty(TIMER_ROLL_TIMES_KEY)) + 1 + "");
            } else {
                MessageAccessor.putProperty(messageExt, TIMER_ROLL_TIMES_KEY,1 + "");
            }
        }
        MessageAccessor.putProperty(messageExt, TIMER_DEQUEUE_KEY, System.currentTimeMillis() + "");
        PutMessageResult putMessageResult = messageStore.putMessage(convertMessage(messageExt, needRoll));
        //TODO handle putMessageResult
    }

    private MessageExtBrokerInner convertMessage(MessageExt msgExt, boolean needRoll) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());
        TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
        long tagsCodeValue =
            MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
        msgInner.setTagsCode(tagsCodeValue);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

        msgInner.setWaitStoreMsgOK(false);

        if (needRoll) {
            msgInner.setTopic(msgExt.getTopic());
            msgInner.setQueueId(msgExt.getQueueId());
        } else {
            msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));
            msgInner.setQueueId(Integer.parseInt(msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID)));
            MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_REAL_TOPIC);
            MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_REAL_QUEUE_ID);
        }
        return msgInner;
    }

    class FlushTimerService  extends ServiceThread {

        @Override public String getServiceName() {
            return FlushTimerService.class.getSimpleName();
        }

        @Override public void run() {
            TimerMessageStore.log.info(this.getServiceName() + " service start");
            while (!this.isStopped()) {
                try {
                    waitForRunning(200);
                } catch (Exception e) {
                    TimerMessageStore.log.info("Error occurred in " + getServiceName(), e);
                }
            }
            TimerMessageStore.log.info(this.getServiceName() + " service end");
        }
    }

    public void prepareCheckPoint() {
        timerCheckpoint.setLastReadTimeMs(currReadTimeMs);
        timerCheckpoint.setLastTimerLogFlushPos(timerLog.getMappedFileQueue().getFlushedWhere());
        timerCheckpoint.setLastTimerQueueOffset(offsetTable.get(0));
    }
}
