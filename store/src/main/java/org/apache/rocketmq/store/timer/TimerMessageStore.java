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
import java.util.List;
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
import org.apache.rocketmq.store.MappedFile;
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
    public static final int TIME_BLANK = 60 * 1000;
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    //currently only use the queue 0
    private final ConcurrentMap<Integer /* queue */, Long/* offset */> offsetTable =
        new ConcurrentHashMap<Integer, Long>(32);

    private final ByteBuffer timerLogBuffer = ByteBuffer.allocate(1024);
    private final int INITIAL= 0, RUNNING = 1, SHUTDOWN = 2;
    private volatile int state = INITIAL;

    private final MessageStore messageStore;
    private final TimerWheel timerWheel;
    private final TimerLog timerLog;
    private final TimerCheckpoint timerCheckpoint;

    private final TimerEnqueueService enqueueService;
    private final TimerDequeueService dequeueService;
    private final TimerFlushService timerFlushService;


    private volatile long currReadTimeMs;
    private volatile long currWriteTimeMs;


    public TimerMessageStore(final MessageStore messageStore, final MessageStoreConfig storeConfig) throws IOException {
        this.messageStore = messageStore;
        this.timerWheel = new TimerWheel(storeConfig.getStorePathRootDir() + File.separator + "timerwheel", 2 * DAY_SECS );
        this.timerLog = new TimerLog(storeConfig);
        this.timerCheckpoint = new TimerCheckpoint(storeConfig.getStorePathRootDir() + File.separator + "timercheck");
        enqueueService = new TimerEnqueueService();
        dequeueService =  new TimerDequeueService();
        timerFlushService = new TimerFlushService();

    }

    public boolean load() {
        return timerLog.load();
    }
    public void recover() {
        //recover timerLog
        long lastFlushPos = timerCheckpoint.getLastTimerLogFlushPos();
        MappedFile lastFile = timerLog.getMappedFileQueue().getLastMappedFile();
        if (null != lastFile) {
            lastFlushPos = lastFlushPos - lastFile.getFileSize();
        }
        long processOffset = recoverAndRevise(lastFlushPos, true);
        prepareTimerLogFlushPos();
        //check timer wheel
        currReadTimeMs = timerCheckpoint.getLastReadTimeMs();
        long minFirst = timerWheel.checkPhyPos(currReadTimeMs, processOffset);
        if (minFirst < processOffset) {
            recoverAndRevise(processOffset, false);
        }
        //dequeue old messages
        maybeMoveWriteTime();
        while (currReadTimeMs < currWriteTimeMs - ((timerWheel.TTL_SECS * 1000)/2)) {
            dequeue();
        }
    }

    //recover timerlog and revise timerwheel
    //return process offset
    private long recoverAndRevise(long beginOffset, boolean checkTimerLog) {
        MappedFile lastFile = timerLog.getMappedFileQueue().getLastMappedFile();
        if (null == lastFile) return 0;

        List<MappedFile> mappedFiles = timerLog.getMappedFileQueue().getMappedFiles();
        int index = mappedFiles.size() - 1;
        for (; index >= 0 ; index--) {
            MappedFile mappedFile = mappedFiles.get(index);
            if (beginOffset >= mappedFile.getFileFromOffset()) {
                break;
            }
        }
        if (index < 0) index = 0;
        long checkOffset = mappedFiles.get(index).getFileFromOffset();
        for (; index < mappedFiles.size() ; index++) {
            SelectMappedBufferResult sbr = mappedFiles.get(index).selectMappedBuffer(0, mappedFiles.get(index).getFileSize());
            ByteBuffer bf = sbr.getByteBuffer();
            int position = 0;
            boolean stopCheck = false;
            for (; position < sbr.getSize() ; position += 40) {
                bf.position(position);
                bf.getInt();//size
                long prevPos = bf.getLong();
                int magic = bf.getInt();
                if (checkTimerLog && !checkMagic(magic)) {
                    //TODO should truncate or continue
                    //if we truncate, then should revert the queue offset too
                    stopCheck = true;
                    break;
                }
                long delayTime = bf.getLong() + bf.getInt();
                timerWheel.reviseSlot(delayTime, TimerWheel.IGNORE, prevPos, true);
            }
            checkOffset =  mappedFiles.get(index).getFileFromOffset() + position;
            if (stopCheck) {
                break;
            }
        }
        if (checkTimerLog) {
            timerLog.getMappedFileQueue().truncateDirtyFiles(checkOffset);
        }
        return checkOffset;
    }
    private boolean checkMagic(int magic) {
        //TODO use more complicated magic
        if (0 == magic || 1 == magic) {
            return true;
        }
        return false;
    }

    public void start() {
        maybeMoveWriteTime();
        if (currWriteTimeMs - currReadTimeMs + TIME_BLANK >= timerWheel.TTL_SECS * 1000) {
            currReadTimeMs = currWriteTimeMs - timerWheel.TTL_SECS * 1000 + TIME_BLANK;
        }
        enqueueService.start();
        dequeueService.start(); //TODO only master do
        timerFlushService.start();
        state = RUNNING;
    }
    public void shutdown() {
        state = SHUTDOWN;
        enqueueService.shutdown();
        dequeueService.shutdown(); //TODO if not start, will it throw exception
        timerWheel.shutdown();
        timerLog.shutdown();
        prepareReadTimeMs();
        prepareTimerQueueOffset();
        prepareTimerLogFlushPos();
        timerCheckpoint.shutdown(); //TODO if the previous shutdown failed

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
        boolean needRoll =  delayedTime - currWriteTimeMs + TIME_BLANK >= timerWheel.TTL_SECS * 1000;
        int magic = needRoll ?  1 : 0;
        if (needRoll) {
            delayedTime =  currWriteTimeMs +  timerWheel.TTL_SECS * 1000 - TIME_BLANK;
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

    class TimerEnqueueService  extends ServiceThread {

        @Override public String getServiceName() {
            return this.getClass().getSimpleName();
        }

        @Override public void run() {
            TimerMessageStore.log.info(this.getServiceName() + " service start");
            while (!this.isStopped()) {
                try {
                    if(!TimerMessageStore.this.enqueue(0)) {
                        waitForRunning(50);
                    }
                } catch (Exception e) {
                    TimerMessageStore.log.info("Error occurred in " + getServiceName(), e);
                }
            }
            TimerMessageStore.log.info(this.getServiceName() + " service end");
        }
    }

    class TimerDequeueService  extends ServiceThread {

        @Override public String getServiceName() {
            return this.getClass().getSimpleName();
        }

        @Override public void run() {
            TimerMessageStore.log.info(this.getServiceName() + " service start");
            while (!this.isStopped()) {
                try {
                    if( -1 != TimerMessageStore.this.dequeue()) {
                        waitForRunning(50);
                    }
                } catch (Exception e) {
                    TimerMessageStore.log.info("Error occurred in " + getServiceName(), e);
                }
            }
            TimerMessageStore.log.info(this.getServiceName() + " service end");
        }
    }

    class TimerFlushService extends ServiceThread {

        @Override public String getServiceName() {
            return this.getClass().getSimpleName();
        }

        @Override public void run() {
            TimerMessageStore.log.info(this.getServiceName() + " service start");
            while (!this.isStopped()) {
                try {
                    prepareReadTimeMs();
                    prepareTimerQueueOffset();
                    timerWheel.flush();
                    timerLog.getMappedFileQueue().flush(0);
                    prepareTimerLogFlushPos();
                    timerCheckpoint.flush();
                    //TODO wait how long
                    waitForRunning(200);
                } catch (Exception e) {
                    TimerMessageStore.log.info("Error occurred in " + getServiceName(), e);
                }
            }
            TimerMessageStore.log.info(this.getServiceName() + " service end");
        }
    }

    public void prepareReadTimeMs() {
        timerCheckpoint.setLastReadTimeMs(currReadTimeMs);
    }
    public void prepareTimerLogFlushPos() {
        timerCheckpoint.setLastTimerLogFlushPos(timerLog.getMappedFileQueue().getFlushedWhere());
    }
    public void prepareTimerQueueOffset() {
        timerCheckpoint.setLastTimerQueueOffset(offsetTable.get(0));
    }
}
