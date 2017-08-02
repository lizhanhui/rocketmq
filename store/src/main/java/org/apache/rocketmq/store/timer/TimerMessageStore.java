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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.MappedFile;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimerMessageStore {
    public static final String TIMER_TOPIC = "%SYS_TIMER_TOPIC%";
    public static final String TIMER_DELAY_MS = MessageConst.PROPERTY_TIMER_IN_MS;
    public static final String TIMER_ENQUEUE_MS = MessageConst.PROPERTY_TIMER_ENQUEUE_MS;
    public static final String TIMER_DEQUEUE_MS = MessageConst.PROPERTY_TIMER_DEQUEUE_MS;
    public static final String TIMER_ROLL_TIMES = MessageConst.PROPERTY_TIMER_ROLL_TIMES;
    public static final String TIMER_DELETE_UNIQKEY = MessageConst.PROPERTY_TIMER_DEL_UNIQKEY;
    public static final int DAY_SECS = 24 * 3600;
    public static final int TIME_BLANK = 60 * 1000;
    public static final int MAGIC_DEFAULT = 1;
    public static final int MAGIC_ROLL = 1 << 1;
    public static final int MAGIC_DELETE = 1 << 2;

    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final PerfCounter.Ticks perfs = new PerfCounter.Ticks(log);
    //currently only use the queue 0
    private final ConcurrentMap<Integer /* queue */, Long/* offset */> offsetTable =
        new ConcurrentHashMap<Integer, Long>(32);

    private final ByteBuffer timerLogBuffer = ByteBuffer.allocate(1024);
    public static final int INITIAL= 0, RUNNING = 1, HAULT = 2 ,SHUTDOWN = 3;
    private volatile int state = INITIAL;
    private final ScheduledExecutorService scheduler =
        Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("TimerScheduledThread"));

    private final MessageStore messageStore;
    private final TimerWheel timerWheel;
    private final TimerLog timerLog;
    private final TimerCheckpoint timerCheckpoint;

    private final TimerEnqueueService enqueueService;
    private final TimerDequeueService dequeueService;
    private final TimerFlushService timerFlushService;


    private volatile long currReadTimeMs;
    private volatile long currWriteTimeMs;


    public TimerMessageStore(final MessageStore messageStore, final String rootDir, final int fileSize) throws IOException {
        this.messageStore = messageStore;
        this.timerWheel = new TimerWheel(rootDir + File.separator + "timerwheel", 2 * DAY_SECS );
        this.timerLog = new TimerLog(rootDir + File.separator + "timerlog", fileSize);
        this.timerCheckpoint = new TimerCheckpoint(rootDir + File.separator + "timercheck");
        enqueueService = new TimerEnqueueService();
        dequeueService =  new TimerDequeueService();
        timerFlushService = new TimerFlushService();

    }

    public boolean load() {
        boolean load = timerLog.load();
        recover();
        return load;
    }


    public void recover() {
        //recover timerLog
        long lastFlushPos = timerCheckpoint.getLastTimerLogFlushPos();
        MappedFile lastFile = timerLog.getMappedFileQueue().getLastMappedFile();
        if (null != lastFile) {
            lastFlushPos = lastFlushPos - lastFile.getFileSize();
        }
        long processOffset = recoverAndRevise(lastFlushPos, true);
        //revise queue offset
        long queueOffset = reviseQueueOffset(processOffset);
        offsetTable.put(0, queueOffset + 1);
        prepareTimerLogFlushPos();

        //check timer wheel
        currReadTimeMs = timerCheckpoint.getLastReadTimeMs();
        long minFirst = timerWheel.checkPhyPos(currReadTimeMs, processOffset);
        if (minFirst < processOffset) {
            recoverAndRevise(processOffset, false);
        }
        log.info("Timer recover ok currReadTimerMs:{} queueOffset:{}", currReadTimeMs, getQueueOffset(0));
        //TODO dequeue old messages  only master do
        /*
        maybeMoveWriteTime();
        while (currReadTimeMs < currWriteTimeMs - ((timerWheel.TTL_SECS * 1000)/2)) {
            dequeue();
        }
        */

    }

    public long reviseQueueOffset(long processOffset) {
        SelectMappedBufferResult selectRes = timerLog.getTimerMessage(processOffset - 12);
        if (null == selectRes) {
            //TODO
            return -1;
        }
        long offsetPy = selectRes.getByteBuffer().getLong();
        int sizePy = selectRes.getByteBuffer().getInt();
        MessageExt messageExt = getMessageByCommitOffset(offsetPy, sizePy);
        if (null == messageExt) {
            //TODO
            return -1;
        }
        return messageExt.getQueueOffset();
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

    public static boolean checkMagic(int magic) {
        if ((magic & MAGIC_DEFAULT) == 0) {
            return false;
        }
        if ((magic & 0xFFFFFFF8) != 0) {
            return false;
        }
        return true;
    }

    public void start() {
        maybeMoveWriteTime();
        if (currWriteTimeMs - currReadTimeMs + TIME_BLANK >= timerWheel.TTL_SECS * 1000) {
            currReadTimeMs = currWriteTimeMs - timerWheel.TTL_SECS * 1000 + TIME_BLANK;
        }
        enqueueService.start();
        dequeueService.start(); //TODO only master do
        timerFlushService.start();
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override public void run() {
                timerLog.cleanExpiredFiles(timerWheel.TTL_SECS * 2 * 1000L);
            }
        }, 30 * 1000, 30, TimeUnit.SECONDS);
        state = RUNNING;
        log.info("Timer start ok currReadTimerMs:{} queueOffset:{}", currReadTimeMs, offsetTable.get(0));
    }
    public void shutdown() {
        if (SHUTDOWN == state) {
            return;
        }
        state = SHUTDOWN;
        enqueueService.shutdown();
        dequeueService.shutdown(); //TODO if not start, will it throw exception
        timerFlushService.shutdown();
        timerWheel.shutdown();
        timerLog.shutdown();
        prepareReadTimeMs();
        prepareTimerQueueOffset();
        prepareTimerLogFlushPos();
        timerCheckpoint.shutdown(); //TODO if the previous shutdown failed

    }

    //testable
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

    //testable
    public void setState(int state) {
        this.state = state;
    }

    private boolean isRunning() {
        return RUNNING == state;
    }

    public boolean enqueue(int queueId) {
        if (!isRunning()) {
            return false;
        }
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
                if (!isRunning()) break;
                maybeMoveWriteTime();
                perfs.startTick("enqueue");
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
                    //TODO TIMER_DELAY_MS dose not exist
                    long delayedTime = Long.valueOf(msgExt.getProperty(TIMER_DELAY_MS));

                    doEnqueue(offsetPy, sizePy, delayedTime, msgExt);
                } catch (Exception e) {
                    //TODO retry
                    e.printStackTrace();
                }
                perfs.endTick("enqueue");
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


    public void doEnqueue(long offsetPy, int sizePy, long delayedTime, MessageExt messageExt) {
        log.debug("Do enqueue [{}]", messageExt);
        if (delayedTime < currWriteTimeMs) {
            //TODO
            doReput(messageExt, System.currentTimeMillis(), false);
            return;
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
        int magic = MAGIC_DEFAULT;
        if (needRoll) {
            magic = magic | MAGIC_ROLL;
            delayedTime =  currWriteTimeMs +  timerWheel.TTL_SECS * 1000 - TIME_BLANK;
        }
        boolean isDelete = messageExt.getProperty(TIMER_DELETE_UNIQKEY) != null;
        if (isDelete) {
            magic = magic | MAGIC_DELETE;
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
        long ret = timerLog.append(tmpBuffer.array(), 0, TimerLog.UNIT_SIZE);
        if (-1 == ret) {
            //TODO
        }
        timerWheel.putSlot(delayedTime/1000, slot.FIRST_POS == -1 ? ret : slot.FIRST_POS, ret);
        maybeMoveWriteTime();

    }

    public int dequeue() {
        if (!isRunning()) return -1;
        if (currReadTimeMs >= currWriteTimeMs) {
            return -1;
        }
        Slot slot = timerWheel.getSlot(currReadTimeMs/1000);
        if (-1 == slot.TIME_SECS) {
            moveReadTime();
            return 0;
        }
        long currOffsetPy = slot.LAST_POS;
        Set<String>  deleteUniqKeys = new HashSet<>();
        LinkedList<SelectMappedBufferResult> stack =  new LinkedList<>();
        //read the msg one by one
        while (currOffsetPy != -1) {
            if (!isRunning()) break;
            perfs.startTick("dequeue_1");
            SelectMappedBufferResult selectRes = timerLog.getTimerMessage(currOffsetPy);
            if (null == selectRes) break;
            selectRes.getByteBuffer().mark();
            selectRes.getByteBuffer().getInt(); //size
            long prevPos = selectRes.getByteBuffer().getLong();
            int magic = selectRes.getByteBuffer().getInt();
            if ((magic & MAGIC_DELETE) != 0 && (magic & MAGIC_ROLL) == 0) {
                long enqueueTime = selectRes.getByteBuffer().getLong();
                long delayedTime = selectRes.getByteBuffer().getInt() + enqueueTime;
                long offsetPy = selectRes.getByteBuffer().getLong();
                int sizePy = selectRes.getByteBuffer().getInt();
                MessageExt msgExt = getMessageByCommitOffset(offsetPy, sizePy);
                if (null == msgExt) {
                    continue;
                }
                if (msgExt.getProperty(TIMER_DELETE_UNIQKEY) != null) {
                    deleteUniqKeys.add(msgExt.getProperty(TIMER_DELETE_UNIQKEY));
                } else {
                    log.warn("Timer magic is delete, but do not have {} [{}]", TIMER_DELETE_UNIQKEY, msgExt);
                }
            } else {
                selectRes.getByteBuffer().reset();
                stack.push(selectRes);
                if (slot.FIRST_POS != -1 && currOffsetPy < slot.FIRST_POS) {
                    break;
                }
            }
            perfs.endTick("dequeue_1");
            currOffsetPy = prevPos;
        }
        int pollNum = 0;
        SelectMappedBufferResult sbr = stack.pollFirst();
        while (sbr != null) {
            perfs.startTick("dequeue_2");
            try {
                ByteBuffer bf = sbr.getByteBuffer();
                bf.getInt(); //size
                bf.getLong(); //prev pos
                int magic = bf.getInt();
                if ((magic & MAGIC_DELETE) != 0 && (magic & MAGIC_ROLL) == 0) {
                    continue;
                }
                long enqueueTime = bf.getLong();
                long delayedTime = bf.getInt() + enqueueTime;
                long offsetPy = bf.getLong();
                int sizePy = bf.getInt();
                MessageExt msgExt = getMessageByCommitOffset(offsetPy, sizePy);
                if (null == msgExt) {
                    log.warn("Get message failed in dequeuing offsetPy:{} sizePy:{}", offsetPy, sizePy);
                    //TODO
                }
                if (!deleteUniqKeys.contains(MessageClientIDSetter.getUniqID(msgExt))) {
                    doReput(msgExt, enqueueTime, (magic & MAGIC_ROLL) != 0);
                }
            } catch (Exception e) {
                //TODO
                log.error("Unknown exception in dequeuing", e);
            } finally {
                sbr.release();
            }
            if (++pollNum % 10 == 0) {
                timerWheel.putSlot(currReadTimeMs/1000, sbr.getStartOffset(), slot.LAST_POS);
            }
            if (!isRunning()) {
                timerWheel.putSlot(currReadTimeMs/1000, sbr.getStartOffset(), slot.LAST_POS);
                return 1;
            }
            perfs.endTick("dequeue_2");
            sbr =  stack.pollFirst();
        }
        log.debug("Do dequeue currReadTimeMs:{} pollNum:{} deleteKeys:{}", getCurrReadTimeMs(), pollNum, deleteUniqKeys);
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
            MessageAccessor.putProperty(messageExt, TIMER_ENQUEUE_MS, enqueueTime + "");
        }
        if (needRoll) {
            if (messageExt.getProperty(TIMER_ROLL_TIMES) != null) {
                MessageAccessor.putProperty(messageExt, TIMER_ROLL_TIMES,Integer.parseInt(messageExt.getProperty(TIMER_ROLL_TIMES)) + 1 + "");
            } else {
                MessageAccessor.putProperty(messageExt, TIMER_ROLL_TIMES,1 + "");
            }
        }
        MessageAccessor.putProperty(messageExt, TIMER_DEQUEUE_MS, System.currentTimeMillis() + "");
        PutMessageResult putMessageResult = messageStore.putMessage(convertMessage(messageExt, needRoll));
        log.debug("Timer do reput [{}]", putMessageResult);
        if (null == putMessageResult
            ||PutMessageStatus.PUT_OK != putMessageResult.getPutMessageStatus()) {
            log.warn("Timer do reput error [{}]", putMessageResult);
        }
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
                    TimerMessageStore.log.error("Error occurred in " + getServiceName(), e);
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
                    if( -1 == TimerMessageStore.this.dequeue()) {
                        waitForRunning(50);
                    }
                } catch (Exception e) {
                    TimerMessageStore.log.error("Error occurred in " + getServiceName(), e);
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
            long start = System.currentTimeMillis();
            while (!this.isStopped()) {
                try {
                    prepareReadTimeMs();
                    prepareTimerQueueOffset();
                    timerLog.getMappedFileQueue().flush(0);
                    prepareTimerLogFlushPos();
                    timerWheel.flush();
                    //make sure the timerwheel is behind the timerlog
                    timerLog.getMappedFileQueue().flush(0);
                    timerCheckpoint.flush();
                    //TODO wait how long
                    waitForRunning(200);
                    if (System.currentTimeMillis() - start > 3000) {
                        start =  System.currentTimeMillis();
                        TimerMessageStore.log.info("Timer progress currRead:%d currWrite:%d readBehind:%d offsetBehind:%d",
                            currReadTimeMs/1000, currWriteTimeMs/1000, (System.currentTimeMillis() - currReadTimeMs)/1000,
                            messageStore.getConsumeQueue(TIMER_TOPIC, 0).getMaxOffsetInQueue() - offsetTable.get(0));
                    }
                } catch (Exception e) {
                    TimerMessageStore.log.error("Error occurred in " + getServiceName(), e);
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

    public long getCurrReadTimeMs() {
        return this.currReadTimeMs;
    }

    public long getQueueOffset(int queue) {
        if (null == offsetTable.get(queue)) {
            return 0;
        }
        return offsetTable.get(queue);
    }

}
