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

import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.*;
import org.apache.rocketmq.store.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
    private static final BlockingQueue<TimerRequest> enqueueQueue = new DisruptorBlockingQueue<TimerRequest>(1024); //TODO configture
    private static final BlockingQueue<TimerRequest> dequeueQueue = new DisruptorBlockingQueue<TimerRequest>(1024); //TODO configture

    private final ByteBuffer timerLogBuffer = ByteBuffer.allocate(4 * 1024);
    public static final int INITIAL= 0, RUNNING = 1, HAULT = 2 ,SHUTDOWN = 3;
    private volatile int state = INITIAL;
    private final ScheduledExecutorService scheduler =
        Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("TimerScheduledThread"));

    private final MessageStore messageStore;
    private final TimerWheel timerWheel;
    private final TimerLog timerLog;
    private final TimerCheckpoint timerCheckpoint;


    private final TimerEnqueueGetService enqueueGetService;
    private final TimerEnqueuePutService enqueuePutService;
    private final TimerDequeueWarmService dequeueWarmService;
    private final TimerDequeueGetService dequeueGetService;
    private final TimerDequeuePutService dequeuePutService;
    private final TimerFlushService timerFlushService;


    private volatile long currReadTimeMs;
    private volatile long currWriteTimeMs;
    private volatile long preReadTimeMs;
    private volatile long commitReadTimeMs;
    private volatile long currQueueOffset; //only one queue that is 0
    private volatile long commitQueueOffset;

    private final int commitLogFileSize;
    private final int timerLogFileSize;



    public TimerMessageStore(final MessageStore messageStore, final String rootDir, final int fileSize) throws IOException {
        this.messageStore = messageStore;
        this.commitLogFileSize = fileSize;
        this.timerLogFileSize =  fileSize / 4;
        this.timerWheel = new TimerWheel(rootDir + File.separator + "timerwheel", 2 * DAY_SECS );
        this.timerLog = new TimerLog(rootDir + File.separator + "timerlog", timerLogFileSize);
        this.timerCheckpoint = new TimerCheckpoint(rootDir + File.separator + "timercheck");
        enqueueGetService = new TimerEnqueueGetService();
        enqueuePutService = new TimerEnqueuePutService();
        dequeueWarmService = new TimerDequeueWarmService();
        dequeueGetService = new TimerDequeueGetService();
        dequeuePutService = new TimerDequeuePutService();
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
        if (lastFlushPos < 0) lastFlushPos = 0;
        long processOffset = recoverAndRevise(lastFlushPos, true);
        //revise queue offset
        long queueOffset = reviseQueueOffset(processOffset);
        if (-1 == queueOffset) {
            currQueueOffset = timerCheckpoint.getLastTimerQueueOffset();
        } else {
            currQueueOffset = queueOffset + 1;
        }


        //check timer wheel
        currReadTimeMs = timerCheckpoint.getLastReadTimeMs();
        if (currReadTimeMs == 0) {
            currReadTimeMs = (System.currentTimeMillis()/1000) * 1000 - 2 * DAY_SECS * 1000;
        }
        long minFirst = timerWheel.checkPhyPos(currReadTimeMs/1000, processOffset);
        if (minFirst < processOffset) {
            recoverAndRevise(processOffset, false);
        }
        log.info("Timer recover ok currReadTimerMs:{} currQueueOffset:{} checkQueueOffset:{}", currReadTimeMs, currQueueOffset, timerCheckpoint.getLastTimerQueueOffset());

        commitReadTimeMs = currReadTimeMs;
        commitQueueOffset = currQueueOffset;

    }

    public long reviseQueueOffset(long processOffset) {
        SelectMappedBufferResult selectRes = timerLog.getTimerMessage(processOffset - (TimerLog.UNIT_SIZE - TimerLog.UNIT_PRE_SIZE));
        if (null == selectRes) {
            return -1;
        }
        long offsetPy = selectRes.getByteBuffer().getLong();
        int sizePy = selectRes.getByteBuffer().getInt();
        MessageExt messageExt = getMessageByCommitOffset(offsetPy, sizePy);
        if (null == messageExt) {
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
            for (; position < sbr.getSize() ; position += TimerLog.UNIT_SIZE) {
                try {
                    bf.position(position);
                    bf.getInt();//size
                    bf.getLong();//prev pos
                    int magic = bf.getInt();
                    if (checkTimerLog && !isMagicOK(magic)) {
                        stopCheck = true;
                        break;
                    }
                    long delayTime = bf.getLong() + bf.getInt();
                    timerWheel.reviseSlot(delayTime/1000, TimerWheel.IGNORE, position, true);
                } catch (Exception e) {
                    stopCheck = true;
                    break;
                }
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

    public static boolean isMagicOK(int magic) {
        return (magic | 0xF) == 0xF;
    }

    public void start() {
        maybeMoveWriteTime();
        if (currWriteTimeMs - currReadTimeMs + TIME_BLANK >= timerWheel.TTL_SECS * 1000) {
            currReadTimeMs = currWriteTimeMs - timerWheel.TTL_SECS * 1000 + TIME_BLANK;
        }
        enqueueGetService.start();
        enqueuePutService.start();
        dequeueWarmService.start();
        dequeueGetService.start();
        dequeuePutService.start();
        timerFlushService.start();
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override public void run() {
                try {
                    long minPy = messageStore.getMinPhyOffset();
                    int checkOffset = timerLog.getOffsetForLastUnit();
                    timerLog.getMappedFileQueue().deleteExpiredFileByOffsetForTimerLog(minPy, checkOffset, TimerLog.UNIT_SIZE);
                } catch (Exception e) {
                    log.error("Error in cleaning timerlog", e);
                }
            }
        }, 30, 30, TimeUnit.SECONDS);
        state = RUNNING;
        log.info("Timer start ok currReadTimerMs:[{}] queueOffset:[{}]", new Timestamp(currReadTimeMs), currQueueOffset);
    }
    public void shutdown() {
        if (SHUTDOWN == state) {
            return;
        }
        state = SHUTDOWN;

        enqueueGetService.shutdown();
        enqueuePutService.shutdown();
        dequeueWarmService.shutdown();
        dequeueGetService.shutdown();
        dequeuePutService.shutdown();
        timerFlushService.shutdown();

        timerWheel.shutdown();
        timerLog.shutdown();
        prepareReadTimeMs();
        prepareTimerQueueOffset();
        prepareTimerLogFlushPos();
        timerCheckpoint.shutdown(); //TODO if the previous shutdown failed

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
        long offset = currQueueOffset;
        SelectMappedBufferResult bufferCQ = cq.getIndexBuffer(offset);
        if (null == bufferCQ) {
            return false;
        }
        try {
            int i = 0;
            for ( ; i < bufferCQ.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                if (!isRunning()) break;
                maybeMoveWriteTime();
                perfs.startTick("enqueue_get");
                try {
                    long offsetPy = bufferCQ.getByteBuffer().getLong();
                    int sizePy = bufferCQ.getByteBuffer().getInt();
                    bufferCQ.getByteBuffer().getLong(); //tags code
                    MessageExt msgExt = getMessageByCommitOffset(offsetPy, sizePy);
                    if (null == msgExt) {
                        perfs.getCounter("enqueue_get_miss");
                    } else {
                        long delayedTime = Long.valueOf(msgExt.getProperty(TIMER_DELAY_MS));
                        enqueueQueue.put(new TimerRequest(offsetPy, sizePy, delayedTime, System.currentTimeMillis(), -1, msgExt));
                    }
                } catch (Exception e) {
                    log.warn("Error in enqueuing", e);
                } finally {
                    perfs.endTick("enqueue_get");
                }
            }
            currQueueOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
            return  i > 0;
        } catch (Exception e) {
           log.error("Unknown exception in enqueuing", e);
        } finally {
            bufferCQ.release();
        }
        return false;
    }


    public void doEnqueue(long offsetPy, int sizePy, long delayedTime, MessageExt messageExt) {
        log.debug("Do enqueue [{}] [{}]", new Timestamp(delayedTime), messageExt);
        if (delayedTime < currWriteTimeMs) {
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
        long ret = timerLog.append(tmpBuffer.array(), 0, TimerLog.UNIT_SIZE);
        if (-1 != ret) {
            timerWheel.putSlot(delayedTime/1000, slot.FIRST_POS == -1 ? ret : slot.FIRST_POS, ret);
        }
        maybeMoveWriteTime();
    }

    public int warmDequeue() {
        if (!isRunning()) return -1;
        if (preReadTimeMs <= currReadTimeMs) {
            preReadTimeMs = currReadTimeMs + 1000;
        }
        if (preReadTimeMs >= currWriteTimeMs) {
            return -1;
        }
        if (preReadTimeMs >= currReadTimeMs + 3000) {
            return -1;
        }
        Slot slot = timerWheel.getSlot(preReadTimeMs/1000);
        if (-1 == slot.TIME_SECS) {
            preReadTimeMs = preReadTimeMs + 1000;
            return 0;
        }
        long currOffsetPy = slot.LAST_POS;
        LinkedList<SelectMappedBufferResult> sbrs =  new LinkedList<>();
        SelectMappedBufferResult timeSbr = null;
        SelectMappedBufferResult msgSbr = null;
        try {
            //read the msg one by one
            while (currOffsetPy != -1) {
                if (!isRunning()) break;
                perfs.startTick("warm_dequeue");
                if (null == timeSbr || timeSbr.getStartOffset() > currOffsetPy) {
                    timeSbr = timerLog.getWholeBuffer(currOffsetPy);
                    if (null != timeSbr) sbrs.add(timeSbr);
                }
                if (null == timeSbr) break;
                long prevPos = -1;
                try {
                    int position = (int) (currOffsetPy % timerLogFileSize);
                    timeSbr.getByteBuffer().position(position);
                    timeSbr.getByteBuffer().getInt(); //size
                    prevPos = timeSbr.getByteBuffer().getLong();
                    timeSbr.getByteBuffer().position(position + 20);
                    long offsetPy = timeSbr.getByteBuffer().getLong();
                    int sizePy = timeSbr.getByteBuffer().getInt();
                    if (null == msgSbr || msgSbr.getStartOffset() > offsetPy) {
                        msgSbr = messageStore.getCommitLogData(offsetPy - offsetPy % commitLogFileSize);
                        if (null != msgSbr) sbrs.add(msgSbr);
                    }
                    if (null != msgSbr) {
                        ByteBuffer bf = msgSbr.getByteBuffer();
                        int firstPos = (int) (offsetPy % commitLogFileSize);
                        for (int pos = firstPos; pos < firstPos + sizePy; pos += 4096) {
                            bf.position(pos);
                            bf.get();
                        }
                    }
                } catch (Exception e)  {
                    log.error("Unexpected error in warm", e);
                } finally {
                    currOffsetPy = prevPos;
                    perfs.endTick("warm_dequeue");
                }
            }
            for (SelectMappedBufferResult sbr : sbrs) {
                if (null != sbr) sbr.release();
            }
        } finally {
            preReadTimeMs = preReadTimeMs + 1000;
        }
        return 1;
    }


    public int dequeue() {
        if (!isRunning()) return -1;
        if (currReadTimeMs >= currWriteTimeMs) {
            return -1;
        }

        Slot slot = timerWheel.getSlot(currReadTimeMs/1000);
        if (-1 == slot.TIME_SECS) {
            moveReadTime();
            //System.out.println("Dequeue1:" + new Timestamp(currReadTimeMs));
            return 0;
        }
        long currOffsetPy = slot.LAST_POS;
        Set<String>  deleteUniqKeys = new HashSet<>(4);
        LinkedList<TimerRequest> stack =  new LinkedList<>();
        LinkedList<SelectMappedBufferResult> sbrs =  new LinkedList<>();
        SelectMappedBufferResult timeSbr = null;
        try {
            //read the msg one by one
            while (currOffsetPy != -1) {
                if (!isRunning()) break;
                perfs.startTick("dequeue_get_1");
                if (null == timeSbr || timeSbr.getStartOffset() > currOffsetPy) {
                    timeSbr = timerLog.getWholeBuffer(currOffsetPy);
                    if (null != timeSbr) sbrs.add(timeSbr);
                }
                if (null == timeSbr) break;
                long prevPos = -1;
                try {
                    int position = (int) (currOffsetPy % timerLogFileSize);
                    timeSbr.getByteBuffer().position(position);
                    timeSbr.getByteBuffer().getInt(); //size
                    prevPos = timeSbr.getByteBuffer().getLong();
                    int magic = timeSbr.getByteBuffer().getInt();
                    long enqueueTime = timeSbr.getByteBuffer().getLong();
                    long delayedTime = timeSbr.getByteBuffer().getInt() + enqueueTime;
                    long offsetPy = timeSbr.getByteBuffer().getLong();
                    int sizePy = timeSbr.getByteBuffer().getInt();
                    if (needDelete(magic) && !needRoll(magic)) {
                        MessageExt msgExt = getMessageByCommitOffset(offsetPy, sizePy);
                        if (null != msgExt) {
                            deleteUniqKeys.add("" + msgExt.getProperty(TIMER_DELETE_UNIQKEY));
                        } else {
                            perfs.getCounter("dequeue_get_1_miss").flow(1);
                        }
                    } else {
                        stack.addFirst(new TimerRequest(offsetPy, sizePy, delayedTime, enqueueTime, magic));
                    }
                } catch (Exception e) {
                    log.error("Error in dequeue", e);
                } finally {
                    currOffsetPy =  prevPos;
                    perfs.endTick("dequeue_get_1");
                }
            }
            for(SelectMappedBufferResult sbr: sbrs) {
                if (null != sbr) sbr.release();
            }
            TimerRequest tr = stack.pollFirst();
            while (tr != null) {
                perfs.startTick("dequeue_get_2");
                try {
                    MessageExt msgExt = getMessageByCommitOffset(tr.getOffsetPy(), tr.getSizePy());
                    if (null == msgExt) {
                        perfs.getCounter("dequeue_get_2_miss").flow(1);
                    } else {
                        if (!deleteUniqKeys.contains(MessageClientIDSetter.getUniqID(msgExt))) {
                            tr.setMsg(msgExt);
                            dequeueQueue.put(tr);
                        }
                    }
                } catch (Exception e) {
                    log.error("Unknown exception in dequeue", e);
                } finally {
                    perfs.endTick("dequeue_get_2");
                }
                tr =  stack.pollFirst();
            }
        } finally {
            //avoid blocking if some unknown exception occurs
            moveReadTime();
        }
        return 1;
    }

    private MessageExt getMessageByCommitOffset(long offsetPy, int sizePy) {
        if (offsetPy < messageStore.getMinPhyOffset()) {
            return null;
        }
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



    private void handleRetry(MessageExtBrokerInner message) {
        PutMessageResult putMessageResult;
        int retryNum = 1;
        try {
            while (retryNum <= 3) {
                Thread.sleep(100);
                putMessageResult = messageStore.putMessage(message);
                log.warn("Retrying do reput for timerLog retryNum:{} putRes:{} msg:{}", retryNum, putMessageResult, message);
                if (null == putMessageResult) {
                    retryNum++;
                } else if (putMessageResult.isOk()){
                    break;
                } else {
                    switch (putMessageResult.getPutMessageStatus()) {
                        case SERVICE_NOT_AVAILABLE:
                            //will block and wait
                            break;
                        case MESSAGE_ILLEGAL:
                        case PROPERTIES_SIZE_EXCEEDED:
                        case PUT_OK:
                            retryNum =  Integer.MAX_VALUE;
                            break;
                        case CREATE_MAPEDFILE_FAILED:
                        case FLUSH_DISK_TIMEOUT:
                        case FLUSH_SLAVE_TIMEOUT:
                        case OS_PAGECACHE_BUSY:
                        case SLAVE_NOT_AVAILABLE:
                        case UNKNOWN_ERROR:
                        default:
                            retryNum++;
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Unknown error in retrying reput msg:{}", message, e);
        }
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
        MessageExtBrokerInner message = convertMessage(messageExt, needRoll);
        PutMessageResult putMessageResult = messageStore.putMessage(message);
        if (null == putMessageResult || !putMessageResult.isOk()) {
            handleRetry(message);
        }
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

    class TimerEnqueueGetService  extends ServiceThread {

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

    class TimerEnqueuePutService  extends ServiceThread {

        @Override public String getServiceName() {
            return this.getClass().getSimpleName();
        }

        @Override public void run() {
            TimerMessageStore.log.info(this.getServiceName() + " service start");
            while (!this.isStopped() || enqueueQueue.size() != 0) {
                try {
                    long tmpCommitQueueOffset = currQueueOffset;
                    TimerRequest req =  enqueueQueue.poll(10, TimeUnit.MILLISECONDS);
                    if (null == req) {
                        commitQueueOffset =  tmpCommitQueueOffset;
                        maybeMoveWriteTime();
                    } else {
                        perfs.startTick("enqueue_put");
                        doEnqueue(req.getOffsetPy(), req.getSizePy(), req.getDelayTime(), req.getMsg());
                        if (enqueueQueue.size() == 0) {
                            commitQueueOffset = tmpCommitQueueOffset;
                        } else {
                            commitQueueOffset = req.getMsg().getQueueOffset();
                        }
                        perfs.endTick("enqueue_put");
                    }
                } catch (Exception e) {
                    TimerMessageStore.log.error("Error occurred in " + getServiceName(), e);
                }
            }
            TimerMessageStore.log.info(this.getServiceName() + " service end");
        }
    }


    class TimerDequeueGetService  extends ServiceThread {

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

    class TimerDequeuePutService  extends ServiceThread {

        @Override public String getServiceName() {
            return this.getClass().getSimpleName();
        }

        @Override public void run() {
            TimerMessageStore.log.info(this.getServiceName() + " service start");
            while (!this.isStopped() || dequeueQueue.size() != 0) {
                try {
                    long tmpReadTimeMs =  currReadTimeMs;
                    TimerRequest tr = dequeueQueue.poll(50, TimeUnit.MILLISECONDS);
                    if (null != tr) {
                        perfs.startTick("dequeue_put");
                        doReput(tr.getMsg(), tr.getEnqueueTime(), needRoll(tr.getMagic()));
                        if (dequeueQueue.size() == 0) {
                            commitReadTimeMs = tmpReadTimeMs;
                        } else {
                            commitReadTimeMs = tr.getDelayTime();
                        }
                        perfs.endTick("dequeue_put");
                    } else {
                        commitReadTimeMs = tmpReadTimeMs;
                    }
                } catch (Exception e) {
                    TimerMessageStore.log.error("Error occurred in " + getServiceName(), e);
                }
            }
            TimerMessageStore.log.info(this.getServiceName() + " service end");
        }
    }



    class TimerDequeueWarmService extends ServiceThread {

        @Override public String getServiceName() {
            return this.getClass().getSimpleName();
        }

        @Override public void run() {
            TimerMessageStore.log.info(this.getServiceName() + " service start");
            while (!this.isStopped()) {
                try {
                    if( -1 == TimerMessageStore.this.warmDequeue()) {
                        waitForRunning(50);
                    }
                } catch (Exception e) {
                    TimerMessageStore.log.error("Error occurred in " + getServiceName(), e);
                }
            }
            TimerMessageStore.log.info(this.getServiceName() + " service end");
        }
    }

    public boolean needRoll(int magic) {
        return (magic & MAGIC_ROLL) != 0;
    }

    public boolean needDelete(int magic) {
        return (magic & MAGIC_DELETE) != 0;
    }

    class TimerFlushService extends ServiceThread {
        private final SimpleDateFormat sdf = new SimpleDateFormat("MM-dd HH:mm:ss");
        @Override public String getServiceName() {
            return this.getClass().getSimpleName();
        }

        private String format(long time) {
            return sdf.format(new Date(time));
        }
        @Override public void run() {
            TimerMessageStore.log.info(this.getServiceName() + " service start");
            long start = System.currentTimeMillis();
            while (!this.isStopped()) {
                try {
                    prepareReadTimeMs();
                    prepareTimerQueueOffset();
                    prepareTimerLogFlushPos();
                    timerLog.getMappedFileQueue().flush(0);
                    timerWheel.flush();
                    timerCheckpoint.flush();
                    //TODO wait how long
                    waitForRunning(1000);
                    if (System.currentTimeMillis() - start > 3000) {
                        start =  System.currentTimeMillis();
                        ConsumeQueue cq = messageStore.getConsumeQueue(TIMER_TOPIC, 0);
                        TimerMessageStore.log.info("Timer progress commitRead:[{}] currRead:[{}] preRead:[{}] currWrite:[{}]  commitOffset:{} queueOffset:{}  readBehind:{} offsetBehind:{} enqSize:{} deqSize:{}",
                            format(commitReadTimeMs), format(currReadTimeMs), format(preReadTimeMs), format(currWriteTimeMs), commitQueueOffset, currQueueOffset, (System.currentTimeMillis() - currReadTimeMs)/1000,
                            cq == null ? 0 : cq.getMaxOffsetInQueue() - currQueueOffset, enqueueQueue.size(), dequeueQueue.size());
                    }
                } catch (Exception e) {
                    TimerMessageStore.log.error("Error occurred in " + getServiceName(), e);
                }
            }
            TimerMessageStore.log.info(this.getServiceName() + " service end");
        }
    }

    public void prepareReadTimeMs() {
        timerCheckpoint.setLastReadTimeMs(commitReadTimeMs);
    }
    public void prepareTimerLogFlushPos() {
        timerCheckpoint.setLastTimerLogFlushPos(timerLog.getMappedFileQueue().getFlushedWhere());
    }
    public void prepareTimerQueueOffset() {
        timerCheckpoint.setLastTimerQueueOffset(commitQueueOffset);
    }

    public long getCurrReadTimeMs() {
        return this.currReadTimeMs;
    }

    public long getQueueOffset() {
        return currQueueOffset;
    }

    public long getCommitQueueOffset() {
        return this.commitQueueOffset;
    }

    public long getCommitReadTimeMs() {
        return this.commitReadTimeMs;
    }



}
