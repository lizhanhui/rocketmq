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
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
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
    private final int timerRollWindowSec;
    private final int ttlSecs;
    private final MessageStoreConfig storeConfig;
    private volatile BrokerRole lastBrokerRole = BrokerRole.SLAVE;



    public TimerMessageStore(final MessageStore messageStore, final MessageStoreConfig storeConfig , TimerCheckpoint timerCheckpoint) throws IOException {
        this.messageStore = messageStore;
        this.storeConfig = storeConfig;
        this.commitLogFileSize = storeConfig.getMapedFileSizeCommitLog();
        this.timerLogFileSize =  storeConfig.getMappedFileSizeTimerLog();
        this.ttlSecs = 2 * DAY_SECS;
        this.timerWheel = new TimerWheel(getTimerWheelPath(storeConfig.getStorePathRootDir()), 2 * DAY_SECS );
        this.timerLog = new TimerLog(getTimerLogPath(storeConfig.getStorePathRootDir()), timerLogFileSize);
        this.timerCheckpoint =  timerCheckpoint;
        this.lastBrokerRole = storeConfig.getBrokerRole();
        if (storeConfig.getTimerRollWindowSec() > ttlSecs - TIME_BLANK || storeConfig.getTimerRollWindowSec() < 3600) {
            this.timerRollWindowSec = ttlSecs - TIME_BLANK;
        } else {
            this.timerRollWindowSec = storeConfig.getTimerRollWindowSec();
        }
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


    public static String getTimerCheckPath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "timercheck";
    }

    public static String getTimerWheelPath(final String rootDir) {
        return rootDir + File.separator + "timerwheel";
    }

    public static String getTimerLogPath(final  String rootDir) {
        return rootDir + File.separator + "timerlog";
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
        currQueueOffset = Math.min(currQueueOffset, timerCheckpoint.getMasterTimerQueueOffset());


        //check timer wheel
        currReadTimeMs = timerCheckpoint.getLastReadTimeMs();
        if (currReadTimeMs < (System.currentTimeMillis()/1000) * 1000 - ttlSecs * 1000 + TIME_BLANK ) {
            currReadTimeMs = (System.currentTimeMillis()/1000) * 1000 - ttlSecs * 1000 + TIME_BLANK;
        }
        long minFirst = timerWheel.checkPhyPos(currReadTimeMs/1000, processOffset);
        if (minFirst < processOffset) {
            log.warn("Timer recheck because of minFirst:{} processOffset:{}", minFirst, processOffset);
            recoverAndRevise(processOffset, false);
        }
        log.info("Timer recover ok currReadTimerMs:{} currQueueOffset:{} checkQueueOffset:{} processOffset:{}",
                currReadTimeMs, currQueueOffset, timerCheckpoint.getLastTimerQueueOffset(), processOffset);

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
        log.info("Begin to recover timerlog offset:{} check:{}", beginOffset, checkTimerLog);
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
                    int size = bf.getInt();//size
                    bf.getLong();//prev pos
                    int magic = bf.getInt();
                    if (checkTimerLog && (!isMagicOK(magic) || TimerLog.UNIT_SIZE != size)) {
                        stopCheck = true;
                        break;
                    }
                    long delayTime = bf.getLong() + bf.getInt();
                    if (TimerLog.UNIT_SIZE == size && isMagicOK(magic)) {
                        timerWheel.reviseSlot(delayTime/1000, TimerWheel.IGNORE, sbr.getStartOffset() + position, true);
                    }
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
        prepareTimerCheckPoint();
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

    private boolean isRunning() {
        return RUNNING == state;
    }

    private boolean isRunningEnqueue() {
        BrokerRole currRole = storeConfig.getBrokerRole();
        if (lastBrokerRole != currRole) {
            log.info("Broker role change from {} to {}", lastBrokerRole, currRole);
            if (BrokerRole.SLAVE != currRole) {
                currQueueOffset = Math.min(currQueueOffset, timerCheckpoint.getMasterTimerQueueOffset());
                commitQueueOffset = currQueueOffset;
                prepareTimerCheckPoint();
                timerCheckpoint.flush();
                currReadTimeMs = timerCheckpoint.getLastReadTimeMs();
                commitReadTimeMs = currReadTimeMs;
            }
            lastBrokerRole = currRole;
        }
        if (BrokerRole.SLAVE == lastBrokerRole && currQueueOffset >= timerCheckpoint.getMasterTimerQueueOffset()) {
                return false;
        }
        return isRunning();
    }

    private boolean isRunningDequeue() {
        if (BrokerRole.SLAVE == lastBrokerRole) {
            currReadTimeMs = timerCheckpoint.getLastReadTimeMs();
            commitReadTimeMs = currReadTimeMs;
            return false;
        }
        return isRunning();
    }

    public boolean enqueue(int queueId) {
        if (!isRunningEnqueue()) {
            return false;
        }
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
                if (!isRunningEnqueue()) break;
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
                currQueueOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
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
        int size = 4  //size
            +  8 //prev pos
            +  4 //magic value
            +  8 //curr write time, for trace
            +  4 //delayed time, for check
            +  8 //offsetPy
            +  4 //sizePy
            ;
        boolean needRoll =  delayedTime - currWriteTimeMs >= timerRollWindowSec * 1000;
        int magic = MAGIC_DEFAULT;
        if (needRoll) {
            magic = magic | MAGIC_ROLL;
            delayedTime =  currWriteTimeMs +  timerRollWindowSec * 1000;
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
        if (!isRunningDequeue()) return -1;
        if (!storeConfig.isTimerWarmEnable()) return -1;
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
        if (!isRunningDequeue()) return -1;
        if (currReadTimeMs >= currWriteTimeMs) {
            return -1;
        }

        Slot slot = timerWheel.getSlot(currReadTimeMs/1000);
        if (-1 == slot.TIME_SECS) {
            moveReadTime();
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

    private MessageExtBrokerInner convert(MessageExt messageExt, long enqueueTime, boolean needRoll) {
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
        return message;
    }
    private int doPut(MessageExtBrokerInner message) {
        PutMessageResult putMessageResult = messageStore.putMessage(message);
        int retryNum = 0;
        while (retryNum < 3) {
            try {
                if (null == putMessageResult || null == putMessageResult.getPutMessageStatus()) {
                    retryNum++;
                } else {
                    switch (putMessageResult.getPutMessageStatus()) {
                        case PUT_OK:
                            return 0;
                        case SERVICE_NOT_AVAILABLE:
                            return 1;
                        case MESSAGE_ILLEGAL:
                        case PROPERTIES_SIZE_EXCEEDED:
                            return 2;
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
                Thread.sleep(50);
                putMessageResult = messageStore.putMessage(message);
                log.warn("Retrying do put timer msg retryNum:{} putRes:{} msg:{}", retryNum, putMessageResult, message);
            } catch (Exception e) {
                log.warn("Unknown error in retrying do put timer msg:{}", message, e);
            }
        }
        return 2;
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
                } catch (Throwable e) {
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
                        if (req.getDelayTime() < currWriteTimeMs) {
                            MessageExtBrokerInner msg = convert(req.getMsg(), System.currentTimeMillis(), false);
                            int putRes =  doPut(msg);
                            while (putRes == 1 && !isStopped()) {
                                putRes = doPut(msg);
                                Thread.sleep(50);
                            }
                            if (isStopped()) {
                                break;
                            }
                        }
                        doEnqueue(req.getOffsetPy(), req.getSizePy(), req.getDelayTime(), req.getMsg());
                        if (enqueueQueue.size() == 0) {
                            commitQueueOffset = tmpCommitQueueOffset;
                        } else {
                            commitQueueOffset = req.getMsg().getQueueOffset();
                        }
                        perfs.endTick("enqueue_put");
                    }
                } catch (Throwable e) {
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
                } catch (Throwable e) {
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
                        MessageExtBrokerInner msg =  convert(tr.getMsg(), tr.getEnqueueTime(), needRoll(tr.getMagic()));
                        int putRes = doPut(msg);
                        while (1 == putRes && !isStopped()) {
                            doPut(msg);
                            Thread.sleep(50);
                        }
                        if (isStopped()) break;
                        if (dequeueQueue.size() == 0) {
                            commitReadTimeMs = tmpReadTimeMs;
                        } else {
                            commitReadTimeMs = tr.getDelayTime();
                        }
                        perfs.endTick("dequeue_put");
                    } else {
                        commitReadTimeMs = tmpReadTimeMs;
                    }
                } catch (Throwable e) {
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
                } catch (Throwable e) {
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
                    prepareTimerCheckPoint();
                    timerLog.getMappedFileQueue().flush(0);
                    timerWheel.flush();
                    timerCheckpoint.flush();
                    waitForRunning(storeConfig.getTimerFlushIntervalMs());
                    if (System.currentTimeMillis() - start > storeConfig.getTimerProgressLogIntervalMs()) {
                        start =  System.currentTimeMillis();
                        ConsumeQueue cq = messageStore.getConsumeQueue(TIMER_TOPIC, 0);
                        long maxOffsetInQueue = cq == null ? 0 : cq.getMaxOffsetInQueue();
                        TimerMessageStore.log.info("Timer progress-time commitRead:[{}] currRead:[{}] preRead:[{}] currWrite:[{}] readBehind:{} enqSize:{} deqSize:{}",
                                format(commitReadTimeMs), (currReadTimeMs - commitReadTimeMs)/1000, (preReadTimeMs - currReadTimeMs)/1000, format(currWriteTimeMs), (System.currentTimeMillis() - currReadTimeMs)/1000, enqueueQueue.size(), dequeueQueue.size());
                        TimerMessageStore.log.info("Timer progress-offset commitOffset:{} currReadOffset:{} offsetBehind:{} behindMaster:{}",
                                commitQueueOffset, currQueueOffset - commitQueueOffset, maxOffsetInQueue - currQueueOffset, timerCheckpoint.getMasterTimerQueueOffset() - currQueueOffset);
                    }
                } catch (Throwable e) {
                    TimerMessageStore.log.error("Error occurred in " + getServiceName(), e);
                }
            }
            TimerMessageStore.log.info(this.getServiceName() + " service end");
        }
    }
    public void prepareTimerCheckPoint() {
        timerCheckpoint.setLastTimerLogFlushPos(timerLog.getMappedFileQueue().getFlushedWhere());
        if (BrokerRole.SLAVE != lastBrokerRole) {
            timerCheckpoint.setLastReadTimeMs(commitReadTimeMs);
            timerCheckpoint.setMasterTimerQueueOffset(commitQueueOffset);
        }
        timerCheckpoint.setLastTimerQueueOffset(Math.min(commitQueueOffset, timerCheckpoint.getMasterTimerQueueOffset()));
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


    public MessageStore getMessageStore() {
        return messageStore;
    }

    public TimerWheel getTimerWheel() {
        return timerWheel;
    }

    public TimerLog getTimerLog() {
        return timerLog;
    }
}
