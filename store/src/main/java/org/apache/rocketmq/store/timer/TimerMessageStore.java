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
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.MixAll;
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
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimerMessageStore {
    public static final String TIMER_TOPIC = MixAll.SYSTEM_TOPIC_PREFIX + "wheel_timer";
    public static final String TIMER_DELAY_MS = MessageConst.PROPERTY_TIMER_OUT_MS;
    public static final String TIMER_ENQUEUE_MS = MessageConst.PROPERTY_TIMER_ENQUEUE_MS;
    public static final String TIMER_DEQUEUE_MS = MessageConst.PROPERTY_TIMER_DEQUEUE_MS;
    public static final String TIMER_ROLL_TIMES = MessageConst.PROPERTY_TIMER_ROLL_TIMES;
    public static final String TIMER_DELETE_UNIQKEY = MessageConst.PROPERTY_TIMER_DEL_UNIQKEY;
    public static final int PUT_OK = 0, PUT_NEED_RETRY = 1, PUT_NO_RETRY = 2;
    public static final int DAY_SECS = 24 * 3600;
    public static final int TIME_BLANK = 60 * 1000;
    public static final int MAGIC_DEFAULT = 1;
    public static final int MAGIC_ROLL = 1 << 1;
    public static final int MAGIC_DELETE = 1 << 2;

    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final PerfCounter.Ticks perfs = new PerfCounter.Ticks(log);
    private final BlockingQueue<TimerRequest> enqueueQueue = new DisruptorBlockingQueue<TimerRequest>(1024); //TO DO configture
    private final BlockingQueue<List<TimerRequest>> dequeueGetQueue = new DisruptorBlockingQueue<List<TimerRequest>>(1024);
    private final BlockingQueue<TimerRequest> dequeuePutQueue = new DisruptorBlockingQueue<TimerRequest>(1024); //TO DO configture

    private final ByteBuffer timerLogBuffer = ByteBuffer.allocate(4 * 1024);
    private final ThreadLocal<ByteBuffer> bufferLocal;
    public static final int INITIAL = 0, RUNNING = 1, HAULT = 2, SHUTDOWN = 3;
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
    private final TimerDequeuePutMessageService[] dequeuePutMessaageServices;
    private final TimerDequeueGetMessageService[] dequeueGetMessageServices;
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
    private TimerMetrics timerMetrics;
    private AtomicInteger frequency = new AtomicInteger(0);

    public TimerMessageStore(final MessageStore messageStore, final MessageStoreConfig storeConfig,
        TimerCheckpoint timerCheckpoint) throws IOException {
        this.messageStore = messageStore;
        this.storeConfig = storeConfig;
        this.commitLogFileSize = storeConfig.getMapedFileSizeCommitLog();
        this.timerLogFileSize = storeConfig.getMappedFileSizeTimerLog();
        this.ttlSecs = 2 * DAY_SECS;
        this.timerWheel = new TimerWheel(getTimerWheelPath(storeConfig.getStorePathRootDir()), 2 * DAY_SECS);
        this.timerLog = new TimerLog(getTimerLogPath(storeConfig.getStorePathRootDir()), timerLogFileSize);
        this.timerMetrics = new TimerMetrics(storeConfig.getStorePathRootDir());
        this.timerCheckpoint = timerCheckpoint;
        this.lastBrokerRole = storeConfig.getBrokerRole();
        if (storeConfig.getTimerRollWindowSec() > ttlSecs - TIME_BLANK || storeConfig.getTimerRollWindowSec() < 2) {
            this.timerRollWindowSec = ttlSecs - TIME_BLANK;
        } else {
            this.timerRollWindowSec = storeConfig.getTimerRollWindowSec();
        }
        bufferLocal = new ThreadLocal<ByteBuffer>() {
            @Override
            protected ByteBuffer initialValue() {
                return ByteBuffer.allocateDirect(storeConfig.getMaxMessageSize() + 100);
            }
        };
        enqueueGetService = new TimerEnqueueGetService();
        enqueuePutService = new TimerEnqueuePutService();
        dequeueWarmService = new TimerDequeueWarmService();
        dequeueGetService = new TimerDequeueGetService();
        timerFlushService = new TimerFlushService();
        int getThreadNum =  storeConfig.getTimerGetMessageThreadNum();
        if (getThreadNum <= 0) getThreadNum =1;
        dequeueGetMessageServices = new TimerDequeueGetMessageService[getThreadNum];
        for (int i = 0; i < dequeueGetMessageServices.length; i++) {
            dequeueGetMessageServices[i] = new TimerDequeueGetMessageService();
        }
        int putThreadNum = storeConfig.getTimerputMessageThreadNum();
        if (putThreadNum <= 0) putThreadNum = 1;
        dequeuePutMessaageServices = new TimerDequeuePutMessageService[putThreadNum];
        for (int i = 0; i < dequeuePutMessaageServices.length; i++) {
            dequeuePutMessaageServices[i] =  new TimerDequeuePutMessageService();
        }


    }

    public boolean load() {
        boolean load = timerLog.load();
        load = load && this.timerMetrics.load();
        recover();
        return load;
    }

    public static String getTimerCheckPath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "timercheck";
    }

    public static String getTimerWheelPath(final String rootDir) {
        return rootDir + File.separator + "timerwheel";
    }

    public static String getTimerLogPath(final String rootDir) {
        return rootDir + File.separator + "timerlog";
    }

    public void recover() {
        //recover timerLog
        long lastFlushPos = timerCheckpoint.getLastTimerLogFlushPos();
        MappedFile lastFile = timerLog.getMappedFileQueue().getLastMappedFile();
        if (null != lastFile) {
            lastFlushPos = lastFlushPos - lastFile.getFileSize();
        }
        if (lastFlushPos < 0)
            lastFlushPos = 0;
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
        if (currReadTimeMs < (System.currentTimeMillis() / 1000) * 1000 - ttlSecs * 1000 + TIME_BLANK) {
            currReadTimeMs = (System.currentTimeMillis() / 1000) * 1000 - ttlSecs * 1000 + TIME_BLANK;
        }
        long minFirst = timerWheel.checkPhyPos(currReadTimeMs / 1000, processOffset);
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
        if (null == lastFile)
            return 0;

        List<MappedFile> mappedFiles = timerLog.getMappedFileQueue().getMappedFiles();
        int index = mappedFiles.size() - 1;
        for (; index >= 0; index--) {
            MappedFile mappedFile = mappedFiles.get(index);
            if (beginOffset >= mappedFile.getFileFromOffset()) {
                break;
            }
        }
        if (index < 0)
            index = 0;
        long checkOffset = mappedFiles.get(index).getFileFromOffset();
        for (; index < mappedFiles.size(); index++) {
            SelectMappedBufferResult sbr = mappedFiles.get(index).selectMappedBuffer(0, mappedFiles.get(index).getFileSize());
            ByteBuffer bf = sbr.getByteBuffer();
            int position = 0;
            boolean stopCheck = false;
            for (; position < sbr.getSize(); position += TimerLog.UNIT_SIZE) {
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
                        timerWheel.reviseSlot(delayTime / 1000, TimerWheel.IGNORE, sbr.getStartOffset() + position, true);
                    }
                } catch (Exception e) {
                    stopCheck = true;
                    break;
                }
            }
            checkOffset = mappedFiles.get(index).getFileFromOffset() + position;
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
        for (int i = 0; i < dequeueGetMessageServices.length; i++) {
            dequeueGetMessageServices[i].start();
        }
        for (int i = 0; i < dequeuePutMessaageServices.length; i++) {
            dequeuePutMessaageServices[i].start();
        }
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
        //first save checkpoint
        prepareTimerCheckPoint();
        timerFlushService.shutdown();
        timerLog.shutdown();
        timerCheckpoint.shutdown();

        enqueueQueue.clear(); //avoid blocking
        dequeuePutQueue.clear(); //avoid blocking

        enqueueGetService.shutdown();
        enqueuePutService.shutdown();
        dequeueWarmService.shutdown();
        dequeueGetService.shutdown();
        for (int i = 0; i < dequeueGetMessageServices.length; i++) {
            dequeueGetMessageServices[i].shutdown();
        }
        for (int i = 0; i < dequeuePutMessaageServices.length; i++) {
            dequeuePutMessaageServices[i].shutdown();
        }
        timerWheel.shutdown(false);

    }

    private void maybeMoveWriteTime() {
        if (currWriteTimeMs < (System.currentTimeMillis() / 1000) * 1000) {
            currWriteTimeMs = (System.currentTimeMillis() / 1000) * 1000;
        }
    }

    private void moveReadTime() {
        currReadTimeMs = currReadTimeMs + 1000;
        commitReadTimeMs = currReadTimeMs;
    }

    private boolean isRunning() {
        return RUNNING == state;
    }

    private void checkBrokerRole() {
        BrokerRole currRole = storeConfig.getBrokerRole();
        if (lastBrokerRole != currRole) {
            synchronized (lastBrokerRole) {
                log.info("Broker role change from {} to {}", lastBrokerRole, currRole);
                //if change to master, do something
                if (BrokerRole.SLAVE != currRole) {
                    currQueueOffset = Math.min(currQueueOffset, timerCheckpoint.getMasterTimerQueueOffset());
                    commitQueueOffset = currQueueOffset;
                    prepareTimerCheckPoint();
                    timerCheckpoint.flush();
                    currReadTimeMs = timerCheckpoint.getLastReadTimeMs();
                    commitReadTimeMs = currReadTimeMs;
                }
                //if change to slave, just let it go
                lastBrokerRole = currRole;
            }
        }
    }

    private boolean isRunningEnqueue() {
        checkBrokerRole();
        if (!isMaster() && currQueueOffset >= timerCheckpoint.getMasterTimerQueueOffset()) {
            return false;
        }
        return isRunning();
    }

    private boolean isRunningDequeue() {
        if (!isMaster()) {
            currReadTimeMs = timerCheckpoint.getLastReadTimeMs();
            commitReadTimeMs = currReadTimeMs;
            return false;
        }
        return isRunning();
    }

    public void addMetric(MessageExt msg, int value) {
        try {
            if (null == msg || null == msg.getProperty(MessageConst.PROPERTY_REAL_TOPIC)) {
                return;
            }
            timerMetrics.addAndGet(msg.getProperty(MessageConst.PROPERTY_REAL_TOPIC), value);
        } catch (Throwable t) {
            if (frequency.incrementAndGet() % 1000 == 0) {
                log.error("error in adding metric", t);
            }
        }

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
            for (; i < bufferCQ.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
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
                        TimerRequest timerRequest = new TimerRequest(offsetPy, sizePy, delayedTime, System.currentTimeMillis(), -1, msgExt);
                        while (true) {
                            if (enqueueQueue.offer(timerRequest, 3, TimeUnit.SECONDS)) {
                                break;
                            }
                            if (!isRunningEnqueue()) {
                                return false;
                            }
                        }
                    }
                } catch (Exception e) {
                    //here may cause the message loss
                    if (storeConfig.isTimerSkipUnknownError()) {
                        log.warn("Unknown error in skipped in enqueuing", e);
                    } else {
                        throw e;
                    }
                } finally {
                    perfs.endTick("enqueue_get");
                }
                //if broker role changes, ignore last enqueue
                if (!isRunningEnqueue()) {
                    return false;
                }
                currQueueOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
            }
            currQueueOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
            return i > 0;
        } catch (Exception e) {
            log.error("Unknown exception in enqueuing", e);
        } finally {
            bufferCQ.release();
        }
        return false;
    }

    public boolean doEnqueue(long offsetPy, int sizePy, long delayedTime, MessageExt messageExt) {
        log.debug("Do enqueue [{}] [{}]", new Timestamp(delayedTime), messageExt);
        int size = 4  //size
            + 8 //prev pos
            + 4 //magic value
            + 8 //curr write time, for trace
            + 4 //delayed time, for check
            + 8 //offsetPy
            + 4; //sizePy
        boolean needRoll = delayedTime - currWriteTimeMs >= timerRollWindowSec * 1000;
        int magic = MAGIC_DEFAULT;
        if (needRoll) {
            magic = magic | MAGIC_ROLL;
            delayedTime = currWriteTimeMs + timerRollWindowSec * 1000;
        }
        boolean isDelete = messageExt.getProperty(TIMER_DELETE_UNIQKEY) != null;
        if (isDelete) {
            magic = magic | MAGIC_DELETE;
        }

        Slot slot = timerWheel.getSlot(delayedTime / 1000);
        ByteBuffer tmpBuffer = timerLogBuffer;
        tmpBuffer.clear();
        tmpBuffer.putInt(size);
        tmpBuffer.putLong(slot.lastPos);
        tmpBuffer.putInt(magic);
        tmpBuffer.putLong(currWriteTimeMs);
        tmpBuffer.putInt((int) (delayedTime - currWriteTimeMs));
        tmpBuffer.putLong(offsetPy);
        tmpBuffer.putInt(sizePy);
        long ret = timerLog.append(tmpBuffer.array(), 0, TimerLog.UNIT_SIZE);
        if (-1 != ret) {
            timerWheel.putSlot(delayedTime / 1000, slot.firstPos == -1 ? ret : slot.firstPos, ret);
            addMetric(messageExt, 1);
        }
        return -1 != ret;
    }

    public int warmDequeue() {
        if (!isRunningDequeue())
            return -1;
        if (!storeConfig.isTimerWarmEnable())
            return -1;
        if (preReadTimeMs <= currReadTimeMs) {
            preReadTimeMs = currReadTimeMs + 1000;
        }
        if (preReadTimeMs >= currWriteTimeMs) {
            return -1;
        }
        if (preReadTimeMs >= currReadTimeMs + 3000) {
            return -1;
        }
        Slot slot = timerWheel.getSlot(preReadTimeMs / 1000);
        if (-1 == slot.timeSecs) {
            preReadTimeMs = preReadTimeMs + 1000;
            return 0;
        }
        long currOffsetPy = slot.lastPos;
        LinkedList<SelectMappedBufferResult> sbrs = new LinkedList<>();
        SelectMappedBufferResult timeSbr = null;
        SelectMappedBufferResult msgSbr = null;
        try {
            //read the msg one by one
            while (currOffsetPy != -1) {
                if (!isRunning())
                    break;
                perfs.startTick("warm_dequeue");
                if (null == timeSbr || timeSbr.getStartOffset() > currOffsetPy) {
                    timeSbr = timerLog.getWholeBuffer(currOffsetPy);
                    if (null != timeSbr)
                        sbrs.add(timeSbr);
                }
                if (null == timeSbr)
                    break;
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
                        if (null != msgSbr)
                            sbrs.add(msgSbr);
                    }
                    if (null != msgSbr) {
                        ByteBuffer bf = msgSbr.getByteBuffer();
                        int firstPos = (int) (offsetPy % commitLogFileSize);
                        for (int pos = firstPos; pos < firstPos + sizePy; pos += 4096) {
                            bf.position(pos);
                            bf.get();
                        }
                    }
                } catch (Exception e) {
                    log.error("Unexpected error in warm", e);
                } finally {
                    currOffsetPy = prevPos;
                    perfs.endTick("warm_dequeue");
                }
            }
            for (SelectMappedBufferResult sbr : sbrs) {
                if (null != sbr)
                    sbr.release();
            }
        } finally {
            preReadTimeMs = preReadTimeMs + 1000;
        }
        return 1;
    }

    public boolean checkStateForPutMessages(int state) {
        for (StateService service : dequeuePutMessaageServices) {
            if (!service.isState(state)) {
                return false;
            }
        }
        return true;
    }

    public boolean checkStateForGetMessages(int state) {
        for (StateService service : dequeueGetMessageServices) {
            if (!service.isState(state)) {
                return false;
            }
        }
        return true;
    }

    public void checkDequeueLatch(CountDownLatch latch, long delayedTime) throws Exception {
        if (latch.await(1, TimeUnit.SECONDS)) {
            return;
        }
        int checkNum = 0;
        while (true) {
            if (dequeueGetQueue.size() > 0 || dequeuePutQueue.size() > 0
                || !checkStateForGetMessages(StateService.WAITING)
                || !checkStateForPutMessages(StateService.WAITING)) {
                //let it go
            } else {
                checkNum++;
                if (checkNum >= 2) {
                    break;
                }
            }
            if (latch.await(1, TimeUnit.SECONDS)) {
                break;
            }
        }
        if (!latch.await(1, TimeUnit.SECONDS)) {
            log.warn("Check latch failed delayedTime:{}", delayedTime);
        }
    }

    public int dequeue() throws Exception {
        if (!isRunningDequeue())
            return -1;
        if (currReadTimeMs >= currWriteTimeMs) {
            return -1;
        }

        Slot slot = timerWheel.getSlot(currReadTimeMs / 1000);
        if (-1 == slot.timeSecs) {
            moveReadTime();
            return 0;
        }
        try {
            long currOffsetPy = slot.lastPos;
            Set<String> deleteUniqKeys = new ConcurrentSkipListSet<>();
            LinkedList<TimerRequest> normalMsgStack = new LinkedList<>();
            LinkedList<TimerRequest> deleteMsgStack = new LinkedList<>();
            LinkedList<SelectMappedBufferResult> sbrs = new LinkedList<>();
            SelectMappedBufferResult timeSbr = null;
            //read the timer log one by one
            while (currOffsetPy != -1) {
                perfs.startTick("dequeue_read_timerlog");
                if (null == timeSbr || timeSbr.getStartOffset() > currOffsetPy) {
                    timeSbr = timerLog.getWholeBuffer(currOffsetPy);
                    if (null != timeSbr)
                        sbrs.add(timeSbr);
                }
                if (null == timeSbr)
                    break;
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
                    TimerRequest timerRequest = new TimerRequest(offsetPy, sizePy, delayedTime, enqueueTime, magic);
                    timerRequest.setDeleteList(deleteUniqKeys);
                    if (needDelete(magic) && !needRoll(magic)) {
                        deleteMsgStack.add(timerRequest);
                    } else {
                        normalMsgStack.addFirst(timerRequest);
                    }
                } catch (Exception e) {
                    log.error("Error in dequeue_read_timerlog", e);
                } finally {
                    currOffsetPy = prevPos;
                    perfs.endTick("dequeue_read_timerlog");
                }
            }
            for (SelectMappedBufferResult sbr : sbrs) {
                if (null != sbr)
                    sbr.release();
            }
            if (!isRunningDequeue()) {
                return -1;
            }
            CountDownLatch deleteLatch = new CountDownLatch(deleteMsgStack.size());
            //read the delete msg: the msg used to mark another msg is deleted
            for (List<TimerRequest> deleteList: splitIntoLists(deleteMsgStack)) {
                for (TimerRequest tr: deleteList) tr.setLatch(deleteLatch);
                dequeueGetQueue.put(deleteList);
            }
            //do we need to use loop with tryAcquire
            checkDequeueLatch(deleteLatch, currReadTimeMs);

            CountDownLatch normalLatch = new CountDownLatch(normalMsgStack.size());
            //read the normal msg
            for (List<TimerRequest> normalList: splitIntoLists(normalMsgStack)) {
                for (TimerRequest tr: normalList) tr.setLatch(normalLatch);
                dequeueGetQueue.put(normalList);
            }
            checkDequeueLatch(normalLatch, currReadTimeMs);
            // if master -> slave -> master, then the read time move forward, and messages will be lossed
            if (!isRunningDequeue()) {
                return -1;
            }
            moveReadTime();
        } catch (Throwable t) {
            log.error("Unknown error in dequeue process", t);
            if (storeConfig.isTimerSkipUnknownError()) {
                moveReadTime();
            }
        }
        return 1;
    }

    private List<List<TimerRequest>> splitIntoLists(List<TimerRequest>  origin) {
        //this method assume that the origin is not null;
        List<List<TimerRequest>> lists = new LinkedList<>();
        if (origin.size() < 100) {
            lists.add(origin);
            return lists;
        }
        List<TimerRequest> currList = null;
        int fileIndexPy = -1;
        for (TimerRequest tr : origin) {
            if (fileIndexPy != tr.getOffsetPy() / commitLogFileSize) {
                if (null != currList && currList.size() > 0) {
                    lists.add(currList);
                }
                currList = new LinkedList<>();
                currList.add(tr);
                fileIndexPy = (int) (tr.getOffsetPy() / commitLogFileSize);
            } else {
                currList.add(tr);
            }
        }
        if (null != currList && currList.size() > 0) {
            lists.add(currList);
        }
        return lists;
    }

    private MessageExt getMessageByCommitOffset(long offsetPy, int sizePy) {
        bufferLocal.get().position(0);
        bufferLocal.get().limit(sizePy);
        boolean res = messageStore.getData(offsetPy, sizePy, bufferLocal.get());
        if (res) {
            bufferLocal.get().flip();
            return MessageDecoder.decode(bufferLocal.get(), true, false);
        }
        return null;
    }

    private MessageExtBrokerInner convert(MessageExt messageExt, long enqueueTime, boolean needRoll) {
        if (enqueueTime != -1) {
            MessageAccessor.putProperty(messageExt, TIMER_ENQUEUE_MS, enqueueTime + "");
        }
        if (needRoll) {
            if (messageExt.getProperty(TIMER_ROLL_TIMES) != null) {
                MessageAccessor.putProperty(messageExt, TIMER_ROLL_TIMES, Integer.parseInt(messageExt.getProperty(TIMER_ROLL_TIMES)) + 1 + "");
            } else {
                MessageAccessor.putProperty(messageExt, TIMER_ROLL_TIMES, 1 + "");
            }
        }
        MessageAccessor.putProperty(messageExt, TIMER_DEQUEUE_MS, System.currentTimeMillis() + "");
        MessageExtBrokerInner message = convertMessage(messageExt, needRoll);
        return message;
    }

    //0 succ; 1 fail, need retry; 2 fail, do not retry;
    private int doPut(MessageExtBrokerInner message) throws Exception {
        if (lastBrokerRole == BrokerRole.SLAVE) {
            log.warn("Trying do put timer msg in slave, [{}]", message);
            return PUT_NO_RETRY;
        }
        if (null != message.getProperty(MessageConst.PROPERTY_TIMER_DEL_UNIQKEY)) {
            log.warn("Trying do put delete timer msg [{}]", message);
            return PUT_NO_RETRY;
        }
        PutMessageResult putMessageResult = messageStore.putMessage(message);
        int retryNum = 0;
        while (retryNum < 3) {
            if (null == putMessageResult || null == putMessageResult.getPutMessageStatus()) {
                retryNum++;
            } else {
                switch (putMessageResult.getPutMessageStatus()) {
                    case PUT_OK:
                        return PUT_OK;
                    case SERVICE_NOT_AVAILABLE:
                        return PUT_NEED_RETRY;
                    case MESSAGE_ILLEGAL:
                    case PROPERTIES_SIZE_EXCEEDED:
                        return PUT_NO_RETRY;
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
            log.warn("Retrying to do put timer msg retryNum:{} putRes:{} msg:{}", retryNum, putMessageResult, message);
        }
        return PUT_NO_RETRY;
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

    class TimerEnqueueGetService extends ServiceThread {

        @Override public String getServiceName() {
            return this.getClass().getSimpleName();
        }

        @Override public void run() {
            TimerMessageStore.log.info(this.getServiceName() + " service start");
            while (!this.isStopped()) {
                try {
                    if (!TimerMessageStore.this.enqueue(0)) {
                        waitForRunning(50);
                    }
                } catch (Throwable e) {
                    TimerMessageStore.log.error("Error occurred in " + getServiceName(), e);
                }
            }
            TimerMessageStore.log.info(this.getServiceName() + " service end");
        }
    }

    class TimerEnqueuePutService extends ServiceThread {

        @Override public String getServiceName() {
            return this.getClass().getSimpleName();
        }

        @Override public void run() {
            TimerMessageStore.log.info(this.getServiceName() + " service start");
            while (!this.isStopped() || enqueueQueue.size() != 0) {
                try {
                    long tmpCommitQueueOffset = currQueueOffset;
                    TimerRequest req = enqueueQueue.poll(10, TimeUnit.MILLISECONDS);
                    boolean doRes =  false;
                    while (!isStopped() && !doRes) {
                        try {
                            if (null == req) {
                                commitQueueOffset = tmpCommitQueueOffset;
                                maybeMoveWriteTime();
                                doRes = true;
                            } else {
                                perfs.startTick("enqueue_put");
                                if (isMaster() && req.getDelayTime() < currWriteTimeMs) {
                                    MessageExtBrokerInner msg = convert(req.getMsg(), System.currentTimeMillis(), false);
                                    while (!doRes && !isStopped() && isMaster()) {
                                        doRes =  PUT_NEED_RETRY != doPut(msg);
                                        if (!doRes) {
                                            Thread.sleep(50);
                                        }
                                        maybeMoveWriteTime();
                                    }
                                }
                                //the broker role may have changed
                                if (!doRes) {
                                    doRes = doEnqueue(req.getOffsetPy(), req.getSizePy(), req.getDelayTime(), req.getMsg());
                                    if (!doRes) {
                                        Thread.sleep(50);
                                    }
                                    maybeMoveWriteTime();
                                }
                                if (!doRes && storeConfig.isTimerSkipUnknownError()) {
                                    break;
                                }
                                if (doRes) {
                                    if (enqueueQueue.size() == 0) {
                                        commitQueueOffset = tmpCommitQueueOffset;
                                    } else {
                                        commitQueueOffset = req.getMsg().getQueueOffset();
                                    }
                                }
                                perfs.endTick("enqueue_put");
                            }
                        } catch (Throwable t) {
                            log.error("Unknown error", t);
                            if (storeConfig.isTimerSkipUnknownError()) {
                                doRes = true;
                            }
                        }
                    }

                } catch (Throwable e) {
                    TimerMessageStore.log.error("Unknown error", e);
                }
            }
            TimerMessageStore.log.info(this.getServiceName() + " service end");
        }
    }

    class TimerDequeueGetService extends ServiceThread {

        @Override public String getServiceName() {
            return this.getClass().getSimpleName();
        }

        @Override public void run() {
            TimerMessageStore.log.info(this.getServiceName() + " service start");
            while (!this.isStopped()) {
                try {
                    if (-1 == TimerMessageStore.this.dequeue()) {
                        waitForRunning(50);
                    }
                } catch (Throwable e) {
                    TimerMessageStore.log.error("Error occurred in " + getServiceName(), e);
                }
            }
            TimerMessageStore.log.info(this.getServiceName() + " service end");
        }
    }

    abstract class StateService extends  ServiceThread {
        public static final int INITIAL = -1, START = 0, WAITING = 1, RUNNING = 2, END = 3;
        protected int state = INITIAL;
        protected void setState(int state) {
            this.state = state;
        }
        protected boolean isState(int state) {
            return this.state == state;
        }
    }

    class TimerDequeuePutMessageService extends StateService {

        @Override public String getServiceName() {
            return this.getClass().getSimpleName();
        }

        @Override public void run() {
            setState(StateService.START);
            TimerMessageStore.log.info(this.getServiceName() + " service start");
            while (!this.isStopped() || dequeuePutQueue.size() != 0) {
                try {
                    setState(StateService.WAITING);
                    TimerRequest tr = dequeuePutQueue.poll(10, TimeUnit.MILLISECONDS);
                    if (null == tr) {
                        continue;
                    }
                    setState(StateService.RUNNING);
                    boolean doRes = false;
                    try {
                        while (!isStopped() && !doRes && isRunningDequeue()) {
                            try {
                                perfs.startTick("dequeue_put");
                                addMetric(tr.getMsg(), -1);
                                MessageExtBrokerInner msg = convert(tr.getMsg(), tr.getEnqueueTime(), needRoll(tr.getMagic()));
                                doRes  = PUT_NEED_RETRY !=  doPut(msg);
                                while (!doRes && !isStopped() && isRunningDequeue()) {
                                    doRes = PUT_NO_RETRY != doPut(msg);
                                    Thread.sleep(50);
                                }
                                perfs.endTick("dequeue_put");
                            } catch (Throwable t) {
                                log.info("Unknown error", t);
                                if (storeConfig.isTimerSkipUnknownError()) {
                                    doRes = true;
                                }
                            }
                        }
                    } finally {
                        tr.idempotentRelease();
                    }

                } catch (Throwable e) {
                    TimerMessageStore.log.error("Error occurred in " + getServiceName(), e);
                }
            }
            TimerMessageStore.log.info(this.getServiceName() + " service end");
            setState(StateService.END);
        }
    }

    class TimerDequeueGetMessageService extends StateService {

        @Override public String getServiceName() {
            return this.getClass().getSimpleName();
        }

        @Override public void run() {
            setState(StateService.START);
            TimerMessageStore.log.info(this.getServiceName() + " service start");
            while (!this.isStopped()) {
                try {
                    setState(StateService.WAITING);
                    List<TimerRequest> trs = dequeueGetQueue.poll(50, TimeUnit.MILLISECONDS);
                    if (null == trs || trs.size() == 0) {
                        continue;
                    }
                    setState(StateService.RUNNING);
                    for (int i = 0; i < trs.size(); ) {
                        TimerRequest tr =  trs.get(i);
                        boolean doRes = false;
                        try {
                            long start = System.currentTimeMillis();
                            MessageExt msgExt = getMessageByCommitOffset(tr.getOffsetPy(), tr.getSizePy());
                            if (null != msgExt) {
                                if (needDelete(tr.getMagic()) && !needRoll(tr.getMagic())) {
                                    if (msgExt.getProperty(MessageConst.PROPERTY_TIMER_DEL_UNIQKEY) != null) {
                                        tr.getDeleteList().add(msgExt.getProperty(MessageConst.PROPERTY_TIMER_DEL_UNIQKEY));
                                    }
                                    tr.idempotentRelease();
                                    doRes = true;
                                } else {
                                    if (tr.getDeleteList().size() > 0 && tr.getDeleteList().contains(MessageClientIDSetter.getUniqID(msgExt))) {
                                        doRes = true;
                                        tr.idempotentRelease();
                                        perfs.getCounter("dequeue_delete").flow(1);
                                    } else {
                                        tr.setMsg(msgExt);
                                        while (!isStopped() && !doRes) {
                                            doRes = dequeuePutQueue.offer(tr, 3, TimeUnit.SECONDS);
                                        }
                                    }
                                }
                                perfs.getCounter("dequeue_get_msg").flow(System.currentTimeMillis() - start);
                            } else {
                                //the tr will never be processed afterwards, so idempotentRelease it
                                tr.idempotentRelease();
                                doRes = true;
                                perfs.getCounter("dequeue_get_msg_miss").flow(System.currentTimeMillis() - start);
                            }
                        } catch (Throwable e) {
                            log.error("Unknown exception", e);
                            if (storeConfig.isTimerSkipUnknownError()) {
                                tr.idempotentRelease();
                                doRes = true;
                            }
                        } finally {
                            if (doRes) {
                                i++;
                            }
                        }
                    }
                    trs.clear();
                } catch (Throwable e) {
                    TimerMessageStore.log.error("Error occurred in " + getServiceName(), e);
                }
            }
            TimerMessageStore.log.info(this.getServiceName() + " service end");
            setState(StateService.END);
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
                    if (!storeConfig.isTimerWarmEnable() || -1 == TimerMessageStore.this.warmDequeue()) {
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
                    timerLog.getMappedFileQueue().flush(0);
                    if (System.currentTimeMillis() - start > storeConfig.getTimerProgressLogIntervalMs()) {
                        start = System.currentTimeMillis();
                        ConsumeQueue cq = messageStore.getConsumeQueue(TIMER_TOPIC, 0);
                        long maxOffsetInQueue = cq == null ? 0 : cq.getMaxOffsetInQueue();
                        TimerMessageStore.log.info("[{}]Timer progress-time commitRead:[{}] currRead:[{}] preRead:[{}] currWrite:[{}] readBehind:{} enqSize:{} deqSize:{}",
                            storeConfig.getBrokerRole(), format(commitReadTimeMs), (currReadTimeMs - commitReadTimeMs) / 1000, (preReadTimeMs - currReadTimeMs) / 1000, format(currWriteTimeMs), (System.currentTimeMillis() - currReadTimeMs) / 1000, enqueueQueue.size(), dequeuePutQueue.size());
                        TimerMessageStore.log.info("[{}]Timer progress-offset commitOffset:{} currReadOffset:{} offsetBehind:{} behindMaster:{}",
                            storeConfig.getBrokerRole(), commitQueueOffset, currQueueOffset - commitQueueOffset, maxOffsetInQueue - currQueueOffset, timerCheckpoint.getMasterTimerQueueOffset() - currQueueOffset);
                    }
                    timerMetrics.persist();
                    waitForRunning(storeConfig.getTimerFlushIntervalMs());
                } catch (Throwable e) {
                    TimerMessageStore.log.error("Error occurred in " + getServiceName(), e);
                }
            }
            TimerMessageStore.log.info(this.getServiceName() + " service end");
        }
    }

    public void prepareTimerCheckPoint() {
        timerCheckpoint.setLastTimerLogFlushPos(timerLog.getMappedFileQueue().getFlushedWhere());
        if (isMaster()) {
            timerCheckpoint.setLastReadTimeMs(commitReadTimeMs);
            timerCheckpoint.setMasterTimerQueueOffset(commitQueueOffset);
        }
        timerCheckpoint.setLastTimerQueueOffset(Math.min(commitQueueOffset, timerCheckpoint.getMasterTimerQueueOffset()));
    }

    public boolean isMaster() {
        return BrokerRole.SLAVE != lastBrokerRole;
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



    public TimerMetrics getTimerMetrics() {
        return this.timerMetrics;
    }


}
