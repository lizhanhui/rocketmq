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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageArrivingListener;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TimerMessageStoreTest {
    private final byte[] msgBody = new byte[1024];
    private MessageStore messageStore;
    private SocketAddress bornHost;
    private SocketAddress storeHost;

    private Set<String> baseDirs = new HashSet<>();
    private List<TimerMessageStore> timerStores = new ArrayList<>();
    private AtomicInteger counter = new AtomicInteger(0);

    private MessageStoreConfig storeConfig;

    @Before
    public void init() throws Exception {
        String baseDir = StoreTestUtils.createBaseDir();
        baseDirs.add(baseDir);
        storeHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        bornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
        storeConfig = new MessageStoreConfig();
        storeConfig.setMapedFileSizeCommitLog(1024 * 100);
        storeConfig.setMappedFileSizeTimerLog(1024 * 10);
        storeConfig.setMapedFileSizeConsumeQueue(1024);
        storeConfig.setMaxHashSlotNum(100);
        storeConfig.setMaxIndexNum(100 * 10);
        storeConfig.setStorePathRootDir(baseDir);
        storeConfig.setStorePathCommitLog(baseDir + File.separator + "commitlog");
        storeConfig.setFlushDiskType(FlushDiskType.ASYNC_FLUSH);
        storeConfig.setTimerInterceptDelayLevel(true);
        messageStore = new DefaultMessageStore(storeConfig, new BrokerStatsManager("TimerTest"), new MyMessageArrivingListener(), new BrokerConfig());
        boolean load = messageStore.load();
        assertTrue(load);
        messageStore.start();
    }

    public TimerMessageStore createTimerMessageStore(String rootDir) throws IOException {
        if (null == rootDir) {
            rootDir = StoreTestUtils.createBaseDir();
        }
        TimerCheckpoint timerCheckpoint = new TimerCheckpoint(rootDir + File.separator + "config" + File.separator + "timercheck");
        TimerMetrics timerMetrics = new TimerMetrics(rootDir + File.separator + "config" + File.separator + "timermetrics");
        TimerMessageStore timerMessageStore = new TimerMessageStore(messageStore, storeConfig, timerCheckpoint, timerMetrics);
        messageStore.setTimerMessageStore(timerMessageStore);
        baseDirs.add(rootDir);
        timerStores.add(timerMessageStore);
        return timerMessageStore;
    }

    @Test
    public void testPutTimerMessage() throws Exception {
        String topic = "TimerTest_testPutTimerMessage";
        TimerMessageStore timerMessageStore = createTimerMessageStore(null);
        timerMessageStore.load();
        timerMessageStore.start();
        long curr = (System.currentTimeMillis() / 1000) * 1000;
        long delayMs = curr + 2000;
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 5; j++) {
                MessageExtBrokerInner inner = buildMessage((i % 2 == 0) ? 1000 : delayMs, topic + i, i % 2 == 0);
                PutMessageResult putMessageResult = messageStore.putMessage(inner);
                assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
            }
        }
        Thread.sleep(1000);
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(5, timerMessageStore.getTimerMetrics().getTimingCount(topic + i));
        }
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 5; j++) {
                ByteBuffer msgBuff = getOneMessage(topic + i, 0, j, 3000);
                assertNotNull(msgBuff);
                MessageExt msgExt = MessageDecoder.decode(msgBuff);
                assertNotNull(msgExt);
                assertEquals(topic + i, msgExt.getTopic());
                assertTrue(System.currentTimeMillis() - delayMs - 1000 < 200);
                assertEquals(topic + i, msgExt.getTopic());
            }
        }
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(0, timerMessageStore.getTimerMetrics().getTimingCount(topic + i));
        }
    }

    @Test
    public void testTimerFlowControl() throws Exception {
        String topic = "TimerTest_testTimerFlowControl";
        storeConfig.setTimerCongestNumEachSec(100);
        TimerMessageStore timerMessageStore = createTimerMessageStore(null);
        timerMessageStore.load();
        timerMessageStore.start();
        long curr = (System.currentTimeMillis() / 1000) * 1000;
        long delayMs = curr + 10000;
        int passFlowControlNum = 0;
        for (int i = 0; i < 500; i++) {
            long congestNum = timerMessageStore.getCongestNum(delayMs - 1000);
            assertTrue(congestNum <= 220);
            MessageExtBrokerInner inner = buildMessage(delayMs, topic, false);
            PutMessageResult putMessageResult = messageStore.putMessage(inner);
            if (congestNum < 100) {
                assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
            } else  {
                if (PutMessageStatus.PUT_OK == putMessageResult.getPutMessageStatus()) {
                    passFlowControlNum++;
                }
            }
            //wait reput
            Thread.sleep(5);
        }
        assertTrue(passFlowControlNum > 80);
        assertTrue(passFlowControlNum < 120);
    }


    @Test
    public void testPutExpiredTimerMessage() throws Exception {
        String topic = "TimerTest_testPutExpiredTimerMessage";
        TimerMessageStore timerMessageStore = createTimerMessageStore(null);
        timerMessageStore.load();
        timerMessageStore.start();
        long delayMs = System.currentTimeMillis() - 2000;
        for (int i = 0; i < 10; i++) {
            MessageExtBrokerInner inner = buildMessage(delayMs, topic, false);
            PutMessageResult putMessageResult = messageStore.putMessage(inner);
            assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
        }
        long curr = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            ByteBuffer msgBuff = getOneMessage(topic, 0, i, 1000);
            assertNotNull(msgBuff);
            assertTrue(System.currentTimeMillis() - curr < 200);
        }
    }

    @Test
    public void testDeleteTimerMessage() throws Exception {
        String topic = "TimerTest_testDeleteTimerMessage";
        TimerMessageStore timerMessageStore = createTimerMessageStore(null);
        timerMessageStore.load();
        timerMessageStore.start();
        long curr = (System.currentTimeMillis() / 1000) * 1000;
        long delayMs = curr + 1000;
        String uniqKey = null;
        for (int i = 0; i < 5; i++) {
            MessageExtBrokerInner inner = buildMessage(delayMs, topic, false);
            if (null == uniqKey) {
                uniqKey = MessageClientIDSetter.getUniqID(inner);
            }
            assertEquals(PutMessageStatus.PUT_OK, messageStore.putMessage(inner).getPutMessageStatus());
        }
        MessageExtBrokerInner delMsg = buildMessage(delayMs, topic, false);
        MessageAccessor.putProperty(delMsg, TimerMessageStore.TIMER_DELETE_UNIQKEY, uniqKey);
        delMsg.setPropertiesString(MessageDecoder.messageProperties2String(delMsg.getProperties()));
        assertEquals(PutMessageStatus.PUT_OK, messageStore.putMessage(delMsg).getPutMessageStatus());
        //the first one should have been deleted
        ByteBuffer msgBuff = getOneMessage(topic, 0, 0, 3000);
        assertNotNull(msgBuff);
        MessageExt msgExt = MessageDecoder.decode(msgBuff);
        assertNotNull(msgExt);
        assertNotEquals(uniqKey, MessageClientIDSetter.getUniqID(msgExt));
        //the last one should be null
        assertNull(getOneMessage(topic, 0, 4, 500));
    }

    @Test
    public void testPutDeleteTimerMessage() throws Exception {
        String topic = "TimerTest_testPutDeleteTimerMessage";
        TimerMessageStore timerMessageStore = createTimerMessageStore(null);
        timerMessageStore.load();
        timerMessageStore.start();
        long curr = (System.currentTimeMillis() / 1000) * 1000;
        long delayMs = curr + 1000;
        for (int i = 0; i < 5; i++) {
            MessageExtBrokerInner inner = buildMessage(delayMs, topic, false);
            assertEquals(PutMessageStatus.PUT_OK, messageStore.putMessage(inner).getPutMessageStatus());
        }
        MessageExtBrokerInner delMsg = buildMessage(delayMs, topic, false);
        MessageAccessor.putProperty(delMsg, TimerMessageStore.TIMER_DELETE_UNIQKEY, "XXX");
        delMsg.setPropertiesString(MessageDecoder.messageProperties2String(delMsg.getProperties()));
        assertEquals(PutMessageStatus.PUT_OK, messageStore.putMessage(delMsg).getPutMessageStatus());
        Thread.sleep(1000);
        for (int i = 0; i < 5; i++) {
            ByteBuffer msgBuff = getOneMessage(topic, 0, i, 1000);
            assertNotNull(msgBuff);
            assertTrue(System.currentTimeMillis() - delayMs < 1000);
        }
        assertNull(getOneMessage(topic, 0, 5, 1000));
    }

    @Test
    public void testStateAndRecover() throws Exception {
        String topic = "TimerTest_testStateAndRecover";
        String base = StoreTestUtils.createBaseDir();
        TimerMessageStore first = createTimerMessageStore(base);
        first.load();
        first.start();
        int msgNum = 500;
        long curr = (System.currentTimeMillis() / 1000) * 1000;
        long delayMs = curr + 3000;
        for (int i = 0; i < msgNum; i++) {
            MessageExtBrokerInner inner = buildMessage((i % 2 == 0) ? 3000 : delayMs, topic, i % 2 == 0);
            PutMessageResult putMessageResult = messageStore.putMessage(inner);
            assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
        }
        Thread.sleep(2000);
        assertEquals(2, first.getTimerLog().getMappedFileQueue().getMappedFiles().size());
        assertEquals(msgNum, first.getQueueOffset());
        assertEquals(first.getCommitQueueOffset(), first.getCommitQueueOffset());
        assertEquals(first.getCurrReadTimeMs(), first.getCommitReadTimeMs());
        curr = (System.currentTimeMillis() / 1000) * 1000;
        assertTrue(first.getCurrReadTimeMs() == curr || first.getCurrReadTimeMs() == curr + 1000);
        first.shutdown();
        TimerMessageStore second = createTimerMessageStore(base);
        assertTrue(second.load());
        assertEquals(msgNum, second.getQueueOffset());
        assertEquals(second.getCommitQueueOffset(), second.getQueueOffset());
        assertEquals(second.getCurrReadTimeMs(), second.getCommitReadTimeMs());
        assertEquals(first.getCommitReadTimeMs(), second.getCommitReadTimeMs());
        second.start();
        for (int i = 0; i < msgNum; i++) {
            ByteBuffer msgBuff = getOneMessage(topic, 0, i, 2000);
            assertNotNull(msgBuff);
        }
        second.shutdown();
    }

    @Test
    public void testMaxDelaySec() throws Exception {
        String topic = "TimerTest_testMaxDelaySec";
        TimerMessageStore first = createTimerMessageStore(null);
        first.load();
        first.start();
        long curr = (System.currentTimeMillis() / 1000) * 1000;
        long delaySec = storeConfig.getTimerMaxDelaySec() + 2;
        MessageExtBrokerInner absolute = buildMessage(curr + delaySec  * 1000, topic, false);
        assertEquals(PutMessageStatus.MESSAGE_ILLEGAL, messageStore.putMessage(absolute).getPutMessageStatus());
        MessageExtBrokerInner relative = buildMessage(delaySec  * 1000, topic, true);
        assertEquals(PutMessageStatus.MESSAGE_ILLEGAL, messageStore.putMessage(relative).getPutMessageStatus());
    }

    @Test
    public void testDisableTimer() throws Exception {
        storeConfig.setTimerWheelEnable(false);
        String topic = "TimerTest_testDisableTimer";
        TimerMessageStore first = createTimerMessageStore(null);
        first.load();
        first.start();
        long start = System.currentTimeMillis();
        long delaySec = storeConfig.getTimerMaxDelaySec() + 2;
        MessageExtBrokerInner relative = buildMessage(delaySec  * 1000, topic, true);
        assertEquals(PutMessageStatus.PUT_OK, messageStore.putMessage(relative).getPutMessageStatus());
        ByteBuffer msgBuff = getOneMessage(topic, 0, 0, 1000);
        assertNotNull(msgBuff);
        assertTrue(System.currentTimeMillis() - start < 1000);
        storeConfig.setTimerWheelEnable(true);
    }

    @Test
    public void testRollMessage() throws Exception {
        storeConfig.setTimerRollWindowSec(2);
        String topic = "TimerTest_testRollMessage";
        TimerMessageStore timerMessageStore = createTimerMessageStore(null);
        timerMessageStore.load();
        timerMessageStore.start();
        long curr = (System.currentTimeMillis() / 1000) * 1000;
        long delayMs = curr + 4000;
        MessageExtBrokerInner inner = buildMessage(delayMs, topic, false);
        assertEquals(PutMessageStatus.PUT_OK, messageStore.putMessage(inner).getPutMessageStatus());
        //the first one should have been deleted
        ByteBuffer msgBuff = getOneMessage(topic, 0, 0, 5000);
        assertNotNull(msgBuff);
        MessageExt msgExt = MessageDecoder.decode(msgBuff);
        assertNotNull(msgExt);
        assertEquals(1, Integer.valueOf(msgExt.getProperty(MessageConst.PROPERTY_TIMER_ROLL_TIMES)).intValue());
        storeConfig.setTimerRollWindowSec(Integer.MAX_VALUE);
    }

    @Test
    public void testInterceptDelayLevel() throws Exception {
        String topic = "TimerTest_testInterceptDelayLevel";
        TimerMessageStore timerMessageStore = createTimerMessageStore(null);
        timerMessageStore.load();
        timerMessageStore.start();
        long curr = (System.currentTimeMillis() / 1000) * 1000;
        MessageExtBrokerInner inner = buildMessage(0, topic , false);
        MessageAccessor.clearProperty(inner, MessageConst.PROPERTY_TIMER_DELIVER_MS);
        inner.setDelayTimeLevel(1);
        PutMessageResult putMessageResult = messageStore.putMessage(inner);
        assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
        Thread.sleep(500);
        Assert.assertEquals(1, timerMessageStore.getTimerMetrics().getTimingCount(topic));
        ByteBuffer msgBuff = getOneMessage(topic, 0, 0, 3000);
        assertNotNull(msgBuff);
        MessageExt msgExt = MessageDecoder.decode(msgBuff);
        assertNotNull(msgExt);
        assertEquals(topic, msgExt.getTopic());
        assertEquals(1, msgExt.getDelayTimeLevel());
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(0, timerMessageStore.getTimerMetrics().getTimingCount(topic + i));
        }
        Assert.assertEquals(0, timerMessageStore.getTimerMetrics().getTimingCount(topic));
    }

    public ByteBuffer getOneMessage(String topic, int queue, long offset, int timeout) throws Exception {
        int retry = timeout / 100;
        while (retry-- > 0) {
            GetMessageResult getMessageResult = messageStore.getMessage("TimerGroup", topic, queue, offset, 1, null);
            if (null != getMessageResult && GetMessageStatus.FOUND == getMessageResult.getStatus()) {
                return getMessageResult.getMessageBufferList().get(0);
            }
            Thread.sleep(100);
        }
        return null;
    }

    public MessageExtBrokerInner buildMessage(long delayedMs, String topic, boolean relative) {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(topic);
        msg.setQueueId(0);
        msg.setTags(counter.incrementAndGet() + "");
        msg.setKeys("timer");
        if (relative) {
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_TIMER_DELAY_SEC, delayedMs / 1000 + "");
        } else {
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_TIMER_DELIVER_MS, delayedMs + "");
        }
        msg.setBody(msgBody);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setBornHost(bornHost);
        msg.setStoreHost(storeHost);
        MessageClientIDSetter.setUniqID(msg);
        TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msg.getSysFlag());
        long tagsCodeValue =
            MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msg.getTags());
        msg.setTagsCode(tagsCodeValue);
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));
        return msg;
    }

    private class MyMessageArrivingListener implements MessageArrivingListener {
        @Override
        public void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime,
            byte[] filterBitMap, Map<String, String> properties) {
        }
    }

    @After
    public void clear() {
        for (TimerMessageStore store : timerStores) {
            store.shutdown();
        }
        for (String baseDir : baseDirs) {
            StoreTestUtils.deleteFile(baseDir);
        }
        if (null != messageStore) {
            messageStore.shutdown();
            messageStore.destroy();
        }
    }
}
