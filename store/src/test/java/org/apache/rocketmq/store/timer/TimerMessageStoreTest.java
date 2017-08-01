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
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TimerMessageStoreTest {
    private final String StoreMessage = "Once, there was a chance for me!";
    private MessageStore messageStore;
    private SocketAddress bornHost;
    private SocketAddress storeHost;

    private Set<String> baseDirs = new HashSet<>();
    private List<TimerMessageStore> timerStores = new ArrayList<>();
    private AtomicInteger counter = new AtomicInteger(0);


    @Before
    public void init() throws Exception {
        String baseDir = StoreTestUtils.createBaseDir();
        baseDirs.add(baseDir);
        storeHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        bornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setMapedFileSizeCommitLog(1024 * 8);
        storeConfig.setMapedFileSizeConsumeQueue(1024 * 4);
        storeConfig.setMaxHashSlotNum(100);
        storeConfig.setMaxIndexNum(100 * 10);
        storeConfig.setStorePathRootDir(baseDir);
        storeConfig.setStorePathCommitLog(baseDir + File.separator + "commitlog");
        storeConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        messageStore = new DefaultMessageStore(storeConfig, new BrokerStatsManager("TimerTest"), new MyMessageArrivingListener(), new BrokerConfig());
        boolean load = messageStore.load();
        assertTrue(load);
        messageStore.start();
    }
    public TimerMessageStore createTimerMessageStore(String rootDir) throws IOException {
        if (null == rootDir) {
            rootDir = StoreTestUtils.createBaseDir();
        }
        TimerMessageStore timerMessageStore = new TimerMessageStore(messageStore, rootDir, 8 * 1024);
        baseDirs.add(rootDir);
        timerStores.add(timerMessageStore);
        return timerMessageStore;
    }

    @Test
    public void testEnqueueAndDequeue() throws Exception {
        String topic = "TimerTest01";
        TimerMessageStore timerMessageStore  = createTimerMessageStore(null);
        timerMessageStore.setState(TimerMessageStore.RUNNING);
        long curr = (System.currentTimeMillis()/1000) * 1000;
        long delayMs = curr + 1000;
        MessageExtBrokerInner inner = buildMessage(delayMs, topic);
        PutMessageResult putMessageResult = messageStore.putMessage(inner);
        assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
        assertNotNull(getOneMessage(TimerMessageStore.TIMER_TOPIC, 0, 0, 1000));
        timerMessageStore.updateOffset(0, 0);
        timerMessageStore.updateCurrReadTimeMs(curr);
        assertTrue(timerMessageStore.enqueue(0));
        int retry = 20;
        int dequeue = 0;
        while (retry-- > 0) {
            dequeue =  timerMessageStore.dequeue();
            if (1 == dequeue) break;
            assertFalse(timerMessageStore.enqueue(0));
            if (-1 == dequeue) {
                Thread.sleep(100);
            }
        }
        assertTrue(1 == dequeue);
        ByteBuffer msgBuff = getOneMessage(topic, 0, 0, 1000);
        assertNotNull(msgBuff);
        MessageExt msgExt = MessageDecoder.decode(msgBuff);
        assertNotNull(msgExt);
        long delayTime = Long.valueOf(msgExt.getProperty(TimerMessageStore.TIMER_DELAY_MS));
        long dequeueTime = Long.valueOf(msgExt.getProperty(TimerMessageStore.TIMER_ENQUEUE_MS));
        assertTrue(dequeueTime - delayTime - 1 <= 300);
    }


    @Test
    public void testPutTimerMessage() throws Exception {
        String topic = "TimerTest02";
        TimerMessageStore timerMessageStore  = createTimerMessageStore(null);
        timerMessageStore.load();
        timerMessageStore.start();
        long curr = (System.currentTimeMillis()/1000) * 1000;
        long delayMs = curr + 1000;
        for (int i = 0; i < 10; i++) {
            MessageExtBrokerInner inner = buildMessage(delayMs, topic);
            PutMessageResult putMessageResult = messageStore.putMessage(inner);
            assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
        }
        int index = 0;
        for (int i = 0; i < 10; i++) {
            ByteBuffer msgBuff = getOneMessage(topic, 0, i, 3000);
            assertNotNull(msgBuff);
            MessageExt msgExt = MessageDecoder.decode(msgBuff);
            assertNotNull(msgExt);
            int tagIndex = Integer.valueOf(msgExt.getTags());
            assertTrue(tagIndex > index);
            index = tagIndex;
        }
    }

    @Test
    public void testPutExpiredTimerMessage() throws Exception {
        String topic = "TimerTest03";
        TimerMessageStore timerMessageStore  = createTimerMessageStore(null);
        timerMessageStore.load();
        timerMessageStore.start();
        long delayMs = System.currentTimeMillis() - 2000;
        for (int i = 0; i < 10; i++) {
            MessageExtBrokerInner inner = buildMessage(delayMs, topic);
            PutMessageResult putMessageResult = messageStore.putMessage(inner);
            assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
        }
        for (int i = 0; i < 10; i++) {
            ByteBuffer msgBuff = getOneMessage(topic, 0, i, 1000);
            assertNotNull(msgBuff);
        }
    }


    @Test
    public void testDeleteTimerMessage() throws Exception {
        String topic = "TimerTest04";
        TimerMessageStore timerMessageStore  = createTimerMessageStore(null);
        timerMessageStore.load();
        timerMessageStore.start();
        long curr = (System.currentTimeMillis()/1000) * 1000;
        long delayMs = curr + 1000;
        String uniqKey = null;
        for (int i = 0; i < 5; i++) {
            MessageExtBrokerInner inner = buildMessage(delayMs, topic);
            if (null == uniqKey) {
                uniqKey = MessageClientIDSetter.getUniqID(inner);
            }
            assertEquals(PutMessageStatus.PUT_OK, messageStore.putMessage(inner).getPutMessageStatus());
        }
        MessageExtBrokerInner delMsg = buildMessage(delayMs, topic);
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
    public void testRecover() throws Exception {
        String topic = "TimerTest03";
        String base = StoreTestUtils.createBaseDir();
        TimerMessageStore first = createTimerMessageStore(base);
        first.load();
        first.start();
        long curr = (System.currentTimeMillis()/1000) * 1000;
        long delayMs = curr + 1000;
        for (int i = 0; i < 10; i++) {
            MessageExtBrokerInner inner = buildMessage(delayMs, topic);
            PutMessageResult putMessageResult = messageStore.putMessage(inner);
            assertEquals(PutMessageStatus.PUT_OK, putMessageResult.getPutMessageStatus());
        }
        Thread.sleep(1000);
        assertEquals(10, first.getQueueOffset(0));
        assertTrue(first.getCurrReadTimeMs() >= System.currentTimeMillis() - 1000);
        first.shutdown();
        TimerMessageStore second = createTimerMessageStore(base);
        second.load();
        second.recover();
        assertEquals(10, second.getQueueOffset(0));
        assertTrue(second.getCurrReadTimeMs() >= System.currentTimeMillis() - 3000);
        second.start();
        for (int i = 0; i < 10; i++) {
            ByteBuffer msgBuff = getOneMessage(topic, 0, i, 2000);
            assertNotNull(msgBuff);
        }
        second.shutdown();
    }



    public ByteBuffer getOneMessage(String topic, int queue, long offset, int timeout) throws Exception {
        int retry = timeout/100;
        while (retry-- > 0) {
            GetMessageResult getMessageResult = messageStore.getMessage("TimerGroup", topic, queue, offset, 1, null);
            if (null != getMessageResult && GetMessageStatus.FOUND == getMessageResult.getStatus()) {
                return getMessageResult.getMessageBufferList().get(0);
            }
            Thread.sleep(100);
        }
        return null;
    }

    public MessageExtBrokerInner buildMessage(long delayedMs, String topic) {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(TimerMessageStore.TIMER_TOPIC);
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_TOPIC, topic);
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_QUEUE_ID, "0");
        msg.setTags(counter.incrementAndGet() + "");
        msg.setKeys("timer");
        MessageAccessor.putProperty(msg, TimerMessageStore.TIMER_DELAY_MS, delayedMs + "");
        msg.setBody(StoreMessage.getBytes());
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
        for (TimerMessageStore store: timerStores) {
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
