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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.message.MessageAccessor;
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
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TimerMessageStoreTest {
    private final String StoreMessage = "Once, there was a chance for me!";
    private int QUEUE_TOTAL = 100;
    private AtomicInteger QueueId = new AtomicInteger(0);
    private String baseDir;
    private MessageStoreConfig storeConfig;
    private MessageStore messageStore;
    private SocketAddress bornHost;
    private SocketAddress storeHost;
    private TimerMessageStore timerMessageStore;

    @Before
    public void init() throws Exception {
        baseDir = StoreTestUtils.createBaseDir();
        storeHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        bornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
        storeConfig = new MessageStoreConfig();
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
        timerMessageStore = new TimerMessageStore(messageStore, storeConfig);
        timerMessageStore.setState(TimerMessageStore.RUNNING);
    }

    @Test
    public void testEnqueueAndDequeue() throws Exception {
        long curr = (System.currentTimeMillis()/1000) * 1000;
        long delayMs = curr + 1000;
        MessageExtBrokerInner inner = buildMessage(delayMs);
        System.out.println(inner);
        PutMessageResult putMessageResult = messageStore.putMessage(inner);
        System.out.println(putMessageResult);
        assertNotNull(getOneMessage(TimerMessageStore.TIMER_TOPIC, 0, 0, 3000));
        timerMessageStore.updateOffset(0, 0);
        timerMessageStore.updateCurrReadTimeMs(curr);
        assertTrue(timerMessageStore.enqueue(0));
        int retry = 20;
        int dequeue = 0;
        while (retry-- > 0) {
            dequeue =  timerMessageStore.dequeue();
            if (1 == dequeue) break;
            assertFalse(timerMessageStore.enqueue(0));
            Thread.sleep(100);
        }
        assertTrue(1 == dequeue);
        ByteBuffer msgBuff = getOneMessage("TimerTest", 0, 0, 1000);
        MessageExt msgExt = MessageDecoder.decode(msgBuff);
        assertNotNull(msgExt);
        long delayTime = Long.valueOf(msgExt.getProperty(TimerMessageStore.TIMER_DELAY_MS));
        long dequeueTime = Long.valueOf(msgExt.getProperty(TimerMessageStore.TIMER_ENQUEUE_MS));
        assertTrue(dequeueTime - delayTime - 1 <= 300);
    }

    public ByteBuffer getOneMessage(String topic, int queue, long offset, int timeout) throws Exception {
        int retry = timeout/100;
        while (retry-- > 0) {
            GetMessageResult getMessageResult = messageStore.getMessage("TimerTest", topic, queue, offset, 1, null);
            if (null != getMessageResult && GetMessageStatus.FOUND == getMessageResult.getStatus()) {
                return getMessageResult.getMessageBufferList().get(0);
            }
            Thread.sleep(100);
        }
        return null;
    }

    public MessageExtBrokerInner buildMessage(long delayedMs) {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(TimerMessageStore.TIMER_TOPIC);
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_TOPIC, "TimerTest");
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_QUEUE_ID, "0");
        msg.setTags("timer");
        msg.setKeys("timer");
        MessageAccessor.putProperty(msg, TimerMessageStore.TIMER_DELAY_MS, delayedMs + "");
        msg.setBody(StoreMessage.getBytes());
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setBornHost(bornHost);
        msg.setStoreHost(storeHost);
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
        if (null != messageStore) {
            messageStore.shutdown();
            messageStore.destroy();
        }
        if (null != baseDir) {
            StoreTestUtils.deleteFile(new File(baseDir));
        }
    }
}
