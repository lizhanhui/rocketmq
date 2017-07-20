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
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageArrivingListener;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TimerMessageFlowTest {
    private final String StoreMessage = "Once, there was a chance for me!";
    private int QUEUE_TOTAL = 100;
    private AtomicInteger QueueId = new AtomicInteger(0);
    private String baseDir;
    private MessageStoreConfig storeConfig;
    private MessageStore messageStore;
    private SocketAddress bornHost;
    private SocketAddress storeHost;
    private TimerMessageFlow timerMessageFlow;

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
        messageStore = new DefaultMessageStore(storeConfig, new BrokerStatsManager("TimerTest"), new MyMessageArrivingListener(), new BrokerConfig());
        boolean load = messageStore.load();
        assertTrue(load);
        timerMessageFlow = new TimerMessageFlow(messageStore, storeConfig);
    }

    @Test
    public void testEnqueueAndDequeue() throws Exception {
        long curr = (System.currentTimeMillis()/1000) * 1000;
        long delayMs = curr + 1000;
        MessageExtBrokerInner inner = buildMessage(delayMs);
        messageStore.putMessage(inner);
        assertNotNull(getOneMessage(TimerMessageFlow.TIMER_TOPIC, 0, 0, 1000));
        timerMessageFlow.updateOffset(0, 0);
        timerMessageFlow.updateCurrReadTimeMs(curr);
        assertTrue(timerMessageFlow.enqueue(0));
        int retry = 20;
        boolean dequeue = false;
        while (retry-- > 0) {
            dequeue =  timerMessageFlow.dequeue();
            if (dequeue) break;
            assertFalse(timerMessageFlow.enqueue(0));
            Thread.sleep(100);
        }
        assertTrue(dequeue);
        ByteBuffer msgBuff = getOneMessage("TimerTest", 0, 0, 1000);
        MessageExt msgExt = MessageDecoder.decode(msgBuff);
        assertNotNull(msgExt);
    }

    public ByteBuffer getOneMessage(String topic, int queue, long offset, int timeout) throws Exception {
        int retry = timeout/100;
        while (retry-- > 0) {
            GetMessageResult getMessageResult = messageStore.getMessage("TimerTest", topic, queue, offset, 1, null);
            if (GetMessageStatus.FOUND == getMessageResult.getStatus()) {
                return getMessageResult.getMessageBufferList().get(0);
            }
            Thread.sleep(100);
        }
        return null;
    }

    public MessageExtBrokerInner buildMessage(long delayedMs) {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(TimerMessageFlow.TIMER_TOPIC);
        msg.putUserProperty(MessageConst.PROPERTY_REAL_TOPIC, "TimerTest");
        msg.putUserProperty(MessageConst.PROPERTY_REAL_QUEUE_ID, "0");
        msg.setTags("timer");
        msg.setKeys("timer");
        msg.putUserProperty(TimerMessageFlow.TIMER_DELAY_KEY, delayedMs + "");
        msg.setBody(StoreMessage.getBytes());
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(0);
        msg.setSysFlag(4);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setBornHost(bornHost);
        msg.setStoreHost(storeHost);
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
