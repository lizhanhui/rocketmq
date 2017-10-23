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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.client.producer.timer;

import java.util.Random;
import org.apache.log4j.Logger;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.store.timer.TimerMessageStore;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.factory.ProducerFactory;
import org.apache.rocketmq.test.util.RandomUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SendTimerMessageIT extends BaseConf {
    private static Logger logger = Logger.getLogger(SendTimerMessageIT.class);
    private String topic = null;
    private DefaultMQProducer producer;
    private Random random = new Random();

    @Before
    public void setUp() {
        topic = initTopic();
        logger.info(String.format("user topic[%s]!", topic));
        producer = ProducerFactory.getRMQProducer(nsAddr);

    }

    @After
    public void tearDown() {
        super.shutDown();
    }

    @Test
    public void testSendTimerMessage() throws Exception {
        long curr = System.currentTimeMillis();
        long delayMs = 2000;
        Message absolute =  new Message(topic, RandomUtils.getStringByUUID().getBytes());
        MessageAccessor.putProperty(absolute, MessageConst.PROPERTY_TIMER_DELIVER_MS, curr + delayMs + "");
        SendResult sendResult = producer.send(absolute);
        assertEquals(SendStatus.SEND_OK, sendResult.getSendStatus());
        //query the message by system topic
        MessageExt sysRes = getMessageByUniqkey(TimerMessageStore.TIMER_TOPIC, sendResult.getMsgId(), 1000);
        assertNotNull(sysRes);
        Thread.sleep(1000);
        //query the message by real topic
        MessageExt abosulteRes = getMessageByUniqkey(topic, sendResult.getMsgId(), 4000);
        assertNotNull(abosulteRes);
        //check the sys message and the real message
        assertEquals(sysRes.getProperty("REAL_TOPIC"), abosulteRes.getTopic());
        long elapse = System.currentTimeMillis() - curr;
        assertTrue(elapse > 2000);
        assertTrue(elapse < 3000);
    }

    private MessageExt getMessageByUniqkey(String topic, String uniqkey, int maxWaitMs) throws Exception {
        long begin = System.currentTimeMillis();
        MessageExt messageExt = null;
        while (System.currentTimeMillis() < begin + maxWaitMs) {
            try {
                messageExt = producer.getDefaultMQProducerImpl().queryMessageByUniqKey(topic, uniqkey);
            } catch (Exception ignored) {

            }
            if (null == messageExt) {
                Thread.sleep(100);
            } else {
                break;
            }
        }
        return messageExt;
    }


}
