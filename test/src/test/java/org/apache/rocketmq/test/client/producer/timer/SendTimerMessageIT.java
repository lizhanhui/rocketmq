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
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.factory.ProducerFactory;
import org.apache.rocketmq.test.util.RandomUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SendTimerMessageIT extends BaseConf {
    private static Logger logger = Logger.getLogger(SendTimerMessageIT.class);
    private String topic = null;
    private Random random = new Random();

    @Before
    public void setUp() {
        topic = initTopic();
        logger.info(String.format("user topic[%s]!", topic));
    }

    @After
    public void tearDown() {
        super.shutDown();
    }

    @Test
    public void testSendTimerMessage() throws Exception {
        Message message =  new Message(topic, RandomUtils.getStringByUUID().getBytes());
        long curr = (System.currentTimeMillis()/ 1000) * 1000;
        MessageAccessor.putProperty(message, MessageConst.PROPERTY_TIMER_DELIVER_MS, curr + 1000 + "");

        DefaultMQProducer producer = ProducerFactory.getRMQProducer(nsAddr);
        SendResult sendResult = producer.send(message);
        assertEquals(SendStatus.SEND_OK, sendResult.getSendStatus());
        int retry = 50;
        MessageExt messageExt = null;
        while (retry-- > 0) {
            try {
                messageExt = producer.getDefaultMQProducerImpl().queryMessageByUniqKey(topic, sendResult.getMsgId());
            } catch (Exception ignored) {

            }
            if (null == messageExt) {
                Thread.sleep(100);
            } else {
                break;
            }
        }
        assertNotNull(messageExt);
    }
}
