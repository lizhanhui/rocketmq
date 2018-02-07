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
package org.apache.rocketmq.example.openmessaging;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.MessagingAccessPointFactory;
import io.openmessaging.OMS;
import io.openmessaging.OMSBuiltinKeys;
import io.openmessaging.producer.Producer;
import io.openmessaging.producer.SendResult;
import java.util.Arrays;

public class SimpleProducer {
    public static void main(String[] args) {
        String accessPointUrl = "oms:rocketmq://localhost:9876/default:default";
        KeyValue properties = OMS.newKeyValue();
        properties.put(OMSBuiltinKeys.DRIVER_IMPL, "io.openmessaging.rocketmq.MessagingAccessPointImpl");
        final MessagingAccessPoint messagingAccessPoint = MessagingAccessPointFactory
            .getMessagingAccessPoint(accessPointUrl, properties);

        final Producer producer = messagingAccessPoint.createProducer();

        messagingAccessPoint.startup();
        System.out.printf("MessagingAccessPoint startup OK%n");

        producer.startup();
        System.out.printf("Producer startup OK%n");

        byte[] data = new byte[1024];
        Arrays.fill(data, (byte) 'x');
        Message message = producer.createTopicBytesMessage("TopicTest", data);
        SendResult sendResult = producer.send(message);
        // log sendResult
        producer.shutdown();
        messagingAccessPoint.shutdown();
    }
}
