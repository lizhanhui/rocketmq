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
package org.apache.rocketmq.client.impl.consumer;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultMQPushConsumerImplTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultMQPushConsumerImplTest.class);

    /**
     * Unit test logging both parameter and exception backtrace.
     */
    @Test
    public void testLog() {
        LOGGER.error("Log with parameter 1: {}, parameter 2: {} and exception backtrace", "abc", 2, new RuntimeException("Some text"));
    }

}