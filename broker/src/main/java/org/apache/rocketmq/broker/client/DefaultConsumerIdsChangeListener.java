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
package org.apache.rocketmq.broker.client;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.netty.channel.Channel;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public class DefaultConsumerIdsChangeListener implements ConsumerIdsChangeListener {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerController brokerController;
    private final int cacheSize = 8096;

    private final ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
            "DefaultConsumerIdsScheduledThread"));

    private ConcurrentHashMap<String,List<Channel>> consumerChannelMap = new ConcurrentHashMap<>(cacheSize);

    public DefaultConsumerIdsChangeListener(BrokerController brokerController) {
        this.brokerController = brokerController;

        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    doConsumerChange();
                } catch (Exception e) {
                    logger.error("schedule doConsumerChange ", e);
                }
            }
        }, 30, 15, TimeUnit.SECONDS);
    }

    @Override
    public void handle(ConsumerGroupEvent event, String group, Object... args) {
        if (event == null) {
            return;
        }
        switch (event) {
            case CHANGE:
                if (args == null || args.length < 1) {
                    return;
                }
                List<Channel> channels = (List<Channel>) args[0];
                if (channels != null && brokerController.getBrokerConfig().isNotifyConsumerIdsChangedEnable()) {
                    if (this.brokerController.getBrokerConfig().isRealTimeNotifyConsumerChange()) {
                        for (Channel chl : channels) {
                            this.brokerController.getBroker2Client().notifyConsumerIdsChanged(chl, group);
                        }
                    } else {
                        consumerChannelMap.put(group, channels);
                    }
                }
                break;
            case UNREGISTER:
                this.brokerController.getConsumerFilterManager().unRegister(group);
                break;
            case REGISTER:
                if (args == null || args.length < 1) {
                    return;
                }
                Collection<SubscriptionData> subscriptionDataList = (Collection<SubscriptionData>) args[0];
                this.brokerController.getConsumerFilterManager().register(group, subscriptionDataList);
                break;
            default:
                throw new RuntimeException("Unknown event " + event);
        }
    }

    private void doConsumerChange() {

        if (consumerChannelMap.isEmpty()) {
            return;
        }

        ConcurrentHashMap<String, List<Channel>> processMap = new ConcurrentHashMap<>(consumerChannelMap);
        consumerChannelMap = new ConcurrentHashMap<>(cacheSize);

        for (Map.Entry<String, List<Channel>> entry : processMap.entrySet()) {
            String consumerId = entry.getKey();
            List<Channel> channelList = entry.getValue();
            try {
                if (channelList != null && brokerController.getBrokerConfig().isNotifyConsumerIdsChangedEnable()) {
                    for (Channel chl : channelList) {
                        this.brokerController.getBroker2Client().notifyConsumerIdsChanged(chl, consumerId);
                    }
                }
            } catch (Exception e) {
                logger.error(String.format("doConsumerChange %s ", consumerId), e);
            }
        }
    }
}
