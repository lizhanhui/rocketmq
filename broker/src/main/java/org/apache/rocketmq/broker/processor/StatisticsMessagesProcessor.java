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
package org.apache.rocketmq.broker.processor;

import io.netty.channel.*;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.consumer.StatisticsMessagesResult;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.admin.OffsetWrapper;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.StatisticsMessagesRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatisticsMessagesProcessor implements NettyRequestProcessor {
    private static final Logger POP_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    private final BrokerController brokerController;

    public StatisticsMessagesProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }
    
    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        return processRequest(ctx.channel(), request, true);
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    private RemotingCommand processRequest(final Channel channel, RemotingCommand request, boolean brokerAllowSuspend) throws RemotingCommandException {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setOpaque(request.getOpaque());

        final StatisticsMessagesRequestHeader requestHeader =
            (StatisticsMessagesRequestHeader) request.decodeCommandCustomHeader(StatisticsMessagesRequestHeader.class);

        if (POP_LOGGER.isDebugEnabled()) {
            POP_LOGGER.debug("receive StatisticsMessages request command, {}", request);
        }

//        if (!PermName.isReadable(this.brokerController.getBrokerConfig().getBrokerPermission())) {
//            response.setCode(ResponseCode.NO_PERMISSION);
//            response.setRemark(String.format("the broker[%s] peeking message is forbidden", this.brokerController.getBrokerConfig().getBrokerIP1()));
//            return response;
//        }
        String topicName = requestHeader.getTopic();
        String consumerGroup = requestHeader.getConsumerGroup();
        long delayMessages = ((DefaultMessageStore) this.brokerController.getMessageStore()).getTimingMessageCount(topicName);

        long activeMessages = 0;
        String remark = "";
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topicName);
        if (null == topicConfig) {
            remark = "consumeStats, topic config not exist, " + topicName;
            POP_LOGGER.warn(remark);
            response.setRemark(remark);
            return response;
        }
        SubscriptionData findSubscriptionData = this.brokerController.getConsumerManager().findSubscriptionData(consumerGroup, topicName);
        if (null == findSubscriptionData && this.brokerController.getConsumerManager().findSubscriptionDataCount(consumerGroup) > 0) {
            remark = "consumeStats, the consumer group[" + consumerGroup + "], topic[" + topicName + "] not exist";
            response.setRemark(remark);
            return response;
        }

        for (int i = 0; i < topicConfig.getReadQueueNums(); i++) {
            long brokerOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topicName, i);
            if (brokerOffset < 0) {
                brokerOffset = 0;
            }
            long consumerOffset = this.brokerController.getConsumerOffsetManager().queryOffset(consumerGroup, topicName, i);
            if (consumerOffset < 0) {
                consumerOffset = 0;
            }
            activeMessages += brokerOffset - consumerOffset;
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        response.setBody(new StatisticsMessagesResult(activeMessages, delayMessages).encode());
        return response;
    }
}
