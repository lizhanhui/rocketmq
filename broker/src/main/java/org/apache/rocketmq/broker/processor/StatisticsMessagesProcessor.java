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
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.StatisticsMessagesRequestHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
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
        if (POP_LOGGER.isDebugEnabled()) {
            POP_LOGGER.debug("receive StatisticsMessages request command, {}", request);
        }
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setOpaque(request.getOpaque());

        final StatisticsMessagesRequestHeader requestHeader =
            (StatisticsMessagesRequestHeader) request.decodeCommandCustomHeader(StatisticsMessagesRequestHeader.class);
        String topicName = requestHeader.getTopic();
        String consumerGroup = requestHeader.getConsumerGroup();

        String remark = "";
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topicName);
        if (null == topicConfig) {
            remark = "consumeStats, topic config not exist, " + topicName;
            POP_LOGGER.warn(remark);
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark(remark);
            return response;
        }
        long delayMessages = this.brokerController.getMessageStore().getTimingMessageCount(topicName);

        long activeMessages = 0;
        long totalMessages = 0;
        for (int i = 0; i < topicConfig.getReadQueueNums(); i++) {
            long maxOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topicName, i);
            if (maxOffset < 0) {
                maxOffset = 0;
            }
            long minOffset = this.brokerController.getMessageStore().getMinOffsetInQueue(topicName, i);
            if (minOffset < 0) {
                minOffset = 0;
            }
            long consumerOffset = this.brokerController.getConsumerOffsetManager().queryOffset(consumerGroup, topicName, i);
            if (consumerOffset < 0) {
                consumerOffset = minOffset;
            }
            activeMessages += maxOffset - consumerOffset;
            totalMessages += maxOffset - minOffset;
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        response.setBody(new StatisticsMessagesResult(activeMessages, delayMessages, totalMessages).encode());
        return response;
    }
}
