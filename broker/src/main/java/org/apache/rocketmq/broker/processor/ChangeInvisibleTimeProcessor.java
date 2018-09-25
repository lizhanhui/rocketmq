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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.common.protocol.header.ChangeInvisibleTimeResponseHeader;
import org.apache.rocketmq.common.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.pop.AckMsg;
import org.apache.rocketmq.store.pop.PopCheckPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

public class ChangeInvisibleTimeProcessor implements NettyRequestProcessor {
    private static final Logger POP_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    private final BrokerController brokerController;
    private String reviveTopic;

    public ChangeInvisibleTimeProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.reviveTopic = PopAckConstants.REVIVE_TOPIC + this.brokerController.getBrokerConfig().getBrokerClusterName();

    }

    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        return this.processRequest(ctx.channel(), request, true);
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    private RemotingCommand processRequest(final Channel channel, RemotingCommand request, boolean brokerAllowSuspend) throws RemotingCommandException {
        final ChangeInvisibleTimeRequestHeader requestHeader = (ChangeInvisibleTimeRequestHeader) request.decodeCommandCustomHeader(ChangeInvisibleTimeRequestHeader.class);
        RemotingCommand response = RemotingCommand.createResponseCommand(ChangeInvisibleTimeResponseHeader.class);
        response.setCode(ResponseCode.SUCCESS);
        final ChangeInvisibleTimeResponseHeader responseHeader = (ChangeInvisibleTimeResponseHeader) response.readCustomHeader();
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            POP_LOGGER.error("The topic {} not exist, consumer: {} ", requestHeader.getTopic(), RemotingHelper.parseChannelRemoteAddr(channel));
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark(String.format("topic[%s] not exist, apply first please! %s", requestHeader.getTopic(), FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL)));
            return response;
        }

        if (requestHeader.getQueueId() >= topicConfig.getReadQueueNums() || requestHeader.getQueueId() < 0) {
            String errorInfo = String.format("queueId[%d] is illegal, topic:[%s] topicConfig.readQueueNums:[%d] consumer:[%s]",
                requestHeader.getQueueId(), requestHeader.getTopic(), topicConfig.getReadQueueNums(), channel.remoteAddress());
            POP_LOGGER.warn(errorInfo);
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            response.setRemark(errorInfo);
            return response;
        }
        long minOffset = this.brokerController.getMessageStore().getMinOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId());
        long maxOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId());
        if (requestHeader.getOffset() < minOffset || requestHeader.getOffset() > maxOffset) {
            response.setCode(ResponseCode.NO_MESSAGE);
            return response;
        }
        // ack origin msg first
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        AckMsg ackMsg = new AckMsg();
        //String[] extraInfo = requestHeader.getExtraInfo().split(MessageConst.KEY_SEPARATOR);
        String[] extraInfo = ExtraInfoUtil.split(requestHeader.getExtraInfo());

        ackMsg.setAo(requestHeader.getOffset());
        //ackMsg.setSo(Long.valueOf(extraInfo[0]));
        ackMsg.setSo(ExtraInfoUtil.getCkQueueOffset(extraInfo));
        ackMsg.setC(requestHeader.getConsumerGroup());
        ackMsg.setT(requestHeader.getTopic());
        ackMsg.setQ(requestHeader.getQueueId());
        //ackMsg.setPt(Long.valueOf(extraInfo[1]));
        ackMsg.setPt(ExtraInfoUtil.getPopTime(extraInfo));
        msgInner.setTopic(reviveTopic);
        msgInner.setBody(JSON.toJSONString(ackMsg).getBytes(DataConverter.charset));
        //msgInner.setQueueId(Integer.valueOf(extraInfo[3]));
        msgInner.setQueueId(ExtraInfoUtil.getReviveQid(extraInfo));
        msgInner.setTags(PopAckConstants.ACK_TAG);
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(this.brokerController.getStoreHost());
        msgInner.setStoreHost(this.brokerController.getStoreHost());
        //msgInner.putUserProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS, String.valueOf(Long.valueOf(extraInfo[1]) + Long.valueOf(extraInfo[2])));
        msgInner.putUserProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS, String.valueOf(ExtraInfoUtil.getPopTime(extraInfo) + ExtraInfoUtil.getInvisibleTime(extraInfo)));
        msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, ackMsg.getT() + PopAckConstants.SPLIT + ackMsg.getQ() + PopAckConstants.SPLIT + ackMsg.getAo() + PopAckConstants.SPLIT + ackMsg.getC());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
        if (putMessageResult.getPutMessageStatus() != PutMessageStatus.PUT_OK
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_DISK_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_SLAVE_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.SLAVE_NOT_AVAILABLE) {
            POP_LOGGER.error("put ack msg error:" + putMessageResult);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            return response;
        }
        // add new ck
        long now = System.currentTimeMillis();
        //appendCheckPoint(channel, requestHeader, Integer.valueOf(extraInfo[3]), requestHeader.getQueueId(), requestHeader.getOffset(),now);
        appendCheckPoint(channel, requestHeader, ExtraInfoUtil.getReviveQid(extraInfo), requestHeader.getQueueId(), requestHeader.getOffset(), now);
        responseHeader.setInvisibleTime(requestHeader.getInvisibleTime());
        responseHeader.setPopTime(now);
        responseHeader.setReviveQid(ExtraInfoUtil.getReviveQid(extraInfo));
        return response;
    }

    private void appendCheckPoint(final Channel channel, final ChangeInvisibleTimeRequestHeader requestHeader, int reviveQid, int queueId, long offset, long popTime) {
        // add check point msg to revive log
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(reviveTopic);
        PopCheckPoint ck = new PopCheckPoint();
        ck.setBm(0);
        ck.setN((byte) 1);
        ck.setPt(popTime);
        ck.setIt(requestHeader.getInvisibleTime());
        ck.setSo(offset);
        ck.setC(requestHeader.getConsumerGroup());
        ck.setT(requestHeader.getTopic());
        ck.setQ((byte) queueId);
        msgInner.setBody(JSON.toJSONString(ck).getBytes(DataConverter.charset));
        msgInner.setQueueId(reviveQid);
        msgInner.setTags(PopAckConstants.CK_TAG);
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(this.brokerController.getStoreHost());
        msgInner.setStoreHost(this.brokerController.getStoreHost());
        msgInner.putUserProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS, String.valueOf(ck.getRt() - PopAckConstants.ackTimeInterval));
        msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, ck.getT() + PopAckConstants.SPLIT + ck.getQ() + PopAckConstants.SPLIT + ck.getSo() + PopAckConstants.SPLIT + ck.getC());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
        POP_LOGGER.info("change Invisible , appendCheckPoint, topic {}, queueId {},reviveId {}, cid {}, startOffset {}, result {}", requestHeader.getTopic(), queueId, reviveQid, requestHeader.getConsumerGroup(), offset,
            putMessageResult);
    }
}
