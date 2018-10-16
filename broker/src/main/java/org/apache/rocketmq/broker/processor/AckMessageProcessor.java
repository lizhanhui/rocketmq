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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.pop.AckMsg;
import org.apache.rocketmq.store.pop.PopCheckPoint;

import com.alibaba.fastjson.JSON;

public class AckMessageProcessor implements NettyRequestProcessor {
    private static final InternalLogger POP_LOGGER = InternalLoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    private final BrokerController brokerController;
    private String reviveTopic;
    private PopReviveService[] popReviveServices;
    private boolean isMaster = false;

    public AckMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.reviveTopic = PopAckConstants.REVIVE_TOPIC + this.brokerController.getBrokerConfig().getBrokerClusterName();
        this.popReviveServices = new PopReviveService[this.brokerController.getBrokerConfig().getReviveQueueNum()];
        for (int i = 0; i < this.brokerController.getBrokerConfig().getReviveQueueNum(); i++) {
            this.popReviveServices[i] = new PopReviveService(i);
        }
    }

    public void startPopReviveService() {
        for (PopReviveService popReviveService : popReviveServices) {
            popReviveService.start();
        }
    }

    public void shutdownPopReviveService() {
        for (PopReviveService popReviveService : popReviveServices) {
            popReviveService.stop();
        }
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
        final AckMessageRequestHeader requestHeader = (AckMessageRequestHeader) request.decodeCommandCustomHeader(AckMessageRequestHeader.class);
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        AckMsg ackMsg = new AckMsg();
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
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
        String[] extraInfo = ExtraInfoUtil.split(requestHeader.getExtraInfo());

        ackMsg.setAo(requestHeader.getOffset());
        ackMsg.setSo(ExtraInfoUtil.getCkQueueOffset(extraInfo));
        ackMsg.setC(requestHeader.getConsumerGroup());
        ackMsg.setT(requestHeader.getTopic());
        ackMsg.setQ(requestHeader.getQueueId());
        ackMsg.setPt(ExtraInfoUtil.getPopTime(extraInfo));

        if (this.brokerController.getPopMessageProcessor().getPopAckBufferMergeService().addAk(ackMsg)) {
            return response;
        }

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
        }
        return response;
    }

    public class PopReviveService extends ServiceThread {
        private int queueId;

        public PopReviveService(int i) {
            this.queueId = i;
        }

        @Override
        public String getServiceName() {
            return "PopReviveService_" + this.queueId;
        }

        private boolean checkMaster() {
            return brokerController.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE;
        }

        private boolean checkAndSetMaster() {
            isMaster = checkMaster();
            return isMaster;
        }

        private void reviveRetry(PopCheckPoint popCheckPoint, MessageExt messageExt) throws Exception {
            if (!checkAndSetMaster()) {
                POP_LOGGER.info("slave skip retry , revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                return;
            }
            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            if (!popCheckPoint.getT().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                msgInner.setTopic(KeyBuilder.buildPopRetryTopic(popCheckPoint.getT(), popCheckPoint.getC()));
            } else {
                msgInner.setTopic(popCheckPoint.getT());
            }
            msgInner.setBody(messageExt.getBody());
            msgInner.setQueueId(0);
            if (messageExt.getTags() != null) {
                msgInner.setTags(messageExt.getTags());
            } else {
                MessageAccessor.setProperties(msgInner, new HashMap<String, String>());
            }
            msgInner.setBornTimestamp(messageExt.getBornTimestamp());
            msgInner.setBornHost(brokerController.getStoreHost());
            msgInner.setStoreHost(brokerController.getStoreHost());
            msgInner.setReconsumeTimes(messageExt.getReconsumeTimes() + 1);
            msgInner.getProperties().putAll(messageExt.getProperties());
            if (messageExt.getReconsumeTimes() == 0 || msgInner.getProperties().get(MessageConst.PROPERTY_FIRST_POP_TIME) == null) {
                msgInner.getProperties().put(MessageConst.PROPERTY_FIRST_POP_TIME, String.valueOf(popCheckPoint.getPt()));
            }
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
            addRetryTopicIfNoExit(msgInner.getTopic());
            PutMessageResult putMessageResult = brokerController.getMessageStore().putMessage(msgInner);
            POP_LOGGER.info("reviveQueueId={},retry msg , ck={}, msg queueId {}, offset {}, reviveDelay={}, result is {} ",
                queueId, popCheckPoint, messageExt.getQueueId(), messageExt.getQueueOffset(),
                (System.currentTimeMillis() - popCheckPoint.getRt()) / 1000, putMessageResult);
            if (putMessageResult.getAppendMessageResult() == null || putMessageResult.getAppendMessageResult().getStatus() != AppendMessageStatus.PUT_OK) {
                throw new Exception("reviveQueueId=" + queueId + ",revive error ,msg is :" + msgInner);
            }
        }

        private boolean addRetryTopicIfNoExit(String topic) {
            TopicConfig topicConfig = brokerController.getTopicConfigManager().selectTopicConfig(topic);
            if (topicConfig != null) {
                return true;
            }
            topicConfig = new TopicConfig(topic);
            topicConfig.setReadQueueNums(PopAckConstants.retryQueueNum);
            topicConfig.setWriteQueueNums(PopAckConstants.retryQueueNum);
            topicConfig.setTopicFilterType(TopicFilterType.SINGLE_TAG);
            topicConfig.setPerm(6);
            topicConfig.setTopicSysFlag(0);
            brokerController.getTopicConfigManager().updateTopicConfig(topicConfig);
            return true;
        }

        private List<MessageExt> getReviveMessage(long offset, int queueId) {
            PullResult pullResult = getMessage(PopAckConstants.REVIVE_GROUP, reviveTopic, queueId, offset, 32);
            if (reachTail(pullResult, offset)) {
                POP_LOGGER.info("reviveQueueId={}, reach tail,offset {}", queueId, offset);
            } else if (pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL || pullResult.getPullStatus() == PullStatus.NO_MATCHED_MSG) {
                POP_LOGGER.error("reviveQueueId={}, OFFSET_ILLEGAL {}, result is {}", queueId, offset, pullResult);
                if (!checkAndSetMaster()) {
                    POP_LOGGER.info("slave skip offset correct topic={}, reviveQueueId={}", reviveTopic, queueId);
                    return null;
                }
                brokerController.getConsumerOffsetManager().commitOffset(PopAckConstants.LOCAL_HOST, PopAckConstants.REVIVE_GROUP, reviveTopic, queueId, pullResult.getNextBeginOffset() - 1);
            }
            return pullResult.getMsgFoundList();
        }

        private boolean reachTail(PullResult pullResult, long offset) {
            return pullResult.getPullStatus() == PullStatus.NO_NEW_MSG
                || (pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL && offset == pullResult.getMaxOffset());
        }

        private MessageExt getBizMessage(String topic, long offset, int queueId) {
            final GetMessageResult getMessageTmpResult = brokerController.getMessageStore().getMessage(PopAckConstants.REVIVE_GROUP, topic, queueId, offset, 1, null);
            List<MessageExt> list = decodeMsgList(getMessageTmpResult);
            if (list.size() == 0) {
                POP_LOGGER.warn("can not get msg , topic {}, offset {}, queueId {}, result is {}", topic, offset, queueId, getMessageTmpResult);
                return null;
            } else {
                return list.get(0);
            }
        }

        public PullResult getMessage(String group, String topic, int queueId, long offset, int nums) {
            GetMessageResult getMessageResult = brokerController.getMessageStore().getMessage(group, topic, queueId, offset, nums, null);

            if (getMessageResult != null) {
                PullStatus pullStatus = PullStatus.NO_NEW_MSG;
                List<MessageExt> foundList = null;
                switch (getMessageResult.getStatus()) {
                    case FOUND:
                        pullStatus = PullStatus.FOUND;
                        foundList = decodeMsgList(getMessageResult);
                        brokerController.getBrokerStatsManager().incGroupGetNums(group, topic, getMessageResult.getMessageCount());
                        brokerController.getBrokerStatsManager().incGroupGetSize(group, topic, getMessageResult.getBufferTotalSize());
                        brokerController.getBrokerStatsManager().incBrokerGetNums(getMessageResult.getMessageCount());
                        brokerController.getBrokerStatsManager().recordDiskFallBehindTime(group, topic, queueId,
                            brokerController.getMessageStore().now() - foundList.get(foundList.size() - 1).getStoreTimestamp());
                        break;
                    case NO_MATCHED_MESSAGE:
                        pullStatus = PullStatus.NO_MATCHED_MSG;
                        POP_LOGGER.warn("no matched message. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}",
                            getMessageResult.getStatus(), topic, group, offset);
                        break;
                    case NO_MESSAGE_IN_QUEUE:
                        pullStatus = PullStatus.NO_NEW_MSG;
                        POP_LOGGER.warn("no new message. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}",
                            getMessageResult.getStatus(), topic, group, offset);
                        break;
                    case MESSAGE_WAS_REMOVING:
                    case NO_MATCHED_LOGIC_QUEUE:
                    case OFFSET_FOUND_NULL:
                    case OFFSET_OVERFLOW_BADLY:
                    case OFFSET_OVERFLOW_ONE:
                    case OFFSET_TOO_SMALL:
                        pullStatus = PullStatus.OFFSET_ILLEGAL;
                        POP_LOGGER.warn("offset illegal. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}",
                            getMessageResult.getStatus(), topic, group, offset);
                        break;
                    default:
                        assert false;
                        break;
                }

                return new PullResult(pullStatus, getMessageResult.getNextBeginOffset(), getMessageResult.getMinOffset(),
                    getMessageResult.getMaxOffset(), foundList);

            } else {
                POP_LOGGER.error("get message from store return null. topic={}, groupId={}, requestOffset={}", topic, group, offset);
                return null;
            }
        }

        private List<MessageExt> decodeMsgList(GetMessageResult getMessageResult) {
            List<MessageExt> foundList = new ArrayList<>();
            try {
                List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
                if (messageBufferList != null) {
                    for (ByteBuffer bb : messageBufferList) {
                        if (bb == null) {
                            POP_LOGGER.error("bb is null {}", getMessageResult);
                            continue;
                        }
                        MessageExt msgExt = MessageDecoder.decode(bb);
                        if (msgExt == null) {
                            POP_LOGGER.error("decode msgExt is null {}", getMessageResult);
                            continue;
                        }
                        foundList.add(msgExt);
                    }
                }
            } finally {
                getMessageResult.release();
            }

            return foundList;
        }

        @Override
        public void run() {
            int slow = 1;
            while (!this.isStopped()) {
                try {
                    this.waitForRunning(brokerController.getBrokerConfig().getReviveInterval());
                    if (!checkAndSetMaster()) {
                        POP_LOGGER.info("slave skip start revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                        continue;
                    }
                    POP_LOGGER.info("start revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                    HashMap<String, PopCheckPoint> map = new HashMap<>();
                    long startScanTime = System.currentTimeMillis();
                    long startTime = 0;
                    long endTime = 0;
                    long oldOffset = brokerController.getConsumerOffsetManager().queryOffset(PopAckConstants.REVIVE_GROUP, reviveTopic, queueId);
                    POP_LOGGER.info("reviveQueueId={}, old offset is {} ", queueId, oldOffset);
                    long offset = oldOffset + 1;
                    // offset self amend
                    while (true) {
                        if (!checkAndSetMaster()) {
                            POP_LOGGER.info("slave skip scan , revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                            break;
                        }
                        long timerDelay = brokerController.getMessageStore().getTimerMessageStore().getReadBehind();
                        long commitLogDelay = brokerController.getMessageStore().getTimerMessageStore().getEnqueueBehind();
                        List<MessageExt> messageExts = getReviveMessage(offset, queueId);
                        if (messageExts == null || messageExts.isEmpty()) {
                            long old = endTime;
                            if (endTime != 0 && ((System.currentTimeMillis() - endTime) > (3 * PopAckConstants.SECOND) && timerDelay == 0 && commitLogDelay == 0)) {
                                endTime = System.currentTimeMillis();
                            }
                            if (timerDelay > 5 || commitLogDelay > 5) {
                                POP_LOGGER.warn("timer is delay,timerDelay={}, commit log is delay,commitLogDelay={}", timerDelay, commitLogDelay);
                            }
                            POP_LOGGER.info("reviveQueueId={}, offset is {}, can not get new msg, old endTime {}, new endTime {}, timerDelay={}, commitLogDelay={} ", queueId,
                                offset, old, endTime, timerDelay, commitLogDelay);
                            break;
                        }
                        if (System.currentTimeMillis() - startScanTime > brokerController.getBrokerConfig().getReviveScanTime()) {
                            POP_LOGGER.info("reviveQueueId={}, scan timeout  ", queueId);
                            break;
                        }
                        for (MessageExt messageExt : messageExts) {
                            long deliverTime = Long.valueOf(messageExt.getUserProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS));
                            if (PopAckConstants.CK_TAG.equals(messageExt.getTags())) {
                                String raw = new String(messageExt.getBody(), DataConverter.charset);
                                POP_LOGGER.info("reviveQueueId={},find ck, offset:{}, raw : {}", messageExt.getQueueId(), messageExt.getQueueOffset(), raw);
                                PopCheckPoint point = JSON.parseObject(raw, PopCheckPoint.class);
                                if (point.getT() == null || point.getC() == null) {
                                    continue;
                                }
                                map.put(point.getT() + point.getC() + point.getQ() + point.getSo() + point.getPt(), point);
                                if (startTime == 0) {
                                    startTime = deliverTime;
                                }
                                point.setRo(messageExt.getQueueOffset());
                            } else if (PopAckConstants.ACK_TAG.equals(messageExt.getTags())) {
                                String raw = new String(messageExt.getBody(), DataConverter.charset);
                                POP_LOGGER.info("reviveQueueId={},find ack, offset:{}, raw : {}", messageExt.getQueueId(), messageExt.getQueueOffset(), raw);
                                AckMsg ackMsg = JSON.parseObject(raw, AckMsg.class);
                                PopCheckPoint point = map.get(ackMsg.getT() + ackMsg.getC() + ackMsg.getQ() + ackMsg.getSo() + ackMsg.getPt());
                                if (point != null) {
                                    int indexOfAck = point.indexOfAck(ackMsg.getAo());
                                    if (indexOfAck > -1) {
                                        point.setBm(DataConverter.setBit(point.getBm(), indexOfAck, true));
                                    } else {
                                        POP_LOGGER.error("invalid ack index, {}, {}", ackMsg, point);
                                    }
                                }
                            }
                            if (deliverTime > endTime) {
                                endTime = deliverTime;
                            }
                        }
                        offset = offset + messageExts.size();
                    }
                    if (!checkAndSetMaster()) {
                        POP_LOGGER.info("slave skip scan , revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                        continue;
                    }
                    ArrayList<PopCheckPoint> sortList = new ArrayList<>(map.values());
                    Collections.sort(sortList, new Comparator<PopCheckPoint>() {
                        @Override
                        public int compare(PopCheckPoint o1, PopCheckPoint o2) {
                            return (int) (o1.getRo() - o2.getRo());
                        }
                    });
                    POP_LOGGER.info("reviveQueueId={},ck listSize={}", queueId, sortList.size());
                    if (sortList.size() != 0) {
                        POP_LOGGER.info("reviveQueueId={}, 1st ck, startOffset={}, reviveOffset={} ; last ck, startOffset={}, reviveOffset={}", queueId, sortList.get(0).getSo(),
                            sortList.get(0).getRo(), sortList.get(sortList.size() - 1).getSo(), sortList.get(sortList.size() - 1).getRo());
                    }
                    long newOffset = oldOffset;
                    for (PopCheckPoint popCheckPoint : sortList) {
                        if (!checkAndSetMaster()) {
                            POP_LOGGER.info("slave skip ck process , revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                            break;
                        }
                        if (endTime - popCheckPoint.getRt() > (PopAckConstants.ackTimeInterval + PopAckConstants.SECOND)) {
                            // check normal topic, skip ck , if normal topic is not exist
                            String normalTopic = KeyBuilder.parseNormalTopic(popCheckPoint.getT(), popCheckPoint.getC());
                            if (brokerController.getTopicConfigManager().selectTopicConfig(normalTopic) == null) {
                                POP_LOGGER.warn("reviveQueueId={},can not get normal topic {} , then continue ", queueId, popCheckPoint.getT());
                                newOffset = popCheckPoint.getRo();
                                continue;
                            }
                            SubscriptionGroupConfig subscriptionGroupConfig = brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(popCheckPoint.getC());
                            if (null == subscriptionGroupConfig) {
                                POP_LOGGER.warn("reviveQueueId={},can not get cid {} , then continue ", queueId, popCheckPoint.getC());
                                newOffset = popCheckPoint.getRo();
                                continue;
                            }
                            for (int j = 0; j < popCheckPoint.getN(); j++) {
                                if (!DataConverter.getBit(popCheckPoint.getBm(), j)) {
                                    // retry msg
                                    MessageExt messageExt;
                                    if (popCheckPoint.getD() == null || popCheckPoint.getD().isEmpty()) {
                                        messageExt = getBizMessage(popCheckPoint.getT(), popCheckPoint.getSo() + j, popCheckPoint.getQ());
                                    } else {
                                        messageExt = getBizMessage(popCheckPoint.getT(), popCheckPoint.getSo() + popCheckPoint.getD().get(j), popCheckPoint.getQ());
                                    }
                                    if (messageExt == null) {
                                        POP_LOGGER.warn("reviveQueueId={},can not get biz msg topic is {}, offset is {} , then continue ", queueId, popCheckPoint.getT(),
                                            popCheckPoint.getSo() + j);
                                        continue;
                                    }
                                    //skip ck from last epoch
                                    if (popCheckPoint.getPt() < messageExt.getStoreTimestamp()) {
                                        POP_LOGGER.warn("reviveQueueId={},skip ck from last epoch {}", queueId, popCheckPoint);
                                        continue;
                                    }
                                    reviveRetry(popCheckPoint, messageExt);
                                }
                            }
                        } else {
                            break;
                        }
                        newOffset = popCheckPoint.getRo();
                    }
                    long delay = 0;
                    if (sortList.size() > 0) {
                        delay = (System.currentTimeMillis() - sortList.get(0).getRt()) / 1000;
                        slow = 1;
                    }
                    POP_LOGGER.info("reviveQueueId={},revive finish,old offset is {}, new offset is {}, ckDelay={}  ", queueId, oldOffset, newOffset, delay);
                    if (newOffset > oldOffset) {
                        if (!checkAndSetMaster()) {
                            POP_LOGGER.info("slave skip commit, revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                            continue;
                        }
                        brokerController.getConsumerOffsetManager().commitOffset(PopAckConstants.LOCAL_HOST, PopAckConstants.REVIVE_GROUP, reviveTopic, queueId, newOffset);
                    }
                    if (sortList.size() == 0) {
                        POP_LOGGER.info("reviveQueueId={},has no new msg ,take a rest {}", queueId, slow);
                        Thread.sleep(slow * brokerController.getBrokerConfig().getReviveInterval());
                        if (slow < brokerController.getBrokerConfig().getReviveMaxSlow()) {
                            slow++;
                        }
                    }

                } catch (Exception e) {
                    POP_LOGGER.error("reviveQueueId=" + queueId + ",revive error", e);
                }
            }
        }
    }

}
