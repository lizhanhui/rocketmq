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
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.PopAckConstants;
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
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.pop.AckMsg;
import org.apache.rocketmq.store.pop.PopCheckPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

public class AckMessageProcessor implements NettyRequestProcessor {
	private static final Logger POP_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
	private final BrokerController brokerController;
	private String reviveTopic;

	public AckMessageProcessor(final BrokerController brokerController) {
		this.brokerController = brokerController;
		this.reviveTopic = PopAckConstants.REVIVE_TOPIC + this.brokerController.getBrokerConfig().getBrokerClusterName();
		Thread thread = new Thread(new Runnable() {
			@Override
			public void run() {
				while (true) {
					try {
						Thread.sleep(5000L);
						TopicConfig topicConfig = brokerController.getTopicConfigManager().selectTopicConfig(reviveTopic);
						POP_LOGGER.info("start revive topic:{}",reviveTopic);
						for (int i = 0; i < topicConfig.getReadQueueNums(); i++) {
							try {

								Thread.sleep(200L);
								HashMap<String, PopCheckPoint> map = new HashMap<>();
								long startScanTime = System.currentTimeMillis();
								long startTime = 0;
								long endTime = 0;
								long oldOffset = brokerController.getConsumerOffsetManager().queryOffset(PopAckConstants.REVIVE_GROUP, reviveTopic, i);
								POP_LOGGER.info("reviveQueueId={}, old offset is {} ",i,oldOffset);
								long offset = oldOffset + 1;
								while (true) {
									List<MessageExt> messageExts = getReviveMessage(offset, i);
									if (messageExts.isEmpty()) {
										if (endTime != 0 && (System.currentTimeMillis() - endTime > 10 * PopAckConstants.ackTimeInterval)) {
											endTime = System.currentTimeMillis();
										}
										POP_LOGGER.info("reviveQueueId={}, offset is {}, can not get new msg  ", i, offset);
										break;
									}
									if (System.currentTimeMillis() - startScanTime > PopAckConstants.scanTime) {
										POP_LOGGER.info("reviveQueueId={}, scan timeout  ",i);
										break;
									}
									for (MessageExt messageExt : messageExts) {
										long deliverTime = Long.valueOf(messageExt.getUserProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS));
										if (PopAckConstants.CK_TAG.equals(messageExt.getTags())) {
											String raw = new String(messageExt.getBody(), DataConverter.charset);
											POP_LOGGER.info("reviveQueueId={},find ck, offset:{}, raw : {}", messageExt.getQueueId(), messageExt.getQueueOffset(), raw);
											PopCheckPoint point = JSON.parseObject(raw, PopCheckPoint.class);
											if (point.getTopic() == null || point.getCid() == null) {
												continue;
											}
											map.put(point.getTopic() + point.getCid() + point.getQueueId() + point.getStartOffset() + point.getPopTime(), point);
											if (startTime == 0) {
												startTime = deliverTime;
											}
											point.setReviveOffset(messageExt.getQueueOffset());
										} else if (PopAckConstants.ACK_TAG.equals(messageExt.getTags())) {
											String raw = new String(messageExt.getBody(), DataConverter.charset);
											POP_LOGGER.info("reviveQueueId={},find ack, offset:{}, raw : {}", messageExt.getQueueId(), messageExt.getQueueOffset(), raw);
											AckMsg ackMsg = JSON.parseObject(raw, AckMsg.class);
											PopCheckPoint point = map.get(ackMsg.getTopic() + ackMsg.getConsumerGroup() + ackMsg.getQueueId() + ackMsg.getStartOffset() + ackMsg.getPopTime());
											if (point != null) {
												point.setBitMap(DataConverter.setBit(point.getBitMap(), (int) (ackMsg.getAckOffset() - ackMsg.getStartOffset()), true));
											}
										}
										if (deliverTime > endTime) {
											endTime = deliverTime;
										}
									}
									offset = offset + messageExts.size();
								}
								ArrayList<PopCheckPoint> sortList = new ArrayList<>(map.values());
								Collections.sort(sortList, new Comparator<PopCheckPoint>() {
									@Override
									public int compare(PopCheckPoint o1, PopCheckPoint o2) {
										return (int) (o1.getReviveOffset() - o2.getReviveOffset());
									}
								});
								POP_LOGGER.info("reviveQueueId={},ck list size={}", i, sortList.size());
								if (sortList.size()!=0) {
									POP_LOGGER.info("reviveQueueId={}, 1st ck, startOffset={}, reviveOffset={} ; last ck, startOffset={}, reviveOffset={}",i, sortList.get(0).getStartOffset(),
											sortList.get(0).getReviveOffset(), sortList.get(sortList.size() - 1).getStartOffset(), sortList.get(sortList.size() - 1).getReviveOffset());
								}
								long newOffset = oldOffset;
								for (PopCheckPoint popCheckPoint : sortList) {
									if (endTime - popCheckPoint.getReviveTime() > PopAckConstants.ackTimeInterval) {
										for (int j = 0; j < popCheckPoint.getNum(); j++) {
											if (!DataConverter.getBit(popCheckPoint.getBitMap(), j)) {
												// retry msg
												MessageExt messageExt = getBizMessage(popCheckPoint.getTopic(), popCheckPoint.getStartOffset() + j, popCheckPoint.getQueueId());
												MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
												if (!popCheckPoint.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
													msgInner.setTopic(KeyBuilder.buildPopRetryTopic(popCheckPoint.getTopic(), popCheckPoint.getCid()));
												} else {
													msgInner.setTopic(popCheckPoint.getTopic());
												}
												msgInner.setBody(messageExt.getBody());
												msgInner.setQueueId(0);
												if (messageExt.getTags() != null) {
													msgInner.setTags(messageExt.getTags());
												}else {
													MessageAccessor.setProperties(msgInner, new HashMap<String, String>());
												}
												msgInner.setBornTimestamp(messageExt.getBornTimestamp());
												msgInner.setBornHost(brokerController.getStoreHost());
												msgInner.setStoreHost(brokerController.getStoreHost());
												msgInner.setReconsumeTimes(messageExt.getReconsumeTimes() + 1);
												msgInner.getProperties().putAll(messageExt.getProperties());
												if (messageExt.getReconsumeTimes() == 0 || msgInner.getProperties().get(MessageConst.PROPERTY_FIRST_POP_TIME) == null) {
													msgInner.getProperties().put(MessageConst.PROPERTY_FIRST_POP_TIME, String.valueOf(popCheckPoint.getPopTime()));
												}
												msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
												addRetryTopicIfNoExit(msgInner.getTopic());
												PutMessageResult putMessageResult = brokerController.getMessageStore().putMessage(msgInner);
												POP_LOGGER.info("reviveQueueId={},retry msg , topic {}, cid {}, msg queueId {}, offset {}, result is {}",i,popCheckPoint.getTopic(),popCheckPoint.getCid(),messageExt.getQueueId(),messageExt.getQueueOffset(),putMessageResult);
												if (putMessageResult.getAppendMessageResult().getStatus()!=AppendMessageStatus.PUT_OK) {
													throw new Exception("reviveQueueId=" + i + "revive error ,msg is :"+msgInner);
												}
											}
										}
									} else {
										break;
									}
									newOffset = popCheckPoint.getReviveOffset();
								}
								POP_LOGGER.info("reviveQueueId={},revive finish,old offset is {}, new offset is {}  ", i, oldOffset, newOffset);
								if (newOffset > oldOffset) {
									brokerController.getConsumerOffsetManager().commitOffset(PopAckConstants.LOCAL_HOST, PopAckConstants.REVIVE_GROUP, reviveTopic, i, newOffset);
								}
							
							} catch (Exception e) {
								POP_LOGGER.error("reviveQueueId=" + i + ",revive error", e);
							}
						}

					} catch (Exception e) {
						POP_LOGGER.error("revive error", e);
					}
				}

			}

			private boolean addRetryTopicIfNoExit(String topic) {
				TopicConfig topicConfig = brokerController.getTopicConfigManager().selectTopicConfig(topic);
				if (topicConfig != null) {
					return true;
				}
				topicConfig=new TopicConfig(topic);
				topicConfig.setReadQueueNums(1);
				topicConfig.setWriteQueueNums(1);
				topicConfig.setTopicFilterType(TopicFilterType.SINGLE_TAG);
				topicConfig.setPerm(6);
				topicConfig.setTopicSysFlag(0);
				brokerController.getTopicConfigManager().updateTopicConfig(topicConfig);
				return true;
			}
			private List<MessageExt> getReviveMessage(long offset, int queueId) {
				final GetMessageResult getMessageTmpResult = brokerController.getMessageStore().getMessage(PopAckConstants.REVIVE_GROUP, reviveTopic, queueId, offset, 32, null);
				return decodeMsgList(getMessageTmpResult);
			}

			private MessageExt getBizMessage(String topic, long offset, int queueId) {
				final GetMessageResult getMessageTmpResult = brokerController.getMessageStore().getMessage(PopAckConstants.REVIVE_GROUP, topic, queueId, offset, 1, null);
				List<MessageExt> list = decodeMsgList(getMessageTmpResult);
				if (list.size() == 0) {
					return null;
				} else {
					return list.get(0);
				}
			}

			private List<MessageExt> decodeMsgList(GetMessageResult getMessageResult) {
				List<MessageExt> foundList = new ArrayList<>();
				try {
					List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
					for (ByteBuffer bb : messageBufferList) {
						MessageExt msgExt = MessageDecoder.decode(bb);
						foundList.add(msgExt);
					}

				} finally {
					getMessageResult.release();
				}

				return foundList;
			}
		});
		thread.setDaemon(true);
		thread.setName("reviveLoop");
		thread.start();
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
		RemotingCommand response=RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
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
		long minOffset=this.brokerController.getMessageStore().getMinOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId());
		long maxOffset=this.brokerController.getMessageStore().getMaxOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId());
		if (requestHeader.getOffset() < minOffset || requestHeader.getOffset() > maxOffset) {
            response.setCode(ResponseCode.NO_MESSAGE);
            return response;
		}
		String[] extraInfo = requestHeader.getExtraInfo().split(MessageConst.KEY_SEPARATOR);
		ackMsg.setAckOffset(requestHeader.getOffset());
		ackMsg.setStartOffset(Long.valueOf(extraInfo[0]));
		ackMsg.setConsumerGroup(requestHeader.getConsumerGroup());
		ackMsg.setTopic(requestHeader.getTopic());
		ackMsg.setQueueId(requestHeader.getQueueId());
		ackMsg.setPopTime(Long.valueOf(extraInfo[1]));
		msgInner.setTopic(reviveTopic);
		msgInner.setBody(JSON.toJSONString(ackMsg).getBytes(DataConverter.charset));
		msgInner.setQueueId(Integer.valueOf(extraInfo[3]));
		msgInner.setTags(PopAckConstants.ACK_TAG);
		msgInner.setBornTimestamp(System.currentTimeMillis());
		msgInner.setBornHost(this.brokerController.getStoreHost());
		msgInner.setStoreHost(this.brokerController.getStoreHost());
		msgInner.putUserProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS, String.valueOf(Long.valueOf(extraInfo[1]) + Long.valueOf(extraInfo[2])));
		PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
		if (putMessageResult.getPutMessageStatus() != PutMessageStatus.PUT_OK 
				&& putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_DISK_TIMEOUT
				&& putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_SLAVE_TIMEOUT 
				&& putMessageResult.getPutMessageStatus() != PutMessageStatus.SLAVE_NOT_AVAILABLE) {
			POP_LOGGER.warn("put ack msg error:" + putMessageResult);
		}
		return response;
	}

}
