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
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.pop.AckMsg;
import org.apache.rocketmq.store.pop.PopCheckPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

public class AckMessageProcessor implements NettyRequestProcessor {
	private static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
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
						for (int i = 0; i < topicConfig.getReadQueueNums(); i++) {
							Thread.sleep(5000L);
							HashMap<String, PopCheckPoint> map = new HashMap<>();
							long startScanTime = System.currentTimeMillis();
							long startTime = 0;
							long endTime = 0;
							long oldOffset = brokerController.getConsumerOffsetManager().queryOffset(PopAckConstants.REVIVE_GROUP, reviveTopic, i);
							long offset = oldOffset+1;
							while (true) {
								List<MessageExt> messageExts = getReviveMessage(offset, i);
								if (messageExts.isEmpty()) {
									if (endTime!=0&&(System.currentTimeMillis()-endTime > 10 * PopAckConstants.ackTimeInterval)) {
										endTime=System.currentTimeMillis();
									}
									break;
								}
								if (System.currentTimeMillis() - startScanTime > PopAckConstants.scanTime) {
									break;
								}
								for (MessageExt messageExt : messageExts) {
									long deliverTime = Long.valueOf(messageExt.getUserProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS));
									if (PopAckConstants.CK_TAG.equals(messageExt.getTags())) {
										PopCheckPoint point = JSON.parseObject(new String(messageExt.getBody(), DataConverter.charset), PopCheckPoint.class);
										if (point.getTopic()==null||point.getCid()==null) {
											continue;
										}
										map.put(point.getTopic() + point.getCid() + point.getQueueId() + point.getStartOffset(), point);
										if (startTime == 0) {
											startTime = deliverTime;
										}
										point.setReviveOffset(messageExt.getQueueOffset());
									} else if (PopAckConstants.ACK_TAG.equals(messageExt.getTags())) {
										AckMsg ackMsg = JSON.parseObject(new String(messageExt.getBody(), DataConverter.charset), AckMsg.class);
										PopCheckPoint point = map.get(ackMsg.getTopic() + ackMsg.getConsumerGroup() + ackMsg.getQueueId() + ackMsg.getStartOffset());
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
							long newOffset = oldOffset;
							for (PopCheckPoint popCheckPoint : sortList) {
								if (endTime - popCheckPoint.getReviveTime() > PopAckConstants.ackTimeInterval) {
									for (int j = 0; j < popCheckPoint.getNum(); j++) {
										if (!DataConverter.getBit(popCheckPoint.getBitMap(), j)) {
											// retry msg
											MessageExt messageExt = getBizMessage(popCheckPoint.getTopic(), popCheckPoint.getStartOffset() + j, popCheckPoint.getQueueId());
											MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
											msgInner.setTopic(messageExt.getTopic());
											msgInner.setBody(messageExt.getBody());
											msgInner.setQueueId(messageExt.getQueueId());
											msgInner.setTags(messageExt.getTags());
											msgInner.setBornTimestamp(System.currentTimeMillis());
											msgInner.setBornHost(brokerController.getStoreHost());
											msgInner.setStoreHost(brokerController.getStoreHost());
											msgInner.getProperties().putAll(messageExt.getProperties());
											PutMessageResult putMessageResult = brokerController.getMessageStore().putMessage(msgInner);
										}
									}
								} else {
									break;
								}
								newOffset = popCheckPoint.getReviveOffset();
							}
							if (newOffset > oldOffset) {
								brokerController.getConsumerOffsetManager().commitOffset(PopAckConstants.LOCAL_HOST, PopAckConstants.REVIVE_GROUP, reviveTopic, i, newOffset);
							}
						}

					} catch (Exception e) {
						LOG.error("revive error", e);
					}
				}

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
		String[] extraInfo = requestHeader.getExtraInfo().split(MessageConst.KEY_SEPARATOR);
		ackMsg.setAckOffset(requestHeader.getOffset());
		ackMsg.setStartOffset(Long.valueOf(extraInfo[0]));
		ackMsg.setConsumerGroup(requestHeader.getConsumerGroup());
		ackMsg.setTopic(requestHeader.getTopic());
		ackMsg.setQueueId(requestHeader.getQueueId());
		msgInner.setTopic(reviveTopic);
		msgInner.setBody(JSON.toJSONString(ackMsg).getBytes(DataConverter.charset));
		msgInner.setQueueId(Integer.valueOf(extraInfo[3]));
		msgInner.setTags(PopAckConstants.ACK_TAG);
		msgInner.setBornTimestamp(System.currentTimeMillis());
		msgInner.setBornHost(this.brokerController.getStoreHost());
		msgInner.setStoreHost(this.brokerController.getStoreHost());
		msgInner.putUserProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS, String.valueOf(Long.valueOf(extraInfo[1]) + Long.valueOf(extraInfo[2])));
		PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
		return null;
	}

}
