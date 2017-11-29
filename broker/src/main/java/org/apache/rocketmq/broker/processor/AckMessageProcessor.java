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
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
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
	private ExecutorService executorService ;
    private Random random=new Random(System.currentTimeMillis());

	public AckMessageProcessor(final BrokerController brokerController) {
		this.brokerController = brokerController;
		this.reviveTopic = PopAckConstants.REVIVE_TOPIC + this.brokerController.getBrokerConfig().getBrokerClusterName();
		executorService = Executors.newFixedThreadPool(this.brokerController.getBrokerConfig().getReviveQueueNum() + 1, new ThreadFactory() {
			private AtomicInteger count = new AtomicInteger();

			@Override
			public Thread newThread(Runnable r) {
				Thread thread = new Thread(r, "reviveTask-" + count.get());
				count.incrementAndGet();
				thread.setDaemon(true);
				return thread;
			}
		});
		for (int qid = 0; qid < this.brokerController.getBrokerConfig().getReviveQueueNum(); qid++) {
			ReviveTask task=new ReviveTask(qid);
			executorService.execute(task);
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
		msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
		PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
		if (putMessageResult.getPutMessageStatus() != PutMessageStatus.PUT_OK 
				&& putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_DISK_TIMEOUT
				&& putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_SLAVE_TIMEOUT 
				&& putMessageResult.getPutMessageStatus() != PutMessageStatus.SLAVE_NOT_AVAILABLE) {
			POP_LOGGER.warn("put ack msg error:" + putMessageResult);
		}
		return response;
	}
	class ReviveTask implements Runnable{
		private int queueId;
		public ReviveTask(int i){
			this.queueId=i;
		}
		@Override
		public void run() {
			try {
				int initDelay = random.nextInt((int) brokerController.getBrokerConfig().getReviveInterval());
				if (initDelay > 0) {
					Thread.sleep(initDelay);
				}
			} catch (Exception e) {
				POP_LOGGER.error("init delay error", e);
			}

			while (true) {
				try {
					POP_LOGGER.info("start revive topic={}, reviveQueueId={}",reviveTopic,queueId);
					try {
						Thread.sleep(brokerController.getBrokerConfig().getReviveInterval());
						HashMap<String, PopCheckPoint> map = new HashMap<>();
						long startScanTime = System.currentTimeMillis();
						long startTime = 0;
						long endTime = 0;
						long oldOffset = brokerController.getConsumerOffsetManager().queryOffset(PopAckConstants.REVIVE_GROUP, reviveTopic, queueId);
						POP_LOGGER.info("reviveQueueId={}, old offset is {} ",queueId,oldOffset);
						long offset = oldOffset + 1;
						//TODO: offset 自我纠正
						while (true) {
							long timerDelay = brokerController.getMessageStore().getTimerMessageStore().getReadBehind();
							List<MessageExt> messageExts = getReviveMessage(offset, queueId);
							if (messageExts.isEmpty()) {
								if (endTime != 0 && (System.currentTimeMillis() - endTime) > (2 * PopAckConstants.ackTimeInterval + timerDelay)) {
									endTime = System.currentTimeMillis();
								}
								POP_LOGGER.info("reviveQueueId={}, offset is {}, can not get new msg  ", queueId, offset);
								break;
							}
							if (System.currentTimeMillis() - startScanTime > brokerController.getBrokerConfig().getReviveScanTime()) {
								POP_LOGGER.info("reviveQueueId={}, scan timeout  ",queueId);
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
						POP_LOGGER.info("reviveQueueId={},ck list size={}", queueId, sortList.size());
						if (sortList.size()!=0) {
							POP_LOGGER.info("reviveQueueId={}, 1st ck, startOffset={}, reviveOffset={} ; last ck, startOffset={}, reviveOffset={}",queueId, sortList.get(0).getStartOffset(),
									sortList.get(0).getReviveOffset(), sortList.get(sortList.size() - 1).getStartOffset(), sortList.get(sortList.size() - 1).getReviveOffset());
						}
						long newOffset = oldOffset;
						for (PopCheckPoint popCheckPoint : sortList) {
							if (endTime - popCheckPoint.getReviveTime() > PopAckConstants.ackTimeInterval) {
								for (int j = 0; j < popCheckPoint.getNum(); j++) {
									if (!DataConverter.getBit(popCheckPoint.getBitMap(), j)) {
										// retry msg
										MessageExt messageExt = getBizMessage(popCheckPoint.getTopic(), popCheckPoint.getStartOffset() + j, popCheckPoint.getQueueId());
										if (messageExt == null) {
											POP_LOGGER.warn("can not get biz msg {} , then continue ", popCheckPoint.getStartOffset() + j);
											continue;
										}
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
										POP_LOGGER.info("reviveQueueId={},retry msg , topic {}, cid {}, msg queueId {}, offset {}, revive delay {}, result is {} ",queueId,popCheckPoint.getTopic(),popCheckPoint.getCid(),messageExt.getQueueId(),messageExt.getQueueOffset(),(System.currentTimeMillis()-popCheckPoint.getReviveTime())/1000,putMessageResult);
										if (putMessageResult.getAppendMessageResult().getStatus()!=AppendMessageStatus.PUT_OK) {
											throw new Exception("reviveQueueId=" + queueId + "revive error ,msg is :"+msgInner);
										}
									}
								}
							} else {
								break;
							}
							newOffset = popCheckPoint.getReviveOffset();
						}
						POP_LOGGER.info("reviveQueueId={},revive finish,old offset is {}, new offset is {}  ", queueId, oldOffset, newOffset);
						if (newOffset > oldOffset) {
							brokerController.getConsumerOffsetManager().commitOffset(PopAckConstants.LOCAL_HOST, PopAckConstants.REVIVE_GROUP, reviveTopic, queueId, newOffset);
						}
					
					} catch (Exception e) {
						POP_LOGGER.error("reviveQueueId=" + queueId + ",revive error", e);
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
			topicConfig.setReadQueueNums(PopAckConstants.retryQueueNum);
			topicConfig.setWriteQueueNums(PopAckConstants.retryQueueNum);
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
				POP_LOGGER.warn("can not get msg , topic {}, offset {}, queueId {}, result is {}", topic, offset, queueId, getMessageTmpResult);
				return null;
			} else {
				return list.get(0);
			}
		}
	    public PullResult getMessage(String group, String topic, int queueId, long offset, int nums) throws Exception {
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
	            for (ByteBuffer bb : messageBufferList) {
	                MessageExt msgExt = MessageDecoder.decode(bb);
	                foundList.add(msgExt);
	            }

	        } finally {
	            getMessageResult.release();
	        }

	        return foundList;
	    }
	}
}
