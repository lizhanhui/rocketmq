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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.longpolling.PopRequest;
import org.apache.rocketmq.broker.pagecache.ManyMessageTransfer;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.PopMessageResponseHeader;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.pop.PopCheckPoint;
import org.apache.rocketmq.util.cache.LockManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

public class PopMessageProcessor implements NettyRequestProcessor {
    private static final Logger POP_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    private final BrokerController brokerController;
	private Random random = new Random(System.currentTimeMillis());
	private String reviveTopic;
	private static String BORN_TIME = "bornTime";
	private static String POLLING = "POLLING";

	private static final int POLLING_SUC = 0;
	private static final int POLLING_FULL = 1;
	private static final int POLLING_TIMEOUT = 2;
	private static final int NOT_POLLING = 3;
  
    private ConcurrentHashMap<String, ConcurrentHashMap<String,Byte>> topicCidMap;
	private ConcurrentLinkedHashMap<String, LinkedBlockingDeque<PopRequest>> pollingMap;
	private AtomicLong totalPollingNum = new AtomicLong(0);
    public PopMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.reviveTopic=PopAckConstants.REVIVE_TOPIC + this.brokerController.getBrokerConfig().getBrokerClusterName();
        // 100000 topic default,  100000 lru topic + cid + qid 
		this.topicCidMap = new ConcurrentHashMap<String, ConcurrentHashMap<String, Byte>>(this.brokerController.getBrokerConfig().getPopPollingMapSize());
		this.pollingMap = new ConcurrentLinkedHashMap.Builder<String, LinkedBlockingDeque<PopRequest>>().maximumWeightedCapacity(this.brokerController.getBrokerConfig().getPopPollingMapSize()).build();
        Thread t=new Thread(new Runnable() {
			
			@Override
			public void run() {
				int i=0;
				while (true) {
					try {
						Thread.sleep(100L);
						i++;
						Set<String> pollingKeys = pollingMap.keySet();
						if (pollingKeys == null) {
							continue;
						}
						long tmpTotalPollingNum = 0;
						for (String key : pollingKeys) {
							LinkedBlockingDeque<PopRequest> popQ = pollingMap.get(key);
							if (popQ == null) {
								continue;
							}
							PopRequest tmPopRequest = popQ.peek();
							while (tmPopRequest != null) {
								if (tmPopRequest.isTimeout()) {
									tmPopRequest = popQ.poll();
									if (tmPopRequest == null) {
										break;
									}
									totalPollingNum.decrementAndGet();
									if (!tmPopRequest.isTimeout()) {
										POP_LOGGER.info("not timeout , but wakeUp polling in advance: {}", tmPopRequest);
										wakeUp(tmPopRequest);
										break;
									} else {
										POP_LOGGER.info("timeout , wakeUp polling : {}", tmPopRequest);
										wakeUp(tmPopRequest);
										tmPopRequest = popQ.peek();
									}
								} else {
									break;
								}
							}
							if (i >= 10) {
								long tmpPollingNum = popQ.size();
								tmpTotalPollingNum = tmpTotalPollingNum + tmpPollingNum;
								POP_LOGGER.info("polling queue {} , size={} ", key, tmpPollingNum);
							}
						}
						if (i >= 10) {
							POP_LOGGER.info("pollingMapSize={},tmpTotalSize={},atomicTotalSize={},diffSize={}", pollingKeys.size(),tmpTotalPollingNum, totalPollingNum.get(), Math.abs(totalPollingNum.get() - tmpTotalPollingNum));
							i = 0;
						}
					} catch (Exception e) {
						POP_LOGGER.error("checkPolling error", e);
					}
				}
			}
		});
        t.setDaemon(true);
        t.setName("checkPolling");
        t.start();
    }
    
    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
		request.addExtField(BORN_TIME, String.valueOf(System.currentTimeMillis()));
        return this.processRequest(ctx.channel(), request);
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    public ConcurrentLinkedHashMap<String, LinkedBlockingDeque<PopRequest>> getPollingMap() {
		return pollingMap;
	}
	public void notifyMessageArriving(final String topic, final int queueId) {
		ConcurrentHashMap<String,Byte> cids = topicCidMap.get(topic);
		if (cids == null) {
			return ;
		}
		if (cids != null) {
			for (Entry<String, Byte> cid : cids.entrySet()) {
				LinkedBlockingDeque<PopRequest> remotingCommands = pollingMap.get(KeyBuilder.buildPollingKey(topic, cid.getKey(), -1));
				if (remotingCommands != null) {
					PopRequest popRequest = remotingCommands.poll();
					if (popRequest != null) {
						totalPollingNum.decrementAndGet();
						POP_LOGGER.info("new msg arrive , wakeUp : {}",popRequest);
						wakeUp(popRequest);
					}
				}
				remotingCommands = pollingMap.get(KeyBuilder.buildPollingKey(topic, cid.getKey(), queueId));
				if (remotingCommands != null) {
					PopRequest popRequest = remotingCommands.poll();
					if (popRequest != null) {
						totalPollingNum.decrementAndGet();
						POP_LOGGER.info("new msg arrive , wakeUp : {}",popRequest);
						wakeUp(popRequest);
					}
				}
			}
		}
	}
    public void notifyMessageArriving(final String topic, final String cid,final int queueId) {
     	LinkedBlockingDeque<PopRequest> remotingCommands = pollingMap.get(KeyBuilder.buildPollingKey(topic, cid, queueId));
		if (remotingCommands==null||remotingCommands.isEmpty()) {
			return;
		}
		PopRequest popRequest=remotingCommands.poll();
		if (popRequest==null) {
			return ;
		}
		totalPollingNum.decrementAndGet();
		POP_LOGGER.info("lock release , wakeUp : {}",popRequest);
		wakeUp(popRequest);
    }
	private void wakeUp(final PopRequest request ) {
		if (request == null||!request.complete()) {
			return ;
		}
		if (!request.getChannel().isActive()) {
			return ;
		}
		Runnable run = new Runnable() {
			@Override
			public void run() {
				try {
					final RemotingCommand response = PopMessageProcessor.this.processRequest(request.getChannel(), request.getRemotingCommand());

					if (response != null) {
						response.setOpaque(request.getRemotingCommand().getOpaque());
						response.markResponseType();
						try {
							request.getChannel().writeAndFlush(response).addListener(new ChannelFutureListener() {
								@Override
								public void operationComplete(ChannelFuture future) throws Exception {
									if (!future.isSuccess()) {
										POP_LOGGER.error("ProcessRequestWrapper response to {} failed", future.channel().remoteAddress(), future.cause());
										POP_LOGGER.error(request.toString());
										POP_LOGGER.error(response.toString());
									}
								}
							});
						} catch (Throwable e) {
							POP_LOGGER.error("ProcessRequestWrapper process request over, but response failed", e);
							POP_LOGGER.error(request.toString());
							POP_LOGGER.error(response.toString());
						}
					}
				} catch (RemotingCommandException e1) {
					POP_LOGGER.error("ExecuteRequestWhenWakeup run", e1);
				}
			}
		};
		this.brokerController.getPullMessageExecutor().submit(new RequestTask(run, request.getChannel(), request.getRemotingCommand()));
	}
    private RemotingCommand processRequest(final Channel channel, RemotingCommand request)
        throws RemotingCommandException {
        RemotingCommand response = RemotingCommand.createResponseCommand(PopMessageResponseHeader.class);
        final PopMessageResponseHeader responseHeader = (PopMessageResponseHeader) response.readCustomHeader();
        final PopMessageRequestHeader requestHeader =
            (PopMessageRequestHeader) request.decodeCommandCustomHeader(PopMessageRequestHeader.class);

        response.setOpaque(request.getOpaque());
        
        if (POP_LOGGER.isDebugEnabled()) {
            POP_LOGGER.debug("receive PopMessage request command, {}", request);
        }
        if (requestHeader.isTimeoutTooMuch()) {
			response.setCode(POLLING_TIMEOUT);
            response.setRemark(String.format("the broker[%s] poping message is timeout too much", this.brokerController.getBrokerConfig().getBrokerIP1()));
            return response;
		}
        if (!PermName.isReadable(this.brokerController.getBrokerConfig().getBrokerPermission())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark(String.format("the broker[%s] poping message is forbidden", this.brokerController.getBrokerConfig().getBrokerIP1()));
            return response;
        }

        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            POP_LOGGER.error("The topic {} not exist, consumer: {} ", requestHeader.getTopic(), RemotingHelper.parseChannelRemoteAddr(channel));
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark(String.format("topic[%s] not exist, apply first please! %s", requestHeader.getTopic(), FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL)));
            return response;
        }

        if (!PermName.isReadable(topicConfig.getPerm())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the topic[" + requestHeader.getTopic() + "] peeking message is forbidden");
            return response;
        }
        
        if (requestHeader.getQueueId() >= topicConfig.getReadQueueNums()) {
            String errorInfo = String.format("queueId[%d] is illegal, topic:[%s] topicConfig.readQueueNums:[%d] consumer:[%s]",
                    requestHeader.getQueueId(), requestHeader.getTopic(), topicConfig.getReadQueueNums(), channel.remoteAddress());
            POP_LOGGER.warn(errorInfo);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(errorInfo);
            return response;
        }
		SubscriptionGroupConfig subscriptionGroupConfig = this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(requestHeader.getConsumerGroup());
		if (null == subscriptionGroupConfig) {
			response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
			response.setRemark(String.format("subscription group [%s] does not exist, %s", requestHeader.getConsumerGroup(), FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST)));
			return response;
		}

		if (!subscriptionGroupConfig.isConsumeEnable()) {
			response.setCode(ResponseCode.NO_PERMISSION);
			response.setRemark("subscription group no permission, " + requestHeader.getConsumerGroup());
			return response;
		}
		int randomQ=random.nextInt(100);
		int reviveQid=randomQ % this.brokerController.getBrokerConfig().getReviveQueueNum();
		GetMessageResult getMessageResult=new GetMessageResult();
		long restNum = 0;
		boolean needRetry=(randomQ % 5 == 0);
		long popTime=System.currentTimeMillis();
		if (needRetry) {
			TopicConfig retryTopicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(KeyBuilder.buildPopRetryTopic(requestHeader.getTopic(), requestHeader.getConsumerGroup()));
			if (retryTopicConfig != null) {
				for (int i = 0; i < retryTopicConfig.getReadQueueNums(); i++) {
					int queueId = (randomQ + i) % retryTopicConfig.getReadQueueNums();
					restNum = popMsgFromQueue(true, getMessageResult, requestHeader, queueId, restNum, reviveQid, channel, popTime);
				}
			}
		}
		if (requestHeader.getQueueId() < 0) {
			// read all queue
			for (int i = 0; i < topicConfig.getReadQueueNums(); i++) {
				int queueId = (randomQ + i) % topicConfig.getReadQueueNums();
				restNum = popMsgFromQueue(false, getMessageResult, requestHeader, queueId, restNum, reviveQid, channel,popTime);
			}
		}else {
			int queueId = requestHeader.getQueueId();
			restNum = popMsgFromQueue(false, getMessageResult, requestHeader, queueId, restNum, reviveQid, channel,popTime);
		}
		// if not full , fetch retry again
		if (!needRetry && getMessageResult.getMessageMapedList().size() < requestHeader.getMaxMsgNums()) {
			TopicConfig retryTopicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(KeyBuilder.buildPopRetryTopic(requestHeader.getTopic(), requestHeader.getConsumerGroup()));
			if (retryTopicConfig != null) {
				for (int i = 0; i < retryTopicConfig.getReadQueueNums(); i++) {
					int queueId = (randomQ + i) % retryTopicConfig.getReadQueueNums();
					restNum = popMsgFromQueue(true, getMessageResult, requestHeader, queueId, restNum, reviveQid, channel, popTime);
				}
			}
		}
		if (!getMessageResult.getMessageBufferList().isEmpty()) {
            response.setCode(ResponseCode.SUCCESS);
            getMessageResult.setStatus(GetMessageStatus.FOUND);
			if (restNum > 0) {
            	// all queue pop can not notify specified queue pop, and vice versa
                notifyMessageArriving(requestHeader.getTopic(), requestHeader.getConsumerGroup(),requestHeader.getQueueId());
			}
		} else {
			int pollingResult = polling(channel, request, requestHeader);
			if (POLLING_SUC == pollingResult) {
				return null;
			} else if (POLLING_FULL == pollingResult) {
				response.setCode(ResponseCode.POLLING_FULL);
			} else {
				response.setCode(ResponseCode.POLLING_TIMEOUT);
			}
			getMessageResult.setStatus(GetMessageStatus.NO_MESSAGE_IN_QUEUE);
		}
		responseHeader.setInvisibleTime(requestHeader.getInvisibleTime());
		responseHeader.setPopTime(popTime);
		responseHeader.setReviveQid(reviveQid);
		responseHeader.setRestNum(restNum);
        response.setRemark(getMessageResult.getStatus().name());
        switch (response.getCode()) {
            case ResponseCode.SUCCESS:

                this.brokerController.getBrokerStatsManager().incGroupGetNums(requestHeader.getConsumerGroup(), requestHeader.getTopic(),
                    getMessageResult.getMessageCount());

                this.brokerController.getBrokerStatsManager().incGroupGetSize(requestHeader.getConsumerGroup(), requestHeader.getTopic(),
                    getMessageResult.getBufferTotalSize());

                this.brokerController.getBrokerStatsManager().incBrokerGetNums(getMessageResult.getMessageCount());
                if (this.brokerController.getBrokerConfig().isTransferMsgByHeap()) {
                    final long beginTimeMills = this.brokerController.getMessageStore().now();
                    final byte[] r = this.readGetMessageResult(getMessageResult, requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId());
                    this.brokerController.getBrokerStatsManager().incGroupGetLatency(requestHeader.getConsumerGroup(),
                        requestHeader.getTopic(), requestHeader.getQueueId(),
                        (int) (this.brokerController.getMessageStore().now() - beginTimeMills));
                    response.setBody(r);
                } else {
                	final GetMessageResult tmpGetMessageResult=getMessageResult;
                    try {
                        FileRegion fileRegion =
                            new ManyMessageTransfer(response.encodeHeader(getMessageResult.getBufferTotalSize()), getMessageResult);
                        channel.writeAndFlush(fileRegion).addListener(new ChannelFutureListener() {
                            @Override
                            public void operationComplete(ChannelFuture future) throws Exception {
                            	tmpGetMessageResult.release();
                                if (!future.isSuccess()) {
                                    POP_LOGGER.error("Fail to transfer messages from page cache to {}", channel.remoteAddress(), future.cause());
                                }
                            }
                        });
                    } catch (Throwable e) {
                        POP_LOGGER.error("Error occurred when transferring messages from page cache", e);
                        getMessageResult.release();
                    }

                    response = null;
                }
                break;
            default:
                assert false;
        }
        return response;
    }
    private long popMsgFromQueue(boolean isRetry,GetMessageResult getMessageResult,PopMessageRequestHeader requestHeader,int queueId,long restNum,int reviveQid, Channel channel,long popTime){
		String topic = isRetry ? KeyBuilder.buildPopRetryTopic(requestHeader.getTopic(), requestHeader.getConsumerGroup()) : requestHeader.getTopic();
		long offset = getPopOffset(topic,requestHeader, queueId);
		if (!LockManager.tryLock(LockManager.buildKey(topic, requestHeader.getConsumerGroup(), queueId), PopAckConstants.lockTime)) {
			restNum = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - offset + restNum;
			return restNum;
		}
		GetMessageResult getMessageTmpResult;
		try {
			if (getMessageResult.getMessageMapedList().size() >= requestHeader.getMaxMsgNums()) {
				restNum = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - offset + restNum;
				return restNum;
			}
			getMessageTmpResult = this.brokerController.getMessageStore().getMessage(requestHeader.getConsumerGroup(), topic, queueId, offset,
					requestHeader.getMaxMsgNums() - getMessageResult.getMessageMapedList().size(), null);
			// maybe store offset is not correct.
			if (GetMessageStatus.OFFSET_TOO_SMALL.equals(getMessageTmpResult.getStatus())
					|| GetMessageStatus.OFFSET_OVERFLOW_BADLY.equals(getMessageTmpResult.getStatus())) {
				offset = getMessageTmpResult.getNextBeginOffset();
				getMessageTmpResult = this.brokerController.getMessageStore().getMessage(requestHeader.getConsumerGroup(), topic, queueId, offset,
						requestHeader.getMaxMsgNums() - getMessageResult.getMessageMapedList().size(), null);
			}

			restNum = getMessageTmpResult.getMaxOffset() - getMessageTmpResult.getNextBeginOffset() + restNum;
			if (!getMessageTmpResult.getMessageMapedList().isEmpty()) {
				appendCheckPoint(channel, requestHeader, topic,reviveQid, queueId, offset, getMessageTmpResult,popTime);
			}
		} finally {
			LockManager.unLock(LockManager.buildKey(topic, requestHeader.getConsumerGroup(), queueId));
		}
		if (getMessageTmpResult != null) {
			for (SelectMappedBufferResult mapedBuffer : getMessageTmpResult.getMessageMapedList()) {
				getMessageResult.addMessage(mapedBuffer);
			}
		}
		return restNum;
    }

	private long getPopOffset(String topic, PopMessageRequestHeader requestHeader, int queueId) {
		long offset = this.brokerController.getConsumerOffsetManager().queryOffset(requestHeader.getConsumerGroup(), topic, queueId);
		if (offset < 0) {
			if (ConsumeInitMode.MIN == requestHeader.getInitMode()) {
				offset = this.brokerController.getMessageStore().getMinOffsetInQueue(topic, queueId);
			} else {
				// pop last one,then commit offset.
				offset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId) - 1;
			}
		}
		return offset;
	}
	public static void main(String[] args) {
		Map<String, LinkedBlockingDeque<PopRequest>> map=new ConcurrentLinkedHashMap.Builder<String, LinkedBlockingDeque<PopRequest>>().maximumWeightedCapacity(100000).build();
		Channel channel=new NioSocketChannel();
		for (int i = 0; i < 100000; i++) {
			String prefix="asdfasdfasdfasdfasdfewevcvcvsdfsxxxv2545678451sdfxvgdsdfsdf5454654587sdfsdf";
			LinkedBlockingDeque<PopRequest> deque=new LinkedBlockingDeque<>(10204);
			for (int j = 0; j < 2; j++) {
				PopMessageRequestHeader pop=new PopMessageRequestHeader();
				pop.setTopic(prefix+i);
				pop.setConsumerGroup(prefix+"c"+i);
				RemotingCommand remotingCommand=RemotingCommand.createRequestCommand(100, pop) ;
				remotingCommand.addExtField("sdfs", j+"");
				deque.add(new PopRequest(remotingCommand, channel, 100000));
			}
			map.put(prefix+i, deque);
		}
		for (LinkedBlockingDeque<PopRequest> entry : map.values()) {
			System.out.println(entry.size());
		}
	}
	/**
	 * 
	 * @param channel
	 * @param remotingCommand
	 * @param requestHeader
	 * @return
	 */
	private int polling(final Channel channel, RemotingCommand remotingCommand, final PopMessageRequestHeader requestHeader) {
		if (requestHeader.getPollTime() <= 0) {
			return NOT_POLLING;
		}
		ConcurrentHashMap<String, Byte> cids = topicCidMap.get(requestHeader.getTopic());
		if (cids == null) {
			cids = new ConcurrentHashMap<String, Byte>();
			cids.putIfAbsent(requestHeader.getConsumerGroup(), Byte.MIN_VALUE);
			topicCidMap.put(requestHeader.getTopic(), cids);
		} else {
			cids.putIfAbsent(requestHeader.getConsumerGroup(), Byte.MIN_VALUE);
		}
		long expired = requestHeader.getBornTime() + requestHeader.getPollTime();
		final PopRequest request = new PopRequest(remotingCommand, channel, expired);
		boolean isFull=(totalPollingNum.get() >= this.brokerController.getBrokerConfig().getMaxPopPollingSize());
		if (isFull) {
			POP_LOGGER.info("polling {}, result POLLING_FULL", remotingCommand);
			return POLLING_FULL;
		}
		boolean isTimeout=request.isTimeout();
		if (isTimeout) {
			POP_LOGGER.info("polling {}, result POLLING_TIMEOUT", remotingCommand);
			return POLLING_TIMEOUT;
		}
		boolean offerResult = false;
		String key = KeyBuilder.buildPollingKey(requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getQueueId());
		LinkedBlockingDeque<PopRequest> queue = pollingMap.get(key);
		if (queue == null) {
			queue = new LinkedBlockingDeque<>(this.brokerController.getBrokerConfig().getPopPollingSize());
			LinkedBlockingDeque<PopRequest> old = pollingMap.putIfAbsent(key, queue);
			if (old != null) {
				queue = old;
			}
		}
		if (remotingCommand.getExtFields().get(POLLING) == null) {
			offerResult = queue.offer(request);
		} else {
			offerResult = queue.offerFirst(request);
		}
		remotingCommand.addExtField(POLLING, POLLING);
		if (offerResult) {
			totalPollingNum.incrementAndGet();
			POP_LOGGER.info("polling {}, result POLLING_SUC", remotingCommand);
			return POLLING_SUC;
		}else {
			POP_LOGGER.info("polling {}, result POLLING_FULL", remotingCommand);
			return POLLING_FULL;
		}
	}

	private void appendCheckPoint(final Channel channel, final PopMessageRequestHeader requestHeader,String topic, int reviveQid, int queueId, long offset,
			final GetMessageResult getMessageTmpResult,long popTime) {
		// add check point msg to revive log
		MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
		msgInner.setTopic(reviveTopic);
		PopCheckPoint ck=new PopCheckPoint();
		ck.setBm(0);
		ck.setN((byte) getMessageTmpResult.getMessageMapedList().size());
		ck.setPt(popTime);
		ck.setIt(requestHeader.getInvisibleTime());
		ck.setSo(offset);
		ck.setC(requestHeader.getConsumerGroup());
		ck.setT(topic);
		ck.setQ((byte) queueId);
		msgInner.setBody(JSON.toJSONString(ck).getBytes(DataConverter.charset));
		msgInner.setQueueId(reviveQid);
		msgInner.setTags(PopAckConstants.CK_TAG);
		msgInner.setBornTimestamp(System.currentTimeMillis());
		msgInner.setBornHost(this.brokerController.getStoreHost());
		msgInner.setStoreHost(this.brokerController.getStoreHost());
		msgInner.putUserProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS, String.valueOf(ck.getRt()-PopAckConstants.ackTimeInterval));
		msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, ck.getT() + PopAckConstants.SPLIT + ck.getQ() + PopAckConstants.SPLIT + ck.getSo() + PopAckConstants.SPLIT + ck.getC());
		msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
		PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
		if (putMessageResult.getAppendMessageResult().getStatus() == AppendMessageStatus.PUT_OK) {
			this.brokerController.getConsumerOffsetManager().commitOffset(channel.remoteAddress().toString(), requestHeader.getConsumerGroup(), topic,
					queueId, offset + getMessageTmpResult.getMessageMapedList().size());
		}
		POP_LOGGER.info("appendCheckPoint, topic {}, queueId {},reviveId {}, cid {}, startOffset {}, commit offset {}, result {}", topic, queueId,reviveQid, requestHeader.getConsumerGroup(), offset, offset + getMessageTmpResult.getMessageMapedList().size(),putMessageResult);
	}

    private byte[] readGetMessageResult(final GetMessageResult getMessageResult, final String group, final String topic, final int queueId) {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(getMessageResult.getBufferTotalSize());

        long storeTimestamp = 0;
        try {
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            for (ByteBuffer bb : messageBufferList) {

                byteBuffer.put(bb);
                storeTimestamp = bb.getLong(MessageDecoder.MESSAGE_STORE_TIMESTAMP_POSTION);
            }
        } finally {
            getMessageResult.release();
        }

        this.brokerController.getBrokerStatsManager().recordDiskFallBehindTime(group, topic, queueId, this.brokerController.getMessageStore().now() - storeTimestamp);
        return byteBuffer.array();
    }
    public void executeRequestWhenWakeup(final Channel channel, final RemotingCommand request) throws RemotingCommandException {
        Runnable run = new Runnable() {
            @Override
            public void run() {
                try {
                    final RemotingCommand response = PopMessageProcessor.this.processRequest(channel, request);

                    if (response != null) {
                        response.setOpaque(request.getOpaque());
                        response.markResponseType();
                        try {
                            channel.writeAndFlush(response).addListener(new ChannelFutureListener() {
                                @Override
                                public void operationComplete(ChannelFuture future) throws Exception {
                                    if (!future.isSuccess()) {
                                        POP_LOGGER.error("ProcessRequestWrapper response to {} failed", future.channel().remoteAddress(), future.cause());
                                        POP_LOGGER.error(request.toString());
                                        POP_LOGGER.error(response.toString());
                                    }
                                }
                            });
                        } catch (Throwable e) {
                            POP_LOGGER.error("ProcessRequestWrapper process request over, but response failed", e);
                            POP_LOGGER.error(request.toString());
                            POP_LOGGER.error(response.toString());
                        }
                    }
                } catch (RemotingCommandException e1) {
                    POP_LOGGER.error("ExecuteRequestWhenWakeup run", e1);
                }
            }
        };
        this.brokerController.getPullMessageExecutor().submit(new RequestTask(run, channel, request));
    }

}
