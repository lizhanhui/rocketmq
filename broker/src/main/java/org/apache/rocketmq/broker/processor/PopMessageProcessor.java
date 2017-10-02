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
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.longpolling.PopRequest;
import org.apache.rocketmq.broker.pagecache.ManyMessageTransfer;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.PopMessageResponseHeader;
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
import org.apache.rocketmq.util.cache.ExpiredLocalCache;
import org.apache.rocketmq.util.cache.LockManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

public class PopMessageProcessor implements NettyRequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerController brokerController;
    private Random random=new Random(System.currentTimeMillis());
	private Charset charset=Charset.forName("UTF-8");
	private String reviveTopic;
	private ExpiredLocalCache<String, HashSet<String>> topicCidMap=new ExpiredLocalCache<String, HashSet<String>>(100000); 
	private ConcurrentLinkedHashMap<String, ArrayBlockingQueue<PopRequest>> pollingMap=new ConcurrentLinkedHashMap.Builder<String, ArrayBlockingQueue<PopRequest>>().maximumWeightedCapacity(100000).build();
    public PopMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.reviveTopic=PopAckConstants.REVIVE_TOPIC + this.brokerController.getBrokerConfig().getBrokerClusterName();
        Thread t=new Thread(new Runnable() {
			
			@Override
			public void run() {
				while (true) {
					try {
						Thread.sleep(2000L);
						Collection<ArrayBlockingQueue<PopRequest>> pops=pollingMap.values();
						for (ArrayBlockingQueue<PopRequest> popQ : pops) {
							PopRequest tmPopRequest=popQ.peek();
							if (tmPopRequest.isTimeout()) {
								tmPopRequest=popQ.poll();
								if (!tmPopRequest.isTimeout()) {
									popQ.offer(tmPopRequest);
								}else {
									LOG.info("timeout , wakeUp : {}",tmPopRequest);
									wakeUp(tmPopRequest);
								}
							}
						}
					} catch (Exception e) {
						LOG.error("checkPolling error",e);
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
        return this.processRequest(ctx.channel(), request, true);
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

	private String buildPollingKey(String topic, String cid, int queueId) {
		return topic + PopAckConstants.SPLIT + cid + PopAckConstants.SPLIT + queueId;
	}

	public void notifyMessageArriving(final String topic, final int queueId) {
		HashSet<String> cids = topicCidMap.get(topic);
		if (cids == null) {
			cids = this.brokerController.getConsumerManager().queryTopicConsumeByWho(topic);
			topicCidMap.put(topic, cids, PopAckConstants.cidCacheTime);
		}
		if (cids != null) {
			for (String cid : cids) {
				ArrayBlockingQueue<PopRequest> remotingCommands = pollingMap.get(buildPollingKey(topic, cid, -1));
				if (remotingCommands != null) {
					PopRequest popRequest = remotingCommands.poll();
					if (popRequest != null) {
						LOG.info("new msg arrive , wakeUp : {}",popRequest);
						wakeUp(popRequest);
					}
				}
				remotingCommands = pollingMap.get(buildPollingKey(topic, cid, queueId));
				if (remotingCommands != null) {
					PopRequest popRequest = remotingCommands.poll();
					if (popRequest != null) {
						LOG.info("new msg arrive , wakeUp : {}",popRequest);
						wakeUp(popRequest);
					}
				}
			}
		}
	}
    public void notifyMessageArriving(final String topic, final String cid,final int queueId) {
		ArrayBlockingQueue<PopRequest> remotingCommands = pollingMap.get(buildPollingKey(topic, cid, queueId));
		if (remotingCommands==null||remotingCommands.isEmpty()) {
			return;
		}
		PopRequest popRequest=remotingCommands.poll();
		if (popRequest==null) {
			return ;
		}
		LOG.info("lock release , wakeUp : {}",popRequest);
		wakeUp(popRequest);
    }
	private void wakeUp(final PopRequest request ) {
		if (request == null||!request.complete()) {
			return ;
		}
		Runnable run = new Runnable() {
			@Override
			public void run() {
				try {
					final RemotingCommand response = PopMessageProcessor.this.processRequest(request.getChannel(), request.getRemotingCommand(), false);

					if (response != null) {
						response.setOpaque(request.getRemotingCommand().getOpaque());
						response.markResponseType();
						try {
							request.getChannel().writeAndFlush(response).addListener(new ChannelFutureListener() {
								@Override
								public void operationComplete(ChannelFuture future) throws Exception {
									if (!future.isSuccess()) {
										LOG.error("ProcessRequestWrapper response to {} failed", future.channel().remoteAddress(), future.cause());
										LOG.error(request.toString());
										LOG.error(response.toString());
									}
								}
							});
						} catch (Throwable e) {
							LOG.error("ProcessRequestWrapper process request over, but response failed", e);
							LOG.error(request.toString());
							LOG.error(response.toString());
						}
					}
				} catch (RemotingCommandException e1) {
					LOG.error("ExecuteRequestWhenWakeup run", e1);
				}
			}
		};
		this.brokerController.getPullMessageExecutor().submit(new RequestTask(run, request.getChannel(), request.getRemotingCommand()));
	}
    private RemotingCommand processRequest(final Channel channel, RemotingCommand request, boolean brokerAllowSuspend)
        throws RemotingCommandException {
        RemotingCommand response = RemotingCommand.createResponseCommand(PopMessageResponseHeader.class);
        final PopMessageResponseHeader responseHeader = (PopMessageResponseHeader) response.readCustomHeader();
        final PopMessageRequestHeader requestHeader =
            (PopMessageRequestHeader) request.decodeCommandCustomHeader(PopMessageRequestHeader.class);

        response.setOpaque(request.getOpaque());
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("receive PopMessage request command, {}", request);
        }

        if (!PermName.isReadable(this.brokerController.getBrokerConfig().getBrokerPermission())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark(String.format("the broker[%s] peeking message is forbidden", this.brokerController.getBrokerConfig().getBrokerIP1()));
            return response;
        }

        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            LOG.error("The topic {} not exist, consumer: {} ", requestHeader.getTopic(), RemotingHelper.parseChannelRemoteAddr(channel));
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
            LOG.warn(errorInfo);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(errorInfo);
            return response;
        }
		int randomQ=random.nextInt(100);
		int reviveQid=randomQ % PopAckConstants.REVIVE_QUEUE_NUM;
		GetMessageResult getMessageResult=new GetMessageResult();
		long  restNum=-1;
		if (requestHeader.getQueueId() < 0) {
			// read all queue
			for (int i = 0; i < topicConfig.getReadQueueNums(); i++) {
				int queueId = (randomQ + i) % topicConfig.getReadQueueNums();
				if (!LockManager.tryLock(LockManager.buildKey(requestHeader, queueId),PopAckConstants.lockTime)) {
					continue;
				}
				GetMessageResult getMessageTmpResult;
				try {
					long offset = this.brokerController.getConsumerOffsetManager().queryOffset(requestHeader.getConsumerGroup(), requestHeader.getTopic(), queueId);
					if (offset < 0) {
						offset = 0;
					}
					getMessageTmpResult = this.brokerController.getMessageStore().getMessage(requestHeader.getConsumerGroup(), requestHeader.getTopic(), queueId, offset,
							requestHeader.getMaxMsgNums() - getMessageResult.getMessageMapedList().size(), null);
					restNum=getMessageTmpResult.getMaxOffset()-getMessageTmpResult.getNextBeginOffset();
					if (!getMessageTmpResult.getMessageMapedList().isEmpty()) {
						appendCheckPoint(channel, requestHeader, reviveQid, queueId, offset, getMessageTmpResult);
					}
				} finally {
					LockManager.unLock(LockManager.buildKey(requestHeader, queueId));
				}
				if (getMessageTmpResult != null) {
					for (SelectMappedBufferResult mapedBuffer : getMessageTmpResult.getMessageMapedList()) {
						getMessageResult.addMessage(mapedBuffer);
					}
				}
				if (getMessageResult.getMessageMapedList().size() >= requestHeader.getMaxMsgNums()) {
					break;
				}
			}
		}else {
			if (LockManager.tryLock(LockManager.buildKey(requestHeader, requestHeader.getQueueId()),PopAckConstants.lockTime)) {
				try{
					// read specified queue
					long offset = this.brokerController.getConsumerOffsetManager().queryOffset(requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId());
					if (offset < 0) {
						offset = 0;
					}
					getMessageResult = this.brokerController.getMessageStore().getMessage(requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId(), offset,
							requestHeader.getMaxMsgNums(), null);
					restNum=getMessageResult.getMaxOffset()-getMessageResult.getNextBeginOffset();
					if (!getMessageResult.getMessageMapedList().isEmpty()) {
						// add check point msg to revive log
						appendCheckPoint(channel, requestHeader, reviveQid, requestHeader.getQueueId(), offset, getMessageResult);
					}	
				}finally {
					LockManager.unLock(LockManager.buildKey(requestHeader, requestHeader.getQueueId()));
				}
			}
		}
		if (!getMessageResult.getMessageBufferList().isEmpty()) {
            response.setCode(ResponseCode.SUCCESS);
            getMessageResult.setStatus(GetMessageStatus.FOUND);
            if (restNum>-1) {
            	// all queue pop can not notify specified queue pop, and vice versa
                notifyMessageArriving(requestHeader.getTopic(), requestHeader.getConsumerGroup(),requestHeader.getQueueId());
			}
		}else{
			if (polling(channel, request, requestHeader)) {
				return null;
			}
            response.setCode(ResponseCode.PULL_NOT_FOUND);
            getMessageResult.setStatus(GetMessageStatus.NO_MESSAGE_IN_QUEUE);
		}
		responseHeader.setInvisibleTime(requestHeader.getInvisibleTime());
		responseHeader.setPopTime(System.currentTimeMillis());
		responseHeader.setReviveQid(reviveQid);
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
                                    LOG.error("Fail to transfer messages from page cache to {}", channel.remoteAddress(), future.cause());
                                }
                            }
                        });
                    } catch (Throwable e) {
                        LOG.error("Error occurred when transferring messages from page cache", e);
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

	private boolean polling(final Channel channel, RemotingCommand remotingCommand, final PopMessageRequestHeader requestHeader) {
		if (requestHeader.getPollTime()<=0) {
			return false;
		}
		long expired=requestHeader.getBornTime()+requestHeader.getPollTime();
		final PopRequest request = new PopRequest(remotingCommand, channel,expired);
		boolean result=false;
		if (!request.isTimeout()) {
			String key=buildPollingKey(requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getQueueId());
			ArrayBlockingQueue<PopRequest> queue=pollingMap.get(key);
			if (queue==null) {
				queue=new ArrayBlockingQueue<>(this.brokerController.getBrokerConfig().getPopPollingSize());
				pollingMap.put(key, queue);
				result= queue.offer(request);
			}else {
				result=queue.offer(request);
			}
		}
		LOG.info("polling {}, result {}",remotingCommand,result);
		return result;
	
	}

	private void appendCheckPoint(final Channel channel, final PopMessageRequestHeader requestHeader, int reviveQid, int queueId, long offset,
			final GetMessageResult getMessageTmpResult) {
		// add check point msg to revive log
		MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
		msgInner.setTopic(reviveTopic);
		PopCheckPoint ck=new PopCheckPoint();
		ck.setBitMap(0);
		ck.setNum((byte) getMessageTmpResult.getMessageMapedList().size());
		ck.setReviveTime(System.currentTimeMillis()+requestHeader.getInvisibleTime());
		ck.setStartOffset(offset);
		ck.setCid(requestHeader.getConsumerGroup());
		ck.setTopic(requestHeader.getTopic());
		ck.setQueueId((byte) queueId);
		msgInner.setBody(JSON.toJSONString(ck).getBytes(charset));
		msgInner.setQueueId(reviveQid);
		msgInner.setTags(PopAckConstants.CK_TAG);
		msgInner.setBornTimestamp(System.currentTimeMillis());
		msgInner.setBornHost(this.brokerController.getStoreHost());
		msgInner.setStoreHost(this.brokerController.getStoreHost());
		msgInner.putUserProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS, String.valueOf(System.currentTimeMillis()+requestHeader.getInvisibleTime()-PopAckConstants.ackTimeInterval));
		PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
		if (putMessageResult.getAppendMessageResult().getStatus() == AppendMessageStatus.PUT_OK) {
			this.brokerController.getConsumerOffsetManager().commitOffset(channel.remoteAddress().toString(), requestHeader.getConsumerGroup(), requestHeader.getTopic(),
					queueId, offset + getMessageTmpResult.getMessageMapedList().size());
		}
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
                    final RemotingCommand response = PopMessageProcessor.this.processRequest(channel, request, false);

                    if (response != null) {
                        response.setOpaque(request.getOpaque());
                        response.markResponseType();
                        try {
                            channel.writeAndFlush(response).addListener(new ChannelFutureListener() {
                                @Override
                                public void operationComplete(ChannelFuture future) throws Exception {
                                    if (!future.isSuccess()) {
                                        LOG.error("ProcessRequestWrapper response to {} failed", future.channel().remoteAddress(), future.cause());
                                        LOG.error(request.toString());
                                        LOG.error(response.toString());
                                    }
                                }
                            });
                        } catch (Throwable e) {
                            LOG.error("ProcessRequestWrapper process request over, but response failed", e);
                            LOG.error(request.toString());
                            LOG.error(response.toString());
                        }
                    }
                } catch (RemotingCommandException e1) {
                    LOG.error("ExecuteRequestWhenWakeup run", e1);
                }
            }
        };
        this.brokerController.getPullMessageExecutor().submit(new RequestTask(run, channel, request));
    }

}
