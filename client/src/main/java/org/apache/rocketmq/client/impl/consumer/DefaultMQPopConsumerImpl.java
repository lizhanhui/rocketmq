package org.apache.rocketmq.client.impl.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.MQPopConsumer;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.common.protocol.header.PeekMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * a consumer support pop and ack mechanism.
 * @author longji.lqs
 *
 */
public class DefaultMQPopConsumerImpl extends DefaultMQPullConsumerImpl implements MQPopConsumer{

	public DefaultMQPopConsumerImpl(DefaultMQPullConsumer defaultMQPullConsumer, RPCHook rpcHook) {
		super(defaultMQPullConsumer, rpcHook);
	}

	@Override
	public PopResult pop(MessageQueue mq, long invisibleTime, int maxNums, String consumerGroup, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
		FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
		if (null == findBrokerResult) {
			this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
			findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
		}
		if (findBrokerResult != null) {
			PopMessageRequestHeader requestHeader = new PopMessageRequestHeader();
			requestHeader.setConsumerGroup(consumerGroup);
			requestHeader.setTopic(mq.getTopic());
			requestHeader.setQueueId(mq.getQueueId());
			requestHeader.setMaxMsgNums(maxNums);
			String brokerAddr = findBrokerResult.getBrokerAddr();
			PopResult popResult = this.mQClientFactory.getMQClientAPIImpl().popMessage(brokerAddr, requestHeader, timeout);

			return popResult;
		}
		throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
	}

	@Override
	public PopResult peek(MessageQueue mq, int maxNums, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
		FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
		if (null == findBrokerResult) {
			this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
			findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
		}
		if (findBrokerResult != null) {
			PeekMessageRequestHeader requestHeader = new PeekMessageRequestHeader();
			requestHeader.setTopic(mq.getTopic());
			requestHeader.setQueueId(mq.getQueueId());
			requestHeader.setMaxMsgNums(maxNums);
			String brokerAddr = findBrokerResult.getBrokerAddr();
			PopResult popResult = this.mQClientFactory.getMQClientAPIImpl().peekMessage(brokerAddr, requestHeader, timeout);
			return popResult;
		}
		throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
	}

	@Override
	public void ack(MessageQueue mq, long offset, String consumerGroup, String extraInfo) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
		FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
		if (null == findBrokerResult) {
			this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
			findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
		}
		if (findBrokerResult != null) {
			AckMessageRequestHeader requestHeader = new AckMessageRequestHeader();
			requestHeader.setTopic(mq.getTopic());
			requestHeader.setQueueId(mq.getQueueId());
			requestHeader.setOffset(offset);
			requestHeader.setConsumerGroup(consumerGroup);
			requestHeader.setExtraInfo(extraInfo);
			String brokerAddr = findBrokerResult.getBrokerAddr();
			this.mQClientFactory.getMQClientAPIImpl().ackMessage(brokerAddr, requestHeader);
		}
		throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
	}

	@Override
	public void changeInvisibleTime(MessageQueue mq, long offset, String consumerGroup, String extraInfo, long invisibleTime)
			throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
		FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
		if (null == findBrokerResult) {
			this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
			findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
		}
		if (findBrokerResult != null) {
			ChangeInvisibleTimeRequestHeader requestHeader = new ChangeInvisibleTimeRequestHeader();
			requestHeader.setTopic(mq.getTopic());
			requestHeader.setQueueId(mq.getQueueId());
			requestHeader.setOffset(offset);
			requestHeader.setConsumerGroup(consumerGroup);
			requestHeader.setExtraInfo(extraInfo);
			requestHeader.setInvisibleTime(invisibleTime);
			String brokerAddr = findBrokerResult.getBrokerAddr();
			this.mQClientFactory.getMQClientAPIImpl().changeInvisibleTime(brokerAddr, requestHeader);
		}
		throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
		
	}

}
