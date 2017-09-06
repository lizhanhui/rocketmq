package org.apache.rocketmq.client;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class Consumer {
	public static void main(String[] args) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
		String consumerGroup="C_longji";
		DefaultMQPullConsumer pullConsumer = new DefaultMQPullConsumer(consumerGroup);
		pullConsumer.setNamesrvAddr("127.0.0.1:9876");
		pullConsumer.start();
        String topic="longji1";
        String brokerName="broker-a";
		MessageQueue mq=new MessageQueue(topic, brokerName, -1);
		//PopResult popResult=pullConsumer.peekMessage(mq, 2, 1000);
		PopResult popResult=pullConsumer.pop(mq, 50000, 5, consumerGroup, 10000000);
		for (MessageExt msg : popResult.getMsgFoundList()) {
			pullConsumer.ackMessage(new MessageQueue(msg.getTopic(),brokerName,msg.getQueueId()), msg.getQueueOffset(), consumerGroup, msg.getProperty(MessageConst.KEY_SEPARATOR));
		}
		System.out.println(popResult);
	}
}
