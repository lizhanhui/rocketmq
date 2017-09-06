package org.apache.rocketmq.client;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
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
		PopResult popResult=pullConsumer.peekMessage(mq, 2, 1000);
		System.out.println(popResult);
	}
}
