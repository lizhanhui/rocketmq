package org.apache.rocketmq.client;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class Producer {
	public static void main(String[] args) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
		String producerGroupTemp = "P_longji";
		DefaultMQProducer producer = new DefaultMQProducer(producerGroupTemp);
		// 觉音测试 10.101.93.75，日常环境 10.101.162.180
		producer.setNamesrvAddr("127.0.0.1:9876");
		// producer.setNamesrvAddr("10.137.84.33:9876");
		producer.start();
		String topic = "longji-stress";
		final String brokerName = "broker-a";
		final MessageQueue mq = new MessageQueue(topic, brokerName, 0);
		for (int i = 0; i < 5; i++) {
			Message msg = new Message(topic, String.valueOf(i).getBytes());
			//SendResult result = producer.send(msg);
		    SendResult result=producer.send(msg, mq);
			System.out.println(result);
		}

	}
}
