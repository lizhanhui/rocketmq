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
		String topic = "xigutest";
		final String brokerName = "broker-a";
		final MessageQueue mq = new MessageQueue(topic, brokerName, 0);
		int counter = 0;
		int total = 5;
		while (total > 0) {
			Message msg = new Message(topic, "abc".getBytes());
			if (counter % 2 == 0) {
				msg.setTags("tag");
			}
//			if (counter % 100 == 0) {
				msg.putUserProperty("a", "1");
//			}
			SendResult result = producer.send(msg);
		    //SendResult result=producer.send(msg, mq);
			System.out.println(result);
			counter++;
			total--;
			if (counter > 100) {
				counter = 0;
				Thread.sleep(1000);
			}
		}

		producer.shutdown();
	}
}
