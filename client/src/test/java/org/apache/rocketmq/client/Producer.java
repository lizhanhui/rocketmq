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
        String producerGroupTemp="P_longji";
        DefaultMQProducer producer = new DefaultMQProducer(producerGroupTemp);
        producer.setNamesrvAddr("127.0.0.1:9876");
       // producer.setNamesrvAddr("10.137.84.33:9876");
        producer.start();
        String topic="longji10";
        final String brokerName="broker-a";
		final MessageQueue mq=new MessageQueue(topic, brokerName, -1);
        for (int i = 0; i < 10; i++) { 
        	Message msg=new Message(topic, new byte[]{1,1});
    		SendResult result=producer.send(msg);
    		//SendResult result=producer.send(msg, mq);
    		System.out.println(result);
		}  
		
	}
}
