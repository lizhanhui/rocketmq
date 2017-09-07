package org.apache.rocketmq.client;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class Producer {
	public static void main(String[] args) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        String producerGroupTemp="P_longji";
        DefaultMQProducer producer = new DefaultMQProducer(producerGroupTemp);
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();
        String topic="longji1";
        for (int i = 0; i < 4; i++) {
        	Message msg=new Message(topic, new byte[]{1,1});
    		SendResult result=producer.send(msg);
    		System.out.println(result);
		} 
		
	}
}
