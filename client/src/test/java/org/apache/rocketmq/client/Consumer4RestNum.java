package org.apache.rocketmq.client;

import java.util.Date;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.consumer.AckCallback;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PopCallback;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class Consumer4RestNum {
	public static void main(String[] args) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
		final String consumerGroup="cid-longji";
		final ScheduledExecutorService executor=Executors.newScheduledThreadPool(10);
		final DefaultMQPullConsumer pullConsumer = new DefaultMQPullConsumer(consumerGroup+"1");
		// 觉音测试 10.101.93.75，日常环境 10.101.162.180
		pullConsumer.setNamesrvAddr("127.0.0.1:9876");
		pullConsumer.start();
		//SendResult [sendStatus=SEND_OK, msgId=1E056189586318B4AAC246B811320003, offsetMsgId=0A89542100002A9F00000005A659F74A, messageQueue=MessageQueue [topic=longji1, brokerName=xigutestdaily-01, queueId=1], queueOffset=0]
//SendResult [sendStatus=SEND_OK, msgId=1E0560847F5B2A139A5560FC2DEF0001, offsetMsgId=1E05608400002A9F00000000000062F8, messageQueue=MessageQueue [topic=longji1, brokerName=broker-a, queueId=3], queueOffset=4]

       // String topic="1_smq_abc"; b2b2d707-4b97-486d-b068-b18f33e97d27 ;longji-stress
		String topic="longji-stress";
        //final String brokerName="xigutestdaily-02";
        final String brokerName="broker-a";
		final MessageQueue mq=new MessageQueue(topic, brokerName, -1);
		PopResult popResult=pullConsumer.peekMessage(mq, 1000, consumerGroup, 1000);
		System.out.println("1st peek:"+popResult);
		//PopResult popResult=pullConsumer.peekMessage(mq, 2, 1000);
	    popResult=pullConsumer.pop(mq, 50000, 10, consumerGroup, 10000000,ConsumeInitMode.MAX);
		System.out.println("pop:"+popResult);
		if (popResult.getPopStatus() == PopStatus.FOUND) {
			for (final MessageExt msg : popResult.getMsgFoundList()) {
				System.out.println(new Date() + ",delay time:" + (System.currentTimeMillis() - msg.getBornTimestamp()) + " msg id:" + new String(msg.getBody()) + ",born time:"
						+ new Date(msg.getBornTimestamp()) + ",retry time:" + msg.getReconsumeTimes() + ",1st pop time:" + msg.getProperty(MessageConst.PROPERTY_FIRST_POP_TIME));
				pullConsumer.ackMessage(msg.getTopic(), consumerGroup, msg.getProperty(MessageConst.PROPERTY_POP_CK));
			}
		}
		popResult=pullConsumer.peekMessage(mq, 1000, consumerGroup, 1000);
		System.out.println("2nd peek:"+popResult);
			


		 
		
		Thread.sleep(10000000L);
		
	}
}
