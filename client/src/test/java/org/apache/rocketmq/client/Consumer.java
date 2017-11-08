package org.apache.rocketmq.client;

import java.util.Date;

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

public class Consumer {
	public static void main(String[] args) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
		final String consumerGroup="C_longji1";
		final DefaultMQPullConsumer pullConsumer = new DefaultMQPullConsumer(consumerGroup+"1");
		pullConsumer.setNamesrvAddr("127.0.0.1:9876");
		//pullConsumer.setNamesrvAddr("10.137.84.33:9876");
		pullConsumer.start();
		//SendResult [sendStatus=SEND_OK, msgId=1E056189586318B4AAC246B811320003, offsetMsgId=0A89542100002A9F00000005A659F74A, messageQueue=MessageQueue [topic=longji1, brokerName=xigutestdaily-01, queueId=1], queueOffset=0]
//SendResult [sendStatus=SEND_OK, msgId=1E0560847F5B2A139A5560FC2DEF0001, offsetMsgId=1E05608400002A9F00000000000062F8, messageQueue=MessageQueue [topic=longji1, brokerName=broker-a, queueId=3], queueOffset=4]

       // String topic="1_smq_abc";
		String topic="longji11";
        final String brokerName="broker-a";
		final MessageQueue mq=new MessageQueue(topic, brokerName, -1);
		/*PopResult popResult=pullConsumer.peekMessage(mq, 1000, 1000);
		System.out.println("sync peek:"+popResult);
		pullConsumer.peekAsync(mq, 2, consumerGroup, 1000, new PopCallback() {
			
			@Override
			public void onSuccess(PopResult popResult) {
				System.out.println("async peek:"+popResult);
				
			}
			
			@Override
			public void onException(Throwable e) {
				// TODO Auto-generated method stub
				e.printStackTrace();
				
			}
		});*/
		//PopResult popResult=pullConsumer.peekMessage(mq, 2, 1000);
		//popResult=pullConsumer.pop(mq, 50000, 4, consumerGroup, 10000000,ConsumeInitMode.MAX);
		//System.out.println("sync pop:"+popResult);
		 
		final PopCallback callback=new PopCallback() {
			
			@Override
			public void onSuccess(PopResult popResult) {
				System.out.println("async pop:"+popResult);

				try {
					//System.out.println(new Date()+"     "+popResult);
					if (popResult.getPopStatus()==PopStatus.FOUND) {
						for (MessageExt msg : popResult.getMsgFoundList()) {
							System.out.println(new Date()+",delay time:"+(System.currentTimeMillis()-msg.getBornTimestamp())+" msg id:"+new String(msg.getBody())+",born time:"+ new Date(msg.getBornTimestamp())+",retry time:"+msg.getReconsumeTimes());
							//pullConsumer.changeInvisibleTime(new MessageQueue(msg.getTopic(),brokerName,msg.getQueueId()), msg.getQueueOffset(), consumerGroup, msg.getProperty(MessageConst.PROPERTY_POP_CK), 30000);
							pullConsumer.ackMessage(new MessageQueue(msg.getTopic(),brokerName,msg.getQueueId()), msg.getQueueOffset(), consumerGroup, msg.getProperty(MessageConst.PROPERTY_POP_CK));
							
							/*pullConsumer.ackMessageAsync(new MessageQueue(msg.getTopic(),brokerName,msg.getQueueId()),  msg.getQueueOffset(), consumerGroup, msg.getProperty(MessageConst.PROPERTY_POP_CK), 1000, new AckCallback() {
								
								@Override
								public void onSuccess(AckResult ackResult) {
									System.out.println(ackResult);
								}
								
								@Override
								public void onException(Throwable e) {
									// TODO Auto-generated method stub
									
								}
							});*/
						}
					}
					PopCallback tmPopCallback=this;
					pullConsumer.popAsync(mq, 10000, 5, consumerGroup, 30000,  tmPopCallback, true,ConsumeInitMode.MIN);
				} catch (Exception e) {
					e.printStackTrace();
				}

			}
			
			@Override
			public void onException(Throwable e) {
				e.printStackTrace();
				
			}
		};
		pullConsumer.popAsync(mq, 10000, 5, consumerGroup, 30000, callback,true,ConsumeInitMode.MIN);
		Thread.sleep(10000000L);
		
	}
}
