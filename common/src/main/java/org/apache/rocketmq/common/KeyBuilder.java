package org.apache.rocketmq.common;

public class KeyBuilder {
	public static String buildPopRetryTopic(String topic,String cid) {
		//return topic;
		return MixAll.RETRY_GROUP_TOPIC_PREFIX+cid+"_"+topic;
	}
	public static String buildPollingKey(String topic, String cid, int queueId) {
		return topic + PopAckConstants.SPLIT + cid + PopAckConstants.SPLIT + queueId;
	}
}
