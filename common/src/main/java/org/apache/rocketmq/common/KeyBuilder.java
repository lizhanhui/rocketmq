package org.apache.rocketmq.common;

public class KeyBuilder {
	public static String buildPopRetryTopic(String topic, String cid) {
		return MixAll.RETRY_GROUP_TOPIC_PREFIX + cid + "_" + topic;
	}
	
	public static String parseNormalTopic(String topic, String cid) {
		if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
			return topic.substring((MixAll.RETRY_GROUP_TOPIC_PREFIX + cid + "_").length());
		} else {
			return topic;
		}
	}

	public static String buildPollingKey(String topic, String cid, int queueId) {
		return topic + PopAckConstants.SPLIT + cid + PopAckConstants.SPLIT + queueId;
	}
	public static String buildPollingNotificationKey(String topic, int queueId) {
		return topic + PopAckConstants.SPLIT + queueId;
	}
}
