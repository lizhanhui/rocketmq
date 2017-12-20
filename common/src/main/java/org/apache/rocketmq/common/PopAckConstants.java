package org.apache.rocketmq.common;


public class PopAckConstants {
	public static long ackTimeInterval = 1000;
	public static long SECOND = 1000;

	public static long lockTime = 5000;
	public static int retryQueueNum = 1;

	public static String REVIVE_GROUP = MixAll.CID_RMQ_SYS_PREFIX + "REVIVE_GROUP";
	public static String LOCAL_HOST = "127.0.0.1";
	public static final String REVIVE_TOPIC = MixAll.SYSTEM_TOPIC_PREFIX + "REVIVE_LOG_";
	public static final String CK_TAG = "ck";
	public static final String ACK_TAG = "ack";
	public static final String SPLIT = "@";

}
