package org.apache.rocketmq.store.pop;

import org.apache.rocketmq.common.MixAll;

public class PopAckConstants {
	public static long ackTimeInterval=1000;
	public static long scanTime=10000;

	public static String REVIVE_GROUP=MixAll.CID_RMQ_SYS_PREFIX+"REVIVE_GROUP";
	public static String LOCAL_HOST="127.0.0.1";
    public static final String REVIVE_TOPIC = "REVIVE_LOG_";
    public static final int REVIVE_QUEUE_NUM = 2;
    public static final String CK_TAG = "ck";
    public static final String ACK_TAG = "ack";

}
