package org.apache.rocketmq.store.pop;

public class AckMsg {
	private long ackOffset;
	private long startOffset;
    private String consumerGroup;
    private String topic;
    private int queueId;
    private long popTime;
    public long getPopTime() {
		return popTime;
	}
    public void setPopTime(long popTime) {
		this.popTime = popTime;
	}
    public void setQueueId(int queueId) {
		this.queueId = queueId;
	}
    public int getQueueId() {
		return queueId;
	}
    public void setTopic(String topic) {
		this.topic = topic;
	}
    public String getTopic() {
		return topic;
	}
	public long getAckOffset() {
		return ackOffset;
	}
	public String getConsumerGroup() {
		return consumerGroup;
	}
	public void setConsumerGroup(String consumerGroup) {
		this.consumerGroup = consumerGroup;
	}
	public void setAckOffset(long ackOffset) {
		this.ackOffset = ackOffset;
	}
	public long getStartOffset() {
		return startOffset;
	}
	public void setStartOffset(long startOffset) {
		this.startOffset = startOffset;
	}
}
