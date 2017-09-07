package org.apache.rocketmq.store.pop;

public class PopCheckPoint {
	Long startOffset;
	Long reviveTime;
	Integer bitMap;
	byte num;
	byte queueId;
	String topic;
	String cid;
	Long reviveOffset;
	public Long getReviveOffset() {
		return reviveOffset;
	}
	public void setReviveOffset(Long reviveOffset) {
		this.reviveOffset = reviveOffset;
	}
	public Long getStartOffset() {
		return startOffset;
	}
	public void setStartOffset(Long startOffset) {
		this.startOffset = startOffset;
	}
	public Long getReviveTime() {
		return reviveTime;
	}
	public void setReviveTime(Long reviveTime) {
		this.reviveTime = reviveTime;
	}
	public Integer getBitMap() {
		return bitMap;
	}
	public void setBitMap(Integer bitMap) {
		this.bitMap = bitMap;
	}
	public byte getNum() {
		return num;
	}
	public void setNum(byte num) {
		this.num = num;
	}
	public byte getQueueId() {
		return queueId;
	}
	public void setQueueId(byte queueId) {
		this.queueId = queueId;
	}

	public String getTopic() {
		return topic;
	}
	public void setTopic(String topic) {
		this.topic = topic;
	}
	public String getCid() {
		return cid;
	}
	public void setCid(String cid) {
		this.cid = cid;
	}

}
