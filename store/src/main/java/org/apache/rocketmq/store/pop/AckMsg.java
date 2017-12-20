package org.apache.rocketmq.store.pop;

import com.alibaba.fastjson.JSON;

public class AckMsg {
	private long ackOffset;
	private long startOffset;
    private String consumerGroup;
    private String topic;
    private int queueId;
    private long popTime;
    public long getPt() {
		return popTime;
	}
    public void setPt(long popTime) {
		this.popTime = popTime;
	}
    public void setQ(int queueId) {
		this.queueId = queueId;
	}
    public int getQ() {
		return queueId;
	}
    public void setT(String topic) {
		this.topic = topic;
	}
    public String getT() {
		return topic;
	}
	public long getAo() {
		return ackOffset;
	}
	public String getC() {
		return consumerGroup;
	}
	public void setC(String consumerGroup) {
		this.consumerGroup = consumerGroup;
	}
	public void setAo(long ackOffset) {
		this.ackOffset = ackOffset;
	}
	public long getSo() {
		return startOffset;
	}
	public void setSo(long startOffset) {
		this.startOffset = startOffset;
	}
	public static void main(String[] args) {
		AckMsg ackMsg=new AckMsg();
		ackMsg.setAo(1);
		ackMsg.setSo(2);
		ackMsg.setC("c");
		ackMsg.setT("t");
		ackMsg.setQ(1);
		ackMsg.setPt(3);
		String s=JSON.toJSONString(ackMsg);
		System.out.println(s);
		AckMsg newAck=JSON.parseObject(s, AckMsg.class);
		System.out.println(JSON.toJSONString(newAck));
	}
}
