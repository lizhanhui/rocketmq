package org.apache.rocketmq.store.pop;

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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AckMsg{");
        sb.append("ackOffset=").append(ackOffset);
        sb.append(", startOffset=").append(startOffset);
        sb.append(", consumerGroup='").append(consumerGroup).append('\'');
        sb.append(", topic='").append(topic).append('\'');
        sb.append(", queueId=").append(queueId);
        sb.append(", popTime=").append(popTime);
        sb.append('}');
        return sb.toString();
    }
}
