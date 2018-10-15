package org.apache.rocketmq.store.pop;

import com.alibaba.fastjson.JSON;

import java.util.ArrayList;
import java.util.List;

public class PopCheckPoint {
    long startOffset;
    long reviveTime;
    long popTime;
    long invisibleTime;
    int bitMap;
    byte num;
    byte queueId;
    String topic;
    String cid;
    long reviveOffset;
    List<Integer> queueOffsetDiff;

    public long getRo() {
        return reviveOffset;
    }

    public void setRo(long reviveOffset) {
        this.reviveOffset = reviveOffset;
    }

    public long getSo() {
        return startOffset;
    }

    public void setSo(long startOffset) {
        this.startOffset = startOffset;
    }

    public void setPt(long popTime) {
        this.popTime = popTime;
    }

    public void setIt(long invisibleTime) {
        this.invisibleTime = invisibleTime;
    }

    public long getPt() {
        return popTime;
    }

    public long getIt() {
        return invisibleTime;
    }

    public long getRt() {
        return popTime + invisibleTime;
    }

    public int getBm() {
        return bitMap;
    }

    public void setBm(int bitMap) {
        this.bitMap = bitMap;
    }

    public byte getN() {
        return num;
    }

    public void setN(byte num) {
        this.num = num;
    }

    public byte getQ() {
        return queueId;
    }

    public void setQ(byte queueId) {
        this.queueId = queueId;
    }

    public String getT() {
        return topic;
    }

    public void setT(String topic) {
        this.topic = topic;
    }

    public String getC() {
        return cid;
    }

    public void setC(String cid) {
        this.cid = cid;
    }

    public List<Integer> getD() {
        return queueOffsetDiff;
    }

    public void setD(List<Integer> queueOffsetDiff) {
        this.queueOffsetDiff = queueOffsetDiff;
    }

    public void addDiff(int diff) {
        if (this.queueOffsetDiff == null) {
            this.queueOffsetDiff = new ArrayList<>(8);
        }
        this.queueOffsetDiff.add(diff);
    }

    public int indexOfAck(long ackOffset) {
        if (ackOffset < startOffset) {
            return -1;
        }

        // old version of checkpoint
        if (queueOffsetDiff == null || queueOffsetDiff.isEmpty()) {

            if (ackOffset - startOffset < num) {
                return (int) (ackOffset - startOffset);
            }

            return -1;
        }

        // new version of checkpoint
        return queueOffsetDiff.indexOf((int) (ackOffset - startOffset));
    }

    public long ackOffsetByIndex(byte index) {
        // old version of checkpoint
        if (queueOffsetDiff == null || queueOffsetDiff.isEmpty()) {
            return startOffset + index;
        }

        return startOffset + queueOffsetDiff.get(index);
    }


    public static void main(String[] args) {
        PopCheckPoint ck = new PopCheckPoint();
        ck.setBm(0);
        ck.setN((byte) 1);
        ck.setPt(2);
        ck.setIt(3);
        ck.setSo(4);
        ck.setC("c");
        ck.setT("t");
        ck.setQ((byte) 5);
        ck.addDiff(123);
        String s = JSON.toJSONString(ck);
        System.out.println(s);
        PopCheckPoint point = JSON.parseObject(s, PopCheckPoint.class);
        System.out.println(JSON.toJSONString(point));

    }

    @Override
    public String toString() {
        return "PopCheckPoint [topic=" + topic + ", cid=" + cid + ", queueId=" + queueId + ", startOffset=" + startOffset + ", bitMap=" + bitMap + ", num=" + num + ", reviveTime=" + getRt()
            + ", reviveOffset=" + reviveOffset + ", diff=" + queueOffsetDiff + "]";
    }

}
