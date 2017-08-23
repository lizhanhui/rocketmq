package org.apache.rocketmq.store.timer;

import org.apache.rocketmq.common.message.MessageExt;

public class TimerRequest {


    private final long offsetPy;
    private final int sizePy;
    private final long delayTime;

    private final long enqueueTime;
    private final int magic;
    private MessageExt msg;

    public TimerRequest(long offsetPy, int sizePy, long delayTime, long enqueueTime, int magic) {
        this(offsetPy, sizePy, delayTime, enqueueTime, magic, null);
    }

    public TimerRequest(long offsetPy, int sizePy, long delayTime, long enqueueTime, int magic, MessageExt msg) {
        this.offsetPy = offsetPy;
        this.sizePy =  sizePy;
        this.delayTime =  delayTime;
        this.enqueueTime = enqueueTime;
        this.magic = magic;
        this.msg = msg;
    }

    public long getOffsetPy() {
        return offsetPy;
    }

    public int getSizePy() {
        return sizePy;
    }

    public long getDelayTime() {
        return delayTime;
    }

    public long getEnqueueTime() {
        return enqueueTime;
    }

    public MessageExt getMsg() {
        return msg;
    }

    public void setMsg(MessageExt msg) {
        this.msg = msg;
    }

    public int getMagic() {
        return magic;
    }
}
