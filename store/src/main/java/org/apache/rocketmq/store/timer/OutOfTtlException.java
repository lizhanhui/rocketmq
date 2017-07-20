package org.apache.rocketmq.store.timer;

public class OutOfTtlException extends RuntimeException {

    public OutOfTtlException(long timeSecs, long ttlSecs, long currSecs) {
        super(String.format("timeSecs:%d ttlSecs:%d currSecs:%d", timeSecs, ttlSecs, currSecs));
    }
}
