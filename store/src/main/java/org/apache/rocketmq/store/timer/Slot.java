package org.apache.rocketmq.store.timer;

public class Slot {
    public static final short SIZE = 24;
    public final long TIME_SECS;
    public final long FIRST_POS;
    public final long LAST_POS;

    public Slot(long timeSecs, long firstPos, long lastPos) {
        this.TIME_SECS = timeSecs;
        this.FIRST_POS = firstPos;
        this.LAST_POS = lastPos;
    }
}
