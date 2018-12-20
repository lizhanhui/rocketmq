package org.apache.rocketmq.store.timer;

import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TimerWheelTest {

    private String BASE_DIR;
    private int TTL_SECS = 30;
    private TimerWheel timerWheel;
    private long defaultDelay = System.currentTimeMillis();

    @Before
    public void init() throws IOException {
        BASE_DIR = StoreTestUtils.createBaseDir();
        timerWheel = new TimerWheel(BASE_DIR, TTL_SECS);
    }

    @Test
    public void testPutGet() {
        long delayedTime = defaultDelay + 1000;
        Slot first = timerWheel.getSlot(delayedTime / 1000);
        assertEquals(-1, first.timeSecs);
        assertEquals(-1, first.firstPos);
        assertEquals(-1, first.lastPos);
        timerWheel.putSlot(delayedTime / 1000, 1, 2, 3, 4);
        Slot second = timerWheel.getSlot(delayedTime / 1000);
        assertEquals(delayedTime / 1000, second.timeSecs);
        assertEquals(1, second.firstPos);
        assertEquals(2, second.lastPos);
        assertEquals(3, second.num);
        assertEquals(4, second.magic);
    }

    @Test
    public void testGetNum() {
        long delayedTime = defaultDelay + 1000;
        timerWheel.putSlot(delayedTime / 1000, 1, 2, 3, 4);
        assertEquals(3, timerWheel.getNum(delayedTime/1000));
        assertEquals(3, timerWheel.getAllNum(delayedTime/1000));
        timerWheel.putSlot(delayedTime / 1000 + 5, 5, 6, 7, 8);
        assertEquals(7, timerWheel.getNum(delayedTime/1000 + 5));
        assertEquals(10, timerWheel.getAllNum(delayedTime/1000));

    }

    @Test
    public void testCheckPhyPos() {
        long delayedTime = defaultDelay + 1000;
        timerWheel.putSlot(delayedTime / 1000, 1, 100, 1, 0);
        timerWheel.putSlot(delayedTime / 1000 + 5, 2, 200, 2, 0);
        timerWheel.putSlot(delayedTime / 1000 + 10, 3, 300, 3, 0);

        assertEquals(1, timerWheel.checkPhyPos(delayedTime/1000, 50));
        assertEquals(2, timerWheel.checkPhyPos(delayedTime/1000, 100));
        assertEquals(3, timerWheel.checkPhyPos(delayedTime/1000, 200));
        assertEquals(Long.MAX_VALUE, timerWheel.checkPhyPos(delayedTime/1000, 300));
        assertEquals(Long.MAX_VALUE, timerWheel.checkPhyPos(delayedTime/1000, 400));

        assertEquals(2, timerWheel.checkPhyPos(delayedTime/1000 + 5, 50));
        assertEquals(2, timerWheel.checkPhyPos(delayedTime/1000 + 5, 100));
        assertEquals(3, timerWheel.checkPhyPos(delayedTime/1000 + 5, 200));
        assertEquals(Long.MAX_VALUE, timerWheel.checkPhyPos(delayedTime/1000 + 5, 300));
        assertEquals(Long.MAX_VALUE, timerWheel.checkPhyPos(delayedTime/1000 + 5, 400));

    }

    @Test
    public void testPutRevise() {
        long delayedTime = System.currentTimeMillis() + 3000;
        timerWheel.putSlot(delayedTime / 1000, 1, 2);
        timerWheel.reviseSlot(delayedTime / 1000 + 1, 3, 4, false);
        Slot second = timerWheel.getSlot(delayedTime / 1000);
        assertEquals(delayedTime / 1000, second.timeSecs);
        assertEquals(1, second.firstPos);
        assertEquals(2, second.lastPos);
        timerWheel.reviseSlot(delayedTime / 1000, TimerWheel.IGNORE, 4, false);
        Slot three = timerWheel.getSlot(delayedTime / 1000);
        assertEquals(1, three.firstPos);
        assertEquals(4, three.lastPos);
        timerWheel.reviseSlot(delayedTime / 1000, 3, TimerWheel.IGNORE, false);
        Slot four = timerWheel.getSlot(delayedTime / 1000);
        assertEquals(3, four.firstPos);
        assertEquals(4, four.lastPos);
        timerWheel.reviseSlot(delayedTime / 1000 + 2 * TTL_SECS, TimerWheel.IGNORE, 5, true);
        Slot five = timerWheel.getRawSlot(delayedTime / 1000);
        assertEquals(delayedTime / 1000 + 2 * TTL_SECS, five.timeSecs);
        assertEquals(5, five.firstPos);
        assertEquals(5, five.lastPos);
    }

    @Test
    public void testRecoveryData() throws Exception {
        long delayedTime = System.currentTimeMillis() + 5000;
        timerWheel.putSlot(delayedTime / 1000, 1, 2, 3, 4);
        timerWheel.flush();
        TimerWheel tmpWheel = new TimerWheel(BASE_DIR, TTL_SECS);
        Slot slot = tmpWheel.getSlot(delayedTime / 1000);
        assertEquals(delayedTime / 1000, slot.timeSecs);
        assertEquals(1, slot.firstPos);
        assertEquals(2, slot.lastPos);
        assertEquals(3, slot.num);
        assertEquals(4, slot.magic);
        tmpWheel.shutdown();
    }

    @Test(expected = RuntimeException.class)
    public void testRecoveryFixedTTL() throws Exception {
        timerWheel.flush();
        TimerWheel tmpWheel = new TimerWheel(BASE_DIR, TTL_SECS + 1);
    }

    @After
    public void shutdown() {
        if (null != timerWheel) {
            timerWheel.shutdown();
        }
        if (null != BASE_DIR) {
            StoreTestUtils.deleteFile(BASE_DIR);
        }
    }


}
