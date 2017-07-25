package org.apache.rocketmq.store.timer;

import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import  static  org.junit.Assert.*;


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
        Slot first =  timerWheel.getSlot(delayedTime/1000);
        assertEquals(-1, first.TIME_SECS);
        assertEquals(-1, first.FIRST_POS);
        assertEquals(-1, first.LAST_POS);
        timerWheel.putSlot(delayedTime/1000, 1, 2);
        Slot second = timerWheel.getSlot(delayedTime/1000);
        assertEquals(delayedTime/1000, second.TIME_SECS);
        assertEquals(1, second.FIRST_POS);
        assertEquals(2, second.LAST_POS);
    }

    @Test
    public void testPutRevise() {
        long delayedTime = System.currentTimeMillis() + 3000;
        timerWheel.putSlot(delayedTime/1000, 1, 2);
        timerWheel.reviseSlot(delayedTime/1000 + 1, 3, 4, false);
        Slot second = timerWheel.getSlot(delayedTime/1000);
        assertEquals(delayedTime/1000, second.TIME_SECS);
        assertEquals(1, second.FIRST_POS);
        assertEquals(2, second.LAST_POS);
        timerWheel.reviseSlot(delayedTime/1000, TimerWheel.IGNORE, 4, false);
        Slot three = timerWheel.getSlot(delayedTime/1000);
        assertEquals(1, three.FIRST_POS);
        assertEquals(4, three.LAST_POS);
        timerWheel.reviseSlot(delayedTime/1000, 3, TimerWheel.IGNORE, false);
        Slot four = timerWheel.getSlot(delayedTime/1000);
        assertEquals(3, four.FIRST_POS);
        assertEquals(4, four.LAST_POS);
        timerWheel.reviseSlot(delayedTime/1000 + 2 * TTL_SECS, TimerWheel.IGNORE, 5, true);
        Slot five = timerWheel.getRawSlot(delayedTime/1000);
        assertEquals(delayedTime/1000 + 2 * TTL_SECS, five.TIME_SECS);
        assertEquals(5, five.FIRST_POS);
        assertEquals(5, five.LAST_POS);
    }

    @Test
    public void testRecoveryData() throws Exception {
        long delayedTime = System.currentTimeMillis() + 5000;
        timerWheel.putSlot(delayedTime/1000, 1, 2);
        timerWheel.flush();
        TimerWheel tmpWheel = new TimerWheel(BASE_DIR, TTL_SECS);
        Slot slot = tmpWheel.getSlot(delayedTime/1000);
        assertEquals(delayedTime/1000, slot.TIME_SECS);
        assertEquals(1, slot.FIRST_POS);
        assertEquals(2, slot.LAST_POS);
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
