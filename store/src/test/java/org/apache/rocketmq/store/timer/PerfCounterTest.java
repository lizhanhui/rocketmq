package org.apache.rocketmq.store.timer;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PerfCounterTest {

    @Test
    public void testCount() {
        PerfCounter pc =  new PerfCounter();
        int num = 10000;
        for (int i = 0; i < num; i++) {
            pc.flow(i);
        }
        assertTrue(pc.getMax() > 9500 && pc.getMax() < 10500);
        assertTrue(pc.getMin() >= 0 && pc.getMin() < 500);
        assertEquals(num, pc.getCount(0, 10000));
        assertTrue(pc.getCount(1000, 2000) > 500 && pc.getCount(1000, 2000) < 1500);
        assertTrue(pc.getRate(3000, 4000) > 5 && pc.getRate(3000, 4000) < 15);
        assertTrue(pc.getTPValue(0.80f) > 7500 && pc.getTPValue(0.8f) < 8500);
    }

    @Test
    public void testTick() throws Exception {
        PerfCounter pc =  new PerfCounter();
        for (int i = 0; i < 100; i++) {
            pc.startTick();
            Thread.sleep(1);
            pc.endTick();
        }
        assertEquals(1, pc.getTPValue(0.5f));
        assertEquals(1, pc.getMin());
        assertTrue(pc.getMax() < 10);
    }

    @Test
    public void testTicks() throws Exception {
        PerfCounter.Ticks ticks = new PerfCounter.Ticks();
        for (int i = 0; i < 100; i++) {
            String key = "key1";
            if (i % 2 == 0) {
                key = "key2";
            }
            ticks.startTick(key);
            Thread.sleep(1);
            ticks.endTick(key);
        }
        assertEquals(1, ticks.getCounter("key1").getTPValue(0.5f));
        assertEquals(1,ticks.getCounter("key1").getMin());
        assertTrue(ticks.getCounter("key1").getMax() < 10);
        assertEquals(1, ticks.getCounter("key2").getTPValue(0.5f));
        assertEquals(1,ticks.getCounter("key2").getMin());
        assertTrue(ticks.getCounter("key2").getMax() < 10);
    }
}
