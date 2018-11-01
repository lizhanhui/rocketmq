package org.apache.rocketmq.store.timer;

import org.junit.Assert;
import org.junit.Test;

public class TimerMetricsTest {


    @Test
    public void testTimingCount() {
        String baseDir = StoreTestUtils.createBaseDir();

        TimerMetrics first = new TimerMetrics(baseDir);
        Assert.assertTrue(first.load());
        first.addAndGet("AAA", 1000);
        first.addAndGet("BBB", 2000);
        Assert.assertEquals(1000, first.getTimingCount("AAA"));
        Assert.assertEquals(2000, first.getTimingCount("BBB"));
        long curr = System.currentTimeMillis();
        Assert.assertTrue(first.getPair("AAA").getTimeStamp() > curr - 10);
        Assert.assertTrue(first.getPair("AAA").getTimeStamp() <= curr);
        first.persist();

        TimerMetrics second = new TimerMetrics(baseDir);
        Assert.assertTrue(second.load());
        Assert.assertEquals(1000, second.getTimingCount("AAA"));
        Assert.assertEquals(2000, second.getTimingCount("BBB"));
        Assert.assertTrue(second.getPair("BBB").getTimeStamp() > curr - 100);
        Assert.assertTrue(second.getPair("BBB").getTimeStamp() <= curr);
        second.persist();
        StoreTestUtils.deleteFile(baseDir);
    }
}
