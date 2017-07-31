package org.apache.rocketmq.store.timer;

import java.io.File;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TimerCheckPointTest {

    private String BASE_DIR;
    @Before
    public void init() throws IOException {
        BASE_DIR = StoreTestUtils.createBaseDir();
    }

    @Test
    public void testCheckPoint() throws IOException {
        String baseSrc = BASE_DIR + File.separator + "timercheck";
        TimerCheckpoint first = new TimerCheckpoint(baseSrc);
        assertEquals(0, first.getLastReadTimeMs());
        assertEquals(0, first.getLastTimerLogFlushPos());
        assertEquals(0, first.getLastTimerQueueOffset());
        first.setLastReadTimeMs(1000);
        first.setLastTimerLogFlushPos(1100);
        first.setLastTimerQueueOffset(1200);
        first.shutdown();
        TimerCheckpoint second =  new TimerCheckpoint(baseSrc);
        assertEquals(1000, second.getLastReadTimeMs());
        assertEquals(1100, second.getLastTimerLogFlushPos());
        assertEquals(1200, second.getLastTimerQueueOffset());
    }

    @After
    public void shutdown() {
        if (null != BASE_DIR) {
            StoreTestUtils.deleteFile(BASE_DIR);
        }
    }
}
