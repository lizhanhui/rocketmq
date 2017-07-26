package org.apache.rocketmq.store.timer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TimerLogTest {

    private Set<String> baseDirs = new HashSet<>();
    private List<TimerLog> timerLogs = new ArrayList<>();

    public TimerLog createTimerLog(String baseDir) {
        if (null == baseDir) baseDir = StoreTestUtils.createBaseDir();
        TimerLog timerLog = new TimerLog(baseDir, 1024);
        timerLogs.add(timerLog);
        baseDirs.add(baseDir);
        return timerLog;
    }

    @Test
    public void testAppendRollSelectDelete() throws Exception {
        TimerLog timerLog =  createTimerLog(null);
        long ret  = timerLog.append(new byte[512]);
        assertEquals(0, ret);
        byte[] data = new byte[510];
        data[0] = 1;
        long ret2  = timerLog.append(data);
        assertEquals(1024, ret2);
        assertEquals(2, timerLog.getMappedFileQueue().getMappedFiles().size());
        SelectMappedBufferResult sbr = timerLog.getTimerMessage(ret2);
        assertNotNull(sbr);
        assertEquals(data.length, sbr.getSize());
        assertEquals(1, sbr.getByteBuffer().get());
        sbr.release();
        Thread.sleep(2000L);
        timerLog.cleanExpiredFiles(1000);
        assertEquals(1, timerLog.getMappedFileQueue().getMappedFiles().size());
    }


    @Test
    public void testRecovery() throws Exception {
        String basedir = StoreTestUtils.createBaseDir();
        TimerLog first =  createTimerLog(basedir);
        first.append(new byte[512]);
        first.append(new byte[510]);
        byte[] data = "Hello Recovery".getBytes();
        first.append(data);
        first.shutdown();
        TimerLog second =  createTimerLog(basedir);
        second.load();
        assertEquals(2, second.getMappedFileQueue().getMappedFiles().size());
        second.getMappedFileQueue().truncateDirtyFiles(1204 + 1000);
        SelectMappedBufferResult sbr =  second.getTimerMessage(1024 + 510);
        byte[] expect = new byte[data.length];
        sbr.getByteBuffer().get(expect);
        assertArrayEquals(expect, data);
    }


    @After
    public void shutdown() {
        for (TimerLog timerLog: timerLogs) {
            timerLog.shutdown();
            timerLog.getMappedFileQueue().destroy();
        }
        for (String baseDir : baseDirs) {
            StoreTestUtils.deleteFile(baseDir);
        }
    }
}
