package org.apache.rocketmq.store.timer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import org.apache.rocketmq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimerWheel {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    public final int TTL_SECS;
    private String fileName;
    private ByteBuffer readBuffer;
    private ByteBuffer writeBuffer;

    public TimerWheel(String fileName, int ttlSecs) throws IOException {
        this.TTL_SECS = ttlSecs;
        this.fileName = fileName;
        init();
    }

    public void init() throws IOException {
        boolean ok = false;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(this.fileName, "rw");
            raf.setLength(TTL_SECS * 2 * Slot.SIZE);
            MappedByteBuffer buffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, TTL_SECS * 2 * Slot.SIZE);
            this.writeBuffer = buffer.duplicate();
            this.readBuffer = buffer.duplicate();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("create file channel " + this.fileName + " Failed. ", e);
            throw e;
        } catch (IOException e) {
            log.error("map file " + this.fileName + " Failed. ", e);
            throw e;
        } finally {
            if (!ok && null != raf) {
                raf.close();
            }
        }
    }


    public Slot getSlot(long timeSecs) {
        int slotIndex = (int)(timeSecs % (TTL_SECS * 2));
        readBuffer.position(slotIndex * Slot.SIZE);
        Slot slot = new Slot(readBuffer.getLong(), readBuffer.getLong(), readBuffer.getLong());
        if (slot.TIME_SECS != timeSecs) {
            return new Slot(-1, -1, -1);
        }
        return slot;
    }

    public void putSlot(long timeSecs, long firstPos, long lastPos){
        int slotIndex = (int)(timeSecs % (TTL_SECS * 2));
        writeBuffer.position(slotIndex * Slot.SIZE);
        writeBuffer.putLong(timeSecs);
        writeBuffer.putLong(firstPos);
        writeBuffer.putLong(lastPos);
    }

}
