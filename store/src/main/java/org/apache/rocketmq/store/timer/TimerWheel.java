package org.apache.rocketmq.store.timer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.store.MappedFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimerWheel {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    public static int BLANK = -1, IGNORE = -2;
    public final int TTL_SECS;
    private String fileName;
    private ByteBuffer readBuffer;
    private ByteBuffer writeBuffer;
    private final RandomAccessFile randomAccessFile;
    private final FileChannel fileChannel;
    private final MappedByteBuffer mappedByteBuffer;
    private final int LENGTH;

    public TimerWheel(String fileName, int ttlSecs) throws IOException {
        this.TTL_SECS = ttlSecs;
        this.fileName = fileName;
        this.LENGTH = TTL_SECS * 2 * Slot.SIZE;
        File file = new File(fileName);

        try {
            randomAccessFile = new RandomAccessFile(this.fileName, "rw");
            if (file.exists() && randomAccessFile.length() != LENGTH) {
                throw new RuntimeException(String.format("Timer wheel length:%d != expected:%s",
                    randomAccessFile.length(), LENGTH));
            }
            randomAccessFile.setLength(TTL_SECS * 2 * Slot.SIZE);
            fileChannel = randomAccessFile.getChannel();
            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, TTL_SECS * 2 * Slot.SIZE);
            this.writeBuffer = mappedByteBuffer.duplicate();
            this.readBuffer = mappedByteBuffer.duplicate();
        } catch (FileNotFoundException e) {
            log.error("create file channel " + this.fileName + " Failed. ", e);
            throw e;
        } catch (IOException e) {
            log.error("map file " + this.fileName + " Failed. ", e);
            throw e;
        }
    }


    public void shutdown() {
        this.flush();

        // unmap mappedByteBuffer
        MappedFile.clean(this.mappedByteBuffer);

        try {
            this.fileChannel.close();
        } catch (IOException e) {
            log.error("Shutdown error in timer wheel", e);
        }
    }

    public void flush() {
        this.mappedByteBuffer.force();
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

    public void reviseSlot(long timeSecs, long firstPos, long lastPos, boolean force){
        int slotIndex = (int)(timeSecs % (TTL_SECS * 2));
        writeBuffer.position(slotIndex * Slot.SIZE);

        if (timeSecs != writeBuffer.getLong()) {
            if (force) {
                putSlot(timeSecs, firstPos != IGNORE ? firstPos : lastPos, lastPos);
            }
        } else  {
            if (IGNORE != firstPos) {
                writeBuffer.putLong(firstPos);
            } else {
                writeBuffer.getLong();
            }
            if (IGNORE != lastPos) {
                writeBuffer.putLong(lastPos);
            }
        }
    }

    public long checkPhyPos(long timeSecs, long maxOffset) {
        long minFirst = Long.MAX_VALUE;
        int slotIndex = (int)(timeSecs % (TTL_SECS * 2));
        for (int i = 0; i < TTL_SECS * 2; i++) {
            slotIndex = (slotIndex + i) % (TTL_SECS * 2);
            writeBuffer.position(slotIndex * Slot.SIZE);
            if ((timeSecs + i) != writeBuffer.getLong()) {
                continue;
            }
            long first = writeBuffer.getLong();
            if (writeBuffer.getLong() > maxOffset) {
                if(first < minFirst) {
                    minFirst = first;
                }
            }
        }
        return  minFirst;
    }

}
