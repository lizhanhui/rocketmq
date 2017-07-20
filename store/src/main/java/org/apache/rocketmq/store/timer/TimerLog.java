package org.apache.rocketmq.store.timer;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MappedFile;
import org.apache.rocketmq.store.MappedFileQueue;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimerLog {
    private static Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final static int BLANK_MAGIC_CODE = 0xBBCCDDEE ^ 1880681586 + 8;
    private final static int MIN_BLANK_LEN = 4 + 4;
    private final MappedFileQueue mappedFileQueue;

    public TimerLog(final MessageStoreConfig storeConfig) {
        this.mappedFileQueue = new MappedFileQueue(storeConfig.getStorePathRootDir() + File.separator + "timerlog",
            storeConfig.getMapedFileSizeCommitLog(), null);
        //TODO check allocate mapped file service
    }

    public long append(ByteBuffer buffer) {
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (null == mappedFile || mappedFile.isFull()) {
            mappedFile = this.mappedFileQueue.getLastMappedFile(0);
        }
        if (null == mappedFile) {
            log.error("Create mapped file1 error for timer log");
            return -1;
        }
        if (buffer.remaining() + MIN_BLANK_LEN > mappedFile.getFileSize() - mappedFile.getWrotePosition()) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(8);
            byteBuffer.putInt(mappedFile.getFileSize() - mappedFile.getWrotePosition());
            byteBuffer.putInt(BLANK_MAGIC_CODE);
            if (mappedFile.appendMessage(byteBuffer.array())) {
                //need to set the wrote position
                mappedFile.setWrotePosition(mappedFile.getFileSize());
            } else {
                log.error("Append blank error for timer log");
                return -1;
            }
            mappedFile = this.mappedFileQueue.getLastMappedFile(0);
            if (null == mappedFile) {
                log.error("create mapped file2 error for timer log");
                return -1;
            }
        }
        int currPosition = mappedFile.getWrotePosition();
        if (!mappedFile.appendMessage(buffer.array())) {
            log.error("Append error for timer log");
            return -1;
        }
        return currPosition;
    }

    public SelectMappedBufferResult getTimerMessage(long offsetPy) {
        MappedFile mappedFile = mappedFileQueue.findMappedFileByOffset(offsetPy);
        if (null == mappedFile) return null;
        return mappedFile.selectMappedBuffer((int) (offsetPy % mappedFile.getFileSize()));
    }

    public MappedFileQueue getMappedFileQueue() {
        return mappedFileQueue;
    }

}
