/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.timer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.store.MappedFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimerCheckpoint {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final RandomAccessFile randomAccessFile;
    private final FileChannel fileChannel;
    private final MappedByteBuffer mappedByteBuffer;
    private volatile long lastReadTimeMs = 0;
    private volatile long lastTimerLogFlushPos = 0;
    private volatile long lastTimerQueueOffset = 0;

    public TimerCheckpoint(final String scpPath) throws IOException {
        File file = new File(scpPath);
        MappedFile.ensureDirOK(file.getParent());
        boolean fileExists = file.exists();

        this.randomAccessFile = new RandomAccessFile(file, "rw");
        this.fileChannel = this.randomAccessFile.getChannel();
        this.mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, 0, MappedFile.OS_PAGE_SIZE);

        if (fileExists) {
            log.info("timer checkpoint file exists, " + scpPath);
            this.lastReadTimeMs = this.mappedByteBuffer.getLong(0);
            this.lastTimerLogFlushPos = this.mappedByteBuffer.getLong(8);
            this.lastTimerQueueOffset = this.mappedByteBuffer.getLong(16);

            log.info("timer checkpoint file lastReadTimeMs " + this.lastReadTimeMs + ", "
                + UtilAll.timeMillisToHumanString(this.lastReadTimeMs));
            log.info("timer checkpoint file lastTimerLogFlushPos " + this.lastTimerLogFlushPos);
            log.info("timer checkpoint file lastTimerQueueOffset " + this.lastTimerQueueOffset);
        } else {
            log.info("timer checkpoint file not exists, " + scpPath);
        }
    }

    public void shutdown() {
        this.flush();

        // unmap mappedByteBuffer
        MappedFile.clean(this.mappedByteBuffer);

        try {
            this.fileChannel.close();
        } catch (IOException e) {
            log.error("Shutdown error in timer check point", e);
        }
    }

    public void flush() {
        this.mappedByteBuffer.putLong(0, this.lastReadTimeMs);
        this.mappedByteBuffer.putLong(8, this.lastTimerLogFlushPos);
        this.mappedByteBuffer.putLong(16, this.lastTimerQueueOffset);
        this.mappedByteBuffer.force();
    }

    public long getLastReadTimeMs() {
        return lastReadTimeMs;
    }

    public void setLastReadTimeMs(long lastReadTimeMs) {
        this.lastReadTimeMs = lastReadTimeMs;
    }

    public long getLastTimerLogFlushPos() {
        return lastTimerLogFlushPos;
    }

    public void setLastTimerLogFlushPos(long lastTimerLogFlushPos) {
        this.lastTimerLogFlushPos = lastTimerLogFlushPos;
    }

    public long getLastTimerQueueOffset() {
        return lastTimerQueueOffset;
    }

    public void setLastTimerQueueOffset(long lastTimerQueueOffset) {
        this.lastTimerQueueOffset = lastTimerQueueOffset;
    }
}
