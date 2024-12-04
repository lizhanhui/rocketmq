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

package org.apache.rocketmq.broker.config.v2;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.config.AbstractRocksDBStorage;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroupCommitOffsetService extends ServiceThread {

    private static final Logger LOGGER = LoggerFactory.getLogger(GroupCommitOffsetService.class);

    private final ConsumerOffsetManagerV2 offsetManager;

    public GroupCommitOffsetService(ConsumerOffsetManagerV2 offsetManager) {
        this.offsetManager = offsetManager;
    }

    @Override
    public String getServiceName() {
        return "GroupCommitOffsetService";
    }

    @Override
    public void run() {
        log.info("{} service started", this.getServiceName());
        while (!this.isStopped()) {
            try {
                this.waitForRunning(10);
                this.groupCommitOffset();
            } catch (Exception e) {
                log.warn("{} service has exception. ", this.getServiceName(), e);
            }
        }
        log.info("{} service end", this.getServiceName());
    }

    private void groupCommitOffset() {
        groupCommitOffset0(offsetManager.inflightConsumerOffset, TableId.CONSUMER_OFFSET);
        groupCommitOffset0(offsetManager.inflightPullOffset, TableId.PULL_OFFSET);
    }

    private void groupCommitOffset0(ConcurrentMap<String, ConcurrentMap<String, ConcurrentMap<Byte, Long>>> cache,
        TableId tableId) {
        if (cache.isEmpty()) {
            return;
        }
        List<ByteBuf> buffers = new ArrayList<>();
        try (WriteBatch writeBatch = new WriteBatch()) {
            for (Map.Entry<String, ConcurrentMap<String, ConcurrentMap<Byte, Long>>> groupEntry : cache.entrySet()) {
                String group = groupEntry.getKey();
                // Data Racing Safety:
                // It is possible other threads are still writing topicMap while we are iterating and some updates
                // therefore are lost. Considering the consequence is message duplication only and this would not violate
                // at-least-once semantics
                ConcurrentMap<String, ConcurrentMap<Byte, Long>> topicMap = cache.remove(group);
                for (Map.Entry<String, ConcurrentMap<Byte, Long>> topicEntry : topicMap.entrySet()) {
                    String topic = topicEntry.getKey();
                    ConcurrentMap<Byte, Long> offsetMap = topicEntry.getValue();
                    for (Map.Entry<Byte, Long> offsetEntry : offsetMap.entrySet()) {
                        byte queueId = offsetEntry.getKey();
                        long offset = offsetMap.remove(offsetEntry.getKey());
                        ByteBuf keyBuf = offsetManager.keyOfOffset(group, topic, queueId, tableId);
                        ByteBuf valueBuf = AbstractRocksDBStorage.POOLED_ALLOCATOR.buffer(8);
                        valueBuf.writeLong(offset);
                        writeBatch.put(keyBuf.nioBuffer(), valueBuf.nioBuffer());
                        buffers.add(keyBuf);
                        buffers.add(valueBuf);
                    }
                }
            }
            BrokerController brokerController = offsetManager.getBrokerController();
            long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
            ConfigHelper.stampDataVersion(writeBatch, tableId, offsetManager.getDataVersion(), stateMachineVersion);
            offsetManager.configStorage.write(writeBatch);
        } catch (RocksDBException e) {
            LOGGER.error("Failed to commit {}", tableId.name(), e);
        } finally {
            for (ByteBuf buffer : buffers) {
                buffer.release();
            }
        }
    }
}
