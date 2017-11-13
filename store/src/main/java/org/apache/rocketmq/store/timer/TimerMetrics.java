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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimerMetrics extends ConfigManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final long LOCK_TIMEOUT_MILLIS = 3000;
    private transient final Lock lock = new ReentrantLock();

    private final ConcurrentMap<String, Metric> timingCount =
        new ConcurrentHashMap<>(1024);
    private final DataVersion dataVersion = new DataVersion();

    private final String configPath;

    public TimerMetrics(String configPath) {
        this.configPath =  configPath;
    }

    public long addAndGet(String topic, int value) {
        Metric pair = getPair(topic);
        getDataVersion().nextVersion();
        pair.setTimeStamp(System.currentTimeMillis());
        return pair.getCount().addAndGet(value);
    }

    public Metric getPair(String topic) {
        Metric pair = timingCount.get(topic);
        if (null == pair) {
            pair = new Metric();
            timingCount.putIfAbsent(topic, pair);
        }
        return pair;
    }

    public long getTimingCount(String topic) {
        Metric pair = timingCount.get(topic);
        if (null == pair) {
            return 0;
        } else {
           return pair.getCount().get();
        }
    }

    public Map<String, Metric> getTimingCount() {
        return timingCount;
    }
    @Override public String encode() {
        return encode(false);
    }

    @Override
    public String configFilePath() {
        return configPath;
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            TimerMetricsSerializeWrapper timerMetricsSerializeWrapper =
                TimerMetricsSerializeWrapper.fromJson(jsonString, TimerMetricsSerializeWrapper.class);
            if (timerMetricsSerializeWrapper != null) {
                this.timingCount.putAll(timerMetricsSerializeWrapper.getTimingCount());
                this.dataVersion.assignNewOne(timerMetricsSerializeWrapper.getDataVersion());
            }
        }
    }


    @Override
    public String encode(boolean prettyFormat) {
        TimerMetricsSerializeWrapper metricsSerializeWrapper = new TimerMetricsSerializeWrapper();
        metricsSerializeWrapper.setDataVersion(this.dataVersion);
        metricsSerializeWrapper.setTimingCount(this.timingCount);
        return metricsSerializeWrapper.toJson(prettyFormat);
    }


    public DataVersion getDataVersion() {
        return dataVersion;
    }


    public static class TimerMetricsSerializeWrapper extends RemotingSerializable {
        private ConcurrentMap<String, Metric> timingCount =
            new ConcurrentHashMap<>(1024);
        private DataVersion dataVersion = new DataVersion();

        public ConcurrentMap<String, Metric> getTimingCount() {
            return timingCount;
        }

        public void setTimingCount(
            ConcurrentMap<String, Metric> timingCount) {
            this.timingCount = timingCount;
        }

        public DataVersion getDataVersion() {
            return dataVersion;
        }

        public void setDataVersion(DataVersion dataVersion) {
            this.dataVersion = dataVersion;
        }
    }

    public static class Metric {
        private AtomicLong count;
        private long timeStamp;

        public Metric() {
            count = new AtomicLong(0);
            timeStamp = System.currentTimeMillis();
        }

        public AtomicLong getCount() {
            return count;
        }

        public void setCount(AtomicLong count) {
            this.count = count;
        }

        public long getTimeStamp() {
            return timeStamp;
        }

        public void setTimeStamp(long timeStamp) {
            this.timeStamp = timeStamp;
        }
        @Override
        public String toString() {
            return String.format("[%d,%d]", count.get(), timeStamp);
        }
    }

}
