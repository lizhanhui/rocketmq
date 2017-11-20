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

package org.apache.rocketmq.client.stat;

import java.util.concurrent.ScheduledExecutorService;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.body.ConsumeStatus;
import org.apache.rocketmq.common.stats.StatsItemSet;
import org.apache.rocketmq.common.stats.StatsSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerStatsManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.CLIENT_LOGGER_NAME);

    private static final String TOPIC_AND_GROUP_CONSUME_OK_TPS = "CONSUME_OK_TPS";
    private static final String TOPIC_AND_GROUP_CONSUME_FAILED_TPS = "CONSUME_FAILED_TPS";
    private static final String TOPIC_AND_GROUP_CONSUME_RT = "CONSUME_RT";
    private static final String TOPIC_AND_GROUP_PULL_TPS = "PULL_TPS";
    private static final String TOPIC_AND_GROUP_PULL_RT = "PULL_RT";

    private final StatsItemSet topicAndGroupConsumeOKTPS;
    private final StatsItemSet topicAndGroupConsumeRT;
    private final StatsItemSet topicAndGroupConsumeFailedTPS;
    private final StatsItemSet topicAndGroupPullTPS;
    private final StatsItemSet topicAndGroupPullRT;

    //smq
    private static final String SMQ_POP_RT = "SMQ_POP_RT";
    private static final String SMQ_POP_QPS = "SMQ_POP_QPS";
    private static final String SMQ_PEEK_RT = "SMQ_PEEK_RT";
    private static final String SMQ_PEEK_QPS = "SMQ_PEEK_QPS";
    private static final String SMQ_ACK_RT = "SMQ_ACK_RT";
    private static final String SMQ_ACK_QPS = "SMQ_ACK_QPS";
    private static final String SMQ_CHANGE_RT = "SMQ_CHANGE_RT";
    private static final String SMQ_CHANGE_QPS = "SMQ_CHANGE_QPS";

    private final StatsItemSet smqPopRT;
    private final StatsItemSet smqPopQPS;
    private final StatsItemSet smqPeekRT;
    private final StatsItemSet smqPeekQPS;
    private final StatsItemSet smqAckRT;
    private final StatsItemSet smqAckQPS;
    private final StatsItemSet smqChangeRT;
    private final StatsItemSet smqChangeQPS;

    public ConsumerStatsManager(final ScheduledExecutorService scheduledExecutorService) {
        this.topicAndGroupConsumeOKTPS =
            new StatsItemSet(TOPIC_AND_GROUP_CONSUME_OK_TPS, scheduledExecutorService, log);

        this.topicAndGroupConsumeRT =
            new StatsItemSet(TOPIC_AND_GROUP_CONSUME_RT, scheduledExecutorService, log);

        this.topicAndGroupConsumeFailedTPS =
            new StatsItemSet(TOPIC_AND_GROUP_CONSUME_FAILED_TPS, scheduledExecutorService, log);

        this.topicAndGroupPullTPS = new StatsItemSet(TOPIC_AND_GROUP_PULL_TPS, scheduledExecutorService, log);

        this.topicAndGroupPullRT = new StatsItemSet(TOPIC_AND_GROUP_PULL_RT, scheduledExecutorService, log);

        //smq
        this.smqPopRT = new StatsItemSet(SMQ_POP_RT, scheduledExecutorService, log);
        this.smqPopQPS = new StatsItemSet(SMQ_POP_QPS, scheduledExecutorService, log);
        this.smqPeekRT = new StatsItemSet(SMQ_PEEK_RT, scheduledExecutorService, log);
        this.smqPeekQPS = new StatsItemSet(SMQ_PEEK_QPS, scheduledExecutorService, log);
        this.smqAckRT = new StatsItemSet(SMQ_ACK_RT, scheduledExecutorService, log);
        this.smqAckQPS = new StatsItemSet(SMQ_ACK_QPS, scheduledExecutorService, log);
        this.smqChangeRT = new StatsItemSet(SMQ_CHANGE_RT, scheduledExecutorService, log);
        this.smqChangeQPS = new StatsItemSet(SMQ_CHANGE_QPS, scheduledExecutorService, log);
    }

    public void start() {
    }

    public void shutdown() {
    }

    public void incPullRT(final String group, final String topic, final long rt) {
        this.topicAndGroupPullRT.addValue(topic + "@" + group, (int) rt, 1);
    }

    public void incPullTPS(final String group, final String topic, final long msgs) {
        this.topicAndGroupPullTPS.addValue(topic + "@" + group, (int) msgs, 1);
    }

    public void incConsumeRT(final String group, final String topic, final long rt) {
        this.topicAndGroupConsumeRT.addValue(topic + "@" + group, (int) rt, 1);
    }

    public void incConsumeOKTPS(final String group, final String topic, final long msgs) {
        this.topicAndGroupConsumeOKTPS.addValue(topic + "@" + group, (int) msgs, 1);
    }

    public void incConsumeFailedTPS(final String group, final String topic, final long msgs) {
        this.topicAndGroupConsumeFailedTPS.addValue(topic + "@" + group, (int) msgs, 1);
    }

    public ConsumeStatus consumeStatus(final String group, final String topic) {
        ConsumeStatus cs = new ConsumeStatus();
        {
            StatsSnapshot ss = this.getPullRT(group, topic);
            if (ss != null) {
                cs.setPullRT(ss.getAvgpt());
            }
        }

        {
            StatsSnapshot ss = this.getPullTPS(group, topic);
            if (ss != null) {
                cs.setPullTPS(ss.getTps());
            }
        }

        {
            StatsSnapshot ss = this.getConsumeRT(group, topic);
            if (ss != null) {
                cs.setConsumeRT(ss.getAvgpt());
            }
        }

        {
            StatsSnapshot ss = this.getConsumeOKTPS(group, topic);
            if (ss != null) {
                cs.setConsumeOKTPS(ss.getTps());
            }
        }

        {
            StatsSnapshot ss = this.getConsumeFailedTPS(group, topic);
            if (ss != null) {
                cs.setConsumeFailedTPS(ss.getTps());
            }
        }

        {
            StatsSnapshot ss = this.topicAndGroupConsumeFailedTPS.getStatsDataInHour(topic + "@" + group);
            if (ss != null) {
                cs.setConsumeFailedMsgs(ss.getSum());
            }
        }

        return cs;
    }

    private StatsSnapshot getPullRT(final String group, final String topic) {
        return this.topicAndGroupPullRT.getStatsDataInMinute(topic + "@" + group);
    }

    private StatsSnapshot getPullTPS(final String group, final String topic) {
        return this.topicAndGroupPullTPS.getStatsDataInMinute(topic + "@" + group);
    }

    private StatsSnapshot getConsumeRT(final String group, final String topic) {
        StatsSnapshot statsData = this.topicAndGroupConsumeRT.getStatsDataInMinute(topic + "@" + group);
        if (0 == statsData.getSum()) {
            statsData = this.topicAndGroupConsumeRT.getStatsDataInHour(topic + "@" + group);
        }

        return statsData;
    }

    private StatsSnapshot getConsumeOKTPS(final String group, final String topic) {
        return this.topicAndGroupConsumeOKTPS.getStatsDataInMinute(topic + "@" + group);
    }

    private StatsSnapshot getConsumeFailedTPS(final String group, final String topic) {
        return this.topicAndGroupConsumeFailedTPS.getStatsDataInMinute(topic + "@" + group);
    }

    public void incSmqPopRT(final String brokerName, final String group, final String topic, final long rt) {
        this.smqPopRT.addValue(brokerName + ":" + topic + "@" + group, (int) rt, 1);
    }

    public void incSmqPopQPS(final String brokerName, final String group, final String topic) {
        this.smqPopQPS.addValue(brokerName + ":" + topic + "@" + group, 1, 1);
    }

    public void incSmqPeekRT(final String brokerName, final String group, final String topic, final long rt) {
        this.smqPeekRT.addValue(brokerName + ":" + topic + "@" + group, (int) rt, 1);
    }

    public void incSmqPeekQPS(final String brokerName, final String group, final String topic) {
        this.smqPeekQPS.addValue(brokerName + ":" + topic + "@" + group, 1, 1);
    }

    public void incSmqAckRT(final String brokerName, final String group, final String topic, final long rt) {
        this.smqAckRT.addValue(brokerName + ":" + topic + "@" + group, (int) rt, 1);
    }

    public void incSmqAckQPS(final String brokerName, final String group, final String topic) {
        this.smqAckQPS.addValue(brokerName + ":" + topic + "@" + group, 1, 1);
    }

    public void incSmqChangeRT(final String brokerName, final String group, final String topic, final long rt) {
        this.smqChangeRT.addValue(brokerName + ":" + topic + "@" + group, (int) rt, 1);
    }

    public void incSmqChangeQPS(final String brokerName, final String group, final String topic) {
        this.smqChangeQPS.addValue(brokerName + ":" + topic + "@" + group, 1, 1);
    }
}
