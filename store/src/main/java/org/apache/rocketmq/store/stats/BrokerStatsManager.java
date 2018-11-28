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
package org.apache.rocketmq.store.stats;

import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.statistics.StatisticsItemFormatter;
import org.apache.rocketmq.common.statistics.StatisticsItemPrinter;
import org.apache.rocketmq.common.statistics.StatisticsItemScheduledIncrementPrinter;
import org.apache.rocketmq.common.statistics.StatisticsKindMeta;
import org.apache.rocketmq.common.statistics.StatisticsManager;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.stats.MomentStatsItemSet;
import org.apache.rocketmq.common.stats.StatsItem;
import org.apache.rocketmq.common.stats.StatsItemSet;

public class BrokerStatsManager {

    public static final String TOPIC_PUT_NUMS = "TOPIC_PUT_NUMS";
    public static final String TOPIC_PUT_SIZE = "TOPIC_PUT_SIZE";
    public static final String GROUP_GET_NUMS = "GROUP_GET_NUMS";
    public static final String GROUP_GET_SIZE = "GROUP_GET_SIZE";
    public static final String SNDBCK_PUT_NUMS = "SNDBCK_PUT_NUMS";
    public static final String DLQ_PUT_NUMS = "DLQ_PUT_NUMS";
    public static final String BROKER_PUT_NUMS = "BROKER_PUT_NUMS";
    public static final String BROKER_GET_NUMS = "BROKER_GET_NUMS";
    public static final String GROUP_GET_FROM_DISK_NUMS = "GROUP_GET_FROM_DISK_NUMS";
    public static final String GROUP_GET_FROM_DISK_SIZE = "GROUP_GET_FROM_DISK_SIZE";
    public static final String BROKER_GET_FROM_DISK_NUMS = "BROKER_GET_FROM_DISK_NUMS";
    public static final String BROKER_GET_FROM_DISK_SIZE = "BROKER_GET_FROM_DISK_SIZE";

    public static final String SNDBCK2DLQ_TIMES = "SNDBCK2DLQ_TIMES";

    // For commercial
    public static final String COMMERCIAL_SEND_TIMES = "COMMERCIAL_SEND_TIMES";
    public static final String COMMERCIAL_SNDBCK_TIMES = "COMMERCIAL_SNDBCK_TIMES";
    public static final String COMMERCIAL_RCV_TIMES = "COMMERCIAL_RCV_TIMES";
    public static final String COMMERCIAL_RCV_EPOLLS = "COMMERCIAL_RCV_EPOLLS";
    public static final String COMMERCIAL_SEND_SIZE = "COMMERCIAL_SEND_SIZE";
    public static final String COMMERCIAL_RCV_SIZE = "COMMERCIAL_RCV_SIZE";
    public static final String COMMERCIAL_PERM_FAILURES = "COMMERCIAL_PERM_FAILURES";
    public static final String COMMERCIAL_OWNER = "Owner";

    public static final String ACCOUNT_SEND_TIMES = "ACCOUNT_SEND_TIMES";
    public static final String ACCOUNT_SNDBCK_TIMES = "ACCOUNT_SNDBCK_TIMES";
    public static final String ACCOUNT_RCV_TIMES = "ACCOUNT_RCV_TIMES";
    public static final String ACCOUNT_RCV_EPOLLS = "ACCOUNT_RCV_EPOLLS";
    public static final String ACCOUNT_SEND_SIZE = "ACCOUNT_SEND_SIZE";
    public static final String ACCOUNT_RCV_SIZE = "ACCOUNT_RCV_SIZE";
    public static final String ACCOUNT_SEND_RT = "ACCOUNT_SEND_RT";
    public static final String ACCOUNT_OWNER_PARENT = "OWNER_PARENT";
    public static final String ACCOUNT_OWNER_SELF = "OWNER_SELF";

    public static final long ACCOUNT_STAT_INVERTAL = 60 * 1000;
    public static final String ACCOUNT_SEND = "ACCOUNT_SEND";
    public static final String ACCOUNT_RCV = "ACCOUNT_RCV";
    public static final String TIMES = "TIMES";
    public static final String SIZE = "SIZE";
    public static final String RT = "RT";

    // Message Size limit for one api-calling count.
    public static final double SIZE_PER_COUNT = 64 * 1024;

    public static final String GROUP_GET_FALL_SIZE = "GROUP_GET_FALL_SIZE";
    public static final String GROUP_GET_FALL_TIME = "GROUP_GET_FALL_TIME";
    // Pull Message Latency
    public static final String GROUP_GET_LATENCY = "GROUP_GET_LATENCY";
    // Consumer Register Time
    public static final String CONSUMER_REGISTER_TIME = "CONSUMER_REGISTER_TIME";
    // Producer Register Time
    public static final String PRODUCER_REGISTER_TIME = "PRODUCER_REGISTER_TIME";
    public static final String CHANNEL_ACTIVITY = "CHANNEL_ACTIVITY";
    public static final String CHANNEL_ACTIVITY_CONNECT = "CONNECT";
    public static final String CHANNEL_ACTIVITY_IDLE = "IDLE";
    public static final String CHANNEL_ACTIVITY_EXCEPTION = "EXCEPTION";
    public static final String CHANNEL_ACTIVITY_CLOSE = "CLOSE";


    /**
     * read disk follow stats
     */
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.ROCKETMQ_STATS_LOGGER_NAME);
    private static final InternalLogger COMMERCIAL_LOG = InternalLoggerFactory.getLogger(LoggerName.COMMERCIAL_LOGGER_NAME);
    private static final InternalLogger ACCOUNT_LOG = InternalLoggerFactory.getLogger(LoggerName.ACCOUNT_LOGGER_NAME);
    private static final InternalLogger DLQ_STAT_LOG = InternalLoggerFactory.getLogger(LoggerName.DLQ_STATS_LOGGER_NAME);
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
        "BrokerStatsThread"));
    private final ScheduledExecutorService commercialExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
        "CommercialStatsThread"));
    private final ScheduledExecutorService accountExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
        "AccountStatsThread"));
    private final HashMap<String, StatsItemSet> statsTable = new HashMap<String, StatsItemSet>();
    private final String clusterName;
    private final MomentStatsItemSet momentStatsItemSetFallSize = new MomentStatsItemSet(GROUP_GET_FALL_SIZE, scheduledExecutorService, log);
    private final MomentStatsItemSet momentStatsItemSetFallTime = new MomentStatsItemSet(GROUP_GET_FALL_TIME, scheduledExecutorService, log);

    private final StatisticsManager accountStatManager = new StatisticsManager();

    public BrokerStatsManager(String clusterName) {
        this.clusterName = clusterName;

        this.statsTable.put(TOPIC_PUT_NUMS, new StatsItemSet(TOPIC_PUT_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(TOPIC_PUT_SIZE, new StatsItemSet(TOPIC_PUT_SIZE, this.scheduledExecutorService, log));
        this.statsTable.put(GROUP_GET_NUMS, new StatsItemSet(GROUP_GET_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(GROUP_GET_SIZE, new StatsItemSet(GROUP_GET_SIZE, this.scheduledExecutorService, log));
        this.statsTable.put(GROUP_GET_LATENCY, new StatsItemSet(GROUP_GET_LATENCY, this.scheduledExecutorService, log));
        this.statsTable.put(SNDBCK_PUT_NUMS, new StatsItemSet(SNDBCK_PUT_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(DLQ_PUT_NUMS, new StatsItemSet(DLQ_PUT_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(BROKER_PUT_NUMS, new StatsItemSet(BROKER_PUT_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(BROKER_GET_NUMS, new StatsItemSet(BROKER_GET_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(GROUP_GET_FROM_DISK_NUMS, new StatsItemSet(GROUP_GET_FROM_DISK_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(GROUP_GET_FROM_DISK_SIZE, new StatsItemSet(GROUP_GET_FROM_DISK_SIZE, this.scheduledExecutorService, log));
        this.statsTable.put(BROKER_GET_FROM_DISK_NUMS, new StatsItemSet(BROKER_GET_FROM_DISK_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(BROKER_GET_FROM_DISK_SIZE, new StatsItemSet(BROKER_GET_FROM_DISK_SIZE, this.scheduledExecutorService, log));

        this.statsTable.put(SNDBCK2DLQ_TIMES, new StatsItemSet(SNDBCK2DLQ_TIMES, this.scheduledExecutorService, DLQ_STAT_LOG));

        this.statsTable.put(ACCOUNT_SEND_TIMES, new StatsItemSet(ACCOUNT_SEND_TIMES, this.accountExecutor, ACCOUNT_LOG));
        this.statsTable.put(ACCOUNT_RCV_TIMES, new StatsItemSet(ACCOUNT_RCV_TIMES, this.accountExecutor, ACCOUNT_LOG));
        this.statsTable.put(ACCOUNT_SEND_SIZE, new StatsItemSet(ACCOUNT_SEND_SIZE, this.accountExecutor, ACCOUNT_LOG));
        this.statsTable.put(ACCOUNT_RCV_SIZE, new StatsItemSet(ACCOUNT_RCV_SIZE, this.accountExecutor, ACCOUNT_LOG));
        this.statsTable.put(ACCOUNT_RCV_EPOLLS, new StatsItemSet(ACCOUNT_RCV_EPOLLS, this.accountExecutor, ACCOUNT_LOG));
        this.statsTable.put(ACCOUNT_SNDBCK_TIMES, new StatsItemSet(ACCOUNT_SNDBCK_TIMES, this.accountExecutor, ACCOUNT_LOG));
        this.statsTable.put(ACCOUNT_SEND_RT, new StatsItemSet(ACCOUNT_SEND_RT, this.accountExecutor, ACCOUNT_LOG));

        this.statsTable.put(COMMERCIAL_SEND_TIMES, new StatsItemSet(COMMERCIAL_SEND_TIMES, this.commercialExecutor, COMMERCIAL_LOG));
        this.statsTable.put(COMMERCIAL_RCV_TIMES, new StatsItemSet(COMMERCIAL_RCV_TIMES, this.commercialExecutor, COMMERCIAL_LOG));
        this.statsTable.put(COMMERCIAL_SEND_SIZE, new StatsItemSet(COMMERCIAL_SEND_SIZE, this.commercialExecutor, COMMERCIAL_LOG));
        this.statsTable.put(COMMERCIAL_RCV_SIZE, new StatsItemSet(COMMERCIAL_RCV_SIZE, this.commercialExecutor, COMMERCIAL_LOG));
        this.statsTable.put(COMMERCIAL_RCV_EPOLLS, new StatsItemSet(COMMERCIAL_RCV_EPOLLS, this.commercialExecutor, COMMERCIAL_LOG));
        this.statsTable.put(COMMERCIAL_SNDBCK_TIMES, new StatsItemSet(COMMERCIAL_SNDBCK_TIMES, this.commercialExecutor, COMMERCIAL_LOG));
        this.statsTable.put(COMMERCIAL_PERM_FAILURES, new StatsItemSet(COMMERCIAL_PERM_FAILURES, this.commercialExecutor, COMMERCIAL_LOG));

        this.statsTable.put(CONSUMER_REGISTER_TIME, new StatsItemSet(CONSUMER_REGISTER_TIME, this.scheduledExecutorService, log));
        this.statsTable.put(PRODUCER_REGISTER_TIME, new StatsItemSet(PRODUCER_REGISTER_TIME, this.scheduledExecutorService, log));

        this.statsTable.put(CHANNEL_ACTIVITY, new StatsItemSet(CHANNEL_ACTIVITY, this.scheduledExecutorService, log));

        StatisticsItemFormatter formatter = new StatisticsItemFormatter();
        this.accountStatManager.addStatisticsKindMeta(createStatisticsKindMeta(
            ACCOUNT_SEND, new String[]{TIMES, SIZE, RT}, this.accountExecutor, formatter, ACCOUNT_LOG, ACCOUNT_STAT_INVERTAL));
        this.accountStatManager.addStatisticsKindMeta(createStatisticsKindMeta(
            ACCOUNT_RCV, new String[]{TIMES, SIZE, RT}, this.accountExecutor, formatter, ACCOUNT_LOG, ACCOUNT_STAT_INVERTAL));
    }

    public MomentStatsItemSet getMomentStatsItemSetFallSize() {
        return momentStatsItemSetFallSize;
    }

    public MomentStatsItemSet getMomentStatsItemSetFallTime() {
        return momentStatsItemSetFallTime;
    }

    public void start() {
    }

    public void shutdown() {
        this.scheduledExecutorService.shutdown();
        this.commercialExecutor.shutdown();
    }

    public StatsItem getStatsItem(final String statsName, final String statsKey) {
        try {
            return this.statsTable.get(statsName).getStatsItem(statsKey);
        } catch (Exception e) {
        }

        return null;
    }

    public void incConsumerRegisterTime(final int incValue) {
        this.statsTable.get(CONSUMER_REGISTER_TIME).addValue(this.clusterName, incValue, 1);
    }

    public void incProducerRegisterTime(final int incValue) {
        this.statsTable.get(PRODUCER_REGISTER_TIME).addValue(this.clusterName, incValue, 1);
    }

    public void incChannelConnectNum() {
        this.statsTable.get(CHANNEL_ACTIVITY).addValue(CHANNEL_ACTIVITY_CONNECT, 1, 1);
    }

    public void incChannelCloseNum() {
        this.statsTable.get(CHANNEL_ACTIVITY).addValue(CHANNEL_ACTIVITY_CLOSE, 1, 1);
    }

    public void incChannelExceptionNum() {
        this.statsTable.get(CHANNEL_ACTIVITY).addValue(CHANNEL_ACTIVITY_EXCEPTION, 1, 1);
    }

    public void incChannelIdleNum() {
        this.statsTable.get(CHANNEL_ACTIVITY).addValue(CHANNEL_ACTIVITY_IDLE, 1, 1);
    }

    public void incTopicPutNums(final String topic) {
        this.statsTable.get(TOPIC_PUT_NUMS).addValue(topic, 1, 1);
    }

    public void incTopicPutNums(final String topic, int num, int times) {
        this.statsTable.get(TOPIC_PUT_NUMS).addValue(topic, num, times);
    }

    public void incTopicPutSize(final String topic, final int size) {
        this.statsTable.get(TOPIC_PUT_SIZE).addValue(topic, size, 1);
    }

    public void incGroupGetNums(final String group, final String topic, final int incValue) {
        final String statsKey = buildStatsKey(topic, group);
        this.statsTable.get(GROUP_GET_NUMS).addValue(statsKey, incValue, 1);
    }

    public String buildStatsKey(String topic, String group) {
        StringBuffer strBuilder = new StringBuffer();
        strBuilder.append(topic);
        strBuilder.append("@");
        strBuilder.append(group);
        return strBuilder.toString();
    }

    public void incGroupGetSize(final String group, final String topic, final int incValue) {
        final String statsKey = buildStatsKey(topic, group);
        this.statsTable.get(GROUP_GET_SIZE).addValue(statsKey, incValue, 1);
    }

    public void incGroupGetLatency(final String group, final String topic, final int queueId, final int incValue) {
        final String statsKey = String.format("%d@%s@%s", queueId, topic, group);
        this.statsTable.get(GROUP_GET_LATENCY).addValue(statsKey, incValue, 1);
    }

    public void incBrokerPutNums() {
        this.statsTable.get(BROKER_PUT_NUMS).getAndCreateStatsItem(this.clusterName).getValue().incrementAndGet();
    }

    public void incBrokerPutNums(final int incValue) {
        this.statsTable.get(BROKER_PUT_NUMS).getAndCreateStatsItem(this.clusterName).getValue().addAndGet(incValue);
    }

    public void incBrokerGetNums(final int incValue) {
        this.statsTable.get(BROKER_GET_NUMS).getAndCreateStatsItem(this.clusterName).getValue().addAndGet(incValue);
    }

    public void incSendBackNums(final String group, final String topic) {
        final String statsKey = buildStatsKey(topic, group);
        this.statsTable.get(SNDBCK_PUT_NUMS).addValue(statsKey, 1, 1);
    }

    public double tpsGroupGetNums(final String group, final String topic) {
        final String statsKey = buildStatsKey(topic, group);
        return this.statsTable.get(GROUP_GET_NUMS).getStatsDataInMinute(statsKey).getTps();
    }

    public void recordDiskFallBehindTime(final String group, final String topic, final int queueId,
        final long fallBehind) {
        final String statsKey = String.format("%d@%s@%s", queueId, topic, group);
        this.momentStatsItemSetFallTime.getAndCreateStatsItem(statsKey).getValue().set(fallBehind);
    }

    public void recordDiskFallBehindSize(final String group, final String topic, final int queueId,
        final long fallBehind) {
        final String statsKey = String.format("%d@%s@%s", queueId, topic, group);
        this.momentStatsItemSetFallSize.getAndCreateStatsItem(statsKey).getValue().set(fallBehind);
    }

    public void incDLQStatValue(final String key, final String owner, final String group,
                                   final String topic, final String type, final int incValue) {
        final String statsKey = buildCommercialStatsKey(owner, topic, group, type);
        this.statsTable.get(key).addValue(statsKey, incValue, 1);
    }

    public void incCommercialValue(final String key, final String owner, final String group,
        final String topic, final String type, final int incValue) {
        final String statsKey = buildCommercialStatsKey(owner, topic, group, type);
        this.statsTable.get(key).addValue(statsKey, incValue, 1);
    }

    public void incAccountValue(final String key, final String accountOwnerParent, final String accountOwnerSelf,
                                final String instanceId, final String group, final String topic,
                                final String topicType, final String msgType, final int incValue) {
        final String statsKey = buildAccountStatsKey(accountOwnerParent, accountOwnerSelf, instanceId, topic, group, topicType, msgType);
        this.statsTable.get(key).addValue(statsKey, incValue, 1);
    }

    public void incAccountValue(final String kind, final String commercialOwner, final String authType,
                                final String accountOwnerParent, final String accountOwnerSelf,
                                final String instanceId, final String topic, final String group,
                                final String topicType, final String msgType, final String stat, final long... incValues) {
        final String key = buildAccountStatKey(commercialOwner, authType, accountOwnerParent, accountOwnerSelf,
            instanceId, topic, group, topicType, msgType, stat);
        this.accountStatManager.inc(kind, key, incValues);
    }

    public String buildCommercialStatsKey(String owner, String topic, String group, String type) {
        StringBuffer strBuilder = new StringBuffer();
        strBuilder.append(owner);
        strBuilder.append("@");
        strBuilder.append(topic);
        strBuilder.append("@");
        strBuilder.append(group);
        strBuilder.append("@");
        strBuilder.append(type);
        return strBuilder.toString();
    }

    public String buildAccountStatsKey(String accountOwnerParent, String accountOwnerSelf, String instanceId, String topic, String group, String topicType, String msgType) {
        StringBuffer strBuilder = new StringBuffer();
        strBuilder.append(accountOwnerParent);
        strBuilder.append("@");
        strBuilder.append(accountOwnerSelf);
        strBuilder.append("@");
        strBuilder.append(instanceId);
        strBuilder.append("@");
        strBuilder.append(topic);
        strBuilder.append("@");
        strBuilder.append(group);
        strBuilder.append("@");
        strBuilder.append(topicType);
        strBuilder.append("@");
        strBuilder.append(msgType);
        return strBuilder.toString();
    }

    public String buildAccountStatKey(final String commercialOwner, final String authType,
                                       final String accountOwnerParent, final String accountOwnerSelf,
                                       final String instanceId, final String topic, final String group,
                                       final String topicType, final String msgType, final String stat) {
        final String sep = "|";
        StringBuffer strBuilder = new StringBuffer();
        strBuilder.append(commercialOwner).append(sep);
        strBuilder.append(authType).append(sep);
        strBuilder.append(accountOwnerParent).append(sep);
        strBuilder.append(accountOwnerSelf).append(sep);
        strBuilder.append(instanceId).append(sep);
        strBuilder.append(topic).append(sep);
        strBuilder.append(group).append(sep);
        strBuilder.append(topicType).append(sep);
        strBuilder.append(msgType).append(sep);
        strBuilder.append(stat).append(sep);
        return strBuilder.toString();
    }

    public static StatisticsKindMeta createStatisticsKindMeta(String name,
                                            String[] itemNames,
                                            ScheduledExecutorService executorService,
                                            StatisticsItemFormatter formatter,
                                            InternalLogger log,
                                            long interval) {
        StatisticsItemPrinter printer = new StatisticsItemPrinter(formatter, log);
        StatisticsKindMeta kindMeta = new StatisticsKindMeta();
        kindMeta.setName(name);
        kindMeta.setItemNames(itemNames);
        kindMeta.setScheduledPrinter(
            new StatisticsItemScheduledIncrementPrinter("Stat In One Minute: ", printer, executorService, interval));
        return kindMeta;
    }

    // msgTyp
    public enum StatsType {
        SEND_SUCCESS,
        SEND_FAILURE,
        SEND_BACK,
        SEND_BACK_TO_DLQ,
        SEND_ORDER,
        SEND_TIMER,
        SEND_TRANSACTION,
        RCV_SUCCESS,
        RCV_EPOLLS,
        PERM_FAILURE
    }
}
