package org.apache.rocketmq.common.statistics;

import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 统计项
 *
 * 包括统计类型、统计对象、统计字段/指标
 */
public class StatisticsItem {
    /** 统计类型 */
    private String statKind;
    /** 统计对象 */
    private String statObject;

    // 统计字段
    /** 统计字段名 */
    private String[] itemNames;
    /** 统计字段累计值 */
    private AtomicLong[] itemAccumulates;
    /** 调用次数 */
    private AtomicLong invokeTimes;

    /** 最近一次数据更新的时间戳 */
    private AtomicLong lastTimeStamp;

    public StatisticsItem(String statKind, String statObject, String... itemNames) {
        if (itemNames == null || itemNames.length <= 0) {
            throw new InvalidParameterException("StatisticsItem \"itemNames\" is empty");
        }

        this.statKind = statKind;
        this.statObject = statObject;
        this.itemNames = itemNames;

        AtomicLong[] accs = new AtomicLong[itemNames.length];
        for (int i = 0; i< itemNames.length;i++){
            accs[i] = new AtomicLong(0);
        }

        this.itemAccumulates = accs;
        this.invokeTimes = new AtomicLong();
        this.lastTimeStamp = new AtomicLong(System.currentTimeMillis());
    }

    /**
     * 累加
     * @param itemIncs
     */
    public void incItems(long... itemIncs) {
        int len = Math.min(itemIncs.length, itemAccumulates.length);
        for (int i = 0; i < len; i++) {
            itemAccumulates[i].addAndGet(itemIncs[i]);
        }

        invokeTimes.addAndGet(1);
        lastTimeStamp.set(System.currentTimeMillis());
    }

    public String getStatKind() {
        return statKind;
    }

    public String getStatObject() {
        return statObject;
    }

    public String[] getItemNames() {
        return itemNames;
    }

    public AtomicLong[] getItemAccumulates() {
        return itemAccumulates;
    }

    public AtomicLong getInvokeTimes() {
        return invokeTimes;
    }

    /**
     * 获取快照
     * 快照的多个值的一致性不严格
     * @return
     */
    public StatisticsItem snapshot() {
        StatisticsItem ret = new StatisticsItem(statKind, statObject, itemNames);

        ret.itemAccumulates = new AtomicLong[itemAccumulates.length];
        for (int i = 0;i < itemAccumulates.length; i++){
            ret.itemAccumulates[i] =  new AtomicLong(itemAccumulates[i].get());
        }

        ret.invokeTimes = new AtomicLong(invokeTimes.longValue());
        ret.lastTimeStamp = new AtomicLong(lastTimeStamp.longValue());

        return ret;
    }

    /**
     * 减法
     * @param item
     * @return
     */
    public StatisticsItem subtract(StatisticsItem item) {
        if (item == null) {
            return snapshot();
        }

        if (!statKind.equals(item.statKind) || !statObject.equals(item.statObject) || !Arrays.equals(itemNames, item.itemNames)) {
            throw new IllegalArgumentException("StatisticsItem's kind, key and itemNames must be exactly the same");
        }

        StatisticsItem ret = new StatisticsItem(statKind, statObject, itemNames);
        ret.invokeTimes = new AtomicLong(invokeTimes.get() - item.invokeTimes.get());
        ret.itemAccumulates = new AtomicLong[itemAccumulates.length];
        for (int i = 0;i < itemAccumulates.length; i++){
            ret.itemAccumulates[i] =  new AtomicLong(itemAccumulates[i].get() - item.itemAccumulates[i].get());
        }
        return ret;
    }
}
