package org.apache.rocketmq.common.statistics;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StatisticsManager {

    /**
     * 类型元数据集
     */
    private Map<String, StatisticsKindMeta> kindMetaMap;

    /**
     * 数据集
     */
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, StatisticsItem>> statsTable
        = new ConcurrentHashMap<String, ConcurrentHashMap<String, StatisticsItem>>();

    public StatisticsManager() {
        kindMetaMap = new HashMap<String, StatisticsKindMeta>();
    }

    public StatisticsManager(Map<String, StatisticsKindMeta> kindMeta) {
        this.kindMetaMap = kindMeta;
    }

    public void addStatisticsKindMeta(StatisticsKindMeta kindMeta) {
        kindMetaMap.put(kindMeta.getName(), kindMeta);
        statsTable.putIfAbsent(kindMeta.getName(), new ConcurrentHashMap<String, StatisticsItem>());
    }

    /**
     * 累加指标
     *
     * @param kind
     * @param key
     * @param itemAccumulates
     */
    public boolean inc(String kind, String key, long... itemAccumulates) {
        ConcurrentHashMap<String, StatisticsItem> itemMap = statsTable.get(kind);
        if (itemMap != null) {
            StatisticsItem item = itemMap.get(key);

            // 不存在key-item，则创建，并加入定时打印
            if (item == null) {
                item = new StatisticsItem(kind, key, kindMetaMap.get(kind).getItemNames());
                StatisticsItem oldItem = itemMap.putIfAbsent(key, item);
                if (oldItem != null) {
                    item = oldItem;
                } else {
                    scheduleStatisticsItem(item);
                }
            }

            // 累加
            item.incItems(itemAccumulates);

            return true;
        }

        return false;
    }

    private void scheduleStatisticsItem(StatisticsItem item) {
        kindMetaMap.get(item.getStatKind()).getScheduledPrinter().schedule(item);
    }
}
