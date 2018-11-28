package org.apache.rocketmq.common.statistics;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class StatisticsItemScheduledIncrementPrinter extends StatisticsItemScheduledPrinter {
    //private String name;
    //
    //private StatisticsItemPrinter printer;
    //private ScheduledExecutorService executor;
    //private long interval;

    /**
     * 上一次的数据
     */
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, StatisticsItem>> lastItemSnapshots
        = new ConcurrentHashMap<String, ConcurrentHashMap<String, StatisticsItem>>();

    public StatisticsItemScheduledIncrementPrinter(String name, StatisticsItemPrinter printer,
                                                   ScheduledExecutorService executor,
                                                   long interval) {
        super(name, printer, executor, interval);
    }

    /**
     * 增加一个StatisticsItem的定时打印
     */
    @Override
    public void schedule(final StatisticsItem item) {
        executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                StatisticsItem snapshot = item.snapshot();
                StatisticsItem lastSnapshot = getLastItemSnapshot(item.getStatKind(), item.getStatObject());
                printer.print(name, snapshot.subtract(lastSnapshot));
                setLastItemSnapshot(snapshot);
            }
        }, 0, interval, TimeUnit.MILLISECONDS);
    }

    private StatisticsItem getLastItemSnapshot(String kind, String key) {
        ConcurrentHashMap<String, StatisticsItem> itemMap = lastItemSnapshots.get(kind);
        return (itemMap != null) ? itemMap.get(key) : null;
    }

    private void setLastItemSnapshot(StatisticsItem item) {
        String kind = item.getStatKind();
        String key = item.getStatObject();
        ConcurrentHashMap<String, StatisticsItem> itemMap = lastItemSnapshots.get(kind);
        if (itemMap==null){
            itemMap = new ConcurrentHashMap<String, StatisticsItem>();
            ConcurrentHashMap<String, StatisticsItem> oldItemMap = lastItemSnapshots.putIfAbsent(kind,  itemMap);
            if (oldItemMap!=null){
                itemMap = oldItemMap;
            }
        }

        itemMap.put(key, item);
    }
}
