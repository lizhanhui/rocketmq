package org.apache.rocketmq.common.statistics;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class StatisticsItemScheduledPrinter {
    protected String name;

    protected StatisticsItemPrinter printer;
    protected ScheduledExecutorService executor;
    protected long interval;

    public StatisticsItemScheduledPrinter(String name, StatisticsItemPrinter printer,
                                          ScheduledExecutorService executor,
                                          long interval) {
        this.name = name;
        this.printer = printer;
        this.executor = executor;
        this.interval = interval;
    }

    /**
     * 增加一个StatisticsItem的定时打印
     */
    public void schedule(final StatisticsItem statisticsItem) {
        executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                printer.print(name, statisticsItem);
            }
        }, 0, interval, TimeUnit.MILLISECONDS);
    }
}
