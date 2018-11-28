package org.apache.rocketmq.common.statistics;

import org.apache.rocketmq.logging.InternalLogger;

import java.util.Date;

public class StatisticsItemPrinter {
    private InternalLogger log;

    private StatisticsItemFormatter formatter;

    public StatisticsItemPrinter(StatisticsItemFormatter formatter, InternalLogger log) {
        this.formatter = formatter;
        this.log = log;
    }

    public void log(InternalLogger log) {
        this.log = log;
    }

    public void formatter(StatisticsItemFormatter formatter) {
        this.formatter = formatter;
    }

    public void print(String prefix, StatisticsItem statItem) {
        log.info("{}{}", prefix, formatter.format(statItem));
        System.out.printf("%s %s%s\n", new Date().toString(), prefix, formatter.format(statItem));
    }
}
