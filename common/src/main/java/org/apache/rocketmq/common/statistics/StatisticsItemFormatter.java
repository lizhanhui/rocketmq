package org.apache.rocketmq.common.statistics;

import java.util.concurrent.atomic.AtomicLong;

public class StatisticsItemFormatter {
    public String format(StatisticsItem statItem) {
        final String seperator = "|";
        StringBuilder sb = new StringBuilder();
        sb.append(statItem.getStatKind()).append(seperator);
        sb.append(statItem.getStatObject()).append(seperator);
        for (AtomicLong acc : statItem.getItemAccumulates()) {
            sb.append(acc.get()).append(seperator);
        }
        sb.append(statItem.getInvokeTimes());
        return sb.toString();
    }
}
