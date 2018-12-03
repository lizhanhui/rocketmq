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
package org.apache.rocketmq.common.statistics;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class StatisticsItemScheduledIncrementPrinter extends StatisticsItemScheduledPrinter {
    /**
     * last snapshots of all scheduled items
     */
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, StatisticsItem>> lastItemSnapshots
        = new ConcurrentHashMap<String, ConcurrentHashMap<String, StatisticsItem>>();

    public StatisticsItemScheduledIncrementPrinter(String name, StatisticsItemPrinter printer,
                                                   ScheduledExecutorService executor,
                                                   InitialDelay initialDelay,
                                                   long interval) {
        super(name, printer, executor, initialDelay, interval);
    }

    /**
     * schedule a StatisticsItem to print the Increments periodically
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
        }, getInitialDelay(), interval, TimeUnit.MILLISECONDS);
    }

    private StatisticsItem getLastItemSnapshot(String kind, String key) {
        ConcurrentHashMap<String, StatisticsItem> itemMap = lastItemSnapshots.get(kind);
        return (itemMap != null) ? itemMap.get(key) : null;
    }

    private void setLastItemSnapshot(StatisticsItem item) {
        String kind = item.getStatKind();
        String key = item.getStatObject();
        ConcurrentHashMap<String, StatisticsItem> itemMap = lastItemSnapshots.get(kind);
        if (itemMap == null) {
            itemMap = new ConcurrentHashMap<String, StatisticsItem>();
            ConcurrentHashMap<String, StatisticsItem> oldItemMap = lastItemSnapshots.putIfAbsent(kind, itemMap);
            if (oldItemMap != null) {
                itemMap = oldItemMap;
            }
        }

        itemMap.put(key, item);
    }
}
