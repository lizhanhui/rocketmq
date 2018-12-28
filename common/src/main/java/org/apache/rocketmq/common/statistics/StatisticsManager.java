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

import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StatisticsManager {

    /**
     * Set of Statistics Kind Metadata
     */
    private Map<String, StatisticsKindMeta> kindMetaMap;

    /**
     * item names to calculate statistics brief
     */
    private Pair<String, long[][]>[] briefMetas;

    /**
     * Statistics
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
        statsTable.putIfAbsent(kindMeta.getName(), new ConcurrentHashMap<String, StatisticsItem>(16));
    }

    public void setBriefMeta(Pair<String, long[][]>[] briefMetas) {
        this.briefMetas = briefMetas;
    }

    /**
     * Increment a StatisticsItem
     *
     * @param kind
     * @param key
     * @param itemAccumulates
     */
    public boolean inc(String kind, String key, long... itemAccumulates) {
        ConcurrentHashMap<String, StatisticsItem> itemMap = statsTable.get(kind);
        if (itemMap != null) {
            StatisticsItem item = itemMap.get(key);

            // if not exist, create and schedule
            if (item == null) {
                item = new StatisticsItem(kind, key, kindMetaMap.get(kind).getItemNames());
                item.setInterceptor(new StatisticsBriefInterceptor(item, briefMetas));
                StatisticsItem oldItem = itemMap.putIfAbsent(key, item);
                if (oldItem != null) {
                    item = oldItem;
                } else {
                    scheduleStatisticsItem(item);
                }
            }

            // do increment
            item.incItems(itemAccumulates);

            return true;
        }

        return false;
    }

    private void scheduleStatisticsItem(StatisticsItem item) {
        kindMetaMap.get(item.getStatKind()).getScheduledPrinter().schedule(item);
    }
}
