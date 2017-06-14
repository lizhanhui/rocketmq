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

package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LockFreeProducerManager extends ProducerManager {
    private final ConcurrentHashMap<String /* group name */, ConcurrentHashMap<Channel, ClientChannelInfo>> groupChannelTable =
        new ConcurrentHashMap<>();

    public LockFreeProducerManager(final BrokerStatsManager brokerStatsManager) {
        super(brokerStatsManager);
    }

    public LockFreeProducerManager() {
        super();
    }

    @Override
    public int groupSize() {
        return -1;
    }

    @Override
    public HashMap<String, HashMap<Channel, ClientChannelInfo>> getGroupChannelTable() {
        HashMap<String /* group name */, HashMap<Channel, ClientChannelInfo>> newGroupChannelTable =
            new HashMap<String /* group name */, HashMap<Channel, ClientChannelInfo>>();
        Iterator<Map.Entry<String, ConcurrentHashMap<Channel, ClientChannelInfo>>> iterator = this.groupChannelTable.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<String, ConcurrentHashMap<Channel, ClientChannelInfo>> entry = iterator.next();
            final String groupName = entry.getKey();
            if (!newGroupChannelTable.containsKey(groupName)) {
                newGroupChannelTable.put(groupName, new HashMap<Channel, ClientChannelInfo>());
            }
            newGroupChannelTable.get(groupName).putAll(entry.getValue());
        }

        return newGroupChannelTable;
    }

    @Override
    public void scanNotActiveChannel() {
        Iterator<Map.Entry<String, ConcurrentHashMap<Channel, ClientChannelInfo>>> iterator = this.groupChannelTable.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<String, ConcurrentHashMap<Channel, ClientChannelInfo>> entry = iterator.next();

            final String group = entry.getKey();
            final ConcurrentHashMap<Channel, ClientChannelInfo> clientChannelInfoTable = entry.getValue();

            Iterator<Map.Entry<Channel, ClientChannelInfo>> channelIterator = clientChannelInfoTable.entrySet().iterator();
            while (channelIterator.hasNext()) {
                Map.Entry<Channel, ClientChannelInfo> channelEntry = channelIterator.next();
                final ClientChannelInfo channelInfo = channelEntry.getValue();
                long diff = System.currentTimeMillis() - channelInfo.getLastUpdateTimestamp();
                if (diff > CHANNEL_EXPIRED_TIMEOUT) {
                    channelIterator.remove();
                    log.warn(
                        "SCAN: remove expired channel[{}] from LockFreeProducerManager groupChannelTable, producer group name: {}",
                        RemotingHelper.parseChannelRemoteAddr(channelInfo.getChannel()), group);
                    RemotingUtil.closeChannel(channelInfo.getChannel());
                }
            }

            if (clientChannelInfoTable.isEmpty()) {
                log.warn("SCAN: remove expired channel from LockFreeProducerManager groupChannelTable, all clear, group={}", group);
                iterator.remove();
            }
        }
    }

    @Override
    public void doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        if (channel == null) {
            return;
        }
        Iterator<Map.Entry<String, ConcurrentHashMap<Channel, ClientChannelInfo>>> iterator = this.groupChannelTable.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<String, ConcurrentHashMap<Channel, ClientChannelInfo>> entry = iterator.next();

            final String group = entry.getKey();
            final ConcurrentHashMap<Channel, ClientChannelInfo> clientChannelInfoTable = entry.getValue();

            final ClientChannelInfo clientChannelInfo = clientChannelInfoTable.remove(channel);
            if (clientChannelInfo != null) {
                log.info(
                    "NETTY EVENT: remove channel[{}][{}] from LockFreeProducerManager groupChannelTable, producer group: {}",
                    clientChannelInfo.toString(), remoteAddr, group);
                if (clientChannelInfoTable.isEmpty()) {
                    ConcurrentHashMap<Channel, ClientChannelInfo> oldGroupTable = this.groupChannelTable.remove(group);
                    if (oldGroupTable != null) {
                        log.info("[LockFreeProducerManager]unregister a producer group[{}] from groupChannelTable", group);
                    }
                }
            }
        }
    }

    @Override
    public void registerProducer(final String group, final ClientChannelInfo clientChannelInfo) {
        long start = System.currentTimeMillis();
        ConcurrentHashMap<Channel, ClientChannelInfo> channelTable = this.groupChannelTable.get(group);
        if (null == channelTable) {
            channelTable = new ConcurrentHashMap<>();
            ConcurrentHashMap<Channel, ClientChannelInfo> prev = this.groupChannelTable.putIfAbsent(group, channelTable);
            channelTable = prev != null ? prev : channelTable;
        }

        ClientChannelInfo clientChannelInfoFound = channelTable.get(clientChannelInfo.getChannel());
        if (null == clientChannelInfoFound) {
            ClientChannelInfo prevChannel = channelTable.put(clientChannelInfo.getChannel(), clientChannelInfo);
            if (prevChannel == null) {
                log.info("[LockFreeProducerManager]new producer connected, group: {} channel: {}", group,
                    clientChannelInfo.toString());
            } else {
                if (!prevChannel.getClientId().equals(clientChannelInfo.getClientId())) {
                    log.warn("[BUG][LockFreeProducerManager] [Concurrent] producer has same channel, but different client id, group: {}, OLD: {}, new: {}",
                        group, prevChannel, clientChannelInfo);
                }
            }

            clientChannelInfoFound = clientChannelInfo;
        } else {
            if (!clientChannelInfoFound.getClientId().equals(clientChannelInfo.getClientId())) {
                log.warn("[BUG][LockFreeProducerManager] producer has same channel, but different client id, group: {}, OLD: {}, new: {}",
                    group, clientChannelInfoFound, clientChannelInfo);
                channelTable.put(clientChannelInfo.getChannel(), clientChannelInfo);
                clientChannelInfoFound = clientChannelInfo;
            }
        }

        if (clientChannelInfoFound != null) {
            clientChannelInfoFound.setLastUpdateTimestamp(System.currentTimeMillis());
        }
        if (null != this.brokerStatsManager) {
            this.brokerStatsManager.incProducerRegisterTime((int) (System.currentTimeMillis() - start));
        }
    }

    @Override
    public void unregisterProducer(final String group, final ClientChannelInfo clientChannelInfo) {
        ConcurrentHashMap<Channel, ClientChannelInfo> channelTable = this.groupChannelTable.get(group);
        if (null != channelTable && !channelTable.isEmpty()) {
            ClientChannelInfo old = channelTable.remove(clientChannelInfo.getChannel());
            if (old != null) {
                log.info("[LockFreeProducerManager]unregister a producer[{}] from groupChannelTable {}", group,
                    clientChannelInfo.toString());
            }

            if (channelTable.isEmpty()) {
                ConcurrentHashMap<Channel, ClientChannelInfo> oldGroupTable = this.groupChannelTable.remove(group);
                if (oldGroupTable != null) {
                    log.info("[LockFreeProducerManager]unregister a producer group[{}] from groupChannelTable", group);
                }
            }
        }
    }
}
