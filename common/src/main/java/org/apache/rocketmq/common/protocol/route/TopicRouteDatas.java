package org.apache.rocketmq.common.protocol.route;

import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashMap;
import java.util.Map;

public class TopicRouteDatas extends RemotingSerializable {

    private Map<String, TopicRouteData> topics = new HashMap<String, TopicRouteData>();

    public Map<String, TopicRouteData> getTopics() {
        return topics;
    }

    public void setTopics(Map<String, TopicRouteData> topics) {
        this.topics = topics;
    }
}
