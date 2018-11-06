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

package org.apache.rocketmq.common.protocol;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import static org.apache.rocketmq.common.MixAll.getDLQTopic;
import static org.apache.rocketmq.common.MixAll.getRetryTopic;

public class NamespaceUtil {
    public static final char NAMESPACE_SEPARATOR = '%';
    public static final int RETRY_PREFIX_LENGTH = MixAll.RETRY_GROUP_TOPIC_PREFIX.length();
    public static final int DLQ_PREFIX_LENGTH = MixAll.DLQ_GROUP_TOPIC_PREFIX.length();

    public static String withNamespace(RemotingCommand request, String resource) {
        return wrapNamespace(getNamespace(request), resource);
    }

    public static String withoutNamespace(String resource) {
        if (StringUtils.isEmpty(resource) || isSystemResource(resource)) {
            return resource;
        }

        if (isRetryTopic(resource)) {
            int index = resource.indexOf(NAMESPACE_SEPARATOR, RETRY_PREFIX_LENGTH);
            if (index > 0) {
                return getRetryTopic(resource.substring(index + 1));
            }
            return resource;
        }

        if (isDLQTopic(resource)) {
            int index = resource.indexOf(NAMESPACE_SEPARATOR, DLQ_PREFIX_LENGTH);
            if (index > 0) {
                return getDLQTopic(resource.substring(index + 1));
            }
            return resource;
        }

        int index = resource.indexOf(NAMESPACE_SEPARATOR);
        if (index > 0) {
            return resource.substring(index + 1);
        }
        return resource;
    }

    public static String wrapNamespace(String namespace, String resource) {
        if (StringUtils.isEmpty(namespace) || StringUtils.isEmpty(resource) || isSystemResource(resource)) {
            return resource;
        }

        StringBuffer strBuffer = new StringBuffer().append(namespace).append(NAMESPACE_SEPARATOR);

        if (isRetryTopic(resource)) {
            strBuffer.append(resource.substring(RETRY_PREFIX_LENGTH));
            return strBuffer.insert(0, MixAll.RETRY_GROUP_TOPIC_PREFIX).toString();
        }

        if (isDLQTopic(resource)) {
            strBuffer.append(resource.substring(DLQ_PREFIX_LENGTH));
            return strBuffer.insert(0, MixAll.DLQ_GROUP_TOPIC_PREFIX).toString();
        }

        return strBuffer.append(resource).toString();

    }

    public static String withNamespaceAndRetry(RemotingCommand request, String consumerGroup) {
        return wrapNamespaceAndRetry(getNamespace(request), consumerGroup);
    }

    public static String wrapNamespaceAndRetry(String namespace, String consumerGroup) {
        if (StringUtils.isEmpty(consumerGroup)) {
            return null;
        }

        return new StringBuffer()
            .append(MixAll.RETRY_GROUP_TOPIC_PREFIX)
            .append(wrapNamespace(namespace, consumerGroup))
            .toString();
    }

    public static String getResource(String resourceWithNamespace, String namespace) {
        if (StringUtils.isEmpty(resourceWithNamespace) || StringUtils.isEmpty(namespace) ||
            !resourceWithNamespace.contains(namespace)) {
            return resourceWithNamespace;
        }

        if (isRetryTopic(resourceWithNamespace)) {
            return resourceWithNamespace.substring(RETRY_PREFIX_LENGTH).substring(namespace.length() + 1);
        }

        if (isDLQTopic(resourceWithNamespace)) {
            return resourceWithNamespace.substring(DLQ_PREFIX_LENGTH).substring(namespace.length() + 1);
        }

        return resourceWithNamespace.substring(namespace.length() + 1);
    }

    public static String getNamespace(RemotingCommand request) {
        String namespace ;

        switch (request.getCode()) {
            case RequestCode.SEND_MESSAGE_V2:
                namespace = request.getExtFields().get("n");
                break;
            default:
                namespace = request.getExtFields().get("namespace");
                break;
        }

        return namespace;

    }

    public static String getNamespace(String resource) {
        if (StringUtils.isEmpty(resource) || isSystemResource(resource)) {
            return "";
        }

        if (isRetryTopic(resource)) {
            int index = resource.indexOf(NAMESPACE_SEPARATOR, RETRY_PREFIX_LENGTH);
            if (index > 0) {
                return resource.substring(RETRY_PREFIX_LENGTH, index);
            }
            return "";
        }

        if (isDLQTopic(resource)) {
            int index = resource.indexOf(NAMESPACE_SEPARATOR, DLQ_PREFIX_LENGTH);
            if (index > 0) {
                return resource.substring(DLQ_PREFIX_LENGTH, index);
            }
            return "";
        }

        int index = resource.indexOf(NAMESPACE_SEPARATOR);
        if (index > 0) {
            return resource.substring(0, index);
        }
        return "";
    }

    private static boolean isSystemResource(String resource) {
        if (StringUtils.isEmpty(resource)) {
            return false;
        }

        return MixAll.isSystemTopic(resource) || MixAll.isSysConsumerGroup(resource) || MixAll.DEFAULT_TOPIC.equals(resource);
    }

    public static boolean isRetryTopic(String resource) {
        if (StringUtils.isEmpty(resource)) {
            return false;
        }

        return resource.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX);
    }

    public static boolean isDLQTopic(String resource) {
        if (StringUtils.isEmpty(resource)) {
            return false;
        }

        return resource.startsWith(MixAll.DLQ_GROUP_TOPIC_PREFIX);
    }
}