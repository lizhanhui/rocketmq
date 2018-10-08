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

public class NamespaceUtil {
    public static final char NAMESPACE_SEPARATOR = '%';
    public static final int RETRY_PREFIX_LENGTH = MixAll.RETRY_GROUP_TOPIC_PREFIX.length();
    public static final int DLQ_PREFIX_LENGTH = MixAll.DLQ_GROUP_TOPIC_PREFIX.length();

    public static String withNamespace(RemotingCommand request, String resource) {
        String namespace;
        switch (request.getCode()) {
            case RequestCode.SEND_MESSAGE_V2:
                namespace = request.getExtFields().get("n");
                break;
            default:
                namespace = request.getExtFields().get("namespace");
                break;
        }

        return wrapNamespace(namespace, resource);
    }

    public static String wrapNamespace(String namespace, String resource) {
        if (StringUtils.isEmpty(namespace)) {
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

    public static String getResource(String resourceWithNamespace) {
        if (StringUtils.isEmpty(resourceWithNamespace)) {
            return null;
        }

        if (isRetryTopic(resourceWithNamespace)) {
            resourceWithNamespace = resourceWithNamespace.substring(RETRY_PREFIX_LENGTH);
        }

        if (isDLQTopic(resourceWithNamespace)) {
            resourceWithNamespace = resourceWithNamespace.substring(DLQ_PREFIX_LENGTH);
        }

        int indexOfSeparator = resourceWithNamespace.indexOf(NAMESPACE_SEPARATOR) + 1;
        return resourceWithNamespace.substring(indexOfSeparator);
    }

    private static boolean isRetryTopic(String resource) {
        if (StringUtils.isEmpty(resource)) {
            return false;
        }

        return resource.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX);
    }

    private static boolean isDLQTopic(String resource) {
        if (StringUtils.isEmpty(resource)) {
            return false;
        }

        return resource.startsWith(MixAll.DLQ_GROUP_TOPIC_PREFIX);
    }
}