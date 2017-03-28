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

package org.apache.rocketmq.client.consumer;

import org.apache.rocketmq.common.message.MessageDecoder;

import java.util.HashMap;
import java.util.Map;

public class MessageSelector {

    protected MessageSelector(String expression) {
        this.expression = expression;
    }

    public static MessageSelector byTag(String tag) {
        return new MessageSelector(tag);
    }

    public static MessageSelector all() {
        return new MessageSelector("*");
    }

    /**
     * expression content.
     */
    private String expression;
    /**
     * self define properties, just an extend point.
     */
    private Map<String, String> properties = new HashMap<String, String>(4);

    public void putProperty(String key, String value) {
        if (key == null || value == null || key.trim() == "" || value.trim() == "") {
            throw new IllegalArgumentException(
                "Key and Value can not be null or empty string!"
            );
        }
        this.properties.put(key, value);
    }

    public void putAllProperties(Map<String, String> puts) {
        if (puts == null || puts.isEmpty()) {
            return;
        }
        this.properties.putAll(puts);
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getExpression() {
        return expression;
    }

    public String getPropertiesStr() {
        if (this.properties == null || this.properties.isEmpty()) {
            return null;
        }
        return MessageDecoder.messageProperties2String(this.properties);
    }
}
