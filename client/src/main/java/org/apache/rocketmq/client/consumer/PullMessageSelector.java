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

public class PullMessageSelector extends MessageSelector {
    protected PullMessageSelector(String expression) {
        super(expression);
    }

    public static PullMessageSelector byTag(String tag) {
        return new PullMessageSelector(tag);
    }

    public static PullMessageSelector all() {
        return new PullMessageSelector("*");
    }

    private long offset = -1;
    private int maxNums = 0;
    private long timeout = 0;
    private boolean blockIfNotFound = false;

    /**
     * from where to pull
     *
     * @param offset
     * @return
     */
    public MessageSelector from(long offset) {
        this.offset = offset;
        return this;
    }

    /**
     * max pulling numbers
     *
     * @param maxNums
     * @return
     */
    public MessageSelector count(int maxNums) {
        this.maxNums = maxNums;
        return this;
    }

    /**
     * timeout
     *
     * @param timeout
     * @return
     */
    public MessageSelector timeout(long timeout) {
        this.timeout = timeout;
        return this;
    }

    /**
     * if no message arrival,whether blocking.
     *
     * @param block
     * @return
     */
    public MessageSelector blockIfNotFound(boolean block) {
        this.blockIfNotFound = block;
        return this;
    }

    public long getOffset() {
        return offset;
    }

    public int getMaxNums() {
        return maxNums;
    }

    public long getTimeout() {
        return timeout;
    }

    public boolean isBlockIfNotFound() {
        return blockIfNotFound;
    }
}
