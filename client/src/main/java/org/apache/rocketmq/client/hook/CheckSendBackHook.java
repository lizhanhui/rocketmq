package org.apache.rocketmq.client.hook;

import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * @author gongshi
 * @date 2018/12/4.
 */
public interface CheckSendBackHook {
    String hookName();

    boolean needSendBack(final MessageExt msg, final ConsumeConcurrentlyContext context);
}
