package org.apache.rocketmq.common.protocol;

import org.apache.rocketmq.common.MixAll;
import org.junit.Assert;
import org.junit.Test;

public class NamespaceUtilTest {

    private static final String INSTANCE_ID = "MQ_INST_XXX";
    private static final String TOPIC = "TOPIC_XXX";
    private static final String GROUP_ID = "GID_XXX";
    private static final String GROUP_ID_WITH_NAMESPACE = INSTANCE_ID + NamespaceUtil.NAMESPACE_SEPARATOR + GROUP_ID;
    private static final String TOPIC_WITH_NAMESPACE = INSTANCE_ID + NamespaceUtil.NAMESPACE_SEPARATOR + TOPIC;
    private static final String RETRY_TOPIC = MixAll.RETRY_GROUP_TOPIC_PREFIX + GROUP_ID;
    private static final String RETRY_TOPIC_WITH_NAMESPACE =
        MixAll.RETRY_GROUP_TOPIC_PREFIX + INSTANCE_ID + NamespaceUtil.NAMESPACE_SEPARATOR + GROUP_ID;

    @Test
    public void withoutNamespace() {
        String topic = NamespaceUtil.withoutNamespace(TOPIC_WITH_NAMESPACE, INSTANCE_ID);
        Assert.assertEquals(topic, TOPIC);
        String topic1 = NamespaceUtil.withoutNamespace(TOPIC_WITH_NAMESPACE);
        Assert.assertEquals(topic1, TOPIC);
        String groupId = NamespaceUtil.withoutNamespace(GROUP_ID_WITH_NAMESPACE, INSTANCE_ID);
        Assert.assertEquals(groupId, GROUP_ID);
        String groupId1 = NamespaceUtil.withoutNamespace(GROUP_ID_WITH_NAMESPACE);
        Assert.assertEquals(groupId1, GROUP_ID);
        String consumerId = NamespaceUtil.withoutNamespace(RETRY_TOPIC_WITH_NAMESPACE, INSTANCE_ID);
        Assert.assertEquals(consumerId, RETRY_TOPIC);
        String consumerId1 = NamespaceUtil.withoutNamespace(RETRY_TOPIC_WITH_NAMESPACE);
        Assert.assertEquals(consumerId1, RETRY_TOPIC);
        String consumerId2 = NamespaceUtil.withoutNamespace(RETRY_TOPIC_WITH_NAMESPACE, INSTANCE_ID + "hello");
        Assert.assertEquals(consumerId2, RETRY_TOPIC_WITH_NAMESPACE);
        Assert.assertNotEquals(consumerId2, RETRY_TOPIC);
    }
}