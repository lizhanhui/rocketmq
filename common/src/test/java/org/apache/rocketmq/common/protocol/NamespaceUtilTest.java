package org.apache.rocketmq.common.protocol;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * NamespaceUtil Tester.
 */
public class NamespaceUtilTest {

    private static final String INSTNACE_ID = "MQ_INST_XXX";
    private static final String INSTNACE_ID1 = "MQ_INST_XXX1";
    private static final String TOPIC = "TOPIC_XXX";
    private static final String GID = "GID_XXX";
    private static String TOPIC_WITH_NAMESPACE;
    private static String RETRY_TOPIC_WITH_NAMESPACE;
    private RemotingCommand sendMsgRequest;

    @Before
    public void before() throws Exception {
        TOPIC_WITH_NAMESPACE = NamespaceUtil.wrapNamespace(INSTNACE_ID, TOPIC);
        RETRY_TOPIC_WITH_NAMESPACE = NamespaceUtil.wrapNamespaceAndRetry(INSTNACE_ID, GID);
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
        requestHeader.setNamespace(INSTNACE_ID);
        sendMsgRequest = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, requestHeader);
    }

    /**
     * Method: withNamespace(RemotingCommand request, String resource)
     */
    @Test
    public void testWithNamespace() throws Exception {
        String resourceWithNamespace = NamespaceUtil.withNamespace(null, TOPIC);
        Assert.assertEquals(resourceWithNamespace, TOPIC);
        sendMsgRequest.addExtField("namespace", INSTNACE_ID);
        String resourceWithNamespace1 = NamespaceUtil.withNamespace(sendMsgRequest, TOPIC);
        Assert.assertEquals(INSTNACE_ID + "%" + TOPIC, resourceWithNamespace1);
        sendMsgRequest.setExtFields(null);
        String resourceWithNamespace2 = NamespaceUtil.withNamespace(sendMsgRequest, TOPIC);
        Assert.assertEquals(TOPIC, resourceWithNamespace2);
    }

    /**
     * Method: withoutNamespace(String resource)
     */
    @Test
    public void testWithoutNamespace() throws Exception {
        String topic = NamespaceUtil.withoutNamespace(TOPIC_WITH_NAMESPACE);
        Assert.assertEquals(topic, TOPIC);
        String topic1 = NamespaceUtil.withoutNamespace(TOPIC_WITH_NAMESPACE, INSTNACE_ID1);
        Assert.assertEquals(topic1, TOPIC_WITH_NAMESPACE);
        Assert.assertNotEquals(topic1, TOPIC);

        String retryTopic = NamespaceUtil.withoutNamespace(RETRY_TOPIC_WITH_NAMESPACE);
        Assert.assertEquals(retryTopic, MixAll.getRetryTopic(GID));

        String retryTopic1 = NamespaceUtil.withoutNamespace(RETRY_TOPIC_WITH_NAMESPACE, null);
        Assert.assertNotEquals(retryTopic1, MixAll.getRetryTopic(GID));
        Assert.assertEquals(retryTopic1, RETRY_TOPIC_WITH_NAMESPACE);
    }

    /**
     * Method: withNamespaceAndRetry(RemotingCommand request, String consumerGroup)
     */
    @Test
    public void testWithNamespaceAndRetry() throws Exception {
        sendMsgRequest.addExtField("namespace", INSTNACE_ID);
        String retryTopic = NamespaceUtil.withNamespaceAndRetry(sendMsgRequest, GID);
        Assert.assertEquals(retryTopic, RETRY_TOPIC_WITH_NAMESPACE);
        Assert.assertTrue(NamespaceUtil.isRetryTopic(retryTopic));
    }

    /**
     * Method: getNamespaceFromRequest(RemotingCommand request)
     */
    @Test
    public void testGetNamespaceFromRequest() throws Exception {
        sendMsgRequest.addExtField("namespace", INSTNACE_ID);
        String namespace = NamespaceUtil.getNamespaceFromRequest(sendMsgRequest);
        Assert.assertEquals(namespace, INSTNACE_ID);
    }

    /**
     * Method: getNamespaceFromResource(String resource)
     */
    @Test
    public void testGetNamespaceFromResource() throws Exception {
        //TODO: Test goes here...
    }
} 
