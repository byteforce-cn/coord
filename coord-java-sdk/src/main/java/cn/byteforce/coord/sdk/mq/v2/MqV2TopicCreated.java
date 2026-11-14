package cn.byteforce.coord.sdk.mq.v2;

/**
 * Result of {@code CreateTopic}. {@code topicId} is an identity that is never
 * reused; deleting and recreating a same-named topic yields a new id and epoch.
 */
public record MqV2TopicCreated(String topic, long topicId, long topicEpoch,
                               int partitionCount, MqV2TopicConfig config) {}
