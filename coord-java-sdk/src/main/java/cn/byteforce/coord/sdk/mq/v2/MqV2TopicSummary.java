package cn.byteforce.coord.sdk.mq.v2;

/** One row of {@code ListTopics}. */
public record MqV2TopicSummary(String topic, long topicId, long topicEpoch,
                               int partitionCount, MqV2TopicConfig config) {}
