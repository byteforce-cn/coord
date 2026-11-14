package cn.byteforce.coord.sdk.mq.v2;

/**
 * Update a topic's retention/quota/message limits. {@code expectedTopicEpoch}
 * is a CAS: 0 skips the check, non-zero must match the current epoch. Partition
 * count is immutable.
 */
public record MqV2UpdateTopic(String topic, long expectedTopicEpoch, MqV2TopicConfig config) {}
