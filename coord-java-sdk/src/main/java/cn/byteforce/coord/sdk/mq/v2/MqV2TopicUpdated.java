package cn.byteforce.coord.sdk.mq.v2;

/** Result of {@code UpdateTopic}; {@code topicEpoch} is the epoch after the update. */
public record MqV2TopicUpdated(String topic, long topicEpoch, MqV2TopicConfig config) {}
