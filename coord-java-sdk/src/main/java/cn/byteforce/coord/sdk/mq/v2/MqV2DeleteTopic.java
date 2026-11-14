package cn.byteforce.coord.sdk.mq.v2;

/** Delete a topic; {@code expectedTopicEpoch} behaves like {@link MqV2UpdateTopic}'s CAS. */
public record MqV2DeleteTopic(String topic, long expectedTopicEpoch) {}
