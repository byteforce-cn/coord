package cn.byteforce.coord.sdk.mq.v2;

/** A {@code (topic, partition)} pair used by {@code GetCommittedOffsets}. */
public record MqV2TopicPartition(String topic, int partition) {}
