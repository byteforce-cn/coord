package cn.byteforce.coord.sdk.mq.v2;

/**
 * Commit one processed offset. {@code offset} is the last processed offset
 * (inclusive); the server moves committed to {@code offset + 1}. Acking an older
 * offset is an idempotent success, never a rollback.
 */
public record MqV2Ack(String topic, int partition, long topicEpoch, long offset) {}
