package cn.byteforce.coord.sdk.mq.v2;

/** Result of {@code Ack}/{@code AckBatch}: committed is the next offset to consume. */
public record MqV2AckResult(String topic, int partition, long committedOffset) {}
