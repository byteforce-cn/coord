package cn.byteforce.coord.sdk.mq.v2;

/** Committed offset of one partition; {@code committed=false} means no record yet. */
public record MqV2CommittedOffset(String topic, int partition, long offset, boolean committed) {}
