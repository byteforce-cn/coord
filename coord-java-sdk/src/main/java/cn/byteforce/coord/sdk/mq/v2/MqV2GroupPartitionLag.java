package cn.byteforce.coord.sdk.mq.v2;

/**
 * Per-partition lag. {@code lag = max(nextOffset - committedOffset, 0)}; with no
 * committed record it equals {@code nextOffset}.
 */
public record MqV2GroupPartitionLag(String topic, int partition, long committedOffset,
                                    long nextOffset, long lag) {}
