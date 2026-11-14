package cn.byteforce.coord.sdk.mq.v2;

/**
 * Result of {@code Publish} / one entry of {@code PublishBatch}.
 * {@code deduplicated} means the idempotency key hit the dedup index and this
 * is the first offset, not a new append.
 */
public record MqV2PublishResult(long offset, long timestampMs, boolean deduplicated) {}
