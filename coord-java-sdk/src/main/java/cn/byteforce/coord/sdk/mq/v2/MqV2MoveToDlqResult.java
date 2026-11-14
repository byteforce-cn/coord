package cn.byteforce.coord.sdk.mq.v2;

/** Result of {@code MoveToDlq}; {@code alreadyMoved} is an idempotent success. */
public record MqV2MoveToDlqResult(long dlqOffset, boolean alreadyMoved) {}
