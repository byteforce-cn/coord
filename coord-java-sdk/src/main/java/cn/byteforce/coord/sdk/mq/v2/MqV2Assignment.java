package cn.byteforce.coord.sdk.mq.v2;

/**
 * One partition assigned to the joining member. {@code committedOffset} is the
 * next offset to consume; 0 means "no committed record".
 */
public record MqV2Assignment(String topic, int partition, long committedOffset) {}
