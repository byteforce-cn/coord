package cn.byteforce.coord.sdk.mq.v2;

/**
 * Heartbeat result. {@code rebalanceRequired} means the generation advanced
 * (member change / session expiry): the member must rejoin before polling or
 * acking again.
 */
public record MqV2Heartbeat(long generation, boolean rebalanceRequired) {}
