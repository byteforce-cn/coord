package cn.byteforce.coord.sdk.mq.v2;

/**
 * What a consumer does when its cursor falls out of the retention window
 * ({@code OFFSET_OUT_OF_RANGE}). The window is the topic retention period.
 */
public enum MqV2ResetPolicy {
    /** Start at the earliest available offset (accepts that history is gone). */
    EARLIEST,
    /** Jump to latest (accepts that the backlog is gone). */
    LATEST,
    /** Surface the error (default: never drop data silently). */
    FAIL
}
