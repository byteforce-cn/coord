package cn.byteforce.coord.sdk.mq.v2;

/** Consumer-group lifecycle state ({@code GroupState}). */
public enum MqV2GroupState {
    UNSPECIFIED,
    /** No members; offsets are kept until their TTL. */
    EMPTY,
    STABLE,
    /** Member change seen; waiting for members to rejoin. */
    PREPARING_REBALANCE,
    /** All sessions expired and offsets passed their TTL (the group is reclaimed). */
    DEAD
}
