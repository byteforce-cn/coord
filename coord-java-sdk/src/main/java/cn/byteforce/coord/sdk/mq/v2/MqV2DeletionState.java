package cn.byteforce.coord.sdk.mq.v2;

/** Reclamation state of a deleted topic ({@code TopicDeletionState}). */
public enum MqV2DeletionState {
    UNSPECIFIED,
    /** Marked deleted; reclamation is running. */
    PENDING,
    /** All partitions reclaimed, including consumer-group offsets. */
    DONE,
    /** Reclamation failed; the progress counters keep the last success. */
    FAILED
}
