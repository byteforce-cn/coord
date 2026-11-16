package cn.byteforce.coord.sdk.workflow;

/**
 * Where an added signer lands (加签位置).
 *
 * <p>{@link #CURRENT} appends seats to the branch's current round (the original
 * {@code addSign} semantics). {@link #BEFORE} / {@link #AFTER} derive a separate
 * task anchored on {@code taskId}; the server rejects them with
 * {@code INVALID_ARGUMENT} when the anchor seat is missing.</p>
 */
public enum AddAssigneePosition {
    /** Append to the current round. */
    CURRENT,
    /** Derive a task before the anchored seat (the original round is suspended). */
    BEFORE,
    /** Derive a task after the anchored seat (runs once the original round passes). */
    AFTER
}
