package cn.byteforce.coord.sdk.workflow;

import java.util.List;

/**
 * One round with all of its seats — the coord projection of a seedj {@code HumanTask}.
 *
 * <p>A coord seat ({@link ApprovalTask}) is participant-level; a round is step-level. This
 * view aggregates the whole round, so a multi-seat countersign task needs one read instead of
 * one call per instance.</p>
 *
 * @param id           the round id (stable; feed a seat id to {@code getTask} for votes)
 * @param instanceId   owning workflow instance
 * @param branchId     owning branch
 * @param roundId      round id (equals {@link #id})
 * @param node         node (step) name
 * @param status       round status label ({@code open} / {@code passed} / {@code rejected} / ...)
 * @param kind         {@code approval} when the round has any approval seat, else {@code read_ack}
 * @param tasks        every seat of the round, ordered by creation
 * @param assignments  assignments flattened across the round's seats
 * @param createdAtMs  round opened at (Unix ms)
 * @param deadlineMs   earliest pending seat deadline (Unix ms); {@code 0} = none
 */
public record TaskGroup(String id, String instanceId, String branchId, String roundId,
                        String node, String status, String kind, List<ApprovalTask> tasks,
                        List<ApprovalAssignment> assignments, long createdAtMs,
                        long deadlineMs) {

    public TaskGroup {
        tasks = tasks == null ? List.of() : List.copyOf(tasks);
        assignments = assignments == null ? List.of() : List.copyOf(assignments);
    }
}
