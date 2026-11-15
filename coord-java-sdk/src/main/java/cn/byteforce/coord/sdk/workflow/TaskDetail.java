package cn.byteforce.coord.sdk.workflow;

import java.util.List;

/**
 * One approval seat with its vote records, returned by
 * {@link WorkflowClient#getTask(String)}.
 *
 * <p>Unlike {@link PendingApprovalTask}, this carries {@link #candidates()} (the current
 * candidate pool) and {@link #votes()}.</p>
 *
 * @param id          the seat (task) ID
 * @param instanceId  the owning workflow instance ID
 * @param node        node (step) name
 * @param branchId    approval branch ID
 * @param roundId     approval round ID
 * @param kind        seat kind (e.g. {@code APPROVAL})
 * @param status      seat status: {@code created|claimed|completed|rejected|cancelled|expired}
 * @param assignee    the seat owner
 * @param candidates  the current candidate pool
 * @param deadlineMs  timeout threshold (Unix ms); 0 = no timeout
 * @param createdAtMs seat creation time (Unix ms)
 * @param votes       vote records on this seat
 */
public record TaskDetail(String id, String instanceId, String node, String branchId,
                         String roundId, String kind, String status, String assignee,
                         List<String> candidates, long deadlineMs, long createdAtMs,
                         List<ApprovalVote> votes) {
}
