package cn.byteforce.coord.sdk.workflow;

import java.util.Objects;

/**
 * A pending approval seat (待办审批席位) returned by
 * {@link WorkflowClient#listPendingTasks(int, String)}.
 */
public final class PendingApprovalTask {

    private final String instanceId;
    private final String taskId;
    private final String node;
    private final String branchId;
    private final String roundId;
    private final String kind;
    private final String status;
    private final String assignee;
    private final long deadlineMs;
    private final long createdAtMs;

    public PendingApprovalTask(String instanceId, String taskId, String node,
                               String branchId, String roundId, String kind,
                               String status, String assignee,
                               long deadlineMs, long createdAtMs) {
        this.instanceId = instanceId;
        this.taskId = taskId;
        this.node = node;
        this.branchId = branchId;
        this.roundId = roundId;
        this.kind = kind;
        this.status = status;
        this.assignee = assignee;
        this.deadlineMs = deadlineMs;
        this.createdAtMs = createdAtMs;
    }

    public String instanceId() { return instanceId; }
    public String taskId() { return taskId; }
    public String node() { return node; }
    public String branchId() { return branchId; }
    public String roundId() { return roundId; }
    public String kind() { return kind; }
    public String status() { return status; }
    public String assignee() { return assignee; }

    /** Timeout threshold (Unix milliseconds); 0 = no timeout. */
    public long deadlineMs() { return deadlineMs; }

    /** Seat creation time (Unix milliseconds). */
    public long createdAtMs() { return createdAtMs; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PendingApprovalTask that)) return false;
        return deadlineMs == that.deadlineMs && createdAtMs == that.createdAtMs
                && Objects.equals(instanceId, that.instanceId)
                && Objects.equals(taskId, that.taskId)
                && Objects.equals(node, that.node)
                && Objects.equals(branchId, that.branchId)
                && Objects.equals(roundId, that.roundId)
                && Objects.equals(kind, that.kind)
                && Objects.equals(status, that.status)
                && Objects.equals(assignee, that.assignee);
    }

    @Override
    public int hashCode() {
        return Objects.hash(instanceId, taskId, node, branchId, roundId,
                kind, status, assignee, deadlineMs, createdAtMs);
    }

    @Override
    public String toString() {
        return "PendingApprovalTask{instanceId='" + instanceId + "', taskId='" + taskId
                + "', node='" + node + "', branchId='" + branchId + "', roundId='" + roundId
                + "', kind='" + kind + "', status='" + status + "', assignee='" + assignee
                + "', deadlineMs=" + deadlineMs + ", createdAtMs=" + createdAtMs + "}";
    }
}
