package cn.byteforce.coord.sdk.workflow;

import java.util.Objects;

/**
 * Summary of a workflow instance (for list operations).
 */
public final class WorkflowInstanceSummary {

    private final String instanceId;
    private final String workflowId;
    private final String state;
    private final long startedAt;
    private final long updatedAt;
    private final String definitionName;

    public WorkflowInstanceSummary(String instanceId, String workflowId,
                                   String state, long startedAt, long updatedAt,
                                   String definitionName) {
        this.instanceId = instanceId;
        this.workflowId = workflowId;
        this.state = state;
        this.startedAt = startedAt;
        this.updatedAt = updatedAt;
        this.definitionName = definitionName;
    }

    // Backward-compatible constructor
    public WorkflowInstanceSummary(String instanceId, String workflowId,
                                   String state, long startedAt, long updatedAt) {
        this(instanceId, workflowId, state, startedAt, updatedAt, "");
    }

    public String instanceId() { return instanceId; }
    public String workflowId() { return workflowId; }
    public String state() { return state; }
    public long startedAt() { return startedAt; }
    public long updatedAt() { return updatedAt; }
    public String definitionName() { return definitionName; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WorkflowInstanceSummary that)) return false;
        return startedAt == that.startedAt && updatedAt == that.updatedAt
                && Objects.equals(instanceId, that.instanceId)
                && Objects.equals(workflowId, that.workflowId)
                && Objects.equals(state, that.state)
                && Objects.equals(definitionName, that.definitionName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(instanceId, workflowId, state, startedAt, updatedAt, definitionName);
    }

    @Override
    public String toString() {
        return "WorkflowInstanceSummary{instanceId='" + instanceId + "', workflowId='"
                + workflowId + "', state='" + state + "', definitionName='" + definitionName + "'}";
    }
}
