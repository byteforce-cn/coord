package cn.byteforce.coord.sdk.workflow;

import java.util.Objects;

/**
 * Summary of a workflow definition (for list operations).
 */
public final class WorkflowDefinitionSummary {

    private final String workflowId;
    private final String name;
    private final String version;
    private final String status;
    private final long createdAt;

    public WorkflowDefinitionSummary(String workflowId, String name,
                                     String version, String status, long createdAt) {
        this.workflowId = workflowId;
        this.name = name;
        this.version = version;
        this.status = status;
        this.createdAt = createdAt;
    }

    public String workflowId() { return workflowId; }
    public String name() { return name; }
    public String version() { return version; }
    public String status() { return status; }
    public long createdAt() { return createdAt; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WorkflowDefinitionSummary that)) return false;
        return createdAt == that.createdAt
                && Objects.equals(workflowId, that.workflowId)
                && Objects.equals(name, that.name)
                && Objects.equals(version, that.version)
                && Objects.equals(status, that.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(workflowId, name, version, status, createdAt);
    }

    @Override
    public String toString() {
        return "WorkflowDefinitionSummary{workflowId='" + workflowId + "', name='"
                + name + "', version=" + version + ", status='" + status + "'}";
    }
}
