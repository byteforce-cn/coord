package cn.byteforce.coord.sdk.workflow;

import java.util.Objects;

/**
 * A single version of a deployed workflow definition (rollback target discovery).
 * <p>
 * Returned by {@link WorkflowClient#listDefinitionVersions}. The semantic version
 * string is the definition's {@code document.version}; the {@code workflowId}
 * identifies that version's definition record.
 */
public final class WorkflowDefinitionVersion {

    private final String version;
    private final String workflowId;
    private final String status;
    private final long createdAt;

    public WorkflowDefinitionVersion(String version, String workflowId,
                                     String status, long createdAt) {
        this.version = version;
        this.workflowId = workflowId;
        this.status = status;
        this.createdAt = createdAt;
    }

    /** Semantic version string (e.g. "1.0"). */
    public String version() { return version; }

    /** Definition record ID of this version. */
    public String workflowId() { return workflowId; }

    public String status() { return status; }

    public long createdAt() { return createdAt; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WorkflowDefinitionVersion that)) return false;
        return createdAt == that.createdAt
                && Objects.equals(version, that.version)
                && Objects.equals(workflowId, that.workflowId)
                && Objects.equals(status, that.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, workflowId, status, createdAt);
    }

    @Override
    public String toString() {
        return "WorkflowDefinitionVersion{version='" + version + "', workflowId='"
                + workflowId + "', status='" + status + "'}";
    }
}
