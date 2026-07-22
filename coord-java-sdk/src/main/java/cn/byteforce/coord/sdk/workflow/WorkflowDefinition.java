package cn.byteforce.coord.sdk.workflow;

import java.util.Objects;

/**
 * A deployed workflow definition.
 */
public final class WorkflowDefinition {

    private final String workflowId;
    private final String name;
    private final String definitionYaml;
    private final String version;
    private final String status;
    private final long createdAt;

    public WorkflowDefinition(String workflowId, String name, String definitionYaml,
                              String version, String status, long createdAt) {
        this.workflowId = workflowId;
        this.name = name;
        this.definitionYaml = definitionYaml;
        this.version = version;
        this.status = status;
        this.createdAt = createdAt;
    }

    public String workflowId() { return workflowId; }
    public String name() { return name; }
    public String definitionYaml() { return definitionYaml; }
    public String version() { return version; }
    public String status() { return status; }
    public long createdAt() { return createdAt; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WorkflowDefinition that)) return false;
        return createdAt == that.createdAt
                && Objects.equals(workflowId, that.workflowId)
                && Objects.equals(name, that.name)
                && Objects.equals(definitionYaml, that.definitionYaml)
                && Objects.equals(version, that.version)
                && Objects.equals(status, that.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(workflowId, name, definitionYaml, version, status, createdAt);
    }

    @Override
    public String toString() {
        return "WorkflowDefinition{workflowId='" + workflowId + "', name='" + name
                + "', version=" + version + ", status='" + status + "'}";
    }
}
