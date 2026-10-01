package cn.byteforce.coord.sdk.workflow;

import java.util.Arrays;
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
    private final String namespace;
    private final byte[] outputJson;
    private final byte[] contextJson;

    public WorkflowInstanceSummary(String instanceId, String workflowId,
                                   String state, long startedAt, long updatedAt,
                                   String definitionName,
                                   String namespace, byte[] outputJson, byte[] contextJson) {
        this.instanceId = instanceId;
        this.workflowId = workflowId;
        this.state = state;
        this.startedAt = startedAt;
        this.updatedAt = updatedAt;
        this.definitionName = definitionName;
        this.namespace = namespace;
        this.outputJson = outputJson;
        this.contextJson = contextJson;
    }

    // Backward-compatible constructor (namespace empty, no output/context)
    public WorkflowInstanceSummary(String instanceId, String workflowId,
                                   String state, long startedAt, long updatedAt,
                                   String definitionName) {
        this(instanceId, workflowId, state, startedAt, updatedAt, definitionName,
                "", null, null);
    }

    // Backward-compatible constructor (without definitionName)
    public WorkflowInstanceSummary(String instanceId, String workflowId,
                                   String state, long startedAt, long updatedAt) {
        this(instanceId, workflowId, state, startedAt, updatedAt, "", "", null, null);
    }

    public String instanceId() { return instanceId; }
    public String workflowId() { return workflowId; }
    public String state() { return state; }
    public long startedAt() { return startedAt; }
    public long updatedAt() { return updatedAt; }
    public String definitionName() { return definitionName; }

    /** The namespace this instance belongs to. */
    public String namespace() { return namespace; }

    /** The output data (JSON bytes), or null if not available. */
    public byte[] outputJson() { return outputJson; }

    /** The runtime context (JSON bytes), or null if not available. */
    public byte[] contextJson() { return contextJson; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WorkflowInstanceSummary that)) return false;
        return startedAt == that.startedAt && updatedAt == that.updatedAt
                && Objects.equals(instanceId, that.instanceId)
                && Objects.equals(workflowId, that.workflowId)
                && Objects.equals(state, that.state)
                && Objects.equals(definitionName, that.definitionName)
                && Objects.equals(namespace, that.namespace)
                && Arrays.equals(outputJson, that.outputJson)
                && Arrays.equals(contextJson, that.contextJson);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(instanceId, workflowId, state, startedAt, updatedAt,
                definitionName, namespace);
        result = 31 * result + Arrays.hashCode(outputJson);
        result = 31 * result + Arrays.hashCode(contextJson);
        return result;
    }

    @Override
    public String toString() {
        return "WorkflowInstanceSummary{instanceId='" + instanceId + "', workflowId='"
                + workflowId + "', state='" + state + "', definitionName='" + definitionName
                + "', namespace='" + namespace + "'}";
    }
}
