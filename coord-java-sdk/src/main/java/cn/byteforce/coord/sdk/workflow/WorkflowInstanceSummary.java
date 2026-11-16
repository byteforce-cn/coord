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
    private final String definitionNamespace;
    private final String definitionVersion;
    private final String initiator;
    private final long completedAt;

    /**
     * Full constructor (definition identity + initiator + completion time).
     *
     * @param definitionNamespace namespace of the pinned definition (may be empty)
     * @param definitionVersion   version of the pinned definition (may be empty)
     * @param initiator           the principal that started the instance (may be empty)
     * @param completedAt         terminal transition time in Unix ms; {@code 0} while running
     */
    public WorkflowInstanceSummary(String instanceId, String workflowId,
                                   String state, long startedAt, long updatedAt,
                                   String definitionName,
                                   String namespace, byte[] outputJson, byte[] contextJson,
                                   String definitionNamespace, String definitionVersion,
                                   String initiator, long completedAt) {
        this.instanceId = instanceId;
        this.workflowId = workflowId;
        this.state = state;
        this.startedAt = startedAt;
        this.updatedAt = updatedAt;
        this.definitionName = definitionName;
        this.namespace = namespace;
        this.outputJson = outputJson;
        this.contextJson = contextJson;
        this.definitionNamespace = definitionNamespace != null ? definitionNamespace : "";
        this.definitionVersion = definitionVersion != null ? definitionVersion : "";
        this.initiator = initiator != null ? initiator : "";
        this.completedAt = completedAt;
    }

    public WorkflowInstanceSummary(String instanceId, String workflowId,
                                   String state, long startedAt, long updatedAt,
                                   String definitionName,
                                   String namespace, byte[] outputJson, byte[] contextJson) {
        this(instanceId, workflowId, state, startedAt, updatedAt, definitionName, namespace,
                outputJson, contextJson, "", "", "", 0);
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

    /** Namespace of the definition pinned when the instance started (may be empty). */
    public String definitionNamespace() { return definitionNamespace; }

    /** Version of the definition pinned when the instance started (may be empty). */
    public String definitionVersion() { return definitionVersion; }

    /** The principal that started the instance (may be empty for legacy snapshots). */
    public String initiator() { return initiator; }

    /** Terminal transition time in Unix ms; {@code 0} while the instance is still running. */
    public long completedAt() { return completedAt; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WorkflowInstanceSummary that)) return false;
        return startedAt == that.startedAt && updatedAt == that.updatedAt
                && completedAt == that.completedAt
                && Objects.equals(instanceId, that.instanceId)
                && Objects.equals(workflowId, that.workflowId)
                && Objects.equals(state, that.state)
                && Objects.equals(definitionName, that.definitionName)
                && Objects.equals(namespace, that.namespace)
                && Objects.equals(definitionNamespace, that.definitionNamespace)
                && Objects.equals(definitionVersion, that.definitionVersion)
                && Objects.equals(initiator, that.initiator)
                && Arrays.equals(outputJson, that.outputJson)
                && Arrays.equals(contextJson, that.contextJson);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(instanceId, workflowId, state, startedAt, updatedAt,
                definitionName, namespace, definitionNamespace, definitionVersion, initiator,
                completedAt);
        result = 31 * result + Arrays.hashCode(outputJson);
        result = 31 * result + Arrays.hashCode(contextJson);
        return result;
    }

    @Override
    public String toString() {
        return "WorkflowInstanceSummary{instanceId='" + instanceId + "', workflowId='"
                + workflowId + "', state='" + state + "', definitionName='" + definitionName
                + "', definitionVersion='" + definitionVersion + "', initiator='" + initiator
                + "', namespace='" + namespace + "', completedAt=" + completedAt + "}";
    }
}
