package cn.byteforce.coord.sdk.workflow;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Full approval detail of a workflow instance, returned by
 * {@link WorkflowClient#getInstanceDetail(String)}.
 */
public final class ApprovalInstanceDetail {

    private final String instanceId;
    private final String status;
    private final String initiator;
    private final long revision;
    private final String definitionName;
    private final List<ApprovalBranch> branches;
    private final List<ApprovalTask> tasks;
    private final List<ApprovalRound> rounds;

    public ApprovalInstanceDetail(String instanceId, String status, String initiator,
                                  long revision, String definitionName,
                                  List<ApprovalBranch> branches, List<ApprovalTask> tasks,
                                  List<ApprovalRound> rounds) {
        this.instanceId = instanceId;
        this.status = status;
        this.initiator = initiator;
        this.revision = revision;
        this.definitionName = definitionName;
        this.branches = branches != null
                ? Collections.unmodifiableList(branches) : Collections.emptyList();
        this.tasks = tasks != null
                ? Collections.unmodifiableList(tasks) : Collections.emptyList();
        this.rounds = rounds != null
                ? Collections.unmodifiableList(rounds) : Collections.emptyList();
    }

    public String instanceId() { return instanceId; }
    public String status() { return status; }
    public String initiator() { return initiator; }

    /** Optimistic concurrency guard value (CAS); incremented on every state change. */
    public long revision() { return revision; }

    public String definitionName() { return definitionName; }
    public List<ApprovalBranch> branches() { return branches; }
    public List<ApprovalTask> tasks() { return tasks; }
    public List<ApprovalRound> rounds() { return rounds; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ApprovalInstanceDetail that)) return false;
        return revision == that.revision
                && Objects.equals(instanceId, that.instanceId)
                && Objects.equals(status, that.status)
                && Objects.equals(initiator, that.initiator)
                && Objects.equals(definitionName, that.definitionName)
                && Objects.equals(branches, that.branches)
                && Objects.equals(tasks, that.tasks)
                && Objects.equals(rounds, that.rounds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(instanceId, status, initiator, revision, definitionName,
                branches, tasks, rounds);
    }

    @Override
    public String toString() {
        return "ApprovalInstanceDetail{instanceId='" + instanceId + "', status='" + status
                + "', initiator='" + initiator + "', revision=" + revision
                + ", definitionName='" + definitionName + "', branches=" + branches.size()
                + ", tasks=" + tasks.size() + ", rounds=" + rounds.size() + "}";
    }
}
