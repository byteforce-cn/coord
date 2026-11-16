package cn.byteforce.coord.sdk.workflow;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * An approval task (席位). Used both by {@link ApprovalInstanceDetail} (instance-detail
 * projection) and by the read surface ({@link WorkflowClient#listTasks} /
 * {@link WorkflowClient#getTask}).
 *
 * <p>Fields that only the read surface provides ({@code instanceId}, {@code node},
 * {@code origin}, {@code createdAtMs}, the task-graph links and {@code assignments})
 * stay empty for instance-detail tasks — the {@code GetInstanceDetail} wire message
 * does not carry them.</p>
 */
public final class ApprovalTask {

    private final String id;
    private final String branchId;
    private final String roundId;
    private final String kind;
    private final String status;
    private final String assignee;
    private final List<String> candidates;
    private final long deadlineMs;
    private final List<ApprovalVote> votes;
    private final String instanceId;
    private final String node;
    private final String origin;
    private final long createdAtMs;
    private final String predecessorTaskId;
    private final String successorTaskId;
    private final String spawnedByActionId;
    private final List<ApprovalAssignment> assignments;

    public ApprovalTask(String id, String branchId, String roundId, String kind,
                        String status, String assignee, List<String> candidates,
                        long deadlineMs) {
        this(id, branchId, roundId, kind, status, assignee, candidates, deadlineMs,
                Collections.emptyList());
    }

    public ApprovalTask(String id, String branchId, String roundId, String kind,
                        String status, String assignee, List<String> candidates,
                        long deadlineMs, List<ApprovalVote> votes) {
        this(id, branchId, roundId, kind, status, assignee, candidates, deadlineMs, votes,
                "", "", "", 0L, "", "", "", Collections.emptyList());
    }

    /**
     * Full constructor, used by the read surface ({@code ListTasks} / {@code GetTask}).
     *
     * @param origin            seat origin ({@code APPROVAL} / {@code ASSIGNEE} / ...)
     * @param predecessorTaskId previous seat in a before-add-sign chain; empty otherwise
     * @param successorTaskId   next seat in an after-add-sign chain; empty otherwise
     * @param spawnedByActionId action id that derived this seat; empty for planned seats
     * @param assignments       per-principal assignment view (see {@link ApprovalAssignment})
     */
    public ApprovalTask(String id, String branchId, String roundId, String kind,
                        String status, String assignee, List<String> candidates,
                        long deadlineMs, List<ApprovalVote> votes,
                        String instanceId, String node, String origin, long createdAtMs,
                        String predecessorTaskId, String successorTaskId,
                        String spawnedByActionId, List<ApprovalAssignment> assignments) {
        this.id = id;
        this.branchId = branchId;
        this.roundId = roundId;
        this.kind = kind;
        this.status = status;
        this.assignee = assignee;
        this.candidates = candidates != null
                ? Collections.unmodifiableList(candidates) : Collections.emptyList();
        this.deadlineMs = deadlineMs;
        this.votes = votes != null
                ? Collections.unmodifiableList(votes) : Collections.emptyList();
        this.instanceId = instanceId != null ? instanceId : "";
        this.node = node != null ? node : "";
        this.origin = origin != null ? origin : "";
        this.createdAtMs = createdAtMs;
        this.predecessorTaskId = predecessorTaskId != null ? predecessorTaskId : "";
        this.successorTaskId = successorTaskId != null ? successorTaskId : "";
        this.spawnedByActionId = spawnedByActionId != null ? spawnedByActionId : "";
        this.assignments = assignments != null
                ? Collections.unmodifiableList(assignments) : Collections.emptyList();
    }

    public String id() { return id; }
    public String branchId() { return branchId; }
    public String roundId() { return roundId; }
    public String kind() { return kind; }
    public String status() { return status; }
    public String assignee() { return assignee; }
    public List<String> candidates() { return candidates; }

    /** Timeout threshold (Unix milliseconds); 0 = no timeout. */
    public long deadlineMs() { return deadlineMs; }

    /** Vote records; {@code actingService} distinguishes service on-behalf-of execution. */
    public List<ApprovalVote> votes() { return votes; }

    /** Owning workflow instance; empty for instance-detail tasks (the detail already scopes it). */
    public String instanceId() { return instanceId; }

    /** Node (step) name; empty for instance-detail tasks. */
    public String node() { return node; }

    /** Seat origin ({@code APPROVAL} / {@code ASSIGNEE} / ...); empty for instance-detail tasks. */
    public String origin() { return origin; }

    /** Seat creation time (Unix milliseconds); 0 for instance-detail tasks. */
    public long createdAtMs() { return createdAtMs; }

    /** Previous seat in a before-add-sign chain; empty otherwise. */
    public String predecessorTaskId() { return predecessorTaskId; }

    /** Next seat in an after-add-sign chain; empty otherwise. */
    public String successorTaskId() { return successorTaskId; }

    /** Action id that derived this seat; empty for planned seats. */
    public String spawnedByActionId() { return spawnedByActionId; }

    /** Per-principal assignment view; empty for instance-detail tasks. */
    public List<ApprovalAssignment> assignments() { return assignments; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ApprovalTask that)) return false;
        return deadlineMs == that.deadlineMs
                && createdAtMs == that.createdAtMs
                && Objects.equals(id, that.id)
                && Objects.equals(branchId, that.branchId)
                && Objects.equals(roundId, that.roundId)
                && Objects.equals(kind, that.kind)
                && Objects.equals(status, that.status)
                && Objects.equals(assignee, that.assignee)
                && Objects.equals(candidates, that.candidates)
                && Objects.equals(votes, that.votes)
                && Objects.equals(instanceId, that.instanceId)
                && Objects.equals(node, that.node)
                && Objects.equals(origin, that.origin)
                && Objects.equals(predecessorTaskId, that.predecessorTaskId)
                && Objects.equals(successorTaskId, that.successorTaskId)
                && Objects.equals(spawnedByActionId, that.spawnedByActionId)
                && Objects.equals(assignments, that.assignments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, branchId, roundId, kind, status, assignee,
                candidates, deadlineMs, votes, instanceId, node, origin, createdAtMs,
                predecessorTaskId, successorTaskId, spawnedByActionId, assignments);
    }

    @Override
    public String toString() {
        return "ApprovalTask{id='" + id + "', branchId='" + branchId + "', roundId='"
                + roundId + "', kind='" + kind + "', status='" + status + "', assignee='"
                + assignee + "', candidates=" + candidates + ", deadlineMs=" + deadlineMs
                + ", instanceId='" + instanceId + "', node='" + node + "', origin='" + origin
                + "', createdAtMs=" + createdAtMs + ", predecessorTaskId='" + predecessorTaskId
                + "', successorTaskId='" + successorTaskId + "', spawnedByActionId='"
                + spawnedByActionId + "', votes=" + votes + ", assignments=" + assignments + "}";
    }
}
