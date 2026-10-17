package cn.byteforce.coord.sdk.workflow;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * An approval task (席位) within an {@link ApprovalInstanceDetail}.
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

    public ApprovalTask(String id, String branchId, String roundId, String kind,
                        String status, String assignee, List<String> candidates,
                        long deadlineMs) {
        this(id, branchId, roundId, kind, status, assignee, candidates, deadlineMs,
                Collections.emptyList());
    }

    public ApprovalTask(String id, String branchId, String roundId, String kind,
                        String status, String assignee, List<String> candidates,
                        long deadlineMs, List<ApprovalVote> votes) {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ApprovalTask that)) return false;
        return deadlineMs == that.deadlineMs
                && Objects.equals(id, that.id)
                && Objects.equals(branchId, that.branchId)
                && Objects.equals(roundId, that.roundId)
                && Objects.equals(kind, that.kind)
                && Objects.equals(status, that.status)
                && Objects.equals(assignee, that.assignee)
                && Objects.equals(candidates, that.candidates)
                && Objects.equals(votes, that.votes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, branchId, roundId, kind, status, assignee,
                candidates, deadlineMs, votes);
    }

    @Override
    public String toString() {
        return "ApprovalTask{id='" + id + "', branchId='" + branchId + "', roundId='"
                + roundId + "', kind='" + kind + "', status='" + status + "', assignee='"
                + assignee + "', candidates=" + candidates + ", deadlineMs=" + deadlineMs
                + ", votes=" + votes + "}";
    }
}
