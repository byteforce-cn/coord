package cn.byteforce.coord.sdk.workflow;

import java.util.Objects;

/**
 * An approval round within an {@link ApprovalInstanceDetail}.
 */
public final class ApprovalRound {

    private final String id;
    private final String branchId;
    private final int number;
    private final String status;
    private final int totalCount;
    private final int completedCount;
    private final int rejectedCount;
    private final String rejectReason;
    private final long openedAtMs;
    private final long closedAtMs;

    public ApprovalRound(String id, String branchId, int number, String status,
                         int totalCount, int completedCount, int rejectedCount) {
        this(id, branchId, number, status, totalCount, completedCount, rejectedCount, "");
    }

    public ApprovalRound(String id, String branchId, int number, String status,
                         int totalCount, int completedCount, int rejectedCount,
                         String rejectReason) {
        this(id, branchId, number, status, totalCount, completedCount, rejectedCount,
                rejectReason, 0L, 0L);
    }

    public ApprovalRound(String id, String branchId, int number, String status,
                         int totalCount, int completedCount, int rejectedCount,
                         String rejectReason, long openedAtMs, long closedAtMs) {
        this.id = id;
        this.branchId = branchId;
        this.number = number;
        this.status = status;
        this.totalCount = totalCount;
        this.completedCount = completedCount;
        this.rejectedCount = rejectedCount;
        this.rejectReason = rejectReason != null ? rejectReason : "";
        this.openedAtMs = openedAtMs;
        this.closedAtMs = closedAtMs;
    }

    public String id() { return id; }
    public String branchId() { return branchId; }
    public int number() { return number; }
    public String status() { return status; }
    public int totalCount() { return totalCount; }
    public int completedCount() { return completedCount; }
    public int rejectedCount() { return rejectedCount; }

    /** Decision reason when the round was rejected ({@code any_reject} / ...); empty otherwise. */
    public String rejectReason() { return rejectReason; }

    /** Round opened time (Unix milliseconds). */
    public long openedAtMs() { return openedAtMs; }

    /** Round closed time (Unix milliseconds); 0 while open. */
    public long closedAtMs() { return closedAtMs; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ApprovalRound that)) return false;
        return number == that.number && totalCount == that.totalCount
                && completedCount == that.completedCount
                && rejectedCount == that.rejectedCount
                && openedAtMs == that.openedAtMs
                && closedAtMs == that.closedAtMs
                && Objects.equals(id, that.id)
                && Objects.equals(branchId, that.branchId)
                && Objects.equals(status, that.status)
                && Objects.equals(rejectReason, that.rejectReason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, branchId, number, status, totalCount,
                completedCount, rejectedCount, rejectReason, openedAtMs, closedAtMs);
    }

    @Override
    public String toString() {
        return "ApprovalRound{id='" + id + "', branchId='" + branchId + "', number=" + number
                + ", status='" + status + "', totalCount=" + totalCount
                + ", completedCount=" + completedCount + ", rejectedCount=" + rejectedCount
                + ", openedAtMs=" + openedAtMs + ", closedAtMs=" + closedAtMs + "}";
    }
}
