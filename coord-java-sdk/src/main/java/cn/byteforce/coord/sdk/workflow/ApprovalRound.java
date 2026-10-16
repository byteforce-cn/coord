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

    public ApprovalRound(String id, String branchId, int number, String status,
                         int totalCount, int completedCount, int rejectedCount) {
        this.id = id;
        this.branchId = branchId;
        this.number = number;
        this.status = status;
        this.totalCount = totalCount;
        this.completedCount = completedCount;
        this.rejectedCount = rejectedCount;
    }

    public String id() { return id; }
    public String branchId() { return branchId; }
    public int number() { return number; }
    public String status() { return status; }
    public int totalCount() { return totalCount; }
    public int completedCount() { return completedCount; }
    public int rejectedCount() { return rejectedCount; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ApprovalRound that)) return false;
        return number == that.number && totalCount == that.totalCount
                && completedCount == that.completedCount
                && rejectedCount == that.rejectedCount
                && Objects.equals(id, that.id)
                && Objects.equals(branchId, that.branchId)
                && Objects.equals(status, that.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, branchId, number, status, totalCount,
                completedCount, rejectedCount);
    }

    @Override
    public String toString() {
        return "ApprovalRound{id='" + id + "', branchId='" + branchId + "', number=" + number
                + ", status='" + status + "', totalCount=" + totalCount
                + ", completedCount=" + completedCount + ", rejectedCount=" + rejectedCount + "}";
    }
}
