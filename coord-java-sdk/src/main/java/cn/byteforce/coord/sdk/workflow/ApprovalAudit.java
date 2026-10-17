package cn.byteforce.coord.sdk.workflow;

import java.util.Objects;

/**
 * A management-action audit entry (add/remove sign, return to node, withdraw, terminate).
 * {@code actingService} distinguishes the executing service from {@code by} (the end user).
 */
public final class ApprovalAudit {

    private final long atMs;
    private final String action;
    private final String branchId;
    private final String by;
    private final String detail;
    private final String actingService;

    public ApprovalAudit(long atMs, String action, String branchId, String by, String detail,
                         String actingService) {
        this.atMs = atMs;
        this.action = action;
        this.branchId = branchId;
        this.by = by;
        this.detail = detail;
        this.actingService = actingService;
    }

    public long atMs() { return atMs; }
    public String action() { return action; }
    public String branchId() { return branchId; }

    /** The represented end user (the caller for direct operations). */
    public String by() { return by; }
    public String detail() { return detail; }

    /** Executing service identity for on-behalf-of commands; empty for direct operations. */
    public String actingService() { return actingService; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ApprovalAudit that)) return false;
        return atMs == that.atMs
                && Objects.equals(action, that.action)
                && Objects.equals(branchId, that.branchId)
                && Objects.equals(by, that.by)
                && Objects.equals(detail, that.detail)
                && Objects.equals(actingService, that.actingService);
    }

    @Override
    public int hashCode() {
        return Objects.hash(atMs, action, branchId, by, detail, actingService);
    }

    @Override
    public String toString() {
        return "ApprovalAudit{action='" + action + "', by='" + by
                + "', actingService='" + actingService + "', atMs=" + atMs + "}";
    }
}
