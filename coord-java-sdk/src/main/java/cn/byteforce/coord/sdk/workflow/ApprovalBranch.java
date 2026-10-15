package cn.byteforce.coord.sdk.workflow;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * An approval branch within an {@link ApprovalInstanceDetail}.
 */
public final class ApprovalBranch {

    private final String id;
    private final String node;
    private final int attempt;
    private final String status;
    private final String join;
    private final String reject;
    private final List<String> roundIds;

    public ApprovalBranch(String id, String node, int attempt, String status,
                          String join, String reject, List<String> roundIds) {
        this.id = id;
        this.node = node;
        this.attempt = attempt;
        this.status = status;
        this.join = join;
        this.reject = reject;
        this.roundIds = roundIds != null
                ? Collections.unmodifiableList(roundIds) : Collections.emptyList();
    }

    public String id() { return id; }
    public String node() { return node; }
    public int attempt() { return attempt; }
    public String status() { return status; }
    public String join() { return join; }
    public String reject() { return reject; }
    public List<String> roundIds() { return roundIds; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ApprovalBranch that)) return false;
        return attempt == that.attempt
                && Objects.equals(id, that.id)
                && Objects.equals(node, that.node)
                && Objects.equals(status, that.status)
                && Objects.equals(join, that.join)
                && Objects.equals(reject, that.reject)
                && Objects.equals(roundIds, that.roundIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, node, attempt, status, join, reject, roundIds);
    }

    @Override
    public String toString() {
        return "ApprovalBranch{id='" + id + "', node='" + node + "', attempt=" + attempt
                + ", status='" + status + "', join='" + join + "', reject='" + reject
                + "', roundIds=" + roundIds + "}";
    }
}
