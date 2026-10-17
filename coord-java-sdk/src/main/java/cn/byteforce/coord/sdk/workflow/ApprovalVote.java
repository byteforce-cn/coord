package cn.byteforce.coord.sdk.workflow;

import java.util.Objects;

/**
 * A vote record on an approval seat.
 *
 * <p>{@code actor} is the represented end user (the caller for direct operations);
 * {@code actingService} is the service that executed the command on the user's behalf
 * (empty for direct operations); {@code onBehalfOf} keeps the delegation semantics
 * (the delegating principal).</p>
 */
public final class ApprovalVote {

    private final String roundId;
    private final String decision;
    private final String actor;
    private final String onBehalfOf;
    private final String comment;
    private final long atMs;
    private final String actingService;

    public ApprovalVote(String roundId, String decision, String actor, String onBehalfOf,
                        String comment, long atMs, String actingService) {
        this.roundId = roundId;
        this.decision = decision;
        this.actor = actor;
        this.onBehalfOf = onBehalfOf;
        this.comment = comment;
        this.atMs = atMs;
        this.actingService = actingService;
    }

    public String roundId() { return roundId; }
    public String decision() { return decision; }

    /** The represented end user (the caller for direct operations). */
    public String actor() { return actor; }

    /** Delegating principal for delegation votes; empty otherwise. */
    public String onBehalfOf() { return onBehalfOf; }
    public String comment() { return comment; }
    public long atMs() { return atMs; }

    /** Executing service identity for on-behalf-of commands; empty for direct operations. */
    public String actingService() { return actingService; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ApprovalVote that)) return false;
        return atMs == that.atMs
                && Objects.equals(roundId, that.roundId)
                && Objects.equals(decision, that.decision)
                && Objects.equals(actor, that.actor)
                && Objects.equals(onBehalfOf, that.onBehalfOf)
                && Objects.equals(comment, that.comment)
                && Objects.equals(actingService, that.actingService);
    }

    @Override
    public int hashCode() {
        return Objects.hash(roundId, decision, actor, onBehalfOf, comment, atMs, actingService);
    }

    @Override
    public String toString() {
        return "ApprovalVote{roundId='" + roundId + "', decision='" + decision
                + "', actor='" + actor + "', onBehalfOf='" + onBehalfOf
                + "', actingService='" + actingService + "', atMs=" + atMs + "}";
    }
}
