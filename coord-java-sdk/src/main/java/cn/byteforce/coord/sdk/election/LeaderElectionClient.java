package cn.byteforce.coord.sdk.election;

import cn.byteforce.coord.sdk.CoordException;

import java.util.function.Consumer;

/**
 * Distributed leader election API.
 * <p>
 * Provides lease-based leader election: at most one candidate is leader for a
 * given group at any moment, and the leadership must be renewed within the TTL
 * before it expires.
 *
 * <pre>{@code
 * try (CoordClient client = CoordClient.create(config)) {
 *     LeaderElectionClient election = client.election();
 *     LeaderLease lease = election.campaign("daily-report", "node-1", 30);
 *     if (lease.isElected()) {
 *         try {
 *             // do leader-only work
 *         } finally {
 *             election.resign("daily-report", "node-1", lease.getLeaseId());
 *         }
 *     }
 * }
 * }</pre>
 */
public interface LeaderElectionClient {

    /**
     * Attempt to become the leader of {@code groupName} (or renew if already leader).
     *
     * @param groupName  election group name
     * @param candidateId this candidate's unique identifier
     * @param ttlSeconds term lease TTL in seconds; must be re-campaigned within it
     * @return the campaign outcome; {@link LeaderLease#isElected()} is {@code false}
     *         when another live leader holds the group (a normal, non-error outcome)
     * @throws CoordException on communication failure
     */
    LeaderLease campaign(String groupName, String candidateId, long ttlSeconds);

    /**
     * Resign leadership. Only the current leader's own
     * {@code (candidateId, leaseId)} can resign; others get {@code false}.
     *
     * @return true if this call stepped down
     * @throws CoordException on communication failure
     */
    boolean resign(String groupName, String candidateId, long leaseId);

    /**
     * Query the current leader. A linearizable read.
     *
     * @return the current leader, or {@code null} if no leader exists
     * @throws CoordException on communication failure
     */
    Leader currentLeader(String groupName);

    /**
     * Subscribe to leader-change events for a group.
     * <p>
     * Delivery is at-least-once; on reconnect, re-establish state via
     * {@link #currentLeader(String)} before continuing.
     *
     * @param groupName       election group name
     * @param onEvent         invoked for every event
     * @param onError         invoked if the stream terminates with an error;
     *                        may be {@code null}
     * @return a handle that stops the subscription when cancelled
     * @throws CoordException on communication failure while establishing the stream
     */
    AutoCloseable watch(String groupName, Consumer<LeaderChangeEvent> onEvent,
                        Consumer<Throwable> onError);

    /** Result of a campaign attempt. */
    final class LeaderLease {
        private final boolean elected;
        private final long leaseId;
        private final String leaderId;

        public LeaderLease(boolean elected, long leaseId, String leaderId) {
            this.elected = elected;
            this.leaseId = leaseId;
            this.leaderId = leaderId;
        }

        /** Whether this candidate is (or remains) the leader. */
        public boolean isElected() {
            return elected;
        }

        /** The term lease id; pass it back to {@link #resign}. */
        public long getLeaseId() {
            return leaseId;
        }

        /** The elected candidate id (equals the candidate we campaigned with). */
        public String getLeaderId() {
            return leaderId;
        }
    }

    /** Snapshot of the current leader, as returned by {@link #currentLeader}. */
    final class Leader {
        private final String leaderId;
        private final long leaseId;
        private final long electedAt;

        public Leader(String leaderId, long leaseId, long electedAt) {
            this.leaderId = leaderId;
            this.leaseId = leaseId;
            this.electedAt = electedAt;
        }

        public String getLeaderId() {
            return leaderId;
        }

        public long getLeaseId() {
            return leaseId;
        }

        /** Unix seconds when the current term started. */
        public long getElectedAt() {
            return electedAt;
        }
    }

    /** Kind of leader change, mirroring the contract's {@code LeaderWatchEvent.EventType}. */
    enum LeaderChangeKind {
        UNKNOWN,
        ELECTED,
        RESIGNED,
        EXPIRED
    }

    /** A single leader-change event. */
    final class LeaderChangeEvent {
        private final LeaderChangeKind kind;
        private final String groupName;
        private final String leaderId;

        public LeaderChangeEvent(LeaderChangeKind kind, String groupName, String leaderId) {
            this.kind = kind;
            this.groupName = groupName;
            this.leaderId = leaderId;
        }

        public LeaderChangeKind getKind() {
            return kind;
        }

        public String getGroupName() {
            return groupName;
        }

        /** For {@link LeaderChangeKind#EXPIRED} this is the leader that lost the term. */
        public String getLeaderId() {
            return leaderId;
        }
    }
}
