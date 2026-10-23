package cn.byteforce.coord.sdk.lease.v2;

import cn.byteforce.coord.sdk.CoordException;

import java.util.List;

/**
 * Global leases ({@code coord.lease.v2}).
 *
 * <p>The lease table lives in region 0; bound keys live in their data regions.
 * {@link #revoke} returns the {@code commit_ts} of the atomic deletion decision
 * ({@code 0} means no new decision: the lease was already revoked/expired, an
 * idempotent success). {@link #keepAlive} only extends the TTL and produces no
 * {@code commit_ts}.
 */
public interface LeaseClient {

    /** Grant result. {@code grantedTs} is the TSO at grant time (informational). */
    record Grant(long leaseId, long ttlSeconds, long grantedTs) {}

    /** Time-to-live result; {@code ttlSeconds <= 0} means expired. */
    record State(long leaseId, long ttlSeconds, long grantedTtlSeconds, List<byte[]> keys) {}

    /** Grant an auto-assigned lease. */
    default Grant grant(long ttlSeconds) {
        return grant(ttlSeconds, 0, new byte[0]);
    }

    /**
     * Grant a lease.
     *
     * @param leaseId   0 = server-assigned; {@code >0} = requested id
     *                  (conflict returns {@code ALREADY_EXISTS})
     * @param requestId idempotency key; empty means no dedup
     */
    Grant grant(long ttlSeconds, long leaseId, byte[] requestId);

    /**
     * Revoke a lease and atomically delete its bound keys.
     *
     * @return the deletion {@code commit_ts}, or {@code 0} when there was no new
     *         decision (already revoked/expired)
     */
    long revoke(long leaseId, byte[] requestId);

    /** Single keep-alive round trip; returns the remaining TTL in seconds. */
    long keepAlive(long leaseId);

    /**
     * Query the remaining TTL; {@code keys=true} also returns the bound keys
     * (server may truncate the list at its return limit).
     */
    State timeToLive(long leaseId, boolean keys);
}
