package cn.byteforce.coord.sdk.kv.v2;

import cn.byteforce.coord.sdk.CoordException;

import java.util.List;

/**
 * KV-T v2 key/value data plane ({@code coord.kv.v2}).
 *
 * <p>Semantics (ADR-0015 D3/D12, {@code coord/kv/v2/kv.proto}):
 * <ul>
 *   <li>{@code revision == commit_ts}; all keys of one transaction share the
 *       same {@code commit_ts};</li>
 *   <li>writes require an explicit, TSO-issued {@code commitTs > 0} — the
 *       server never allocates one per key (multi-key atomicity needs one
 *       timestamp);</li>
 *   <li>{@code readTs = 0} means the server's local {@code safe_ts}; a
 *       {@code readTs} above it waits up to {@code [kv_t].read_ts_wait_ms}
 *       then fails with {@code SAFE_TS_LAG} (retryable);</li>
 *   <li>the agent forwards requests to the server; callers do not carry the
 *       routing epoch themselves.</li>
 * </ul>
 */
public interface KvClient {

    /** A stored key. Empty {@code value} is a tombstone in low-level responses. */
    record KeyValue(byte[] key, byte[] value, long createRevision, long modRevision,
                    long version, long leaseId) {}

    /** Put result; {@code prevKv} is non-null only when requested and present. */
    record PutResult(KeyValue prevKv, long commitTs) {}

    /** Range result; {@code readTs} echoes the snapshot actually used. */
    record RangeResult(List<KeyValue> kvs, long count, long readTs) {}

    /** Delete result; {@code prevKvs} is filled only when requested. */
    record DeleteResult(long deleted, List<KeyValue> prevKvs, long commitTs) {}

    /**
     * Convenience: allocate one fresh timestamp from {@code coord.tso.v2}.
     * Equivalent to {@code client.tsoV2().getTimestamp()}.
     */
    long getCommitTs();

    /**
     * Put one key at {@code commitTs}.
     *
     * @param requestId idempotency key; empty means no dedup
     */
    PutResult put(byte[] key, byte[] value, long commitTs, boolean prevKv, byte[] requestId);

    /** Put with {@code prevKv=false} and no idempotency key. */
    default PutResult put(byte[] key, byte[] value, long commitTs) {
        return put(key, value, commitTs, false, new byte[0]);
    }

    /** Put with a fresh TSO timestamp. */
    default PutResult put(byte[] key, byte[] value) {
        return put(key, value, getCommitTs(), false, new byte[0]);
    }

    /**
     * Put one key bound to a lease. The server rejects unknown/revoked/expired
     * leases with {@code FAILED_PRECONDITION}.
     */
    PutResult putWithLease(byte[] key, byte[] value, long leaseId, long commitTs,
                           boolean prevKv, byte[] requestId);

    /**
     * Range read.
     *
     * @param rangeEnd empty = exact key; otherwise {@code [key, rangeEnd)}
     * @param limit    0 = unlimited (still bounded by the server's limits)
     * @param readTs   0 = server local {@code safe_ts}
     */
    RangeResult range(byte[] key, byte[] rangeEnd, long limit, long readTs,
                      boolean keysOnly, boolean countOnly);

    /** Exact-key read at the current {@code safe_ts}. */
    default RangeResult range(byte[] key) {
        return range(key, new byte[0], 0, 0, false, false);
    }

    /** Range read at the current {@code safe_ts}. */
    default RangeResult range(byte[] key, byte[] rangeEnd) {
        return range(key, rangeEnd, 0, 0, false, false);
    }

    /**
     * Delete a single key or {@code [key, rangeEnd)} at {@code commitTs}.
     * A delete of an absent key succeeds with {@code deleted = 0}.
     */
    DeleteResult delete(byte[] key, byte[] rangeEnd, long commitTs, boolean prevKv,
                        byte[] requestId);

    /** Delete one key with a fresh TSO timestamp. */
    default DeleteResult delete(byte[] key) {
        return delete(key, new byte[0], getCommitTs(), false, new byte[0]);
    }
}
