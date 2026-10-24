package cn.byteforce.coord.sdk.txn.v2;

import cn.byteforce.coord.sdk.CoordException;
import cn.byteforce.coord.sdk.kv.v2.KvClient;

import java.util.List;

/**
 * KV-T v2 serializable transactions ({@code coord.txn.v2}).
 *
 * <p>Semantics (ADR-0015 D4/D12, {@code coord/txn/v2/txn.proto}):
 * <ul>
 *   <li>{@link #begin} creates a <b>server-side session</b> pinned to the
 *       coordinator node that answered it; all session calls reuse that node.
 *       A crash/restart loses the session ({@code TXN_NOT_FOUND}); the
 *       transaction must be replayed from a fresh {@code start_ts}. Atomicity
 *       is unaffected: commit is a single Commit Sequencer decision.</li>
 *   <li>{@link Session#read} runs on the {@code start_ts} snapshot and lets the
 *       server record the read set; clients cannot omit read-set entries.</li>
 *   <li>{@link Session#commit} validates the read set in
 *       {@code (startTs, commitTs]}. A conflict is returned as
 *       {@link CommitResult#committed()} {@code == false} — <b>not</b> thrown:
 *       the caller must replay the transaction with a new {@code start_ts}.
 *       Hiding that replay would turn "read an old snapshot" into an
 *       unobservable duplicate read.</li>
 *   <li>{@code commitTs} is returned once the decision is durable; per-region
 *       apply is asynchronous until {@code commitTs <= safe_ts}.</li>
 * </ul>
 *
 * <p>A conflicted or committed session is finished; use a new {@link #begin}.
 * Unfinished sessions are reclaimed by the server after their TTL.
 */
public interface TxnClient {

    /** A write-set entry; {@code OP_UNSPECIFIED} is rejected by the server. */
    record Mutation(Op op, byte[] key, byte[] rangeEnd, byte[] value, long leaseId) {

        public enum Op { PUT, DELETE, DELETE_RANGE }

        public static Mutation put(byte[] key, byte[] value) {
            return new Mutation(Op.PUT, key, new byte[0], value, 0);
        }

        public static Mutation put(byte[] key, byte[] value, long leaseId) {
            return new Mutation(Op.PUT, key, new byte[0], value, leaseId);
        }

        public static Mutation delete(byte[] key) {
            return new Mutation(Op.DELETE, key, new byte[0], new byte[0], 0);
        }

        /** Delete {@code [key, rangeEnd)}; {@code rangeEnd} must be non-empty. */
        public static Mutation deleteRange(byte[] key, byte[] rangeEnd) {
            return new Mutation(Op.DELETE_RANGE, key, rangeEnd, new byte[0], 0);
        }
    }

    /** Snapshot read result; {@code readTs} echoes the transaction's {@code startTs}. */
    record ReadResult(List<KvClient.KeyValue> kvs, long readTs) {}

    /**
     * Commit outcome. {@code committed == false} means the OCC validation found
     * a conflict: no {@code CommitDecision} was written and {@code commitTs} is 0.
     */
    record CommitResult(boolean committed, long commitTs) {

        /** The read set/predicate changed inside {@code (startTs, commitTs]}. */
        public static final CommitResult CONFLICT = new CommitResult(false, 0);
    }

    /** Begin a session; {@code startTs = 0} lets the server fetch a fresh TSO stamp. */
    Session begin(long startTs, int ttlMs);

    /** Begin a session with a server-allocated {@code startTs} and the default TTL. */
    default Session begin() {
        return begin(0, 0);
    }

    /** A server-side transaction session; calls are pinned to one coordinator. */
    interface Session {

        byte[] txnId();

        long startTs();

        /**
         * Snapshot read that also records the read set.
         *
         * @param rangeEnd empty = point read; otherwise {@code [key, rangeEnd)}
         * @param limit    0 = unlimited (still bounded by the server's range limit)
         */
        ReadResult read(byte[] key, byte[] rangeEnd, long limit, boolean keysOnly);

        /** Exact-key snapshot read. */
        default ReadResult read(byte[] key) {
            return read(key, new byte[0], 0, false);
        }

        /**
         * Validate the read set and commit the write set.
         *
         * @param requestId idempotency key; a retry with the same id within the
         *                  session TTL replays the first {@code commitTs}
         * @throws CoordException on any failure other than an OCC conflict
         */
        CommitResult commit(List<Mutation> mutations, byte[] requestId);

        /** Commit without an idempotency key. */
        default CommitResult commit(List<Mutation> mutations) {
            return commit(mutations, new byte[0]);
        }

        /**
         * Idempotent abort. An unknown/expired session succeeds: the caller's
         * intent ("this transaction must not commit") already holds.
         */
        void abort();
    }
}
