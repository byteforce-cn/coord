package cn.byteforce.coord.sdk.tso.v2;

import cn.byteforce.coord.sdk.CoordException;

/**
 * Global timestamp oracle ({@code coord.tso.v2}).
 *
 * <p>Timestamps are the global {@code start_ts}/{@code commit_ts}/{@code read_ts}
 * of KV-T v2. A batch is global, unique and monotonic, but may contain holes and
 * must never be reused across independent write transactions. Batches are
 * process-local: do not persist and replay them after a restart.
 *
 * <p>The agent forwards these calls to the server's region-0 TSO leader.
 */
public interface TsoClient {

    /** A left-closed, right-open timestamp batch {@code [startTs, endTs)}. */
    record TimestampBatch(long startTs, long endTs) {}

    /**
     * Allocate one timestamp ({@code GetTs}).
     *
     * <p>Safe to reuse for multiple reads at the same snapshot; never reuse it
     * for two independent write transactions.
     */
    long getTimestamp();

    /**
     * Allocate {@code batchSize} timestamps (0 is treated as 1 by the server).
     * Server-side upper bounds apply; an over-limit batch is rejected, not
     * silently truncated.
     *
     * @throws CoordException on communication failure or an invalid batch size
     */
    TimestampBatch allocate(int batchSize);
}
