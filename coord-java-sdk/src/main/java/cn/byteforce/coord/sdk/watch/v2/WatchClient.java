package cn.byteforce.coord.sdk.watch.v2;

import cn.byteforce.coord.sdk.CoordException;
import cn.byteforce.coord.sdk.kv.v2.KvClient;

import java.util.List;

/**
 * KV-T v2 change subscription ({@code coord.watch.v2}).
 *
 * <p>Semantics (ADR-0015 D5, {@code coord/watch/v2/watch.proto}):
 * <ul>
 *   <li>one transaction = one atomic {@link Event}; a batch carries the
 *       {@code commit_ts} order;</li>
 *   <li>delivery is <b>at-least-once</b>: on a transport error the client
 *       reopens the stream from the last consumed {@link Cursor}. Apply events
 *       idempotently by {@code commitTs} if exactly-once matters;</li>
 *   <li>{@code HISTORY_UNAVAILABLE} is terminal — the retention window has
 *       reclaimed the position, so reconnecting would fail forever. Resync the
 *       whole key space instead;</li>
 *   <li>frames without events only advance the cursor/watermark and are not
 *       delivered;</li>
 *   <li>{@link Subscription#close()} cancels the RPC; no callback runs after it
 *       returns.</li>
 * </ul>
 */
public interface WatchClient {

    /** Event type filter (server-side). */
    enum Filter { ALL, PUTS_ONLY, DELETES_ONLY }

    /** Server cursor {@code (shardId, commitTs)}: opaque, replay it verbatim. */
    record Cursor(long shardId, long commitTs) {}

    /**
     * Subscription start position: either a revision or an exact cursor
     * (mutually exclusive; the later builder call wins).
     */
    record Options(byte[] key, byte[] rangeEnd, long startRevision, Cursor startCursor,
                   boolean prevKv, Filter filter) {

        /** Subscribe to one key from the current watermark. */
        public static Options key(byte[] key) {
            return new Options(key, new byte[0], 0, null, false, Filter.ALL);
        }

        /** Subscribe to {@code [key, rangeEnd)}. */
        public Options range(byte[] rangeEnd) {
            return new Options(key, rangeEnd, startRevision, startCursor, prevKv, filter);
        }

        /** Start at {@code revision} (inclusive); clears an explicit cursor. */
        public Options fromRevision(long revision) {
            return new Options(key, rangeEnd, revision, null, prevKv, filter);
        }

        /** Resume from an exact cursor; clears {@code startRevision}. */
        public Options fromCursor(Cursor cursor) {
            return new Options(key, rangeEnd, 0, cursor, prevKv, filter);
        }

        public Options prevKv(boolean enabled) {
            return new Options(key, rangeEnd, startRevision, startCursor, enabled, filter);
        }

        public Options filter(Filter value) {
            return new Options(key, rangeEnd, startRevision, startCursor, prevKv, value);
        }
    }

    /**
     * One change. {@code value} is {@code null} for a delete; {@code prevKv} is
     * non-null only when requested and the key existed.
     */
    record Change(byte[] key, byte[] value, KvClient.KeyValue prevKv) {

        public boolean deleted() {
            return value == null;
        }
    }

    /** One transaction: all changes share {@code commitTs} and apply atomically. */
    record Event(long commitTs, List<Change> changes) {}

    /** A delivered batch; ack-only frames never reach the listener. */
    record Batch(List<Event> events, Cursor cursor, long watermark, boolean sealed) {}

    /** Callbacks run on the subscription's own worker; do not block it for long. */
    interface Listener {

        void onBatch(Batch batch);

        /** Terminal failure (e.g. {@code HISTORY_UNAVAILABLE}); the subscription ended. */
        void onError(CoordException error);
    }

    /** Subscription handle: the last consumed position and cancellation. */
    interface Subscription extends AutoCloseable {

        /** Latest position (advanced by ack-only frames too); {@code null} before the first frame. */
        Cursor cursor();

        /** Cancel and wait (bounded) for the worker to exit; idempotent. */
        @Override
        void close();
    }

    /**
     * Open a subscription. Connection failures surface through
     * {@link Listener#onError} — they are not thrown here.
     */
    Subscription watch(Options options, Listener listener);
}
