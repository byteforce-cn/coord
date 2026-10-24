package cn.byteforce.coord.sdk.storage.v2;

import cn.byteforce.coord.sdk.CoordException;

/**
 * KV-T v2 object storage ({@code coord.storage.v2}).
 *
 * <p>Differs from the v1 object store only in the revision contract: the
 * manifest {@code commitTs} is allocated by the server at commit time and
 * returned; {@code readTs = 0} means the server-local {@code safe_ts}, while an
 * explicit {@code readTs} is a snapshot read (an already-compacted revision
 * fails with {@code OUT_OF_RANGE}).
 *
 * <p>Objects live under {@code /obj/} and are uploaded whole; {@code put} is
 * idempotent per {@code (bucket, objectId)} — an existing committed object is
 * rejected with {@code ALREADY_EXISTS} (v2 does not support overwrite).
 */
public interface StorageClient {

    /** Upload result: {@code commitTs} is the manifest's global timestamp. */
    record PutResult(long commitTs, long size, long chunks) {}

    /** Manifest metadata. {@code exists=false} is a zero stat, not an error. */
    record ObjectStat(String bucket, byte[] objectId, long size, long chunks,
                      long commitTs, long createTs, boolean exists, boolean committed) {}

    /** Download result: the manifest stat plus the reassembled payload. */
    record ObjectData(ObjectStat stat, byte[] data) {}

    /** Delete result; {@code commitTs} is the tombstone's timestamp. */
    record DeleteResult(boolean deleted, long commitTs) {}

    /** Upload a whole object with the default 4 MiB chunk size. */
    default PutResult put(String bucket, byte[] objectId, byte[] data) {
        return put(bucket, objectId, data, 4 * 1024 * 1024);
    }

    /**
     * Upload a whole object.
     *
     * @param chunkSizeBytes stream chunk size, {@code (0, 4MiB]}
     * @throws CoordException {@code INVALID_ARGUMENT} for empty objects,
     *                        {@code ALREADY_EXISTS} for a committed object
     */
    PutResult put(String bucket, byte[] objectId, byte[] data, int chunkSizeBytes);

    /** Download at {@code readTs}; {@code 0} = server-local {@code safe_ts}. */
    ObjectData get(String bucket, byte[] objectId, long readTs);

    /** Download at the current {@code safe_ts}. */
    default ObjectData get(String bucket, byte[] objectId) {
        return get(bucket, objectId, 0);
    }

    /**
     * Query the manifest. A missing/deleted object returns a zero stat with
     * {@code exists=false} (matching the agent's v2 zero-stat contract); it is
     * not reported as {@code NOT_FOUND}.
     */
    ObjectStat stat(String bucket, byte[] objectId, long readTs);

    /** Query at the current {@code safe_ts}. */
    default ObjectStat stat(String bucket, byte[] objectId) {
        return stat(bucket, objectId, 0);
    }

    /**
     * Delete an object (tombstone plus chunk cleanup).
     *
     * @param requestId idempotency key; a retried delete replays the same result
     */
    DeleteResult delete(String bucket, byte[] objectId, byte[] requestId);
}
