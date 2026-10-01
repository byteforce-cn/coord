package cn.byteforce.coord.sdk.objectstore;

import cn.byteforce.coord.sdk.CoordException;

import java.util.Optional;

/**
 * Object storage client ({@code coord.storage}) — EXPERIMENTAL data plane.
 * <p>
 * Talks to the Coord Agent, which proxies {@code coord.storage.Storage} to the
 * server cluster. Object = (bucket, objectId): bucket is non-empty UTF-8 (≤255
 * bytes, no '/'), objectId is arbitrary bytes (≤1024).
 * <p>
 * v1 semantics (mirrored through the proxy):
 * <ul>
 *   <li>{@link #put} to an existing object → {@code CoordException}
 *       (server returns ALREADY_EXISTS; no overwrite);</li>
 *   <li>{@link #get}/{@link #stat}/{@link #delete} of a missing/deleted object:
 *       {@link #stat} yields {@link Optional#empty()}, {@link #delete} returns
 *       {@code false}, {@link #get} throws {@link CoordException} (NOT_FOUND);</li>
 *   <li>interrupted uploads are reclaimed server-side by GC after the
 *       configured {@code upload_timeout}; retry after {@link #delete}.</li>
 * </ul>
 */
public interface ObjectStoreClient {

    /** Result of a successful object upload. */
    record PutResult(long revision, long size, long chunks) {}

    /** Result of a delete: whether an object was actually removed + tombstone revision. */
    record DeleteResult(boolean deleted, long revision) {}

    /** Object metadata as reported by the server (Stat / Get first message). */
    record ObjectInfo(String bucket, byte[] objectId, long size, long chunks,
                      long revision, boolean exists, boolean committed) {}

    /** Download result: object metadata + full content bytes. */
    record GetResult(ObjectInfo info, byte[] data) {}

    /**
     * Upload {@code data} as a new object (default 4MiB chunking, matching the
     * server default {@code chunk_size_bytes}).
     *
     * @throws CoordException if the object already exists, arguments are invalid,
     *                        the agent/server is unreachable, or upload fails
     */
    PutResult put(String bucket, byte[] objectId, byte[] data);

    /**
     * Upload {@code data} with an explicit chunk size (≤ 4MiB and ≤ the server's
     * configured {@code chunk_size_bytes}).
     */
    PutResult put(String bucket, byte[] objectId, byte[] data, int chunkSizeBytes);

    /**
     * Download an object (server streams: stat first, then chunks).
     *
     * @throws CoordException if the object does not exist or the download fails
     */
    GetResult get(String bucket, byte[] objectId);

    /**
     * Stat an object.
     *
     * @return {@link Optional#empty()} when the object does not exist / was deleted
     */
    Optional<ObjectInfo> stat(String bucket, byte[] objectId);

    /**
     * Delete an object (manifest tombstone + chunk files removed server-side).
     *
     * @return {@code false} when the object did not exist (NOT_FOUND)
     */
    DeleteResult delete(String bucket, byte[] objectId);
}
