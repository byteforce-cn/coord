package cn.byteforce.coord.sdk.lock;

import cn.byteforce.coord.sdk.CoordException;

/**
 * Distributed lock API.
 * <p>
 * Provides acquire, release, renew, and query operations for distributed mutex locks.
 * Locks are backed by Coord's Lease + Txn primitives.
 *
 * <pre>{@code
 * try (CoordClient client = CoordClient.create(config)) {
 *     LockClient lock = client.lock();
 *     LockInfo info = lock.acquire("my-lock", "worker-1", 30);
 *     if (info.isAcquired()) {
 *         try {
 *             // critical section
 *         } finally {
 *             lock.release("my-lock", "worker-1", info.getLeaseId());
 *         }
 *     }
 * }
 * }</pre>
 */
public interface LockClient {

    /**
     * Acquire a distributed lock (non-blocking).
     *
     * @param name      unique lock name / resource identifier
     * @param holderId  identifier of the lock requester
     * @param ttlSeconds lock TTL in seconds
     * @return lock acquisition result
     * @throws CoordException on communication failure
     */
    LockInfo acquire(String name, String holderId, long ttlSeconds);

    /**
     * Release a previously acquired lock.
     *
     * @param name     lock name
     * @param holderId holder identifier
     * @param leaseId  lease ID returned by acquire
     * @return true if the lock was released
     * @throws CoordException on communication failure
     */
    boolean release(String name, String holderId, long leaseId);

    /**
     * Renew (extend) a lock's TTL.
     *
     * @param name     lock name
     * @param holderId holder identifier
     * @param leaseId  lease ID returned by acquire
     * @return true if the lease was renewed
     * @throws CoordException on communication failure
     */
    boolean renew(String name, String holderId, long leaseId);

    /**
     * Query the current state of a lock.
     *
     * @param name lock name
     * @return lock info, or {@link LockInfo#NOT_FOUND} if the lock does not exist
     * @throws CoordException on communication failure
     */
    LockInfo getLockInfo(String name);
}
