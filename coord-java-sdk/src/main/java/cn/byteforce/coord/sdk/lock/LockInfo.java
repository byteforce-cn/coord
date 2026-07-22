package cn.byteforce.coord.sdk.lock;

/**
 * Result of a distributed lock acquire or query operation.
 */
public final class LockInfo {

    /** Pre-built instance representing a non-existent lock. */
    public static final LockInfo NOT_FOUND = new LockInfo("", "", 0, 0, 0, false);

    private final String name;
    private final String holderId;
    private final long leaseId;
    private final long acquiredAt;
    private final long ttlSeconds;
    private final boolean exists;

    public LockInfo(String name, String holderId, long leaseId,
                    long acquiredAt, long ttlSeconds, boolean exists) {
        this.name = name;
        this.holderId = holderId;
        this.leaseId = leaseId;
        this.acquiredAt = acquiredAt;
        this.ttlSeconds = ttlSeconds;
        this.exists = exists;
    }

    /** Lock name / resource identifier. */
    public String getName() { return name; }

    /** Current holder identifier. */
    public String getHolderId() { return holderId; }

    /** Bound lease ID. */
    public long getLeaseId() { return leaseId; }

    /** Unix timestamp (seconds) when the lock was acquired. */
    public long getAcquiredAt() { return acquiredAt; }

    /** Lock TTL in seconds. */
    public long getTtlSeconds() { return ttlSeconds; }

    /** Whether the lock was successfully acquired or exists. */
    public boolean isAcquired() { return exists && !holderId.isEmpty(); }

    /** Whether the lock exists (may be free). */
    public boolean exists() { return exists; }

    @Override
    public String toString() {
        return "LockInfo{name='" + name + "', holderId='" + holderId
                + "', leaseId=" + leaseId + ", ttlSeconds=" + ttlSeconds + ", exists=" + exists + '}';
    }
}
