package cn.byteforce.coord.sdk.cache;

/** Outcome of {@link CacheClient#clear(String)}. */
public final class CacheClearResult {
    private final long removedEntries;
    private final long bytesReclaimed;

    public CacheClearResult(long removedEntries, long bytesReclaimed) {
        this.removedEntries = removedEntries;
        this.bytesReclaimed = bytesReclaimed;
    }

    /** Physical rows removed (hash fields / list elements / set members count individually). */
    public long getRemovedEntries() {
        return removedEntries;
    }

    /** Active bytes reclaimed (capacity-accounting basis, B-PL-3). */
    public long getBytesReclaimed() {
        return bytesReclaimed;
    }

    @Override
    public String toString() {
        return "CacheClearResult{removedEntries=" + removedEntries
                + ", bytesReclaimed=" + bytesReclaimed + "}";
    }
}
