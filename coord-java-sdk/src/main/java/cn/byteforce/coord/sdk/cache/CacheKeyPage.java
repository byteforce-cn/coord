package cn.byteforce.coord.sdk.cache;

import java.util.Collections;
import java.util.List;

/**
 * One page of {@link CacheClient#keys(String, int, String)}.
 *
 * <p>Pagination contract: pass {@link #getNextCursor()} to the next call;
 * an <b>empty</b> {@code nextCursor} means the end has been reached. A page whose
 * {@code nextCursor} is non-empty may still be the last one — the following call
 * then returns an empty page (with an empty cursor).
 */
public final class CacheKeyPage {
    private final List<String> keys;
    private final String nextCursor;

    public CacheKeyPage(List<String> keys, String nextCursor) {
        this.keys = Collections.unmodifiableList(List.copyOf(keys));
        this.nextCursor = nextCursor == null ? "" : nextCursor;
    }

    /** Keys in this page (ascending order, deduplicated across data types). */
    public List<String> getKeys() {
        return keys;
    }

    /** Continuation cursor; empty = end of iteration. */
    public String getNextCursor() {
        return nextCursor;
    }

    /** Whether more pages may follow. */
    public boolean hasMore() {
        return !nextCursor.isEmpty();
    }

    @Override
    public String toString() {
        return "CacheKeyPage{keys=" + keys.size() + ", nextCursor='" + nextCursor + "'}";
    }
}
