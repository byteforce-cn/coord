package cn.byteforce.coord.sdk.config;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Configuration snapshot: {@code entries} plus the snapshot position {@code revision}.
 *
 * <p><b>Resync discipline</b> (same as {@code config.proto}): take this snapshot first,
 * then subscribe with {@link ConfigClient#watchFrom(String, long, ConfigListener)} using
 * {@code revision} as the start position — changes (including <b>deletions</b>) made
 * after the snapshot are reliably replayed from the position. Delivery may overlap
 * with the snapshot (duplicates), so event application must be idempotent.
 */
public final class ConfigSnapshot {
    private final Map<String, String> entries;
    private final long revision;

    public ConfigSnapshot(Map<String, String> entries, long revision) {
        this.entries = Collections.unmodifiableMap(new LinkedHashMap<>(entries));
        this.revision = revision;
    }

    /** Entries under the requested prefix (immutable). */
    public Map<String, String> getEntries() {
        return entries;
    }

    /**
     * Snapshot position (server-wide KV revision coordinates).
     * {@code 0} means "no position established yet" — treat as unknown and
     * re-snapshot before relying on position-based resumption.
     */
    public long getRevision() {
        return revision;
    }
}
