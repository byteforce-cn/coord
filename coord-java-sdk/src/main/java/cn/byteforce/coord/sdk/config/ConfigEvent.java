package cn.byteforce.coord.sdk.config;

import java.util.Optional;

/** An event describing a configuration change. */
public final class ConfigEvent {
    private final String key;
    private final Optional<String> newValue;
    private final long revision;

    public ConfigEvent(String key, Optional<String> newValue, long revision) {
        this.key = key;
        this.newValue = newValue;
        this.revision = revision;
    }

    public String getKey() { return key; }

    /**
     * The new value as presence semantics (per {@code config.proto}): an
     * <b>empty Optional means the key was deleted</b>; a stored empty value arrives
     * as {@code Optional.of("")}.
     */
    public Optional<String> getNewValue() { return newValue; }

    /**
     * Global position (server-wide KV revision) of this event; monotonically
     * increasing within a stream. Persist it — reconnect resumes from
     * {@code lastRevision + 1}, and it seeds a fresh watch after a history-loss
     * termination.
     */
    public long getRevision() { return revision; }

    @Override
    public String toString() {
        return "ConfigEvent{key='" + key + "', revision=" + revision + "}";
    }
}
