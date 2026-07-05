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
    public Optional<String> getNewValue() { return newValue; }
    public long getRevision() { return revision; }

    @Override
    public String toString() {
        return "ConfigEvent{key='" + key + "', revision=" + revision + "}";
    }
}
