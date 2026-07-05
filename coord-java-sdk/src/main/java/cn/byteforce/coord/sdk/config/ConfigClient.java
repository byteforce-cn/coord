package cn.byteforce.coord.sdk.config;

import java.util.Map;
import java.util.Optional;

/**
 * Public API for dynamic configuration access.
 */
public interface ConfigClient {
    Optional<String> getString(String key);
    Optional<Integer> getInt(String key);
    Optional<Long> getLong(String key);
    Optional<Boolean> getBoolean(String key);
    <T> Optional<T> getObject(String key, Class<T> type);
    Map<String, String> list(String prefix);
    ConfigWatchSubscription watch(String prefix, ConfigListener listener);
}
