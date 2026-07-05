package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.CoordException;
import cn.byteforce.coord.sdk.ErrorCode;
import cn.byteforce.coord.sdk.config.*;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.sdk.internal.proto.ConfigGetRequest;
import cn.byteforce.coord.sdk.internal.proto.ConfigGetResponse;
import cn.byteforce.coord.sdk.internal.proto.ConfigGrpc;
import cn.byteforce.coord.sdk.internal.proto.ConfigListRequest;
import cn.byteforce.coord.sdk.internal.proto.ConfigListResponse;
import cn.byteforce.coord.sdk.internal.proto.ConfigWatchEvent;
import cn.byteforce.coord.sdk.internal.proto.ConfigWatchRequest;
import cn.byteforce.coord.sdk.internal.watch.WatchManager;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of {@link ConfigClient} backed by gRPC calls to the Coord Agent.
 */
public final class ConfigClientImpl extends AgentRpcClient implements ConfigClient {

    private static final Logger log = LoggerFactory.getLogger(ConfigClientImpl.class);

    private final CoordConfig config;
    private final WatchManager watchManager;

    public ConfigClientImpl(AgentChannelManager channelManager, ErrorMapper errorMapper,
                     RetryTemplate retryTemplate, ObservabilityProvider observability,
                     CoordConfig config, WatchManager watchManager) {
        super(channelManager, errorMapper, retryTemplate, observability);
        this.config = config;
        this.watchManager = watchManager;
    }

    @Override
    public Optional<String> getString(String key) {
        ConfigGetRequest request = ConfigGetRequest.newBuilder().setKey(key).build();
        try {
            ConfigGetResponse response = callWithRetry(
                    (ch, req) -> ConfigGrpc.newBlockingStub(ch)
                            .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                            .get((ConfigGetRequest) req),
                    request, "config.get");
            if (response.getFound()) {
                return Optional.of(response.getValue());
            }
            return Optional.empty();
        } catch (CoordException e) {
            if (e.getErrorCode() == ErrorCode.CONFIG_KEY_NOT_FOUND
                    || e.getErrorCode() == ErrorCode.REGISTRY_SERVICE_NOT_FOUND) {
                return Optional.empty();
            }
            throw e; // Only known "not found" codes produce empty; others throw
        }
    }

    @Override
    public void put(String key, String value) {
        cn.byteforce.coord.sdk.internal.proto.ConfigPutRequest request =
                cn.byteforce.coord.sdk.internal.proto.ConfigPutRequest.newBuilder()
                        .setKey(key)
                        .setValue(value)
                        .build();
        callWithRetry(
                (ch, req) -> cn.byteforce.coord.sdk.internal.proto.ConfigGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .put((cn.byteforce.coord.sdk.internal.proto.ConfigPutRequest) req),
                request, "config.put");
        log.debug("Config put: key={}", key);
    }

    @Override
    public Optional<Integer> getInt(String key) {
        return getString(key).flatMap(v -> {
            try {
                return Optional.of(Integer.parseInt(v));
            } catch (NumberFormatException e) {
                log.warn("Config key '{}' value '{}' is not a valid integer", key, v);
                return Optional.empty();
            }
        });
    }

    @Override
    public Optional<Long> getLong(String key) {
        return getString(key).flatMap(v -> {
            try {
                return Optional.of(Long.parseLong(v));
            } catch (NumberFormatException e) {
                log.warn("Config key '{}' value '{}' is not a valid long", key, v);
                return Optional.empty();
            }
        });
    }

    @Override
    public Optional<Boolean> getBoolean(String key) {
        return getString(key).flatMap(v -> {
            if ("true".equalsIgnoreCase(v) || "1".equals(v)) {
                return Optional.of(true);
            }
            if ("false".equalsIgnoreCase(v) || "0".equals(v)) {
                return Optional.of(false);
            }
            log.warn("Config key '{}' value '{}' is not a valid boolean", key, v);
            return Optional.empty();
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Optional<T> getObject(String key, Class<T> type) {
        // Simple JSON deserialization — uses basic string parsing
        // Full JSON support would require jackson/gson as optional dependency
        return getString(key).flatMap(v -> {
            try {
                if (type == String.class) return Optional.of((T) v);
                if (type == Integer.class) return (Optional<T>) getInt(key);
                if (type == Long.class) return (Optional<T>) getLong(key);
                if (type == Boolean.class) return (Optional<T>) getBoolean(key);
                log.warn("getObject for type {} not supported without JSON library", type.getName());
                return Optional.empty();
            } catch (Exception e) {
                log.warn("Failed to deserialize config key '{}' to type {}", key, type.getName(), e);
                return Optional.empty();
            }
        });
    }

    @Override
    public Map<String, String> list(String prefix) {
        ConfigListRequest request = ConfigListRequest.newBuilder().setPrefix(prefix).build();
        try {
            ConfigListResponse response = callWithRetry(
                    (ch, req) -> ConfigGrpc.newBlockingStub(ch)
                            .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                            .list((ConfigListRequest) req),
                    request, "config.list");
            return new HashMap<>(response.getEntriesMap());
        } catch (CoordException e) {
            log.warn("Config list failed for prefix '{}': {}", prefix, e.getMessage());
            return Map.of();
        }
    }

    @Override
    public ConfigWatchSubscription watch(String prefix, ConfigListener listener) {
        String watchId = "cfg-" + prefix + "-" + UUID.randomUUID().toString().substring(0, 8);

        WatchManager.ActiveWatch[] holder = new WatchManager.ActiveWatch[1];

        holder[0] = new WatchManager.ActiveWatch(
                watchId,
                () -> {
                    ConfigWatchRequest request = ConfigWatchRequest.newBuilder()
                            .setPrefix(prefix)
                            .setStartRevision(0)
                            .build();
                    return ConfigGrpc.newBlockingStub(channelManager.getChannel())
                            .watch(request);
                },
                (ConfigWatchEvent protoEvent) -> {
                    Optional<String> newValue = protoEvent.hasNewValue()
                            ? Optional.of(protoEvent.getNewValue())
                            : Optional.empty();
                    holder[0].setLastRevision(protoEvent.getRevision());
                    listener.onEvent(new ConfigEvent(protoEvent.getKey(), newValue, protoEvent.getRevision()));
                },
                0
        );

        watchManager.startWatch(holder[0]);
        return () -> watchManager.cancelWatch(watchId);
    }
}
