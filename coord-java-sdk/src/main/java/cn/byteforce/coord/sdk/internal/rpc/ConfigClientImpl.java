package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.CoordException;
import cn.byteforce.coord.sdk.ErrorCode;
import cn.byteforce.coord.sdk.config.*;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.contracts.config.v1.ConfigGetRequest;
import cn.byteforce.coord.contracts.config.v1.ConfigGetResponse;
import cn.byteforce.coord.contracts.config.v1.ConfigGrpc;
import cn.byteforce.coord.contracts.config.v1.ConfigListRequest;
import cn.byteforce.coord.contracts.config.v1.ConfigListResponse;
import cn.byteforce.coord.contracts.config.v1.ConfigWatchEvent;
import cn.byteforce.coord.contracts.config.v1.ConfigWatchRequest;
import cn.byteforce.coord.sdk.internal.watch.GrpcWatchStream;
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
            // NOT_FOUND 是服务端现在发出的通用"资源不存在"码（第四轮 §3.14.2）；
            // 两个 SDK 本地码保留以兼容既有调用路径。
            if (e.getErrorCode() == ErrorCode.CONFIG_KEY_NOT_FOUND
                    || e.getErrorCode() == ErrorCode.NOT_FOUND
                    || e.getErrorCode() == ErrorCode.REGISTRY_SERVICE_NOT_FOUND) {
                return Optional.empty();
            }
            throw e; // Only known "not found" codes produce empty; others throw
        }
    }

    @Override
    public void put(String key, String value) {
        cn.byteforce.coord.contracts.config.v1.ConfigPutRequest request =
                cn.byteforce.coord.contracts.config.v1.ConfigPutRequest.newBuilder()
                        .setKey(key)
                        .setValue(value)
                        .build();
        callWithRetry(
                (ch, req) -> cn.byteforce.coord.contracts.config.v1.ConfigGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .put((cn.byteforce.coord.contracts.config.v1.ConfigPutRequest) req),
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
        // 第四轮 §3.14.5：不支持的**类型**必须报错，而不是返回 Optional.empty()。
        // 修复前：对 String/Integer/Long/Boolean 以外的类型只打一条 warn 然后返回空
        // ——调用方无法区分"键不存在"与"这个类型我们不支持"，是**静默数据丢失**。
        if (type != String.class && type != Integer.class && type != Long.class
                && type != Boolean.class) {
            throw new CoordException(ErrorCode.INVALID_ARGUMENT,
                    "getObject does not support type " + type.getName()
                            + " (supported: String, Integer, Long, Boolean). "
                            + "Add a JSON library and deserialize getString(key) yourself.");
        }
        return getString(key).flatMap(v -> {
            try {
                if (type == String.class) return Optional.of((T) v);
                if (type == Integer.class) return (Optional<T>) getInt(key);
                if (type == Long.class) return (Optional<T>) getLong(key);
                if (type == Boolean.class) return (Optional<T>) getBoolean(key);
                // 不可达（上面已穷举）；保留以满足编译器的"可能无返回值"分析
                return Optional.empty();
            } catch (Exception e) {
                // 值存在但解析失败：这是**数据问题**，不得当成"不存在"。
                throw new CoordException(ErrorCode.CONFIG_INVALID,
                        "config key '" + key + "' exists but cannot be parsed as "
                                + type.getName() + ": " + e.getMessage(), e);
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
            // 第四轮 §3.14.5：不能把"查询失败"变成"没有配置"——调用方会据此认为
            // 前缀下确实为空，从而做出错误的接管/清空决策。除了明确的 NOT_FOUND，
            // 其余错误一律向上抛。
            if (e.getErrorCode() == ErrorCode.NOT_FOUND
                    || e.getErrorCode() == ErrorCode.CONFIG_KEY_NOT_FOUND) {
                return Map.of();
            }
            throw e;
        }
    }

    @Override
    public ConfigWatchSubscription watch(String prefix, ConfigListener listener) {
        String watchId = "cfg-" + prefix + "-" + UUID.randomUUID().toString().substring(0, 8);

        // 第四轮 §3.14.3：流改为**可取消**的 GrpcWatchStream（原先用阻塞式 stub iterator，
        // 取消要等到下一条事件才生效），并支持断线重连 + 从 lastRevision+1 续订。
        // 重连所需的 start revision 由 WatchManager 传入（首次为 0 = 从最新开始）。
        WatchManager.ActiveWatch watch = new WatchManager.ActiveWatch(
                watchId,
                (startRevision) -> new GrpcWatchStream(
                        channelManager.getChannel(),
                        ConfigGrpc.getWatchMethod(),
                        ConfigWatchRequest.newBuilder()
                                .setPrefix(prefix)
                                .setStartRevision(startRevision)
                                .build()),
                (ConfigWatchEvent protoEvent) -> {
                    Optional<String> newValue = protoEvent.hasNewValue()
                            ? Optional.of(protoEvent.getNewValue())
                            : Optional.empty();
                    listener.onEvent(new ConfigEvent(protoEvent.getKey(), newValue, protoEvent.getRevision()));
                },
                // revision 水位在消费线程上同步推进（见 WatchManager），不再由异步回调维护
                (Object e) -> ((ConfigWatchEvent) e).getRevision(),
                0,
                listener::onTerminated
        );

        watchManager.startWatch(watch);
        return () -> watchManager.cancelWatch(watchId);
    }
}
