package cn.byteforce.coord.example;

import coord.kv.Kv;

import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * 服务注册发现 — Java 微服务接入 Coord 的推荐模式。
 *
 * 使用 Coord 的 KV + Lease 实现服务注册发现：
 * 1. 服务启动时向 /_registry/services/{name}/instances/{id} 注册自身
 * 2. 绑定 Lease 实现自动注销（心跳断开 → Lease 过期 → key 自动删除）
 * 3. 通过前缀扫描发现依赖服务的所有实例
 *
 * 与架构文档 一致。
 *
 * 用法:
 * <pre>{@code
 *   CoordClient client = CoordClient.connectToLocalAgent();
 *   ServiceRegistry registry = new ServiceRegistry(client);
 *
 *   // 注册自身
 *   registry.register("order-service", "node1",
 *       ServiceInfo.of("10.0.1.10", 8080));
 *
 *   // 发现依赖
 *   List<ServiceInfo> instances = registry.discover("payment-service");
 * }</pre>
 */
public class ServiceRegistry {

    private static final Logger log = Logger.getLogger(ServiceRegistry.class.getName());

    private static final String REGISTRY_PREFIX = "/_registry/services/";

    /**
     * 注册 TTL（秒）。心跳间隔取 TTL/3（见 {@link CoordClient#DEFAULT_KEEPALIVE_INTERVAL_SECS}）。
     */
    private static final long REGISTRY_TTL_SECS = 30;

    private final CoordClient client;
    private volatile long leaseId;
    private volatile CoordClient.KeepAliveHandle keepAliveHandle;

    public ServiceRegistry(CoordClient client) {
        this.client = client;
    }

    /**
     * 注册服务实例并维持租约。
     *
     * <p><b>第四轮 §3.14.6 修复</b>：此前这里每 10s 重复 `put` 同一个 key，注释
     * 自称"续约方式之一"——**这是错的**：重复 put 只会覆盖值并刷新 revision，
     * **不会延长 lease TTL**（服务端只把 key 绑定到租约；延长 TTL 只有
     * `LeaseKeepAlive` 会做）。于是实例在 30s TTL 到期后被自动删除，而
     * `CoordClient.keepAlive(...)` 存在却从未被调用。
     *
     * <p>现在：一次 `put`（绑定租约）+ 一条持续心跳流，租约不再过期。
     *
     * @param serviceName 服务名（如 "order-service"）
     * @param instanceId  实例标识（如 "node1" 或 UUID）
     * @param info        服务实例信息（host, port, metadata）
     */
    public void register(String serviceName, String instanceId, ServiceInfo info) {
        // 1. Grant Lease
        long granted = client.grantLease(REGISTRY_TTL_SECS);

        // 2. 写入注册信息（绑定 Lease）
        String key = instanceKey(serviceName, instanceId);
        client.put(key, info.toJson(), granted);

        // 3. 真正续约：心跳间隔 = TTL/3
        CoordClient.KeepAliveHandle handle = client.keepAlive(
                granted,
                Math.max(1, REGISTRY_TTL_SECS / 3),
                () -> log.warning(() -> "registry lease for " + serviceName + "/" + instanceId
                        + " was lost (keep-alive stream closed)"));
        this.leaseId = granted;
        this.keepAliveHandle = handle;
    }

    /**
     * 发现指定服务的所有实例。
     *
     * @param serviceName 服务名
     * @return 在线实例列表
     */
    public List<ServiceInfo> discover(String serviceName) {
        String prefix = REGISTRY_PREFIX + serviceName + "/instances/";
        List<Kv.KeyValue> kvs = client.scan(prefix);
        return kvs.stream()
                .map(kv -> ServiceInfo.fromJson(kv.getValue().toStringUtf8()))
                .collect(Collectors.toList());
    }

    /**
     * 注销自身（停心跳 → Revoke Lease → 所有绑定 key 自动删除）。
     */
    public void deregister() {
        CoordClient.KeepAliveHandle handle = this.keepAliveHandle;
        if (handle != null) {
            handle.cancel();
            this.keepAliveHandle = null;
        }
        long id = this.leaseId;
        if (id > 0) {
            client.revokeLease(id);
            this.leaseId = 0;
        }
    }

    static String instanceKey(String serviceName, String instanceId) {
        return REGISTRY_PREFIX + serviceName + "/instances/" + instanceId;
    }

    /**
     * 服务实例信息 — JSON 序列化格式。
     */
    public record ServiceInfo(String service, String instance, String host, int port, String status) {

        public static ServiceInfo of(String host, int port) {
            return new ServiceInfo("", "", host, port, "UP");
        }

        public ServiceInfo withService(String service, String instance) {
            return new ServiceInfo(service, instance, this.host, this.port, this.status);
        }

        public String toJson() {
            return String.format("""
                    {"service":"%s","instance":"%s","host":"%s","port":%d,"status":"%s"}""",
                    service, instance, host, port, status);
        }

        public static ServiceInfo fromJson(String json) {
            // 简化 JSON 解析（生产环境建议用 Jackson/Gson）
            String service = extractJsonField(json, "service");
            String instance = extractJsonField(json, "instance");
            String host = extractJsonField(json, "host");
            int port = Integer.parseInt(extractJsonField(json, "port"));
            String status = extractJsonField(json, "status");
            return new ServiceInfo(service, instance, host, port, status);
        }

        private static String extractJsonField(String json, String field) {
            String pattern = "\"" + field + "\":\"";
            int start = json.indexOf(pattern);
            if (start < 0) {
                // Try integer field
                pattern = "\"" + field + "\":";
                start = json.indexOf(pattern);
                if (start < 0) return "";
                start += pattern.length();
                int end = json.indexOf(",", start);
                if (end < 0) end = json.indexOf("}", start);
                return json.substring(start, end).trim();
            }
            start += pattern.length();
            int end = json.indexOf("\"", start);
            return json.substring(start, end);
        }
    }
}
