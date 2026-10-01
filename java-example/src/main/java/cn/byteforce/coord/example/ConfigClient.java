package cn.byteforce.coord.example;

import java.util.Optional;

/**
 * 配置中心客户端 — Java 应用通过 Agent 读写 Coord 配置。
 *
 * 基于 Coord KV 存储：
 * - 配置 key 约定: /_config/{app-name}/{profile}/{key}
 * - 支持 Watch 驱动的配置热更新
 *
 * 用法:
 * <pre>{@code
 *   CoordClient client = CoordClient.connectToLocalAgent();
 *   ConfigClient config = new ConfigClient(client, "order-service", "prod");
 *
 *   String dbUrl = config.get("db.url").orElse("jdbc:mysql://localhost:3306/default");
 * }</pre>
 */
public class ConfigClient {

    private static final String CONFIG_PREFIX = "/_config/";

    private final CoordClient client;
    private final String appName;
    private final String profile;

    public ConfigClient(CoordClient client, String appName, String profile) {
        this.client = client;
        this.appName = appName;
        this.profile = profile;
    }

    /**
     * 获取配置值。
     */
    public Optional<String> get(String key) {
        String fullKey = configKey(key);
        String value = client.get(fullKey);
        return Optional.ofNullable(value);
    }

    /**
     * 获取配置值，不存在时返回默认值。
     */
    public String getOrDefault(String key, String defaultValue) {
        return get(key).orElse(defaultValue);
    }

    /**
     * 设置配置值。
     */
    public void set(String key, String value) {
        client.put(configKey(key), value);
    }

    /**
     * 删除配置。
     */
    public void delete(String key) {
        client.delete(configKey(key));
    }

    /**
     * 获取应用的所有配置（前缀扫描）。
     */
    public java.util.Map<String, String> getAll() {
        String prefix = CONFIG_PREFIX + appName + "/" + profile + "/";
        java.util.Map<String, String> result = new java.util.LinkedHashMap<>();
        for (var kv : client.scan(prefix)) {
            String shortKey = kv.getKey().toStringUtf8().substring(prefix.length());
            result.put(shortKey, kv.getValue().toStringUtf8());
        }
        return result;
    }

    private String configKey(String key) {
        return CONFIG_PREFIX + appName + "/" + profile + "/" + key;
    }
}
