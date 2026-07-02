package cn.byteforce.coord.spring.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

/**
 * Coord Spring Boot 配置属性
 *
 * 映射 application.yml 中 coord.* 前缀的属性。
 */
@ConfigurationProperties(prefix = "coord")
public class CoordProperties {

    /** Agent gRPC 连接配置 */
    @NestedConfigurationProperty
    private final AgentProperties agent = new AgentProperties();

    /** 服务注册发现配置 */
    @NestedConfigurationProperty
    private final DiscoveryProperties discovery = new DiscoveryProperties();

    /** 配置中心配置 */
    @NestedConfigurationProperty
    private final ConfigProperties config = new ConfigProperties();

    /** 分布式锁配置 */
    @NestedConfigurationProperty
    private final LockProperties lock = new LockProperties();

    /** 分布式缓存配置 */
    @NestedConfigurationProperty
    private final CacheProperties cache = new CacheProperties();

    // ──── Agent 连接 ────

    public String getAgentHost() { return agent.host; }
    public void setAgentHost(String host) { this.agent.host = host; }
    public int getAgentPort() { return agent.port; }
    public void setAgentPort(int port) { this.agent.port = port; }
    public int getRequestTimeoutMs() { return agent.requestTimeoutMs; }
    public void setRequestTimeoutMs(int ms) { this.agent.requestTimeoutMs = ms; }
    public int getMaxRetries() { return agent.maxRetries; }
    public void setMaxRetries(int retries) { this.agent.maxRetries = retries; }

    // ──── 嵌套配置 ────

    public AgentProperties getAgent() { return agent; }
    public DiscoveryProperties getDiscovery() { return discovery; }
    public ConfigProperties getConfig() { return config; }
    public LockProperties getLock() { return lock; }
    public CacheProperties getCache() { return cache; }

    // ──── 内部类 ────

    public static class AgentProperties {
        private String host = "localhost";
        private int port = 19527;
        private int requestTimeoutMs = 5000;
        private int maxRetries = 3;

        public String getHost() { return host; }
        public void setHost(String host) { this.host = host; }
        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
        public int getRequestTimeoutMs() { return requestTimeoutMs; }
        public void setRequestTimeoutMs(int ms) { this.requestTimeoutMs = ms; }
        public int getMaxRetries() { return maxRetries; }
        public void setMaxRetries(int retries) { this.maxRetries = retries; }
    }

    public static class DiscoveryProperties {
        private boolean enabled = true;
        private int heartbeatIntervalMs = 10000;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public int getHeartbeatIntervalMs() { return heartbeatIntervalMs; }
        public void setHeartbeatIntervalMs(int ms) { this.heartbeatIntervalMs = ms; }
    }

    public static class ConfigProperties {
        private boolean enabled = true;
        private boolean watchEnabled = true;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public boolean getWatchEnabled() { return watchEnabled; }
        public void setWatchEnabled(boolean watchEnabled) { this.watchEnabled = watchEnabled; }
    }

    public static class LockProperties {
        private boolean enabled = false;
        private long defaultTimeoutMs = 30000;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public long getDefaultTimeoutMs() { return defaultTimeoutMs; }
        public void setDefaultTimeoutMs(long ms) { this.defaultTimeoutMs = ms; }
    }

    public static class CacheProperties {
        private boolean enabled = false;
        private String backend = "moka";
        private int maxEntries = 10000;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public String getBackend() { return backend; }
        public void setBackend(String backend) { this.backend = backend; }
        public int getMaxEntries() { return maxEntries; }
        public void setMaxEntries(int maxEntries) { this.maxEntries = maxEntries; }
    }
}
