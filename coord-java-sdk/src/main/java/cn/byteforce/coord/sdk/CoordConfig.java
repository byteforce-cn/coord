package cn.byteforce.coord.sdk;

import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import cn.byteforce.coord.sdk.internal.observability.NoopObservabilityProvider;

import java.time.Duration;
import java.util.Objects;

/**
 * Immutable configuration for {@link CoordClient}.
 * Must be constructed via {@link #builder()}. Once built, all fields are read-only.
 * Modifying configuration requires creating a new {@link CoordClient} instance.
 */
public final class CoordConfig {

    private final String agentHost;
    private final int agentPort;
    private final Duration requestTimeout;
    private final boolean autoRestoreWatches;
    private final int heartbeatThreads;
    private final boolean useTls;
    private final String tlsCaCertPath;
    private final String tlsClientCertPath;
    private final String tlsClientKeyPath;
    private final ObservabilityProvider observabilityProvider;

    private CoordConfig(Builder builder) {
        this.agentHost = builder.agentHost;
        this.agentPort = builder.agentPort;
        this.requestTimeout = builder.requestTimeout;
        this.autoRestoreWatches = builder.autoRestoreWatches;
        this.heartbeatThreads = builder.heartbeatThreads;
        this.useTls = builder.useTls;
        this.tlsCaCertPath = builder.tlsCaCertPath;
        this.tlsClientCertPath = builder.tlsClientCertPath;
        this.tlsClientKeyPath = builder.tlsClientKeyPath;
        this.observabilityProvider = builder.observabilityProvider;
    }

    public static Builder builder() {
        return new Builder();
    }

    // --- Getters ---

    public String getAgentHost() { return agentHost; }
    public int getAgentPort() { return agentPort; }
    public Duration getRequestTimeout() { return requestTimeout; }
    public boolean isAutoRestoreWatches() { return autoRestoreWatches; }
    public int getHeartbeatThreads() { return heartbeatThreads; }
    public boolean isUseTls() { return useTls; }
    public String getTlsCaCertPath() { return tlsCaCertPath; }
    public String getTlsClientCertPath() { return tlsClientCertPath; }
    public String getTlsClientKeyPath() { return tlsClientKeyPath; }
    public ObservabilityProvider getObservabilityProvider() { return observabilityProvider; }

    /**
     * Builder for {@link CoordConfig}.
     */
    public static final class Builder {
        private String agentHost;
        private int agentPort = 19527;
        private Duration requestTimeout = Duration.ofSeconds(5);
        private boolean autoRestoreWatches = true;
        private int heartbeatThreads = 4;
        private boolean useTls = false;
        private String tlsCaCertPath;
        private String tlsClientCertPath;
        private String tlsClientKeyPath;
        private ObservabilityProvider observabilityProvider = new NoopObservabilityProvider();

        private Builder() {}

        public Builder agentHost(String agentHost) {
            this.agentHost = agentHost;
            return this;
        }

        public Builder agentPort(int agentPort) {
            this.agentPort = agentPort;
            return this;
        }

        public Builder requestTimeout(Duration requestTimeout) {
            this.requestTimeout = requestTimeout;
            return this;
        }

        public Builder autoRestoreWatches(boolean autoRestoreWatches) {
            this.autoRestoreWatches = autoRestoreWatches;
            return this;
        }

        public Builder heartbeatThreads(int heartbeatThreads) {
            this.heartbeatThreads = heartbeatThreads;
            return this;
        }

        public Builder useTls(boolean useTls) {
            this.useTls = useTls;
            return this;
        }

        public Builder tlsCaCertPath(String tlsCaCertPath) {
            this.tlsCaCertPath = tlsCaCertPath;
            return this;
        }

        public Builder tlsClientCertPath(String tlsClientCertPath) {
            this.tlsClientCertPath = tlsClientCertPath;
            return this;
        }

        public Builder tlsClientKeyPath(String tlsClientKeyPath) {
            this.tlsClientKeyPath = tlsClientKeyPath;
            return this;
        }

        public Builder observabilityProvider(ObservabilityProvider observabilityProvider) {
            this.observabilityProvider = Objects.requireNonNull(observabilityProvider, "observabilityProvider");
            return this;
        }

        public CoordConfig build() {
            validate();
            return new CoordConfig(this);
        }

        private void validate() {
            if (agentHost == null || agentHost.isBlank()) {
                throw new IllegalArgumentException("agentHost must not be null or blank");
            }
            if (agentPort <= 0 || agentPort > 65535) {
                throw new IllegalArgumentException("agentPort must be between 1 and 65535, got: " + agentPort);
            }
            if (requestTimeout == null) {
                throw new IllegalArgumentException("requestTimeout must not be null");
            }
            if (heartbeatThreads <= 0) {
                throw new IllegalArgumentException("heartbeatThreads must be positive, got: " + heartbeatThreads);
            }
            if (useTls) {
                throw new UnsupportedOperationException("TLS is not supported in this version");
            }
        }
    }
}
