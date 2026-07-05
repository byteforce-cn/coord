package cn.byteforce.coord.sdk;

import cn.byteforce.coord.sdk.config.ConfigClient;
import cn.byteforce.coord.sdk.health.HealthStatus;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.sdk.internal.proto.HealthCheckRequest;
import cn.byteforce.coord.sdk.internal.proto.HealthCheckResponse;
import cn.byteforce.coord.sdk.internal.proto.HealthGrpc;
import cn.byteforce.coord.sdk.internal.rpc.ConfigClientImpl;
import cn.byteforce.coord.sdk.internal.rpc.ErrorMapper;
import cn.byteforce.coord.sdk.internal.rpc.RegistryImpl;
import cn.byteforce.coord.sdk.internal.rpc.RetryTemplate;
import cn.byteforce.coord.sdk.internal.thread.ThreadPoolManager;
import cn.byteforce.coord.sdk.internal.watch.WatchManager;
import cn.byteforce.coord.sdk.registry.Registry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * The main entry point for interacting with a Coord Agent.
 * <p>
 * Provides {@link #registry()} for service registration/discovery and
 * {@link #configClient()} for dynamic configuration. Must be closed
 * via {@link #close()} to release resources gracefully.
 *
 * <pre>{@code
 * CoordConfig config = CoordConfig.builder()
 *         .agentHost("localhost")
 *         .agentPort(19527)
 *         .build();
 *
 * try (CoordClient client = CoordClient.create(config)) {
 *     Registry registry = client.registry();
 *     Registration reg = registry.register("my-svc", "inst-1", "{}", 30);
 *     // ... use the client ...
 * }
 * }</pre>
 */
public final class CoordClient implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(CoordClient.class);

    private final CoordConfig config;
    private final ThreadPoolManager threadPoolManager;
    private final AgentChannelManager channelManager;
    private final WatchManager watchManager;
    private final ErrorMapper errorMapper;
    private final RetryTemplate retryTemplate;
    private final RegistryImpl registry;
    private final ConfigClientImpl configClient;

    private CoordClient(CoordConfig config) {
        this.config = config;
        this.threadPoolManager = new ThreadPoolManager(config.getHeartbeatThreads());
        this.channelManager = new AgentChannelManager(config, threadPoolManager,
                config.getObservabilityProvider());
        this.watchManager = new WatchManager(threadPoolManager);
        this.errorMapper = new ErrorMapper();
        this.retryTemplate = new RetryTemplate();

        this.registry = new RegistryImpl(channelManager, errorMapper, retryTemplate,
                config.getObservabilityProvider(), config, watchManager,
                threadPoolManager.getHeartbeatScheduler());
        this.configClient = new ConfigClientImpl(channelManager, errorMapper, retryTemplate,
                config.getObservabilityProvider(), config, watchManager);
    }

    /**
     * Create a new {@link CoordClient} with the given configuration.
     * The client is ready to use immediately after creation.
     *
     * @param config the immutable configuration
     * @return a new client instance
     */
    public static CoordClient create(CoordConfig config) {
        return new CoordClient(config);
    }

    /**
     * Returns the {@link Registry} API for service registration and discovery.
     */
    public Registry registry() {
        return registry;
    }

    /**
     * Returns the {@link ConfigClient} API for dynamic configuration access.
     */
    public ConfigClient configClient() {
        return configClient;
    }

    /**
     * Check the health of the Agent connection.
     *
     * @return {@link HealthStatus#SERVING} if the Agent responds healthy,
     *         {@link HealthStatus#NOT_SERVING} otherwise (including timeout/error).
     */
    public HealthStatus healthCheck() {
        try {
            HealthCheckRequest request = HealthCheckRequest.getDefaultInstance();
            HealthCheckResponse response = HealthGrpc.newBlockingStub(channelManager.getChannel())
                    .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                    .check(request);
            return response.getStatus() == HealthCheckResponse.ServingStatus.SERVING
                    ? HealthStatus.SERVING : HealthStatus.NOT_SERVING;
        } catch (Exception e) {
            log.debug("Health check failed: {}", e.getMessage());
            return HealthStatus.NOT_SERVING;
        }
    }

    /**
     * Close with the default grace period of 10 seconds.
     */
    @Override
    public void close() {
        close(Duration.ofSeconds(10));
    }

    /**
     * Close with an explicit grace period for deregistration.
     * <p>
     * Shutdown order (mandatory per design):
     * <ol>
     *   <li>Execute all pending deregistrations (with timeout)</li>
     *   <li>Cancel all Watch streams</li>
     *   <li>Shut down the gRPC channel</li>
     *   <li>Shut down thread pools</li>
     * </ol>
     */
    public void close(Duration gracePeriod) {
        log.info("Shutting down CoordClient (gracePeriod={})", gracePeriod);

        // 1. Deregister all active registrations
        registry.deregisterAll(gracePeriod);

        // 2. Shutdown watch manager
        watchManager.shutdown();

        // 3. Shutdown channel
        channelManager.shutdown();

        // 4. Shutdown thread pools
        threadPoolManager.close();

        log.info("CoordClient shutdown complete");
    }
}
