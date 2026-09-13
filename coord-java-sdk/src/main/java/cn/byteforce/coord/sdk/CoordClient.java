package cn.byteforce.coord.sdk;

import cn.byteforce.coord.sdk.cache.CacheClient;
import cn.byteforce.coord.sdk.config.ConfigClient;
import cn.byteforce.coord.sdk.health.HealthStatus;
import cn.byteforce.coord.sdk.idgen.IdGenClient;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.sdk.internal.proto.HealthCheckRequest;
import cn.byteforce.coord.sdk.internal.proto.HealthCheckResponse;
import cn.byteforce.coord.sdk.internal.proto.HealthGrpc;
import cn.byteforce.coord.sdk.internal.rpc.*;
import cn.byteforce.coord.sdk.internal.thread.ThreadPoolManager;
import cn.byteforce.coord.sdk.internal.watch.WatchManager;
import cn.byteforce.coord.sdk.lock.LockClient;
import cn.byteforce.coord.sdk.mq.MqClient;
import cn.byteforce.coord.sdk.objectstore.ObjectStoreClient;
import cn.byteforce.coord.sdk.pki.PkiClient;
import cn.byteforce.coord.sdk.policy.PolicyClient;
import cn.byteforce.coord.sdk.registry.Registry;
import cn.byteforce.coord.sdk.transit.TransitClient;
import cn.byteforce.coord.sdk.workflow.WorkflowClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * The main entry point for interacting with a Coord Agent.
 * <p>
 * Provides APIs for:
 * <ul>
 *   <li>{@link #registry()} — Service registration and discovery</li>
 *   <li>{@link #configClient()} — Dynamic configuration with watch</li>
 *   <li>{@link #lock()} — Distributed mutex locks</li>
 *   <li>{@link #idgen()} — Distributed unique ID generation</li>
 *   <li>{@link #cache()} — Distributed cache (String/Hash/List/Set)</li>
 *   <li>{@link #mq()} — Message queue (topic/publish/poll/ack)</li>
 *   <li>{@link #transit()} — Envelope encryption/decryption</li>
 *   <li>{@link #workflow()} — Workflow definition and instance management</li>
 *   <li>{@link #policy()} — RBAC/ABAC policy evaluation</li>
 *   <li>{@link #pki()} — Local PKI CA certificate operations</li>
 * </ul>
 * Must be closed via {@link #close()} to release resources gracefully.
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
    private final LockClientImpl lockClient;
    private final IdGenClientImpl idgenClient;
    private final CacheClientImpl cacheClient;
    private final MqClientImpl mqClient;
    private final TransitClientImpl transitClient;
    private final WorkflowClientImpl workflowClient;
    private final PolicyClientImpl policyClient;
    private final PkiClientImpl pkiClient;
    private final ObjectStoreClientImpl objectStoreClient;

    private CoordClient(CoordConfig config) {
        this.config = config;
        this.threadPoolManager = new ThreadPoolManager(config.getHeartbeatThreads());
        this.channelManager = new AgentChannelManager(config, threadPoolManager,
                config.getObservabilityProvider());
        // 第四轮 §3.14.3：autoRestoreWatches 终于被读到了。此前它被声明、有 getter、
        // 默认 true、有 setter，但**主代码零处读取**（死配置）——而它承诺的能力
        // （断线自动恢复订阅）根本不存在。现在由 WatchManager 真正执行。
        this.watchManager = new WatchManager(threadPoolManager, config.isAutoRestoreWatches());
        this.errorMapper = new ErrorMapper();
        this.retryTemplate = new RetryTemplate();

        this.registry = new RegistryImpl(channelManager, errorMapper, retryTemplate,
                config.getObservabilityProvider(), config, watchManager,
                threadPoolManager.getHeartbeatScheduler());
        this.configClient = new ConfigClientImpl(channelManager, errorMapper, retryTemplate,
                config.getObservabilityProvider(), config, watchManager);
        this.lockClient = new LockClientImpl(channelManager, errorMapper, retryTemplate,
                config.getObservabilityProvider(), config);
        this.idgenClient = new IdGenClientImpl(channelManager, errorMapper, retryTemplate,
                config.getObservabilityProvider(), config);
        this.cacheClient = new CacheClientImpl(channelManager, errorMapper, retryTemplate,
                config.getObservabilityProvider(), config);
        this.mqClient = new MqClientImpl(channelManager, errorMapper, retryTemplate,
                config.getObservabilityProvider(), config);
        this.transitClient = new TransitClientImpl(channelManager, errorMapper, retryTemplate,
                config.getObservabilityProvider(), config);
        this.workflowClient = new WorkflowClientImpl(channelManager, errorMapper, retryTemplate,
                config.getObservabilityProvider(), config,
                // 第四轮 §3.14.4：轮询必须跑在受管理的执行器上，close() 才能中断它。
                threadPoolManager.getVirtualThreadExecutor());
        this.policyClient = new PolicyClientImpl(channelManager, errorMapper, retryTemplate,
                config.getObservabilityProvider(), config);
        this.pkiClient = new PkiClientImpl(channelManager, errorMapper, retryTemplate,
                config.getObservabilityProvider(), config);
        this.objectStoreClient = new ObjectStoreClientImpl(channelManager, errorMapper,
                retryTemplate, config.getObservabilityProvider(), config);
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
     * Returns the {@link LockClient} API for distributed lock operations.
     */
    public LockClient lock() {
        return lockClient;
    }

    /**
     * Returns the {@link IdGenClient} API for distributed ID generation.
     */
    public IdGenClient idgen() {
        return idgenClient;
    }

    /**
     * Returns the {@link CacheClient} API for distributed cache operations.
     */
    public CacheClient cache() {
        return cacheClient;
    }

    /**
     * Returns the {@link MqClient} API for message queue operations
     * (topic management, publish, poll + ack consumption, DLQ observation).
     */
    public MqClient mq() {
        return mqClient;
    }

    /**
     * Returns the {@link TransitClient} API for envelope encryption/decryption.
     */
    public TransitClient transit() {
        return transitClient;
    }

    /**
     * Returns the {@link WorkflowClient} API for workflow definition management
     * and instance lifecycle.
     */
    public WorkflowClient workflow() {
        return workflowClient;
    }

    /**
     * Returns the {@link PolicyClient} API for RBAC/ABAC policy evaluation.
     */
    public PolicyClient policy() {
        return policyClient;
    }

    /**
     * Returns the {@link PkiClient} API for PKI CA certificate operations.
     * <p>
     * Backed by the Coord Agent's PKI service via gRPC.
     * Call {@link PkiClient#initCa(String)} before issuing certificates.
     */
    public PkiClient pki() {
        return pkiClient;
    }

    /**
     * Returns the {@link ObjectStoreClient} API for object storage
     * (coord.storage, EXPERIMENTAL).
     * <p>
     * Proxied through the Coord Agent to the server cluster.
     */
    public ObjectStoreClient objectStore() {
        return objectStoreClient;
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

        // 2. Complete pending workflow watches exceptionally.
        //    第四轮 §3.14.4：不这样做，watchInstance()/startAsync() 的调用方会**永久挂起**
        //    （轮询线程不属于任何受管理线程池，close() 之后还在无限重试）。
        //    必须排在关闭线程池**之前**，否则这里会先撞上已经拒绝任务的执行器。
        workflowClient.shutdownWatches(new IllegalStateException(
                "CoordClient was closed while watching this workflow instance"));

        // 3. Cancel MQ subscriptions (此前不被 close() 追踪)
        int cancelledSubs = mqClient.cancelAllSubscriptions();
        if (cancelledSubs > 0) {
            log.debug("Cancelled {} MQ subscription(s)", cancelledSubs);
        }

        // 4. Shutdown watch manager (cancel all watch streams)
        watchManager.shutdown();

        // 5. Shutdown channel
        channelManager.shutdown();

        // 6. Shutdown thread pools (interrupts any in-flight polling)
        threadPoolManager.close();

        log.info("CoordClient shutdown complete");
    }
}
