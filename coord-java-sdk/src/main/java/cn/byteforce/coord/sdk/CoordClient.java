package cn.byteforce.coord.sdk;

import cn.byteforce.coord.sdk.cache.CacheClient;
import cn.byteforce.coord.sdk.circuitbreaker.CircuitBreakerClient;
import cn.byteforce.coord.sdk.config.ConfigClient;
import cn.byteforce.coord.sdk.election.LeaderElectionClient;
import cn.byteforce.coord.sdk.event.EventClient;
import cn.byteforce.coord.sdk.featureflags.FeatureFlagClient;
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
import cn.byteforce.coord.sdk.ratelimiter.RateLimiterClient;
import cn.byteforce.coord.sdk.registry.Registry;
import cn.byteforce.coord.sdk.scheduler.SchedulerClient;
import cn.byteforce.coord.sdk.transit.TransitClient;
import cn.byteforce.coord.sdk.workflow.WorkflowClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.List;
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
 *   <li>{@link #election()} — Lease-based leader election</li>
 *   <li>{@link #events()} — Publish/subscribe event notification</li>
 *   <li>{@link #scheduler()} — Distributed job scheduling (register/claim/complete)</li>
 *   <li>{@link #circuitBreaker()} — Agent-local circuit breaker state</li>
 *   <li>{@link #rateLimiter()} — Agent-local token-bucket rate limiting</li>
 *   <li>{@link #featureFlags()} — Feature flag evaluation (read-only wire)</li>
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
    private final LeaderElectionClientImpl electionClient;
    private final EventClientImpl eventClient;
    private final SchedulerClientImpl schedulerClient;
    private final CircuitBreakerClientImpl circuitBreakerClient;
    private final RateLimiterClientImpl rateLimiterClient;
    private final FeatureFlagClientImpl featureFlagClient;

    /** P0-4 / D6：协议协商是否已成功完成（只做一次；失败会回滚以便重试）。 */
    private final java.util.concurrent.atomic.AtomicBoolean protocolNegotiated =
            new java.util.concurrent.atomic.AtomicBoolean(false);
    /** 最近一次协商得到的 agent 版本列表（供诊断 / 运维可见性）。 */
    private final List<String> agentProtocolVersions =
            java.util.Collections.synchronizedList(new java.util.ArrayList<>());

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
        this.electionClient = new LeaderElectionClientImpl(channelManager, errorMapper,
                retryTemplate, config.getObservabilityProvider(), config);
        // 事件订阅的取流循环跑在受管理的虚拟线程执行器上（与 Watch / Workflow 同纪律）：
        // 这样 close() 才能真的中断它。
        this.eventClient = new EventClientImpl(channelManager, errorMapper, retryTemplate,
                config.getObservabilityProvider(), config,
                threadPoolManager.getVirtualThreadExecutor());
        this.schedulerClient = new SchedulerClientImpl(channelManager, errorMapper, retryTemplate,
                config.getObservabilityProvider(), config);
        this.circuitBreakerClient = new CircuitBreakerClientImpl(channelManager, errorMapper,
                retryTemplate, config.getObservabilityProvider(), config);
        this.rateLimiterClient = new RateLimiterClientImpl(channelManager, errorMapper,
                retryTemplate, config.getObservabilityProvider(), config);
        this.featureFlagClient = new FeatureFlagClientImpl(channelManager, errorMapper,
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
     * Returns the {@link LeaderElectionClient} API for lease-based leader election.
     */
    public LeaderElectionClient election() {
        return electionClient;
    }

    /**
     * Returns the {@link EventClient} API for publish/subscribe event notification.
     * <p>
     * <b>Boundary:</b> delivery is at-least-once only while the subscription stream is
     * alive — the disconnect window is <b>not</b> replayed (no persisted cursor).
     */
    public EventClient events() {
        return eventClient;
    }

    /**
     * Returns the {@link SchedulerClient} API for distributed job scheduling.
     * <p>
     * Claiming is a cross-node atomic CAS, and the returned job id is the claim
     * credential (the wire carries no worker identity) — treat it as a secret.
     */
    public SchedulerClient scheduler() {
        return schedulerClient;
    }

    /**
     * Returns the {@link CircuitBreakerClient} API.
     * <p>
     * <b>Boundary:</b> breaker state lives in <b>one agent's memory</b> — it is not
     * shared across agents and is lost on agent restart.
     */
    public CircuitBreakerClient circuitBreaker() {
        return circuitBreakerClient;
    }

    /**
     * Returns the {@link RateLimiterClient} API.
     * <p>
     * <b>Boundary:</b> the token bucket is <b>per-agent</b> — the effective
     * cluster-wide rate is (limit × number of agents) unless callers are pinned to one
     * agent.
     */
    public RateLimiterClient rateLimiter() {
        return rateLimiterClient;
    }

    /**
     * Returns the {@link FeatureFlagClient} API.
     * <p>
     * The wire surface is read-only and uncached by design: flags are updated out of
     * band, so a local cache without invalidation would serve stale values silently.
     */
    public FeatureFlagClient featureFlags() {
        return featureFlagClient;
    }

    /**
     * Check the health of the Agent connection.
     *
     * @return {@link HealthStatus#SERVING} if the Agent responds healthy,
     *         {@link HealthStatus#NOT_SERVING} otherwise (including timeout/error).
     */
    public HealthStatus healthCheck() {
        // P0-4 / D6：健康探测是"我能否与 agent 对话"的自然入口，因此顺带做一次
        // 协议协商（幂等、只做一次）。这样任何调用 healthCheck 的应用（含
        // java-example）都会在启动期接触到版本协商。
        try {
            awaitConnected(config.getRequestTimeout());
        } catch (CoordException e) {
            // 版本不匹配是**可诊断**故障，不是"暂时不可用"：必须打到 WARN 并带上
            // 三要素（SDK 版本 / agent 广告的版本 / 该怎么办），否则 D6 修复的意义
            // ——"让失败可诊断"—— 会被一个 NOT_SERVING 布尔值吞掉。
            if (e.getErrorCode() == ErrorCode.PROTOCOL_MISMATCH) {
                log.warn("Protocol negotiation failed; agent NOT usable: {}", e.getMessage());
            } else {
                log.debug("Protocol negotiation during health check failed: {}", e.getMessage());
            }
            return HealthStatus.NOT_SERVING;
        } catch (RuntimeException e) {
            log.debug("Protocol negotiation during health check failed: {}", e.getMessage());
            return HealthStatus.NOT_SERVING;
        }
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
     * Wait for the agent to become reachable and complete protocol version negotiation.
     *
     * <p><b>P0-4 / D6.</b> Before this existed, the SDK never called
     * {@code /coord.agent.Handshake/Negotiate} — and the agent never implemented it. The SDK
     * could therefore connect successfully to a version-incompatible agent and only fail
     * later with a bare gRPC {@code UNIMPLEMENTED} ("unknown service"), which says nothing
     * about a version mismatch. That matters because the agent's services were renamed in
     * {@code contracts/v1.2.0} in a <b>one-shot switch</b> (no dual-serving period): old
     * clients must fail with a version error, not with an unexplained unknown-service error.
     *
     * <p>Idempotent and cached: after a successful negotiation this returns immediately, so it
     * is safe (and recommended) to call once at application startup.
     *
     * @param timeout budget covering connect + negotiation
     * @throws CoordException {@link ErrorCode#AGENT_UNAVAILABLE} if the agent is unreachable,
     *         {@link ErrorCode#PROTOCOL_MISMATCH} if it does not speak this SDK's version
     */
    public void awaitConnected(Duration timeout) {
        if (protocolNegotiated.compareAndSet(false, true)) {
            try {
                List<String> versions = channelManager.connectAndNegotiate(timeout);
                agentProtocolVersions.clear();
                agentProtocolVersions.addAll(versions);
            } catch (RuntimeException e) {
                // 协商失败 ⇒ 允许下一次重试（不得把"一次网络抖动"固化成永久失败）。
                protocolNegotiated.set(false);
                throw e;
            }
        }
    }

    /**
     * The protocol versions advertised by the agent, or an empty list if negotiation has not
     * succeeded yet. Populated by {@link #awaitConnected(Duration)} / {@link #healthCheck()}.
     */
    public List<String> agentProtocolVersions() {
        return List.copyOf(agentProtocolVersions);
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
