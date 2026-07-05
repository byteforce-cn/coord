package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.CoordException;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.sdk.internal.proto.DeregisterRequest;
import cn.byteforce.coord.sdk.internal.proto.DiscoverRequest;
import cn.byteforce.coord.sdk.internal.proto.DiscoverResponse;
import cn.byteforce.coord.sdk.internal.proto.HeartbeatRequest;
import cn.byteforce.coord.sdk.internal.proto.RegisterRequest;
import cn.byteforce.coord.sdk.internal.proto.RegisterResponse;
import cn.byteforce.coord.sdk.internal.proto.RegistryGrpc;
import cn.byteforce.coord.sdk.internal.proto.WatchEvent;
import cn.byteforce.coord.sdk.internal.proto.WatchRequest;
import cn.byteforce.coord.sdk.internal.watch.WatchManager;
import cn.byteforce.coord.sdk.registry.*;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implementation of {@link Registry} backed by gRPC calls to the Coord Agent.
 */
public final class RegistryImpl extends AgentRpcClient implements Registry {

    private static final Logger log = LoggerFactory.getLogger(RegistryImpl.class);
    private static final int HEARTBEAT_FAIL_THRESHOLD = 3;
    private static final long HEARTBEAT_FAIL_COOLDOWN_MS = 30_000;

    private final CoordConfig config;
    private final WatchManager watchManager;
    private final ScheduledExecutorService heartbeatScheduler;
    private final List<RegistrationImpl> activeRegistrations = new ArrayList<>();

    public RegistryImpl(AgentChannelManager channelManager, ErrorMapper errorMapper,
                 RetryTemplate retryTemplate, ObservabilityProvider observability,
                 CoordConfig config, WatchManager watchManager,
                 ScheduledExecutorService heartbeatScheduler) {
        super(channelManager, errorMapper, retryTemplate, observability);
        this.config = config;
        this.watchManager = watchManager;
        this.heartbeatScheduler = heartbeatScheduler;
    }

    @Override
    public Registration register(String serviceName, String instanceId,
                                  String metadata, int ttlSeconds) throws CoordException {
        RegisterRequest request = RegisterRequest.newBuilder()
                .setServiceName(serviceName)
                .setInstanceId(instanceId)
                .setMetadata(metadata != null ? metadata : "")
                .setTtlSeconds(ttlSeconds)
                .build();

        RegisterResponse response = callWithRetry(
                (ch, req) -> RegistryGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .register((RegisterRequest) req),
                request, "registry.register");

        long leaseId = response.getLeaseId();
        log.info("Registered service={} instance={} leaseId={} ttl={}s",
                serviceName, instanceId, leaseId, ttlSeconds);

        RegistrationImpl reg = new RegistrationImpl(serviceName, instanceId, leaseId, ttlSeconds);
        reg.startHeartbeat();
        synchronized (activeRegistrations) {
            activeRegistrations.add(reg);
        }
        return reg;
    }

    @Override
    public List<ServiceInstance> discover(String serviceName) throws CoordException {
        return discover(serviceName, FilterMode.EXACT);
    }

    @Override
    public DiscoverResult discoverWithRevision(String serviceName) throws CoordException {
        return discoverWithRevision(serviceName, FilterMode.EXACT);
    }

    @Override
    public List<ServiceInstance> discover(String serviceName, FilterMode filterMode) throws CoordException {
        return discoverWithRevision(serviceName, filterMode).instances();
    }

    @Override
    public DiscoverResult discoverWithRevision(String serviceName, FilterMode filterMode) throws CoordException {
        DiscoverRequest request = DiscoverRequest.newBuilder()
                .setServiceName(serviceName)
                .setFilterModeValue(filterMode.toProtoValue())
                .build();

        DiscoverResponse response = callWithRetry(
                (ch, req) -> RegistryGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .discover((DiscoverRequest) req),
                request, "registry.discover");

        List<ServiceInstance> instances = new ArrayList<>();
        for (cn.byteforce.coord.sdk.internal.proto.ServiceInstance si : response.getInstancesList()) {
            instances.add(new ServiceInstance(si.getInstanceId(), si.getServiceName(), si.getMetadata()));
        }
        return new DiscoverResult(instances, response.getRevision());
    }

    @Override
    public List<ServiceInstance> discoverAll() throws CoordException {
        return discoverAllWithRevision().instances();
    }

    @Override
    public DiscoverResult discoverAllWithRevision() throws CoordException {
        DiscoverRequest request = DiscoverRequest.newBuilder()
                .setServiceName("")
                .setFilterModeValue(FilterMode.ALL.toProtoValue())
                .build();

        DiscoverResponse response = callWithRetry(
                (ch, req) -> RegistryGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .discover((DiscoverRequest) req),
                request, "registry.discoverAll");

        List<ServiceInstance> instances = new ArrayList<>();
        for (cn.byteforce.coord.sdk.internal.proto.ServiceInstance si : response.getInstancesList()) {
            instances.add(new ServiceInstance(si.getInstanceId(), si.getServiceName(), si.getMetadata()));
        }
        return new DiscoverResult(instances, response.getRevision());
    }

    @Override
    public WatchSubscription watch(String serviceName, RegistryListener listener) throws CoordException {
        return watchFrom(serviceName, 0, listener);
    }

    @Override
    public WatchSubscription watchFrom(String serviceName, long startRevision,
                                        RegistryListener listener) throws CoordException {
        String watchId = "reg-" + serviceName + "-" + UUID.randomUUID().toString().substring(0, 8);

        // Use holder array to capture reference before constructor completes
        WatchManager.ActiveWatch[] holder = new WatchManager.ActiveWatch[1];

        holder[0] = new WatchManager.ActiveWatch(
                watchId,
                () -> {
                    WatchRequest request = WatchRequest.newBuilder()
                            .setServiceName(serviceName)
                            .setStartRevision(startRevision)
                            .build();
                    return RegistryGrpc.newBlockingStub(channelManager.getChannel())
                            .watch(request);
                },
                (WatchEvent protoEvent) -> {
                    RegistryEvent.EventType eventType = switch (protoEvent.getType()) {
                        case INSTANCES_ADDED -> RegistryEvent.EventType.INSTANCES_ADDED;
                        case INSTANCES_REMOVED -> RegistryEvent.EventType.INSTANCES_REMOVED;
                        case INSTANCES_UPDATED -> RegistryEvent.EventType.INSTANCES_UPDATED;
                        default -> RegistryEvent.EventType.INSTANCES_UPDATED;
                    };
                    List<ServiceInstance> instances = new ArrayList<>();
                    for (cn.byteforce.coord.sdk.internal.proto.ServiceInstance si : protoEvent.getInstancesList()) {
                        instances.add(new ServiceInstance(si.getInstanceId(), si.getServiceName(), si.getMetadata()));
                    }
                    holder[0].setLastRevision(protoEvent.getRevision());
                    listener.onEvent(new RegistryEvent(eventType, instances, protoEvent.getRevision()));
                },
                startRevision
        );

        watchManager.startWatch(holder[0]);
        return () -> watchManager.cancelWatch(watchId);
    }

    public void deregisterAll(Duration gracePeriod) {
        List<RegistrationImpl> snapshot;
        synchronized (activeRegistrations) {
            snapshot = new ArrayList<>(activeRegistrations);
        }
        for (RegistrationImpl reg : snapshot) {
            try {
                reg.close(gracePeriod);
            } catch (Exception e) {
                log.warn("Shutdown deregister failed for {}: {}", reg.instanceId, e.getMessage());
            }
        }
    }

    private class RegistrationImpl implements Registration {
        final String serviceName;
        final String instanceId;
        private final long leaseId;
        private final int ttlSeconds;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
        private final AtomicLong lastFailureCallbackTime = new AtomicLong(0);
        private volatile ScheduledFuture<?> heartbeatFuture;
        private volatile HeartbeatFailedCallback failedCallback;

        RegistrationImpl(String serviceName, String instanceId, long leaseId, int ttlSeconds) {
            this.serviceName = serviceName;
            this.instanceId = instanceId;
            this.leaseId = leaseId;
            this.ttlSeconds = ttlSeconds;
        }

        void startHeartbeat() {
            long intervalMs = (ttlSeconds * 1000L) / 3;
            heartbeatFuture = heartbeatScheduler.scheduleAtFixedRate(
                    this::sendHeartbeat, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        }

        private void sendHeartbeat() {
            if (closed.get()) return;
            try {
                HeartbeatRequest request = HeartbeatRequest.newBuilder()
                        .setServiceName(serviceName)
                        .setInstanceId(instanceId)
                        .setLeaseId(leaseId)
                        .build();
                RegistryGrpc.newBlockingStub(channelManager.getChannel())
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .heartbeat(request);
                consecutiveFailures.set(0);
            } catch (Exception e) {
                int failures = consecutiveFailures.incrementAndGet();
                log.debug("Heartbeat failed ({}/{}) for {}: {}",
                        failures, HEARTBEAT_FAIL_THRESHOLD, instanceId, e.getMessage());
                if (failures >= HEARTBEAT_FAIL_THRESHOLD) {
                    fireThrottledCallback();
                }
            }
        }

        private void fireThrottledCallback() {
            if (failedCallback == null) return;
            long now = System.currentTimeMillis();
            long lastFire = lastFailureCallbackTime.get();
            if (now - lastFire >= HEARTBEAT_FAIL_COOLDOWN_MS) {
                if (lastFailureCallbackTime.compareAndSet(lastFire, now)) {
                    try {
                        failedCallback.onHeartbeatFailed(this);
                    } catch (Exception ex) {
                        log.warn("HeartbeatFailedCallback threw exception", ex);
                    }
                }
            }
        }

        @Override
        public Registration onHeartbeatFailed(HeartbeatFailedCallback callback) {
            this.failedCallback = callback;
            return this;
        }

        @Override
        public void close() {
            close(config.getRequestTimeout());
        }

        @Override
        public void close(Duration timeout) {
            if (!closed.compareAndSet(false, true)) return;
            if (heartbeatFuture != null) {
                heartbeatFuture.cancel(false);
            }
            synchronized (activeRegistrations) {
                activeRegistrations.remove(this);
            }
            try {
                DeregisterRequest request = DeregisterRequest.newBuilder()
                        .setServiceName(serviceName)
                        .setInstanceId(instanceId)
                        .setLeaseId(leaseId)
                        .build();
                RegistryGrpc.newBlockingStub(channelManager.getChannel())
                        .withDeadlineAfter(timeout.toMillis(), TimeUnit.MILLISECONDS)
                        .deregister(request);
                log.info("Deregistered service={} instance={}", serviceName, instanceId);
            } catch (Exception e) {
                log.warn("Deregister failed for {}: {}", instanceId, e.getMessage());
            }
        }

        String getServiceName() { return serviceName; }
        String getInstanceId() { return instanceId; }
    }
}
