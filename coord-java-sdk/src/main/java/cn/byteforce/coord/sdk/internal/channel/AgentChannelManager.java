package cn.byteforce.coord.sdk.internal.channel;

import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.CoordException;
import cn.byteforce.coord.sdk.ErrorCode;
import cn.byteforce.coord.sdk.internal.thread.ThreadPoolManager;
import cn.byteforce.coord.sdk.internal.proto.HandshakeGrpc;
import cn.byteforce.coord.sdk.internal.proto.HandshakeRequest;
import cn.byteforce.coord.sdk.internal.proto.HandshakeResponse;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages a single gRPC {@link ManagedChannel} connection to the Coord Agent.
 * <p>
 * Transport security is driven by {@link CoordConfig#isUseTls()} (D1): TLS config that is
 * incomplete or points at missing files is <b>rejected</b> rather than silently downgraded to
 * plaintext. When a CCT supplier is configured, every call carries
 * {@code authorization: Bearer <cct>} (D2).
 * <p>
 * Note: connectivity (including reconnection after the agent restarts) is handled by gRPC's
 * own channel state machine plus the keep-alive settings below — this class does not run its
 * own backoff loop.
 */
public class AgentChannelManager {

    private static final Logger log = LoggerFactory.getLogger(AgentChannelManager.class);

    private final CoordConfig config;
    private final ThreadPoolManager threadPoolManager;
    private final ObservabilityProvider observability;

    private volatile ManagedChannel channel;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final ProtocolNegotiator negotiator;

    public AgentChannelManager(CoordConfig config, ThreadPoolManager threadPoolManager,
                                ObservabilityProvider observability) {
        this.config = config;
        this.threadPoolManager = threadPoolManager;
        this.observability = observability;
        this.negotiator = new ProtocolNegotiator(ProtocolNegotiator.SDK_PROTOCOL_VERSION);
        this.channel = createChannel();
    }

    private ManagedChannel createChannel() {
        NettyChannelBuilder builder = NettyChannelBuilder
                .forAddress(config.getAgentHost(), config.getAgentPort())
                .keepAliveTime(30, TimeUnit.SECONDS)
                .keepAliveTimeout(10, TimeUnit.SECONDS)
                .keepAliveWithoutCalls(true);

        if (config.isUseTls()) {
            // D1：配了 TLS 就必须真正建 TLS 链路；证书缺失即拒绝连接（fail-closed）。
            File caFile = requireFile(config.getTlsCaCertPath(), "tlsCaCertPath");
            String clientCert = config.getTlsClientCertPath();
            boolean mtls = clientCert != null && !clientCert.isBlank();

            SslContextBuilder ssl = GrpcSslContexts.forClient().trustManager(caFile);
            if (mtls) {
                ssl.keyManager(
                        requireFile(clientCert, "tlsClientCertPath"),
                        requireFile(config.getTlsClientKeyPath(), "tlsClientKeyPath"));
            }
            try {
                builder.sslContext(ssl.build());
            } catch (SSLException e) {
                throw new CoordException(ErrorCode.CONFIG_INVALID,
                        "failed to build TLS context: " + e.getMessage(), e);
            }
            log.info("Coord Java SDK: TLS enabled (mTLS={})", mtls);
        } else {
            // 明文仅用于开发；生产必须 useTls(true)。
            builder.usePlaintext();
            log.warn("Coord Java SDK: PLAINTEXT channel (dev only; set useTls(true) for production)");
        }

        // D2：每次调用读取当前 CCT 并注入 Metadata（刷新无需重建 channel）。
        if (config.getAuthTokenSupplier() != null) {
            builder.intercept(new AuthTokenInterceptor(config.getAuthTokenSupplier()));
        }

        return builder.build();
    }

    /**
     * D1：当 TLS 启用时，证书/私钥路径必须指向真实文件，否则拒绝建链。
     */
    private static File requireFile(String path, String field) {
        if (path == null || path.isBlank()) {
            throw new CoordException(ErrorCode.CONFIG_INVALID,
                    field + " must be set when TLS is enabled");
        }
        File file = new File(path);
        if (!file.isFile()) {
            throw new CoordException(ErrorCode.CONFIG_INVALID,
                    field + " does not exist or is not a regular file: " + path);
        }
        return file;
    }

    /**
     * Returns the current channel.
     *
     * @throws CoordException if the channel has been shut down
     */
    public ManagedChannel getChannel() {
        if (shutdown.get()) {
            throw new CoordException(ErrorCode.AGENT_UNAVAILABLE, "Channel is shut down");
        }
        return channel;
    }

    /**
     * Block until the channel reaches a ready state or the timeout expires.
     *
     * @param timeout maximum time to wait
     * @return true if connected, false if timeout elapsed
     */
    public boolean awaitReady(Duration timeout) {
        try {
            var state = channel.getState(true);
            var deadline = System.nanoTime() + timeout.toNanos();
            while (state != io.grpc.ConnectivityState.READY
                    && state != io.grpc.ConnectivityState.SHUTDOWN) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) return false;
                long waitMs = Math.min(remaining / 1_000_000, 100);
                state = channel.getState(true);
                if (state == io.grpc.ConnectivityState.READY
                        || state == io.grpc.ConnectivityState.SHUTDOWN) break;
                Thread.sleep(waitMs);
            }
            return state == io.grpc.ConnectivityState.READY;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Perform a real protocol version negotiation against the agent
     * ({@code /coord.agent.Handshake/Negotiate}).
     *
     * <p><b>P0-4 / D6.</b> Before this, the SDK never called the endpoint and the agent never
     * implemented it: an SDK/agent version mismatch surfaced as a bare gRPC
     * {@code UNIMPLEMENTED} ("unknown service") when the first real RPC was issued, with no
     * hint that a version mismatch was the cause. Since the agent's services were renamed in
     * {@code contracts/v1.2.0} in a one-shot switch (no dual-serving period), that
     * un-diagnosable failure mode is precisely what this closes.
     *
     * @param deadline maximum time to wait for the negotiation RPC
     * @return the versions advertised by the agent (never null; possibly empty)
     * @throws CoordException {@link ErrorCode#PROTOCOL_MISMATCH} if the agent does not
     *         advertise this SDK's version, {@link ErrorCode#AGENT_UNAVAILABLE} if unreachable
     */
    public List<String> negotiate(Duration deadline) {
        var stub = HandshakeGrpc.newBlockingStub(getChannel())
                .withDeadlineAfter(Math.max(1, deadline.toMillis()), TimeUnit.MILLISECONDS);
        HandshakeResponse resp;
        try {
            resp = stub.negotiate(HandshakeRequest.newBuilder()
                    .setClientVersion(negotiator.getSdkVersion())
                    .build());
        } catch (StatusRuntimeException e) {
            throw new CoordException(ErrorCode.AGENT_UNAVAILABLE,
                    "Handshake.Negotiate failed against the agent: " + e.getStatus(), e);
        }
        List<String> agentVersions = resp.getSupportedVersionsList();
        // 版本不匹配 -> 可诊断异常（而不是等第一个业务 RPC 报 UNIMPLEMENTED）。
        negotiator.requireSupported(agentVersions);
        log.info("Coord Java SDK: protocol negotiation OK (sdk={}, agent={})",
                negotiator.getSdkVersion(), agentVersions);
        return agentVersions;
    }

    /**
     * Wait for readiness and then negotiate the protocol version.
     *
     * @param timeout budget shared by the connect wait and the negotiation RPC
     * @return the versions advertised by the agent (never null)
     * @throws CoordException {@link ErrorCode#AGENT_UNAVAILABLE} on connect timeout,
     *         {@link ErrorCode#PROTOCOL_MISMATCH} on version mismatch
     */
    public List<String> connectAndNegotiate(Duration timeout) {
        long startNanos = System.nanoTime();
        if (!awaitReady(timeout)) {
            throw new CoordException(ErrorCode.AGENT_UNAVAILABLE,
                    "agent channel did not become ready within " + timeout);
        }
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
        long remainingMs = Math.max(1, timeout.toMillis() - elapsedMs);
        return negotiate(Duration.ofMillis(remainingMs));
    }

    /**
     * Check if the channel has been shut down.
     */
    public boolean isShutdown() {
        return shutdown.get();
    }

    /**
     * Gracefully shut down the channel.
     */
    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            log.info("Shutting down Agent channel");
            if (channel != null && !channel.isShutdown()) {
                channel.shutdown();
                try {
                    if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                        channel.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    channel.shutdownNow();
                }
            }
        }
    }

}
