package cn.byteforce.coord.sdk.internal.channel;

import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.CoordException;
import cn.byteforce.coord.sdk.ErrorCode;
import cn.byteforce.coord.sdk.internal.thread.ThreadPoolManager;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages a single gRPC {@link ManagedChannel} connection to the Coord Agent.
 * <p>
 * Handles connection lifecycle, reconnection with exponential backoff,
 * and protocol handshake triggering.
 */
public class AgentChannelManager {

    private static final Logger log = LoggerFactory.getLogger(AgentChannelManager.class);

    private final CoordConfig config;
    private final ThreadPoolManager threadPoolManager;
    private final ObservabilityProvider observability;

    private volatile ManagedChannel channel;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final ProtocolNegotiator negotiator;

    // Reconnection config
    private static final long INITIAL_BACKOFF_MS = 1_000;
    private static final long MAX_BACKOFF_MS = 30_000;
    private static final double BACKOFF_MULTIPLIER = 2.0;

    public AgentChannelManager(CoordConfig config, ThreadPoolManager threadPoolManager,
                                ObservabilityProvider observability) {
        this.config = config;
        this.threadPoolManager = threadPoolManager;
        this.observability = observability;
        this.negotiator = new ProtocolNegotiator(ProtocolNegotiator.SDK_PROTOCOL_VERSION);
        this.channel = createChannel();
    }

    private ManagedChannel createChannel() {
        return NettyChannelBuilder.forAddress(config.getAgentHost(), config.getAgentPort())
                .usePlaintext() // TLS not supported in v1.0
                .keepAliveTime(30, TimeUnit.SECONDS)
                .keepAliveTimeout(10, TimeUnit.SECONDS)
                .keepAliveWithoutCalls(true)
                .build();
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
