package cn.byteforce.coord.sdk.registry;

import java.io.Closeable;
import java.time.Duration;

/**
 * Handle for a registered service instance.
 * Provides lifecycle management including heartbeat and deregistration.
 * <p>
 * Heartbeat is automatically managed — the SDK sends keep-alive pings at
 * {@code ttl/3} intervals. Call {@link #close()} to deregister and stop heartbeats.
 */
public interface Registration extends Closeable {

    /** Synchronously deregister with default timeout (5 seconds). */
    @Override
    void close();

    /**
     * Deregister with an explicit timeout.
     * @param timeout maximum time to wait for the deregister RPC
     */
    void close(Duration timeout);

    /**
     * Register a callback invoked when heartbeats fail 3 consecutive times.
     * The callback fires at most once per 30-second window (throttled).
     * @return this Registration for chaining
     */
    Registration onHeartbeatFailed(HeartbeatFailedCallback callback);

    /** Callback for heartbeat failures (throttled: at most once per 30s). */
    @FunctionalInterface
    interface HeartbeatFailedCallback {
        void onHeartbeatFailed(Registration registration);
    }
}
