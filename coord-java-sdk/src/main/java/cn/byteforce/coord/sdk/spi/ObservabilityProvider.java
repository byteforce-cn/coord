package cn.byteforce.coord.sdk.spi;

/**
 * SPI for observability instrumentation.
 * Implementations receive callbacks for RPC calls, watch events, and connection state changes.
 * The default no-op implementation is used when none is provided.
 */
public interface ObservabilityProvider {

    /** Called after every RPC call completes (success or failure). */
    default void recordRpcCall(String operation, long durationNanos, boolean success) {}

    /** Called when a watch event is received. */
    default void recordWatchEvent(String watchId) {}

    /** Called when the connection state transitions. */
    default void recordConnectionStateChange(String from, String to) {}
}
