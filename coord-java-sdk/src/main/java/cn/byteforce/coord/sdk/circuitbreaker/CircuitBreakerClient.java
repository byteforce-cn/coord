package cn.byteforce.coord.sdk.circuitbreaker;

import cn.byteforce.coord.sdk.CoordException;

/**
 * Circuit breaker API (per-name breaker state held by the Coord Agent).
 * <p>
 * <b>Boundary (explicit, per {@code WHITEPAPER.md} §10 rule 4):</b> breaker state is
 * <b>agent-local memory</b>. It is deliberately <b>not</b> shared or replicated across
 * agents, and it is not persisted — an agent restart resets every breaker. Route all
 * traffic for a given breaker name through one agent if you need a single shared view.
 *
 * <pre>{@code
 * CircuitBreakerClient cb = client.circuitBreaker();
 * try {
 *     callDownstream();
 *     cb.reportSuccess("payment-gw");
 * } catch (Exception e) {
 *     cb.reportFailure("payment-gw");
 * }
 * if (cb.getState("payment-gw").getState() == CircuitState.OPEN) { ... }
 * }</pre>
 */
public interface CircuitBreakerClient {

    /**
     * Read the current state of a breaker.
     *
     * @param name breaker name
     * @return the state snapshot
     * @throws CoordException on communication failure
     */
    BreakerState getState(String name);

    /**
     * Record a successful call (may close a half-open breaker).
     *
     * @throws CoordException on communication failure
     */
    void reportSuccess(String name);

    /**
     * Record a failed call (may trip the breaker open).
     *
     * @throws CoordException on communication failure
     */
    void reportFailure(String name);

    /**
     * Force a breaker back to its initial (closed) state.
     *
     * @throws CoordException on communication failure
     */
    void reset(String name);

    /** Breaker states as defined by the contract. */
    enum CircuitState {
        /** Failing count is under threshold; calls pass through. */
        CLOSED,
        /** Threshold tripped; calls are short-circuited. */
        OPEN,
        /** Probing: a limited number of calls are let through. */
        HALF_OPEN
    }

    /** Immutable breaker snapshot. */
    final class BreakerState {
        private final CircuitState state;
        private final long lastFailureTime;

        public BreakerState(CircuitState state, long lastFailureTime) {
            this.state = state;
            this.lastFailureTime = lastFailureTime;
        }

        public CircuitState getState() {
            return state;
        }

        /** Unix millis of the most recent recorded failure, or {@code 0}. */
        public long getLastFailureTime() {
            return lastFailureTime;
        }
    }
}
