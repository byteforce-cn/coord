package cn.byteforce.coord.sdk.ratelimiter;

import cn.byteforce.coord.sdk.CoordException;

/**
 * Rate limiter API (token bucket).
 * <p>
 * <b>Boundary (explicit, per {@code WHITEPAPER.md} §10 rule 4):</b> the token bucket is
 * <b>agent-local memory</b>. It is deliberately <b>not</b> shared across agents, so the
 * effective cluster-wide limit is {@code perAgentLimit × numberOfAgents} unless callers
 * are pinned to a single agent. The bucket is not persisted across agent restarts.
 *
 * <pre>{@code
 * RateLimiterClient rl = client.rateLimiter();
 * RateLimitDecision d = rl.allow("orders-api", 1);
 * if (!d.isAllowed()) {
 *     // shed or retry after d.getResetTime()
 * }
 * }</pre>
 */
public interface RateLimiterClient {

    /**
     * Try to take {@code permits} tokens from the bucket named {@code key}.
     *
     * @param key     bucket name (resource or tenant identifier)
     * @param permits number of tokens to take; must be positive
     * @return the decision, including remaining tokens and the reset time
     * @throws CoordException on communication failure
     */
    RateLimitDecision allow(String key, int permits);

    /** Outcome of a single {@link #allow} call. */
    final class RateLimitDecision {
        private final boolean allowed;
        private final long remaining;
        private final long resetTime;

        public RateLimitDecision(boolean allowed, long remaining, long resetTime) {
            this.allowed = allowed;
            this.remaining = remaining;
            this.resetTime = resetTime;
        }

        /** Whether the requested permits were granted. */
        public boolean isAllowed() {
            return allowed;
        }

        /** Tokens left in the bucket after this call. */
        public long getRemaining() {
            return remaining;
        }

        /** Unix millis when the bucket refills, per the agent's clock. */
        public long getResetTime() {
            return resetTime;
        }
    }
}
