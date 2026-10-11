package cn.byteforce.coord.sdk.featureflags;

import cn.byteforce.coord.sdk.CoordException;

/**
 * Feature flag API.
 *
 * <h2>读面</h2>
 *
 * Flags are evaluated by the Coord Agent; evaluation has <b>no local caching</b>
 * (reads go straight to the shared store), so a successful management write takes
 * effect immediately — evaluated consistently from any connected agent.
 *
 * <h2>管理面</h2>
 *
 * {@link #putFlag} / {@link #putPercentageFlag} / {@link #deleteFlag} write the flag
 * state (stored in the server-wide KV: survives restarts, shared across agents —
 * capability {@code coord:flags:manage}); {@link #listFlags} enumerates them
 * ({@code coord:flags:read}).
 *
 * <pre>{@code
 * FeatureFlagClient ff = client.featureFlags();
 * ff.putFlag("new-checkout", true);
 * if (ff.isEnabled("new-checkout", "{\"userId\":\"u-1\"}").isEnabled()) {
 *     // new code path
 * }
 * }</pre>
 */
public interface FeatureFlagClient {

    /**
     * Evaluate a boolean flag.
     *
     * @param flagName flag name
     * @param context  optional JSON evaluation context (may be {@code null})
     * @return the decision, including the selected variant when the flag defines one
     * @throws CoordException on communication failure
     */
    FlagDecision isEnabled(String flagName, String context);

    /**
     * Evaluate a flag and return the raw JSON result (for non-boolean flags).
     *
     * @param flagName flag name
     * @param context  optional JSON evaluation context (may be {@code null})
     * @return the JSON evaluation result as a string (empty when the flag has none)
     * @throws CoordException on communication failure
     */
    String evaluate(String flagName, String context);

    /**
     * Create or overwrite a full on/off flag (idempotent). Takes effect immediately.
     *
     * @throws CoordException on communication failure
     */
    void putFlag(String flagName, boolean enabled);

    /**
     * Create or overwrite a percentage-rollout flag (idempotent).
     *
     * <p>Evaluation buckets by {@code user_id} (consistent hashing);
     * {@code percentage} must be within {@code [0, 100]}.
     *
     * @throws CoordException on communication failure (server rejects out-of-range
     *                         values with {@code INVALID_ARGUMENT})
     */
    void putPercentageFlag(String flagName, boolean enabled, int percentage);

    /**
     * Delete a flag (idempotent: deleting a missing flag also succeeds).
     * After deletion the flag evaluates to disabled (same as never registered).
     */
    void deleteFlag(String flagName);

    /**
     * List all flags, ordered by name (stable and comparable).
     */
    java.util.List<FlagInfo> listFlags();

    /** Flag state as returned by {@link #listFlags}. */
    final class FlagInfo {
        private final String flagName;
        private final boolean enabled;
        private final Integer percentage;

        public FlagInfo(String flagName, boolean enabled, Integer percentage) {
            this.flagName = flagName;
            this.enabled = enabled;
            this.percentage = percentage;
        }

        public String getFlagName() {
            return flagName;
        }

        public boolean isEnabled() {
            return enabled;
        }

        /** Rollout percentage {@code 0..100}; {@code null} = full on/off flag. */
        public Integer getPercentage() {
            return percentage;
        }

        @Override
        public String toString() {
            return "FlagInfo{flagName=" + flagName + ", enabled=" + enabled
                    + ", percentage=" + percentage + "}";
        }
    }

    /** Outcome of {@link #isEnabled}. */
    final class FlagDecision {
        private final boolean enabled;
        private final String variant;

        public FlagDecision(boolean enabled, String variant) {
            this.enabled = enabled;
            this.variant = variant;
        }

        public boolean isEnabled() {
            return enabled;
        }

        /** Selected variant, or {@code null} when the flag defines no variants. */
        public String getVariant() {
            return variant;
        }
    }
}
