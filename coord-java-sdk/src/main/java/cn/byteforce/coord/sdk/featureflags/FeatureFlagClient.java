package cn.byteforce.coord.sdk.featureflags;

import cn.byteforce.coord.sdk.CoordException;

/**
 * Feature flag API.
 * <p>
 * Flags are evaluated by the Coord Agent. The wire surface is <b>read-only</b>
 * ({@code IsEnabled} / {@code Evaluate}) — flags are created and updated out of band
 * (configuration / admin plane), so the SDK deliberately performs no local caching:
 * a cache without invalidation would go stale silently.
 *
 * <pre>{@code
 * FeatureFlagClient ff = client.featureFlags();
 * if (ff.isEnabled("new-checkout", "{\"userId\":\"u-1\"}")) {
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
