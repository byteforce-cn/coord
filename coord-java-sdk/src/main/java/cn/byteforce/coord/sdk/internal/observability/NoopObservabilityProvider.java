package cn.byteforce.coord.sdk.internal.observability;

import cn.byteforce.coord.sdk.spi.ObservabilityProvider;

/**
 * No-op implementation of {@link ObservabilityProvider}.
 * Used as the default when no custom provider is configured.
 */
public final class NoopObservabilityProvider implements ObservabilityProvider {
    // All methods use default no-op implementations from the interface.
}
