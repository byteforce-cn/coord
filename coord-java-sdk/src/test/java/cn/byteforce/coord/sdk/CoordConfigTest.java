package cn.byteforce.coord.sdk;

import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.*;

class CoordConfigTest {

    @Test
    void shouldBuildWithRequiredFields() {
        CoordConfig config = CoordConfig.builder()
                .agentHost("localhost")
                .agentPort(19527)
                .build();

        assertThat(config.getAgentHost()).isEqualTo("localhost");
        assertThat(config.getAgentPort()).isEqualTo(19527);
    }

    @Test
    void shouldRequireAgentHost() {
        assertThatThrownBy(() -> CoordConfig.builder().build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("agentHost");
    }

    @Test
    void shouldApplyDefaults() {
        CoordConfig config = CoordConfig.builder()
                .agentHost("10.0.0.1")
                .agentPort(19527)
                .build();

        assertThat(config.getRequestTimeout()).isEqualTo(Duration.ofSeconds(5));
        assertThat(config.isAutoRestoreWatches()).isTrue();
        assertThat(config.getHeartbeatThreads()).isEqualTo(4);
        assertThat(config.isUseTls()).isFalse();
        assertThat(config.getTlsCaCertPath()).isNull();
        assertThat(config.getTlsClientCertPath()).isNull();
        assertThat(config.getTlsClientKeyPath()).isNull();
        assertThat(config.getObservabilityProvider()).isNotNull();
    }

    @Test
    void shouldAllowCustomRequestTimeout() {
        CoordConfig config = CoordConfig.builder()
                .agentHost("localhost")
                .agentPort(19527)
                .requestTimeout(Duration.ofSeconds(10))
                .build();

        assertThat(config.getRequestTimeout()).isEqualTo(Duration.ofSeconds(10));
    }

    @Test
    void shouldAllowDisablingAutoRestoreWatches() {
        CoordConfig config = CoordConfig.builder()
                .agentHost("localhost")
                .agentPort(19527)
                .autoRestoreWatches(false)
                .build();

        assertThat(config.isAutoRestoreWatches()).isFalse();
    }

    @Test
    void shouldAllowCustomHeartbeatThreads() {
        CoordConfig config = CoordConfig.builder()
                .agentHost("localhost")
                .agentPort(19527)
                .heartbeatThreads(8)
                .build();

        assertThat(config.getHeartbeatThreads()).isEqualTo(8);
    }

    @Test
    void tlsShouldThrowUnsupportedOperationWhenSetTrue() {
        assertThatThrownBy(() -> CoordConfig.builder()
                .agentHost("localhost")
                .agentPort(19527)
                .useTls(true)
                .build())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("TLS");
    }

    @Test
    void shouldAcceptCustomObservabilityProvider() {
        ObservabilityProvider customProvider = new ObservabilityProvider() {};
        CoordConfig config = CoordConfig.builder()
                .agentHost("localhost")
                .agentPort(19527)
                .observabilityProvider(customProvider)
                .build();

        assertThat(config.getObservabilityProvider()).isSameAs(customProvider);
    }

    @Test
    void shouldBeImmutableAfterBuild() {
        CoordConfig config = CoordConfig.builder()
                .agentHost("localhost")
                .agentPort(19527)
                .build();

        // No setters exposed — verify through reflection that the class has no setter methods
        assertThat(config.getClass().getMethods())
                .filteredOn(m -> m.getName().startsWith("set"))
                .isEmpty();
    }

    @Test
    void shouldRejectNegativePort() {
        assertThatThrownBy(() -> CoordConfig.builder()
                .agentHost("localhost")
                .agentPort(-1)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("agentPort");
    }

    @Test
    void shouldRejectZeroPort() {
        assertThatThrownBy(() -> CoordConfig.builder()
                .agentHost("localhost")
                .agentPort(0)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("agentPort");
    }

    @Test
    void shouldRejectBlankAgentHost() {
        assertThatThrownBy(() -> CoordConfig.builder()
                .agentHost("  ")
                .agentPort(19527)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("agentHost");
    }

    @Test
    void shouldRejectNullRequestTimeout() {
        assertThatThrownBy(() -> CoordConfig.builder()
                .agentHost("localhost")
                .agentPort(19527)
                .requestTimeout(null)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requestTimeout");
    }

    @Test
    void shouldRejectNonPositiveHeartbeatThreads() {
        assertThatThrownBy(() -> CoordConfig.builder()
                .agentHost("localhost")
                .agentPort(19527)
                .heartbeatThreads(0)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("heartbeatThreads");
    }
}
