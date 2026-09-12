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
        // D1 残余项：非 loopback + 明文默认被拒 → 测试显式声明接受明文（本地/可信网段）
        CoordConfig config = CoordConfig.builder()
                .agentHost("10.0.0.1")
                .agentPort(19527)
                .allowInsecurePlaintext(true)
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
    void tlsRequiresCaCertPath() {
        // D1：配了 TLS 但未提供 CA 证书路径 → 拒绝构建（fail-closed，不静默降级明文）
        assertThatThrownBy(() -> CoordConfig.builder()
                .agentHost("localhost")
                .agentPort(19527)
                .useTls(true)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tlsCaCertPath");
    }

    @Test
    void tlsAcceptsCaCertPath() {
        CoordConfig config = CoordConfig.builder()
                .agentHost("localhost")
                .agentPort(19527)
                .useTls(true)
                .tlsCaCertPath("/etc/coord/ca.pem")
                .build();

        assertThat(config.isUseTls()).isTrue();
        assertThat(config.getTlsCaCertPath()).isEqualTo("/etc/coord/ca.pem");
    }

    @Test
    void tlsRequiresClientCertAndKeyTogether() {
        assertThatThrownBy(() -> CoordConfig.builder()
                .agentHost("localhost")
                .agentPort(19527)
                .useTls(true)
                .tlsCaCertPath("/etc/coord/ca.pem")
                .tlsClientCertPath("/etc/coord/client.pem")
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tlsClientKeyPath");
    }

    @Test
    void authTokenSupplierIsExposedAndReadAtCallTime() {
        // D2：CCT 注入来源可配置；supplier 每次取值 → 刷新后无需重建 channel
        java.util.concurrent.atomic.AtomicReference<String> token =
                new java.util.concurrent.atomic.AtomicReference<>("cct-1");
        CoordConfig config = CoordConfig.builder()
                .agentHost("localhost")
                .agentPort(19527)
                .authTokenSupplier(token::get)
                .build();

        assertThat(config.getAuthTokenSupplier()).isNotNull();
        assertThat(config.getAuthTokenSupplier().get()).isEqualTo("cct-1");
        token.set("cct-2");
        assertThat(config.getAuthTokenSupplier().get()).isEqualTo("cct-2");
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

    // ──── D1（残余项）：默认不再明文 ────

    @Test
    void shouldAllowPlaintextToLoopbackByDefault() {
        // 本机开发场景不受影响（localhost / 127.x / ::1）
        CoordConfig.builder().agentHost("localhost").build();
        CoordConfig.builder().agentHost("127.0.0.1").build();
        CoordConfig.builder().agentHost("127.8.8.8").build();
        CoordConfig.builder().agentHost("::1").build();
        CoordConfig.builder().agentHost("[::1]").build();
    }

    @Test
    void shouldRejectPlaintextToNonLoopbackHostByDefault() {
        assertThatThrownBy(() -> CoordConfig.builder()
                .agentHost("10.0.0.1")
                .agentPort(19527)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("plaintext channel to non-loopback host")
                .hasMessageContaining("fail-closed");
    }

    @Test
    void shouldAllowNonLoopbackPlaintextWhenExplicitlyOptedIn() {
        CoordConfig config = CoordConfig.builder()
                .agentHost("10.0.0.1")
                .allowInsecurePlaintext(true)
                .build();
        assertThat(config.isUseTls()).isFalse();
        assertThat(config.getAgentHost()).isEqualTo("10.0.0.1");
    }

    @Test
    void shouldAllowNonLoopbackWithTls() {
        CoordConfig config = CoordConfig.builder()
                .agentHost("10.0.0.1")
                .useTls(true)
                .tlsCaCertPath("/tmp/ca.pem")
                .build();
        assertThat(config.isUseTls()).isTrue();
    }
}
