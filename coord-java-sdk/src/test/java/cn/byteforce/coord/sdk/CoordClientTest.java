package cn.byteforce.coord.sdk;

import cn.byteforce.coord.sdk.health.HealthStatus;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CoordClientTest {

    @Test
    void shouldCreateAndCloseGracefully() {
        CoordConfig config = CoordConfig.builder()
                .agentHost("localhost")
                .agentPort(19527)
                .build();

        CoordClient client = CoordClient.create(config);
        assertThat(client).isNotNull();
        assertThat(client.registry()).isNotNull();
        assertThat(client.configClient()).isNotNull();

        // Close should not throw
        client.close();
    }

    @Test
    void shouldReturnNotServingWhenNoAgent() {
        CoordConfig config = CoordConfig.builder()
                .agentHost("localhost")
                .agentPort(19527)
                .build();

        try (CoordClient client = CoordClient.create(config)) {
            HealthStatus status = client.healthCheck();
            // No agent running, so should be NOT_SERVING
            assertThat(status).isEqualTo(HealthStatus.NOT_SERVING);
        }
    }

    @Test
    void shouldCloseWithCustomGracePeriod() {
        CoordConfig config = CoordConfig.builder()
                .agentHost("localhost")
                .agentPort(19527)
                .build();

        CoordClient client = CoordClient.create(config);
        client.close(java.time.Duration.ofSeconds(5));
        // No exception = success
    }
}
