package cn.byteforce.coord.sdk;

import cn.byteforce.coord.sdk.health.HealthStatus;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;

import static org.assertj.core.api.Assertions.assertThat;

class CoordClientTest {

    /**
     * Allocates a currently-free port so the test never collides with a locally
     * running Coord agent (which occupies the default port 19527).
     */
    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("Unable to allocate a free port for test", e);
        }
    }

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
        assertThat(client.cache()).isNotNull();
        assertThat(client.mq()).isNotNull();

        // Close should not throw
        client.close();
    }

    @Test
    void shouldReturnNotServingWhenNoAgent() {
        CoordConfig config = CoordConfig.builder()
                .agentHost("localhost")
                .agentPort(freePort())
                .build();

        try (CoordClient client = CoordClient.create(config)) {
            HealthStatus status = client.healthCheck();
            // No agent running on this port, so should be NOT_SERVING
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
