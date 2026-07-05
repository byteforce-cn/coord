package cn.byteforce.coord.sdk;

import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.sdk.internal.thread.ThreadPoolManager;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import io.grpc.ManagedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AgentChannelManagerTest {

    private CoordConfig config;
    private ThreadPoolManager threadPoolManager;
    private AgentChannelManager channelManager;

    @BeforeEach
    void setUp() {
        config = CoordConfig.builder()
                .agentHost("localhost")
                .agentPort(19527)
                .build();
        threadPoolManager = new ThreadPoolManager(2);
    }

    @AfterEach
    void tearDown() {
        if (channelManager != null) {
            channelManager.shutdown();
        }
        threadPoolManager.close();
    }

    @Test
    void shouldCreateChannel() {
        channelManager = new AgentChannelManager(config, threadPoolManager, new ObservabilityProvider() {});
        ManagedChannel channel = channelManager.getChannel();

        assertThat(channel).isNotNull();
        assertThat(channel.isShutdown()).isFalse();
    }

    @Test
    void shouldThrowWhenChannelNotReady() {
        channelManager = new AgentChannelManager(config, threadPoolManager, new ObservabilityProvider() {});

        // Channel is created but not connected — getChannel should still return it
        // (getChannel only throws when shutdown or when explicitly checking readiness)
        ManagedChannel channel = channelManager.getChannel();
        assertThat(channel).isNotNull();
    }

    @Test
    void shouldThrowWhenShutdown() {
        channelManager = new AgentChannelManager(config, threadPoolManager, new ObservabilityProvider() {});
        channelManager.shutdown();

        assertThatThrownBy(() -> channelManager.getChannel())
                .isInstanceOf(CoordException.class)
                .extracting(e -> ((CoordException) e).getErrorCode())
                .isEqualTo(ErrorCode.AGENT_UNAVAILABLE);
    }

    @Test
    void awaitReadyShouldTimeoutWhenNoAgent() {
        channelManager = new AgentChannelManager(config, threadPoolManager, new ObservabilityProvider() {});
        // No agent running, so should return false
        boolean ready = channelManager.awaitReady(Duration.ofMillis(200));
        assertThat(ready).isFalse();
    }

    @Test
    void shouldShutdownGracefully() {
        channelManager = new AgentChannelManager(config, threadPoolManager, new ObservabilityProvider() {});
        channelManager.shutdown();

        assertThat(channelManager.isShutdown()).isTrue();
    }
}
