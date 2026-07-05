package cn.byteforce.coord.sdk;

import cn.byteforce.coord.sdk.internal.channel.ProtocolNegotiator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ProtocolNegotiatorTest {

    private static final String SDK_VERSION = "coord-agent-api-v1";

    @Test
    void shouldAcceptWhenVersionIsSupported() {
        ProtocolNegotiator negotiator = new ProtocolNegotiator(SDK_VERSION);
        assertThat(negotiator.isVersionSupported("coord-agent-api-v1")).isTrue();
    }

    @Test
    void shouldRejectWhenVersionIsNotSupported() {
        ProtocolNegotiator negotiator = new ProtocolNegotiator(SDK_VERSION);
        assertThat(negotiator.isVersionSupported("coord-agent-api-v2")).isFalse();
    }

    @Test
    void shouldRejectNullVersion() {
        ProtocolNegotiator negotiator = new ProtocolNegotiator(SDK_VERSION);
        assertThat(negotiator.isVersionSupported(null)).isFalse();
    }

    @Test
    void shouldReturnSdkVersion() {
        ProtocolNegotiator negotiator = new ProtocolNegotiator(SDK_VERSION);
        assertThat(negotiator.getSdkVersion()).isEqualTo(SDK_VERSION);
    }
}
