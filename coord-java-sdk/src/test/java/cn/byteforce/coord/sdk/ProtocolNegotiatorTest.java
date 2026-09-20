package cn.byteforce.coord.sdk;

import cn.byteforce.coord.sdk.internal.channel.ProtocolNegotiator;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtocolNegotiatorTest {

    private static final String SDK_VERSION = ProtocolNegotiator.SDK_PROTOCOL_VERSION;

    @Test
    void shouldAcceptWhenVersionIsSupported() {
        ProtocolNegotiator negotiator = new ProtocolNegotiator(SDK_VERSION);
        assertThat(negotiator.isVersionSupported("coord-agent-api-v2")).isTrue();
    }

    @Test
    void shouldRejectWhenVersionIsNotSupported() {
        ProtocolNegotiator negotiator = new ProtocolNegotiator(SDK_VERSION);
        assertThat(negotiator.isVersionSupported("coord-agent-api-v1")).isFalse();
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

    // ──── P0-4 / D6：协商结果判定（此前这些分支在主代码里零调用方）────

    @Test
    void shouldAcceptWhenAgentAdvertisesSdkVersion() {
        ProtocolNegotiator negotiator = new ProtocolNegotiator(SDK_VERSION);
        assertThat(negotiator.isSupportedBy(List.of("coord-agent-api-v2", "coord-agent-api-v3")))
                .isTrue();
    }

    @Test
    void shouldRejectWhenAgentDoesNotAdvertiseSdkVersion() {
        ProtocolNegotiator negotiator = new ProtocolNegotiator(SDK_VERSION);
        assertThat(negotiator.isSupportedBy(List.of("coord-agent-api-v1"))).isFalse();
        assertThat(negotiator.isSupportedBy(List.of())).isFalse();
        assertThat(negotiator.isSupportedBy(null)).isFalse();
    }

    /** 不匹配必须抛出**可诊断**错误：带版本号、带 agent 广告的列表、带处置建议。 */
    @Test
    void requireSupportedShouldThrowDiagnosableProtocolMismatch() {
        ProtocolNegotiator negotiator = new ProtocolNegotiator(SDK_VERSION);
        assertThatThrownBy(() -> negotiator.requireSupported(List.of("coord-agent-api-v1")))
                .isInstanceOf(CoordException.class)
                .hasMessageContaining(SDK_VERSION)
                .hasMessageContaining("coord-agent-api-v1")
                .hasMessageContaining("UNIMPLEMENTED");
    }

    /** agent 未实现 Negotiate（返回空列表）时同样必须可诊断。 */
    @Test
    void requireSupportedShouldExplainMissingHandshake() {
        ProtocolNegotiator negotiator = new ProtocolNegotiator(SDK_VERSION);
        assertThatThrownBy(() -> negotiator.requireSupported(List.of()))
                .isInstanceOf(CoordException.class)
                .hasMessageContaining("did not implement Handshake.Negotiate");
    }

    @Test
    void requireSupportedShouldPassSilentlyWhenSupported() {
        ProtocolNegotiator negotiator = new ProtocolNegotiator(SDK_VERSION);
        negotiator.requireSupported(List.of("coord-agent-api-v2"));
    }
}
