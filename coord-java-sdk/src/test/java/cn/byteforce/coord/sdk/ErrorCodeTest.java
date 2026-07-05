package cn.byteforce.coord.sdk;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ErrorCodeTest {

    @Test
    void shouldHaveAllRequiredErrorCodes() {
        assertThat(ErrorCode.values()).containsExactlyInAnyOrder(
                ErrorCode.PROTOCOL_MISMATCH,
                ErrorCode.AGENT_UNAVAILABLE,
                ErrorCode.REGISTRY_SERVICE_NOT_FOUND,
                ErrorCode.REGISTRY_INSTANCE_ALREADY_EXISTS,
                ErrorCode.REGISTRY_LEASE_EXPIRED,
                ErrorCode.CONFIG_KEY_NOT_FOUND,
                ErrorCode.CONFIG_CAS_FAILED,
                ErrorCode.WATCH_STREAM_ERROR,
                ErrorCode.RESOURCE_EXHAUSTED,
                ErrorCode.INTERNAL
        );
    }

    @Test
    void protoNameShouldMatchTrailerHeaderValues() {
        assertThat(ErrorCode.PROTOCOL_MISMATCH.getProtoName()).isEqualTo("PROTOCOL_MISMATCH");
        assertThat(ErrorCode.AGENT_UNAVAILABLE.getProtoName()).isEqualTo("AGENT_UNAVAILABLE");
        assertThat(ErrorCode.REGISTRY_SERVICE_NOT_FOUND.getProtoName()).isEqualTo("REGISTRY_SERVICE_NOT_FOUND");
        assertThat(ErrorCode.REGISTRY_INSTANCE_ALREADY_EXISTS.getProtoName()).isEqualTo("REGISTRY_INSTANCE_ALREADY_EXISTS");
        assertThat(ErrorCode.REGISTRY_LEASE_EXPIRED.getProtoName()).isEqualTo("REGISTRY_LEASE_EXPIRED");
        assertThat(ErrorCode.CONFIG_KEY_NOT_FOUND.getProtoName()).isEqualTo("CONFIG_KEY_NOT_FOUND");
        assertThat(ErrorCode.CONFIG_CAS_FAILED.getProtoName()).isEqualTo("CONFIG_CAS_FAILED");
        assertThat(ErrorCode.WATCH_STREAM_ERROR.getProtoName()).isEqualTo("WATCH_STREAM_ERROR");
        assertThat(ErrorCode.RESOURCE_EXHAUSTED.getProtoName()).isEqualTo("RESOURCE_EXHAUSTED");
        assertThat(ErrorCode.INTERNAL.getProtoName()).isEqualTo("INTERNAL");
    }
}
