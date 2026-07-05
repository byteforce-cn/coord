package cn.byteforce.coord.sdk;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CoordExceptionTest {

    @Test
    void shouldCreateExceptionWithErrorCode() {
        CoordException ex = new CoordException(ErrorCode.AGENT_UNAVAILABLE);

        assertThat(ex.getErrorCode()).isEqualTo(ErrorCode.AGENT_UNAVAILABLE);
        assertThat(ex.getMessage()).contains("AGENT_UNAVAILABLE");
    }

    @Test
    void shouldCreateExceptionWithErrorCodeAndMessage() {
        CoordException ex = new CoordException(ErrorCode.PROTOCOL_MISMATCH, "Agent does not support v1");

        assertThat(ex.getErrorCode()).isEqualTo(ErrorCode.PROTOCOL_MISMATCH);
        assertThat(ex.getMessage()).isEqualTo("Agent does not support v1");
    }

    @Test
    void shouldCreateExceptionWithErrorCodeAndCause() {
        RuntimeException cause = new RuntimeException("connection refused");
        CoordException ex = new CoordException(ErrorCode.AGENT_UNAVAILABLE, cause);

        assertThat(ex.getErrorCode()).isEqualTo(ErrorCode.AGENT_UNAVAILABLE);
        assertThat(ex.getCause()).isSameAs(cause);
    }

    @Test
    void shouldCreateExceptionWithErrorCodeMessageAndCause() {
        RuntimeException cause = new RuntimeException("timeout");
        CoordException ex = new CoordException(ErrorCode.INTERNAL, "Unexpected error", cause);

        assertThat(ex.getErrorCode()).isEqualTo(ErrorCode.INTERNAL);
        assertThat(ex.getMessage()).isEqualTo("Unexpected error");
        assertThat(ex.getCause()).isSameAs(cause);
    }

    @ParameterizedTest
    @MethodSource("errorCodeNames")
    void everyErrorCodeShouldBeCreatableInException(ErrorCode code) {
        CoordException ex = new CoordException(code, "test");
        assertThat(ex.getErrorCode()).isEqualTo(code);
    }

    static Stream<Arguments> errorCodeNames() {
        return Stream.of(ErrorCode.values()).map(Arguments::of);
    }

    @Test
    void shouldBeRuntimeException() {
        CoordException ex = new CoordException(ErrorCode.INTERNAL);
        assertThat(ex).isInstanceOf(RuntimeException.class);
    }

    @Test
    void errorCodeFromProtoNameShouldMatchKnownNames() {
        assertThat(ErrorCode.fromProtoName("PROTOCOL_MISMATCH")).isEqualTo(ErrorCode.PROTOCOL_MISMATCH);
        assertThat(ErrorCode.fromProtoName("AGENT_UNAVAILABLE")).isEqualTo(ErrorCode.AGENT_UNAVAILABLE);
        assertThat(ErrorCode.fromProtoName("REGISTRY_SERVICE_NOT_FOUND")).isEqualTo(ErrorCode.REGISTRY_SERVICE_NOT_FOUND);
        assertThat(ErrorCode.fromProtoName("REGISTRY_INSTANCE_ALREADY_EXISTS")).isEqualTo(ErrorCode.REGISTRY_INSTANCE_ALREADY_EXISTS);
        assertThat(ErrorCode.fromProtoName("REGISTRY_LEASE_EXPIRED")).isEqualTo(ErrorCode.REGISTRY_LEASE_EXPIRED);
        assertThat(ErrorCode.fromProtoName("CONFIG_KEY_NOT_FOUND")).isEqualTo(ErrorCode.CONFIG_KEY_NOT_FOUND);
        assertThat(ErrorCode.fromProtoName("CONFIG_CAS_FAILED")).isEqualTo(ErrorCode.CONFIG_CAS_FAILED);
        assertThat(ErrorCode.fromProtoName("WATCH_STREAM_ERROR")).isEqualTo(ErrorCode.WATCH_STREAM_ERROR);
        assertThat(ErrorCode.fromProtoName("RESOURCE_EXHAUSTED")).isEqualTo(ErrorCode.RESOURCE_EXHAUSTED);
        assertThat(ErrorCode.fromProtoName("INTERNAL")).isEqualTo(ErrorCode.INTERNAL);
    }

    @Test
    void errorCodeFromUnknownProtoNameShouldReturnInternal() {
        assertThat(ErrorCode.fromProtoName("NONEXISTENT_CODE")).isEqualTo(ErrorCode.INTERNAL);
    }

    @Test
    void errorCodeFromNullProtoNameShouldReturnInternal() {
        assertThat(ErrorCode.fromProtoName(null)).isEqualTo(ErrorCode.INTERNAL);
    }
}
