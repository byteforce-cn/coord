package cn.byteforce.coord.sdk;

import cn.byteforce.coord.sdk.internal.rpc.ErrorMapper;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ErrorMapperTest {

    private final ErrorMapper errorMapper = new ErrorMapper();

    @Test
    void shouldMapTrailerErrorCode() {
        Metadata trailers = new Metadata();
        trailers.put(ErrorMapper.ERROR_CODE_KEY, "PROTOCOL_MISMATCH");
        StatusRuntimeException sre = new StatusRuntimeException(
                Status.INTERNAL.withDescription("some description"), trailers);

        CoordException ex = errorMapper.map(sre);

        assertThat(ex.getErrorCode()).isEqualTo(ErrorCode.PROTOCOL_MISMATCH);
    }

    @Test
    void unknownTrailerErrorCodeShouldMapToInternal() {
        Metadata trailers = new Metadata();
        trailers.put(ErrorMapper.ERROR_CODE_KEY, "UNKNOWN_CODE_XYZ");
        StatusRuntimeException sre = new StatusRuntimeException(
                Status.INTERNAL.withDescription("some description"), trailers);

        CoordException ex = errorMapper.map(sre);

        assertThat(ex.getErrorCode()).isEqualTo(ErrorCode.INTERNAL);
    }

    @Test
    void shouldMapGrpcNotFoundToRegistryServiceNotFound() {
        StatusRuntimeException sre = new StatusRuntimeException(Status.NOT_FOUND);

        CoordException ex = errorMapper.map(sre);

        assertThat(ex.getErrorCode()).isEqualTo(ErrorCode.REGISTRY_SERVICE_NOT_FOUND);
    }

    @Test
    void shouldMapGrpcAlreadyExistsToRegistryInstanceAlreadyExists() {
        StatusRuntimeException sre = new StatusRuntimeException(Status.ALREADY_EXISTS);

        CoordException ex = errorMapper.map(sre);

        assertThat(ex.getErrorCode()).isEqualTo(ErrorCode.REGISTRY_INSTANCE_ALREADY_EXISTS);
    }

    @Test
    void shouldMapGrpcUnavailableToAgentUnavailable() {
        StatusRuntimeException sre = new StatusRuntimeException(Status.UNAVAILABLE);

        CoordException ex = errorMapper.map(sre);

        assertThat(ex.getErrorCode()).isEqualTo(ErrorCode.AGENT_UNAVAILABLE);
    }

    @Test
    void shouldMapGrpcResourceExhausted() {
        StatusRuntimeException sre = new StatusRuntimeException(Status.RESOURCE_EXHAUSTED);

        CoordException ex = errorMapper.map(sre);

        assertThat(ex.getErrorCode()).isEqualTo(ErrorCode.RESOURCE_EXHAUSTED);
    }

    @Test
    void shouldMapOtherGrpcStatusToInternal() {
        StatusRuntimeException sre = new StatusRuntimeException(Status.ABORTED);

        CoordException ex = errorMapper.map(sre);

        assertThat(ex.getErrorCode()).isEqualTo(ErrorCode.INTERNAL);
    }

    @Test
    void trailerErrorCodeTakesPrecedenceOverGrpcStatus() {
        Metadata trailers = new Metadata();
        trailers.put(ErrorMapper.ERROR_CODE_KEY, "CONFIG_CAS_FAILED");
        StatusRuntimeException sre = new StatusRuntimeException(
                Status.NOT_FOUND.withDescription("some description"), trailers);

        CoordException ex = errorMapper.map(sre);

        // Trailer takes precedence regardless of gRPC status code
        assertThat(ex.getErrorCode()).isEqualTo(ErrorCode.CONFIG_CAS_FAILED);
    }

    @Test
    void shouldNeverParseDescriptionString() {
        // Even if the description contains an error code name, it must be ignored
        StatusRuntimeException sre = new StatusRuntimeException(
                Status.INTERNAL.withDescription("PROTOCOL_MISMATCH in description"));

        CoordException ex = errorMapper.map(sre);

        // Without trailers, description is NOT parsed
        assertThat(ex.getErrorCode()).isEqualTo(ErrorCode.INTERNAL);
    }
}
