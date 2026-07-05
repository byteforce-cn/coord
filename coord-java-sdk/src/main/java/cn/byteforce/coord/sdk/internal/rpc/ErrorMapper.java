package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.CoordException;
import cn.byteforce.coord.sdk.ErrorCode;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Maps gRPC {@link StatusRuntimeException} to structured {@link CoordException}.
 * <p>
 * <b>Mandatory rule:</b> Error codes are extracted ONLY from the gRPC trailers header
 * {@code x-coord-error-code} or from the gRPC {@link Status.Code}. The error description
 * string MUST NEVER be parsed or matched.
 */
public final class ErrorMapper {

    /** The gRPC trailers metadata key for the Coord error code. */
    public static final Metadata.Key<String> ERROR_CODE_KEY =
            Metadata.Key.of("x-coord-error-code", Metadata.ASCII_STRING_MARSHALLER);

    /**
     * Map a gRPC exception to a {@link CoordException}.
     */
    public CoordException map(StatusRuntimeException sre) {
        // 1. Check trailers for explicit error code (takes highest precedence)
        Metadata trailers = sre.getTrailers();
        if (trailers != null) {
            String trailerCode = trailers.get(ERROR_CODE_KEY);
            if (trailerCode != null) {
                ErrorCode code = ErrorCode.fromProtoName(trailerCode);
                return new CoordException(code, sre.getMessage(), sre);
            }
        }

        // 2. Map gRPC status code to error code
        ErrorCode code = mapGrpcStatus(sre.getStatus());
        return new CoordException(code, sre.getMessage(), sre);
    }

    private ErrorCode mapGrpcStatus(Status status) {
        return switch (status.getCode()) {
            case NOT_FOUND -> ErrorCode.REGISTRY_SERVICE_NOT_FOUND;
            case ALREADY_EXISTS -> ErrorCode.REGISTRY_INSTANCE_ALREADY_EXISTS;
            case UNAVAILABLE -> ErrorCode.AGENT_UNAVAILABLE;
            case RESOURCE_EXHAUSTED -> ErrorCode.RESOURCE_EXHAUSTED;
            default -> ErrorCode.INTERNAL;
        };
    }
}
