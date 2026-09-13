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
            // 第四轮 §3.14.2：此前 NOT_FOUND 一律映射为 REGISTRY_SERVICE_NOT_FOUND
            // （KV 查不到 key、证书不存在、租约不存在全被报成"注册中心服务不存在"），
            // UNAVAILABLE（含 not-leader）一律映射为 AGENT_UNAVAILABLE。
            // 现在服务端会附上精确的 trailer（上面的首选分支），本表只是**兜底**：
            // 它不再猜测业务语义，只做传输层到结构化码的忠实映射。
            case NOT_FOUND -> ErrorCode.NOT_FOUND;
            case ALREADY_EXISTS -> ErrorCode.ALREADY_EXISTS;
            case INVALID_ARGUMENT -> ErrorCode.INVALID_ARGUMENT;
            case UNAUTHENTICATED -> ErrorCode.UNAUTHENTICATED;
            case PERMISSION_DENIED -> ErrorCode.PERMISSION_DENIED;
            case FAILED_PRECONDITION -> ErrorCode.FAILED_PRECONDITION;
            case OUT_OF_RANGE -> ErrorCode.OUT_OF_RANGE;
            // 对齐 Rust 客户端重试矩阵（unavailable/deadline/timeout → 重试）。
            // 注意：not-leader 与 agent 不可达 **都**落到这里，只有在缺 trailer 的
            // 情况下才会发生；正常情况下二者分别带 NOT_LEADER / UNAVAILABLE。
            case UNAVAILABLE -> ErrorCode.UNAVAILABLE;
            case RESOURCE_EXHAUSTED -> ErrorCode.RESOURCE_EXHAUSTED;
            case DEADLINE_EXCEEDED -> ErrorCode.DEADLINE_EXCEEDED;
            default -> ErrorCode.INTERNAL;
        };
    }
}
