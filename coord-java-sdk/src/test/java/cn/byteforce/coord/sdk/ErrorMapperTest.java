package cn.byteforce.coord.sdk;

import cn.byteforce.coord.sdk.internal.rpc.ErrorMapper;
import cn.byteforce.coord.sdk.internal.rpc.RetryTemplate;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 第四轮 §3.14.2 回归：`ErrorMapper` 的**首选分支**（读 trailer
 * `x-coord-error-code`）此前在真实流量上是**死代码**——Rust 侧全仓 0 处写入该
 * trailer，因此所有错误都落到状态码兜底表，而那张表把 `NOT_FOUND` 一律报成
 * "注册中心服务不存在"（KV 查不到 key、证书不存在、租约不存在全中）、把
 * `UNAVAILABLE`（含 **not-leader**）一律报成"agent 挂了"——而 SDK 的重试矩阵
 * 正是按这些码决策的。
 *
 * <p>本轮起服务端/agent 真的附着 trailer（`coord-core::error_code`），故：
 * <ul>
 *   <li>兜底表不再猜测业务语义，只做传输层 → 结构化码的忠实映射
 *       （`NOT_FOUND` → {@code NOT_FOUND}，`UNAVAILABLE` → {@code UNAVAILABLE}）；</li>
 *   <li>{@code NOT_LEADER} 成为独立的码，可重试且语义为"换 leader 重试"。</li>
 * </ul>
 */
class ErrorMapperTest {

    private final ErrorMapper errorMapper = new ErrorMapper();

    private static StatusRuntimeException withTrailer(Status status, String code) {
        Metadata trailers = new Metadata();
        if (code != null) {
            trailers.put(ErrorMapper.ERROR_CODE_KEY, code);
        }
        return status.asRuntimeException(trailers);
    }

    @Test
    void shouldMapTrailerErrorCode() {
        StatusRuntimeException sre = withTrailer(Status.INTERNAL, "PROTOCOL_MISMATCH");

        CoordException ex = errorMapper.map(sre);

        assertThat(ex.getErrorCode()).isEqualTo(ErrorCode.PROTOCOL_MISMATCH);
    }

    @Test
    void unknownTrailerErrorCodeShouldMapToInternal() {
        StatusRuntimeException sre = withTrailer(Status.INTERNAL, "UNKNOWN_CODE_XYZ");

        CoordException ex = errorMapper.map(sre);

        assertThat(ex.getErrorCode()).isEqualTo(ErrorCode.INTERNAL);
    }

    /**
     * 兜底表不再把"任何 NOT_FOUND"说成"注册中心服务不存在"。
     *
     * <p>修复前该断言是 `REGISTRY_SERVICE_NOT_FOUND`——即 KV miss、证书不存在、
     * `LeaseRevoke` 不存在的租约全部被误分类。
     */
    @Test
    void shouldMapGrpcNotFoundToGenericNotFound() {
        StatusRuntimeException sre = withTrailer(Status.NOT_FOUND, null);

        CoordException ex = errorMapper.map(sre);

        assertThat(ex.getErrorCode()).isEqualTo(ErrorCode.NOT_FOUND);
        assertThat(ex.getErrorCode()).isNotEqualTo(ErrorCode.REGISTRY_SERVICE_NOT_FOUND);
    }

    @Test
    void shouldMapGrpcAlreadyExistsToGenericAlreadyExists() {
        StatusRuntimeException sre = withTrailer(Status.ALREADY_EXISTS, null);

        CoordException ex = errorMapper.map(sre);

        assertThat(ex.getErrorCode()).isEqualTo(ErrorCode.ALREADY_EXISTS);
    }

    /**
     * 兜底表不再把"任何 UNAVAILABLE"说成"agent 挂了"——否则 not-leader 会被当成
     * agent 故障（应当等待）而不是 leader 切换（应当重试）。
     */
    @Test
    void shouldMapGrpcUnavailableToGenericUnavailable() {
        StatusRuntimeException sre = withTrailer(Status.UNAVAILABLE, null);

        CoordException ex = errorMapper.map(sre);

        assertThat(ex.getErrorCode()).isEqualTo(ErrorCode.UNAVAILABLE);
        assertThat(ex.getErrorCode()).isNotEqualTo(ErrorCode.AGENT_UNAVAILABLE);
    }

    @Test
    void shouldMapGrpcResourceExhausted() {
        StatusRuntimeException sre = withTrailer(Status.RESOURCE_EXHAUSTED, null);

        CoordException ex = errorMapper.map(sre);

        assertThat(ex.getErrorCode()).isEqualTo(ErrorCode.RESOURCE_EXHAUSTED);
    }

    @Test
    void shouldMapOtherGrpcStatusToInternal() {
        StatusRuntimeException sre = withTrailer(Status.ABORTED, null);

        CoordException ex = errorMapper.map(sre);

        assertThat(ex.getErrorCode()).isEqualTo(ErrorCode.INTERNAL);
    }

    @Test
    void trailerErrorCodeTakesPrecedenceOverGrpcStatus() {
        StatusRuntimeException sre = withTrailer(Status.NOT_FOUND, "CONFIG_CAS_FAILED");

        CoordException ex = errorMapper.map(sre);

        // Trailer takes precedence regardless of gRPC status code
        assertThat(ex.getErrorCode()).isEqualTo(ErrorCode.CONFIG_CAS_FAILED);
    }

    /**
     * 首选分支必须压过兜底表，且两者给**不同**的码——
     * 这一条直接证明首选分支不是死代码（修复前用 `NOT_FOUND` 状态码永远拿不到
     * `NOT_LEADER`，因为 Rust 从不发 trailer）。
     */
    @Test
    void notLeaderTrailerWinsOverUnavailableStatus() {
        StatusRuntimeException sre = withTrailer(Status.UNAVAILABLE, "NOT_LEADER");

        CoordException ex = errorMapper.map(sre);

        assertThat(ex.getErrorCode()).isEqualTo(ErrorCode.NOT_LEADER);
        assertThat(ex.getErrorCode()).isNotEqualTo(ErrorCode.UNAVAILABLE);
    }

    /** 本轮新增的四个码都必须能经 trailer 解出来（否则仍会被误报为不可重试）。 */
    @Test
    void newlyEmittedCodesAreRecognised() {
        assertThat(errorMapper.map(withTrailer(Status.UNAVAILABLE, "NOT_LEADER")).getErrorCode())
                .isEqualTo(ErrorCode.NOT_LEADER);
        assertThat(errorMapper.map(withTrailer(Status.INTERNAL, "UNAUTHENTICATED")).getErrorCode())
                .isEqualTo(ErrorCode.UNAUTHENTICATED);
        assertThat(errorMapper.map(withTrailer(Status.INTERNAL, "PERMISSION_DENIED")).getErrorCode())
                .isEqualTo(ErrorCode.PERMISSION_DENIED);
        assertThat(errorMapper.map(withTrailer(Status.INTERNAL, "TXN_CAS_FAILED")).getErrorCode())
                .isEqualTo(ErrorCode.TXN_CAS_FAILED);
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

    /**
     * 重试矩阵：`NOT_LEADER` 必须可重试（换 leader 后重试），
     * `UNAUTHENTICATED` 必须**不可**重试（重试只会重复失败）。
     */
    @Test
    void retryMatrixDistinguishesNotLeaderFromAuthFailures() {
        RetryTemplate template = new RetryTemplate();

        assertThat(attemptsUntilFailure(
                        template, errorMapper.map(withTrailer(Status.UNAVAILABLE, "NOT_LEADER"))))
                .as("not-leader 必须重试")
                .isGreaterThan(1);

        assertThat(attemptsUntilFailure(
                        template, errorMapper.map(withTrailer(Status.INTERNAL, "UNAUTHENTICATED"))))
                .as("UNAUTHENTICATED 不得重试")
                .isEqualTo(1);
    }

    /** 跑 RetryTemplate 直到放弃，返回总尝试次数。 */
    private static int attemptsUntilFailure(RetryTemplate template, CoordException toThrow) {
        int[] calls = {0};
        try {
            template.execute(ctx -> {
                calls[0]++;
                throw toThrow;
            });
        } catch (CoordException expected) {
            // 重试耗尽（或首次即抛）后到这里
        }
        return calls[0];
    }
}
