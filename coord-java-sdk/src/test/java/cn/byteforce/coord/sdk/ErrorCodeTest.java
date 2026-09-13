package cn.byteforce.coord.sdk;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class ErrorCodeTest {

    /**
     * 服务端/agent 实际会发出的 wire 码（第四轮 §3.14.2）。
     *
     * <p>这份清单必须与 `coord-core/src/error_code.rs::CoordErrorCode::ALL` 逐字一致；
     * 跨语言一致性由 `scripts/check-error-code-contract.sh`（CI 门禁）强制。
     * 本测试是 Java 侧的独立卡口：即使脚本没被跑，删掉某个 wire 码也会红。
     */
    private static final Set<String> WIRE_CODES = Set.of(
            "UNAUTHENTICATED",
            "PERMISSION_DENIED",
            "NOT_FOUND",
            "ALREADY_EXISTS",
            "INVALID_ARGUMENT",
            "FAILED_PRECONDITION",
            "OUT_OF_RANGE",
            "RESOURCE_EXHAUSTED",
            "DEADLINE_EXCEEDED",
            "UNAVAILABLE",
            "NOT_LEADER",
            "TXN_CAS_FAILED",
            "INTERNAL");

    @Test
    void shouldHaveAllRequiredErrorCodes() {
        assertThat(ErrorCode.values()).containsExactlyInAnyOrder(
                // ──── 服务端经 trailer 发出的码 ────
                ErrorCode.UNAUTHENTICATED,
                ErrorCode.PERMISSION_DENIED,
                ErrorCode.NOT_FOUND,
                ErrorCode.ALREADY_EXISTS,
                ErrorCode.INVALID_ARGUMENT,
                ErrorCode.FAILED_PRECONDITION,
                ErrorCode.OUT_OF_RANGE,
                ErrorCode.RESOURCE_EXHAUSTED,
                ErrorCode.DEADLINE_EXCEEDED,
                ErrorCode.UNAVAILABLE,
                ErrorCode.NOT_LEADER,
                ErrorCode.TXN_CAS_FAILED,
                ErrorCode.INTERNAL,
                // ──── SDK 本地码 ────
                ErrorCode.PROTOCOL_MISMATCH,
                ErrorCode.AGENT_UNAVAILABLE,
                ErrorCode.REGISTRY_SERVICE_NOT_FOUND,
                ErrorCode.REGISTRY_INSTANCE_ALREADY_EXISTS,
                ErrorCode.REGISTRY_LEASE_EXPIRED,
                ErrorCode.CONFIG_KEY_NOT_FOUND,
                ErrorCode.CONFIG_CAS_FAILED,
                ErrorCode.WATCH_STREAM_ERROR,
                // D1：TLS/证书等配置错误（恶意/错误配置必须 fail-closed，不做明文降级）
                ErrorCode.CONFIG_INVALID);
    }

    /**
     * 每个 wire 码都必须存在（缺一个，该错误在客户端就静默变成 INTERNAL = 不可重试）。
     */
    @Test
    void everyWireCodeFromRustExistsInTheEnum() {
        Set<String> declared = new HashSet<>();
        for (ErrorCode code : ErrorCode.values()) {
            declared.add(code.getProtoName());
        }
        assertThat(declared).containsAll(WIRE_CODES);
    }

    /**
     * `fromProtoName` 必须对**每个**常量做往返（这是真实逻辑，不只是字面量比对）：
     * 若两个常量共用同一个 `protoName`，查表会只留下一项，往返即失败。
     *
     * <p>旧测试逐条断言 `getProtoName()` 等于常量名的字面量拷贝——不可证伪，且漏了
     * `CONFIG_INVALID`（第四轮 §3.14.2 指出）。本测试由 `values()` 驱动，新增常量自动
     * 被覆盖，无法再漏。
     */
    @Test
    void protoNameRoundTripsThroughLookupTable() {
        for (ErrorCode code : ErrorCode.values()) {
            assertThat(code.getProtoName()).as("%s 的 protoName 不得为空", code).isNotBlank();
            assertThat(ErrorCode.fromProtoName(code.getProtoName()))
                    .as("%s 的 protoName 必须在查表中唯一且可往返", code)
                    .isEqualTo(code);
        }
    }

    /**
     * 未知码必须 fail-closed 到 INTERNAL（不可重试），而不是 null 或抛异常——
     * `ErrorMapper` 的首选分支直接吃这个返回值。
     */
    @Test
    void unknownProtoNameMapsToInternal() {
        assertThat(ErrorCode.fromProtoName(null)).isEqualTo(ErrorCode.INTERNAL);
        assertThat(ErrorCode.fromProtoName("")).isEqualTo(ErrorCode.INTERNAL);
        assertThat(ErrorCode.fromProtoName("SOMETHING_NEW_FROM_A_FUTURE_SERVER"))
                .isEqualTo(ErrorCode.INTERNAL);
    }

    /**
     * 本轮修复的语义：`NOT_LEADER` 与 `UNAVAILABLE` 必须是**不同**的码——
     * 前者应重定向到 leader 重试，后者应退避等待；压成一个码是第四轮 §3.14.2 的原始缺陷。
     */
    @Test
    void notLeaderIsDistinctFromUnavailable() {
        assertThat(ErrorCode.NOT_LEADER).isNotEqualTo(ErrorCode.UNAVAILABLE);
        assertThat(ErrorCode.fromProtoName("NOT_LEADER")).isEqualTo(ErrorCode.NOT_LEADER);
        assertThat(ErrorCode.fromProtoName("UNAVAILABLE")).isEqualTo(ErrorCode.UNAVAILABLE);
    }
}
