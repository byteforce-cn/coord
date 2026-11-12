package cn.byteforce.coord.sdk;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Structured error codes for Coord SDK.
 * All RPC errors MUST be identified by these codes, never by parsing error description strings.
 *
 * <p><b>Wire contract.</b> The {@code protoName} strings are the
 * values carried in the gRPC trailers header {@code x-coord-error-code}. The server and
 * agent must actually emit that header ({@code coord-core::error_code::CoordErrorCode}) —
 * otherwise {@link cn.byteforce.coord.sdk.internal.rpc.ErrorMapper}'s preferred
 * branch is dead code and only a lossy status-code table applies — {@code NOT_FOUND}
 * (any KV miss, missing certificate, missing lease…) was reported as
 * "registry service not found", and {@code UNAVAILABLE} (including <i>not leader</i>)
 * as "agent is down", while the retry matrix keys off exactly those codes.
 *
 * <p>Every value emitted by the server MUST appear here verbatim; an unknown value maps to
 * {@link #INTERNAL} (a silent downgrade to "not retryable"). This is enforced mechanically
 * by {@code scripts/check-error-code-contract.sh} (run in CI) plus the exhaustive
 * {@code ErrorCodeTest}.
 *
 * <p>The constants in the second block below are produced <i>client-side only</i>
 * (local validation, channel lifecycle, SDK-side classification); the server never emits
 * them. That split is asserted mechanically: the CI script requires every Rust-emitted code
 * to exist here, and every code here that Rust never emits to be on the documented
 * client-local allowlist — so a new code cannot be added on one side only by accident.
 */
public enum ErrorCode {
    // ──── Codes emitted by the server/agent over the wire ────
    UNAUTHENTICATED("UNAUTHENTICATED"),
    PERMISSION_DENIED("PERMISSION_DENIED"),
    NOT_FOUND("NOT_FOUND"),
    ALREADY_EXISTS("ALREADY_EXISTS"),
    INVALID_ARGUMENT("INVALID_ARGUMENT"),
    FAILED_PRECONDITION("FAILED_PRECONDITION"),
    OUT_OF_RANGE("OUT_OF_RANGE"),
    RESOURCE_EXHAUSTED("RESOURCE_EXHAUSTED"),
    DEADLINE_EXCEEDED("DEADLINE_EXCEEDED"),
    UNAVAILABLE("UNAVAILABLE"),
    /**
     * The node is not the Raft leader. Distinct from {@link #UNAVAILABLE} on purpose:
     * the correct reaction is to redirect to the leader and retry, not to wait for the
     * agent to recover.
     */
    NOT_LEADER("NOT_LEADER"),
    /** Transaction compare-and-swap failed — a business outcome, not a failure. */
    TXN_CAS_FAILED("TXN_CAS_FAILED"),
    INTERNAL("INTERNAL"),

    // ──── KV-T v2（ADR-0015）────
    /** OCC 冲突（读集/谓词被写过）。gRPC ABORTED；换 start_ts 重跑整个事务。 */
    TXN_CONFLICT("TXN_CONFLICT"),
    /** 事务会话不存在/已过期。gRPC ABORTED；重跑整个事务。 */
    TXN_NOT_FOUND("TXN_NOT_FOUND"),
    /** 写集超过 max_txn_bytes。gRPC INVALID_ARGUMENT；拆事务。 */
    TXN_TOO_LARGE("TXN_TOO_LARGE"),
    /** 读集/range 读超限。gRPC INVALID_ARGUMENT；拆事务。 */
    READ_SET_TOO_LARGE("READ_SET_TOO_LARGE"),
    /** commit_ts 不合法（CommitRejected）。gRPC FAILED_PRECONDITION；不可重试。 */
    COMMIT_REJECTED("COMMIT_REJECTED"),
    /** Region 路由 epoch 过期。gRPC ABORTED；刷新路由后重试。 */
    EPOCH_STALE("EPOCH_STALE"),
    /** Watch 位点超出保留窗口。gRPC OUT_OF_RANGE；全量重同步。 */
    HISTORY_UNAVAILABLE("HISTORY_UNAVAILABLE"),
    /** read_ts 等待 safe_ts 超时。gRPC UNAVAILABLE；退避重试。 */
    SAFE_TS_LAG("SAFE_TS_LAG"),

    // ──── MQ v2（ADR-0016）────
    /** topic 不存在/已删除。gRPC NOT_FOUND；不可重试（重建后旧代际仍不行）。 */
    TOPIC_NOT_FOUND("TOPIC_NOT_FOUND"),
    /** 同名 topic 已存在且配置不同。gRPC ALREADY_EXISTS；不可重试。 */
    TOPIC_EXISTS("TOPIC_EXISTS"),
    /** topic_epoch 与当前代际不一致。gRPC ABORTED；刷新元数据后重试。 */
    TOPIC_EPOCH_STALE("TOPIC_EPOCH_STALE"),
    /** partition 越界/不存在。gRPC NOT_FOUND；不可重试。 */
    PARTITION_NOT_FOUND("PARTITION_NOT_FOUND"),
    /** start_offset 低于最早可用 offset。gRPC OUT_OF_RANGE；按 reset 策略处理。 */
    OFFSET_OUT_OF_RANGE("OFFSET_OUT_OF_RANGE"),
    /** 消费组不存在。gRPC NOT_FOUND；重新 JoinGroup。 */
    GROUP_NOT_FOUND("GROUP_NOT_FOUND"),
    /** 不是组成员或分区未分配给该成员。gRPC FAILED_PRECONDITION；重新 JoinGroup。 */
    NOT_GROUP_MEMBER("NOT_GROUP_MEMBER"),
    /** generation 过期。gRPC ABORTED；重新 JoinGroup。 */
    GROUP_EPOCH_STALE("GROUP_EPOCH_STALE"),
    /** 组正在再平衡。gRPC ABORTED；退避后重新 JoinGroup。 */
    REBALANCE_IN_PROGRESS("REBALANCE_IN_PROGRESS"),
    /** 单条消息超过 max_message_bytes。gRPC INVALID_ARGUMENT；整批不追加。 */
    MESSAGE_TOO_LARGE("MESSAGE_TOO_LARGE"),
    /** 超过 topic 容量配额。gRPC RESOURCE_EXHAUSTED；整批不追加。 */
    QUOTA_EXCEEDED("QUOTA_EXCEEDED"),

    // ──── Client-local codes (never emitted by the server) ────
    PROTOCOL_MISMATCH("PROTOCOL_MISMATCH"),
    AGENT_UNAVAILABLE("AGENT_UNAVAILABLE"),
    REGISTRY_SERVICE_NOT_FOUND("REGISTRY_SERVICE_NOT_FOUND"),
    REGISTRY_INSTANCE_ALREADY_EXISTS("REGISTRY_INSTANCE_ALREADY_EXISTS"),
    REGISTRY_LEASE_EXPIRED("REGISTRY_LEASE_EXPIRED"),
    CONFIG_KEY_NOT_FOUND("CONFIG_KEY_NOT_FOUND"),
    CONFIG_CAS_FAILED("CONFIG_CAS_FAILED"),
    WATCH_STREAM_ERROR("WATCH_STREAM_ERROR"),
    CONFIG_INVALID("CONFIG_INVALID");

    private final String protoName;

    private static final Map<String, ErrorCode> BY_PROTO_NAME = new ConcurrentHashMap<>();

    static {
        for (ErrorCode code : values()) {
            BY_PROTO_NAME.put(code.protoName, code);
        }
    }

    ErrorCode(String protoName) {
        this.protoName = protoName;
    }

    /** The string value used in gRPC trailers header {@code x-coord-error-code}. */
    public String getProtoName() {
        return protoName;
    }

    /**
     * Look up an ErrorCode by its proto/trailer name.
     * Returns {@link #INTERNAL} if the name is unknown or null.
     */
    public static ErrorCode fromProtoName(String protoName) {
        if (protoName == null) {
            return INTERNAL;
        }
        return BY_PROTO_NAME.getOrDefault(protoName, INTERNAL);
    }
}
