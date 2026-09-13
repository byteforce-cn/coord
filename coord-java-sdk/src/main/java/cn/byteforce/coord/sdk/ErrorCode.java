package cn.byteforce.coord.sdk;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Structured error codes for Coord SDK.
 * All RPC errors MUST be identified by these codes, never by parsing error description strings.
 *
 * <p><b>Wire contract (fourth review §3.14.2).</b> The {@code protoName} strings are the
 * values carried in the gRPC trailers header {@code x-coord-error-code}. The server and
 * agent now actually emit that header ({@code coord-core::error_code::CoordErrorCode});
 * before that fix, {@link cn.byteforce.coord.sdk.internal.rpc.ErrorMapper}'s preferred
 * branch was dead code and only a lossy status-code table applied — {@code NOT_FOUND}
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
