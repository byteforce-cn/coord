package cn.byteforce.coord.sdk;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Structured error codes for Coord SDK.
 * All RPC errors MUST be identified by these codes, never by parsing error description strings.
 */
public enum ErrorCode {
    PROTOCOL_MISMATCH("PROTOCOL_MISMATCH"),
    AGENT_UNAVAILABLE("AGENT_UNAVAILABLE"),
    REGISTRY_SERVICE_NOT_FOUND("REGISTRY_SERVICE_NOT_FOUND"),
    REGISTRY_INSTANCE_ALREADY_EXISTS("REGISTRY_INSTANCE_ALREADY_EXISTS"),
    REGISTRY_LEASE_EXPIRED("REGISTRY_LEASE_EXPIRED"),
    CONFIG_KEY_NOT_FOUND("CONFIG_KEY_NOT_FOUND"),
    CONFIG_CAS_FAILED("CONFIG_CAS_FAILED"),
    WATCH_STREAM_ERROR("WATCH_STREAM_ERROR"),
    RESOURCE_EXHAUSTED("RESOURCE_EXHAUSTED"),
    INTERNAL("INTERNAL");

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
