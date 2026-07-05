package cn.byteforce.coord.sdk.internal.channel;

/**
 * Handles protocol version negotiation between SDK and Agent.
 * <p>
 * The SDK sends its protocol version and the Agent responds with a list of supported versions.
 * If the SDK version is not in that list, the connection is rejected.
 */
public final class ProtocolNegotiator {

    /** The protocol version this SDK implements. */
    public static final String SDK_PROTOCOL_VERSION = "coord-agent-api-v1";

    private final String sdkVersion;

    public ProtocolNegotiator(String sdkVersion) {
        this.sdkVersion = sdkVersion;
    }

    /** The SDK protocol version string. */
    public String getSdkVersion() {
        return sdkVersion;
    }

    /**
     * Check whether the given version string matches the SDK's protocol version.
     * Returns true only for exact match.
     */
    public boolean isVersionSupported(String agentVersion) {
        return sdkVersion.equals(agentVersion);
    }
}
