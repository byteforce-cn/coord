package cn.byteforce.coord.sdk.internal.channel;

import cn.byteforce.coord.sdk.CoordException;
import cn.byteforce.coord.sdk.ErrorCode;

import java.util.List;

/**
 * Handles protocol version negotiation between SDK and Agent.
 * <p>
 * The SDK sends its protocol version and the Agent responds with the list of versions it
 * supports. If the SDK version is not in that list, the connection is <b>rejected with a
 * diagnosable error</b>.
 *
 * <p><b>Empty-promise fix (P0-4 / D6, 2026-09-19).</b> This class previously documented the
 * behaviour above while the behaviour did not exist anywhere: {@link #isVersionSupported}
 * was only ever called from unit tests, and {@code AgentChannelManager} constructed a
 * negotiator and never used it. The agent side had no handler at all (only a capability-table
 * entry in {@code coord-core/src/grpc_auth.rs}). The prompt for fixing it is the one-shot
 * package rename (D2): after the rename an old SDK calling
 * {@code /coord.agent.Registry/Register} receives a bare gRPC {@code UNIMPLEMENTED} (unknown
 * service), whose message says nothing about a version mismatch. Negotiation is what makes
 * that failure diagnosable.
 *
 * <p>The SDK-side version constant lives here; the agent-side single source of truth is
 * {@code coord_agent::services::handshake::SUPPORTED_PROTOCOL_VERSIONS}. The two are kept in
 * step by {@code coord/tests/agent_handshake_test.rs}, so this cannot silently drift back
 * into an empty promise.
 */
public final class ProtocolNegotiator {

    /** The protocol version this SDK implements. */
    public static final String SDK_PROTOCOL_VERSION = "coord-agent-api-v2";

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

    /**
     * Evaluate the Agent's advertised version list.
     *
     * @param agentVersions versions returned by {@code Handshake.Negotiate} (may be null/empty)
     * @return true when this SDK's version is advertised by the agent
     */
    public boolean isSupportedBy(List<String> agentVersions) {
        return agentVersions != null && agentVersions.contains(sdkVersion);
    }

    /**
     * Fail with a diagnosable error when the agent does not speak this SDK's protocol.
     * <p>
     * The message carries all three facts needed to act on it: what this SDK speaks, what the
     * agent advertised, and why it matters. This is the whole point of negotiating before
     * issuing real calls — an un-diagnosable failure is exactly what
     * {@code PROTOCOL_MISMATCH} exists to avoid.
     *
     * @param agentVersions versions returned by the agent
     * @throws CoordException with {@link ErrorCode#PROTOCOL_MISMATCH} when unsupported
     */
    public void requireSupported(List<String> agentVersions) {
        if (isSupportedBy(agentVersions)) {
            return;
        }
        throw new CoordException(ErrorCode.PROTOCOL_MISMATCH,
                "agent does not support SDK protocol version '" + sdkVersion + "'"
                        + " (agent advertises " + describe(agentVersions) + "); "
                        + "upgrade the SDK/agent to matching versions — the agent's services were "
                        + "renamed in contracts/v1.2.0, so calls will otherwise fail as "
                        + "UNIMPLEMENTED (unknown service) with no version hint");
    }

    private static String describe(List<String> versions) {
        if (versions == null || versions.isEmpty()) {
            return "nothing (agent did not implement Handshake.Negotiate)";
        }
        return versions.toString();
    }
}
