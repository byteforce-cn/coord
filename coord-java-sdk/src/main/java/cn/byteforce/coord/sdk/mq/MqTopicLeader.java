package cn.byteforce.coord.sdk.mq;

import java.util.List;

/**
 * Topic routing / ISR topology snapshot returned by
 * {@link MqClient#getTopicLeader(String)} (G-MQ-1).
 * <p>
 * Routing rules:
 * <ul>
 *   <li>Single-agent deployment: {@code replicationEnabled() == false} and
 *       {@code leaderAgent()} is empty — treat the connected agent as the
 *       leader for every partition;</li>
 *   <li>ISR enabled: partition-scoped writes ({@link MqClient#publish} /
 *       {@link MqClient#ack}) must be routed to {@code leaderAgent()}; other
 *       agents reject them with a structured error carrying a
 *       {@code coord-leader-hint} trailer (see
 *       {@link cn.byteforce.coord.sdk.CoordException#getLeaderHint()});</li>
 *   <li>{@code degraded() == true} means the current ISR size is below
 *       {@code minIsr} — writes are rejected until ISR recovers; retry with
 *       backoff (do not re-route).</li>
 * </ul>
 */
public final class MqTopicLeader {

    private final String topic;
    private final String leaderAgent;
    private final List<String> isrMembers;
    private final boolean replicationEnabled;
    private final boolean degraded;
    private final int partitions;
    private final long minIsr;

    public MqTopicLeader(String topic, String leaderAgent, List<String> isrMembers,
                         boolean replicationEnabled, boolean degraded, int partitions, long minIsr) {
        this.topic = topic;
        this.leaderAgent = leaderAgent == null ? "" : leaderAgent;
        this.isrMembers = isrMembers == null ? List.of() : List.copyOf(isrMembers);
        this.replicationEnabled = replicationEnabled;
        this.degraded = degraded;
        this.partitions = partitions;
        this.minIsr = minIsr;
    }

    /** Topic name. */
    public String topic() {
        return topic;
    }

    /** Leader agent address ({@code host:port}); empty in single-agent deployments. */
    public String leaderAgent() {
        return leaderAgent;
    }

    /** Current ISR members (including the leader); empty in single-agent deployments. */
    public List<String> isrMembers() {
        return isrMembers;
    }

    /** Whether cross-agent ISR replication is enabled for this topic. */
    public boolean replicationEnabled() {
        return replicationEnabled;
    }

    /** Whether the ISR is below {@code minIsr} (writes rejected until it recovers). */
    public boolean degraded() {
        return degraded;
    }

    /** Number of partitions of the topic. */
    public int partitions() {
        return partitions;
    }

    /** Effective {@code minIsr} (1 when replication is off). */
    public long minIsr() {
        return minIsr;
    }

    @Override
    public String toString() {
        return "MqTopicLeader{topic='" + topic + "', leaderAgent='" + leaderAgent
                + "', replicationEnabled=" + replicationEnabled
                + ", degraded=" + degraded + ", isrMembers=" + isrMembers.size() + "}";
    }
}
