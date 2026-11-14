package cn.byteforce.coord.sdk.mq.v2;

import java.util.List;

/** Result of {@code JoinGroup}: the member id, generation and range assignment. */
public record MqV2Joined(String memberId, long generation, List<MqV2Assignment> assignments,
                         int heartbeatIntervalMs, int sessionTimeoutMs) {
    public MqV2Joined {
        assignments = assignments == null ? List.of() : List.copyOf(assignments);
    }
}
