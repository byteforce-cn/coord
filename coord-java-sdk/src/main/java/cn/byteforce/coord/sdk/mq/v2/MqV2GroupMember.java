package cn.byteforce.coord.sdk.mq.v2;

import java.util.List;

/** One live member in {@code DescribeGroup}. */
public record MqV2GroupMember(String memberId, List<String> topics,
                              List<MqV2Assignment> assignments, long lastHeartbeatMs) {
    public MqV2GroupMember {
        topics = topics == null ? List.of() : List.copyOf(topics);
        assignments = assignments == null ? List.of() : List.copyOf(assignments);
    }
}
