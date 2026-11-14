package cn.byteforce.coord.sdk.mq.v2;

import java.util.List;

/** Group state, members, assignments and per-partition lag ({@code DescribeGroup}). */
public record MqV2GroupDescription(String group, long generation, MqV2GroupState state,
                                   List<MqV2GroupMember> members,
                                   List<MqV2GroupPartitionLag> partitions) {
    public MqV2GroupDescription {
        state = state == null ? MqV2GroupState.UNSPECIFIED : state;
        members = members == null ? List.of() : List.copyOf(members);
        partitions = partitions == null ? List.of() : List.copyOf(partitions);
    }
}
