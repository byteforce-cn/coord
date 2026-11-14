package cn.byteforce.coord.sdk.mq.v2;

import java.util.List;

/**
 * Partition-to-shard routing snapshot ({@code GetTopicLeader}). An empty
 * {@code leaderAddr} means the shard has no leader right now; the agent retries
 * with its own hint cache, callers do not route.
 */
public record MqV2Leaders(String topic, long topicId, long topicEpoch,
                          List<MqV2PartitionInfo> partitions) {
    public MqV2Leaders {
        partitions = partitions == null ? List.of() : List.copyOf(partitions);
    }
}
