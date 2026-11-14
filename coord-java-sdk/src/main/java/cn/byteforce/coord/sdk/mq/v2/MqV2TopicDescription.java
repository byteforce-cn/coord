package cn.byteforce.coord.sdk.mq.v2;

import java.util.List;

/** Full topic metadata plus the per-partition routing snapshot. */
public record MqV2TopicDescription(String topic, long topicId, long topicEpoch,
                                   int partitionCount, MqV2TopicConfig config,
                                   long createdAtMs, List<MqV2PartitionInfo> partitions) {
    public MqV2TopicDescription {
        partitions = partitions == null ? List.of() : List.copyOf(partitions);
    }
}
