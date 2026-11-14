package cn.byteforce.coord.sdk.mq.v2;

/**
 * One partition's routing snapshot. {@code earliestOffset} advances when
 * retention drops whole segments; {@code nextOffset} is the next offset to be
 * assigned (= latest committed + 1). {@code leaderAddr} is empty while the
 * shard has no leader (election).
 */
public record MqV2PartitionInfo(int partition, int shardId, String leaderAddr,
                                long earliestOffset, long nextOffset, long logBytes) {
    public MqV2PartitionInfo {
        leaderAddr = leaderAddr == null ? "" : leaderAddr;
    }
}
