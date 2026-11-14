package cn.byteforce.coord.sdk.mq.v2;

import java.util.List;

/**
 * MQ v2 client surface ({@code coord.mq.v2}, ADR-0016).
 *
 * <p>This SDK talks to the Coord Agent, whose {@code MqV2Proxy} forwards the call
 * to the shard leader. Data-plane calls (publish/poll/DLQ) must reach the log
 * shard leader; group calls must reach the consumer-group shard leader. The
 * agent caches the {@code NOT_LEADER} hint and retries, so callers pass the
 * topic metadata, not a server address.
 *
 * <p>Semantics callers must honour:
 * <ul>
 *   <li>delivery is <b>at least once</b>: duplicate {@code (topic, partition, offset)}
 *       is a normal shape; {@link #poll} + {@link #ack} is the only path that
 *       commits progress;</li>
 *   <li>{@code topicEpoch} on the write path is the epoch from
 *       {@link #createTopic} / {@link #describeTopic}; {@code 0} means "do not
 *       check" and is for read paths only — a stale epoch is rejected with
 *       {@code TOPIC_EPOCH_STALE} instead of writing into a recreated topic;</li>
 *   <li>{@code startOffset == null} means "field unset" (server starts at 0);
 *       an explicit offset below {@code earliestOffset} fails with
 *       {@code OFFSET_OUT_OF_RANGE} — never silently clamps;</li>
 *   <li>branch on {@link cn.byteforce.coord.sdk.ErrorCode} via
 *       {@link cn.byteforce.coord.sdk.CoordException}, never on message text.</li>
 * </ul>
 *
 * <p>{@code Subscribe} is deliberately absent: the contract declares it
 * best-effort (drops under backpressure, does not replay, does not commit).
 * Use {@link #poll} + {@link #ack} when at-least-once is required.
 */
public interface MqV2Client {

    /** Create a topic; an empty {@code requestId} skips create idempotency. */
    MqV2TopicCreated createTopic(String topic, int partitions, String requestId,
                                 MqV2TopicConfig config);

    /** Topic metadata and the per-partition routing snapshot. */
    MqV2TopicDescription describeTopic(String topic);

    /** First page of topics for {@code prefix} (empty pageToken). */
    List<MqV2TopicSummary> listTopics(String prefix, int pageSize, String pageToken);

    /** One page of topics; {@code nextPageToken} is echoed for the next call. */
    Page<MqV2TopicSummary> listTopicsPage(String prefix, int pageSize, String pageToken);

    /** Update retention/quota/message limits; {@code expectedTopicEpoch} is a CAS (0 = skip). */
    MqV2TopicUpdated updateTopic(MqV2UpdateTopic update);

    /** Mark a topic deleted and start asynchronous reclamation. */
    MqV2TopicDeleted deleteTopic(MqV2DeleteTopic delete);

    /** Reclamation progress of one delete id. */
    MqV2TopicDeletion getTopicDeletion(String deleteId);

    /** Partition → shard/leader snapshot for a topic. */
    MqV2Leaders getTopicLeaders(String topic);

    /** Publish one message; the response is returned only after Raft commit. */
    MqV2PublishResult publish(MqV2Publish publish);

    /** Publish up to 64 entries to one partition; any failure appends nothing. */
    List<MqV2PublishResult> publishBatch(String topic, int partition, long topicEpoch,
                                         List<MqV2BatchEntry> entries);

    /** Fetch already-committed messages; {@code waitMs} is the long-poll bound. */
    MqV2PollResult poll(MqV2Poll poll);

    /** Commit one processed offset; returns the new committed offset (next to consume). */
    long ack(String group, String memberId, long generation, MqV2Ack ack);

    /** Commit several offsets atomically for one member/generation. */
    List<MqV2AckResult> ackBatch(String group, String memberId, long generation,
                                 List<MqV2Ack> acks);

    /** Join a group (empty memberId = new member; non-empty = resume a live session). */
    MqV2Joined joinGroup(MqV2JoinGroup join);

    /** Renew the session; {@code rebalanceRequired} means rejoin before the next poll/ack. */
    MqV2Heartbeat heartbeat(String group, String memberId, long generation);

    /** Leave the group (triggers a rebalance). */
    void leaveGroup(String group, String memberId, long generation);

    /** Committed offsets for the given partitions ({@code committed=false} = none). */
    List<MqV2CommittedOffset> getCommittedOffsets(String group,
                                                  List<MqV2TopicPartition> partitions);

    /** Group state, members, assignments and per-partition lag. */
    MqV2GroupDescription describeGroup(String group);

    /** One page of group names. */
    Page<String> listGroupsPage(String prefix, int pageSize, String pageToken);

    /** Copy a main-log offset into the DLQ and tombstone the original (idempotent). */
    MqV2MoveToDlqResult moveToDlq(MqV2MoveToDlq move);

    /** Read DLQ entries; {@code nextDlqOffset} is the DLQ cursor, not the main-log offset. */
    MqV2PollDlqResult pollDlq(String topic, int partition, long topicEpoch,
                              Long startOffset, int maxMessages);
}
