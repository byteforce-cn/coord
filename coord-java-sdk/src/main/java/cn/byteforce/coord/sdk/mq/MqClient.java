package cn.byteforce.coord.sdk.mq;

import cn.byteforce.coord.sdk.CoordException;

import java.util.List;

/**
 * Message Queue API backed by the Coord Agent's MQ data-plane service
 * (per-agent redb segmented log; Topic / Partition / ConsumerGroup / DLQ).
 * <p>
 * <b>Capability level:</b> createTopic / publish / poll / ack fully supported.
 * Consumption follows <b>poll + ack</b> semantics:
 * <ul>
 *   <li>poll returns messages from a given {@code startOffset} (incremental cursor)</li>
 *   <li>ack commits the consumer-group offset (at-least-once — a message is only
 *       re-delivered if the consumer crashes before acking)</li>
 *   <li>poison messages can be observed via {@link #pollDlq}</li>
 * </ul>
 * <p>
 * <b>Data-plane boundary:</b> storage is local to a single agent by
 * default (<code>services.replication=false</code>). When cross-agent ISR
 * replication is enabled (<code>services.replication=true</code> +
 * <code>replication_peers</code>), published messages are synchronously
 * replicated to ISR followers (<code>min_isr</code> configurable; the partition
 * leader exclusively allocates offsets) — the data plane is then distributed /
 * highly available.
 * For multi-instance reliable decoupling, prefer the DB Outbox pattern for
 * business-side concerns.
 *
 * <p>
 * <b>Consumer routing (ISR replication enabled):</b> partition-scoped writes
 * ({@link #publish}, {@link #ack}) must reach the partition leader. Query it with
 * {@link #getTopicLeader(String)}; when a non-leader rejects the call it carries a
 * structured error with a {@code coord-leader-hint} trailer surfaced by
 * {@link CoordException#getLeaderHint()} — re-route to that address and retry.
 * Never parse the exception message to discover the leader.
 *
 * <pre>{@code
 * try (CoordClient client = CoordClient.create(config)) {
 *     MqClient mq = client.mq();
 *     mq.createTopic("orders", 4);
 *
 *     long offset = mq.publish("orders", 0, key, payload);
 *     List<MqMessage> batch = mq.poll("orders", 0, "icps-svc", 0, 100);
 *     for (MqMessage m: batch) {
 *         process(m);
 *         mq.ack("orders", "icps-svc", m.partition(), m.offset());
 *     }
 * }
 * }</pre>
 */
public interface MqClient {

    /**
     * Create a topic with the given number of partitions.
     * <p>
     * Idempotency is not guaranteed — creating an existing topic fails.
     *
     * @param topic      topic name
     * @param partitions number of partitions (must be &gt; 0)
     * @throws CoordException on communication failure or if the topic already exists
     */
    void createTopic(String topic, int partitions);

    /**
     * Publish a message to a partition.
     *
     * @param topic     topic name
     * @param partition partition number (0-based, must be &lt; topic partitions)
     * @param key       optional message key (may be null or empty)
     * @param payload   message payload
     * @return the assigned monotonic offset
     * @throws CoordException on communication failure, unknown topic, or partition out of range
     */
    long publish(String topic, int partition, byte[] key, byte[] payload);

    /**
     * Incrementally pull messages from {@code startOffset}.
     * <p>
     * Returns up to {@code maxCount} messages with offset &ge; {@code startOffset}.
     * Combine with {@link #ack} for at-least-once consumption.
     *
     * @param topic       topic name
     * @param partition   partition number
     * @param group       consumer group (used only for offset bookkeeping context)
     * @param startOffset minimum offset to return (inclusive cursor)
     * @param maxCount    maximum number of messages to return (&le; 0 means server default 100)
     * @return pulled messages (may be empty)
     * @throws CoordException on communication failure
     */
    List<MqMessage> poll(String topic, int partition, String group, long startOffset, int maxCount);

    /**
     * Pull messages from the dead-letter queue of a partition.
     *
     * @param topic     topic name
     * @param partition partition number
     * @param maxCount  maximum number of messages to return (&le; 0 means server default 100)
     * @return DLQ messages (may be empty)
     * @throws CoordException on communication failure
     */
    List<MqMessage> pollDlq(String topic, int partition, int maxCount);

    /**
     * Commit the consumer-group offset for a partition.
     * <p>
     * After acking offset N, subsequent {@link #poll} from N+1 will not
     * re-deliver already-acked messages (at-least-once).
     *
     * @param topic     topic name
     * @param group     consumer group
     * @param partition partition number
     * @param offset    offset to commit (last successfully processed message offset)
     * @throws CoordException on communication failure
     */
    void ack(String topic, String group, int partition, long offset);

    /**
     * Register a push-style subscriber (server-streaming).
     * <p>
     * The subscription resumes from the consumer group's committed offset —
     * already-acked messages are not re-delivered. Messages are pushed as they
     * are published. The returned handle must be closed to cancel the
     * subscription.
     * <p>
     * <b>Reliability:</b> subscribe auto-commits on delivery; under backpressure
     * the agent may drop messages. For strict at-least-once consumption,
     * prefer {@link #poll} + {@link #ack}.
     *
     * @param request  subscription parameters (topic, consumer group)
     * @param listener message callback (may be invoked from a gRPC callback thread)
     * @return an {@link AutoCloseable} that cancels the subscription when closed
     * @throws CoordException on communication failure or unknown topic
     */
    AutoCloseable subscribe(MqSubscribeRequest request, java.util.function.Consumer<MqMessage> listener);

    /**
     * Query the routing topology of a topic: leader agent address + current ISR
     * members (G-MQ-1). Any agent answers from the replicated topic config.
     * <p>
     * Single-agent deployment: {@code replicationEnabled() == false} and an empty
     * {@code leaderAgent()} — treat the connected agent as the leader. ISR enabled:
     * route {@link #publish} / {@link #ack} to {@code leaderAgent()}. When the ISR is
     * below {@code minIsr} ({@code degraded() == true}) writes are rejected until it
     * recovers — retry with backoff instead of re-routing.
     *
     * @param topic topic name
     * @return routing snapshot (never {@code null})
     * @throws CoordException on communication failure, or {@code NOT_FOUND} for an
     *                        unknown topic
     */
    MqTopicLeader getTopicLeader(String topic);

    /**
     * Explicitly move a message into the dead-letter queue (management path;
     * capability {@code coord:mq:manage}, G-MQ-2).
     * <p>
     * The message is removed from the main log and appended to the DLQ within one
     * transaction (net accounting — the partition is not blocked). {@code reason} and
     * {@code detail} stay readable via {@link #pollDlq}. There is no automatic
     * poison-message detection: the decision stays with the consumer.
     *
     * @param topic     topic name
     * @param partition partition number
     * @param offset    offset of the message to move
     * @param reason    short machine-readable reason (e.g. {@code "poison"})
     * @param detail    optional free-text detail (may be null or empty)
     * @throws CoordException on communication failure, or {@code NOT_FOUND} when the
     *                        topic or message does not exist
     */
    void moveToDlq(String topic, int partition, long offset, String reason, String detail);

    /**
     * Delete a topic and reclaim all of its state (management path; capability
     * {@code coord:mq:manage}, G-MQ-4).
     * <p>
     * Removes the topic config plus every stored message, DLQ entry, consumer
     * offset and idempotency-index row, and returns the accounted bytes to the
     * quota. Precondition: stop all readers/writers of the topic first — concurrent
     * publish/poll/ack observe explicit errors (unknown topic / not found) rather
     * than silent partial state. With ISR enabled the deletion is applied
     * consistently across all ISR members. Re-creating the same topic afterwards
     * yields an empty topic.
     *
     * @param topic topic name
     * @return per-table reclamation counts
     * @throws CoordException on communication failure or unknown topic
     */
    MqDeleteTopicResult deleteTopic(String topic);
}
