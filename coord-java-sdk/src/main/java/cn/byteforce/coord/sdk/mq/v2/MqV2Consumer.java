package cn.byteforce.coord.sdk.mq.v2;

import cn.byteforce.coord.sdk.CoordException;
import cn.byteforce.coord.sdk.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Consumer-group helper: Join → Poll → process → Ack → Heartbeat.
 *
 * <p>It handles the three things a hand-written loop gets wrong:
 * <ol>
 *   <li><b>generation</b>: a heartbeat reply with {@code rebalanceRequired} or a
 *       group error ({@code GROUP_EPOCH_STALE} / {@code REBALANCE_IN_PROGRESS} /
 *       {@code NOT_GROUP_MEMBER} / {@code GROUP_NOT_FOUND}) marks the consumer
 *       dirty; the next {@link #poll()} / {@link #ack} rejoins first and keeps
 *       the cursors of partitions it still owns;</li>
 *   <li><b>out-of-range cursor</b>: {@code OFFSET_OUT_OF_RANGE} follows the
 *       configured {@link MqV2ResetPolicy} (default: fail — never silently drop
 *       history);</li>
 *   <li><b>topic_epoch</b>: after Join it fetches the routing snapshot once per
 *       topic and stamps every data-plane request, so an old client cannot read
 *       a recreated topic with the same name.</li>
 * </ol>
 *
 * <p>Delivery is <b>at least once</b>: duplicate {@code (topic, partition, offset)}
 * is a normal shape, and callers must be idempotent. A fresh assignment (no
 * committed offset) starts at the earliest available offset, mirroring the Rust
 * helper. This class is not thread-safe for concurrent
 * {@link #poll()}/{@link #ack} calls from several threads; the heartbeat runs on
 * its own daemon thread.
 */
public final class MqV2Consumer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MqV2Consumer.class);
    private static final long DEFAULT_HEARTBEAT_MS = 10_000;

    private final MqV2Client client;
    private final MqV2ConsumerOptions options;
    private final ScheduledExecutorService heartbeat;
    private final Object lock = new Object();

    private String memberId = "";
    private long generation;
    private long heartbeatIntervalMs;
    private List<Cursor> cursors = List.of();
    private boolean needsRejoin;
    private boolean closed;

    private MqV2Consumer(MqV2Client client, MqV2ConsumerOptions options) {
        this.client = client;
        this.options = options;
        this.heartbeat = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "coord-mq-v2-heartbeat");
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * Join the group and start the heartbeat.
     *
     * @throws IllegalArgumentException when the group or topic list is empty
     */
    public static MqV2Consumer join(MqV2Client client, MqV2ConsumerOptions options) {
        MqV2Consumer consumer = new MqV2Consumer(client, options);
        synchronized (consumer.lock) {
            consumer.joinOnce(false);
        }
        consumer.scheduleHeartbeat(consumer.heartbeatDelayMs());
        return consumer;
    }

    /** Member id assigned by the server (stable across rejoins until Leave/expiry). */
    public String memberId() {
        synchronized (lock) {
            return memberId;
        }
    }

    /** Current generation; every Ack is fenced by it. */
    public long generation() {
        synchronized (lock) {
            return generation;
        }
    }

    /** Partitions currently assigned to this member. */
    public List<MqV2TopicPartition> assignments() {
        synchronized (lock) {
            List<MqV2TopicPartition> result = new ArrayList<>(cursors.size());
            for (Cursor cursor : cursors) {
                result.add(new MqV2TopicPartition(cursor.topic, cursor.partition));
            }
            return result;
        }
    }

    /**
     * Fetch one batch per assigned partition; when nothing came back and
     * {@code waitMs > 0}, long-poll the first partition once. An empty list means
     * the long poll expired, not that the log has no unread history.
     */
    public synchronized List<MqV2Message> poll() {
        ensureJoined();
        List<Cursor> snapshot;
        synchronized (lock) {
            snapshot = List.copyOf(cursors);
        }
        List<MqV2Message> messages = new ArrayList<>();
        for (Cursor cursor : snapshot) {
            messages.addAll(pollPartition(cursor, 0));
        }
        if (messages.isEmpty() && options.waitMs() > 0 && !snapshot.isEmpty()) {
            messages.addAll(pollPartition(snapshot.get(0), options.waitMs()));
        }
        return messages;
    }

    /**
     * Commit one processed message. A stale generation triggers one rejoin and a
     * retry — never a silent success.
     *
     * @return the new committed offset (next to consume)
     */
    public synchronized long ack(MqV2Message message) {
        ensureJoined();
        MqV2Ack ack;
        String group;
        String member;
        long gen;
        synchronized (lock) {
            ack = new MqV2Ack(message.topic(), message.partition(), epochOf(message.topic()),
                    message.offset());
            group = options.group();
            member = memberId;
            gen = generation;
        }
        try {
            return client.ack(group, member, gen, ack);
        } catch (CoordException e) {
            if (!isRebalance(e)) {
                throw e;
            }
            synchronized (lock) {
                joinOnce(true);
                member = memberId;
                gen = generation;
            }
            return client.ack(group, member, gen, ack);
        }
    }

    /** Stop the heartbeat and leave the group (idempotent; leave errors are logged only). */
    @Override
    public void close() {
        String member;
        long gen;
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            member = memberId;
            gen = generation;
        }
        heartbeat.shutdownNow();
        if (member.isEmpty()) {
            return;
        }
        try {
            client.leaveGroup(options.group(), member, gen);
        } catch (RuntimeException e) {
            log.warn("MQ v2 leaveGroup failed (group={}, member={}): {}",
                    options.group(), member, e.toString());
        }
    }

    private void ensureJoined() {
        synchronized (lock) {
            if (needsRejoin) {
                joinOnce(true);
            }
        }
    }

    /**
     * One Join call. With {@code preserveCursors}, partitions that are still
     * assigned keep their local next offset (a rejoin must not replay or skip).
     */
    private void joinOnce(boolean preserveCursors) {
        Map<String, Long> previous = new HashMap<>();
        if (preserveCursors) {
            for (Cursor cursor : cursors) {
                previous.put(cursor.topic + "\u0000" + cursor.partition, cursor.nextOffset);
            }
        }
        String member = memberId.isEmpty() ? options.memberId() : memberId;
        MqV2Joined joined = client.joinGroup(new MqV2JoinGroup(
                options.group(), member, options.topics(), options.sessionTimeoutMs()));

        Map<String, Long> epochs = new HashMap<>();
        for (MqV2Assignment assignment : joined.assignments()) {
            if (epochs.containsKey(assignment.topic())) {
                continue;
            }
            try {
                epochs.put(assignment.topic(),
                        client.getTopicLeaders(assignment.topic()).topicEpoch());
            } catch (RuntimeException e) {
                // A routing snapshot failure leaves the epoch at 0 (server does not
                // check it on read). The next Join refreshes it.
                log.debug("MQ v2 topic epoch lookup failed for {}: {}",
                        assignment.topic(), e.toString());
            }
        }

        Map<String, Cursor> rebuilt = new HashMap<>();
        for (MqV2Assignment assignment : joined.assignments()) {
            Long nextOffset = previous.get(assignment.topic() + "\u0000" + assignment.partition());
            if (nextOffset == null && assignment.committedOffset() > 0) {
                nextOffset = assignment.committedOffset();
            }
            rebuilt.put(assignment.topic() + "\u0000" + assignment.partition(),
                    new Cursor(assignment.topic(), assignment.partition(),
                            epochs.getOrDefault(assignment.topic(), 0L), nextOffset));
        }

        memberId = joined.memberId();
        generation = joined.generation();
        heartbeatIntervalMs = joined.heartbeatIntervalMs();
        cursors = List.copyOf(rebuilt.values());
        needsRejoin = false;
    }

    private List<MqV2Message> pollPartition(Cursor cursor, int waitMs) {
        Long startOffset = cursor.nextOffset;
        boolean resetApplied = false;
        while (true) {
            MqV2Poll request = new MqV2Poll(cursor.topic, cursor.partition, cursor.topicEpoch,
                    startOffset, options.maxMessages(), options.maxBytes(), waitMs);
            try {
                MqV2PollResult result = client.poll(request);
                updateCursor(cursor, result.nextOffset());
                return result.messages();
            } catch (CoordException e) {
                if (!isOffsetOutOfRange(e) || resetApplied) {
                    throw e;
                }
                switch (options.resetPolicy()) {
                    case FAIL -> throw e;
                    // null = field unset: the server starts at its current earliest
                    // available offset (it never clamps an explicit value).
                    case EARLIEST -> startOffset = null;
                    case LATEST -> startOffset = latestOffset(cursor.topic, cursor.partition);
                }
                resetApplied = true;
            }
        }
    }

    private long latestOffset(String topic, int partition) {
        MqV2Leaders leaders = client.getTopicLeaders(topic);
        for (MqV2PartitionInfo info : leaders.partitions()) {
            if (info.partition() == partition) {
                return info.nextOffset();
            }
        }
        throw new CoordException(ErrorCode.NOT_FOUND,
                "partition " + partition + " not found in topic " + topic);
    }

    /** The server echoes the request start for an empty batch; never move backwards. */
    private void updateCursor(Cursor cursor, long nextOffset) {
        synchronized (lock) {
            Long current = cursor.nextOffset;
            cursor.nextOffset = current == null ? nextOffset : Math.max(current, nextOffset);
        }
    }

    private long epochOf(String topic) {
        for (Cursor cursor : cursors) {
            if (cursor.topic.equals(topic)) {
                return cursor.topicEpoch;
            }
        }
        return 0;
    }

    private long heartbeatDelayMs() {
        if (options.heartbeatIntervalMs() > 0) {
            return options.heartbeatIntervalMs();
        }
        return heartbeatIntervalMs > 0 ? heartbeatIntervalMs : DEFAULT_HEARTBEAT_MS;
    }

    private void scheduleHeartbeat(long delayMs) {
        try {
            heartbeat.schedule(this::heartbeatTick, delayMs, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ignored) {
            // close() won the race; no further heartbeat is wanted.
        }
    }

    private void heartbeatTick() {
        String member;
        long gen;
        synchronized (lock) {
            if (closed) {
                return;
            }
            member = memberId;
            gen = generation;
        }
        if (!member.isEmpty()) {
            try {
                if (client.heartbeat(options.group(), member, gen).rebalanceRequired()) {
                    markDirty();
                }
            } catch (CoordException e) {
                if (isRebalance(e)) {
                    markDirty();
                }
                // Transport / no-quorum: retry next tick; an outage is not a rebalance.
            } catch (RuntimeException e) {
                log.debug("MQ v2 heartbeat failed (group={}): {}", options.group(), e.toString());
            }
        }
        long delay;
        synchronized (lock) {
            if (closed) {
                return;
            }
            delay = heartbeatDelayMs();
        }
        scheduleHeartbeat(delay);
    }

    private void markDirty() {
        synchronized (lock) {
            needsRejoin = true;
        }
    }

    private static boolean isRebalance(CoordException e) {
        ErrorCode code = e.getErrorCode();
        return code == ErrorCode.GROUP_EPOCH_STALE
                || code == ErrorCode.REBALANCE_IN_PROGRESS
                || code == ErrorCode.NOT_GROUP_MEMBER
                || code == ErrorCode.GROUP_NOT_FOUND;
    }

    private static boolean isOffsetOutOfRange(CoordException e) {
        return e.getErrorCode() == ErrorCode.OFFSET_OUT_OF_RANGE;
    }

    /** One assigned partition's local cursor; {@code nextOffset == null} = start at earliest. */
    private static final class Cursor {
        private final String topic;
        private final int partition;
        private final long topicEpoch;
        private Long nextOffset;

        private Cursor(String topic, int partition, long topicEpoch, Long nextOffset) {
            this.topic = topic;
            this.partition = partition;
            this.topicEpoch = topicEpoch;
            this.nextOffset = nextOffset;
        }
    }
}
