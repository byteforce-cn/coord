package cn.byteforce.coord.sdk;

import cn.byteforce.coord.sdk.mq.v2.MqV2AckResult;
import cn.byteforce.coord.sdk.mq.v2.MqV2BatchEntry;
import cn.byteforce.coord.sdk.mq.v2.MqV2Client;
import cn.byteforce.coord.sdk.mq.v2.MqV2Consumer;
import cn.byteforce.coord.sdk.mq.v2.MqV2ConsumerOptions;
import cn.byteforce.coord.sdk.mq.v2.MqV2DeletionState;
import cn.byteforce.coord.sdk.mq.v2.MqV2GroupDescription;
import cn.byteforce.coord.sdk.mq.v2.MqV2Message;
import cn.byteforce.coord.sdk.mq.v2.MqV2MoveToDlq;
import cn.byteforce.coord.sdk.mq.v2.MqV2MoveToDlqResult;
import cn.byteforce.coord.sdk.mq.v2.MqV2Poll;
import cn.byteforce.coord.sdk.mq.v2.MqV2PollDlqResult;
import cn.byteforce.coord.sdk.mq.v2.MqV2PollResult;
import cn.byteforce.coord.sdk.mq.v2.MqV2Publish;
import cn.byteforce.coord.sdk.mq.v2.MqV2PublishResult;
import cn.byteforce.coord.sdk.mq.v2.MqV2TopicConfig;
import cn.byteforce.coord.sdk.mq.v2.MqV2TopicCreated;
import cn.byteforce.coord.sdk.mq.v2.MqV2TopicDeleted;
import cn.byteforce.coord.sdk.mq.v2.MqV2TopicDescription;
import cn.byteforce.coord.sdk.mq.v2.MqV2TopicDeletion;
import cn.byteforce.coord.sdk.mq.v2.MqV2TopicPartition;
import cn.byteforce.coord.sdk.mq.v2.MqV2UpdateTopic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * MQ v2 SDK against a real {@code coord server} + {@code coord agent}.
 *
 * <p>The fake-server suite pins the wire mapping; this suite pins the whole
 * chain through the agent proxy: create → publish (idempotent replay) → group
 * consumption with two members → committed offsets/lag → DLQ → update →
 * delete/reclamation.
 *
 * <p>The cluster is started by {@code scripts/ci-java-it-cluster.sh start v2};
 * a missing cluster is a hard failure — {@code Assumptions} would make "not
 * run" indistinguishable from "passed". Only under {@code -Pit}.
 */
@DisplayName("MQ v2 SDK — real v2 cluster through the agent")
class MqV2ClusterIntegrationTest {

    private static final String RUN = "it-" + System.currentTimeMillis();
    private static final String TOPIC = "orders-" + RUN;
    private static final String GROUP = "g-" + RUN;

    private static CoordClient client;

    @BeforeAll
    static void setUp() {
        String host = env("COORD_V2_AGENT_HOST", "localhost");
        int port = Integer.parseInt(env("COORD_V2_AGENT_PORT", "19637"));
        client = CoordClient.create(CoordConfig.builder()
                .agentHost(host)
                .agentPort(port)
                .requestTimeout(Duration.ofSeconds(10))
                .build());
        try {
            assertThat(client.tsoV2().getTimestamp()).isPositive();
            client.mqV2().listTopics(TOPIC, 1, "");
        } catch (RuntimeException e) {
            client.close(Duration.ZERO);
            throw new IllegalStateException(
                    "coord-agent v2 cluster with [mq_v2].enabled=true is not usable at "
                            + host + ":" + port
                            + ". Start one with `bash scripts/ci-java-it-cluster.sh start v2`"
                            + " (or set COORD_V2_AGENT_HOST/COORD_V2_AGENT_PORT).", e);
        }
    }

    @AfterAll
    static void tearDown() {
        if (client != null) {
            try {
                client.mqV2().deleteTopic(new cn.byteforce.coord.sdk.mq.v2.MqV2DeleteTopic(TOPIC, 0));
            } catch (RuntimeException ignored) {
                // best-effort cleanup; the cluster is disposable
            }
            client.close(Duration.ofSeconds(5));
        }
    }

    @Test
    void fullLifecyclePublishConsumeDlqUpdateDelete() throws Exception {
        MqV2Client mq = client.mqV2();

        // 1. Create (idempotent replay returns the same identity).
        MqV2TopicCreated created = mq.createTopic(TOPIC, 2, "req-" + RUN,
                new MqV2TopicConfig(3600, 0, 0, 0, 0));
        assertThat(created.topic()).isEqualTo(TOPIC);
        assertThat(created.partitionCount()).isEqualTo(2);
        assertThat(created.topicEpoch()).isPositive();
        MqV2TopicCreated replayed = mq.createTopic(TOPIC, 2, "req-" + RUN,
                new MqV2TopicConfig(3600, 0, 0, 0, 0));
        assertThat(replayed.topicId()).isEqualTo(created.topicId());
        assertThat(replayed.topicEpoch()).isEqualTo(created.topicEpoch());
        long epoch = created.topicEpoch();

        assertThat(mq.listTopics(TOPIC, 10, ""))
                .anySatisfy(summary -> assertThat(summary.topic()).isEqualTo(TOPIC));
        assertThat(mq.getTopicLeaders(TOPIC).partitions()).hasSize(2)
                .allSatisfy(partition -> assertThat(partition.leaderAddr()).isNotBlank());

        // 2. Join two members and let the group settle before publishing: the
        //    join of the second member only becomes visible after a rejoin, and
        //    polling to force it while messages exist would advance a cursor
        //    whose batch this member then no longer owns.
        MqV2Consumer left = MqV2Consumer.join(mq,
                MqV2ConsumerOptions.builder(GROUP, List.of(TOPIC)).waitMs(200).build());
        MqV2Consumer right = MqV2Consumer.join(mq,
                MqV2ConsumerOptions.builder(GROUP, List.of(TOPIC)).waitMs(200).build());
        MqV2PublishResult first;
        try (left; right) {
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while ((left.assignments().size() != 1 || right.assignments().size() != 1)
                    && System.nanoTime() < deadline) {
                left.poll();
                right.poll();
                Thread.sleep(50);
            }
            assertThat(left.assignments()).hasSize(1);
            assertThat(right.assignments()).hasSize(1);
            assertThat(left.assignments().get(0)).isNotEqualTo(right.assignments().get(0));

            // 3. Publish: one message on each partition, an idempotent replay and
            //    one batch on partition 0 (three unique rows in total).
            first = mq.publish(new MqV2Publish(TOPIC, 0, epoch,
                    bytes("k0"), bytes("p0"), "idem-0", Map.of("ce-id", "1"), "it"));
            assertThat(first.deduplicated()).isFalse();
            MqV2PublishResult replay = mq.publish(new MqV2Publish(TOPIC, 0, epoch,
                    bytes("k0"), bytes("p0"), "idem-0", Map.of("ce-id", "1"), "it"));
            assertThat(replay.deduplicated()).isTrue();
            assertThat(replay.offset()).isEqualTo(first.offset());
            mq.publish(new MqV2Publish(TOPIC, 1, epoch,
                    bytes("k1"), bytes("p1"), "", Map.of(), "it"));

            List<MqV2PublishResult> batch = mq.publishBatch(TOPIC, 0, epoch,
                    List.of(new MqV2BatchEntry(bytes("b1"), bytes("p2"), "", Map.of(), "it"),
                            new MqV2BatchEntry(bytes("b2"), bytes("p3"), "", Map.of(), "it")));
            assertThat(batch).hasSize(2);
            assertThat(batch.get(1).offset()).isEqualTo(first.offset() + 2);

            MqV2PollResult direct0 = mq.poll(new MqV2Poll(TOPIC, 0, epoch, 0L, 100, 0, 0));
            MqV2PollResult direct1 = mq.poll(new MqV2Poll(TOPIC, 1, epoch, 0L, 100, 0, 0));
            assertThat(direct0.messages()).hasSize(3);
            assertThat(direct1.messages()).hasSize(1);
            assertThat(direct0.messages().get(0).headers()).containsEntry("ce-id", "1");

            Set<String> consumed = new HashSet<>();
            int expected = 4;
            while (consumed.size() < expected && System.nanoTime() < deadline) {
                for (MqV2Consumer consumer : List.of(left, right)) {
                    for (MqV2Message message : consumer.poll()) {
                        consumer.ack(message);
                        consumed.add(message.partition() + "/" + message.offset());
                    }
                }
                Thread.sleep(50);
            }
            assertThat(consumed).hasSize(expected);

            MqV2GroupDescription description = mq.describeGroup(GROUP);
            assertThat(description.generation()).isPositive();
            assertThat(description.members()).hasSize(2);
            assertThat(description.partitions()).hasSize(2)
                    .allSatisfy(lag -> assertThat(lag.lag()).isZero());
            assertThat(mq.getCommittedOffsets(GROUP, List.of(
                    new MqV2TopicPartition(TOPIC, 0), new MqV2TopicPartition(TOPIC, 1))))
                    .allSatisfy(offset -> assertThat(offset.committed()).isTrue());
            assertThat(mq.listGroupsPage(GROUP, 10, "").items()).contains(GROUP);
        }

        // 4. DLQ: move offset 0 of partition 0, then poll it back with its reason.
        MqV2MoveToDlqResult moved = mq.moveToDlq(new MqV2MoveToDlq(
                TOPIC, 0, epoch, first.offset(), "poison-it", "decode failed in IT"));
        assertThat(moved.alreadyMoved()).isFalse();
        MqV2MoveToDlqResult movedAgain = mq.moveToDlq(new MqV2MoveToDlq(
                TOPIC, 0, epoch, first.offset(), "poison-it", "decode failed in IT"));
        assertThat(movedAgain.alreadyMoved()).isTrue();
        assertThat(movedAgain.dlqOffset()).isEqualTo(moved.dlqOffset());

        MqV2PollDlqResult dlq = mq.pollDlq(TOPIC, 0, epoch, 0L, 10);
        assertThat(dlq.messages()).hasSize(1);
        assertThat(dlq.messages().get(0).message().offset()).isEqualTo(first.offset());
        assertThat(dlq.messages().get(0).reason()).isEqualTo("poison-it");
        assertThat(dlq.messages().get(0).detail()).isEqualTo("decode failed in IT");
        assertThat(dlq.messages().get(0).message().payload()).isEqualTo(bytes("p0"));

        MqV2PollResult afterTombstone = mq.poll(new MqV2Poll(TOPIC, 0, epoch, 0L, 100, 0, 0));
        assertThat(afterTombstone.messages())
                .noneSatisfy(message -> assertThat(message.offset()).isEqualTo(first.offset()));
        assertThat(afterTombstone.messages()).hasSize(2);

        // 5. Update (epoch CAS), then delete and wait for reclamation.
        long updatedEpoch = mq.updateTopic(new MqV2UpdateTopic(TOPIC, epoch,
                new MqV2TopicConfig(7200, 0, 0, 0, 0))).topicEpoch();
        MqV2TopicDescription described = mq.describeTopic(TOPIC);
        assertThat(described.config().retentionSecs()).isEqualTo(7200);

        MqV2TopicDeleted deleted = mq.deleteTopic(
                new cn.byteforce.coord.sdk.mq.v2.MqV2DeleteTopic(TOPIC, updatedEpoch));
        assertThat(deleted.deleteId()).isNotBlank();

        CoordException gone = catchThrowableOfType(CoordException.class,
                () -> mq.describeTopic(TOPIC));
        assertThat(gone.getErrorCode()).isEqualTo(ErrorCode.TOPIC_NOT_FOUND);

        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        MqV2TopicDeletion progress = null;
        while (System.nanoTime() < deadline) {
            progress = mq.getTopicDeletion(deleted.deleteId());
            if (progress.state() == MqV2DeletionState.DONE) {
                break;
            }
            Thread.sleep(200);
        }
        assertThat(progress).isNotNull();
        assertThat(progress.state()).isEqualTo(MqV2DeletionState.DONE);
        assertThat(progress.partitionsReclaimed()).isEqualTo(2);
        assertThat(progress.messagesRemoved()).isPositive();

        // Same name can be recreated immediately, but it is a new identity.
        MqV2TopicCreated recreated = mq.createTopic(TOPIC, 1, "req2-" + RUN,
                MqV2TopicConfig.defaults());
        assertThat(recreated.topicId()).isNotEqualTo(created.topicId());
        mq.deleteTopic(new cn.byteforce.coord.sdk.mq.v2.MqV2DeleteTopic(TOPIC,
                recreated.topicEpoch()));
    }

    @Test
    void subscribeIsNotOnTheSdkSurface() {
        // Documented boundary: Subscribe is best-effort and not exposed.
        assertThat(MqV2Client.class.getMethods())
                .noneSatisfy(method -> assertThat(method.getName()).isEqualTo("subscribe"));
    }

    @Test
    void ackBatchAndAssignmentShapeAreUsable() {
        // The two-member lifecycle above is the real proof; this pins the batch
        // surface against the live cluster without depending on its ordering.
        String topic = TOPIC + "-batch";
        MqV2TopicCreated created = client.mqV2().createTopic(topic, 1, "req-batch-" + RUN,
                MqV2TopicConfig.defaults());
        try {
            client.mqV2().publish(new MqV2Publish(topic, 0, created.topicEpoch(),
                    bytes("k"), bytes("v"), "", Map.of(), "it"));
            MqV2PollResult polled = client.mqV2().poll(new MqV2Poll(
                    topic, 0, created.topicEpoch(), 0L, 10, 0, 0));
            assertThat(polled.messages()).hasSize(1);

            String group = GROUP + "-batch";
            MqV2Consumer consumer = MqV2Consumer.join(client.mqV2(),
                    MqV2ConsumerOptions.builder(group, List.of(topic)).build());
            try {
                List<MqV2AckResult> results = client.mqV2().ackBatch(group, consumer.memberId(),
                        consumer.generation(), List.of(new cn.byteforce.coord.sdk.mq.v2.MqV2Ack(
                                topic, 0, created.topicEpoch(), polled.messages().get(0).offset())));
                assertThat(results).hasSize(1);
                assertThat(results.get(0).committedOffset())
                        .isEqualTo(polled.messages().get(0).offset() + 1);
            } finally {
                consumer.close();
            }
        } finally {
            client.mqV2().deleteTopic(new cn.byteforce.coord.sdk.mq.v2.MqV2DeleteTopic(
                    topic, created.topicEpoch()));
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isEmpty() ? fallback : value;
    }
}
