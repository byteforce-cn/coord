package cn.byteforce.coord.sdk.mq.v2;

import cn.byteforce.coord.sdk.CoordException;
import cn.byteforce.coord.sdk.ErrorCode;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Consumer-helper discipline: epoch from routing, rejoin on a stale generation
 * before the next poll/ack, and offset-out-of-range handled only by the
 * configured policy (default: fail, never silently drop history).
 */
class MqV2ConsumerTest {

    @Test
    void joinResumesFromTheCommittedOffsetAndCarriesTheTopicEpoch() {
        FakeClient client = new FakeClient(assignment("orders", 0, 5));
        try (MqV2Consumer consumer = MqV2Consumer.join(client,
                MqV2ConsumerOptions.builder("g", List.of("orders")).build())) {
            assertThat(consumer.assignments()).containsExactly(new MqV2TopicPartition("orders", 0));

            List<MqV2Message> first = consumer.poll();
            assertThat(first).hasSize(1);
            assertThat(client.polls.get(0).startOffset()).isEqualTo(5L);
            assertThat(client.polls.get(0).topicEpoch()).isEqualTo(3);

            consumer.poll();
            assertThat(client.polls.get(1).startOffset()).isEqualTo(6L);
        }
    }

    @Test
    void aFreshAssignmentLeavesStartOffsetUnsetSoTheServerStartsAtEarliest() {
        FakeClient client = new FakeClient(assignment("orders", 0, 0));
        try (MqV2Consumer consumer = MqV2Consumer.join(client,
                MqV2ConsumerOptions.builder("g", List.of("orders")).build())) {
            consumer.poll();
            assertThat(client.polls.get(0).startOffset()).isNull();
        }
    }

    @Test
    void heartbeatRebalanceRejoinsBeforeTheNextPoll() throws Exception {
        FakeClient client = new FakeClient(assignment("orders", 0, 5));
        client.rebalanceOnHeartbeat = true;
        client.heartbeatLatch = new CountDownLatch(1);
        try (MqV2Consumer consumer = MqV2Consumer.join(client,
                MqV2ConsumerOptions.builder("g", List.of("orders"))
                        .heartbeatIntervalMs(20).build())) {
            assertThat(client.heartbeatLatch.await(2, TimeUnit.SECONDS)).isTrue();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (client.joins.size() < 2 && System.nanoTime() < deadline) {
                consumer.poll();
                Thread.sleep(10);
            }
            assertThat(client.joins).hasSize(2);
            assertThat(consumer.generation()).isEqualTo(2);
        }
    }

    @Test
    void aStaleAckRejoinsAndRetriesOnce() {
        FakeClient client = new FakeClient(assignment("orders", 0, 5));
        try (MqV2Consumer consumer = MqV2Consumer.join(client,
                MqV2ConsumerOptions.builder("g", List.of("orders")).build())) {
            MqV2Message message = consumer.poll().get(0);
            client.ackFailure = new CoordException(ErrorCode.GROUP_EPOCH_STALE, "stale");

            long committed = consumer.ack(message);
            assertThat(committed).isEqualTo(6);
            assertThat(client.acks).hasSize(2);
            assertThat(client.joins).hasSize(2);
        }
    }

    @Test
    void offsetOutOfRangeFailsByDefault() {
        FakeClient client = new FakeClient(assignment("orders", 0, 5));
        try (MqV2Consumer consumer = MqV2Consumer.join(client,
                MqV2ConsumerOptions.builder("g", List.of("orders")).build())) {
            client.pollFailure = new CoordException(ErrorCode.OFFSET_OUT_OF_RANGE, "gone");
            assertThatThrownBy(consumer::poll).isInstanceOfSatisfying(CoordException.class,
                    e -> assertThat(e.getErrorCode()).isEqualTo(ErrorCode.OFFSET_OUT_OF_RANGE));
        }
    }

    @Test
    void offsetOutOfRangeCanResetToEarliest() {
        FakeClient client = new FakeClient(assignment("orders", 0, 5));
        try (MqV2Consumer consumer = MqV2Consumer.join(client,
                MqV2ConsumerOptions.builder("g", List.of("orders"))
                        .resetPolicy(MqV2ResetPolicy.EARLIEST).build())) {
            client.pollFailure = new CoordException(ErrorCode.OFFSET_OUT_OF_RANGE, "gone");
            assertThat(consumer.poll()).hasSize(1);
            assertThat(client.polls).hasSize(2);
            assertThat(client.polls.get(1).startOffset()).isNull();
        }
    }

    @Test
    void offsetOutOfRangeCanResetToLatest() {
        FakeClient client = new FakeClient(assignment("orders", 0, 5));
        client.nextOffset = 100;
        try (MqV2Consumer consumer = MqV2Consumer.join(client,
                MqV2ConsumerOptions.builder("g", List.of("orders"))
                        .resetPolicy(MqV2ResetPolicy.LATEST).build())) {
            client.pollFailure = new CoordException(ErrorCode.OFFSET_OUT_OF_RANGE, "gone");
            assertThat(consumer.poll()).hasSize(1);
            assertThat(client.polls.get(1).startOffset()).isEqualTo(100L);
        }
    }

    @Test
    void closeLeavesOnceAndSwallowsLeaveFailures() {
        FakeClient client = new FakeClient(assignment("orders", 0, 5));
        client.leaveFailure = new CoordException(ErrorCode.UNAVAILABLE, "down");
        MqV2Consumer consumer = MqV2Consumer.join(client,
                MqV2ConsumerOptions.builder("g", List.of("orders")).build());

        consumer.close();
        consumer.close();
        assertThat(client.leaves.get()).isEqualTo(1);
        assertThat(client.joins).hasSize(1);
    }

    @Test
    void optionsRejectAnEmptyGroupOrTopicList() {
        assertThatThrownBy(() -> MqV2ConsumerOptions.builder("", List.of("orders")).build())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> MqV2ConsumerOptions.builder("g", List.of()).build())
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(MqV2ConsumerOptions.builder("g", List.of("orders")).build().resetPolicy())
                .isEqualTo(MqV2ResetPolicy.FAIL);
    }

    private static MqV2Assignment assignment(String topic, int partition, long committed) {
        return new MqV2Assignment(topic, partition, committed);
    }

    /** Hand-written client: records the request shapes the helper must produce. */
    private static final class FakeClient implements MqV2Client {

        private final MqV2Assignment assignment;
        final List<MqV2JoinGroup> joins = Collections.synchronizedList(new ArrayList<>());
        final List<MqV2Poll> polls = Collections.synchronizedList(new ArrayList<>());
        final List<MqV2Ack> acks = Collections.synchronizedList(new ArrayList<>());
        final AtomicInteger leaves = new AtomicInteger();
        volatile CoordException pollFailure;
        volatile CoordException ackFailure;
        volatile CoordException leaveFailure;
        volatile boolean rebalanceOnHeartbeat;
        volatile CountDownLatch heartbeatLatch;
        volatile long nextOffset = 6;

        FakeClient(MqV2Assignment assignment) {
            this.assignment = assignment;
        }

        @Override
        public MqV2TopicCreated createTopic(String topic, int partitions, String requestId,
                                            MqV2TopicConfig config) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MqV2TopicDescription describeTopic(String topic) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<MqV2TopicSummary> listTopics(String prefix, int pageSize, String pageToken) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Page<MqV2TopicSummary> listTopicsPage(String prefix, int pageSize, String pageToken) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MqV2TopicUpdated updateTopic(MqV2UpdateTopic update) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MqV2TopicDeleted deleteTopic(MqV2DeleteTopic delete) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MqV2TopicDeletion getTopicDeletion(String deleteId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MqV2Leaders getTopicLeaders(String topic) {
            return new MqV2Leaders(topic, 7, 3,
                    List.of(new MqV2PartitionInfo(assignment.partition(), 1, "10.0.0.1:50051",
                            0, nextOffset, 0)));
        }

        @Override
        public MqV2PublishResult publish(MqV2Publish publish) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<MqV2PublishResult> publishBatch(String topic, int partition, long topicEpoch,
                                                    List<MqV2BatchEntry> entries) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MqV2PollResult poll(MqV2Poll poll) {
            polls.add(poll);
            if (pollFailure != null) {
                CoordException failure = pollFailure;
                pollFailure = null;
                throw failure;
            }
            long start = poll.startOffset() == null ? 0 : poll.startOffset();
            MqV2Message message = new MqV2Message(poll.topic(), poll.partition(), start, 1L,
                    new byte[0], "body".getBytes(StandardCharsets.UTF_8), Map.of(), "", "", "");
            return new MqV2PollResult(List.of(message), start + 1, 0, nextOffset);
        }

        @Override
        public long ack(String group, String memberId, long generation, MqV2Ack ack) {
            acks.add(ack);
            if (ackFailure != null) {
                CoordException failure = ackFailure;
                ackFailure = null;
                throw failure;
            }
            return ack.offset() + 1;
        }

        @Override
        public List<MqV2AckResult> ackBatch(String group, String memberId, long generation,
                                            List<MqV2Ack> batch) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MqV2Joined joinGroup(MqV2JoinGroup join) {
            joins.add(join);
            return new MqV2Joined("m-" + joins.size(), joins.size(),
                    List.of(assignment), 3000, 30000);
        }

        @Override
        public MqV2Heartbeat heartbeat(String group, String memberId, long generation) {
            if (rebalanceOnHeartbeat) {
                CountDownLatch latch = heartbeatLatch;
                if (latch != null) {
                    latch.countDown();
                }
                return new MqV2Heartbeat(generation + 1, true);
            }
            return new MqV2Heartbeat(generation, false);
        }

        @Override
        public void leaveGroup(String group, String memberId, long generation) {
            leaves.incrementAndGet();
            if (leaveFailure != null) {
                throw leaveFailure;
            }
        }

        @Override
        public List<MqV2CommittedOffset> getCommittedOffsets(String group,
                                                             List<MqV2TopicPartition> partitions) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MqV2GroupDescription describeGroup(String group) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Page<String> listGroupsPage(String prefix, int pageSize, String pageToken) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MqV2MoveToDlqResult moveToDlq(MqV2MoveToDlq move) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MqV2PollDlqResult pollDlq(String topic, int partition, long topicEpoch,
                                         Long startOffset, int maxMessages) {
            throw new UnsupportedOperationException();
        }
    }
}
