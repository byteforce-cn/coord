package cn.byteforce.coord.sdk.mq.v2;

import cn.byteforce.coord.contracts.mq.v2.AckBatchRequest;
import cn.byteforce.coord.contracts.mq.v2.AckRequest;
import cn.byteforce.coord.contracts.mq.v2.AckResult;
import cn.byteforce.coord.contracts.mq.v2.CreateTopicRequest;
import cn.byteforce.coord.contracts.mq.v2.CreateTopicResponse;
import cn.byteforce.coord.contracts.mq.v2.DeleteTopicRequest;
import cn.byteforce.coord.contracts.mq.v2.DeleteTopicResponse;
import cn.byteforce.coord.contracts.mq.v2.DescribeGroupRequest;
import cn.byteforce.coord.contracts.mq.v2.DescribeGroupResponse;
import cn.byteforce.coord.contracts.mq.v2.DescribeTopicRequest;
import cn.byteforce.coord.contracts.mq.v2.DescribeTopicResponse;
import cn.byteforce.coord.contracts.mq.v2.GetCommittedOffsetsRequest;
import cn.byteforce.coord.contracts.mq.v2.GetCommittedOffsetsResponse;
import cn.byteforce.coord.contracts.mq.v2.GetTopicDeletionRequest;
import cn.byteforce.coord.contracts.mq.v2.GetTopicDeletionResponse;
import cn.byteforce.coord.contracts.mq.v2.GetTopicLeaderRequest;
import cn.byteforce.coord.contracts.mq.v2.GetTopicLeaderResponse;
import cn.byteforce.coord.contracts.mq.v2.GroupAssignment;
import cn.byteforce.coord.contracts.mq.v2.GroupMember;
import cn.byteforce.coord.contracts.mq.v2.GroupPartitionLag;
import cn.byteforce.coord.contracts.mq.v2.GroupState;
import cn.byteforce.coord.contracts.mq.v2.HeartbeatRequest;
import cn.byteforce.coord.contracts.mq.v2.HeartbeatResponse;
import cn.byteforce.coord.contracts.mq.v2.JoinGroupRequest;
import cn.byteforce.coord.contracts.mq.v2.JoinGroupResponse;
import cn.byteforce.coord.contracts.mq.v2.LeaveGroupRequest;
import cn.byteforce.coord.contracts.mq.v2.LeaveGroupResponse;
import cn.byteforce.coord.contracts.mq.v2.ListGroupsRequest;
import cn.byteforce.coord.contracts.mq.v2.ListGroupsResponse;
import cn.byteforce.coord.contracts.mq.v2.ListTopicsRequest;
import cn.byteforce.coord.contracts.mq.v2.ListTopicsResponse;
import cn.byteforce.coord.contracts.mq.v2.MQGrpc;
import cn.byteforce.coord.contracts.mq.v2.Message;
import cn.byteforce.coord.contracts.mq.v2.MoveToDlqRequest;
import cn.byteforce.coord.contracts.mq.v2.MoveToDlqResponse;
import cn.byteforce.coord.contracts.mq.v2.PartitionInfo;
import cn.byteforce.coord.contracts.mq.v2.PartitionLeader;
import cn.byteforce.coord.contracts.mq.v2.PollDlqRequest;
import cn.byteforce.coord.contracts.mq.v2.PollDlqResponse;
import cn.byteforce.coord.contracts.mq.v2.PollRequest;
import cn.byteforce.coord.contracts.mq.v2.PollResponse;
import cn.byteforce.coord.contracts.mq.v2.PublishBatchRequest;
import cn.byteforce.coord.contracts.mq.v2.PublishBatchResponse;
import cn.byteforce.coord.contracts.mq.v2.PublishRequest;
import cn.byteforce.coord.contracts.mq.v2.PublishResponse;
import cn.byteforce.coord.contracts.mq.v2.TopicConfig;
import cn.byteforce.coord.contracts.mq.v2.TopicDeletionState;
import cn.byteforce.coord.contracts.mq.v2.TopicSummary;
import cn.byteforce.coord.contracts.mq.v2.UpdateTopicRequest;
import cn.byteforce.coord.contracts.mq.v2.UpdateTopicResponse;
import cn.byteforce.coord.sdk.CoordClient;
import cn.byteforce.coord.sdk.CoordConfig;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Wire contract of {@link MqV2Client} against a real Netty gRPC server: every
 * RPC is sent once and both sides of the mapping are asserted. The fake service
 * is the agent-transport contract (the agent's {@code MqV2Proxy} forwards these
 * calls to the shard leader).
 */
class MqV2ClientTest {

    private Server server;
    private CoordClient client;
    private FakeMq fake;

    @BeforeEach
    void setUp() throws IOException {
        fake = new FakeMq();
        server = NettyServerBuilder.forPort(0).addService(fake).build().start();
        client = CoordClient.create(CoordConfig.builder()
                .agentHost("127.0.0.1")
                .agentPort(server.getPort())
                .requestTimeout(Duration.ofSeconds(5))
                .build());
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close(Duration.ZERO);
        }
        if (server != null) {
            server.shutdownNow();
        }
    }

    @Test
    void createAndDescribeTopicCarryTheEpochAndConfig() {
        MqV2TopicConfig config = new MqV2TopicConfig(3600, 1_000_000, 65536, 4_000_000, 7200);
        MqV2TopicCreated created = client.mqV2().createTopic("orders", 2, "req-1", config);

        CreateTopicRequest sent = (CreateTopicRequest) fake.requests.get("createTopic");
        assertThat(sent.getTopic()).isEqualTo("orders");
        assertThat(sent.getPartitions()).isEqualTo(2);
        assertThat(sent.getRequestId()).isEqualTo("req-1");
        assertThat(sent.getConfig().getRetentionSecs()).isEqualTo(3600);
        assertThat(sent.getConfig().getMaxMessageBytes()).isEqualTo(65536);
        assertThat(created.topicId()).isEqualTo(7);
        assertThat(created.topicEpoch()).isEqualTo(3);
        assertThat(created.config().dlqRetentionSecs()).isEqualTo(7200);

        MqV2TopicDescription described = client.mqV2().describeTopic("orders");
        assertThat(described.partitions()).hasSize(1);
        assertThat(described.partitions().get(0).leaderAddr()).isEqualTo("10.0.0.1:50051");
        assertThat(described.partitions().get(0).nextOffset()).isEqualTo(12);
        assertThat(described.createdAtMs()).isEqualTo(1700000000000L);
    }

    @Test
    void listTopicsExposesBothTheConvenienceAndThePagedShape() {
        List<MqV2TopicSummary> first = client.mqV2().listTopics("ord", 10, "");
        assertThat(first).hasSize(1);
        assertThat(first.get(0).topic()).isEqualTo("orders");

        Page<MqV2TopicSummary> page = client.mqV2().listTopicsPage("ord", 10, "");
        assertThat(page.nextPageToken()).isEqualTo("next");
        assertThat(page.items()).hasSize(1);

        ListTopicsRequest sent = (ListTopicsRequest) fake.requests.get("listTopics");
        assertThat(sent.getPrefix()).isEqualTo("ord");
        assertThat(sent.getPageSize()).isEqualTo(10);
    }

    @Test
    void updateDeleteAndDeletionProgressMapTheCasEpoch() {
        MqV2TopicUpdated updated = client.mqV2().updateTopic(
                new MqV2UpdateTopic("orders", 3, new MqV2TopicConfig(7200, 0, 0, 0, 0)));
        assertThat(updated.topicEpoch()).isEqualTo(4);
        assertThat(updated.config().retentionSecs()).isEqualTo(7200);
        UpdateTopicRequest update = (UpdateTopicRequest) fake.requests.get("updateTopic");
        assertThat(update.getExpectedTopicEpoch()).isEqualTo(3);

        MqV2TopicDeleted deleted = client.mqV2().deleteTopic(new MqV2DeleteTopic("orders", 4));
        assertThat(deleted.deleteId()).isEqualTo("del-1");
        assertThat(deleted.deletedTopicEpoch()).isEqualTo(5);
        DeleteTopicRequest delete = (DeleteTopicRequest) fake.requests.get("deleteTopic");
        assertThat(delete.getExpectedTopicEpoch()).isEqualTo(4);

        MqV2TopicDeletion progress = client.mqV2().getTopicDeletion("del-1");
        assertThat(progress.state()).isEqualTo(MqV2DeletionState.DONE);
        assertThat(progress.partitionsTotal()).isEqualTo(2);
        assertThat(progress.messagesRemoved()).isEqualTo(10);
    }

    @Test
    void publishCarriesBytesIdempotencyKeyHeadersAndEpoch() {
        MqV2PublishResult result = client.mqV2().publish(new MqV2Publish(
                "orders", 1, 3, bytes("k"), bytes("payload"), "idem-1",
                Map.of("ce-id", "9"), "producer-a"));
        assertThat(result.offset()).isEqualTo(41);
        assertThat(result.deduplicated()).isTrue();

        PublishRequest sent = (PublishRequest) fake.requests.get("publish");
        assertThat(sent.getTopicEpoch()).isEqualTo(3);
        assertThat(sent.getKey().toByteArray()).isEqualTo(bytes("k"));
        assertThat(sent.getPayload().toByteArray()).isEqualTo(bytes("payload"));
        assertThat(sent.getIdempotencyKey()).isEqualTo("idem-1");
        assertThat(sent.getHeadersMap()).containsEntry("ce-id", "9");
        assertThat(sent.getProducer()).isEqualTo("producer-a");
    }

    @Test
    void publishBatchKeepsEntryOrderAndReturnsOneResultPerEntry() {
        List<MqV2PublishResult> results = client.mqV2().publishBatch(
                "orders", 0, 3,
                List.of(new MqV2BatchEntry(bytes("a"), bytes("1"), "i1", Map.of(), "p"),
                        new MqV2BatchEntry(bytes("b"), bytes("2"), "", Map.of("h", "v"), "")));
        assertThat(results).hasSize(2);
        assertThat(results.get(1).offset()).isEqualTo(43);

        PublishBatchRequest sent = (PublishBatchRequest) fake.requests.get("publishBatch");
        assertThat(sent.getEntriesCount()).isEqualTo(2);
        assertThat(sent.getEntries(0).getKey().toByteArray()).isEqualTo(bytes("a"));
        assertThat(sent.getEntries(1).getHeadersMap()).containsEntry("h", "v");
    }

    @Test
    void pollMapsAnUnsetStartOffsetAndNeverDropsTheTrailerBounds() {
        MqV2PollResult result = client.mqV2().poll(
                new MqV2Poll("orders", 0, 3, null, 10, 4096, 100));
        assertThat(result.nextOffset()).isEqualTo(9);
        assertThat(result.earliestOffset()).isEqualTo(2);
        assertThat(result.latestOffset()).isEqualTo(9);
        assertThat(result.messages()).hasSize(1);
        assertThat(result.messages().get(0).payload()).isEqualTo(bytes("body"));
        assertThat(result.messages().get(0).headers()).containsEntry("ce-type", "created");

        PollRequest unset = (PollRequest) fake.requests.get("poll");
        assertThat(unset.hasStartOffset()).isFalse();
        assertThat(unset.getWaitMs()).isEqualTo(100);

        client.mqV2().poll(new MqV2Poll("orders", 0, 3, 0L, 10, 4096, 0));
        PollRequest explicitZero = (PollRequest) fake.requests.get("poll");
        assertThat(explicitZero.hasStartOffset()).isTrue();
        assertThat(explicitZero.getStartOffset()).isZero();
    }

    @Test
    void ackAndAckBatchCarryTheMemberGeneration() {
        long committed = client.mqV2().ack("g", "m-1", 9,
                new MqV2Ack("orders", 0, 3, 5));
        assertThat(committed).isEqualTo(6);
        AckRequest sent = (AckRequest) fake.requests.get("ack");
        assertThat(sent.getGroup()).isEqualTo("g");
        assertThat(sent.getMemberId()).isEqualTo("m-1");
        assertThat(sent.getGeneration()).isEqualTo(9);
        assertThat(sent.getTopicEpoch()).isEqualTo(3);
        assertThat(sent.getOffset()).isEqualTo(5);

        List<MqV2AckResult> results = client.mqV2().ackBatch("g", "m-1", 9,
                List.of(new MqV2Ack("orders", 0, 3, 5), new MqV2Ack("orders", 1, 3, 8)));
        assertThat(results).hasSize(2);
        assertThat(results.get(1).committedOffset()).isEqualTo(9);
        AckBatchRequest batch = (AckBatchRequest) fake.requests.get("ackBatch");
        assertThat(batch.getAcksCount()).isEqualTo(2);
    }

    @Test
    void joinHeartbeatLeaveAndCommittedOffsetsMapTheGroupSession() {
        MqV2Joined joined = client.mqV2().joinGroup(
                new MqV2JoinGroup("g", "", List.of("orders"), 30000));
        assertThat(joined.memberId()).isEqualTo("m-1");
        assertThat(joined.generation()).isEqualTo(9);
        assertThat(joined.assignments()).hasSize(1);
        assertThat(joined.assignments().get(0).committedOffset()).isEqualTo(4);
        JoinGroupRequest join = (JoinGroupRequest) fake.requests.get("joinGroup");
        assertThat(join.getTopicsList()).containsExactly("orders");
        assertThat(join.getSessionTimeoutMs()).isEqualTo(30000);

        MqV2Heartbeat heartbeat = client.mqV2().heartbeat("g", "m-1", 9);
        assertThat(heartbeat.generation()).isEqualTo(10);
        assertThat(heartbeat.rebalanceRequired()).isTrue();

        client.mqV2().leaveGroup("g", "m-1", 10);
        LeaveGroupRequest leave = (LeaveGroupRequest) fake.requests.get("leaveGroup");
        assertThat(leave.getMemberId()).isEqualTo("m-1");

        List<MqV2CommittedOffset> offsets = client.mqV2().getCommittedOffsets(
                "g", List.of(new MqV2TopicPartition("orders", 0),
                        new MqV2TopicPartition("orders", 1)));
        assertThat(offsets).hasSize(2);
        assertThat(offsets.get(0).committed()).isTrue();
        assertThat(offsets.get(0).offset()).isEqualTo(6);
        assertThat(offsets.get(1).committed()).isFalse();
        GetCommittedOffsetsRequest committed = (GetCommittedOffsetsRequest) fake.requests.get("getCommittedOffsets");
        assertThat(committed.getPartitionsCount()).isEqualTo(2);
    }

    @Test
    void describeGroupAndListGroupsExposeLagAndPagination() {
        MqV2GroupDescription description = client.mqV2().describeGroup("g");
        assertThat(description.state()).isEqualTo(MqV2GroupState.STABLE);
        assertThat(description.members()).hasSize(1);
        assertThat(description.members().get(0).topics()).containsExactly("orders");
        assertThat(description.partitions()).hasSize(1);
        assertThat(description.partitions().get(0).lag()).isEqualTo(3);

        Page<String> groups = client.mqV2().listGroupsPage("", 10, "");
        assertThat(groups.items()).containsExactly("g");
        assertThat(groups.nextPageToken()).isEqualTo("g-next");
    }

    @Test
    void moveToDlqAndPollDlqKeepTheOriginalOffsetAndReason() {
        MqV2MoveToDlqResult moved = client.mqV2().moveToDlq(
                new MqV2MoveToDlq("orders", 0, 3, 11, "poison", "decode failed"));
        assertThat(moved.dlqOffset()).isEqualTo(2);
        assertThat(moved.alreadyMoved()).isTrue();
        MoveToDlqRequest move = (MoveToDlqRequest) fake.requests.get("moveToDlq");
        assertThat(move.getReason()).isEqualTo("poison");
        assertThat(move.getDetail()).isEqualTo("decode failed");

        MqV2PollDlqResult dlq = client.mqV2().pollDlq("orders", 0, 3, null, 10);
        assertThat(dlq.nextDlqOffset()).isEqualTo(3);
        assertThat(dlq.messages()).hasSize(1);
        assertThat(dlq.messages().get(0).message().offset()).isEqualTo(11);
        assertThat(dlq.messages().get(0).reason()).isEqualTo("poison");
        assertThat(dlq.messages().get(0).detail()).isEqualTo("decode failed");
        PollDlqRequest pollDlq = (PollDlqRequest) fake.requests.get("pollDlq");
        assertThat(pollDlq.hasStartOffset()).isFalse();
    }

    @Test
    void topicLeadersExposeTheShardRoutingSnapshot() {
        MqV2Leaders leaders = client.mqV2().getTopicLeaders("orders");
        assertThat(leaders.topicId()).isEqualTo(7);
        assertThat(leaders.topicEpoch()).isEqualTo(3);
        assertThat(leaders.partitions()).hasSize(1);
        assertThat(leaders.partitions().get(0).shardId()).isEqualTo(1);
        GetTopicLeaderRequest sent = (GetTopicLeaderRequest) fake.requests.get("getTopicLeader");
        assertThat(sent.getTopic()).isEqualTo("orders");
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static ByteString bin(String s) {
        return ByteString.copyFrom(bytes(s));
    }

    private static TopicConfig config() {
        return TopicConfig.newBuilder().setRetentionSecs(3600).setMaxMessageBytes(65536)
                .setDlqRetentionSecs(7200).build();
    }

    private static final class FakeMq extends MQGrpc.MQImplBase {
        private final Map<String, Object> requests = new ConcurrentHashMap<>();

        @Override
        public void createTopic(CreateTopicRequest request,
                                StreamObserver<CreateTopicResponse> observer) {
            requests.put("createTopic", request);
            observer.onNext(CreateTopicResponse.newBuilder()
                    .setTopic(request.getTopic()).setTopicId(7).setTopicEpoch(3)
                    .setPartitionCount(request.getPartitions()).setConfig(config()).build());
            observer.onCompleted();
        }

        @Override
        public void describeTopic(DescribeTopicRequest request,
                                  StreamObserver<DescribeTopicResponse> observer) {
            requests.put("describeTopic", request);
            observer.onNext(DescribeTopicResponse.newBuilder()
                    .setTopic(request.getTopic()).setTopicId(7).setTopicEpoch(3)
                    .setPartitionCount(1).setConfig(config()).setCreatedAtMs(1700000000000L)
                    .addPartitions(PartitionInfo.newBuilder()
                            .setPartition(0).setShardId(1).setLeaderAddr("10.0.0.1:50051")
                            .setEarliestOffset(2).setNextOffset(12).setLogBytes(4096))
                    .build());
            observer.onCompleted();
        }

        @Override
        public void listTopics(ListTopicsRequest request, StreamObserver<ListTopicsResponse> observer) {
            requests.put("listTopics", request);
            observer.onNext(ListTopicsResponse.newBuilder()
                    .addTopics(TopicSummary.newBuilder()
                            .setTopic("orders").setTopicId(7).setTopicEpoch(3)
                            .setPartitionCount(1).setConfig(config()))
                    .setNextPageToken("next").build());
            observer.onCompleted();
        }

        @Override
        public void updateTopic(UpdateTopicRequest request, StreamObserver<UpdateTopicResponse> observer) {
            requests.put("updateTopic", request);
            observer.onNext(UpdateTopicResponse.newBuilder()
                    .setTopic(request.getTopic()).setTopicEpoch(4).setConfig(request.getConfig()).build());
            observer.onCompleted();
        }

        @Override
        public void deleteTopic(DeleteTopicRequest request, StreamObserver<DeleteTopicResponse> observer) {
            requests.put("deleteTopic", request);
            observer.onNext(DeleteTopicResponse.newBuilder()
                    .setDeleteId("del-1").setDeletedTopicEpoch(5).build());
            observer.onCompleted();
        }

        @Override
        public void getTopicDeletion(GetTopicDeletionRequest request,
                                     StreamObserver<GetTopicDeletionResponse> observer) {
            requests.put("getTopicDeletion", request);
            observer.onNext(GetTopicDeletionResponse.newBuilder()
                    .setDeleteId(request.getDeleteId()).setTopic("orders").setTopicId(7)
                    .setState(TopicDeletionState.TOPIC_DELETION_STATE_DONE)
                    .setPartitionsTotal(2).setPartitionsReclaimed(2).setMessagesRemoved(10)
                    .setBytesReclaimed(4096).build());
            observer.onCompleted();
        }

        @Override
        public void publish(PublishRequest request, StreamObserver<PublishResponse> observer) {
            requests.put("publish", request);
            observer.onNext(PublishResponse.newBuilder()
                    .setOffset(41).setTimestampMs(1700000000000L).setDeduplicated(true).build());
            observer.onCompleted();
        }

        @Override
        public void publishBatch(PublishBatchRequest request,
                                 StreamObserver<PublishBatchResponse> observer) {
            requests.put("publishBatch", request);
            PublishBatchResponse.Builder builder = PublishBatchResponse.newBuilder();
            for (int i = 0; i < request.getEntriesCount(); i++) {
                builder.addResults(PublishResponse.newBuilder()
                        .setOffset(42 + i).setTimestampMs(1700000000000L));
            }
            observer.onNext(builder.build());
            observer.onCompleted();
        }

        @Override
        public void poll(PollRequest request, StreamObserver<PollResponse> observer) {
            requests.put("poll", request);
            observer.onNext(PollResponse.newBuilder()
                    .addMessages(Message.newBuilder()
                            .setTopic(request.getTopic()).setPartition(request.getPartition())
                            .setOffset(8).setTimestampMs(1700000000000L)
                            .setKey(bin("k")).setPayload(bin("body"))
                            .putHeaders("ce-type", "created").setProducer("p"))
                    .setNextOffset(9).setEarliestOffset(2).setLatestOffset(9).build());
            observer.onCompleted();
        }

        @Override
        public void ack(AckRequest request, StreamObserver<cn.byteforce.coord.contracts.mq.v2.AckResponse> observer) {
            requests.put("ack", request);
            observer.onNext(cn.byteforce.coord.contracts.mq.v2.AckResponse.newBuilder()
                    .setCommittedOffset(request.getOffset() + 1).build());
            observer.onCompleted();
        }

        @Override
        public void ackBatch(AckBatchRequest request,
                             StreamObserver<cn.byteforce.coord.contracts.mq.v2.AckBatchResponse> observer) {
            requests.put("ackBatch", request);
            cn.byteforce.coord.contracts.mq.v2.AckBatchResponse.Builder builder =
                    cn.byteforce.coord.contracts.mq.v2.AckBatchResponse.newBuilder();
            for (cn.byteforce.coord.contracts.mq.v2.AckEntry ack : request.getAcksList()) {
                builder.addResults(AckResult.newBuilder()
                        .setTopic(ack.getTopic()).setPartition(ack.getPartition())
                        .setCommittedOffset(ack.getOffset() + 1));
            }
            observer.onNext(builder.build());
            observer.onCompleted();
        }

        @Override
        public void joinGroup(JoinGroupRequest request, StreamObserver<JoinGroupResponse> observer) {
            requests.put("joinGroup", request);
            observer.onNext(JoinGroupResponse.newBuilder()
                    .setMemberId("m-1").setGeneration(9)
                    .addAssignments(GroupAssignment.newBuilder()
                            .setTopic("orders").setPartition(0).setCommittedOffset(4))
                    .setHeartbeatIntervalMs(3000).setSessionTimeoutMs(30000).build());
            observer.onCompleted();
        }

        @Override
        public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> observer) {
            requests.put("heartbeat", request);
            observer.onNext(HeartbeatResponse.newBuilder()
                    .setGeneration(10).setRebalanceRequired(true).build());
            observer.onCompleted();
        }

        @Override
        public void leaveGroup(LeaveGroupRequest request, StreamObserver<LeaveGroupResponse> observer) {
            requests.put("leaveGroup", request);
            observer.onNext(LeaveGroupResponse.getDefaultInstance());
            observer.onCompleted();
        }

        @Override
        public void getCommittedOffsets(GetCommittedOffsetsRequest request,
                                        StreamObserver<GetCommittedOffsetsResponse> observer) {
            requests.put("getCommittedOffsets", request);
            observer.onNext(GetCommittedOffsetsResponse.newBuilder()
                    .addOffsets(cn.byteforce.coord.contracts.mq.v2.CommittedOffset.newBuilder()
                            .setTopic("orders").setPartition(0).setOffset(6).setCommitted(true))
                    .addOffsets(cn.byteforce.coord.contracts.mq.v2.CommittedOffset.newBuilder()
                            .setTopic("orders").setPartition(1).setOffset(0).setCommitted(false))
                    .build());
            observer.onCompleted();
        }

        @Override
        public void describeGroup(DescribeGroupRequest request,
                                  StreamObserver<DescribeGroupResponse> observer) {
            requests.put("describeGroup", request);
            observer.onNext(DescribeGroupResponse.newBuilder()
                    .setGroup(request.getGroup()).setGeneration(9)
                    .setState(GroupState.GROUP_STATE_STABLE)
                    .addMembers(GroupMember.newBuilder().setMemberId("m-1")
                            .addTopics("orders")
                            .addAssignments(GroupAssignment.newBuilder()
                                    .setTopic("orders").setPartition(0).setCommittedOffset(6))
                            .setLastHeartbeatMs(1700000000000L))
                    .addPartitions(GroupPartitionLag.newBuilder()
                            .setTopic("orders").setPartition(0)
                            .setCommittedOffset(6).setNextOffset(9).setLag(3))
                    .build());
            observer.onCompleted();
        }

        @Override
        public void listGroups(ListGroupsRequest request, StreamObserver<ListGroupsResponse> observer) {
            requests.put("listGroups", request);
            observer.onNext(ListGroupsResponse.newBuilder()
                    .addGroups("g").setNextPageToken("g-next").build());
            observer.onCompleted();
        }

        @Override
        public void moveToDlq(MoveToDlqRequest request, StreamObserver<MoveToDlqResponse> observer) {
            requests.put("moveToDlq", request);
            observer.onNext(MoveToDlqResponse.newBuilder()
                    .setDlqOffset(2).setAlreadyMoved(true).build());
            observer.onCompleted();
        }

        @Override
        public void pollDlq(PollDlqRequest request, StreamObserver<PollDlqResponse> observer) {
            requests.put("pollDlq", request);
            observer.onNext(PollDlqResponse.newBuilder()
                    .addMessages(Message.newBuilder()
                            .setTopic(request.getTopic()).setPartition(request.getPartition())
                            .setOffset(11).setTimestampMs(1700000000000L)
                            .setPayload(bin("body"))
                            .setDlqReason("poison").setDlqDetail("decode failed"))
                    .setNextDlqOffset(3).build());
            observer.onCompleted();
        }

        @Override
        public void getTopicLeader(GetTopicLeaderRequest request,
                                   StreamObserver<GetTopicLeaderResponse> observer) {
            requests.put("getTopicLeader", request);
            observer.onNext(GetTopicLeaderResponse.newBuilder()
                    .setTopic(request.getTopic()).setTopicId(7).setTopicEpoch(3)
                    .addPartitions(PartitionLeader.newBuilder()
                            .setPartition(0).setShardId(1).setLeaderAddr("10.0.0.1:50051")
                            .setEarliestOffset(2).setNextOffset(12))
                    .build());
            observer.onCompleted();
        }
    }
}
