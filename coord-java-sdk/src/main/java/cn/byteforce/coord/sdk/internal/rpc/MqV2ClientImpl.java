package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.contracts.mq.v2.AckBatchRequest;
import cn.byteforce.coord.contracts.mq.v2.AckBatchResponse;
import cn.byteforce.coord.contracts.mq.v2.AckEntry;
import cn.byteforce.coord.contracts.mq.v2.AckRequest;
import cn.byteforce.coord.contracts.mq.v2.BatchEntry;
import cn.byteforce.coord.contracts.mq.v2.CreateTopicRequest;
import cn.byteforce.coord.contracts.mq.v2.DeleteTopicRequest;
import cn.byteforce.coord.contracts.mq.v2.DescribeGroupRequest;
import cn.byteforce.coord.contracts.mq.v2.DescribeGroupResponse;
import cn.byteforce.coord.contracts.mq.v2.DescribeTopicRequest;
import cn.byteforce.coord.contracts.mq.v2.GetCommittedOffsetsRequest;
import cn.byteforce.coord.contracts.mq.v2.GetTopicDeletionRequest;
import cn.byteforce.coord.contracts.mq.v2.GetTopicLeaderRequest;
import cn.byteforce.coord.contracts.mq.v2.GroupAssignment;
import cn.byteforce.coord.contracts.mq.v2.GroupMember;
import cn.byteforce.coord.contracts.mq.v2.GroupPartitionLag;
import cn.byteforce.coord.contracts.mq.v2.GroupState;
import cn.byteforce.coord.contracts.mq.v2.HeartbeatRequest;
import cn.byteforce.coord.contracts.mq.v2.JoinGroupRequest;
import cn.byteforce.coord.contracts.mq.v2.LeaveGroupRequest;
import cn.byteforce.coord.contracts.mq.v2.ListGroupsRequest;
import cn.byteforce.coord.contracts.mq.v2.ListTopicsRequest;
import cn.byteforce.coord.contracts.mq.v2.MQGrpc;
import cn.byteforce.coord.contracts.mq.v2.MoveToDlqRequest;
import cn.byteforce.coord.contracts.mq.v2.PartitionInfo;
import cn.byteforce.coord.contracts.mq.v2.PollDlqRequest;
import cn.byteforce.coord.contracts.mq.v2.PollRequest;
import cn.byteforce.coord.contracts.mq.v2.PublishBatchRequest;
import cn.byteforce.coord.contracts.mq.v2.PublishRequest;
import cn.byteforce.coord.contracts.mq.v2.PublishResponse;
import cn.byteforce.coord.contracts.mq.v2.TopicConfig;
import cn.byteforce.coord.contracts.mq.v2.TopicDeletionState;
import cn.byteforce.coord.contracts.mq.v2.TopicPartition;
import cn.byteforce.coord.contracts.mq.v2.TopicSummary;
import cn.byteforce.coord.contracts.mq.v2.UpdateTopicRequest;
import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.sdk.mq.v2.MqV2Ack;
import cn.byteforce.coord.sdk.mq.v2.MqV2AckResult;
import cn.byteforce.coord.sdk.mq.v2.MqV2Assignment;
import cn.byteforce.coord.sdk.mq.v2.MqV2BatchEntry;
import cn.byteforce.coord.sdk.mq.v2.MqV2Client;
import cn.byteforce.coord.sdk.mq.v2.MqV2CommittedOffset;
import cn.byteforce.coord.sdk.mq.v2.MqV2DeleteTopic;
import cn.byteforce.coord.sdk.mq.v2.MqV2DeletionState;
import cn.byteforce.coord.sdk.mq.v2.MqV2DlqMessage;
import cn.byteforce.coord.sdk.mq.v2.MqV2GroupDescription;
import cn.byteforce.coord.sdk.mq.v2.MqV2GroupMember;
import cn.byteforce.coord.sdk.mq.v2.MqV2GroupPartitionLag;
import cn.byteforce.coord.sdk.mq.v2.MqV2GroupState;
import cn.byteforce.coord.sdk.mq.v2.MqV2Heartbeat;
import cn.byteforce.coord.sdk.mq.v2.MqV2JoinGroup;
import cn.byteforce.coord.sdk.mq.v2.MqV2Joined;
import cn.byteforce.coord.sdk.mq.v2.MqV2Leaders;
import cn.byteforce.coord.sdk.mq.v2.MqV2Message;
import cn.byteforce.coord.sdk.mq.v2.MqV2MoveToDlq;
import cn.byteforce.coord.sdk.mq.v2.MqV2MoveToDlqResult;
import cn.byteforce.coord.sdk.mq.v2.MqV2PartitionInfo;
import cn.byteforce.coord.sdk.mq.v2.MqV2Poll;
import cn.byteforce.coord.sdk.mq.v2.MqV2PollDlqResult;
import cn.byteforce.coord.sdk.mq.v2.MqV2PollResult;
import cn.byteforce.coord.sdk.mq.v2.MqV2Publish;
import cn.byteforce.coord.sdk.mq.v2.MqV2PublishResult;
import cn.byteforce.coord.sdk.mq.v2.MqV2TopicConfig;
import cn.byteforce.coord.sdk.mq.v2.MqV2TopicCreated;
import cn.byteforce.coord.sdk.mq.v2.MqV2TopicDeleted;
import cn.byteforce.coord.sdk.mq.v2.MqV2TopicDeletion;
import cn.byteforce.coord.sdk.mq.v2.MqV2TopicDescription;
import cn.byteforce.coord.sdk.mq.v2.MqV2TopicPartition;
import cn.byteforce.coord.sdk.mq.v2.MqV2TopicSummary;
import cn.byteforce.coord.sdk.mq.v2.MqV2TopicUpdated;
import cn.byteforce.coord.sdk.mq.v2.MqV2UpdateTopic;
import cn.byteforce.coord.sdk.mq.v2.Page;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * {@link MqV2Client} backed by {@code coord.mq.v2} calls to the Coord Agent.
 *
 * <p>The agent's {@code MqV2Proxy} owns leader routing (hint cache + retry);
 * this class only maps the wire and forwards stable error codes. It does not
 * re-route, retry a write against another node, or clamp offsets — all three
 * would hide a contract violation from the caller.
 */
public final class MqV2ClientImpl extends AgentRpcClient implements MqV2Client {

    private final CoordConfig config;

    public MqV2ClientImpl(AgentChannelManager channelManager, ErrorMapper errorMapper,
                          RetryTemplate retryTemplate, ObservabilityProvider observability,
                          CoordConfig config) {
        super(channelManager, errorMapper, retryTemplate, observability);
        this.config = config;
    }

    private long deadlineMs() {
        return config.getRequestTimeout().toMillis();
    }

    private MQGrpc.MQBlockingStub stub(io.grpc.ManagedChannel channel) {
        return MQGrpc.newBlockingStub(channel)
                .withDeadlineAfter(deadlineMs(), TimeUnit.MILLISECONDS);
    }

    @Override
    public MqV2TopicCreated createTopic(String topic, int partitions, String requestId,
                                        MqV2TopicConfig config) {
        CreateTopicRequest request = CreateTopicRequest.newBuilder()
                .setTopic(topic)
                .setPartitions(partitions)
                .setRequestId(requestId == null ? "" : requestId)
                .setConfig(toProto(config))
                .build();
        cn.byteforce.coord.contracts.mq.v2.CreateTopicResponse response = callWithRetry(
                (ch, req) -> stub(ch).createTopic((CreateTopicRequest) req),
                request, "mqV2.createTopic");
        return new MqV2TopicCreated(response.getTopic(), response.getTopicId(),
                response.getTopicEpoch(), response.getPartitionCount(),
                toSdk(response.getConfig()));
    }

    @Override
    public MqV2TopicDescription describeTopic(String topic) {
        DescribeTopicRequest request = DescribeTopicRequest.newBuilder().setTopic(topic).build();
        cn.byteforce.coord.contracts.mq.v2.DescribeTopicResponse response = callWithRetry(
                (ch, req) -> stub(ch).describeTopic((DescribeTopicRequest) req),
                request, "mqV2.describeTopic");
        List<MqV2PartitionInfo> partitions = new ArrayList<>(response.getPartitionsCount());
        for (PartitionInfo info : response.getPartitionsList()) {
            partitions.add(toSdk(info));
        }
        return new MqV2TopicDescription(response.getTopic(), response.getTopicId(),
                response.getTopicEpoch(), response.getPartitionCount(),
                toSdk(response.getConfig()), response.getCreatedAtMs(), partitions);
    }

    @Override
    public List<MqV2TopicSummary> listTopics(String prefix, int pageSize, String pageToken) {
        return listTopicsPage(prefix, pageSize, pageToken).items();
    }

    @Override
    public Page<MqV2TopicSummary> listTopicsPage(String prefix, int pageSize, String pageToken) {
        ListTopicsRequest request = ListTopicsRequest.newBuilder()
                .setPrefix(prefix == null ? "" : prefix)
                .setPageSize(pageSize)
                .setPageToken(pageToken == null ? "" : pageToken)
                .build();
        cn.byteforce.coord.contracts.mq.v2.ListTopicsResponse response = callWithRetry(
                (ch, req) -> stub(ch).listTopics((ListTopicsRequest) req),
                request, "mqV2.listTopics");
        List<MqV2TopicSummary> topics = new ArrayList<>(response.getTopicsCount());
        for (TopicSummary summary : response.getTopicsList()) {
            topics.add(new MqV2TopicSummary(summary.getTopic(), summary.getTopicId(),
                    summary.getTopicEpoch(), summary.getPartitionCount(),
                    toSdk(summary.getConfig())));
        }
        return new Page<>(topics, response.getNextPageToken());
    }

    @Override
    public MqV2TopicUpdated updateTopic(MqV2UpdateTopic update) {
        UpdateTopicRequest request = UpdateTopicRequest.newBuilder()
                .setTopic(update.topic())
                .setExpectedTopicEpoch(update.expectedTopicEpoch())
                .setConfig(toProto(update.config()))
                .build();
        cn.byteforce.coord.contracts.mq.v2.UpdateTopicResponse response = callWithRetry(
                (ch, req) -> stub(ch).updateTopic((UpdateTopicRequest) req),
                request, "mqV2.updateTopic");
        return new MqV2TopicUpdated(response.getTopic(), response.getTopicEpoch(),
                toSdk(response.getConfig()));
    }

    @Override
    public MqV2TopicDeleted deleteTopic(MqV2DeleteTopic delete) {
        DeleteTopicRequest request = DeleteTopicRequest.newBuilder()
                .setTopic(delete.topic())
                .setExpectedTopicEpoch(delete.expectedTopicEpoch())
                .build();
        cn.byteforce.coord.contracts.mq.v2.DeleteTopicResponse response = callWithRetry(
                (ch, req) -> stub(ch).deleteTopic((DeleteTopicRequest) req),
                request, "mqV2.deleteTopic");
        return new MqV2TopicDeleted(response.getDeleteId(), response.getDeletedTopicEpoch());
    }

    @Override
    public MqV2TopicDeletion getTopicDeletion(String deleteId) {
        GetTopicDeletionRequest request = GetTopicDeletionRequest.newBuilder()
                .setDeleteId(deleteId == null ? "" : deleteId)
                .build();
        cn.byteforce.coord.contracts.mq.v2.GetTopicDeletionResponse response = callWithRetry(
                (ch, req) -> stub(ch).getTopicDeletion((GetTopicDeletionRequest) req),
                request, "mqV2.getTopicDeletion");
        return new MqV2TopicDeletion(response.getDeleteId(), response.getTopic(),
                response.getTopicId(), toSdk(response.getState()),
                response.getPartitionsTotal(), response.getPartitionsReclaimed(),
                response.getMessagesRemoved(), response.getBytesReclaimed());
    }

    @Override
    public MqV2Leaders getTopicLeaders(String topic) {
        GetTopicLeaderRequest request = GetTopicLeaderRequest.newBuilder().setTopic(topic).build();
        cn.byteforce.coord.contracts.mq.v2.GetTopicLeaderResponse response = callWithRetry(
                (ch, req) -> stub(ch).getTopicLeader((GetTopicLeaderRequest) req),
                request, "mqV2.getTopicLeader");
        List<MqV2PartitionInfo> partitions = new ArrayList<>(response.getPartitionsCount());
        for (cn.byteforce.coord.contracts.mq.v2.PartitionLeader leader : response.getPartitionsList()) {
            partitions.add(new MqV2PartitionInfo(leader.getPartition(), leader.getShardId(),
                    leader.getLeaderAddr(), leader.getEarliestOffset(), leader.getNextOffset(),
                    0));
        }
        return new MqV2Leaders(response.getTopic(), response.getTopicId(),
                response.getTopicEpoch(), partitions);
    }

    @Override
    public MqV2PublishResult publish(MqV2Publish publish) {
        PublishRequest request = PublishRequest.newBuilder()
                .setTopic(publish.topic())
                .setPartition(publish.partition())
                .setTopicEpoch(publish.topicEpoch())
                .setKey(bytes(publish.key()))
                .setPayload(bytes(publish.payload()))
                .setIdempotencyKey(publish.idempotencyKey())
                .putAllHeaders(publish.headers())
                .setProducer(publish.producer())
                .build();
        PublishResponse response = callWithRetry(
                (ch, req) -> stub(ch).publish((PublishRequest) req),
                request, "mqV2.publish");
        return toSdk(response);
    }

    @Override
    public List<MqV2PublishResult> publishBatch(String topic, int partition, long topicEpoch,
                                                List<MqV2BatchEntry> entries) {
        PublishBatchRequest.Builder builder = PublishBatchRequest.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .setTopicEpoch(topicEpoch);
        for (MqV2BatchEntry entry : entries) {
            builder.addEntries(BatchEntry.newBuilder()
                    .setKey(bytes(entry.key()))
                    .setPayload(bytes(entry.payload()))
                    .setIdempotencyKey(entry.idempotencyKey())
                    .putAllHeaders(entry.headers())
                    .setProducer(entry.producer()));
        }
        PublishBatchRequest request = builder.build();
        cn.byteforce.coord.contracts.mq.v2.PublishBatchResponse response = callWithRetry(
                (ch, req) -> stub(ch).publishBatch((PublishBatchRequest) req),
                request, "mqV2.publishBatch");
        List<MqV2PublishResult> results = new ArrayList<>(response.getResultsCount());
        for (PublishResponse result : response.getResultsList()) {
            results.add(toSdk(result));
        }
        return results;
    }

    @Override
    public MqV2PollResult poll(MqV2Poll poll) {
        PollRequest.Builder builder = PollRequest.newBuilder()
                .setTopic(poll.topic())
                .setPartition(poll.partition())
                .setTopicEpoch(poll.topicEpoch())
                .setMaxMessages(poll.maxMessages())
                .setMaxBytes(poll.maxBytes())
                .setWaitMs(poll.waitMs());
        if (poll.startOffset() != null) {
            builder.setStartOffset(poll.startOffset());
        }
        PollRequest request = builder.build();
        cn.byteforce.coord.contracts.mq.v2.PollResponse response = callWithRetry(
                (ch, req) -> stub(ch).poll((PollRequest) req),
                request, "mqV2.poll");
        return new MqV2PollResult(toSdkMessages(response.getMessagesList()),
                response.getNextOffset(), response.getEarliestOffset(),
                response.getLatestOffset());
    }

    @Override
    public long ack(String group, String memberId, long generation, MqV2Ack ack) {
        AckRequest request = AckRequest.newBuilder()
                .setTopic(ack.topic())
                .setPartition(ack.partition())
                .setTopicEpoch(ack.topicEpoch())
                .setGroup(group == null ? "" : group)
                .setMemberId(memberId == null ? "" : memberId)
                .setGeneration(generation)
                .setOffset(ack.offset())
                .build();
        cn.byteforce.coord.contracts.mq.v2.AckResponse response = callWithRetry(
                (ch, req) -> stub(ch).ack((AckRequest) req),
                request, "mqV2.ack");
        return response.getCommittedOffset();
    }

    @Override
    public List<MqV2AckResult> ackBatch(String group, String memberId, long generation,
                                        List<MqV2Ack> acks) {
        AckBatchRequest.Builder builder = AckBatchRequest.newBuilder()
                .setGroup(group == null ? "" : group)
                .setMemberId(memberId == null ? "" : memberId)
                .setGeneration(generation);
        for (MqV2Ack ack : acks) {
            builder.addAcks(AckEntry.newBuilder()
                    .setTopic(ack.topic())
                    .setPartition(ack.partition())
                    .setTopicEpoch(ack.topicEpoch())
                    .setOffset(ack.offset()));
        }
        AckBatchRequest request = builder.build();
        AckBatchResponse response = callWithRetry(
                (ch, req) -> stub(ch).ackBatch((AckBatchRequest) req),
                request, "mqV2.ackBatch");
        List<MqV2AckResult> results = new ArrayList<>(response.getResultsCount());
        for (cn.byteforce.coord.contracts.mq.v2.AckResult result : response.getResultsList()) {
            results.add(new MqV2AckResult(result.getTopic(), result.getPartition(),
                    result.getCommittedOffset()));
        }
        return results;
    }

    @Override
    public MqV2Joined joinGroup(MqV2JoinGroup join) {
        JoinGroupRequest request = JoinGroupRequest.newBuilder()
                .setGroup(join.group())
                .setMemberId(join.memberId())
                .addAllTopics(join.topics())
                .setSessionTimeoutMs(join.sessionTimeoutMs())
                .build();
        cn.byteforce.coord.contracts.mq.v2.JoinGroupResponse response = callWithRetry(
                (ch, req) -> stub(ch).joinGroup((JoinGroupRequest) req),
                request, "mqV2.joinGroup");
        List<MqV2Assignment> assignments = new ArrayList<>(response.getAssignmentsCount());
        for (GroupAssignment assignment : response.getAssignmentsList()) {
            assignments.add(new MqV2Assignment(assignment.getTopic(), assignment.getPartition(),
                    assignment.getCommittedOffset()));
        }
        return new MqV2Joined(response.getMemberId(), response.getGeneration(), assignments,
                response.getHeartbeatIntervalMs(), response.getSessionTimeoutMs());
    }

    @Override
    public MqV2Heartbeat heartbeat(String group, String memberId, long generation) {
        HeartbeatRequest request = HeartbeatRequest.newBuilder()
                .setGroup(group == null ? "" : group)
                .setMemberId(memberId == null ? "" : memberId)
                .setGeneration(generation)
                .build();
        cn.byteforce.coord.contracts.mq.v2.HeartbeatResponse response = callWithRetry(
                (ch, req) -> stub(ch).heartbeat((HeartbeatRequest) req),
                request, "mqV2.heartbeat");
        return new MqV2Heartbeat(response.getGeneration(), response.getRebalanceRequired());
    }

    @Override
    public void leaveGroup(String group, String memberId, long generation) {
        LeaveGroupRequest request = LeaveGroupRequest.newBuilder()
                .setGroup(group == null ? "" : group)
                .setMemberId(memberId == null ? "" : memberId)
                .setGeneration(generation)
                .build();
        callWithRetry((ch, req) -> stub(ch).leaveGroup((LeaveGroupRequest) req),
                request, "mqV2.leaveGroup");
    }

    @Override
    public List<MqV2CommittedOffset> getCommittedOffsets(String group,
                                                         List<MqV2TopicPartition> partitions) {
        GetCommittedOffsetsRequest.Builder builder = GetCommittedOffsetsRequest.newBuilder()
                .setGroup(group == null ? "" : group);
        for (MqV2TopicPartition partition : partitions) {
            builder.addPartitions(TopicPartition.newBuilder()
                    .setTopic(partition.topic())
                    .setPartition(partition.partition()));
        }
        GetCommittedOffsetsRequest request = builder.build();
        cn.byteforce.coord.contracts.mq.v2.GetCommittedOffsetsResponse response = callWithRetry(
                (ch, req) -> stub(ch).getCommittedOffsets((GetCommittedOffsetsRequest) req),
                request, "mqV2.getCommittedOffsets");
        List<MqV2CommittedOffset> offsets = new ArrayList<>(response.getOffsetsCount());
        for (cn.byteforce.coord.contracts.mq.v2.CommittedOffset offset : response.getOffsetsList()) {
            offsets.add(new MqV2CommittedOffset(offset.getTopic(), offset.getPartition(),
                    offset.getOffset(), offset.getCommitted()));
        }
        return offsets;
    }

    @Override
    public MqV2GroupDescription describeGroup(String group) {
        DescribeGroupRequest request = DescribeGroupRequest.newBuilder()
                .setGroup(group == null ? "" : group)
                .build();
        DescribeGroupResponse response = callWithRetry(
                (ch, req) -> stub(ch).describeGroup((DescribeGroupRequest) req),
                request, "mqV2.describeGroup");
        List<MqV2GroupMember> members = new ArrayList<>(response.getMembersCount());
        for (GroupMember member : response.getMembersList()) {
            List<MqV2Assignment> assignments = new ArrayList<>(member.getAssignmentsCount());
            for (GroupAssignment assignment : member.getAssignmentsList()) {
                assignments.add(new MqV2Assignment(assignment.getTopic(),
                        assignment.getPartition(), assignment.getCommittedOffset()));
            }
            members.add(new MqV2GroupMember(member.getMemberId(), member.getTopicsList(),
                    assignments, member.getLastHeartbeatMs()));
        }
        List<MqV2GroupPartitionLag> partitions = new ArrayList<>(response.getPartitionsCount());
        for (GroupPartitionLag lag : response.getPartitionsList()) {
            partitions.add(new MqV2GroupPartitionLag(lag.getTopic(), lag.getPartition(),
                    lag.getCommittedOffset(), lag.getNextOffset(), lag.getLag()));
        }
        return new MqV2GroupDescription(response.getGroup(), response.getGeneration(),
                toSdk(response.getState()), members, partitions);
    }

    @Override
    public Page<String> listGroupsPage(String prefix, int pageSize, String pageToken) {
        ListGroupsRequest request = ListGroupsRequest.newBuilder()
                .setPrefix(prefix == null ? "" : prefix)
                .setPageSize(pageSize)
                .setPageToken(pageToken == null ? "" : pageToken)
                .build();
        cn.byteforce.coord.contracts.mq.v2.ListGroupsResponse response = callWithRetry(
                (ch, req) -> stub(ch).listGroups((ListGroupsRequest) req),
                request, "mqV2.listGroups");
        return new Page<>(response.getGroupsList(), response.getNextPageToken());
    }

    @Override
    public MqV2MoveToDlqResult moveToDlq(MqV2MoveToDlq move) {
        MoveToDlqRequest request = MoveToDlqRequest.newBuilder()
                .setTopic(move.topic())
                .setPartition(move.partition())
                .setTopicEpoch(move.topicEpoch())
                .setOffset(move.offset())
                .setReason(move.reason())
                .setDetail(move.detail())
                .build();
        cn.byteforce.coord.contracts.mq.v2.MoveToDlqResponse response = callWithRetry(
                (ch, req) -> stub(ch).moveToDlq((MoveToDlqRequest) req),
                request, "mqV2.moveToDlq");
        return new MqV2MoveToDlqResult(response.getDlqOffset(), response.getAlreadyMoved());
    }

    @Override
    public MqV2PollDlqResult pollDlq(String topic, int partition, long topicEpoch,
                                     Long startOffset, int maxMessages) {
        PollDlqRequest.Builder builder = PollDlqRequest.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .setTopicEpoch(topicEpoch)
                .setMaxMessages(maxMessages);
        if (startOffset != null) {
            builder.setStartOffset(startOffset);
        }
        PollDlqRequest request = builder.build();
        cn.byteforce.coord.contracts.mq.v2.PollDlqResponse response = callWithRetry(
                (ch, req) -> stub(ch).pollDlq((PollDlqRequest) req),
                request, "mqV2.pollDlq");
        List<MqV2DlqMessage> messages = new ArrayList<>(response.getMessagesCount());
        for (cn.byteforce.coord.contracts.mq.v2.Message message : response.getMessagesList()) {
            messages.add(new MqV2DlqMessage(toSdk(message), message.getDlqReason(),
                    message.getDlqDetail()));
        }
        return new MqV2PollDlqResult(messages, response.getNextDlqOffset());
    }

    private static ByteString bytes(byte[] value) {
        return ByteString.copyFrom(value);
    }

    private static MqV2PublishResult toSdk(PublishResponse response) {
        return new MqV2PublishResult(response.getOffset(), response.getTimestampMs(),
                response.getDeduplicated());
    }

    private static MqV2Message toSdk(cn.byteforce.coord.contracts.mq.v2.Message message) {
        return new MqV2Message(message.getTopic(), message.getPartition(), message.getOffset(),
                message.getTimestampMs(), message.getKey().toByteArray(),
                message.getPayload().toByteArray(), message.getHeadersMap(),
                message.getProducer(), message.getDlqReason(), message.getDlqDetail());
    }

    private static List<MqV2Message> toSdkMessages(
            List<cn.byteforce.coord.contracts.mq.v2.Message> messages) {
        List<MqV2Message> result = new ArrayList<>(messages.size());
        for (cn.byteforce.coord.contracts.mq.v2.Message message : messages) {
            result.add(toSdk(message));
        }
        return result;
    }

    private static MqV2PartitionInfo toSdk(PartitionInfo info) {
        return new MqV2PartitionInfo(info.getPartition(), info.getShardId(),
                info.getLeaderAddr(), info.getEarliestOffset(), info.getNextOffset(),
                info.getLogBytes());
    }

    private static TopicConfig toProto(MqV2TopicConfig config) {
        if (config == null) {
            return TopicConfig.getDefaultInstance();
        }
        return TopicConfig.newBuilder()
                .setRetentionSecs(config.retentionSecs())
                .setMaxBytes(config.maxBytes())
                .setMaxMessageBytes(config.maxMessageBytes())
                .setSegmentBytes(config.segmentBytes())
                .setDlqRetentionSecs(config.dlqRetentionSecs())
                .build();
    }

    private static MqV2TopicConfig toSdk(TopicConfig config) {
        return new MqV2TopicConfig(config.getRetentionSecs(), config.getMaxBytes(),
                config.getMaxMessageBytes(), config.getSegmentBytes(),
                config.getDlqRetentionSecs());
    }

    private static MqV2DeletionState toSdk(TopicDeletionState state) {
        return switch (state) {
            case TOPIC_DELETION_STATE_PENDING -> MqV2DeletionState.PENDING;
            case TOPIC_DELETION_STATE_DONE -> MqV2DeletionState.DONE;
            case TOPIC_DELETION_STATE_FAILED -> MqV2DeletionState.FAILED;
            default -> MqV2DeletionState.UNSPECIFIED;
        };
    }

    private static MqV2GroupState toSdk(GroupState state) {
        return switch (state) {
            case GROUP_STATE_EMPTY -> MqV2GroupState.EMPTY;
            case GROUP_STATE_STABLE -> MqV2GroupState.STABLE;
            case GROUP_STATE_PREPARING_REBALANCE -> MqV2GroupState.PREPARING_REBALANCE;
            case GROUP_STATE_DEAD -> MqV2GroupState.DEAD;
            default -> MqV2GroupState.UNSPECIFIED;
        };
    }
}
