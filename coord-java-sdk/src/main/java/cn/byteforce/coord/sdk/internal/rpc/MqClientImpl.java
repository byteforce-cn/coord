package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.sdk.internal.proto.MQGrpc;
import cn.byteforce.coord.sdk.internal.proto.MqAckRequest;
import cn.byteforce.coord.sdk.internal.proto.MqCreateTopicRequest;
import cn.byteforce.coord.sdk.internal.proto.MqMessage;
import cn.byteforce.coord.sdk.internal.proto.MqPollDlqRequest;
import cn.byteforce.coord.sdk.internal.proto.MqPollDlqResponse;
import cn.byteforce.coord.sdk.internal.proto.MqPollRequest;
import cn.byteforce.coord.sdk.internal.proto.MqPollResponse;
import cn.byteforce.coord.sdk.internal.proto.MqPublishRequest;
import cn.byteforce.coord.sdk.internal.proto.MqPublishResponse;
import cn.byteforce.coord.sdk.mq.MqClient;
import cn.byteforce.coord.sdk.mq.MqSubscribeRequest;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import com.google.protobuf.ByteString;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Implementation of {@link MqClient} backed by gRPC calls to the Coord Agent's
 * MQ data-plane service.
 */
public final class MqClientImpl extends AgentRpcClient implements MqClient {

    private static final Logger log = LoggerFactory.getLogger(MqClientImpl.class);
    private final CoordConfig config;

    public MqClientImpl(AgentChannelManager channelManager, ErrorMapper errorMapper,
                        RetryTemplate retryTemplate, ObservabilityProvider observability,
                        CoordConfig config) {
        super(channelManager, errorMapper, retryTemplate, observability);
        this.config = config;
    }

    @Override
    public void createTopic(String topic, int partitions) {
        MqCreateTopicRequest request = MqCreateTopicRequest.newBuilder()
                .setTopic(topic)
                .setPartitions(partitions)
                .build();
        callWithRetry(
                (ch, req) -> MQGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .createTopic((MqCreateTopicRequest) req),
                request, "mq.createTopic");
        log.debug("MQ topic created: topic={}, partitions={}", topic, partitions);
    }

    @Override
    public long publish(String topic, int partition, byte[] key, byte[] payload) {
        MqPublishRequest.Builder req = MqPublishRequest.newBuilder()
                .setTopic(topic)
                .setPartition(partition);
        if (key != null && key.length > 0) {
            req.setKey(ByteString.copyFrom(key));
        }
        if (payload != null) {
            req.setPayload(ByteString.copyFrom(payload));
        }
        MqPublishResponse response = callWithRetry(
                (ch, r) -> MQGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .publish((MqPublishRequest) r),
                req.build(), "mq.publish");
        log.debug("MQ publish: topic={}, partition={}, offset={}", topic, partition, response.getOffset());
        return response.getOffset();
    }

    @Override
    public List<cn.byteforce.coord.sdk.mq.MqMessage> poll(String topic, int partition, String group,
                                                           long startOffset, int maxCount) {
        MqPollRequest request = MqPollRequest.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .setConsumerGroup(group == null ? "" : group)
                .setStartOffset(startOffset)
                .setMaxCount(maxCount)
                .build();
        MqPollResponse response = callWithRetry(
                (ch, req) -> MQGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .poll((MqPollRequest) req),
                request, "mq.poll");
        return toSdkMessages(response.getMessagesList());
    }

    @Override
    public List<cn.byteforce.coord.sdk.mq.MqMessage> pollDlq(String topic, int partition, int maxCount) {
        MqPollDlqRequest request = MqPollDlqRequest.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .setMaxCount(maxCount)
                .build();
        MqPollDlqResponse response = callWithRetry(
                (ch, req) -> MQGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .pollDlq((MqPollDlqRequest) req),
                request, "mq.pollDlq");
        return toSdkMessages(response.getMessagesList());
    }

    @Override
    public void ack(String topic, String group, int partition, long offset) {
        MqAckRequest request = MqAckRequest.newBuilder()
                .setTopic(topic)
                .setConsumerGroup(group == null ? "" : group)
                .setPartition(partition)
                .setOffset(offset)
                .build();
        callWithRetry(
                (ch, req) -> MQGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .ack((MqAckRequest) req),
                request, "mq.ack");
        log.debug("MQ ack: topic={}, group={}, partition={}, offset={}", topic, group, partition, offset);
    }

    @Override
    public AutoCloseable subscribe(MqSubscribeRequest request,
                                   Consumer<cn.byteforce.coord.sdk.mq.MqMessage> listener) {
        cn.byteforce.coord.sdk.internal.proto.MqSubscribeRequest proto =
                cn.byteforce.coord.sdk.internal.proto.MqSubscribeRequest.newBuilder()
                        .setTopic(request.topic())
                        .setConsumerGroup(request.consumerGroup())
                        .build();

        ManagedChannel channel = channelManager.getChannel();
        ClientCall<cn.byteforce.coord.sdk.internal.proto.MqSubscribeRequest, MqMessage> call =
                channel.newCall(MQGrpc.getSubscribeMethod(), CallOptions.DEFAULT);

        ClientCall.Listener<MqMessage> callListener = new ClientCall.Listener<>() {
            @Override
            public void onMessage(MqMessage message) {
                listener.accept(toSdk(message));
                // flow control：每收到一条再请求下一条
                call.request(1);
            }

            @Override
            public void onClose(Status status, Metadata trailers) {
                if (status.isOk()) {
                    log.debug("MQ subscribe closed normally: topic={}, group={}",
                            request.topic(), request.consumerGroup());
                } else {
                    log.debug("MQ subscribe closed with status: topic={}, group={}, status={}",
                            request.topic(), request.consumerGroup(), status);
                }
            }
        };

        call.start(callListener, new Metadata());
        call.request(1);
        call.sendMessage(proto);
        call.halfClose();
        log.debug("MQ subscribe started: topic={}, group={}", request.topic(), request.consumerGroup());

        return () -> {
            call.cancel("unsubscribe", null);
            log.debug("MQ subscribe cancelled: topic={}, group={}", request.topic(), request.consumerGroup());
        };
    }

    private static cn.byteforce.coord.sdk.mq.MqMessage toSdk(MqMessage m) {
        return new cn.byteforce.coord.sdk.mq.MqMessage(
                m.getTopic(),
                m.getPartition(),
                m.getOffset(),
                m.getKey().toByteArray(),
                m.getPayload().toByteArray(),
                m.getTimestamp());
    }

    private static List<cn.byteforce.coord.sdk.mq.MqMessage> toSdkMessages(List<MqMessage> protoMessages) {
        List<cn.byteforce.coord.sdk.mq.MqMessage> result = new ArrayList<>(protoMessages.size());
        for (MqMessage m : protoMessages) {
            result.add(toSdk(m));
        }
        return result;
    }
}
