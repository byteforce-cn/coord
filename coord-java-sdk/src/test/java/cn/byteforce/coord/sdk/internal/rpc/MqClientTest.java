package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.sdk.internal.proto.MQGrpc;
import cn.byteforce.coord.sdk.internal.proto.MqAckRequest;
import cn.byteforce.coord.sdk.internal.proto.MqAckResponse;
import cn.byteforce.coord.sdk.internal.proto.MqCreateTopicRequest;
import cn.byteforce.coord.sdk.internal.proto.MqCreateTopicResponse;
import cn.byteforce.coord.sdk.internal.proto.MqMessage;
import cn.byteforce.coord.sdk.internal.proto.MqPollDlqRequest;
import cn.byteforce.coord.sdk.internal.proto.MqPollDlqResponse;
import cn.byteforce.coord.sdk.internal.proto.MqPollRequest;
import cn.byteforce.coord.sdk.internal.proto.MqPollResponse;
import cn.byteforce.coord.sdk.internal.proto.MqPublishRequest;
import cn.byteforce.coord.sdk.internal.proto.MqPublishResponse;
import cn.byteforce.coord.sdk.internal.proto.MqSubscribeRequest;
import cn.byteforce.coord.sdk.mq.MqClient;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the {@link MqClient} gRPC client against an in-process stub server.
 * <p>
 * Verifies Phase 1 MQ SDK surface: createTopic / publish (increasing offsets) /
 * poll (incremental by offset) / ack / pollDlq.
 */
class MqClientTest {

    private Server server;
    private ManagedChannel channel;
    private MqClient mq;
    private FakeMqService fake;

    @BeforeEach
    void setUp() throws Exception {
        fake = new FakeMqService();
        server = ServerBuilder.forPort(0).addService(fake).build().start();
        channel = ManagedChannelBuilder.forAddress("localhost", server.getPort())
                .usePlaintext()
                .build();

        AgentChannelManager channelManager = mock(AgentChannelManager.class);
        when(channelManager.getChannel()).thenReturn(channel);

        mq = new MqClientImpl(
                channelManager,
                new ErrorMapper(),
                new RetryTemplate(),
                new ObservabilityProvider() {
                },
                CoordConfig.builder().agentHost("localhost").build());
    }

    @AfterEach
    void tearDown() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    @Test
    void shouldCreateTopicAndPublishWithIncreasingOffsets() {
        mq.createTopic("orders", 4);
        assertThat(fake.topics).containsKey("orders");
        assertThat(fake.topics.get("orders")).isEqualTo(4);

        long offset0 = mq.publish("orders", 0, "k1".getBytes(), "m1".getBytes());
        long offset1 = mq.publish("orders", 0, null, "m2".getBytes());
        assertThat(offset0).isEqualTo(0);
        assertThat(offset1).isEqualTo(1);

        assertThat(fake.messages.get(0).getPayload().toStringUtf8()).isEqualTo("m1");
        assertThat(fake.messages.get(1).getPayload().toStringUtf8()).isEqualTo("m2");
    }

    @Test
    void shouldPollIncrementallyByOffset() {
        mq.createTopic("orders", 1);
        for (int i = 0; i < 5; i++) {
            mq.publish("orders", 0, null, ("msg-" + i).getBytes());
        }

        List<cn.byteforce.coord.sdk.mq.MqMessage> batch = mq.poll("orders", 0, "cg1", 0, 2);
        assertThat(batch).hasSize(2);
        assertThat(batch.get(0).offset()).isEqualTo(0);
        assertThat(batch.get(0).payload()).isEqualTo("msg-0".getBytes());
        assertThat(batch.get(1).offset()).isEqualTo(1);

        // Incremental cursor — pull from offset 3
        List<cn.byteforce.coord.sdk.mq.MqMessage> tail = mq.poll("orders", 0, "cg1", 3, 100);
        assertThat(tail).hasSize(2);
        assertThat(tail.get(0).offset()).isEqualTo(3);
        assertThat(tail.get(1).offset()).isEqualTo(4);
    }

    @Test
    void shouldAckAndTrackCommittedOffset() {
        mq.createTopic("orders", 1);
        for (int i = 0; i < 3; i++) {
            mq.publish("orders", 0, null, ("msg-" + i).getBytes());
        }

        mq.ack("orders", "cg1", 0, 1);
        assertThat(fake.committedOffsets.get("cg1")).isEqualTo(1L);

        // ack 后从已确认 offset 继续，不重复消费已 ack 消息
        List<cn.byteforce.coord.sdk.mq.MqMessage> after = mq.poll("orders", 0, "cg1", 2, 100);
        assertThat(after).hasSize(1);
        assertThat(after.get(0).offset()).isEqualTo(2);
    }

    @Test
    void shouldPollDlq() {
        // 预置一条 DLQ 消息（模拟毒消息入队）
        fake.dlq.add(MqMessage.newBuilder()
                .setTopic("orders").setPartition(0).setOffset(7)
                .setPayload(ByteString.copyFromUtf8("poison"))
                .setTimestamp(1234)
                .build());

        List<cn.byteforce.coord.sdk.mq.MqMessage> dlq = mq.pollDlq("orders", 0, 100);
        assertThat(dlq).hasSize(1);
        assertThat(dlq.get(0).payload()).isEqualTo("poison".getBytes());
        assertThat(dlq.get(0).timestamp()).isEqualTo(1234L);
    }

    @Test
    void shouldSubscribeReplayAndPush() throws Exception {
        mq.createTopic("orders", 1);
        mq.publish("orders", 0, null, "m1".getBytes());

        CountDownLatch latch = new CountDownLatch(2);
        List<byte[]> received = Collections.synchronizedList(new ArrayList<>());
        cn.byteforce.coord.sdk.mq.MqSubscribeRequest req =
                cn.byteforce.coord.sdk.mq.MqSubscribeRequest.builder()
                        .topic("orders")
                        .consumerGroup("cg-sub")
                        .build();
        AutoCloseable sub = mq.subscribe(req, msg -> {
            received.add(msg.payload());
            latch.countDown();
        });

        // 回放已有 1 条；再发布 1 条实时推送
        mq.publish("orders", 0, null, "m2".getBytes());
        assertThat(latch.await(5, TimeUnit.SECONDS)).as("订阅应收到回放+实时共 2 条").isTrue();
        assertThat(received).containsExactly("m1".getBytes(), "m2".getBytes());

        sub.close();
    }

    // ──── In-process stub server ────

    /** Minimal MQ server stub mimicking the agent's single-agent semantics. */
    static class FakeMqService extends MQGrpc.MQImplBase {
        final Map<String, Integer> topics = new HashMap<>();
        final List<MqMessage> messages = new ArrayList<>();
        final List<MqMessage> dlq = new ArrayList<>();
        final Map<String, Long> committedOffsets = new HashMap<>();
        final List<Subscriber> subscribers = new ArrayList<>();
        long nextOffset = 0;

        static class Subscriber {
            final String group;
            final StreamObserver<MqMessage> observer;

            Subscriber(String group, StreamObserver<MqMessage> observer) {
                this.group = group;
                this.observer = observer;
            }
        }

        @Override
        public void createTopic(MqCreateTopicRequest request, StreamObserver<MqCreateTopicResponse> responseObserver) {
            topics.put(request.getTopic(), request.getPartitions());
            responseObserver.onNext(MqCreateTopicResponse.getDefaultInstance());
            responseObserver.onCompleted();
        }

        @Override
        public void publish(MqPublishRequest request, StreamObserver<MqPublishResponse> responseObserver) {
            long offset = nextOffset++;
            MqMessage msg = MqMessage.newBuilder()
                    .setTopic(request.getTopic())
                    .setPartition(request.getPartition())
                    .setOffset(offset)
                    .setKey(request.getKey())
                    .setPayload(request.getPayload())
                    .setTimestamp(System.currentTimeMillis())
                    .build();
            messages.add(msg);

            synchronized (this) {
                for (Subscriber s : subscribers) {
                    long committed = committedOffsets.getOrDefault(s.group, 0L);
                    if (msg.getOffset() >= committed) {
                        s.observer.onNext(msg);
                        committedOffsets.put(s.group, msg.getOffset() + 1);
                    }
                }
            }

            responseObserver.onNext(MqPublishResponse.newBuilder().setOffset(offset).build());
            responseObserver.onCompleted();
        }

        @Override
        public void subscribe(MqSubscribeRequest request, StreamObserver<MqMessage> responseObserver) {
            synchronized (this) {
                long committed = committedOffsets.getOrDefault(request.getConsumerGroup(), 0L);
                for (MqMessage m : messages) {
                    if (m.getOffset() >= committed) {
                        responseObserver.onNext(m);
                    }
                }
                committedOffsets.put(request.getConsumerGroup(), (long) messages.size());
                subscribers.add(new Subscriber(request.getConsumerGroup(), responseObserver));
            }
        }

        @Override
        public void poll(MqPollRequest request, StreamObserver<MqPollResponse> responseObserver) {
            MqPollResponse.Builder resp = MqPollResponse.newBuilder();
            long start = request.getStartOffset();
            int max = request.getMaxCount() <= 0 ? 100 : request.getMaxCount();
            int added = 0;
            for (MqMessage m : messages) {
                if (m.getOffset() >= start && added < max) {
                    resp.addMessages(m);
                    added++;
                }
            }
            responseObserver.onNext(resp.build());
            responseObserver.onCompleted();
        }

        @Override
        public void pollDlq(MqPollDlqRequest request, StreamObserver<MqPollDlqResponse> responseObserver) {
            MqPollDlqResponse.Builder resp = MqPollDlqResponse.newBuilder();
            int max = request.getMaxCount() <= 0 ? 100 : request.getMaxCount();
            for (int i = 0; i < dlq.size() && i < max; i++) {
                resp.addMessages(dlq.get(i));
            }
            responseObserver.onNext(resp.build());
            responseObserver.onCompleted();
        }

        @Override
        public void ack(MqAckRequest request, StreamObserver<MqAckResponse> responseObserver) {
            committedOffsets.put(request.getConsumerGroup(), request.getOffset());
            responseObserver.onNext(MqAckResponse.getDefaultInstance());
            responseObserver.onCompleted();
        }
    }
}
