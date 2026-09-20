package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.contracts.event.v1.CloudEventMessage;
import cn.byteforce.coord.contracts.event.v1.EventGrpc;
import cn.byteforce.coord.contracts.event.v1.EventPublishRequest;
import cn.byteforce.coord.contracts.event.v1.EventPublishResponse;
import cn.byteforce.coord.contracts.event.v1.EventSubscribeRequest;
import cn.byteforce.coord.contracts.event.v1.EventUnsubscribeRequest;
import cn.byteforce.coord.contracts.event.v1.EventUnsubscribeResponse;
import cn.byteforce.coord.contracts.scheduler.v1.SchedulerClaimJobRequest;
import cn.byteforce.coord.contracts.scheduler.v1.SchedulerClaimJobResponse;
import cn.byteforce.coord.contracts.scheduler.v1.SchedulerCompleteJobRequest;
import cn.byteforce.coord.contracts.scheduler.v1.SchedulerCompleteJobResponse;
import cn.byteforce.coord.contracts.scheduler.v1.SchedulerGrpc;
import cn.byteforce.coord.contracts.scheduler.v1.SchedulerHeartbeatRequest;
import cn.byteforce.coord.contracts.scheduler.v1.SchedulerHeartbeatResponse;
import cn.byteforce.coord.contracts.scheduler.v1.SchedulerRegisterJobRequest;
import cn.byteforce.coord.contracts.scheduler.v1.SchedulerRegisterJobResponse;
import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.CoordException;
import cn.byteforce.coord.sdk.event.EventClient;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.sdk.scheduler.SchedulerClient;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * D9 —— 契约测试：{@code coord.event.v1} 与 {@code coord.scheduler.v1} 的 SDK 面。
 *
 * <p><b>为什么这两个面单列一个测试类</b>：它们是**最后两个补齐的 GA 客户端面**，
 * 此前 SDK 里连接口都没有（由 {@code check-sdk-sync.sh} 的反向覆盖卡口抓出，
 * 属 P1-3 同型缺陷）。因此这里的用例不只断言"方法能调用"，而是逐条断言
 * **真实 RPC 字段**与**响应映射**——字段号/类型/语义漂移时必须变红。
 *
 * <p>与 F-67（jepsen 侧 descriptor 把 {@code consumer_group}/{@code partition}
 * 字段号写反 ⇒ 客户端把 int32 发在 string 字段上 ⇒ 解码恒失败）同源：契约测试的价值
 * 全在"服务端收到的是什么"，而不在"客户端有没有这个方法"。
 */
class EventSchedulerContractTest {

    private Server server;
    private ManagedChannel channel;
    private final FakeEvent event = new FakeEvent();
    private final FakeScheduler scheduler = new FakeScheduler();

    private EventClient eventClient;
    private SchedulerClient schedulerClient;

    @BeforeEach
    void setUp() throws Exception {
        server = ServerBuilder.forPort(0)
                .addService(event)
                .addService(scheduler)
                .build()
                .start();
        channel = ManagedChannelBuilder.forAddress("localhost", server.getPort())
                .usePlaintext()
                .build();

        AgentChannelManager channelManager = mock(AgentChannelManager.class);
        when(channelManager.getChannel()).thenReturn(channel);
        CoordConfig config = CoordConfig.builder().agentHost("localhost").build();
        ObservabilityProvider obs = new ObservabilityProvider() {
        };

        eventClient = new EventClientImpl(channelManager, new ErrorMapper(), new RetryTemplate(),
                obs, config, java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor());
        schedulerClient = new SchedulerClientImpl(channelManager, new ErrorMapper(),
                new RetryTemplate(), obs, config);
    }

    @AfterEach
    void tearDown() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    // ── event ───────────────────────────────────────────────────────────────

    @Test
    @DisplayName("publish：五个契约字段逐一送达，返回服务端分配的 event_id")
    void eventPublishSendsEveryContractField() {
        String eventId = eventClient.publish("order.created", "order-svc",
                "{\"id\":1}".getBytes(StandardCharsets.UTF_8), "application/json", "order/1");

        assertThat(eventId).isEqualTo("evt-123");
        EventPublishRequest sent = event.lastPublish;
        assertThat(sent.getEventType()).isEqualTo("order.created");
        assertThat(sent.getSource()).isEqualTo("order-svc");
        assertThat(sent.getData().toStringUtf8()).isEqualTo("{\"id\":1}");
        assertThat(sent.getDataContentType()).isEqualTo("application/json");
        assertThat(sent.getSubject()).isEqualTo("order/1");
    }

    @Test
    @DisplayName("publish：String 重载按 UTF-8 编码，不静默改成平台默认字符集")
    void eventPublishStringOverloadUsesUtf8() {
        eventClient.publish("t", "s", "中文payload");

        assertThat(event.lastPublish.getData().toStringUtf8()).isEqualTo("中文payload");
        assertThat(event.lastPublish.getDataContentType()).isEqualTo("application/json");
    }

    @Test
    @DisplayName("subscribe：过滤条件送达，投递事件被逐字段映射")
    void eventSubscribeMapsDeliveredEvents() throws Exception {
        CountDownLatch delivered = new CountDownLatch(2);
        List<EventClient.CloudEvent> received = new CopyOnWriteArrayList<>();

        EventClient.EventSubscription sub = eventClient.subscribe("order.created", e -> {
            received.add(e);
            delivered.countDown();
        });
        assertThat(delivered.await(5, TimeUnit.SECONDS)).isTrue();
        sub.close();

        assertThat(event.lastSubscribe.getEventType()).isEqualTo("order.created");
        EventClient.CloudEvent first = received.get(0);
        assertThat(first.getId()).isEqualTo("e-1");
        assertThat(first.getSpecversion()).isEqualTo("1.0");
        assertThat(first.getType()).isEqualTo("order.created");
        assertThat(first.getSource()).isEqualTo("order-svc");
        assertThat(first.getDataAsString()).isEqualTo("payload-1");
        assertThat(first.getDataContentType()).isEqualTo("application/json");
        assertThat(first.getSubject()).isEqualTo("order/1");
        assertThat(first.getTime()).isEqualTo("2026-09-20T00:00:00Z");
    }

    @Test
    @DisplayName("subscribe：close() 在**没有新事件**时也要立刻返回（不能等下一跳）")
    void eventSubscriptionCloseUnblocksWithoutWaitingForNextEvent() throws Exception {
        // 服务端只发一条然后**保持流开着**（不 onCompleted）。阻塞式 iterator 的写法
        // 会在这里卡死：hasNext() 挂在网络上，close() 只能置标志位。
        CountDownLatch firstDelivered = new CountDownLatch(1);
        EventClient.EventSubscription sub = eventClient.subscribe("t", e -> firstDelivered.countDown());
        assertThat(firstDelivered.await(5, TimeUnit.SECONDS)).isTrue();

        long start = System.nanoTime();
        sub.close();
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(elapsedMs).isLessThan(2000);
        assertThatCode(sub::close).doesNotThrowAnyException(); // 幂等
    }

    @Test
    @DisplayName("unsubscribe：契约声明它、SDK 调用它，但订阅的真实取消是关流")
    void eventUnsubscribeIsStateFreeButStillReachesTheWire() {
        String id = "evt-order.created-1";
        eventClient.unsubscribe(id);

        assertThat(event.lastUnsubscribe.getSubscriptionId()).isEqualTo(id);
    }

    // ── scheduler ───────────────────────────────────────────────────────────

    @Test
    @DisplayName("registerJob：name/cron_expression/payload 三字段逐一送达")
    void schedulerRegisterJobSendsAllFields() {
        String jobId = schedulerClient.registerJob("nightly-report", "0 0 3 * * ?", "{\"k\":1}");

        assertThat(jobId).isEqualTo("nightly-report");
        SchedulerRegisterJobRequest sent = scheduler.lastRegister;
        assertThat(sent.getName()).isEqualTo("nightly-report");
        assertThat(sent.getCronExpression()).isEqualTo("0 0 3 * * ?");
        assertThat(sent.getPayload().toStringUtf8()).isEqualTo("{\"k\":1}");
    }

    @Test
    @DisplayName("claimJob：found=false 是**正常返回**（空 Optional），不是异常")
    void schedulerClaimJobMapsNotFoundToEmpty() {
        scheduler.claimFound = false;

        Optional<SchedulerClient.ClaimedJob> claimed = schedulerClient.claimJob("nightly-report");

        assertThat(claimed).isEmpty();
        assertThat(scheduler.lastClaim.getName()).isEqualTo("nightly-report");
    }

    @Test
    @DisplayName("claimJob：认领成功后 job_id 与 payload 都要带回来（payload 是认领者唯一的输入）")
    void schedulerClaimJobReturnsHandleAndPayload() {
        Optional<SchedulerClient.ClaimedJob> claimed = schedulerClient.claimJob("nightly-report");

        assertThat(claimed).isPresent();
        assertThat(claimed.get().getJobId()).isEqualTo("nightly-report");
        assertThat(claimed.get().getPayloadAsString()).isEqualTo("payload-bytes");
    }

    @Test
    @DisplayName("heartbeat：以 job_id 作凭据；claim 失效时**必须报错**，不能静默成功")
    void schedulerHeartbeatIsFailLoudWhenClaimIsLost() {
        schedulerClient.heartbeat("nightly-report");
        assertThat(scheduler.lastHeartbeat.getJobId()).isEqualTo("nightly-report");

        scheduler.heartbeatFails = true;
        assertThatThrownBy(() -> schedulerClient.heartbeat("nightly-report"))
                .isInstanceOf(CoordException.class);
    }

    @Test
    @DisplayName("completeJob：job_id 与 result 都送达（result 契约接受但不留档）")
    void schedulerCompleteJobSendsHandleAndResult() {
        schedulerClient.completeJob("nightly-report", "ok");

        SchedulerCompleteJobRequest sent = scheduler.lastComplete;
        assertThat(sent.getJobId()).isEqualTo("nightly-report");
        assertThat(sent.getResult().toStringUtf8()).isEqualTo("ok");
    }

    // ── fakes ───────────────────────────────────────────────────────────────

    private static final class FakeEvent extends EventGrpc.EventImplBase {
        volatile EventPublishRequest lastPublish;
        volatile EventSubscribeRequest lastSubscribe;
        volatile EventUnsubscribeRequest lastUnsubscribe;

        @Override
        public void publish(EventPublishRequest req, StreamObserver<EventPublishResponse> o) {
            lastPublish = req;
            o.onNext(EventPublishResponse.newBuilder().setEventId("evt-123").build());
            o.onCompleted();
        }

        @Override
        public void subscribe(EventSubscribeRequest req, StreamObserver<CloudEventMessage> o) {
            lastSubscribe = req;
            o.onNext(cloudEvent("e-1", "payload-1"));
            o.onNext(cloudEvent("e-2", "payload-2"));
            // 刻意**不** onCompleted：模拟"话题安静下来但订阅仍然活着"的真实情形，
            // 这正是"close() 必须立刻生效"用例要覆盖的形态。
        }

        @Override
        public void unsubscribe(EventUnsubscribeRequest req,
                                StreamObserver<EventUnsubscribeResponse> o) {
            lastUnsubscribe = req;
            o.onNext(EventUnsubscribeResponse.getDefaultInstance());
            o.onCompleted();
        }

        private static CloudEventMessage cloudEvent(String id, String data) {
            return CloudEventMessage.newBuilder()
                    .setId(id)
                    .setSpecversion("1.0")
                    .setType("order.created")
                    .setSource("order-svc")
                    .setData(ByteString.copyFromUtf8(data))
                    .setDataContentType("application/json")
                    .setSubject("order/1")
                    .setTime("2026-09-20T00:00:00Z")
                    .build();
        }
    }

    private static final class FakeScheduler extends SchedulerGrpc.SchedulerImplBase {
        volatile boolean claimFound = true;
        volatile boolean heartbeatFails = false;
        volatile SchedulerRegisterJobRequest lastRegister;
        volatile SchedulerClaimJobRequest lastClaim;
        volatile SchedulerHeartbeatRequest lastHeartbeat;
        volatile SchedulerCompleteJobRequest lastComplete;
        private final AtomicBoolean claimConsumed = new AtomicBoolean(false);

        @Override
        public void registerJob(SchedulerRegisterJobRequest req,
                                StreamObserver<SchedulerRegisterJobResponse> o) {
            lastRegister = req;
            // 服务端当前把 job_id 定为 name（task_id 必须与 ClaimJob 的查询键一致）
            o.onNext(SchedulerRegisterJobResponse.newBuilder().setJobId(req.getName()).build());
            o.onCompleted();
        }

        @Override
        public void claimJob(SchedulerClaimJobRequest req,
                             StreamObserver<SchedulerClaimJobResponse> o) {
            lastClaim = req;
            if (!claimFound) {
                // 契约里 found=false 走默认实例（三个字段全空）
                o.onNext(SchedulerClaimJobResponse.getDefaultInstance());
                o.onCompleted();
                return;
            }
            claimConsumed.set(true);
            o.onNext(SchedulerClaimJobResponse.newBuilder()
                    .setJobId(req.getName())
                    .setPayload(ByteString.copyFromUtf8("payload-bytes"))
                    .setFound(true)
                    .build());
            o.onCompleted();
        }

        @Override
        public void heartbeat(SchedulerHeartbeatRequest req,
                              StreamObserver<SchedulerHeartbeatResponse> o) {
            lastHeartbeat = req;
            if (heartbeatFails) {
                // 服务端在 claim 已失效时必须这样回（FAILED_PRECONDITION），
                // 而不是回一个空的 OK —— 后者会让调用方以为续期成功。
                o.onError(Status.FAILED_PRECONDITION
                        .withDescription("scheduler: claim for '" + req.getJobId() + "' is no longer valid")
                        .asRuntimeException());
                return;
            }
            o.onNext(SchedulerHeartbeatResponse.getDefaultInstance());
            o.onCompleted();
        }

        @Override
        public void completeJob(SchedulerCompleteJobRequest req,
                                StreamObserver<SchedulerCompleteJobResponse> o) {
            lastComplete = req;
            o.onNext(SchedulerCompleteJobResponse.getDefaultInstance());
            o.onCompleted();
        }
    }
}
