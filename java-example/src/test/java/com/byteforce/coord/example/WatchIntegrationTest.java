package cn.byteforce.coord.example;

import com.google.protobuf.ByteString;
import coord.kv.KVGrpc;
import coord.kv.Kv;
import coord.watch.WatchGrpc;
import coord.watch.WatchOuterClass;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Watch 集成测试 — TDD RED 阶段
 *
 * 验证 Java 应用通过 Agent 的 Watch 功能:
 * Create Watch → Put key → 收到 WatchEvent → 验证事件内容
 */
@DisplayName("Watch Integration Tests (Java → Agent gRPC)")
class WatchIntegrationTest {

    private static ManagedChannel channel;
    private static WatchGrpc.WatchStub watchStub;
    private static KVGrpc.KVBlockingStub kvStub;

    @BeforeAll
    static void setUp() {
        AgentEndpoint.requireReachable();
        channel = ManagedChannelBuilder
                .forAddress(AgentEndpoint.host(), AgentEndpoint.port())
                .usePlaintext()
                .build();
        watchStub = WatchGrpc.newStub(channel);
        kvStub = KVGrpc.newBlockingStub(channel);
    }

    @AfterAll
    static void tearDown() throws InterruptedException {
        if (channel != null) {
            channel.shutdown();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    @DisplayName("Watch a key and receive event when key is updated")
    void testWatchSingleKey() throws Exception {
        String watchKey = "/test/watch/hello";
        String watchValue = "watched-value";

        // 先写一条并记下 revision，用于**确定性地**证明订阅已在 agent 侧登记。
        //
        // 为什么不能"watch 之后立刻 put"：`start_revision = 0` 的语义是**从最新开始**
        // （不是回放全部历史，见 `WatchAdvancedTest.testWatchWithStartRevision` 的说明）。
        // 因此若 create 在 agent 侧登记完成之前就发生了写入，该写入**既不会被回放、也不会
        // 以实时事件送达** —— 测试就退化成一场竞态。协议里没有"订阅已建立"的 ack，
        // 客户端唯一能依靠的锚点就是"先回放、后实时"。
        //
        // 这不是理论：CI 上 `WatchIntegrationTest` 曾以
        // `[Watch event received] expecting value to be true but was false` 失败过一次
        // （51 个用例中的 1 个），而同一提交此前是 51/51。
        long startRev = kvStub
                .put(Kv.PutRequest.newBuilder()
                        .setKey(ByteString.copyFromUtf8(watchKey))
                        .setValue(ByteString.copyFromUtf8("before"))
                        .build())
                .getRevision();

        BlockingQueue<WatchOuterClass.WatchResponse> eventQueue = new LinkedBlockingQueue<>();
        AtomicReference<Throwable> streamError = new AtomicReference<>();

        StreamObserver<WatchOuterClass.WatchRequest> requestObserver =
                watchStub.watch(new StreamObserver<>() {
                    @Override
                    public void onNext(WatchOuterClass.WatchResponse resp) {
                        eventQueue.add(resp);
                    }

                    @Override
                    public void onError(Throwable t) {
                        streamError.set(t);
                    }

                    @Override
                    public void onCompleted() {
                        // 正常收尾，无需处理
                    }
                });

        requestObserver.onNext(WatchOuterClass.WatchRequest.newBuilder()
                .setCreate(WatchOuterClass.WatchCreateRequest.newBuilder()
                        .setKey(ByteString.copyFromUtf8(watchKey))
                        .setStartRevision(startRev)
                        .build())
                .build());

        // ① 回放：既验证历史事件、又证明订阅已经建立（此后不再有登记竞态）
        WatchOuterClass.WatchResponse replayed = eventQueue.poll(5, TimeUnit.SECONDS);
        assertThat(replayed)
                .as("Watch replay received (proves the subscription is registered); streamError=%s",
                        streamError.get())
                .isNotNull();

        // ② 实时推送：登记已完成，事件必然会到
        kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(watchKey))
                .setValue(ByteString.copyFromUtf8(watchValue))
                .build());

        WatchOuterClass.WatchResponse event = eventQueue.poll(5, TimeUnit.SECONDS);
        requestObserver.onCompleted();

        assertThat(event)
                .as("Live watch event received; streamError=%s", streamError.get())
                .isNotNull();
        assertThat(event.getEventsCount()).isGreaterThan(0);

        WatchOuterClass.WatchEvent watchEvent = event.getEvents(0);
        assertThat(watchEvent.getType()).isEqualTo(WatchOuterClass.WatchEvent.EventType.PUT);
        assertThat(watchEvent.getKvsCount()).isGreaterThan(0);

        Kv.KeyValue kv = watchEvent.getKvs(0);
        assertThat(kv.getKey().toStringUtf8()).isEqualTo(watchKey);
        assertThat(kv.getValue().toStringUtf8()).isEqualTo(watchValue);
    }

    @Test
    @DisplayName("Watch multiple keys under a prefix")
    void testWatchPrefix() throws Exception {
        String prefix = "/test/watch/prefix/";
        BlockingQueue<WatchOuterClass.WatchResponse> eventQueue = new LinkedBlockingQueue<>();
        AtomicReference<Throwable> streamError = new AtomicReference<>();

        // 同 `testWatchSingleKey`：先用一次写入锚定"订阅已登记"（回放），再做实时写入。
        // 直接"watch 后连写两条"同样是在跟登记窗口赛跑 —— 两条都可能丢。
        String first = prefix + "a";
        long startRev = kvStub
                .put(Kv.PutRequest.newBuilder()
                        .setKey(ByteString.copyFromUtf8(first))
                        .setValue(ByteString.copyFromUtf8("val-a"))
                        .build())
                .getRevision();

        StreamObserver<WatchOuterClass.WatchRequest> requestObserver =
                watchStub.watch(new StreamObserver<>() {
                    @Override
                    public void onNext(WatchOuterClass.WatchResponse resp) {
                        eventQueue.add(resp);
                    }

                    @Override
                    public void onError(Throwable t) {
                        streamError.set(t);
                    }

                    @Override
                    public void onCompleted() {
                        // 正常收尾，无需处理
                    }
                });

        ByteString prefixBytes = ByteString.copyFromUtf8(prefix);
        ByteString rangeEnd = PrefixScan.end(prefix);
        requestObserver.onNext(WatchOuterClass.WatchRequest.newBuilder()
                .setCreate(WatchOuterClass.WatchCreateRequest.newBuilder()
                        .setKey(prefixBytes)
                        .setRangeEnd(rangeEnd)
                        .setStartRevision(startRev)
                        .build())
                .build());

        // ① 回放 `prefix + "a"`（证明订阅已登记）
        assertThat(eventQueue.poll(5, TimeUnit.SECONDS))
                .as("Prefix replay received (proves the subscription is registered); streamError=%s",
                        streamError.get())
                .isNotNull();

        // ② 实时推送第二条
        kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(prefix + "b"))
                .setValue(ByteString.copyFromUtf8("val-b"))
                .build());

        WatchOuterClass.WatchResponse live = eventQueue.poll(5, TimeUnit.SECONDS);
        requestObserver.onCompleted();

        assertThat(live)
                .as("Second (live) prefix event received; streamError=%s", streamError.get())
                .isNotNull();
        assertThat(live.getEvents(0).getKvs(0).getKey().toStringUtf8())
                .isEqualTo(prefix + "b");
    }
}
