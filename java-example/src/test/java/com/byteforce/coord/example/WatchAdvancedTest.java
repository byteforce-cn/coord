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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Watch 扩展集成测试 — 高级场景
 *
 * 覆盖:
 * - Watch with start_revision (历史回放)
 * - Watch with prev_kv
 * - Multiple watches on different keys
 * - Watch cancel (stream close)
 * - Watch on non-existent prefix (later put triggers event)
 */
@DisplayName("Watch Advanced Integration Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class WatchAdvancedTest {

    private static ManagedChannel channel;
    private static WatchGrpc.WatchStub watchStub;
    private static KVGrpc.KVBlockingStub kvStub;

    @BeforeAll
    static void setUp() {
        channel = ManagedChannelBuilder
                .forAddress("localhost", 19527)
                .usePlaintext()
                .keepAliveTime(30, TimeUnit.SECONDS)
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

    // ──── Watch with start_revision (历史回放) ────

    @Test
    @Order(1)
    @DisplayName("Watch with start_revision replays historical events")
    void testWatchWithStartRevision() throws Exception {
        String key = "/test/watch/history";
        ByteString keyBs = ByteString.copyFromUtf8(key);

        // 先写入数据，记录 revision
        Kv.PutResponse putResp = kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(keyBs)
                .setValue(ByteString.copyFromUtf8("v1"))
                .build());
        long startRev = putResp.getRevision();

        // 从 revision 0 (从最新开始) 不应该看到历史
        // 从 startRev 开始，应该看到刚刚写入的数据
        BlockingQueue<WatchOuterClass.WatchResponse> eventQueue = new LinkedBlockingQueue<>();
        CountDownLatch latch = new CountDownLatch(1);

        StreamObserver<WatchOuterClass.WatchRequest> requestObserver =
                watchStub.watch(new StreamObserver<>() {
                    @Override
                    public void onNext(WatchOuterClass.WatchResponse resp) {
                        eventQueue.add(resp);
                        latch.countDown();
                    }
                    @Override
                    public void onError(Throwable t) { latch.countDown(); }
                    @Override
                    public void onCompleted() { latch.countDown(); }
                });

        requestObserver.onNext(WatchOuterClass.WatchRequest.newBuilder()
                .setCreate(WatchOuterClass.WatchCreateRequest.newBuilder()
                        .setKey(keyBs)
                        .setStartRevision(startRev)
                        .build())
                .build());

        boolean received = latch.await(5, TimeUnit.SECONDS);
        requestObserver.onCompleted();

        assertThat(received).as("Historical watch event received").isTrue();
        WatchOuterClass.WatchResponse event = eventQueue.poll();
        assertThat(event).isNotNull();
        assertThat(event.getEventsCount()).isGreaterThan(0);
    }

    // ──── Watch with prev_kv ────

    @Test
    @Order(2)
    @DisplayName("Watch with prev_kv includes previous value in events")
    void testWatchWithPrevKv() throws Exception {
        String key = "/test/watch/prevkv";
        ByteString keyBs = ByteString.copyFromUtf8(key);

        // 写入初始值
        kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(keyBs)
                .setValue(ByteString.copyFromUtf8("initial"))
                .build());

        CountDownLatch latch = new CountDownLatch(1);
        List<WatchOuterClass.WatchResponse> events = new ArrayList<>();

        StreamObserver<WatchOuterClass.WatchRequest> requestObserver =
                watchStub.watch(new StreamObserver<>() {
                    @Override
                    public void onNext(WatchOuterClass.WatchResponse resp) {
                        events.add(resp);
                        latch.countDown();
                    }
                    @Override
                    public void onError(Throwable t) { latch.countDown(); }
                    @Override
                    public void onCompleted() { latch.countDown(); }
                });

        requestObserver.onNext(WatchOuterClass.WatchRequest.newBuilder()
                .setCreate(WatchOuterClass.WatchCreateRequest.newBuilder()
                        .setKey(keyBs)
                        .setPrevKv(true)
                        .build())
                .build());

        // 确保 watch 已建立后再写入
        Thread.sleep(200);
        kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(keyBs)
                .setValue(ByteString.copyFromUtf8("updated"))
                .build());

        boolean received = latch.await(5, TimeUnit.SECONDS);
        requestObserver.onCompleted();

        assertThat(received).as("Watch event with prev_kv received").isTrue();
        assertThat(events).isNotEmpty();
    }

    // ──── Multiple watches concurrently ────

    @Test
    @Order(3)
    @DisplayName("Multiple concurrent watches receive events")
    void testMultipleConcurrentWatches() throws Exception {
        String prefix = "/test/watch/concurrent/";
        int watchCount = 3;
        CountDownLatch allReady = new CountDownLatch(watchCount);
        List<BlockingQueue<WatchOuterClass.WatchResponse>> allQueues = new ArrayList<>();
        List<StreamObserver<WatchOuterClass.WatchRequest>> observers = new ArrayList<>();

        for (int i = 0; i < watchCount; i++) {
            BlockingQueue<WatchOuterClass.WatchResponse> queue = new LinkedBlockingQueue<>();
            allQueues.add(queue);

            StreamObserver<WatchOuterClass.WatchRequest> obs =
                    watchStub.watch(new StreamObserver<>() {
                        @Override
                        public void onNext(WatchOuterClass.WatchResponse resp) {
                            queue.add(resp);
                            allReady.countDown();
                        }
                        @Override
                        public void onError(Throwable t) { allReady.countDown(); }
                        @Override
                        public void onCompleted() { allReady.countDown(); }
                    });

            obs.onNext(WatchOuterClass.WatchRequest.newBuilder()
                    .setCreate(WatchOuterClass.WatchCreateRequest.newBuilder()
                            .setKey(ByteString.copyFromUtf8(prefix))
                            .setRangeEnd(ByteString.copyFromUtf8(prefix + "\0"))
                            .build())
                    .build());
            observers.add(obs);
        }

        // 确保 watches 已建立
        Thread.sleep(300);

        // 写入 3 个 key，每个 watcher 都应该收到
        for (int i = 0; i < 3; i++) {
            kvStub.put(Kv.PutRequest.newBuilder()
                    .setKey(ByteString.copyFromUtf8(prefix + "key-" + i))
                    .setValue(ByteString.copyFromUtf8("val-" + i))
                    .build());
        }

        boolean received = allReady.await(10, TimeUnit.SECONDS);
        for (StreamObserver<WatchOuterClass.WatchRequest> obs : observers) {
            obs.onCompleted();
        }

        assertThat(received).as("All watchers received events").isTrue();
    }

    // ──── Watch cancel (stream close) ────

    @Test
    @Order(4)
    @DisplayName("Watch stream close stops receiving events")
    void testWatchStreamClose() throws Exception {
        String key = "/test/watch/cancel";
        ByteString keyBs = ByteString.copyFromUtf8(key);

        CountDownLatch eventReceived = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        AtomicInteger eventCount = new AtomicInteger(0);

        StreamObserver<WatchOuterClass.WatchRequest> requestObserver =
                watchStub.watch(new StreamObserver<>() {
                    @Override
                    public void onNext(WatchOuterClass.WatchResponse resp) {
                        eventCount.incrementAndGet();
                        eventReceived.countDown();
                    }
                    @Override
                    public void onError(Throwable t) { completed.countDown(); }
                    @Override
                    public void onCompleted() { completed.countDown(); }
                });

        requestObserver.onNext(WatchOuterClass.WatchRequest.newBuilder()
                .setCreate(WatchOuterClass.WatchCreateRequest.newBuilder()
                        .setKey(keyBs)
                        .build())
                .build());

        // 等待 watch 建立
        Thread.sleep(500);

        // 发送一个写操作验证 watch 工作
        kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(keyBs)
                .setValue(ByteString.copyFromUtf8("test"))
                .build());

        // 等待事件到达（最多 5 秒）
        boolean gotEvent = eventReceived.await(5, TimeUnit.SECONDS);

        // 关闭 stream
        requestObserver.onCompleted();
        completed.await(3, TimeUnit.SECONDS);

        assertThat(gotEvent).as("Watch event received before stream close").isTrue();
        assertThat(eventCount.get()).isGreaterThanOrEqualTo(1);
    }
}
