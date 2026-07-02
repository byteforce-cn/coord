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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

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
        channel = ManagedChannelBuilder
                .forAddress("localhost", 19527)
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
        BlockingQueue<WatchOuterClass.WatchResponse> eventQueue = new LinkedBlockingQueue<>();
        CountDownLatch firstEventLatch = new CountDownLatch(1);

        StreamObserver<WatchOuterClass.WatchRequest> requestObserver =
                watchStub.watch(new StreamObserver<>() {
                    @Override
                    public void onNext(WatchOuterClass.WatchResponse resp) {
                        eventQueue.add(resp);
                        firstEventLatch.countDown();
                    }

                    @Override
                    public void onError(Throwable t) {
                        firstEventLatch.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        firstEventLatch.countDown();
                    }
                });

        requestObserver.onNext(WatchOuterClass.WatchRequest.newBuilder()
                .setCreate(WatchOuterClass.WatchCreateRequest.newBuilder()
                        .setKey(ByteString.copyFromUtf8(watchKey))
                        .build())
                .build());

        kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(watchKey))
                .setValue(ByteString.copyFromUtf8(watchValue))
                .build());

        boolean received = firstEventLatch.await(5, TimeUnit.SECONDS);
        requestObserver.onCompleted();

        assertThat(received).as("Watch event received").isTrue();
        WatchOuterClass.WatchResponse event = eventQueue.poll();
        assertThat(event).isNotNull();
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
        CountDownLatch latch = new CountDownLatch(2);

        StreamObserver<WatchOuterClass.WatchRequest> requestObserver =
                watchStub.watch(new StreamObserver<>() {
                    @Override
                    public void onNext(WatchOuterClass.WatchResponse resp) {
                        eventQueue.add(resp);
                        latch.countDown();
                    }

                    @Override
                    public void onError(Throwable t) {
                        while (latch.getCount() > 0) latch.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        while (latch.getCount() > 0) latch.countDown();
                    }
                });

        ByteString prefixBytes = ByteString.copyFromUtf8(prefix);
        ByteString rangeEnd = ByteString.copyFromUtf8(prefix + "\0");
        requestObserver.onNext(WatchOuterClass.WatchRequest.newBuilder()
                .setCreate(WatchOuterClass.WatchCreateRequest.newBuilder()
                        .setKey(prefixBytes)
                        .setRangeEnd(rangeEnd)
                        .build())
                .build());

        kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(prefix + "a"))
                .setValue(ByteString.copyFromUtf8("val-a"))
                .build());
        kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(prefix + "b"))
                .setValue(ByteString.copyFromUtf8("val-b"))
                .build());

        boolean received = latch.await(5, TimeUnit.SECONDS);
        requestObserver.onCompleted();

        assertThat(received).as("Two watch events received for prefix").isTrue();
        assertThat(eventQueue.size()).isGreaterThanOrEqualTo(2);
    }
}
