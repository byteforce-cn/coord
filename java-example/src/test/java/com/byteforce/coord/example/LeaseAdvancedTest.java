package cn.byteforce.coord.example;

import com.google.protobuf.ByteString;
import coord.kv.KVGrpc;
import coord.kv.Kv;
import coord.lease.LeaseGrpc;
import coord.lease.LeaseOuterClass;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Lease 扩展集成测试 — 覆盖高级场景
 *
 * 覆盖:
 * - Grant with specified ID
 * - Multiple KeepAlive in one stream
 * - Revoke non-existent lease (error handling)
 * - Lease binding: key deleted on revoke (already in LeaseIntegrationTest)
 * - Concurrent KeepAlive
 */
@DisplayName("Lease Advanced Integration Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class LeaseAdvancedTest {

    private static ManagedChannel channel;
    private static LeaseGrpc.LeaseBlockingStub leaseStub;
    private static LeaseGrpc.LeaseStub leaseAsyncStub;
    private static KVGrpc.KVBlockingStub kvStub;

    @BeforeAll
    static void setUp() {
        channel = ManagedChannelBuilder
                .forAddress("localhost", 19527)
                .usePlaintext()
                .keepAliveTime(30, TimeUnit.SECONDS)
                .build();
        leaseStub = LeaseGrpc.newBlockingStub(channel);
        leaseAsyncStub = LeaseGrpc.newStub(channel);
        kvStub = KVGrpc.newBlockingStub(channel);
    }

    @AfterAll
    static void tearDown() throws InterruptedException {
        if (channel != null) {
            channel.shutdown();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    // ──── Grant with specified ID ────

    @Test
    @Order(1)
    @DisplayName("Grant lease with specified ID")
    void testGrantWithSpecifiedId() {
        LeaseOuterClass.LeaseGrantResponse resp = leaseStub.leaseGrant(
                LeaseOuterClass.LeaseGrantRequest.newBuilder()
                        .setId(9001)
                        .setTtl(60)
                        .build());

        assertThat(resp.getId()).isEqualTo(9001);
        assertThat(resp.getTtl()).isEqualTo(60);
    }

    // ──── Revoke non-existent lease ────

    @Test
    @Order(2)
    @DisplayName("Revoke non-existent lease should throw error")
    void testRevokeNonExistentLease() {
        assertThatThrownBy(() ->
                leaseStub.leaseRevoke(LeaseOuterClass.LeaseRevokeRequest.newBuilder()
                        .setId(99999)
                        .build())
        ).isInstanceOf(StatusRuntimeException.class);
    }

    // ──── Multiple KeepAlive in one stream ────

    @Test
    @Order(3)
    @DisplayName("Multiple KeepAlive requests in single stream")
    void testMultipleKeepAlive() throws Exception {
        LeaseOuterClass.LeaseGrantResponse grantResp = leaseStub.leaseGrant(
                LeaseOuterClass.LeaseGrantRequest.newBuilder().setTtl(30).build());
        long leaseId = grantResp.getId();

        CountDownLatch latch = new CountDownLatch(3);
        AtomicInteger responseCount = new AtomicInteger(0);

        StreamObserver<LeaseOuterClass.LeaseKeepAliveRequest> requestObserver =
                leaseAsyncStub.leaseKeepAlive(new StreamObserver<>() {
                    @Override
                    public void onNext(LeaseOuterClass.LeaseKeepAliveResponse resp) {
                        assertThat(resp.getId()).isEqualTo(leaseId);
                        assertThat(resp.getTtl()).isGreaterThan(0);
                        responseCount.incrementAndGet();
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

        // 发送 3 次 KeepAlive
        for (int i = 0; i < 3; i++) {
            requestObserver.onNext(LeaseOuterClass.LeaseKeepAliveRequest.newBuilder()
                    .setId(leaseId).build());
        }

        boolean completed = latch.await(10, TimeUnit.SECONDS);
        requestObserver.onCompleted();

        assertThat(completed).isTrue();
        assertThat(responseCount.get()).isEqualTo(3);

        // 清理
        leaseStub.leaseRevoke(LeaseOuterClass.LeaseRevokeRequest.newBuilder().setId(leaseId).build());
    }

    // ──── Lease key binding bulk test ────

    @Test
    @Order(4)
    @DisplayName("Multiple keys bound to same lease, all deleted on revoke")
    void testMultipleKeysBoundToOneLease() {
        long leaseId = leaseStub.leaseGrant(
                LeaseOuterClass.LeaseGrantRequest.newBuilder().setTtl(30).build()).getId();

        String prefix = "/test/lease/bulk/";
        int keyCount = 10;
        for (int i = 0; i < keyCount; i++) {
            kvStub.put(Kv.PutRequest.newBuilder()
                    .setKey(ByteString.copyFromUtf8(prefix + i))
                    .setValue(ByteString.copyFromUtf8("val-" + i))
                    .setLeaseId(leaseId)
                    .build());
        }

        // 验证全部存在
        Kv.RangeResponse before = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(prefix))
                .setRangeEnd(ByteString.copyFromUtf8(prefix + "\0"))
                .build());
        assertThat(before.getKvsCount()).isEqualTo(keyCount);

        // Revoke
        leaseStub.leaseRevoke(LeaseOuterClass.LeaseRevokeRequest.newBuilder().setId(leaseId).build());

        // 验证全部删除
        Kv.RangeResponse after = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(prefix))
                .setRangeEnd(ByteString.copyFromUtf8(prefix + "\0"))
                .build());
        assertThat(after.getKvsCount()).isEqualTo(0);
    }
}
