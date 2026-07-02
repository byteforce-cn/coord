package cn.byteforce.coord.example;

import com.google.protobuf.ByteString;
import coord.kv.KVGrpc;
import coord.kv.Kv;
import coord.lease.LeaseGrpc;
import coord.lease.LeaseOuterClass;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Lease 集成测试 — TDD RED 阶段
 *
 * 验证 Java 应用通过 Agent 的 Lease 操作:
 * Grant → KeepAlive → Revoke
 */
@DisplayName("Lease Integration Tests (Java → Agent gRPC)")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class LeaseIntegrationTest {

    private static ManagedChannel channel;
    private static LeaseGrpc.LeaseBlockingStub leaseStub;
    private static LeaseGrpc.LeaseStub leaseAsyncStub;
    private static KVGrpc.KVBlockingStub kvStub;

    @BeforeAll
    static void setUp() {
        channel = ManagedChannelBuilder
                .forAddress("localhost", 19527)
                .usePlaintext()
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

    @Test
    @Order(1)
    @DisplayName("Grant a lease and verify TTL")
    void testLeaseGrant() {
        LeaseOuterClass.LeaseGrantResponse resp = leaseStub.leaseGrant(
                LeaseOuterClass.LeaseGrantRequest.newBuilder()
                        .setTtl(30)
                        .build());

        assertThat(resp.getId()).isGreaterThan(0);
        assertThat(resp.getTtl()).isEqualTo(30);
    }

    @Test
    @Order(2)
    @DisplayName("Bind a key to a lease, key expires when lease revoked")
    void testLeaseBindingAndRevoke() {
        LeaseOuterClass.LeaseGrantResponse grantResp = leaseStub.leaseGrant(
                LeaseOuterClass.LeaseGrantRequest.newBuilder().setTtl(30).build());
        long leaseId = grantResp.getId();

        String key = "/test/lease/bound-key";
        kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8("lease-value"))
                .setLeaseId(leaseId)
                .build());

        Kv.RangeResponse r1 = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key)).build());
        assertThat(r1.getKvsCount()).isEqualTo(1);

        leaseStub.leaseRevoke(LeaseOuterClass.LeaseRevokeRequest.newBuilder()
                .setId(leaseId).build());

        Kv.RangeResponse r2 = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key)).build());
        assertThat(r2.getKvsCount()).isEqualTo(0);
    }

    @Test
    @Order(3)
    @DisplayName("Lease KeepAlive via bidirectional stream")
    void testLeaseKeepAlive() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicLong receivedId = new AtomicLong();
        AtomicLong receivedTtl = new AtomicLong();

        LeaseOuterClass.LeaseGrantResponse grantResp = leaseStub.leaseGrant(
                LeaseOuterClass.LeaseGrantRequest.newBuilder().setTtl(30).build());
        long leaseId = grantResp.getId();

        StreamObserver<LeaseOuterClass.LeaseKeepAliveRequest> requestObserver =
                leaseAsyncStub.leaseKeepAlive(new StreamObserver<>() {
                    @Override
                    public void onNext(LeaseOuterClass.LeaseKeepAliveResponse resp) {
                        receivedId.set(resp.getId());
                        receivedTtl.set(resp.getTtl());
                        latch.countDown();
                    }

                    @Override
                    public void onError(Throwable t) {
                        latch.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        latch.countDown();
                    }
                });

        requestObserver.onNext(LeaseOuterClass.LeaseKeepAliveRequest.newBuilder()
                .setId(leaseId).build());

        boolean completed = latch.await(5, TimeUnit.SECONDS);
        requestObserver.onCompleted();

        assertThat(completed).isTrue();
        assertThat(receivedId.get()).isEqualTo(leaseId);
        assertThat(receivedTtl.get()).isGreaterThan(0);
    }
}
