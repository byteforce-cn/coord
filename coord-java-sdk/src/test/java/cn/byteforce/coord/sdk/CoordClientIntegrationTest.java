package cn.byteforce.coord.sdk;

import coord.kv.KVGrpc;
import coord.kv.Kv;
import coord.kv.Kv.PutRequest;
import coord.kv.Kv.RangeRequest;
import coord.kv.Kv.DeleteRequest;
import coord.kv.Kv.PutResponse;
import coord.kv.Kv.RangeResponse;
import coord.kv.Kv.DeleteResponse;
import coord.lease.LeaseGrpc;
import coord.lease.LeaseOuterClass;
import coord.lease.LeaseOuterClass.LeaseGrantRequest;
import coord.lease.LeaseOuterClass.LeaseGrantResponse;
import coord.lease.LeaseOuterClass.LeaseRevokeRequest;
import coord.maintenance.MaintenanceGrpc;
import coord.maintenance.MaintenanceOuterClass;
import coord.maintenance.MaintenanceOuterClass.StatusRequest;
import coord.maintenance.MaintenanceOuterClass.StatusResponse;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * 集成测试 — 连接本机正在运行的 Agent 验证全链路 gRPC 代理。
 * <p>
 * Agent 代理 5 个核心 gRPC 服务：KV / Txn / Lease / Watch / Maintenance。
 * 本测试直接使用 proto stubs 验证 Agent→Server 转发路径。
 * <p>
 * 环境变量（默认值适配本地开发）：
 * <ul>
 *   <li>{@code COORD_AGENT_HOST} — Agent 地址，默认 {@code localhost}</li>
 *   <li>{@code COORD_AGENT_PORT} — Agent gRPC 端口，默认 {@code 19527}</li>
 * </ul>
 * <p>
 * 若 Agent 不可达，测试自动跳过（通过 {@code Assumptions}）。
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CoordClientIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(CoordClientIntegrationTest.class);

    private static String agentHost;
    private static int agentPort;
    private static ManagedChannel channel;
    private static KVGrpc.KVBlockingStub kvStub;
    private static LeaseGrpc.LeaseBlockingStub leaseStub;
    private static MaintenanceGrpc.MaintenanceBlockingStub maintStub;

    @BeforeAll
    static void setUp() {
        agentHost = System.getenv().getOrDefault("COORD_AGENT_HOST", "localhost");
        agentPort = Integer.parseInt(System.getenv().getOrDefault("COORD_AGENT_PORT", "19527"));

        channel = ManagedChannelBuilder.forAddress(agentHost, agentPort)
                .usePlaintext()
                .build();

        kvStub = KVGrpc.newBlockingStub(channel);
        leaseStub = LeaseGrpc.newBlockingStub(channel);
        maintStub = MaintenanceGrpc.newBlockingStub(channel);

        // Agent 可用性探测：尝试 Maintenance.Status 调用
        boolean agentAvailable;
        try {
            MaintenanceOuterClass.StatusResponse resp = maintStub
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .status(MaintenanceOuterClass.StatusRequest.getDefaultInstance());
            agentAvailable = resp != null && !resp.getRaftLeader().isEmpty();
            log.info("Agent reachable: raftLeader={}, sealStatus={}, revision={}",
                    resp.getRaftLeader(), resp.getSealStatus(), resp.getRevision());
        } catch (Exception e) {
            agentAvailable = false;
            log.warn("Agent not reachable at {}:{} — {}", agentHost, agentPort, e.toString());
        }

        assumeThat(agentAvailable)
                .as("Agent not reachable at %s:%d — skipping integration tests", agentHost, agentPort)
                .isTrue();

        log.info("Integration tests connected to Agent at {}:{}", agentHost, agentPort);
    }

    @AfterAll
    static void tearDown() throws InterruptedException {
        if (channel != null) {
            channel.shutdown();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    // ──── Maintenance: Status ────

    @Test
    @Order(1)
    void shouldGetAgentStatus() {
        MaintenanceOuterClass.StatusResponse resp = maintStub
                .withDeadlineAfter(10, TimeUnit.SECONDS)
                .status(MaintenanceOuterClass.StatusRequest.getDefaultInstance());

        assertThat(resp.getRaftLeader()).isNotEmpty();
        assertThat(resp.getRaftTerm()).isPositive();
        assertThat(resp.getSealStatus()).isEqualTo("unsealed");
        log.info("Agent Status: leader={}, term={}, revision={}",
                resp.getRaftLeader(), resp.getRaftTerm(), resp.getRevision());
    }

    // ──── KV: Put / Range / Delete ────

    @Test
    @Order(2)
    void shouldPutAndRangeKeyThroughAgent() {
        String key = "/integration/test-key-put-range";
        String value = "hello-agent-42";

        // Put
        PutRequest putReq = PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8(value))
                .build();
        Kv.PutResponse putResp = kvStub
                .withDeadlineAfter(10, TimeUnit.SECONDS)
                .put(putReq);
        assertThat(putResp.getRevision()).isPositive();
        log.info("Put {} -> revision={}", key, putResp.getRevision());

        // Range
        RangeRequest rangeReq = RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .build();
        Kv.RangeResponse rangeResp = kvStub
                .withDeadlineAfter(10, TimeUnit.SECONDS)
                .range(rangeReq);
        assertThat(rangeResp.getKvsCount()).isEqualTo(1);
        assertThat(rangeResp.getKvs(0).getKey().toStringUtf8()).isEqualTo(key);
        assertThat(rangeResp.getKvs(0).getValue().toStringUtf8()).isEqualTo(value);
    }

    @Test
    @Order(3)
    void shouldDeleteKeyThroughAgent() {
        String key = "/integration/test-key-to-delete";

        // Put first
        kvStub.put(PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8("will-be-deleted"))
                .build());

        // Verify exists
        Kv.RangeResponse before = kvStub.range(RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .build());
        assertThat(before.getKvsCount()).isEqualTo(1);

        // Delete
        Kv.DeleteResponse delResp = kvStub.delete(DeleteRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .build());
        assertThat(delResp.getDeleted()).isPositive();
        log.info("Deleted {} keys", delResp.getDeleted());

        // Verify gone
        Kv.RangeResponse after = kvStub.range(RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .build());
        assertThat(after.getKvsCount()).isEqualTo(0);
    }

    @Test
    @Order(4)
    void shouldRangeMultipleKeysWithPrefix() {
        String prefix = "/integration/prefix/";

        // Put 3 keys
        for (int i = 1; i <= 3; i++) {
            kvStub.put(PutRequest.newBuilder()
                    .setKey(ByteString.copyFromUtf8(prefix + "key-" + i))
                    .setValue(ByteString.copyFromUtf8("value-" + i))
                    .build());
        }

        // Range with prefix (range_end = key with last byte incremented)
        Kv.RangeResponse resp = kvStub.range(RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(prefix))
                .setRangeEnd(ByteString.copyFrom(new byte[]{(byte) 0xFF}))
                .build());
        assertThat(resp.getKvsCount()).isGreaterThanOrEqualTo(3);
        log.info("Prefix range returned {} kvs", resp.getKvsCount());
    }

    @Test
    @Order(5)
    void shouldReturnEmptyForNonExistentKey() {
        Kv.RangeResponse resp = kvStub.range(RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("/nonexistent/key/xyz"))
                .build());
        assertThat(resp.getKvsCount()).isEqualTo(0);
    }

    // ──── Lease: Grant / Revoke ────

    @Test
    @Order(6)
    void shouldGrantAndRevokeLeaseThroughAgent() {
        // Grant lease (TTL 10s)
        LeaseGrantRequest grantReq = LeaseGrantRequest.newBuilder()
                .setTtl(10)
                .build();
        LeaseGrantResponse grantResp = leaseStub
                .withDeadlineAfter(10, TimeUnit.SECONDS)
                .leaseGrant(grantReq);

        long leaseId = grantResp.getId();
        assertThat(leaseId).isPositive();
        assertThat(grantResp.getTtl()).isEqualTo(10);
        log.info("Lease granted: id={}, ttl=10", leaseId);

        // Revoke
        LeaseRevokeRequest revokeReq = LeaseRevokeRequest.newBuilder()
                .setId(leaseId)
                .build();
        try {
            leaseStub.leaseRevoke(revokeReq);
            log.info("Lease {} revoked", leaseId);
        } catch (io.grpc.StatusRuntimeException e) {
            // NOT_FOUND is acceptable if lease already expired
            assertThat(e.getStatus().getCode())
                    .isIn(io.grpc.Status.Code.OK, io.grpc.Status.Code.NOT_FOUND);
            log.info("Lease revoke result: {}", e.getStatus().getCode());
        }
    }

    // ──── Channel lifecycle ────

    @Test
    @Order(7)
    void shouldReconnectWithNewChannel() throws InterruptedException {
        ManagedChannel tempChannel = ManagedChannelBuilder.forAddress(agentHost, agentPort)
                .usePlaintext()
                .build();

        try {
            MaintenanceGrpc.MaintenanceBlockingStub tempMaint = MaintenanceGrpc.newBlockingStub(tempChannel);
            StatusResponse resp = tempMaint
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .status(StatusRequest.getDefaultInstance());
            assertThat(resp.getRaftLeader()).isNotEmpty();
        } finally {
            tempChannel.shutdown();
            tempChannel.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    // ──── Concurrent access ────

    @Test
    @Order(8)
    void shouldHandleConcurrentKvOperations() throws InterruptedException {
        int numThreads = 4;
        java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(numThreads);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            new Thread(() -> {
                try {
                    String key = "/integration/concurrent/thread-" + threadId;
                    kvStub.put(PutRequest.newBuilder()
                            .setKey(ByteString.copyFromUtf8(key))
                            .setValue(ByteString.copyFromUtf8("val-" + threadId))
                            .build());
                    Kv.RangeResponse resp = kvStub.range(RangeRequest.newBuilder()
                            .setKey(ByteString.copyFromUtf8(key))
                            .build());
                    if (resp.getKvsCount() == 1) {
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    log.error("Thread {} failed: {}", threadId, e.getMessage());
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        latch.await(30, TimeUnit.SECONDS);
        assertThat(successCount.get()).isEqualTo(numThreads);
        log.info("Concurrent test: {}/{} succeeded", successCount.get(), numThreads);
    }
}
