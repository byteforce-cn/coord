package cn.byteforce.coord.example;

import com.google.protobuf.ByteString;
import coord.kv.KVGrpc;
import coord.kv.Kv;
import coord.lease.LeaseGrpc;
import coord.lease.LeaseOuterClass;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;

import java.util.concurrent.TimeUnit;

/**
 * 服务注册发现集成测试 — TDD RED 阶段
 *
 * 验证 Java 微服务通过 Agent 进行服务注册发现的完整流程:
 * 1. Grant Lease (TTL 30s)
 * 2. 注册服务实例 (绑定 Lease)
 * 3. 发现依赖服务 (Range prefix scan /_registry/)
 * 4. 注销 (Revoke Lease → 绑定 key 自动删除)
 *
 * 与架构文档 §9.3 一致。
 */
@DisplayName("Service Registry Integration Tests (Java → Agent gRPC)")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ServiceRegistryTest {

    private static final String REGISTRY_PREFIX = "/_registry/services/";

    private static ManagedChannel channel;
    private static KVGrpc.KVBlockingStub kvStub;
    private static LeaseGrpc.LeaseBlockingStub leaseStub;
    private static long leaseId;

    @BeforeAll
    static void setUp() {
        channel = ManagedChannelBuilder
                .forAddress("localhost", 19527)
                .usePlaintext()
                .keepAliveTime(30, TimeUnit.SECONDS)
                .build();
        kvStub = KVGrpc.newBlockingStub(channel);
        leaseStub = LeaseGrpc.newBlockingStub(channel);
    }

    @AfterAll
    static void tearDown() throws InterruptedException {
        if (leaseId > 0) {
            try { leaseStub.leaseRevoke(LeaseOuterClass.LeaseRevokeRequest.newBuilder().setId(leaseId).build()); }
            catch (Exception ignored) {}
        }
        if (channel != null) {
            channel.shutdown();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    @Order(1)
    @DisplayName("Register a service instance with lease binding")
    void testRegisterServiceInstance() {
        LeaseOuterClass.LeaseGrantResponse grantResp = leaseStub.leaseGrant(
                LeaseOuterClass.LeaseGrantRequest.newBuilder().setTtl(30).build());
        leaseId = grantResp.getId();
        assertThat(leaseId).isGreaterThan(0);

        String serviceName = "order-service";
        String instanceId = "node1";
        String instanceKey = REGISTRY_PREFIX + serviceName + "/instances/" + instanceId;
        String instanceJson = """
                {
                    "service": "order-service",
                    "instance": "node1",
                    "host": "10.0.1.10",
                    "port": 8080,
                    "status": "UP"
                }""";

        Kv.PutResponse putResp = kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(instanceKey))
                .setValue(ByteString.copyFromUtf8(instanceJson))
                .setLeaseId(leaseId)
                .build());
        assertThat(putResp.getRevision()).isGreaterThan(0);

        Kv.RangeResponse rangeResp = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(instanceKey))
                .build());
        assertThat(rangeResp.getKvsCount()).isEqualTo(1);
        Kv.KeyValue kv = rangeResp.getKvs(0);
        assertThat(kv.getKey().toStringUtf8()).isEqualTo(instanceKey);
        assertThat(kv.getValue().toStringUtf8()).contains("order-service");
        assertThat(kv.getLeaseId()).isEqualTo(leaseId);
    }

    @Test
    @Order(2)
    @DisplayName("Discover all instances of a service via prefix scan")
    void testDiscoverServiceInstances() {
        String serviceName = "payment-service";

        long tmpLease = leaseStub.leaseGrant(
                LeaseOuterClass.LeaseGrantRequest.newBuilder().setTtl(30).build()).getId();

        kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(REGISTRY_PREFIX + serviceName + "/instances/pay-1"))
                .setValue(ByteString.copyFromUtf8("""
                        {"service":"payment-service","instance":"pay-1","host":"10.0.1.20","port":8081}"""))
                .setLeaseId(tmpLease)
                .build());

        kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(REGISTRY_PREFIX + serviceName + "/instances/pay-2"))
                .setValue(ByteString.copyFromUtf8("""
                        {"service":"payment-service","instance":"pay-2","host":"10.0.1.21","port":8081}"""))
                .setLeaseId(tmpLease)
                .build());

        ByteString prefix = ByteString.copyFromUtf8(REGISTRY_PREFIX + serviceName + "/");
        ByteString rangeEnd = ByteString.copyFromUtf8(REGISTRY_PREFIX + serviceName + "0");

        Kv.RangeResponse resp = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(prefix)
                .setRangeEnd(rangeEnd)
                .build());

        assertThat(resp.getKvsCount()).isEqualTo(2);
        for (Kv.KeyValue kv : resp.getKvsList()) {
            assertThat(kv.getValue().toStringUtf8()).contains("payment-service");
        }

        leaseStub.leaseRevoke(LeaseOuterClass.LeaseRevokeRequest.newBuilder().setId(tmpLease).build());
    }

    @Test
    @Order(3)
    @DisplayName("Service instance is automatically removed when lease expires/revoked")
    void testInstanceRemovedOnLeaseRevoke() {
        long shortLease = leaseStub.leaseGrant(
                LeaseOuterClass.LeaseGrantRequest.newBuilder().setTtl(5).build()).getId();

        String key = REGISTRY_PREFIX + "temp-service/instances/temp-1";
        kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8("{\"service\":\"temp-service\"}"))
                .setLeaseId(shortLease)
                .build());

        Kv.RangeResponse r1 = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key)).build());
        assertThat(r1.getKvsCount()).isEqualTo(1);

        leaseStub.leaseRevoke(LeaseOuterClass.LeaseRevokeRequest.newBuilder().setId(shortLease).build());

        Kv.RangeResponse r2 = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key)).build());
        assertThat(r2.getKvsCount()).isEqualTo(0);
    }
}
