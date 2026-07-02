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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 服务注册发现扩展集成测试 — 多服务、多实例场景
 *
 * 覆盖:
 * - 多服务独立注册
 * - 单服务多实例
 * - 实例健康状态更新
 * - 跨服务隔离验证
 * - 服务元数据存储
 */
@DisplayName("Service Registry Advanced Integration Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ServiceRegistryAdvancedTest {

    private static final String REGISTRY_PREFIX = "/_registry/services/";

    private static ManagedChannel channel;
    private static KVGrpc.KVBlockingStub kvStub;
    private static LeaseGrpc.LeaseBlockingStub leaseStub;
    private static final Map<String, Long> serviceLeases = new HashMap<>();

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
        for (long leaseId : serviceLeases.values()) {
            try { leaseStub.leaseRevoke(LeaseOuterClass.LeaseRevokeRequest.newBuilder().setId(leaseId).build()); }
            catch (Exception ignored) {}
        }
        if (channel != null) {
            channel.shutdown();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private long grantLease() {
        return leaseStub.leaseGrant(
                LeaseOuterClass.LeaseGrantRequest.newBuilder().setTtl(60).build()).getId();
    }

    private void registerInstance(String serviceName, String instanceId, String host, int port, String status) {
        long leaseId = grantLease();
        serviceLeases.put(serviceName + "/" + instanceId, leaseId);

        String key = instanceKey(serviceName, instanceId);
        String json = String.format(
                "{\"service\":\"%s\",\"instance\":\"%s\",\"host\":\"%s\",\"port\":%d,\"status\":\"%s\"}",
                serviceName, instanceId, host, port, status);

        kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8(json))
                .setLeaseId(leaseId)
                .build());
    }

    private String instanceKey(String serviceName, String instanceId) {
        return REGISTRY_PREFIX + serviceName + "/instances/" + instanceId;
    }

    private String servicePrefix(String serviceName) {
        return REGISTRY_PREFIX + serviceName + "/";
    }

    // ──── 多服务独立注册 ────

    @Test
    @Order(1)
    @DisplayName("Multiple services registered independently")
    void testMultipleServicesIndependent() {
        registerInstance("order-service", "ord-1", "10.0.1.10", 8080, "UP");
        registerInstance("order-service", "ord-2", "10.0.1.11", 8080, "UP");
        registerInstance("payment-service", "pay-1", "10.0.2.10", 8081, "UP");
        registerInstance("inventory-service", "inv-1", "10.0.3.10", 8082, "UP");

        // 验证 order-service 有 2 个实例
        Kv.RangeResponse orderInstances = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(servicePrefix("order-service")))
                .setRangeEnd(ByteString.copyFromUtf8(servicePrefix("order-service") + "\0"))
                .build());
        assertThat(orderInstances.getKvsCount()).isEqualTo(2);

        // 验证 payment-service 有 1 个实例
        Kv.RangeResponse payInstances = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(servicePrefix("payment-service")))
                .setRangeEnd(ByteString.copyFromUtf8(servicePrefix("payment-service") + "\0"))
                .build());
        assertThat(payInstances.getKvsCount()).isEqualTo(1);

        // 验证 inventory-service 有 1 个实例
        Kv.RangeResponse invInstances = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(servicePrefix("inventory-service")))
                .setRangeEnd(ByteString.copyFromUtf8(servicePrefix("inventory-service") + "\0"))
                .build());
        assertThat(invInstances.getKvsCount()).isEqualTo(1);

        // 验证跨服务隔离：列所有注册的服务
        Kv.RangeResponse allServices = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(REGISTRY_PREFIX))
                .setRangeEnd(ByteString.copyFromUtf8(REGISTRY_PREFIX + "\0"))
                .build());
        assertThat(allServices.getKvsCount()).isEqualTo(4);
    }

    @Test
    @Order(2)
    @DisplayName("Instance health status update")
    void testInstanceHealthStatusUpdate() {
        String serviceName = "health-check-service";
        String instanceKey = instanceKey(serviceName, "hc-1");
        long leaseId = grantLease();
        serviceLeases.put(serviceName + "/hc-1", leaseId);

        // 初始注册为 UP
        kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(instanceKey))
                .setValue(ByteString.copyFromUtf8(
                        "{\"service\":\"health-check-service\",\"instance\":\"hc-1\",\"host\":\"10.0.1.50\",\"port\":8080,\"status\":\"UP\"}"))
                .setLeaseId(leaseId)
                .build());

        // 读回验证
        Kv.KeyValue kv = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(instanceKey))
                .build()).getKvs(0);
        assertThat(kv.getValue().toStringUtf8()).contains("\"status\":\"UP\"");

        // 更新为 DOWN（注意：这只是 value 更新，不改变 lease 绑定）
        kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(instanceKey))
                .setValue(ByteString.copyFromUtf8(
                        "{\"service\":\"health-check-service\",\"instance\":\"hc-1\",\"host\":\"10.0.1.50\",\"port\":8080,\"status\":\"DOWN\"}"))
                .build());

        Kv.KeyValue updated = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(instanceKey))
                .build()).getKvs(0);
        assertThat(updated.getValue().toStringUtf8()).contains("\"status\":\"DOWN\"");
        // version 应递增
        assertThat(updated.getVersion()).isGreaterThan(kv.getVersion());
    }

    @Test
    @Order(3)
    @DisplayName("Discover all registered services via listing")
    void testDiscoverAllServices() {
        // 这个测试是增量的：前面的测试已注册了多个服务
        // 这里只验证当前能发现的服务数量
        int countBefore = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(REGISTRY_PREFIX))
                .setRangeEnd(ByteString.copyFromUtf8(REGISTRY_PREFIX + "\0"))
                .build()).getKvsCount();
        // 注册多个不同服务
        registerInstance("svc-alpha", "a-1", "10.0.1.1", 8001, "UP");
        registerInstance("svc-alpha", "a-2", "10.0.1.2", 8001, "UP");
        registerInstance("svc-beta", "b-1", "10.0.2.1", 8002, "UP");
        registerInstance("svc-gamma", "g-1", "10.0.3.1", 8003, "UP");

        // 全量列出：应至少有刚刚注册的 4 个实例
        Kv.RangeResponse all = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(REGISTRY_PREFIX))
                .setRangeEnd(ByteString.copyFromUtf8(REGISTRY_PREFIX + "\0"))
                .build());
        // 前面的测试可能已注册服务（跨测试共享状态），所以至少 ≥4
        assertThat(all.getKvsCount()).isGreaterThanOrEqualTo(4);

        // 每个实例的 JSON 应包含 host 和 port
        for (Kv.KeyValue kv : all.getKvsList()) {
            String json = kv.getValue().toStringUtf8();
            assertThat(json).contains("\"host\"");
            assertThat(json).contains("\"port\"");
        }
    }

    @Test
    @Order(4)
    @DisplayName("Instance auto-removed when lease revoked (single)")
    void testSingleInstanceRemovedOnLeaseRevoke() {
        long leaseId = grantLease();
        String key = instanceKey("temp-svc", "temp-1");

        kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8("{\"service\":\"temp-svc\",\"instance\":\"temp-1\",\"host\":\"10.0.9.9\",\"port\":9000,\"status\":\"UP\"}"))
                .setLeaseId(leaseId)
                .build());

        assertThat(kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key)).build()).getKvsCount()).isEqualTo(1);

        leaseStub.leaseRevoke(LeaseOuterClass.LeaseRevokeRequest.newBuilder().setId(leaseId).build());

        assertThat(kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key)).build()).getKvsCount()).isEqualTo(0);
    }
}
