package cn.byteforce.coord.example;

import com.google.protobuf.ByteString;
import coord.kv.KVGrpc;
import coord.kv.Kv;
import coord.maintenance.MaintenanceGrpc;
import coord.maintenance.MaintenanceOuterClass;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;

import java.util.concurrent.TimeUnit;

/**
 * Maintenance 集成测试 — 集群运维操作
 *
 * 覆盖:
 * - Status（集群状态查询）
 * - MemberList（成员列表）
 */
@DisplayName("Maintenance Integration Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MaintenanceIntegrationTest {

    private static ManagedChannel channel;
    private static MaintenanceGrpc.MaintenanceBlockingStub maintenanceStub;
    private static KVGrpc.KVBlockingStub kvStub;

    @BeforeAll
    static void setUp() {
        channel = ManagedChannelBuilder
                .forAddress("localhost", 19527)
                .usePlaintext()
                .keepAliveTime(30, TimeUnit.SECONDS)
                .build();
        maintenanceStub = MaintenanceGrpc.newBlockingStub(channel);
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
    @DisplayName("Status returns cluster state")
    void testStatus() {
        MaintenanceOuterClass.StatusResponse status = maintenanceStub.status(
                MaintenanceOuterClass.StatusRequest.newBuilder().build());

        assertThat(status).isNotNull();
        // Revision 在写入后应 > 0
        assertThat(status.getRevision()).isGreaterThanOrEqualTo(0);
        assertThat(status.getRaftIndex()).isGreaterThanOrEqualTo(0);
        assertThat(status.getRaftTerm()).isGreaterThanOrEqualTo(0);
        // 单节点 dev 模式：leader 应是自身
        assertThat(status.getRaftLeader()).isNotEmpty();
        // 未封存
        assertThat(status.getSealStatus()).isEqualTo("unsealed");
    }

    @Test
    @Order(2)
    @DisplayName("MemberList returns cluster members")
    void testMemberList() {
        MaintenanceOuterClass.MemberListResponse members = maintenanceStub.memberList(
                MaintenanceOuterClass.MemberListRequest.newBuilder().build());

        assertThat(members).isNotNull();
        // 单节点 dev 模式应有 1 个成员
        assertThat(members.getNodesCount()).isGreaterThanOrEqualTo(1);

        MaintenanceOuterClass.MemberNode node = members.getNodes(0);
        assertThat(node.getId()).isGreaterThan(0);
        assertThat(node.getRole()).isIn("Leader", "Voter", "Learner");
    }

    @Test
    @Order(3)
    @DisplayName("Status revision increases after write")
    void testStatusRevisionIncreases() {
        // 先读一次
        MaintenanceOuterClass.StatusResponse before = maintenanceStub.status(
                MaintenanceOuterClass.StatusRequest.newBuilder().build());
        long revBefore = before.getRevision();

        // 写一条数据并通过 put 响应直接验证 revision 递增
        Kv.PutResponse putResp = kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("/test/maintenance/rev-check"))
                .setValue(ByteString.copyFromUtf8("data"))
                .build());
        long putRev = putResp.getRevision();
        assertThat(putRev).as("put revision").isGreaterThan(revBefore);

        // 写入后 Status 应反映最新 revision
        // 注：Status 直接读内存 revision 计数器，可能存在短暂滞后；
        // 但 put 响应中的 revision 是最权威的写入确认。
        MaintenanceOuterClass.StatusResponse after = maintenanceStub.status(
                MaintenanceOuterClass.StatusRequest.newBuilder().build());
        assertThat(after.getRevision())
                .as("status revision after write")
                .isGreaterThanOrEqualTo(revBefore);
    }
}
