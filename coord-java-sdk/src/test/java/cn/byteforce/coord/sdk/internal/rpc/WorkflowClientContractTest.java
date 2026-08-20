package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.sdk.internal.proto.SuspensionMeta;
import cn.byteforce.coord.sdk.internal.proto.WorkflowGetStatusRequest;
import cn.byteforce.coord.sdk.internal.proto.WorkflowGetStatusResponse;
import cn.byteforce.coord.sdk.internal.proto.WorkflowGrpc;
import cn.byteforce.coord.sdk.internal.proto.WorkflowStartRequest;
import cn.byteforce.coord.sdk.internal.proto.WorkflowStartResponse;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import cn.byteforce.coord.sdk.workflow.WorkflowClient;
import cn.byteforce.coord.sdk.workflow.WorkflowStatus;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * ISSUE-010 契约测试 —— 验证 Java SDK 与 coord 契约对齐（业务方为 Java）。
 *
 * <p>针对 in-process stub server 验证：
 * <ul>
 *   <li>{@code startByDefinition} 真契约：发送 {@code definition_id}（非 DSL 占位包装），携带 input</li>
 *   <li>{@code start} 保持发送 {@code definition_dsl}</li>
 *   <li>{@code getStatus} 映射 {@code currentStateName} + {@code suspension}（SuspensionMeta）</li>
 * </ul>
 */
class WorkflowClientContractTest {

    private Server server;
    private ManagedChannel channel;
    private WorkflowClient wf;
    private FakeWorkflowService fake;

    @BeforeEach
    void setUp() throws Exception {
        fake = new FakeWorkflowService();
        server = ServerBuilder.forPort(0).addService(fake).build().start();
        channel = ManagedChannelBuilder.forAddress("localhost", server.getPort())
                .usePlaintext()
                .build();

        AgentChannelManager channelManager = mock(AgentChannelManager.class);
        when(channelManager.getChannel()).thenReturn(channel);

        wf = new WorkflowClientImpl(
                channelManager,
                new ErrorMapper(),
                new RetryTemplate(),
                new ObservabilityProvider() {
                },
                CoordConfig.builder().agentHost("localhost").build());
    }

    @AfterEach
    void tearDown() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    @Test
    @DisplayName("startByDefinition 发送 definition_id（真契约），携带 input，无 DSL")
    void startByDefinitionSendsDefinitionIdNotDsl() {
        wf.startByDefinition("icps-flow-123", "{\"orderId\":\"1\"}".getBytes());

        assertThat(fake.lastStart.getDefinitionId()).isEqualTo("icps-flow-123");
        assertThat(fake.lastStart.getDefinitionDsl()).isEmpty();
        assertThat(fake.lastStart.getInput().toStringUtf8()).isEqualTo("{\"orderId\":\"1\"}");
    }

    @Test
    @DisplayName("start 保持发送 definition_dsl（内联 deploy + start）")
    void startSendsDslNotDefinitionId() {
        wf.start("{\"id\":\"wf-x\"}", null);

        assertThat(fake.lastStart.getDefinitionDsl()).isEqualTo("{\"id\":\"wf-x\"}");
        assertThat(fake.lastStart.getDefinitionId()).isEmpty();
    }

    @Test
    @DisplayName("getStatus 映射 currentStateName + suspension（挂起元信息）")
    void getStatusMapsCurrentStateNameAndSuspension() {
        fake.nextStatus = WorkflowGetStatusResponse.newBuilder()
                .setWorkflowId("inst-1")
                .setStatus("SUSPENDED")
                .setCurrentStateName("approve")
                .setSuspension(SuspensionMeta.newBuilder()
                        .setReason("listen")
                        .setExpectedSignal("icps.approval.approved")
                        .setEventType("icps.approval.approved")
                        .setUntilMs(1755000000000L)
                        .build())
                .build();

        WorkflowStatus s = wf.getStatus("inst-1");

        assertThat(s.currentStateName()).isEqualTo("approve");
        assertThat(s.suspension()).isNotNull();
        assertThat(s.suspension().reason()).isEqualTo("listen");
        assertThat(s.suspension().expectedSignal()).isEqualTo("icps.approval.approved");
        assertThat(s.suspension().eventType()).isEqualTo("icps.approval.approved");
        assertThat(s.suspension().untilMs()).isEqualTo(1755000000000L);
    }

    @Test
    @DisplayName("getStatus 无挂起时 suspension 为 null")
    void getStatusNoSuspensionReturnsNull() {
        fake.nextStatus = WorkflowGetStatusResponse.newBuilder()
                .setWorkflowId("inst-2")
                .setStatus("RUNNING")
                .setCurrentStateName("op1")
                .build();

        WorkflowStatus s = wf.getStatus("inst-2");

        assertThat(s.currentStateName()).isEqualTo("op1");
        assertThat(s.suspension()).isNull();
    }

    /** In-process fake Workflow service —— 仅实现 Start/GetStatus，其余走 gRPC 默认 UNIMPLEMENTED。 */
    static class FakeWorkflowService extends WorkflowGrpc.WorkflowImplBase {

        WorkflowStartRequest lastStart;
        WorkflowGetStatusResponse nextStatus = WorkflowGetStatusResponse.newBuilder().build();

        @Override
        public void start(WorkflowStartRequest request,
                          StreamObserver<WorkflowStartResponse> responseObserver) {
            lastStart = request;
            responseObserver.onNext(WorkflowStartResponse.newBuilder()
                    .setWorkflowId("inst-new").build());
            responseObserver.onCompleted();
        }

        @Override
        public void getStatus(WorkflowGetStatusRequest request,
                              StreamObserver<WorkflowGetStatusResponse> responseObserver) {
            responseObserver.onNext(nextStatus);
            responseObserver.onCompleted();
        }
    }
}
