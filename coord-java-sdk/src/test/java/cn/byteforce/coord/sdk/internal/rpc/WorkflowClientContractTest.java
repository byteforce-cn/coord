package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.contracts.workflow.v1.SuspensionMeta;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowApproveRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowApproveResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowGetInstanceDetailRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowGetInstanceDetailResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowGetStatusRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowGetStatusResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowGrpc;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowListPendingTasksRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowListPendingTasksResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowStartRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowStartResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowWithdrawRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowWithdrawResponse;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import cn.byteforce.coord.sdk.workflow.ApprovalInstanceDetail;
import cn.byteforce.coord.sdk.workflow.PendingApprovalTask;
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

import java.util.List;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 契约测试 —— 验证 Java SDK 与 coord 契约对齐（业务方为 Java）。
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
                CoordConfig.builder().agentHost("localhost").build(),
                // 轮询跑在受管理的执行器上（生产由 ThreadPoolManager 提供）
                Executors.newVirtualThreadPerTaskExecutor());
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

    @Test
    @DisplayName("approve 发送 workflow_id/task_id/idempotency_key")
    void approveSendsIdempotencyKeyAndTaskId() {
        wf.approve("inst-1", "task-9", "同意", "idem-abc");

        assertThat(fake.lastApprove.getWorkflowId()).isEqualTo("inst-1");
        assertThat(fake.lastApprove.getTaskId()).isEqualTo("task-9");
        assertThat(fake.lastApprove.getComment()).isEqualTo("同意");
        assertThat(fake.lastApprove.getIdempotencyKey()).isEqualTo("idem-abc");
    }

    @Test
    @DisplayName("命令响应 status 映射为实例状态")
    void commandStatusMapped() {
        fake.nextApproveStatus = "COMPLETED";

        String status = wf.approve("inst-1", "task-9", null, "idem-abc");

        assertThat(status).isEqualTo("COMPLETED");
    }

    @Test
    @DisplayName("withdraw 携带 comment 与 idempotency_key")
    void withdrawCarriesCommentAndKey() {
        fake.nextWithdrawStatus = "CANCELLED";

        String status = wf.withdraw("inst-1", "撤回", "idem-wd");

        assertThat(status).isEqualTo("CANCELLED");
        assertThat(fake.lastWithdraw.getWorkflowId()).isEqualTo("inst-1");
        assertThat(fake.lastWithdraw.getComment()).isEqualTo("撤回");
        assertThat(fake.lastWithdraw.getIdempotencyKey()).isEqualTo("idem-wd");
    }

    @Test
    @DisplayName("listPendingTasks 发送 page_size/page_token 并映射席位")
    void listPendingTasksMapsPageToken() {
        fake.nextPendingTasks = WorkflowListPendingTasksResponse.newBuilder()
                .addTasks(cn.byteforce.coord.contracts.workflow.v1.PendingApprovalTask.newBuilder()
                        .setInstanceId("inst-1")
                        .setTaskId("task-9")
                        .setNode("approve")
                        .setBranchId("br-1")
                        .setRoundId("rd-1")
                        .setKind("APPROVAL")
                        .setStatus("PENDING")
                        .setAssignee("alice")
                        .setDeadlineMs(1755000000000L)
                        .setCreatedAtMs(1754000000000L)
                        .build())
                .setNextPageToken("next-2")
                .build();

        List<PendingApprovalTask> tasks = wf.listPendingTasks(20, "page-1");

        assertThat(fake.lastListPendingTasks.getPageSize()).isEqualTo(20);
        assertThat(fake.lastListPendingTasks.getPageToken()).isEqualTo("page-1");
        assertThat(tasks).hasSize(1);
        PendingApprovalTask t = tasks.get(0);
        assertThat(t.instanceId()).isEqualTo("inst-1");
        assertThat(t.taskId()).isEqualTo("task-9");
        assertThat(t.node()).isEqualTo("approve");
        assertThat(t.branchId()).isEqualTo("br-1");
        assertThat(t.roundId()).isEqualTo("rd-1");
        assertThat(t.kind()).isEqualTo("APPROVAL");
        assertThat(t.status()).isEqualTo("PENDING");
        assertThat(t.assignee()).isEqualTo("alice");
        assertThat(t.deadlineMs()).isEqualTo(1755000000000L);
        assertThat(t.createdAtMs()).isEqualTo(1754000000000L);
    }

    @Test
    @DisplayName("getInstanceDetail 映射 branches/tasks/rounds")
    void getInstanceDetailMapsBranchesTasksRounds() {
        fake.nextDetail = WorkflowGetInstanceDetailResponse.newBuilder()
                .setInstanceId("inst-1")
                .setStatus("RUNNING")
                .setInitiator("bob")
                .setRevision(7L)
                .setDefinitionName("order-approval")
                .addBranches(cn.byteforce.coord.contracts.workflow.v1.ApprovalBranch.newBuilder()
                        .setId("br-1")
                        .setNode("approve")
                        .setAttempt(1)
                        .setStatus("ACTIVE")
                        .setJoin("all")
                        .setReject("any")
                        .addRoundIds("rd-1")
                        .build())
                .addTasks(cn.byteforce.coord.contracts.workflow.v1.ApprovalTask.newBuilder()
                        .setId("task-9")
                        .setBranchId("br-1")
                        .setRoundId("rd-1")
                        .setKind("APPROVAL")
                        .setStatus("PENDING")
                        .setAssignee("alice")
                        .addCandidates("alice")
                        .addCandidates("carol")
                        .setDeadlineMs(1755000000000L)
                        .build())
                .addRounds(cn.byteforce.coord.contracts.workflow.v1.ApprovalRound.newBuilder()
                        .setId("rd-1")
                        .setBranchId("br-1")
                        .setNumber(2)
                        .setStatus("IN_PROGRESS")
                        .setTotalCount(3)
                        .setCompletedCount(1)
                        .setRejectedCount(0)
                        .build())
                .build();

        ApprovalInstanceDetail d = wf.getInstanceDetail("inst-1");

        assertThat(fake.lastDetail.getWorkflowId()).isEqualTo("inst-1");
        assertThat(d.instanceId()).isEqualTo("inst-1");
        assertThat(d.status()).isEqualTo("RUNNING");
        assertThat(d.initiator()).isEqualTo("bob");
        assertThat(d.revision()).isEqualTo(7L);
        assertThat(d.definitionName()).isEqualTo("order-approval");
        assertThat(d.branches()).hasSize(1);
        assertThat(d.branches().get(0).id()).isEqualTo("br-1");
        assertThat(d.branches().get(0).node()).isEqualTo("approve");
        assertThat(d.branches().get(0).attempt()).isEqualTo(1);
        assertThat(d.branches().get(0).join()).isEqualTo("all");
        assertThat(d.branches().get(0).reject()).isEqualTo("any");
        assertThat(d.branches().get(0).roundIds()).containsExactly("rd-1");
        assertThat(d.tasks()).hasSize(1);
        assertThat(d.tasks().get(0).id()).isEqualTo("task-9");
        assertThat(d.tasks().get(0).branchId()).isEqualTo("br-1");
        assertThat(d.tasks().get(0).roundId()).isEqualTo("rd-1");
        assertThat(d.tasks().get(0).assignee()).isEqualTo("alice");
        assertThat(d.tasks().get(0).candidates()).containsExactly("alice", "carol");
        assertThat(d.tasks().get(0).deadlineMs()).isEqualTo(1755000000000L);
        assertThat(d.rounds()).hasSize(1);
        assertThat(d.rounds().get(0).id()).isEqualTo("rd-1");
        assertThat(d.rounds().get(0).branchId()).isEqualTo("br-1");
        assertThat(d.rounds().get(0).number()).isEqualTo(2);
        assertThat(d.rounds().get(0).status()).isEqualTo("IN_PROGRESS");
        assertThat(d.rounds().get(0).totalCount()).isEqualTo(3);
        assertThat(d.rounds().get(0).completedCount()).isEqualTo(1);
        assertThat(d.rounds().get(0).rejectedCount()).isEqualTo(0);
    }

    /** In-process fake Workflow service —— 仅实现被测 RPC，其余走 gRPC 默认 UNIMPLEMENTED。 */
    static class FakeWorkflowService extends WorkflowGrpc.WorkflowImplBase {

        WorkflowStartRequest lastStart;
        WorkflowGetStatusResponse nextStatus = WorkflowGetStatusResponse.newBuilder().build();

        WorkflowApproveRequest lastApprove;
        String nextApproveStatus = "RUNNING";

        WorkflowWithdrawRequest lastWithdraw;
        String nextWithdrawStatus = "RUNNING";

        WorkflowListPendingTasksRequest lastListPendingTasks;
        WorkflowListPendingTasksResponse nextPendingTasks =
                WorkflowListPendingTasksResponse.newBuilder().build();

        WorkflowGetInstanceDetailRequest lastDetail;
        WorkflowGetInstanceDetailResponse nextDetail =
                WorkflowGetInstanceDetailResponse.newBuilder().build();

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

        @Override
        public void approve(WorkflowApproveRequest request,
                            StreamObserver<WorkflowApproveResponse> responseObserver) {
            lastApprove = request;
            responseObserver.onNext(WorkflowApproveResponse.newBuilder()
                    .setInstanceId(request.getWorkflowId())
                    .setStatus(nextApproveStatus)
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void withdraw(WorkflowWithdrawRequest request,
                             StreamObserver<WorkflowWithdrawResponse> responseObserver) {
            lastWithdraw = request;
            responseObserver.onNext(WorkflowWithdrawResponse.newBuilder()
                    .setInstanceId(request.getWorkflowId())
                    .setStatus(nextWithdrawStatus)
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void listPendingTasks(WorkflowListPendingTasksRequest request,
                                     StreamObserver<WorkflowListPendingTasksResponse> responseObserver) {
            lastListPendingTasks = request;
            responseObserver.onNext(nextPendingTasks);
            responseObserver.onCompleted();
        }

        @Override
        public void getInstanceDetail(WorkflowGetInstanceDetailRequest request,
                                      StreamObserver<WorkflowGetInstanceDetailResponse> responseObserver) {
            lastDetail = request;
            responseObserver.onNext(nextDetail);
            responseObserver.onCompleted();
        }
    }
}
