package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.CoordException;
import cn.byteforce.coord.sdk.ErrorCode;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.contracts.workflow.v1.SuspensionMeta;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowApproveRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowApproveResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowCcRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowCcResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowAddSignRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowAddSignResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowDelegateRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowDelegateResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowGetDefinitionRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowGetDefinitionResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowGetInstanceDetailRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowGetInstanceDetailResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowListInstancesRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowListInstancesResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowRemoveSignRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowRemoveSignResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowResumeWorkflowRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowResumeWorkflowResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowGetStatusRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowGetStatusResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowGetTaskRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowGetTaskResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowGrpc;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowHistoryRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowHistoryResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowListPendingTasksRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowListPendingTasksResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowListTasksRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowListTasksResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowStartRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowStartResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowUrgeRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowUrgeResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowWithdrawTaskRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowWithdrawTaskResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowWithdrawRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowWithdrawResponse;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import cn.byteforce.coord.sdk.workflow.AddAssigneePosition;
import cn.byteforce.coord.sdk.workflow.ApprovalHistoryEvent;
import cn.byteforce.coord.sdk.workflow.ApprovalInstanceDetail;
import cn.byteforce.coord.sdk.workflow.ApprovalTask;
import cn.byteforce.coord.sdk.workflow.InstancePage;
import cn.byteforce.coord.sdk.workflow.PendingApprovalTask;
import cn.byteforce.coord.sdk.workflow.TaskDetail;
import cn.byteforce.coord.sdk.workflow.TaskPage;
import cn.byteforce.coord.sdk.workflow.TaskQueryFilter;
import cn.byteforce.coord.sdk.workflow.WorkflowClient;
import cn.byteforce.coord.sdk.workflow.WorkflowDefinition;
import cn.byteforce.coord.sdk.workflow.WorkflowInstanceSummary;
import cn.byteforce.coord.sdk.workflow.WorkflowStatus;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
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
    @DisplayName("startByDefinition*OnBehalfOf 发送 on_behalf_of（服务代终端用户发起）")
    void startOnBehalfOfSendsRepresentedUser() {
        wf.startByDefinitionOnBehalfOf("icps-flow-123", "{}".getBytes(), "alice");
        assertThat(fake.lastStart.getDefinitionId()).isEqualTo("icps-flow-123");
        assertThat(fake.lastStart.getOnBehalfOf()).isEqualTo("alice");
        assertThat(fake.lastStart.getWorkflowId()).isEmpty();

        wf.startByDefinitionWithIdOnBehalfOf("icps-flow-123", "inst-9",
                "{}".getBytes(), "bob");
        assertThat(fake.lastStart.getWorkflowId()).isEqualTo("inst-9");
        assertThat(fake.lastStart.getDefinitionId()).isEqualTo("icps-flow-123");
        assertThat(fake.lastStart.getOnBehalfOf()).isEqualTo("bob");

        // 既有方法不发送 on_behalf_of（直接以 CCT 主体发起）
        wf.startByDefinition("icps-flow-123", null);
        assertThat(fake.lastStart.getOnBehalfOf()).isEmpty();
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
    @DisplayName("approveOnBehalfOf 发送 on_behalf_of（服务代终端用户）")
    void approveOnBehalfOfSendsRepresentedUser() {
        wf.approveOnBehalfOf("inst-1", "task-9", "代批", "idem-ob", "alice");

        assertThat(fake.lastApprove.getWorkflowId()).isEqualTo("inst-1");
        assertThat(fake.lastApprove.getTaskId()).isEqualTo("task-9");
        assertThat(fake.lastApprove.getIdempotencyKey()).isEqualTo("idem-ob");
        assertThat(fake.lastApprove.getOnBehalfOf()).isEqualTo("alice");
    }

    @Test
    @DisplayName("listPendingTasksFor 发送 assignee（服务代查待办）")
    void listPendingTasksForSendsAssignee() {
        wf.listPendingTasksFor("alice", 20, "page-1");

        assertThat(fake.lastListPendingTasks.getAssignee()).isEqualTo("alice");
        assertThat(fake.lastListPendingTasks.getPageSize()).isEqualTo(20);
        assertThat(fake.lastListPendingTasks.getPageToken()).isEqualTo("page-1");
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
                .setActingService("svc-seedj")
                .setRevision(7L)
                .setDefinitionName("order-approval")
                .setDefinitionNamespace("icps")
                .setDefinitionVersion("1.2")
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
                        .addVotes(cn.byteforce.coord.contracts.workflow.v1.ApprovalVote.newBuilder()
                                .setRoundId("rd-1")
                                .setDecision("approve")
                                .setActor("alice")
                                .setOnBehalfOf("")
                                .setComment("ok")
                                .setAtMs(1755000000000L)
                                .setActingService("svc-bff")
                                .build())
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
                .addAudit(cn.byteforce.coord.contracts.workflow.v1.ApprovalAudit.newBuilder()
                        .setAtMs(1755000000000L)
                        .setAction("add_sign")
                        .setBranchId("br-1")
                        .setBy("alice")
                        .setDetail("seats=1")
                        .setActingService("svc-bff")
                        .build())
                .build();

        ApprovalInstanceDetail d = wf.getInstanceDetail("inst-1");

        assertThat(fake.lastDetail.getWorkflowId()).isEqualTo("inst-1");
        assertThat(d.instanceId()).isEqualTo("inst-1");
        assertThat(d.status()).isEqualTo("RUNNING");
        assertThat(d.initiator()).isEqualTo("bob");
        assertThat(d.actingService()).isEqualTo("svc-seedj");
        assertThat(d.revision()).isEqualTo(7L);
        assertThat(d.definitionName()).isEqualTo("order-approval");
        assertThat(d.definitionNamespace()).isEqualTo("icps");
        assertThat(d.definitionVersion()).isEqualTo("1.2");
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
        assertThat(d.tasks().get(0).votes()).hasSize(1);
        assertThat(d.tasks().get(0).votes().get(0).actor()).isEqualTo("alice");
        assertThat(d.tasks().get(0).votes().get(0).actingService()).isEqualTo("svc-bff");
        assertThat(d.audit()).hasSize(1);
        assertThat(d.audit().get(0).by()).isEqualTo("alice");
        assertThat(d.audit().get(0).actingService()).isEqualTo("svc-bff");
        assertThat(d.rounds()).hasSize(1);
        assertThat(d.rounds().get(0).id()).isEqualTo("rd-1");
        assertThat(d.rounds().get(0).branchId()).isEqualTo("br-1");
        assertThat(d.rounds().get(0).number()).isEqualTo(2);
        assertThat(d.rounds().get(0).status()).isEqualTo("IN_PROGRESS");
        assertThat(d.rounds().get(0).totalCount()).isEqualTo(3);
        assertThat(d.rounds().get(0).completedCount()).isEqualTo(1);
        assertThat(d.rounds().get(0).rejectedCount()).isEqualTo(0);
    }

    @Test
    @DisplayName("urge 发送 task_id/message/idempotency_key 并返回实例状态")
    void urgeSendsMessageAndKey() {
        fake.nextUrgeStatus = "RUNNING";

        String status = wf.urge("inst-1", "task-9", "请尽快处理", "idem-urg");

        assertThat(status).isEqualTo("RUNNING");
        assertThat(fake.lastUrge.getWorkflowId()).isEqualTo("inst-1");
        assertThat(fake.lastUrge.getTaskId()).isEqualTo("task-9");
        assertThat(fake.lastUrge.getMessage()).isEqualTo("请尽快处理");
        assertThat(fake.lastUrge.getIdempotencyKey()).isEqualTo("idem-urg");
        assertThat(fake.lastUrge.getOnBehalfOf()).isEmpty();
    }

    @Test
    @DisplayName("urge/cc/withdrawTask 的 onBehalfOf 变体发送 on_behalf_of")
    void taskCommandsOnBehalfOfSendRepresentedUser() {
        wf.urgeOnBehalfOf("inst-1", "task-9", null, "idem-1", "alice");
        assertThat(fake.lastUrge.getOnBehalfOf()).isEqualTo("alice");

        wf.ccOnBehalfOf("inst-1", "task-9", List.of("bob"), "知会", "idem-2", "alice");
        assertThat(fake.lastCc.getOnBehalfOf()).isEqualTo("alice");

        wf.withdrawTaskOnBehalfOf("inst-1", "task-9", "submit", "撤回", "idem-3", "alice");
        assertThat(fake.lastWithdrawTask.getOnBehalfOf()).isEqualTo("alice");
    }

    @Test
    @DisplayName("cc 任务级发送 task_id/recipients；实例级不发送 task_id")
    void ccSendsRecipientsAndOptionalTask() {
        wf.cc("inst-1", "task-9", List.of("bob", "carol"), "知会", "idem-cc");
        assertThat(fake.lastCc.getWorkflowId()).isEqualTo("inst-1");
        assertThat(fake.lastCc.getTaskId()).isEqualTo("task-9");
        assertThat(fake.lastCc.getRecipientsList()).containsExactly("bob", "carol");
        assertThat(fake.lastCc.getMessage()).isEqualTo("知会");
        assertThat(fake.lastCc.getIdempotencyKey()).isEqualTo("idem-cc");

        wf.cc("inst-1", null, List.of("bob"), null, null);
        assertThat(fake.lastCc.getTaskId()).isEmpty();
    }

    @Test
    @DisplayName("withdrawTask 发送 task_id/target_node/comment")
    void withdrawTaskSendsTargetNodeAndComment() {
        fake.nextWithdrawTaskStatus = "RUNNING";

        String status = wf.withdrawTask("inst-1", "task-9", "submit", "撤回", "idem-wdt");

        assertThat(status).isEqualTo("RUNNING");
        assertThat(fake.lastWithdrawTask.getWorkflowId()).isEqualTo("inst-1");
        assertThat(fake.lastWithdrawTask.getTaskId()).isEqualTo("task-9");
        assertThat(fake.lastWithdrawTask.getTargetNode()).isEqualTo("submit");
        assertThat(fake.lastWithdrawTask.getComment()).isEqualTo("撤回");
        assertThat(fake.lastWithdrawTask.getIdempotencyKey()).isEqualTo("idem-wdt");
    }

    @Test
    @DisplayName("listTasks 发送全部过滤条件、钳制 page_size 并回传 next_page_token")
    void listTasksSendsFiltersAndReturnsPageToken() {
        fake.nextTasks = WorkflowListTasksResponse.newBuilder()
                .addTasks(cn.byteforce.coord.contracts.workflow.v1.ApprovalTaskSummary.newBuilder()
                        .setInstanceId("inst-1")
                        .setTaskId("task-9")
                        .setNode("approve")
                        .setBranchId("br-1")
                        .setRoundId("rd-1")
                        .setKind("APPROVAL")
                        .setStatus("claimed")
                        .setAssignee("alice")
                        .addCandidates("alice")
                        .addCandidates("bob")
                        .setDeadlineMs(1755000000000L)
                        .setCreatedAtMs(1754000000000L)
                        .setOrigin("APPROVAL")
                        .setPredecessorTaskId("task-1")
                        .setSuccessorTaskId("task-10")
                        .setSpawnedByActionId("act-7")
                        .addAssignments(cn.byteforce.coord.contracts.workflow.v1.ApprovalAssignment
                                .newBuilder()
                                .setPrincipal("alice")
                                .setStatus("pending")
                                .build())
                        .build())
                .setNextPageToken("next-2")
                .build();
        TaskQueryFilter filter = new TaskQueryFilter(
                "inst-1", "alice", "bob", "grp-a", "approve", "claimed", 1756000000000L);

        TaskPage page = wf.listTasks(filter, 500, "page-1");

        assertThat(fake.lastListTasks.getPageSize()).isEqualTo(200);
        assertThat(fake.lastListTasks.getInstanceId()).isEqualTo("inst-1");
        assertThat(fake.lastListTasks.getAssignee()).isEqualTo("alice");
        assertThat(fake.lastListTasks.getCompletedBy()).isEqualTo("bob");
        assertThat(fake.lastListTasks.getCandidateGroup()).isEqualTo("grp-a");
        assertThat(fake.lastListTasks.getNode()).isEqualTo("approve");
        assertThat(fake.lastListTasks.getStatus()).isEqualTo("claimed");
        assertThat(fake.lastListTasks.getDueBeforeMs()).isEqualTo(1756000000000L);
        assertThat(fake.lastListTasks.getPageToken()).isEqualTo("page-1");
        assertThat(page.nextPageToken()).isEqualTo("next-2");
        assertThat(page.tasks()).hasSize(1);
        ApprovalTask t = page.tasks().get(0);
        assertThat(t.instanceId()).isEqualTo("inst-1");
        assertThat(t.id()).isEqualTo("task-9");
        assertThat(t.node()).isEqualTo("approve");
        assertThat(t.status()).isEqualTo("claimed");
        assertThat(t.assignee()).isEqualTo("alice");
        assertThat(t.candidates()).containsExactly("alice", "bob");
        assertThat(t.createdAtMs()).isEqualTo(1754000000000L);
        assertThat(t.origin()).isEqualTo("APPROVAL");
        assertThat(t.predecessorTaskId()).isEqualTo("task-1");
        assertThat(t.successorTaskId()).isEqualTo("task-10");
        assertThat(t.spawnedByActionId()).isEqualTo("act-7");
        assertThat(t.assignments()).hasSize(1);
        assertThat(t.assignments().get(0).principal()).isEqualTo("alice");
        assertThat(t.assignments().get(0).status()).isEqualTo("pending");
        assertThat(t.assignments().get(0).vote()).isNull();
        // 列表投影不带 votes（proto 的 summary 不携带）；需要投票时用 getTask。
        assertThat(t.votes()).isEmpty();
    }

    @Test
    @DisplayName("listTasks 空过滤不上 wire；page_size 下限为 1")
    void listTasksWithoutFilterSendsNoFilters() {
        wf.listTasks(null, 0, null);

        assertThat(fake.lastListTasks.getPageSize()).isEqualTo(1);
        assertThat(fake.lastListTasks.getInstanceId()).isEmpty();
        assertThat(fake.lastListTasks.getAssignee()).isEmpty();
        assertThat(fake.lastListTasks.getCompletedBy()).isEmpty();
        assertThat(fake.lastListTasks.getCandidateGroup()).isEmpty();
        assertThat(fake.lastListTasks.getNode()).isEmpty();
        assertThat(fake.lastListTasks.getStatus()).isEmpty();
        assertThat(fake.lastListTasks.getDueBeforeMs()).isZero();
        assertThat(fake.lastListTasks.getPageToken()).isEmpty();
    }

    @Test
    @DisplayName("getTask 映射席位（含 assignments）+ votes + 实例状态")
    void getTaskMapsSeatVotesAndInstanceStatus() {
        fake.nextTask = WorkflowGetTaskResponse.newBuilder()
                .setTask(cn.byteforce.coord.contracts.workflow.v1.ApprovalTaskSummary.newBuilder()
                        .setInstanceId("inst-1")
                        .setTaskId("task-9")
                        .setNode("approve")
                        .setBranchId("br-1")
                        .setRoundId("rd-1")
                        .setKind("APPROVAL")
                        .setStatus("completed")
                        .setAssignee("alice")
                        .addCandidates("alice")
                        .setDeadlineMs(1755000000000L)
                        .setCreatedAtMs(1754000000000L)
                        .addAssignments(cn.byteforce.coord.contracts.workflow.v1.ApprovalAssignment
                                .newBuilder()
                                .setPrincipal("alice")
                                .setStatus("completed")
                                .setResolvedReason("voted")
                                .setRecord(cn.byteforce.coord.contracts.workflow.v1.ApprovalVote
                                        .newBuilder()
                                        .setRoundId("rd-1")
                                        .setDecision("approve")
                                        .setActor("alice")
                                        .setComment("ok")
                                        .setAtMs(1755000000000L)
                                        .setActingService("svc-bff")
                                        .build())
                                .build())
                        .build())
                .addVotes(cn.byteforce.coord.contracts.workflow.v1.ApprovalVote.newBuilder()
                        .setRoundId("rd-1")
                        .setDecision("approve")
                        .setActor("alice")
                        .setComment("ok")
                        .setAtMs(1755000000000L)
                        .setActingService("svc-bff")
                        .build())
                .setInstanceStatus("COMPLETED")
                .build();

        TaskDetail detail = wf.getTask("task-9");

        assertThat(fake.lastGetTask.getTaskId()).isEqualTo("task-9");
        assertThat(detail.instanceStatus()).isEqualTo("COMPLETED");
        ApprovalTask t = detail.task();
        assertThat(t.id()).isEqualTo("task-9");
        assertThat(t.instanceId()).isEqualTo("inst-1");
        assertThat(t.status()).isEqualTo("completed");
        assertThat(t.votes()).hasSize(1);
        assertThat(t.votes().get(0).actingService()).isEqualTo("svc-bff");
        assertThat(t.assignments()).hasSize(1);
        assertThat(t.assignments().get(0).resolvedReason()).isEqualTo("voted");
        assertThat(t.assignments().get(0).vote()).isNotNull();
        assertThat(t.assignments().get(0).vote().decision()).isEqualTo("approve");
    }

    @Test
    @DisplayName("history 原样透传 attributes_json（不解析/不重排）")
    void historyPassesAttributesJsonVerbatim() {
        String raw = "{\"b\": 1,   \"a\": [2, 3]}";
        fake.nextHistory = WorkflowHistoryResponse.newBuilder()
                .addEvents(cn.byteforce.coord.contracts.workflow.v1.WorkflowHistoryEvent.newBuilder()
                        .setInstanceId("inst-1")
                        .setEventId("ev-1")
                        .setEventType("TASK_URGED")
                        .setStepName("approve")
                        .setTaskId("task-9")
                        .setOccurredAt(1755000000000L)
                        .setAttributesJson(raw)
                        .build())
                .build();

        List<ApprovalHistoryEvent> events = wf.history("inst-1");

        assertThat(fake.lastHistory.getWorkflowId()).isEqualTo("inst-1");
        assertThat(events).hasSize(1);
        ApprovalHistoryEvent e = events.get(0);
        assertThat(e.instanceId()).isEqualTo("inst-1");
        assertThat(e.eventType()).isEqualTo("TASK_URGED");
        assertThat(e.node()).isEqualTo("approve");
        assertThat(e.taskId()).isEqualTo("task-9");
        assertThat(e.occurredAtMs()).isEqualTo(1755000000000L);
        assertThat(e.attributesJson()).isEqualTo(raw);
    }

    @Test
    @DisplayName("未知 status 由服务端以 INVALID_ARGUMENT 拒绝（既有 ErrorMapper）")
    void listTasksUnknownStatusSurfacesInvalidArgument() {
        CoordException ex = catchThrowableOfType(
                () -> wf.listTasks(new TaskQueryFilter(null, null, null, null, null, "bogus", null),
                        20, null),
                CoordException.class);

        assertThat(ex).isNotNull();
        assertThat(ex.getErrorCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT);
    }

    @Test
    @DisplayName("resumeWorkflow 发送 new_due_in_ms/reason/idempotency_key（含 onBehalfOf 变体）")
    void resumeWorkflowSendsDueReasonAndKey() {
        fake.nextResumeStatus = "RUNNING";

        String status = wf.resumeWorkflow("inst-1", 3_600_000L, "延期一天", "idem-rs");

        assertThat(status).isEqualTo("RUNNING");
        assertThat(fake.lastResumeWorkflow.getWorkflowId()).isEqualTo("inst-1");
        assertThat(fake.lastResumeWorkflow.getNewDueInMs()).isEqualTo(3_600_000L);
        assertThat(fake.lastResumeWorkflow.getReason()).isEqualTo("延期一天");
        assertThat(fake.lastResumeWorkflow.getIdempotencyKey()).isEqualTo("idem-rs");
        assertThat(fake.lastResumeWorkflow.getOnBehalfOf()).isEmpty();

        wf.resumeWorkflowOnBehalfOf("inst-1", 1_000L, null, "idem-rs2", "alice");
        assertThat(fake.lastResumeWorkflow.getNewDueInMs()).isEqualTo(1_000L);
        assertThat(fake.lastResumeWorkflow.getReason()).isEmpty();
        assertThat(fake.lastResumeWorkflow.getOnBehalfOf()).isEqualTo("alice");
    }

    @Test
    @DisplayName("delegate 重载发送 delegate_for_ms；旧 5 参不发送（保持不自动到期）")
    void delegateSendsOptionalExpiry() {
        fake.nextDelegateStatus = "RUNNING";

        String status = wf.delegate("inst-1", "task-9", "bob", "代理", 7_200_000L, "idem-dg");

        assertThat(status).isEqualTo("RUNNING");
        assertThat(fake.lastDelegate.getWorkflowId()).isEqualTo("inst-1");
        assertThat(fake.lastDelegate.getTaskId()).isEqualTo("task-9");
        assertThat(fake.lastDelegate.getTo()).isEqualTo("bob");
        assertThat(fake.lastDelegate.getDelegateForMs()).isEqualTo(7_200_000L);
        assertThat(fake.lastDelegate.getIdempotencyKey()).isEqualTo("idem-dg");
        assertThat(fake.lastDelegate.getOnBehalfOf()).isEmpty();

        wf.delegate("inst-1", "task-9", "bob", null, "idem-dg2");
        assertThat(fake.lastDelegate.getDelegateForMs()).isZero();

        wf.delegateOnBehalfOf("inst-1", "task-9", "bob", null, 1_000L, "idem-dg3", "alice");
        assertThat(fake.lastDelegate.getDelegateForMs()).isEqualTo(1_000L);
        assertThat(fake.lastDelegate.getOnBehalfOf()).isEqualTo("alice");
    }

    @Test
    @DisplayName("addSign 重载发送 position/task_id/action_id（CURRENT 不发送 task_id）")
    void addSignSendsPositionAnchorAndActionId() {
        wf.addSign("inst-1", "br-1", List.of("bob"), null, null, "加签", "idem-as",
                AddAssigneePosition.BEFORE, "task-9", "act-7");

        assertThat(fake.lastAddSign.getWorkflowId()).isEqualTo("inst-1");
        assertThat(fake.lastAddSign.getBranchId()).isEqualTo("br-1");
        assertThat(fake.lastAddSign.getApproversList()).containsExactly("bob");
        assertThat(fake.lastAddSign.getPosition())
                .isEqualTo(cn.byteforce.coord.contracts.workflow.v1.AddAssigneePosition
                        .ADD_ASSIGNEE_POSITION_BEFORE);
        assertThat(fake.lastAddSign.getTaskId()).isEqualTo("task-9");
        assertThat(fake.lastAddSign.getActionId()).isEqualTo("act-7");
        assertThat(fake.lastAddSign.getOnBehalfOf()).isEmpty();

        wf.addSignOnBehalfOf("inst-1", "br-1", List.of("bob"), null, null, null, "idem-as2",
                "alice", AddAssigneePosition.CURRENT, null, null);
        assertThat(fake.lastAddSign.getPosition()).isEqualTo(
                cn.byteforce.coord.contracts.workflow.v1.AddAssigneePosition
                        .ADD_ASSIGNEE_POSITION_CURRENT);
        assertThat(fake.lastAddSign.getTaskId()).isEmpty();
        assertThat(fake.lastAddSign.getActionId()).isEmpty();
        assertThat(fake.lastAddSign.getOnBehalfOf()).isEqualTo("alice");
    }

    @Test
    @DisplayName("removeSign 重载发送 assignees/task_id（按人减签）")
    void removeSignSendsAssigneesAndAnchor() {
        wf.removeSign("inst-1", "br-1", null, "减签", "idem-rsg",
                List.of("bob", "carol"), "task-9");

        assertThat(fake.lastRemoveSign.getWorkflowId()).isEqualTo("inst-1");
        assertThat(fake.lastRemoveSign.getBranchId()).isEqualTo("br-1");
        assertThat(fake.lastRemoveSign.getTaskIdsList()).isEmpty();
        assertThat(fake.lastRemoveSign.getAssigneesList()).containsExactly("bob", "carol");
        assertThat(fake.lastRemoveSign.getTaskId()).isEqualTo("task-9");
        assertThat(fake.lastRemoveSign.getIdempotencyKey()).isEqualTo("idem-rsg");

        wf.removeSignOnBehalfOf("inst-1", "br-1", List.of("task-1"), null, null, "alice",
                null, null);
        assertThat(fake.lastRemoveSign.getTaskIdsList()).containsExactly("task-1");
        assertThat(fake.lastRemoveSign.getAssigneesList()).isEmpty();
        assertThat(fake.lastRemoveSign.getTaskId()).isEmpty();
        assertThat(fake.lastRemoveSign.getOnBehalfOf()).isEqualTo("alice");
    }

    @Test
    @DisplayName("listInstances 重载发送时间窗/定义过滤并回传 next_page_token")
    void listInstancesWithWindowAndToken() {
        fake.nextInstances = WorkflowListInstancesResponse.newBuilder()
                .addInstances(cn.byteforce.coord.contracts.workflow.v1.WorkflowInstanceSummary
                        .newBuilder()
                        .setInstanceId("inst-1")
                        .setWorkflowId("inst-1")
                        .setState("RUNNING")
                        .setStartedAt(1_755_000_000_000L)
                        .setUpdatedAt(1_755_000_100_000L)
                        .setDefinitionName("order-approval")
                        .setNamespace("icps")
                        .setDefinitionNamespace("icps")
                        .setDefinitionVersion("1.2")
                        .setInitiator("alice")
                        .setCompletedAt(1_755_000_100_000L)
                        .build())
                .setNextPageToken("next-2")
                .build();

        InstancePage page = wf.listInstances("icps", "order-approval", "RUNNING",
                1_754_000_000_000L, 1_756_000_000_000L, 500, "page-1");

        assertThat(fake.lastListInstances.getNamespace()).isEqualTo("icps");
        assertThat(fake.lastListInstances.getDefinition()).isEqualTo("order-approval");
        assertThat(fake.lastListInstances.getStatusFilter()).isEqualTo("RUNNING");
        assertThat(fake.lastListInstances.getStartedAfterMs()).isEqualTo(1_754_000_000_000L);
        assertThat(fake.lastListInstances.getStartedBeforeMs()).isEqualTo(1_756_000_000_000L);
        assertThat(fake.lastListInstances.getPageSize()).isEqualTo(200);
        assertThat(fake.lastListInstances.getPageToken()).isEqualTo("page-1");
        assertThat(page.nextPageToken()).isEqualTo("next-2");
        assertThat(page.instances()).hasSize(1);
        WorkflowInstanceSummary s = page.instances().get(0);
        assertThat(s.instanceId()).isEqualTo("inst-1");
        assertThat(s.state()).isEqualTo("RUNNING");
        assertThat(s.definitionName()).isEqualTo("order-approval");
        assertThat(s.namespace()).isEqualTo("icps");
        assertThat(s.startedAt()).isEqualTo(1_755_000_000_000L);
        assertThat(s.definitionNamespace()).isEqualTo("icps");
        assertThat(s.definitionVersion()).isEqualTo("1.2");
        assertThat(s.initiator()).isEqualTo("alice");
        assertThat(s.completedAt()).isEqualTo(1_755_000_100_000L);
    }

    @Test
    @DisplayName("getDefinition 重载发送 (namespace, name, version) 三元组")
    void getDefinitionSendsTriple() {
        fake.nextDefinition = WorkflowGetDefinitionResponse.newBuilder()
                .setWorkflowId("def-7")
                .setName("order-approval")
                .setVersion("1.2")
                .setStatus("ACTIVE")
                .setDefinitionYaml("id: order-approval")
                .setCreatedAt(1_755_000_000_000L)
                .build();

        WorkflowDefinition def = wf.getDefinition("icps", "order-approval", "1.2");

        assertThat(fake.lastGetDefinition.getNamespace()).isEqualTo("icps");
        assertThat(fake.lastGetDefinition.getName()).isEqualTo("order-approval");
        assertThat(fake.lastGetDefinition.getVersion()).isEqualTo("1.2");
        assertThat(fake.lastGetDefinition.getWorkflowId()).isEmpty();
        assertThat(def.workflowId()).isEqualTo("def-7");
        assertThat(def.version()).isEqualTo("1.2");
        assertThat(def.definitionYaml()).isEqualTo("id: order-approval");
    }

    @Test
    @DisplayName("getStatus 映射 definition_namespace/version 与结构化 fault")
    void getStatusMapsDefinitionIdentityAndFault() {
        fake.nextStatus = WorkflowGetStatusResponse.newBuilder()
                .setWorkflowId("inst-1")
                .setStatus("FAULTED")
                .setDefinitionName("order-approval")
                .setDefinitionNamespace("icps")
                .setDefinitionVersion("1.2")
                .setErrorMessage("审批超时")
                .setFault(cn.byteforce.coord.contracts.workflow.v1.WorkflowFault.newBuilder()
                        .setType("urn:io.serverlessworkflow:errors/timeout")
                        .setTitle("审批超时")
                        .setDetail("deadline exceeded")
                        .setStatus(408)
                        .setInstance("/tasks/0")
                        .build())
                .build();

        WorkflowStatus s = wf.getStatus("inst-1");

        assertThat(s.state()).isEqualTo(cn.byteforce.coord.sdk.workflow.WorkflowState.FAULTED);
        assertThat(s.definitionNamespace()).isEqualTo("icps");
        assertThat(s.definitionVersion()).isEqualTo("1.2");
        // errorMessage 是 fault.title 的兼容副本
        assertThat(s.errorMessage()).isEqualTo("审批超时");
        assertThat(s.fault()).isNotNull();
        assertThat(s.fault().type()).isEqualTo("urn:io.serverlessworkflow:errors/timeout");
        assertThat(s.fault().title()).isEqualTo("审批超时");
        assertThat(s.fault().detail()).isEqualTo("deadline exceeded");
        assertThat(s.fault().status()).isEqualTo(408);
        assertThat(s.fault().instance()).isEqualTo("/tasks/0");
    }

    @Test
    @DisplayName("getStatus 无 fault 时 fault 为 null，定义身份缺省为空串")
    void getStatusWithoutFaultHasNoFault() {
        fake.nextStatus = WorkflowGetStatusResponse.newBuilder()
                .setWorkflowId("inst-2")
                .setStatus("RUNNING")
                .build();

        WorkflowStatus s = wf.getStatus("inst-2");

        assertThat(s.fault()).isNull();
        assertThat(s.definitionNamespace()).isEmpty();
        assertThat(s.definitionVersion()).isEmpty();
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

        WorkflowUrgeRequest lastUrge;
        String nextUrgeStatus = "RUNNING";

        WorkflowCcRequest lastCc;
        String nextCcStatus = "RUNNING";

        WorkflowWithdrawTaskRequest lastWithdrawTask;
        String nextWithdrawTaskStatus = "RUNNING";

        WorkflowListTasksRequest lastListTasks;
        WorkflowListTasksResponse nextTasks = WorkflowListTasksResponse.newBuilder().build();

        WorkflowGetTaskRequest lastGetTask;
        WorkflowGetTaskResponse nextTask = WorkflowGetTaskResponse.newBuilder().build();

        WorkflowHistoryRequest lastHistory;
        WorkflowHistoryResponse nextHistory = WorkflowHistoryResponse.newBuilder().build();

        WorkflowResumeWorkflowRequest lastResumeWorkflow;
        String nextResumeStatus = "RUNNING";

        WorkflowDelegateRequest lastDelegate;
        String nextDelegateStatus = "RUNNING";

        WorkflowAddSignRequest lastAddSign;
        String nextAddSignStatus = "RUNNING";

        WorkflowRemoveSignRequest lastRemoveSign;
        String nextRemoveSignStatus = "RUNNING";

        WorkflowListInstancesRequest lastListInstances;
        WorkflowListInstancesResponse nextInstances =
                WorkflowListInstancesResponse.newBuilder().build();

        WorkflowGetDefinitionRequest lastGetDefinition;
        WorkflowGetDefinitionResponse nextDefinition =
                WorkflowGetDefinitionResponse.newBuilder().build();

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

        @Override
        public void urge(WorkflowUrgeRequest request,
                         StreamObserver<WorkflowUrgeResponse> responseObserver) {
            lastUrge = request;
            responseObserver.onNext(WorkflowUrgeResponse.newBuilder()
                    .setInstanceId(request.getWorkflowId())
                    .setStatus(nextUrgeStatus)
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void cc(WorkflowCcRequest request,
                       StreamObserver<WorkflowCcResponse> responseObserver) {
            lastCc = request;
            responseObserver.onNext(WorkflowCcResponse.newBuilder()
                    .setInstanceId(request.getWorkflowId())
                    .setStatus(nextCcStatus)
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void withdrawTask(WorkflowWithdrawTaskRequest request,
                                 StreamObserver<WorkflowWithdrawTaskResponse> responseObserver) {
            lastWithdrawTask = request;
            responseObserver.onNext(WorkflowWithdrawTaskResponse.newBuilder()
                    .setInstanceId(request.getWorkflowId())
                    .setStatus(nextWithdrawTaskStatus)
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void listTasks(WorkflowListTasksRequest request,
                              StreamObserver<WorkflowListTasksResponse> responseObserver) {
            if ("bogus".equals(request.getStatus())) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("unknown seat status: bogus")
                        .asRuntimeException());
                return;
            }
            lastListTasks = request;
            responseObserver.onNext(nextTasks);
            responseObserver.onCompleted();
        }

        @Override
        public void getTask(WorkflowGetTaskRequest request,
                            StreamObserver<WorkflowGetTaskResponse> responseObserver) {
            lastGetTask = request;
            responseObserver.onNext(nextTask);
            responseObserver.onCompleted();
        }

        @Override
        public void history(WorkflowHistoryRequest request,
                            StreamObserver<WorkflowHistoryResponse> responseObserver) {
            lastHistory = request;
            responseObserver.onNext(nextHistory);
            responseObserver.onCompleted();
        }

        @Override
        public void resumeWorkflow(WorkflowResumeWorkflowRequest request,
                                   StreamObserver<WorkflowResumeWorkflowResponse> responseObserver) {
            lastResumeWorkflow = request;
            responseObserver.onNext(WorkflowResumeWorkflowResponse.newBuilder()
                    .setInstanceId(request.getWorkflowId())
                    .setStatus(nextResumeStatus)
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void delegate(WorkflowDelegateRequest request,
                            StreamObserver<WorkflowDelegateResponse> responseObserver) {
            lastDelegate = request;
            responseObserver.onNext(WorkflowDelegateResponse.newBuilder()
                    .setInstanceId(request.getWorkflowId())
                    .setStatus(nextDelegateStatus)
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void addSign(WorkflowAddSignRequest request,
                           StreamObserver<WorkflowAddSignResponse> responseObserver) {
            lastAddSign = request;
            responseObserver.onNext(WorkflowAddSignResponse.newBuilder()
                    .setInstanceId(request.getWorkflowId())
                    .setStatus(nextAddSignStatus)
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void removeSign(WorkflowRemoveSignRequest request,
                              StreamObserver<WorkflowRemoveSignResponse> responseObserver) {
            lastRemoveSign = request;
            responseObserver.onNext(WorkflowRemoveSignResponse.newBuilder()
                    .setInstanceId(request.getWorkflowId())
                    .setStatus(nextRemoveSignStatus)
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void listInstances(WorkflowListInstancesRequest request,
                                  StreamObserver<WorkflowListInstancesResponse> responseObserver) {
            lastListInstances = request;
            responseObserver.onNext(nextInstances);
            responseObserver.onCompleted();
        }

        @Override
        public void getDefinition(WorkflowGetDefinitionRequest request,
                                  StreamObserver<WorkflowGetDefinitionResponse> responseObserver) {
            lastGetDefinition = request;
            responseObserver.onNext(nextDefinition);
            responseObserver.onCompleted();
        }
    }
}
