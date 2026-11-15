package cn.byteforce.coord.sdk.workflow;

import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.sdk.internal.rpc.ErrorMapper;
import cn.byteforce.coord.sdk.internal.rpc.RetryTemplate;
import cn.byteforce.coord.sdk.internal.rpc.WorkflowClientImpl;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import cn.byteforce.coord.contracts.workflow.v1.ApprovalTaskSummary;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowCcRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowCcResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowGetTaskRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowGetTaskResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowGrpc;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowHistoryEvent;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowHistoryRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowHistoryResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowListTasksRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowListTasksResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowUrgeRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowUrgeResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowWithdrawTaskRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowWithdrawTaskResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 契约测试 —— 任务级命令（催办 / 抄送 / 撤回）与任务查询面（{@code ListTasks} /
 * {@code GetTask} / {@code History}）。
 *
 * <p>这些 RPC 在 agent 侧已实现但 SDK 0.4.3 未暴露；本测试固定三件事：
 * <ul>
 *   <li>{@link WorkflowClient} 声明了消费方依赖的方法签名（反射断言，防止签名漂移）；</li>
 *   <li>请求字段映射：{@code note → message}、{@code targets → recipients}、
 *       {@code comment → message}、过滤字段空值不落 wire；</li>
 *   <li>响应映射：席位 / 任务详情 / 历史事件，{@code attributes_json} **原样透传**
 *       （服务端已给 JSON 字符串，SDK 不再重序列化）。</li>
 * </ul>
 */
class WorkflowClientReadSurfaceTest {

    private Server server;
    private ManagedChannel channel;
    private WorkflowClient wf;
    private FakeTaskService fake;

    @BeforeEach
    void setUp() throws Exception {
        fake = new FakeTaskService();
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
                Executors.newVirtualThreadPerTaskExecutor());
    }

    @AfterEach
    void tearDown() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    @Test
    @DisplayName("WorkflowClient 声明任务级命令与查询面的方法签名")
    void declaresTaskCommandAndQuerySurface() throws Exception {
        assertReturnType("urge", String.class, String.class, String.class, String.class, String.class);
        assertReturnType("cc", String.class, String.class, String.class, List.class,
                String.class, String.class);
        assertReturnType("withdrawTask", String.class, String.class, String.class, String.class,
                String.class, String.class);
        assertReturnType("listTasks", List.class, TaskQueryFilter.class, int.class, String.class);
        assertReturnType("getTask", TaskDetail.class, String.class);
        assertReturnType("history", List.class, String.class);
        // 既有委派方法保持 5 参（proto 无 delegate_for_ms，见映射文档 §3）
        assertReturnType("delegate", String.class, String.class, String.class, String.class,
                String.class, String.class);
    }

    @Test
    @DisplayName("urge 发送 task_id/note/idempotency_key 并回传实例状态")
    void urgeSendsNoteAndIdempotencyKey() {
        fake.nextStatus = "RUNNING";

        String status = wf.urge("inst-1", "task-9", "请尽快处理", "idem-urge");

        assertThat(status).isEqualTo("RUNNING");
        assertThat(fake.lastUrge.getWorkflowId()).isEqualTo("inst-1");
        assertThat(fake.lastUrge.getTaskId()).isEqualTo("task-9");
        assertThat(fake.lastUrge.getMessage()).isEqualTo("请尽快处理");
        assertThat(fake.lastUrge.getIdempotencyKey()).isEqualTo("idem-urge");
    }

    @Test
    @DisplayName("cc 发送 recipients/comment；task_id 为空时是实例级抄送")
    void ccSendsRecipientsAndMessage() {
        fake.nextStatus = "RUNNING";

        wf.cc("inst-1", null, List.of("alice", "bob"), "周知", "idem-cc");

        assertThat(fake.lastCc.getWorkflowId()).isEqualTo("inst-1");
        assertThat(fake.lastCc.getTaskId()).isEmpty();
        assertThat(fake.lastCc.getRecipientsList()).containsExactly("alice", "bob");
        assertThat(fake.lastCc.getMessage()).isEqualTo("周知");
        assertThat(fake.lastCc.getIdempotencyKey()).isEqualTo("idem-cc");
    }

    @Test
    @DisplayName("withdrawTask 发送 task_id/target_node/comment/idempotency_key")
    void withdrawTaskSendsTargetNode() {
        fake.nextStatus = "RUNNING";

        wf.withdrawTask("inst-1", "task-9", "apply", "撤回重填", "idem-wdt");

        assertThat(fake.lastWithdrawTask.getWorkflowId()).isEqualTo("inst-1");
        assertThat(fake.lastWithdrawTask.getTaskId()).isEqualTo("task-9");
        assertThat(fake.lastWithdrawTask.getTargetNode()).isEqualTo("apply");
        assertThat(fake.lastWithdrawTask.getComment()).isEqualTo("撤回重填");
        assertThat(fake.lastWithdrawTask.getIdempotencyKey()).isEqualTo("idem-wdt");
    }

    @Test
    @DisplayName("listTasks 发送全部过滤字段并按席位映射结果")
    void listTasksSendsFiltersAndMapsSeats() {
        fake.nextTasks = WorkflowListTasksResponse.newBuilder()
                .addTasks(ApprovalTaskSummary.newBuilder()
                        .setInstanceId("inst-1")
                        .setTaskId("task-9")
                        .setNode("approve")
                        .setBranchId("br-1")
                        .setRoundId("rd-1")
                        .setKind("APPROVAL")
                        .setStatus("created")
                        .setAssignee("alice")
                        .addCandidates("alice")
                        .setDeadlineMs(1755000000000L)
                        .setCreatedAtMs(1754000000000L)
                        .build())
                .setNextPageToken("page-2")
                .build();

        List<PendingApprovalTask> tasks = wf.listTasks(new TaskQueryFilter(
                "inst-1", "alice", "bob", "group-a", "approve", "created", 1755000000000L),
                500, "page-1");

        assertThat(fake.lastListTasks.getPageSize()).isEqualTo(200); // 服务端上限
        assertThat(fake.lastListTasks.getPageToken()).isEqualTo("page-1");
        assertThat(fake.lastListTasks.getInstanceId()).isEqualTo("inst-1");
        assertThat(fake.lastListTasks.getAssignee()).isEqualTo("alice");
        assertThat(fake.lastListTasks.getCompletedBy()).isEqualTo("bob");
        assertThat(fake.lastListTasks.getCandidateGroup()).isEqualTo("group-a");
        assertThat(fake.lastListTasks.getNode()).isEqualTo("approve");
        assertThat(fake.lastListTasks.getStatus()).isEqualTo("created");
        assertThat(fake.lastListTasks.getDueBeforeMs()).isEqualTo(1755000000000L);

        assertThat(tasks).hasSize(1);
        PendingApprovalTask t = tasks.get(0);
        assertThat(t.instanceId()).isEqualTo("inst-1");
        assertThat(t.taskId()).isEqualTo("task-9");
        assertThat(t.node()).isEqualTo("approve");
        assertThat(t.branchId()).isEqualTo("br-1");
        assertThat(t.roundId()).isEqualTo("rd-1");
        assertThat(t.kind()).isEqualTo("APPROVAL");
        assertThat(t.status()).isEqualTo("created");
        assertThat(t.assignee()).isEqualTo("alice");
        assertThat(t.deadlineMs()).isEqualTo(1755000000000L);
        assertThat(t.createdAtMs()).isEqualTo(1754000000000L);
    }

    @Test
    @DisplayName("listTasks 传 null filter：不落任何过滤字段（不改变待办语义）")
    void listTasksWithNullFilterLeavesFiltersUnset() {
        fake.nextTasks = WorkflowListTasksResponse.newBuilder().build();

        wf.listTasks(null, 20, null);

        assertThat(fake.lastListTasks.getPageSize()).isEqualTo(20);
        assertThat(fake.lastListTasks.getPageToken()).isEmpty();
        assertThat(fake.lastListTasks.getInstanceId()).isEmpty();
        assertThat(fake.lastListTasks.getAssignee()).isEmpty();
        assertThat(fake.lastListTasks.getCompletedBy()).isEmpty();
        assertThat(fake.lastListTasks.getCandidateGroup()).isEmpty();
        assertThat(fake.lastListTasks.getNode()).isEmpty();
        assertThat(fake.lastListTasks.getStatus()).isEmpty();
        assertThat(fake.lastListTasks.getDueBeforeMs()).isZero();
    }

    @Test
    @DisplayName("getTask 发送 task_id 并映射席位 + 投票")
    void getTaskMapsSeatAndVotes() {
        fake.nextTask = WorkflowGetTaskResponse.newBuilder()
                .setTask(ApprovalTaskSummary.newBuilder()
                        .setInstanceId("inst-1")
                        .setTaskId("task-9")
                        .setNode("approve")
                        .setBranchId("br-1")
                        .setRoundId("rd-1")
                        .setKind("APPROVAL")
                        .setStatus("completed")
                        .setAssignee("alice")
                        .addCandidates("alice")
                        .addCandidates("carol")
                        .setDeadlineMs(1755000000000L)
                        .setCreatedAtMs(1754000000000L)
                        .build())
                .addVotes(cn.byteforce.coord.contracts.workflow.v1.ApprovalVote.newBuilder()
                        .setRoundId("rd-1")
                        .setDecision("approve")
                        .setActor("alice")
                        .setComment("ok")
                        .setAtMs(1755000000000L)
                        .setActingService("svc-bff")
                        .build())
                .setInstanceStatus("RUNNING")
                .build();

        TaskDetail detail = wf.getTask("task-9");

        assertThat(fake.lastGetTask.getTaskId()).isEqualTo("task-9");
        assertThat(detail.id()).isEqualTo("task-9");
        assertThat(detail.instanceId()).isEqualTo("inst-1");
        assertThat(detail.node()).isEqualTo("approve");
        assertThat(detail.branchId()).isEqualTo("br-1");
        assertThat(detail.roundId()).isEqualTo("rd-1");
        assertThat(detail.kind()).isEqualTo("APPROVAL");
        assertThat(detail.status()).isEqualTo("completed");
        assertThat(detail.assignee()).isEqualTo("alice");
        assertThat(detail.candidates()).containsExactly("alice", "carol");
        assertThat(detail.deadlineMs()).isEqualTo(1755000000000L);
        assertThat(detail.createdAtMs()).isEqualTo(1754000000000L);
        assertThat(detail.votes()).hasSize(1);
        assertThat(detail.votes().get(0).actor()).isEqualTo("alice");
        assertThat(detail.votes().get(0).actingService()).isEqualTo("svc-bff");
    }

    @Test
    @DisplayName("history 原样透传服务端 attributes_json（不重序列化）")
    void historyPassesAttributesJsonThrough() {
        String attributes = "{\"assignee\":\"alice\",\"reason\":\"timeout\"}";
        fake.nextHistory = WorkflowHistoryResponse.newBuilder()
                .addEvents(WorkflowHistoryEvent.newBuilder()
                        .setEventId("evt-1")
                        .setEventType("TASK_CREATED")
                        .setInstanceId("inst-1")
                        .setStepName("approve")
                        .setTaskId("task-9")
                        .setActionId("")
                        .setOccurredAt(1755000000000L)
                        .setAttributesJson(attributes)
                        .build())
                .build();

        List<ApprovalHistoryEvent> events = wf.history("inst-1");

        assertThat(fake.lastHistory.getWorkflowId()).isEqualTo("inst-1");
        assertThat(events).hasSize(1);
        ApprovalHistoryEvent e = events.get(0);
        assertThat(e.eventId()).isEqualTo("evt-1");
        assertThat(e.eventType()).isEqualTo("TASK_CREATED");
        assertThat(e.node()).isEqualTo("approve");
        assertThat(e.taskId()).isEqualTo("task-9");
        assertThat(e.actionId()).isEmpty();
        assertThat(e.occurredAtMs()).isEqualTo(1755000000000L);
        assertThat(e.attributesJson()).isEqualTo(attributes);
    }

    private static void assertReturnType(String name, Class<?> returnType, Class<?>... params)
            throws NoSuchMethodException {
        Method m = WorkflowClient.class.getMethod(name, params);
        assertThat(m.getReturnType())
                .as("%s%s 的返回类型", name, List.of(params))
                .isEqualTo(returnType);
    }

    /** In-process fake Workflow service —— 仅实现被测 RPC，其余走 gRPC 默认 UNIMPLEMENTED。 */
    static class FakeTaskService extends WorkflowGrpc.WorkflowImplBase {

        String nextStatus = "RUNNING";

        WorkflowUrgeRequest lastUrge;
        WorkflowCcRequest lastCc;
        WorkflowWithdrawTaskRequest lastWithdrawTask;
        WorkflowListTasksRequest lastListTasks;
        WorkflowListTasksResponse nextTasks = WorkflowListTasksResponse.newBuilder().build();
        WorkflowGetTaskRequest lastGetTask;
        WorkflowGetTaskResponse nextTask = WorkflowGetTaskResponse.newBuilder().build();
        WorkflowHistoryRequest lastHistory;
        WorkflowHistoryResponse nextHistory = WorkflowHistoryResponse.newBuilder().build();

        @Override
        public void urge(WorkflowUrgeRequest request,
                         StreamObserver<WorkflowUrgeResponse> responseObserver) {
            lastUrge = request;
            responseObserver.onNext(WorkflowUrgeResponse.newBuilder()
                    .setInstanceId(request.getWorkflowId())
                    .setStatus(nextStatus)
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void cc(WorkflowCcRequest request,
                       StreamObserver<WorkflowCcResponse> responseObserver) {
            lastCc = request;
            responseObserver.onNext(WorkflowCcResponse.newBuilder()
                    .setInstanceId(request.getWorkflowId())
                    .setStatus(nextStatus)
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void withdrawTask(WorkflowWithdrawTaskRequest request,
                                 StreamObserver<WorkflowWithdrawTaskResponse> responseObserver) {
            lastWithdrawTask = request;
            responseObserver.onNext(WorkflowWithdrawTaskResponse.newBuilder()
                    .setInstanceId(request.getWorkflowId())
                    .setStatus(nextStatus)
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void listTasks(WorkflowListTasksRequest request,
                              StreamObserver<WorkflowListTasksResponse> responseObserver) {
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
    }
}
