package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.CoordException;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowAcknowledgeRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowAcknowledgeResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowAddSignRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowAddSignResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowApproveRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowApproveResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowCancelRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowCancelResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowDelegateRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowDelegateResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowDeployRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowDeployResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowDeleteDefinitionRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowDeleteInstanceRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowGetDefinitionRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowGetDefinitionResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowGetInstanceDetailRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowGetInstanceDetailResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowGetStatusRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowGetStatusResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowGrpc;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowListDefinitionVersionsRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowListDefinitionVersionsResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowListDefinitionsRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowListDefinitionsResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowListInstancesRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowListInstancesResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowListPendingTasksRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowListPendingTasksResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowRejectRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowRejectResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowRemoveSignRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowRemoveSignResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowReturnToNodeRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowReturnToNodeResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowRollbackDefinitionRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowRollbackDefinitionResponse;
import cn.byteforce.coord.contracts.workflow.v1.SuspensionMeta;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowSignalRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowSignalResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowStartRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowStartResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowTerminateRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowTerminateResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowTransferRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowTransferResponse;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowWithdrawRequest;
import cn.byteforce.coord.contracts.workflow.v1.WorkflowWithdrawResponse;
import cn.byteforce.coord.contracts.workflow.v1.TaskFrame;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import cn.byteforce.coord.sdk.workflow.ApprovalAudit;
import cn.byteforce.coord.sdk.workflow.ApprovalBranch;
import cn.byteforce.coord.sdk.workflow.ApprovalInstanceDetail;
import cn.byteforce.coord.sdk.workflow.ApprovalRound;
import cn.byteforce.coord.sdk.workflow.ApprovalTask;
import cn.byteforce.coord.sdk.workflow.ApprovalVote;
import cn.byteforce.coord.sdk.workflow.PendingApprovalTask;
import cn.byteforce.coord.sdk.workflow.Suspension;
import cn.byteforce.coord.sdk.workflow.WorkflowClient;
import cn.byteforce.coord.sdk.workflow.WorkflowDefinition;
import cn.byteforce.coord.sdk.workflow.WorkflowDefinitionSummary;
import cn.byteforce.coord.sdk.workflow.WorkflowDefinitionVersion;
import cn.byteforce.coord.sdk.workflow.WorkflowInstanceSummary;
import cn.byteforce.coord.sdk.workflow.WorkflowState;
import cn.byteforce.coord.sdk.workflow.WorkflowStatus;

import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of {@link WorkflowClient} backed by gRPC calls to the Coord Agent.
 */
public final class WorkflowClientImpl extends AgentRpcClient implements WorkflowClient {

    private static final Logger log = LoggerFactory.getLogger(WorkflowClientImpl.class);
    private final CoordConfig config;
    /** 轮询工作流状态用的虚拟线程执行器（由 ThreadPoolManager 提供，close() 可中断）。 */
    private final java.util.concurrent.ExecutorService watchExecutor;

    public WorkflowClientImpl(AgentChannelManager channelManager, ErrorMapper errorMapper,
                              RetryTemplate retryTemplate, ObservabilityProvider observability,
                              CoordConfig config,
                              java.util.concurrent.ExecutorService watchExecutor) {
        super(channelManager, errorMapper, retryTemplate, observability);
        this.config = config;
        this.watchExecutor = watchExecutor;
    }

    // ──── 实例生命周期 ────

    @Override
    public String start(String definitionDsl, byte[] input) {
        WorkflowStartRequest.Builder req = WorkflowStartRequest.newBuilder()
                .setDefinitionDsl(definitionDsl);
        if (input != null && input.length > 0) {
            req.setInput(ByteString.copyFrom(input));
        }

        WorkflowStartResponse response = callWithRetry(
                (ch, r) -> WorkflowGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .start((WorkflowStartRequest) r),
                req.build(), "workflow.start");

        log.debug("Workflow started: id={}", response.getWorkflowId());
        return response.getWorkflowId();
    }

    @Override
    public String startByDefinition(String definitionId, byte[] input) {
        // 真契约：按已部署定义 ID 启动（definition_id），携带 input，跳过内联 deploy。
        // 由宿主在发布时保存 definition id（deployDefinition 返回），submit 按 id 启动，消除重复部署。
        WorkflowStartRequest.Builder req = WorkflowStartRequest.newBuilder()
                .setDefinitionId(definitionId);
        if (input != null && input.length > 0) {
            req.setInput(ByteString.copyFrom(input));
        }

        WorkflowStartResponse response = callWithRetry(
                (ch, r) -> WorkflowGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .start((WorkflowStartRequest) r),
                req.build(), "workflow.startByDefinition");

        log.debug("Workflow started by definition: id={}, defId={}",
                response.getWorkflowId(), definitionId);
        return response.getWorkflowId();
    }

    @Override
    public WorkflowStatus getStatus(String workflowId) {
        WorkflowGetStatusRequest request = WorkflowGetStatusRequest.newBuilder()
                .setWorkflowId(workflowId)
                .build();

        WorkflowGetStatusResponse response = callWithRetry(
                (ch, r) -> WorkflowGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .getStatus((WorkflowGetStatusRequest) r),
                request, "workflow.getStatus");

        WorkflowState state = WorkflowState.fromProtoName(response.getStatus());
        byte[] output = response.getOutput().toByteArray();
        String errorMsg = response.getErrorMessage();
        String definitionName = response.getDefinitionName();
        byte[] input = response.getInput().toByteArray();
        long createdAt = response.getCreatedAt();
        long updatedAt = response.getUpdatedAt();

        // Map task_stack from proto to SDK model
        List<cn.byteforce.coord.sdk.workflow.TaskFrame> taskStack = new ArrayList<>();
        for (TaskFrame tf : response.getTaskStackList()) {
            taskStack.add(new cn.byteforce.coord.sdk.workflow.TaskFrame(
                    tf.getTaskName(), tf.getTaskType(), tf.getStatus(),
                    tf.getInput().toByteArray(), tf.getOutput().toByteArray(),
                    tf.getStartedAt(), tf.getEndedAt(), tf.getRetryCount()));
        }

        // currentStep derived from task stack size
        int currentStep = taskStack.size();

        // 当前状态名 + 挂起元信息
        String currentStateName = response.getCurrentStateName();
        Suspension suspension = null;
        if (response.hasSuspension()) {
            SuspensionMeta sm = response.getSuspension();
            suspension = new Suspension(sm.getReason(), sm.getUntilMs(),
                    sm.getExpectedSignal(), sm.getEventType(), sm.getService());
        }

        log.debug("Workflow status: id={}, state={}, defName={}, currentState={}, taskStackSize={}",
                workflowId, state, definitionName, currentStateName, taskStack.size());
        return new WorkflowStatus(response.getWorkflowId(), state,
                currentStep, output, errorMsg, definitionName, input,
                createdAt, updatedAt, taskStack, currentStateName, suspension);
    }

    @Override
    public void signal(String workflowId, String signalName, byte[] payload) {
        signal(workflowId, signalName, payload, null);
    }

    @Override
    public void signal(String workflowId, String signalName, byte[] payload, String idempotencyKey) {
        WorkflowSignalRequest.Builder req = WorkflowSignalRequest.newBuilder()
                .setWorkflowId(workflowId)
                .setSignalName(signalName);
        if (payload != null && payload.length > 0) {
            req.setPayload(ByteString.copyFrom(payload));
        }
        if (idempotencyKey != null && !idempotencyKey.isEmpty()) {
            req.setIdempotencyKey(idempotencyKey);
        }

        callWithRetry(
                (ch, r) -> WorkflowGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .signal((WorkflowSignalRequest) r),
                req.build(), "workflow.signal");

        log.debug("Workflow signal sent: id={}, signal={}, idemKey={}",
                workflowId, signalName,
                idempotencyKey != null ? idempotencyKey : "<none>");
    }

    @Override
    public void cancel(String workflowId) {
        WorkflowCancelRequest request = WorkflowCancelRequest.newBuilder()
                .setWorkflowId(workflowId)
                .build();

        callWithRetry(
                (ch, r) -> WorkflowGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .cancel((WorkflowCancelRequest) r),
                request, "workflow.cancel");

        log.debug("Workflow cancelled: id={}", workflowId);
    }

    // ──── 工作流定义管理 ────

    @Override
    public WorkflowDefinition deployDefinition(String namespace, String definitionYaml) {
        WorkflowDeployRequest request = WorkflowDeployRequest.newBuilder()
                .setNamespace(namespace)
                .setDefinitionYaml(definitionYaml)
                .build();

        WorkflowDeployResponse response = callWithRetry(
                (ch, r) -> WorkflowGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .deploy((WorkflowDeployRequest) r),
                request, "workflow.deploy");

        log.debug("Workflow deployed: id={}, namespace={}, name={}",
                response.getWorkflowId(), response.getNamespace(), response.getName());
        return new WorkflowDefinition(
                response.getWorkflowId(), response.getName(), definitionYaml,
                response.getVersion(), "active", System.currentTimeMillis() / 1000);
    }

    @Override
    public List<WorkflowDefinitionSummary> listDefinitions(String namespace) {
        WorkflowListDefinitionsRequest request = WorkflowListDefinitionsRequest.newBuilder()
                .setNamespace(namespace)
                .setPageSize(50)
                .build();

        WorkflowListDefinitionsResponse response = callWithRetry(
                (ch, r) -> WorkflowGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .listDefinitions((WorkflowListDefinitionsRequest) r),
                request, "workflow.listDefinitions");

        List<WorkflowDefinitionSummary> result = new ArrayList<>();
        for (cn.byteforce.coord.contracts.workflow.v1.WorkflowDefinitionSummary s : response.getDefinitionsList()) {
            result.add(new WorkflowDefinitionSummary(
                    s.getWorkflowId(), s.getName(), s.getVersion(),
                    s.getStatus(), s.getCreatedAt()));
        }
        log.debug("Workflow listDefinitions: namespace={}, count={}", namespace, result.size());
        return result;
    }

    @Override
    public WorkflowDefinition getDefinition(String workflowId) {
        WorkflowGetDefinitionRequest request = WorkflowGetDefinitionRequest.newBuilder()
                .setWorkflowId(workflowId)
                .build();

        WorkflowGetDefinitionResponse response = callWithRetry(
                (ch, r) -> WorkflowGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .getDefinition((WorkflowGetDefinitionRequest) r),
                request, "workflow.getDefinition");

        log.debug("Workflow getDefinition: id={}, name={}, hasYaml={}",
                response.getWorkflowId(), response.getName(),
                !response.getDefinitionYaml().isEmpty());
        return new WorkflowDefinition(
                response.getWorkflowId(), response.getName(), response.getDefinitionYaml(),
                response.getVersion(), response.getStatus(), response.getCreatedAt());
    }

    @Override
    public List<WorkflowDefinitionVersion> listDefinitionVersions(String namespace, String name) {
        WorkflowListDefinitionVersionsRequest request = WorkflowListDefinitionVersionsRequest.newBuilder()
                .setNamespace(namespace)
                .setName(name)
                .build();

        WorkflowListDefinitionVersionsResponse response = callWithRetry(
                (ch, r) -> WorkflowGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .listDefinitionVersions((WorkflowListDefinitionVersionsRequest) r),
                request, "workflow.listDefinitionVersions");

        List<WorkflowDefinitionVersion> result = new ArrayList<>();
        for (cn.byteforce.coord.contracts.workflow.v1.WorkflowDefinitionVersion v : response.getVersionsList()) {
            result.add(new WorkflowDefinitionVersion(
                    v.getVersion(), v.getWorkflowId(), v.getStatus(), v.getCreatedAt()));
        }
        log.debug("Workflow listDefinitionVersions: ns={}, name={}, count={}",
                namespace, name, result.size());
        return result;
    }

    @Override
    public WorkflowDefinition rollbackDefinition(String namespace, String name, String version) {
        WorkflowRollbackDefinitionRequest request = WorkflowRollbackDefinitionRequest.newBuilder()
                .setNamespace(namespace)
                .setName(name)
                .setVersion(version)
                .build();

        WorkflowRollbackDefinitionResponse response = callWithRetry(
                (ch, r) -> WorkflowGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .rollbackDefinition((WorkflowRollbackDefinitionRequest) r),
                request, "workflow.rollbackDefinition");

        log.debug("Workflow rollbackDefinition: ns={}, name={}, fromVersion={}, newVersion={}",
                namespace, name, version, response.getVersion());
        // 响应不含 DSL 全文；完整文档可通过 getDefinition(workflowId) 获取
        return new WorkflowDefinition(
                response.getWorkflowId(), response.getName(), "",
                response.getVersion(), "active", System.currentTimeMillis() / 1000);
    }

    // ──── 保留策略（G-WF-1）────

    @Override
    public void deleteInstance(String workflowId) {
        WorkflowDeleteInstanceRequest request = WorkflowDeleteInstanceRequest.newBuilder()
                .setWorkflowId(workflowId == null ? "" : workflowId)
                .build();
        callWithRetry(
                (ch, r) -> WorkflowGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .deleteInstance((WorkflowDeleteInstanceRequest) r),
                request, "workflow.deleteInstance");
        log.debug("Workflow deleteInstance: id={}", workflowId);
    }

    @Override
    public void deleteDefinition(String namespace, String name, String version) {
        WorkflowDeleteDefinitionRequest request = WorkflowDeleteDefinitionRequest.newBuilder()
                .setNamespace(namespace == null ? "" : namespace)
                .setName(name == null ? "" : name)
                .setVersion(version == null ? "" : version)
                .build();
        callWithRetry(
                (ch, r) -> WorkflowGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .deleteDefinition((WorkflowDeleteDefinitionRequest) r),
                request, "workflow.deleteDefinition");
        log.debug("Workflow deleteDefinition: ns={}, name={}, version={}",
                namespace, name, version);
    }

    // ──── 工作流实例查询 ────

    @Override
    public List<WorkflowInstanceSummary> listInstances(String workflowId) {
        return listInstances(null, workflowId, null, 50, "");
    }

    @Override
    public List<WorkflowInstanceSummary> listInstances(
            String namespace, String workflowId, String statusFilter, int pageSize, String pageToken) {
        WorkflowListInstancesRequest.Builder req = WorkflowListInstancesRequest.newBuilder()
                .setPageSize(Math.max(1, Math.min(pageSize, 200)));
        if (namespace != null && !namespace.isEmpty()) {
            req.setNamespace(namespace);
        }
        if (workflowId != null && !workflowId.isEmpty()) {
            req.setWorkflowId(workflowId);
        }
        if (statusFilter != null && !statusFilter.isEmpty()) {
            req.setStatusFilter(statusFilter);
        }
        if (pageToken != null && !pageToken.isEmpty()) {
            req.setPageToken(pageToken);
        }

        WorkflowListInstancesResponse response = callWithRetry(
                (ch, r) -> WorkflowGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .listInstances((WorkflowListInstancesRequest) r),
                req.build(), "workflow.listInstances");

        List<WorkflowInstanceSummary> result = new ArrayList<>();
        for (cn.byteforce.coord.contracts.workflow.v1.WorkflowInstanceSummary s : response.getInstancesList()) {
            byte[] outputJson = s.getOutputJson().toByteArray();
            byte[] contextJson = s.getContextJson().toByteArray();
            result.add(new WorkflowInstanceSummary(
                    s.getInstanceId(), s.getWorkflowId(), s.getState(),
                    s.getStartedAt(), s.getUpdatedAt(), s.getDefinitionName(),
                    s.getNamespace(),
                    outputJson.length > 0 ? outputJson : null,
                    contextJson.length > 0 ? contextJson : null));
        }
        log.debug("Workflow listInstances: namespace={}, workflowId={}, statusFilter={}, count={}",
                namespace, workflowId, statusFilter, result.size());
        return result;
    }

    // ──── 人工审批 v2（ADR-0013 D7）────

    @Override
    public String approve(String workflowId, String taskId, String comment, String idempotencyKey) {
        return approveOnBehalfOf(workflowId, taskId, comment, idempotencyKey, null);
    }

    @Override
    public String approveOnBehalfOf(String workflowId, String taskId, String comment,
                                    String idempotencyKey, String onBehalfOf) {
        WorkflowApproveRequest.Builder req = WorkflowApproveRequest.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskId(taskId);
        setOptional(req::setComment, comment);
        setOptional(req::setIdempotencyKey, idempotencyKey);
        setOptional(req::setOnBehalfOf, onBehalfOf);

        WorkflowApproveResponse response = callWithRetry(
                (ch, r) -> WorkflowGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .approve((WorkflowApproveRequest) r),
                req.build(), "workflow.approve");

        log.debug("Workflow approve: id={}, taskId={}, status={}",
                workflowId, taskId, response.getStatus());
        return response.getStatus();
    }

    @Override
    public String reject(String workflowId, String taskId, String comment, String idempotencyKey) {
        return rejectOnBehalfOf(workflowId, taskId, comment, idempotencyKey, null);
    }

    @Override
    public String rejectOnBehalfOf(String workflowId, String taskId, String comment,
                                   String idempotencyKey, String onBehalfOf) {
        WorkflowRejectRequest.Builder req = WorkflowRejectRequest.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskId(taskId);
        setOptional(req::setComment, comment);
        setOptional(req::setIdempotencyKey, idempotencyKey);
        setOptional(req::setOnBehalfOf, onBehalfOf);

        WorkflowRejectResponse response = callWithRetry(
                (ch, r) -> WorkflowGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .reject((WorkflowRejectRequest) r),
                req.build(), "workflow.reject");

        log.debug("Workflow reject: id={}, taskId={}, status={}",
                workflowId, taskId, response.getStatus());
        return response.getStatus();
    }

    @Override
    public String acknowledge(String workflowId, String taskId, String idempotencyKey) {
        return acknowledgeOnBehalfOf(workflowId, taskId, idempotencyKey, null);
    }

    @Override
    public String acknowledgeOnBehalfOf(String workflowId, String taskId,
                                        String idempotencyKey, String onBehalfOf) {
        WorkflowAcknowledgeRequest.Builder req = WorkflowAcknowledgeRequest.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskId(taskId);
        setOptional(req::setIdempotencyKey, idempotencyKey);
        setOptional(req::setOnBehalfOf, onBehalfOf);

        WorkflowAcknowledgeResponse response = callWithRetry(
                (ch, r) -> WorkflowGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .acknowledge((WorkflowAcknowledgeRequest) r),
                req.build(), "workflow.acknowledge");

        log.debug("Workflow acknowledge: id={}, taskId={}, status={}",
                workflowId, taskId, response.getStatus());
        return response.getStatus();
    }

    @Override
    public String addSign(String workflowId, String branchId, List<String> approvers,
                          List<String> pool, List<String> readers, String comment,
                          String idempotencyKey) {
        return addSignOnBehalfOf(workflowId, branchId, approvers, pool, readers, comment,
                idempotencyKey, null);
    }

    @Override
    public String addSignOnBehalfOf(String workflowId, String branchId, List<String> approvers,
                                    List<String> pool, List<String> readers, String comment,
                                    String idempotencyKey, String onBehalfOf) {
        WorkflowAddSignRequest.Builder req = WorkflowAddSignRequest.newBuilder()
                .setWorkflowId(workflowId)
                .setBranchId(branchId);
        if (approvers != null) {
            req.addAllApprovers(approvers);
        }
        if (pool != null) {
            req.addAllPool(pool);
        }
        if (readers != null) {
            req.addAllReaders(readers);
        }
        setOptional(req::setComment, comment);
        setOptional(req::setIdempotencyKey, idempotencyKey);
        setOptional(req::setOnBehalfOf, onBehalfOf);

        WorkflowAddSignResponse response = callWithRetry(
                (ch, r) -> WorkflowGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .addSign((WorkflowAddSignRequest) r),
                req.build(), "workflow.addSign");

        log.debug("Workflow addSign: id={}, branchId={}, status={}",
                workflowId, branchId, response.getStatus());
        return response.getStatus();
    }

    @Override
    public String removeSign(String workflowId, String branchId, List<String> taskIds,
                             String comment, String idempotencyKey) {
        return removeSignOnBehalfOf(workflowId, branchId, taskIds, comment, idempotencyKey, null);
    }

    @Override
    public String removeSignOnBehalfOf(String workflowId, String branchId, List<String> taskIds,
                                       String comment, String idempotencyKey, String onBehalfOf) {
        WorkflowRemoveSignRequest.Builder req = WorkflowRemoveSignRequest.newBuilder()
                .setWorkflowId(workflowId)
                .setBranchId(branchId);
        if (taskIds != null) {
            req.addAllTaskIds(taskIds);
        }
        setOptional(req::setComment, comment);
        setOptional(req::setIdempotencyKey, idempotencyKey);
        setOptional(req::setOnBehalfOf, onBehalfOf);

        WorkflowRemoveSignResponse response = callWithRetry(
                (ch, r) -> WorkflowGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .removeSign((WorkflowRemoveSignRequest) r),
                req.build(), "workflow.removeSign");

        log.debug("Workflow removeSign: id={}, branchId={}, status={}",
                workflowId, branchId, response.getStatus());
        return response.getStatus();
    }

    @Override
    public String returnToNode(String workflowId, String branchId, String targetNode,
                               String comment, String idempotencyKey) {
        return returnToNodeOnBehalfOf(workflowId, branchId, targetNode, comment, idempotencyKey,
                null);
    }

    @Override
    public String returnToNodeOnBehalfOf(String workflowId, String branchId, String targetNode,
                                         String comment, String idempotencyKey, String onBehalfOf) {
        WorkflowReturnToNodeRequest.Builder req = WorkflowReturnToNodeRequest.newBuilder()
                .setWorkflowId(workflowId)
                .setBranchId(branchId)
                .setTargetNode(targetNode);
        setOptional(req::setComment, comment);
        setOptional(req::setIdempotencyKey, idempotencyKey);
        setOptional(req::setOnBehalfOf, onBehalfOf);

        WorkflowReturnToNodeResponse response = callWithRetry(
                (ch, r) -> WorkflowGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .returnToNode((WorkflowReturnToNodeRequest) r),
                req.build(), "workflow.returnToNode");

        log.debug("Workflow returnToNode: id={}, branchId={}, targetNode={}, status={}",
                workflowId, branchId, targetNode, response.getStatus());
        return response.getStatus();
    }

    @Override
    public String transfer(String workflowId, String taskId, String to, String comment,
                           String idempotencyKey) {
        return transferOnBehalfOf(workflowId, taskId, to, comment, idempotencyKey, null);
    }

    @Override
    public String transferOnBehalfOf(String workflowId, String taskId, String to, String comment,
                                     String idempotencyKey, String onBehalfOf) {
        WorkflowTransferRequest.Builder req = WorkflowTransferRequest.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskId(taskId)
                .setTo(to);
        setOptional(req::setComment, comment);
        setOptional(req::setIdempotencyKey, idempotencyKey);
        setOptional(req::setOnBehalfOf, onBehalfOf);

        WorkflowTransferResponse response = callWithRetry(
                (ch, r) -> WorkflowGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .transfer((WorkflowTransferRequest) r),
                req.build(), "workflow.transfer");

        log.debug("Workflow transfer: id={}, taskId={}, to={}, status={}",
                workflowId, taskId, to, response.getStatus());
        return response.getStatus();
    }

    @Override
    public String delegate(String workflowId, String taskId, String to, String comment,
                           String idempotencyKey) {
        return delegateOnBehalfOf(workflowId, taskId, to, comment, idempotencyKey, null);
    }

    @Override
    public String delegateOnBehalfOf(String workflowId, String taskId, String to, String comment,
                                     String idempotencyKey, String onBehalfOf) {
        WorkflowDelegateRequest.Builder req = WorkflowDelegateRequest.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskId(taskId)
                .setTo(to);
        setOptional(req::setComment, comment);
        setOptional(req::setIdempotencyKey, idempotencyKey);
        setOptional(req::setOnBehalfOf, onBehalfOf);

        WorkflowDelegateResponse response = callWithRetry(
                (ch, r) -> WorkflowGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .delegate((WorkflowDelegateRequest) r),
                req.build(), "workflow.delegate");

        log.debug("Workflow delegate: id={}, taskId={}, to={}, status={}",
                workflowId, taskId, to, response.getStatus());
        return response.getStatus();
    }

    @Override
    public String withdraw(String workflowId, String comment, String idempotencyKey) {
        return withdrawOnBehalfOf(workflowId, comment, idempotencyKey, null);
    }

    @Override
    public String withdrawOnBehalfOf(String workflowId, String comment, String idempotencyKey,
                                     String onBehalfOf) {
        WorkflowWithdrawRequest.Builder req = WorkflowWithdrawRequest.newBuilder()
                .setWorkflowId(workflowId);
        setOptional(req::setComment, comment);
        setOptional(req::setIdempotencyKey, idempotencyKey);
        setOptional(req::setOnBehalfOf, onBehalfOf);

        WorkflowWithdrawResponse response = callWithRetry(
                (ch, r) -> WorkflowGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .withdraw((WorkflowWithdrawRequest) r),
                req.build(), "workflow.withdraw");

        log.debug("Workflow withdraw: id={}, status={}", workflowId, response.getStatus());
        return response.getStatus();
    }

    @Override
    public String terminate(String workflowId, String reason, String idempotencyKey) {
        return terminateOnBehalfOf(workflowId, reason, idempotencyKey, null);
    }

    @Override
    public String terminateOnBehalfOf(String workflowId, String reason, String idempotencyKey,
                                      String onBehalfOf) {
        WorkflowTerminateRequest.Builder req = WorkflowTerminateRequest.newBuilder()
                .setWorkflowId(workflowId);
        setOptional(req::setReason, reason);
        setOptional(req::setIdempotencyKey, idempotencyKey);
        setOptional(req::setOnBehalfOf, onBehalfOf);

        WorkflowTerminateResponse response = callWithRetry(
                (ch, r) -> WorkflowGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .terminate((WorkflowTerminateRequest) r),
                req.build(), "workflow.terminate");

        log.debug("Workflow terminate: id={}, status={}", workflowId, response.getStatus());
        return response.getStatus();
    }

    @Override
    public List<PendingApprovalTask> listPendingTasks(int pageSize, String pageToken) {
        return listPendingTasksFor(null, pageSize, pageToken);
    }

    @Override
    public List<PendingApprovalTask> listPendingTasksFor(String assignee, int pageSize,
                                                         String pageToken) {
        WorkflowListPendingTasksRequest.Builder req = WorkflowListPendingTasksRequest.newBuilder()
                .setPageSize(Math.max(1, Math.min(pageSize, 200)));
        setOptional(req::setPageToken, pageToken);
        setOptional(req::setAssignee, assignee);

        WorkflowListPendingTasksResponse response = callWithRetry(
                (ch, r) -> WorkflowGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .listPendingTasks((WorkflowListPendingTasksRequest) r),
                req.build(), "workflow.listPendingTasks");

        List<PendingApprovalTask> result = new ArrayList<>();
        for (cn.byteforce.coord.contracts.workflow.v1.PendingApprovalTask t : response.getTasksList()) {
            result.add(new PendingApprovalTask(
                    t.getInstanceId(), t.getTaskId(), t.getNode(), t.getBranchId(),
                    t.getRoundId(), t.getKind(), t.getStatus(), t.getAssignee(),
                    t.getDeadlineMs(), t.getCreatedAtMs()));
        }
        log.debug("Workflow listPendingTasks: pageSize={}, count={}", pageSize, result.size());
        return result;
    }

    @Override
    public ApprovalInstanceDetail getInstanceDetail(String workflowId) {
        WorkflowGetInstanceDetailRequest request = WorkflowGetInstanceDetailRequest.newBuilder()
                .setWorkflowId(workflowId)
                .build();

        WorkflowGetInstanceDetailResponse response = callWithRetry(
                (ch, r) -> WorkflowGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .getInstanceDetail((WorkflowGetInstanceDetailRequest) r),
                request, "workflow.getInstanceDetail");

        List<ApprovalBranch> branches = new ArrayList<>();
        for (cn.byteforce.coord.contracts.workflow.v1.ApprovalBranch b : response.getBranchesList()) {
            branches.add(new ApprovalBranch(b.getId(), b.getNode(), b.getAttempt(),
                    b.getStatus(), b.getJoin(), b.getReject(), b.getRoundIdsList()));
        }
        List<ApprovalTask> tasks = new ArrayList<>();
        for (cn.byteforce.coord.contracts.workflow.v1.ApprovalTask t : response.getTasksList()) {
            List<ApprovalVote> votes = new ArrayList<>();
            for (cn.byteforce.coord.contracts.workflow.v1.ApprovalVote v : t.getVotesList()) {
                votes.add(new ApprovalVote(v.getRoundId(), v.getDecision(), v.getActor(),
                        v.getOnBehalfOf(), v.getComment(), v.getAtMs(), v.getActingService()));
            }
            tasks.add(new ApprovalTask(t.getId(), t.getBranchId(), t.getRoundId(), t.getKind(),
                    t.getStatus(), t.getAssignee(), t.getCandidatesList(), t.getDeadlineMs(),
                    votes));
        }
        List<ApprovalRound> rounds = new ArrayList<>();
        for (cn.byteforce.coord.contracts.workflow.v1.ApprovalRound r : response.getRoundsList()) {
            rounds.add(new ApprovalRound(r.getId(), r.getBranchId(), r.getNumber(), r.getStatus(),
                    r.getTotalCount(), r.getCompletedCount(), r.getRejectedCount()));
        }
        List<ApprovalAudit> audit = new ArrayList<>();
        for (cn.byteforce.coord.contracts.workflow.v1.ApprovalAudit a : response.getAuditList()) {
            audit.add(new ApprovalAudit(a.getAtMs(), a.getAction(), a.getBranchId(), a.getBy(),
                    a.getDetail(), a.getActingService()));
        }

        log.debug("Workflow getInstanceDetail: id={}, status={}, branches={}, tasks={}, rounds={}, audit={}",
                workflowId, response.getStatus(), branches.size(), tasks.size(), rounds.size(),
                audit.size());
        return new ApprovalInstanceDetail(
                response.getInstanceId(), response.getStatus(), response.getInitiator(),
                response.getRevision(), response.getDefinitionName(), branches, tasks, rounds,
                audit);
    }

    /** 仅在值非空时写入可选字段（空值保持 proto3 默认，wire 上不出现）。 */
    private static void setOptional(java.util.function.Consumer<String> setter, String value) {
        if (value != null && !value.isEmpty()) {
            setter.accept(value);
        }
    }

    // ──── 异步回调 ────

    @Override
    public CompletableFuture<WorkflowStatus> startAsync(String definitionId, byte[] input) {
        String instanceId = startByDefinition(definitionId, input);
        return watchInstance(instanceId);
    }

    @Override
    public CompletableFuture<WorkflowStatus> watchInstance(String instanceId) {
        return WorkflowWatchHandler.startWatching(instanceId, this::getStatus, watchExecutor);
    }

    /**
     * 结束全部未完成的实例观察（{@code CoordClient.close()} 调用）。
     *
     * <p>不这样做就会留下**永久挂起**的 future——调用方
     * （{@code watchInstance()} / {@code startAsync()}）再也收不到任何结果。
     *
     * @param cause 用于以异常完成各 future 的原因
     */
    public void shutdownWatches(Throwable cause) {
        WorkflowWatchHandler.cancelAll(cause);
    }
}
