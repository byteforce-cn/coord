package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.CoordException;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.sdk.internal.proto.WorkflowCancelRequest;
import cn.byteforce.coord.sdk.internal.proto.WorkflowCancelResponse;
import cn.byteforce.coord.sdk.internal.proto.WorkflowDeployRequest;
import cn.byteforce.coord.sdk.internal.proto.WorkflowDeployResponse;
import cn.byteforce.coord.sdk.internal.proto.WorkflowGetDefinitionRequest;
import cn.byteforce.coord.sdk.internal.proto.WorkflowGetDefinitionResponse;
import cn.byteforce.coord.sdk.internal.proto.WorkflowGetStatusRequest;
import cn.byteforce.coord.sdk.internal.proto.WorkflowGetStatusResponse;
import cn.byteforce.coord.sdk.internal.proto.WorkflowGrpc;
import cn.byteforce.coord.sdk.internal.proto.WorkflowListDefinitionVersionsRequest;
import cn.byteforce.coord.sdk.internal.proto.WorkflowListDefinitionVersionsResponse;
import cn.byteforce.coord.sdk.internal.proto.WorkflowListDefinitionsRequest;
import cn.byteforce.coord.sdk.internal.proto.WorkflowListDefinitionsResponse;
import cn.byteforce.coord.sdk.internal.proto.WorkflowListInstancesRequest;
import cn.byteforce.coord.sdk.internal.proto.WorkflowListInstancesResponse;
import cn.byteforce.coord.sdk.internal.proto.WorkflowRollbackDefinitionRequest;
import cn.byteforce.coord.sdk.internal.proto.WorkflowRollbackDefinitionResponse;
import cn.byteforce.coord.sdk.internal.proto.WorkflowSignalRequest;
import cn.byteforce.coord.sdk.internal.proto.WorkflowSignalResponse;
import cn.byteforce.coord.sdk.internal.proto.WorkflowStartRequest;
import cn.byteforce.coord.sdk.internal.proto.WorkflowStartResponse;
import cn.byteforce.coord.sdk.internal.proto.TaskFrame;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
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

    public WorkflowClientImpl(AgentChannelManager channelManager, ErrorMapper errorMapper,
                              RetryTemplate retryTemplate, ObservabilityProvider observability,
                              CoordConfig config) {
        super(channelManager, errorMapper, retryTemplate, observability);
        this.config = config;
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
        // Reuse the start RPC with a minimal DSL wrapper referencing the definition ID.
        // The coord-agent start handler accepts both raw DSL and definition references.
        String dsl = "{\"definitionId\":\"" + definitionId + "\"}";
        return start(dsl, input);
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

        log.debug("Workflow status: id={}, state={}, defName={}, taskStackSize={}",
                workflowId, state, definitionName, taskStack.size());
        return new WorkflowStatus(response.getWorkflowId(), state,
                currentStep, output, errorMsg, definitionName, input,
                createdAt, updatedAt, taskStack);
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
        for (cn.byteforce.coord.sdk.internal.proto.WorkflowDefinitionSummary s : response.getDefinitionsList()) {
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
        for (cn.byteforce.coord.sdk.internal.proto.WorkflowDefinitionVersion v : response.getVersionsList()) {
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
        for (cn.byteforce.coord.sdk.internal.proto.WorkflowInstanceSummary s : response.getInstancesList()) {
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

    // ──── 异步回调 ────

    @Override
    public CompletableFuture<WorkflowStatus> startAsync(String definitionId, byte[] input) {
        String instanceId = startByDefinition(definitionId, input);
        return watchInstance(instanceId);
    }

    @Override
    public CompletableFuture<WorkflowStatus> watchInstance(String instanceId) {
        return WorkflowWatchHandler.startWatching(instanceId, this::getStatus);
    }
}
