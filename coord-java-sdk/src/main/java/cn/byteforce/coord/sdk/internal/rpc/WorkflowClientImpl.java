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
import cn.byteforce.coord.sdk.internal.proto.WorkflowListDefinitionsRequest;
import cn.byteforce.coord.sdk.internal.proto.WorkflowListDefinitionsResponse;
import cn.byteforce.coord.sdk.internal.proto.WorkflowListInstancesRequest;
import cn.byteforce.coord.sdk.internal.proto.WorkflowListInstancesResponse;
import cn.byteforce.coord.sdk.internal.proto.WorkflowSignalRequest;
import cn.byteforce.coord.sdk.internal.proto.WorkflowSignalResponse;
import cn.byteforce.coord.sdk.internal.proto.WorkflowStartRequest;
import cn.byteforce.coord.sdk.internal.proto.WorkflowStartResponse;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import cn.byteforce.coord.sdk.workflow.WorkflowClient;
import cn.byteforce.coord.sdk.workflow.WorkflowDefinition;
import cn.byteforce.coord.sdk.workflow.WorkflowDefinitionSummary;
import cn.byteforce.coord.sdk.workflow.WorkflowInstanceSummary;
import cn.byteforce.coord.sdk.workflow.WorkflowState;
import cn.byteforce.coord.sdk.workflow.WorkflowStatus;

import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
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

        log.debug("Workflow status: id={}, state={}, defName={}", workflowId, state, definitionName);
        return new WorkflowStatus(response.getWorkflowId(), state,
                0, output, errorMsg, definitionName, input);
    }

    @Override
    public void signal(String workflowId, String signalName, byte[] payload) {
        WorkflowSignalRequest.Builder req = WorkflowSignalRequest.newBuilder()
                .setWorkflowId(workflowId)
                .setSignalName(signalName);
        if (payload != null && payload.length > 0) {
            req.setPayload(ByteString.copyFrom(payload));
        }

        callWithRetry(
                (ch, r) -> WorkflowGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .signal((WorkflowSignalRequest) r),
                req.build(), "workflow.signal");

        log.debug("Workflow signal sent: id={}, signal={}", workflowId, signalName);
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

    // ──── 工作流定义管理 (Phase C.2) ────

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

        log.debug("Workflow deployed: id={}, namespace={}, name={}", response.getWorkflowId(), response.getNamespace(), response.getName());
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

        log.debug("Workflow getDefinition: id={}, name={}", response.getWorkflowId(), response.getName());
        return new WorkflowDefinition(
                response.getWorkflowId(), response.getName(), response.getDefinitionYaml(),
                response.getVersion(), response.getStatus(), response.getCreatedAt());
    }

    @Override
    public List<WorkflowInstanceSummary> listInstances(String workflowId) {
        WorkflowListInstancesRequest.Builder req = WorkflowListInstancesRequest.newBuilder()
                .setPageSize(50);
        if (workflowId != null && !workflowId.isEmpty()) {
            req.setWorkflowId(workflowId);
        }

        WorkflowListInstancesResponse response = callWithRetry(
                (ch, r) -> WorkflowGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .listInstances((WorkflowListInstancesRequest) r),
                req.build(), "workflow.listInstances");

        List<WorkflowInstanceSummary> result = new ArrayList<>();
        for (cn.byteforce.coord.sdk.internal.proto.WorkflowInstanceSummary s : response.getInstancesList()) {
            result.add(new WorkflowInstanceSummary(
                    s.getInstanceId(), s.getWorkflowId(), s.getState(),
                    s.getStartedAt(), s.getUpdatedAt(), s.getDefinitionName()));
        }
        log.debug("Workflow listInstances: workflowId={}, count={}", workflowId, result.size());
        return result;
    }
}
