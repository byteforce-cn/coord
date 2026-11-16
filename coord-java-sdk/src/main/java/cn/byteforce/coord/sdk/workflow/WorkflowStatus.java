package cn.byteforce.coord.sdk.workflow;

import java.util.Collections;
import java.util.List;

/**
 * Snapshot of a workflow instance status.
 */
public final class WorkflowStatus {

    private final String workflowId;
    private final WorkflowState state;
    private final int currentStep;
    private final byte[] output;
    private final String errorMessage;
    private final String definitionName;
    private final byte[] input;
    private final long createdAt;
    private final long updatedAt;
    private final List<TaskFrame> taskStack;
    private final String currentStateName;
    private final Suspension suspension;
    private final String definitionNamespace;
    private final String definitionVersion;
    private final WorkflowFault fault;
    private final String approvalStatus;

    /**
     * Full constructor with all fields (当前状态名 + 挂起元信息).
     */
    public WorkflowStatus(String workflowId, WorkflowState state,
                          int currentStep, byte[] output, String errorMessage,
                          String definitionName, byte[] input,
                          long createdAt, long updatedAt,
                          List<TaskFrame> taskStack,
                          String currentStateName, Suspension suspension) {
        this(workflowId, state, currentStep, output, errorMessage, definitionName, input,
                createdAt, updatedAt, taskStack, currentStateName, suspension, "", "", null, "");
    }

    /**
     * Full constructor with the approval aggregate status.
     *
     * @param approvalStatus uppercase approval status ({@code RUNNING} / {@code CANCELLED} /
     *                       {@code TERMINATED} / ...); empty when no approval aggregate exists
     */
    public WorkflowStatus(String workflowId, WorkflowState state,
                          int currentStep, byte[] output, String errorMessage,
                          String definitionName, byte[] input,
                          long createdAt, long updatedAt,
                          List<TaskFrame> taskStack,
                          String currentStateName, Suspension suspension,
                          String approvalStatus) {
        this(workflowId, state, currentStep, output, errorMessage, definitionName, input,
                createdAt, updatedAt, taskStack, currentStateName, suspension, "", "", null,
                approvalStatus);
    }

    /**
     * Full constructor with the definition identity and the structured fault.
     *
     * @param definitionNamespace namespace of the pinned definition (may be empty)
     * @param definitionVersion   version of the pinned definition (may be empty)
     * @param fault               structured fault, or {@code null} when the instance has not failed
     */
    public WorkflowStatus(String workflowId, WorkflowState state,
                          int currentStep, byte[] output, String errorMessage,
                          String definitionName, byte[] input,
                          long createdAt, long updatedAt,
                          List<TaskFrame> taskStack,
                          String currentStateName, Suspension suspension,
                          String definitionNamespace, String definitionVersion,
                          WorkflowFault fault) {
        this(workflowId, state, currentStep, output, errorMessage, definitionName, input,
                createdAt, updatedAt, taskStack, currentStateName, suspension,
                definitionNamespace, definitionVersion, fault, "");
    }

    /**
     * Full constructor with every field (definition identity + structured fault + approval status).
     */
    public WorkflowStatus(String workflowId, WorkflowState state,
                          int currentStep, byte[] output, String errorMessage,
                          String definitionName, byte[] input,
                          long createdAt, long updatedAt,
                          List<TaskFrame> taskStack,
                          String currentStateName, Suspension suspension,
                          String definitionNamespace, String definitionVersion,
                          WorkflowFault fault, String approvalStatus) {
        this.workflowId = workflowId;
        this.state = state;
        this.currentStep = currentStep;
        this.output = output;
        this.errorMessage = errorMessage;
        this.definitionName = definitionName;
        this.input = input;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.taskStack = taskStack != null ? Collections.unmodifiableList(taskStack) : Collections.emptyList();
        this.currentStateName = currentStateName != null ? currentStateName : "";
        this.suspension = suspension;
        this.definitionNamespace = definitionNamespace != null ? definitionNamespace : "";
        this.definitionVersion = definitionVersion != null ? definitionVersion : "";
        this.fault = fault;
        this.approvalStatus = approvalStatus != null ? approvalStatus : "";
    }

    /**
     * Backward-compatible constructor (timestamps 0, empty taskStack, no suspension).
     */
    public WorkflowStatus(String workflowId, WorkflowState state,
                          int currentStep, byte[] output, String errorMessage,
                          String definitionName, byte[] input,
                          long createdAt, long updatedAt,
                          List<TaskFrame> taskStack) {
        this(workflowId, state, currentStep, output, errorMessage,
                definitionName, input, createdAt, updatedAt, taskStack, "", null);
    }

    /**
     * Backward-compatible constructor (createdAt/updatedAt default to 0, taskStack empty).
     */
    public WorkflowStatus(String workflowId, WorkflowState state,
                          int currentStep, byte[] output, String errorMessage,
                          String definitionName, byte[] input) {
        this(workflowId, state, currentStep, output, errorMessage,
                definitionName, input, 0, 0, Collections.emptyList(), "", null);
    }

    public String workflowId() { return workflowId; }
    public WorkflowState state() { return state; }
    public int currentStep() { return currentStep; }
    public byte[] output() { return output; }
    public String errorMessage() { return errorMessage; }
    public String definitionName() { return definitionName; }
    public byte[] input() { return input; }

    /**
     * Instance creation time (Unix milliseconds).
     */
    public long createdAt() { return createdAt; }

    /**
     * Instance last update time (Unix milliseconds).
     */
    public long updatedAt() { return updatedAt; }

    /**
     * Current task execution stack.
     * Returns an unmodifiable list; may be empty if not available.
     */
    public List<TaskFrame> taskStack() { return taskStack; }

    /**
     * 当前状态名 —— 定义里声明的稳定状态名（如 {@code human} / {@code autostart}）。
     *
     * <p>CNCF SW 编译器为路由 / 转移生成的内部任务名（{@code <状态>__route} /
     * {@code <状态>__transition} / {@code <状态>__action_<i>} 等）不会出现：读取时已按
     * 实例钉死的定义归一化回声明状态名。遗留 {@code document/do} DSL 无「声明状态」
     * 概念，仍返回当前任务名。需要原始引擎任务名做诊断时用 {@link #taskStack()}。
     *
     * @return current state name, or empty string if not available
     */
    public String currentStateName() { return currentStateName; }

    /**
     * Namespace of the definition pinned when the instance started.
     *
     * @return definition namespace, or empty string for legacy snapshots
     */
    public String definitionNamespace() { return definitionNamespace; }

    /**
     * Version of the definition pinned when the instance started.
     *
     * <p>This is the version the instance runs, not the definition's latest version.</p>
     *
     * @return definition version, or empty string for legacy snapshots
     */
    public String definitionVersion() { return definitionVersion; }

    /**
     * Structured fault of a failed instance.
     *
     * @return the RFC 7807 projection, or {@code null} if the instance has not failed
     */
    public WorkflowFault fault() { return fault; }

    /**
     * 挂起元信息（SUSPENDED/WAITING 时返回，否则为 {@code null}）。
     *
     * @return suspension metadata, or {@code null} if not suspended
     */
    public Suspension suspension() { return suspension; }

    /**
     * 审批聚合状态（大写：RUNNING / COMPLETED / REJECTED / CANCELLED / TERMINATED / FAULTED）；
     * 未进入 approve 节点时为空串。
     *
     * <p>用于区分工作流状态同为 {@code FAILED/FAULTED} 的「管理终止」
     * （{@code TERMINATED}）与运行 fault（聚合仍 {@code RUNNING} 或已 {@code CANCELLED}）。</p>
     */
    public String approvalStatus() { return approvalStatus; }

    @Override
    public String toString() {
        return "WorkflowStatus{workflowId='" + workflowId + "', state=" + state
                + ", currentStep=" + currentStep + ", definitionName='" + definitionName
                + "', currentStateName='" + currentStateName
                + "', errorMessage='" + errorMessage
                + "', definitionVersion='" + definitionVersion
                + "', createdAt=" + createdAt + ", updatedAt=" + updatedAt
                + ", taskStackSize=" + taskStack.size()
                + ", suspension=" + suspension
                + ", fault=" + fault
                + ", approvalStatus='" + approvalStatus + "'}";
    }
}
