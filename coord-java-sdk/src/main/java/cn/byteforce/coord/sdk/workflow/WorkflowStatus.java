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

    /**
     * Full constructor with all fields (ISSUE-010 §2：当前状态名 + 挂起元信息).
     */
    public WorkflowStatus(String workflowId, WorkflowState state,
                          int currentStep, byte[] output, String errorMessage,
                          String definitionName, byte[] input,
                          long createdAt, long updatedAt,
                          List<TaskFrame> taskStack,
                          String currentStateName, Suspension suspension) {
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
     * 当前状态名（SW 状态名 = 当前任务名，含审批驳回 / 分支路由后的目标状态名）。
     *
     * @return current state name, or empty string if not available
     */
    public String currentStateName() { return currentStateName; }

    /**
     * 挂起元信息（SUSPENDED/WAITING 时返回，否则为 {@code null}）。
     *
     * @return suspension metadata, or {@code null} if not suspended
     */
    public Suspension suspension() { return suspension; }

    @Override
    public String toString() {
        return "WorkflowStatus{workflowId='" + workflowId + "', state=" + state
                + ", currentStep=" + currentStep + ", definitionName='" + definitionName
                + "', currentStateName='" + currentStateName
                + "', errorMessage='" + errorMessage
                + "', createdAt=" + createdAt + ", updatedAt=" + updatedAt
                + ", taskStackSize=" + taskStack.size()
                + ", suspension=" + suspension + "}";
    }
}
