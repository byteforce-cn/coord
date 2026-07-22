package cn.byteforce.coord.sdk.workflow;

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

    public WorkflowStatus(String workflowId, WorkflowState state,
                          int currentStep, byte[] output, String errorMessage,
                          String definitionName, byte[] input) {
        this.workflowId = workflowId;
        this.state = state;
        this.currentStep = currentStep;
        this.output = output;
        this.errorMessage = errorMessage;
        this.definitionName = definitionName;
        this.input = input;
    }

    public String workflowId() { return workflowId; }
    public WorkflowState state() { return state; }
    public int currentStep() { return currentStep; }
    public byte[] output() { return output; }
    public String errorMessage() { return errorMessage; }
    public String definitionName() { return definitionName; }
    public byte[] input() { return input; }

    @Override
    public String toString() {
        return "WorkflowStatus{workflowId='" + workflowId + "', state=" + state
                + ", currentStep=" + currentStep + ", definitionName='" + definitionName
                + "', errorMessage='" + errorMessage + "'}";
    }
}
