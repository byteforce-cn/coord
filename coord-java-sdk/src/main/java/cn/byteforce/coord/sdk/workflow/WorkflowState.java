package cn.byteforce.coord.sdk.workflow;

/**
 * Represents the current state of a workflow instance.
 */
public enum WorkflowState {
    PENDING("pending"),
    RUNNING("running"),
    SUSPENDED("suspended"),
    COMPLETED("completed"),
    FAILED("failed"),
    COMPENSATED("compensated"),
    CANCELLED("cancelled"),
    TIMED_OUT("timed_out");

    private final String protoName;

    WorkflowState(String protoName) {
        this.protoName = protoName;
    }

    public String getProtoName() {
        return protoName;
    }

    /**
     * Returns true if this state represents a terminal (finished) state.
     */
    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED
                || this == COMPENSATED || this == CANCELLED
                || this == TIMED_OUT;
    }

    public static WorkflowState fromProtoName(String name) {
        for (WorkflowState s : values()) {
            if (s.protoName.equals(name)) {
                return s;
            }
        }
        return PENDING;
    }
}
