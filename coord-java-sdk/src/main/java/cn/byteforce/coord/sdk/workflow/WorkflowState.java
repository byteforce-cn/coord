package cn.byteforce.coord.sdk.workflow;

/**
 * Represents the current state of a workflow instance.
 *
 * <p>对齐 Open Workflow DSL §Status Phases：pending / running / waiting / suspended /
 * completed / faulted / cancelled（FAULTED 为旧 FAILED 的语义别名）。
 */
public enum WorkflowState {
    PENDING("pending"),
    RUNNING("running"),
    WAITING("waiting"),
    SUSPENDED("suspended"),
    COMPLETED("completed"),
    FAILED("failed"),
    FAULTED("faulted"),
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
        return this == COMPLETED || this == FAILED || this == FAULTED
                || this == COMPENSATED || this == CANCELLED
                || this == TIMED_OUT;
    }

    /**
     * 解析 gRPC 状态字符串（大小写不敏感；FAULTED/FAILED 等价）。
     */
    public static WorkflowState fromProtoName(String name) {
        if (name == null) {
            return PENDING;
        }
        String lower = name.toLowerCase(java.util.Locale.ROOT);
        for (WorkflowState s : values()) {
            if (s.protoName.equals(lower)) {
                return s;
            }
        }
        // 兼容旧 "FAILED" 状态字符串
        if ("failed".equals(lower)) {
            return FAILED;
        }
        return PENDING;
    }
}
