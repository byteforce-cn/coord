package cn.byteforce.coord.sdk.workflow;

import java.util.List;

/**
 * A seat returned by {@link WorkflowClient#getTask(String)}.
 *
 * <p>Composes the shared {@link ApprovalTask} model with the instance status the
 * {@code GetTask} response carries, instead of duplicating the seat fields in a
 * second type.</p>
 *
 * @param task           the seat (includes votes, assignments and task-graph links)
 * @param instanceStatus uppercase instance status at read time
 *                       ({@code RUNNING} / {@code COMPLETED} / ...)
 * @param roundTasks     every seat of the same round (includes {@code task} itself)
 * @param round          the owning round (counts + decision), or {@code null} if unknown
 */
public record TaskDetail(ApprovalTask task, String instanceStatus,
                         List<ApprovalTask> roundTasks, ApprovalRound round) {

    public TaskDetail {
        roundTasks = roundTasks == null ? List.of() : List.copyOf(roundTasks);
    }

    /** Backward-compatible constructor (no round aggregation). */
    public TaskDetail(ApprovalTask task, String instanceStatus) {
        this(task, instanceStatus, List.of(), null);
    }
}
