package cn.byteforce.coord.sdk.workflow;

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
 */
public record TaskDetail(ApprovalTask task, String instanceStatus) {
}
