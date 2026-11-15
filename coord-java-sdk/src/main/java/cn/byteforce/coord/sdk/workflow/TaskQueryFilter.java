package cn.byteforce.coord.sdk.workflow;

/**
 * Filter for {@link WorkflowClient#listTasks(TaskQueryFilter, int, String)}.
 *
 * <p>Every field is optional: {@code null} (or a {@code null} {@code dueBeforeMs}) means
 * "do not filter on this field". {@code assignee} matches the principal that can work the
 * seat (owner / pool member / delegatee); {@code completedBy} matches any principal that
 * already voted (including on-behalf-of actors).</p>
 *
 * @param instanceId     restrict to one workflow instance (no cross-instance scan)
 * @param assignee       principal that can work the seat
 * @param completedBy    principal that already voted on the seat
 * @param candidateGroup candidate pool containing this principal/group
 * @param node           node (step) name
 * @param status         seat status: {@code created|claimed|completed|rejected|cancelled|expired}
 *                       (null/empty = all; unknown values are rejected by the server with
 *                       {@code INVALID_ARGUMENT})
 * @param dueBeforeMs    only seats with {@code deadline <= dueBeforeMs} (Unix ms; null = no filter)
 */
public record TaskQueryFilter(String instanceId, String assignee, String completedBy,
                              String candidateGroup, String node, String status,
                              Long dueBeforeMs) {
}
