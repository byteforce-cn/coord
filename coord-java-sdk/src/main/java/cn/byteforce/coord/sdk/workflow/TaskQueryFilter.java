package cn.byteforce.coord.sdk.workflow;

/**
 * Filter for {@link WorkflowClient#listTasks(TaskQueryFilter, int, String)}.
 *
 * <p>Every component is optional: {@code null} (or an empty string) leaves that
 * dimension unfiltered. {@code dueBeforeMs} only filters when it is non-null and
 * positive (Unix milliseconds); {@code null} or {@code 0} means "no due filter".</p>
 *
 * <p><b>The filter is not an authorization scope.</b> {@code assignee} selects the
 * seats a principal can act on (assignee / pool member / delegate), but the RPC
 * itself is a read surface gated by {@code coord:workflow:read}: with an empty
 * filter it scans every instance visible to the agent.</p>
 *
 * @param instanceId     restrict to a single workflow instance (no cross-instance scan)
 * @param assignee       principal that can act on the seat
 * @param completedBy    principal that already voted (including on-behalf-of actors)
 * @param candidateGroup candidate pool containing this principal / group
 * @param node           node (step) name
 * @param status         seat status: {@code created} / {@code claimed} / {@code suspended} /
 *                       {@code scheduled} / {@code completed} / {@code rejected} /
 *                       {@code cancelled} / {@code expired}; unknown values are rejected
 *                       by the server with {@code INVALID_ARGUMENT}
 * @param dueBeforeMs    only seats whose deadline is {@code <=} this value (Unix ms)
 */
public record TaskQueryFilter(String instanceId, String assignee, String completedBy,
                              String candidateGroup, String node, String status,
                              Long dueBeforeMs) {
}
