package cn.byteforce.coord.sdk.workflow;

import java.util.List;

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
 * @param statuses       multi-value seat statuses (any match counts); {@code PENDING} is not
 *                       a seat status — expand it to
 *                       {@code created/claimed/suspended/scheduled} first
 * @param candidateGroups multi-value candidate pools (any match counts)
 */
public record TaskQueryFilter(String instanceId, String assignee, String completedBy,
                              String candidateGroup, String node, String status,
                              Long dueBeforeMs, List<String> statuses,
                              List<String> candidateGroups) {

    public TaskQueryFilter {
        statuses = statuses == null ? List.of() : List.copyOf(statuses);
        candidateGroups = candidateGroups == null ? List.of() : List.copyOf(candidateGroups);
    }

    /** Backward-compatible single-value filter (no multi-value filters). */
    public TaskQueryFilter(String instanceId, String assignee, String completedBy,
                           String candidateGroup, String node, String status,
                           Long dueBeforeMs) {
        this(instanceId, assignee, completedBy, candidateGroup, node, status, dueBeforeMs,
                List.of(), List.of());
    }
}
