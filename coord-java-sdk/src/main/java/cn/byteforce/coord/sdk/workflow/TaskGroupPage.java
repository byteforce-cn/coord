package cn.byteforce.coord.sdk.workflow;

import java.util.List;

/**
 * One page of {@link WorkflowClient#listTaskGroups(TaskQueryFilter, int, String)}.
 *
 * <p>{@code pageSize} is a strict upper bound on the number of rounds in a page (unlike
 * {@link TaskPage}, which paginates by instance), and {@code nextPageToken} is stable: fetching
 * every page never duplicates or skips a round.</p>
 */
public record TaskGroupPage(List<TaskGroup> groups, String nextPageToken) {

    public TaskGroupPage {
        groups = groups == null ? List.of() : List.copyOf(groups);
        nextPageToken = nextPageToken == null ? "" : nextPageToken;
    }

    /** Whether the server reported another page. */
    public boolean hasNextPage() {
        return !nextPageToken.isEmpty();
    }
}
