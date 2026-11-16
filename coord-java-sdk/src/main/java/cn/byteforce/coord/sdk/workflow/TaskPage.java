package cn.byteforce.coord.sdk.workflow;

import java.util.List;

/**
 * One page of {@link WorkflowClient#listTasks(TaskQueryFilter, int, String)}.
 *
 * <p>{@code nextPageToken} is opaque: pass it back as {@code pageToken} to fetch the
 * next page; an empty string means the last page. Note that the server paginates by
 * <i>instance</i>, so a single page may contain more seats than {@code pageSize} when
 * several seats of one instance match. Seats in a list projection carry
 * {@code assignments} but no {@code votes} — use {@link WorkflowClient#getTask(String)}
 * when votes are needed.</p>
 */
public record TaskPage(List<ApprovalTask> tasks, String nextPageToken) {

    public TaskPage {
        tasks = tasks == null ? List.of() : List.copyOf(tasks);
        nextPageToken = nextPageToken == null ? "" : nextPageToken;
    }
}
