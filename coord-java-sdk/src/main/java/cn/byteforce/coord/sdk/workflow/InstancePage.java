package cn.byteforce.coord.sdk.workflow;

import java.util.List;
import java.util.Objects;

/**
 * A page of workflow instances returned by the windowed {@code listInstances} overload.
 *
 * <p>Unlike the legacy {@code List}-returning overloads this page carries the server's
 * {@code next_page_token}, so a caller can walk every page without re-querying from the
 * start.</p>
 *
 * @param instances     the instances in this page (never {@code null})
 * @param nextPageToken token for the next page; empty string means "last page"
 */
public record InstancePage(List<WorkflowInstanceSummary> instances, String nextPageToken) {

    public InstancePage {
        instances = instances == null ? List.of() : List.copyOf(instances);
        nextPageToken = nextPageToken == null ? "" : nextPageToken;
    }

    /** Whether the server reported another page. */
    public boolean hasNextPage() {
        return !nextPageToken.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof InstancePage that)) {
            return false;
        }
        return Objects.equals(instances, that.instances)
                && Objects.equals(nextPageToken, that.nextPageToken);
    }

    @Override
    public int hashCode() {
        return Objects.hash(instances, nextPageToken);
    }
}
