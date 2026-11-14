package cn.byteforce.coord.sdk.mq.v2;

import java.util.List;

/**
 * One page of a list RPC. {@code nextPageToken} is echoed verbatim from the
 * wire; an empty token means the listing is exhausted.
 */
public record Page<T>(List<T> items, String nextPageToken) {
    public Page {
        items = items == null ? List.of() : List.copyOf(items);
        nextPageToken = nextPageToken == null ? "" : nextPageToken;
    }
}
