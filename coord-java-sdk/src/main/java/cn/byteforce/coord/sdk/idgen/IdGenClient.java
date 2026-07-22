package cn.byteforce.coord.sdk.idgen;

import cn.byteforce.coord.sdk.CoordException;

import java.util.List;

/**
 * Distributed ID generator API.
 * <p>
 * Provides globally unique ID generation using Coord's segment-based allocation.
 * IDs are trend-increasing with potential gaps (gap rate ≤0.1% at segment size 1000).
 *
 * <pre>{@code
 * try (CoordClient client = CoordClient.create(config)) {
 *     IdGenClient idgen = client.idgen();
 *     long orderId = idgen.nextId("orders");
 *     List<Long> batch = idgen.nextBatch("orders", 100);
 * }
 * }</pre>
 */
public interface IdGenClient {

    /**
     * Generate the next unique ID for the given business key.
     *
     * @param name business key (e.g., "orders", "users")
     * @return a globally unique, trend-increasing ID
     * @throws CoordException on communication or allocation failure
     */
    long nextId(String name);

    /**
     * Generate a batch of unique IDs for the given business key.
     *
     * @param name  business key
     * @param count number of IDs to generate (1–10000)
     * @return list of unique, trend-increasing IDs
     * @throws CoordException on communication or allocation failure
     */
    List<Long> nextBatch(String name, int count);
}
