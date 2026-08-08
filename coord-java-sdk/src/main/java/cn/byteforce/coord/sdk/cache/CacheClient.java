package cn.byteforce.coord.sdk.cache;

import cn.byteforce.coord.sdk.CoordException;

import java.util.List;
import java.util.Map;

/**
 * Distributed cache API (data-plane, per-agent redb backend).
 * <p>
 * Supports String, Hash, List, and Set data types with optional TTL.
 * <p>
 * <b>Data-plane boundary (v2.1):</b> data is stored on the Agent's embedded redb
 * engine. By default (<code>services.replication=false</code>) this is
 * <b>single-agent semantics</b>. When cross-agent ISR replication is enabled
 * (<code>services.replication=true</code> + <code>replication_peers</code>),
 * writes are synchronously replicated to ISR followers (<code>min_isr</code>
 * configurable) — the data plane is then distributed / highly available.
 * See docs/cache-mq-isr-evaluation.md (v2.1: implemented).
 *
 * <pre>{@code
 * try (CoordClient client = CoordClient.create(config)) {
 *     CacheClient cache = client.cache();
 *     cache.set("user:1001", "{\"name\":\"Alice\"}".getBytes(), 3600);
 *     byte[] value = cache.get("user:1001");
 *     cache.hset("user:1001:profile", "email", "alice@example.com".getBytes());
 * }
 * }</pre>
 */
public interface CacheClient {

    // ──── String operations ────

    /**
     * Get a string value by key.
     *
     * @return the value, or null if the key does not exist
     */
    byte[] get(String key);

    /**
     * Set a string value with optional TTL.
     *
     * @param key        cache key
     * @param value      value bytes
     * @param ttlSeconds TTL in seconds, 0 or negative means no expiration
     */
    void set(String key, byte[] value, long ttlSeconds);

    /**
     * Delete a key.
     *
     * @return true if the key existed and was deleted
     */
    boolean delete(String key);

    // ──── Hash operations ────

    /**
     * Get a hash field value.
     *
     * @return the field value, or null if the key or field does not exist
     */
    byte[] hget(String key, String field);

    /**
     * Set a hash field value.
     */
    void hset(String key, String field, byte[] value);

    /**
     * Get all fields and values of a hash.
     */
    Map<String, byte[]> hgetAll(String key);

    // ──── List operations ────

    /**
     * Push a value to the left of a list.
     *
     * @return the new length of the list
     */
    long lpush(String key, byte[] value);

    /**
     * Get a range of elements from a list.
     *
     * @param start start index (inclusive, 0-based)
     * @param stop  stop index (inclusive, -1 for end)
     */
    List<byte[]> lrange(String key, long start, long stop);

    /**
     * Atomically remove and return the rightmost (tail) element of a list.
     * <p>
     * The pop is executed as a single write transaction on the agent, so
     * concurrent consumers never see duplicates or lost elements.
     *
     * @param key list key
     * @return the popped value, or null if the list does not exist or is empty
     */
    byte[] rpop(String key);

    /**
     * Get the number of elements in a list.
     *
     * @param key list key
     * @return the current list length (0 if the key does not exist)
     */
    long llen(String key);

    // ──── Set operations ────

    /**
     * Add a member to a set.
     */
    void sadd(String key, byte[] member);

    /**
     * Get all members of a set.
     */
    List<byte[]> smembers(String key);
}
