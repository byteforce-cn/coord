package cn.byteforce.coord.sdk.cache;

import cn.byteforce.coord.sdk.CoordException;

import java.util.List;
import java.util.Map;

/**
 * Distributed cache API (data-plane, per-agent redb backend).
 * <p>
 * Supports String, Hash, List, and Set data types with optional TTL.
 * <p>
 * <b>Data-plane boundary:</b> data is stored on the Agent's embedded redb
 * engine. By default (<code>services.replication=false</code>) this is
 * <b>single-agent semantics</b>. When cross-agent ISR replication is enabled
 * (<code>services.replication=true</code> + <code>replication_peers</code>),
 * writes are synchronously replicated to ISR followers (<code>min_isr</code>
 * configurable) — the data plane is then distributed / highly available.

 *
 * <p><b>Error semantics:</b> only <i>misses</i> return {@code null} / empty
 * collections (wire-level {@code found=false}). Every other failure — including
 * {@code PERMISSION_DENIED} when the CCT lacks {@code coord:cache:read} /
 * {@code coord:cache:write} — propagates as {@link CoordException}; it is never
 * converted into "not found".
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
     * Atomically set a string value <b>only if the key does not exist</b>
     * (SETNX / putIfAbsent). Expired keys count as absent.
     *
     * <p>Semantics:
     * <ul>
     *   <li>returns {@code true} when this call performed the write; the TTL applies
     *       only then. When {@code false}, the existing value and TTL are untouched.</li>
     *   <li>Single-agent: check-and-set in one write transaction (atomic under
     *       concurrency). ISR enabled: decided in the Leader's local transaction and
     *       replicated on success.</li>
     *   <li><b>Error semantics match {@code set}</b>: on error the local write may
     *       already be in effect. A retry is a <i>fresh attempt to acquire</i> — if the
     *       first attempt actually wrote, the retry returns {@code false} (like Redis
     *       SETNX); never use {@code false} to infer "this call did not write".</li>
     * </ul>
     *
     * @return {@code true} iff this call wrote the value
     */
    boolean setIfAbsent(String key, byte[] value, long ttlSeconds);

    /**
     * Enumerate keys under a logical-key prefix (bounded, cursor-paginated,
     * string/hash/list/set logical keys deduplicated and ascending).
     *
     * <p>Bounded scan per call (never a full-table blocking scan); best-effort view
     * of <b>this agent's</b> data (a lagging ISR replica may miss recent keys). Keys
     * must not contain NUL bytes. Pass an empty {@code cursor} to start; loop while
     * {@link CacheKeyPage#hasMore()}.
     *
     * @param prefix logical-key prefix ("" = all keys)
     * @param limit  page size; {@code 0} = server default (1000), capped server-side
     * @param cursor continuation cursor from the previous page ("" to start)
     */
    CacheKeyPage keys(String prefix, int limit, String cursor);

    /**
     * Clear every entry whose <b>logical key</b> starts with {@code prefix}
     * (all data types: string / hash / list / set).
     *
     * <p>Administrative path for namespace cleanup / capacity reclaim:
     * <ul>
     *   <li>no matches = success (no-op); deletion cannot be rolled back (no archive);</li>
     *   <li>executed in bounded batches; concurrent writes race by arrival order;</li>
     *   <li>capacity accounting (B-PL-3) is decremented with the deletions;</li>
     *   <li>ISR enabled: the clear decision is replicated cluster-wide (idempotent apply).</li>
     * </ul>
     *
     * @param prefix logical-key prefix ("" = every key)
     */
    CacheClearResult clear(String prefix);

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
