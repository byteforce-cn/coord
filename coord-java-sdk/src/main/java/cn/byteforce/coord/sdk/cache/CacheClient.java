package cn.byteforce.coord.sdk.cache;

import cn.byteforce.coord.sdk.CoordException;

import java.util.List;
import java.util.Map;

/**
 * Distributed cache API (data-plane, redb-backed by default).
 * <p>
 * Supports String, Hash, List, and Set data types with optional TTL.
 * Data is stored on the local Agent's embedded redb engine with optional
 * cross-agent synchronous replication (ISR≥2).
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
