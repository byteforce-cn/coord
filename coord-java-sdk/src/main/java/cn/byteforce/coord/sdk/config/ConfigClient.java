package cn.byteforce.coord.sdk.config;

import java.util.Map;
import java.util.Optional;

/**
 * Public API for dynamic configuration access.
 *
 * <h2>快照 + 增量（resync 纪律）</h2>
 *
 * <p>配置查看是**收敛通知**：先用 {@link #listWithRevision(String)} 取快照
 * （entries + 位点 {@code revision}），再从该位点
 * {@link #watchFrom(String, long, ConfigListener)} 订阅——快照后**被删除**的 key
 * 也能从位点事件可靠收敛（服务端先回放 revision ≥ 位点的变更再转实时；
 * 可能重复，应用需幂等）。断线重连由 SDK 自动从 {@code last_revision + 1} 续订。
 *
 * <h2>删除与空值</h2>
 *
 * <p>{@link ConfigEvent#getNewValue()} 为 {@link java.util.Optional#empty()} 表示
 * **该 key 已被删除**；存储的空值表现为存在且为空串
 * （{@code Optional.of("")}）。键存在性以 {@code getString} 的返回值/
 * {@code found} 语义为准，不以"空串"推断"不存在"。
 */
public interface ConfigClient {
    Optional<String> getString(String key);
    Optional<Integer> getInt(String key);
    Optional<Long> getLong(String key);
    Optional<Boolean> getBoolean(String key);
    <T> Optional<T> getObject(String key, Class<T> type);
    Map<String, String> list(String prefix);

    /**
     * 按前缀读取**带位点**的快照（resync 纪律的起点）。
     *
     * @param prefix key 前缀（空串 = 全部）
     * @return 快照：entries + 位点 revision（0 = 位点尚未建立）
     */
    ConfigSnapshot listWithRevision(String prefix);

    ConfigWatchSubscription watch(String prefix, ConfigListener listener);

    /**
     * 从已知位点**续订**。
     *
     * <p>语义与断线重连一致：首次请求位点 = {@code startRevision + 1}
     * （即该位点及其之前的变更不重放）。对快照位点 W 而言，≤ W 的变更已包含在快照里；
     * 对「上次处理的事件位点」而言这是不重复的续订。传 {@code 0} = 从最新开始。
     *
     * @param prefix         key 前缀
     * @param startRevision  已知位点（快照位点 / 上次处理的事件位点）
     * @param listener       事件回调（删除事件 = {@code newValue} 缺省）
     */
    ConfigWatchSubscription watchFrom(String prefix, long startRevision, ConfigListener listener);

    /**
     * Put a configuration key-value pair.
     *
     * @param key   the configuration key
     * @param value the configuration value
     */
    void put(String key, String value);

    /**
     * Put and return the write's global position (same coordinates as
     * {@link ConfigSnapshot#getRevision()}); usable as a lower bound for
     * "watch guaranteed to include this write".
     */
    long putReturningRevision(String key, String value);
}
