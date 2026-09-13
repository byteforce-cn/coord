package cn.byteforce.coord.sdk.internal.watch;

import cn.byteforce.coord.sdk.internal.thread.ThreadPoolManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import java.util.function.ToLongFunction;

/**
 * Manages all active Watch subscriptions.
 *
 * <p>每个订阅在一条虚拟线程上阻塞消费自己的 {@link CancellableStream}；事件按到达顺序
 * 在**消费线程上同步**记录 revision 水位后，再异步派发给业务回调。
 *
 * <h2>第四轮 §3.14.3：这里此前是一个"静默死亡"的实现</h2>
 *
 * 修复前：{@code runWatchLoop} 把流**迭代一次**，正常结束或任何异常都只打一条
 * {@code log.info}、置 {@code active=false} 就返回 —— 不重连、不 backoff、
 * 不按 {@code lastRevision} 续订、也**不通知调用方**。类注释里"连接恢复后从 last known
 * revision 恢复"是假的。同时被 kill 的 watch **不从 map 移除**，于是 map 无界增长、
 * 陈旧条目永久残留。
 *
 * <p>现在：
 * <ul>
 *   <li><b>重连并续订</b>：流结束（无论正常还是异常）且订阅未被取消时，按指数退避重连，
 *       并从 {@code lastRevision + 1} 续订。服务端 {@code start_revision} 是**闭区间**
 *       起点（{@code coord-server/src/watch/mod.rs}：读取 {@code [start_revision, ∞)}
 *       且**包含** start_revision），故 +1 恰好从"下一条未投递事件"开始 ——
 *       既不丢事件也不重复投递；</li>
 *   <li><b>可配置</b>：{@code CoordConfig.autoRestoreWatches}（默认 true）终于被读到了。
 *       关闭它则退化为"单次订阅"（旧行为），但**仍会**通知调用方，不再静默；</li>
 *   <li><b>终态清理</b>：订阅彻底结束（取消、重连耗尽、或自动恢复被关闭）时从注册表
 *       移除，并回调 {@link ActiveWatch#onTerminated}，让业务知道"这条订阅已经死了"；</li>
 *   <li><b>取消立即生效</b>：{@link ActiveWatch#cancel()} 关闭底层流，阻塞中的消费
 *       线程立刻醒来（见 {@link CancellableStream}），不再等到下一条事件。</li>
 * </ul>
 */
public final class WatchManager {

    private static final Logger log = LoggerFactory.getLogger(WatchManager.class);

    /** 首次重连退避。 */
    static final long INITIAL_RETRY_BACKOFF_MS = 200L;
    /** 退避上限。 */
    static final long MAX_RETRY_BACKOFF_MS = 30_000L;

    /**
     * 连续重连失败的上限，超过即放弃并把订阅判定为终态（约 5 分钟）。
     *
     * <p>不设上限意味着"永远重试"——那同样是一种静默：业务看到的是一个看起来活着、
     * 实际永远收不到事件的订阅。放弃时必须**通知**，这正是本类要修的缺陷。
     */
    static final int MAX_CONSECUTIVE_RETRIES = 20;

    private final ThreadPoolManager threadPoolManager;
    private final boolean autoRestoreWatches;
    private final long initialRetryBackoffMs;
    private final long maxRetryBackoffMs;
    private final int maxConsecutiveRetries;
    private final ConcurrentHashMap<String, ActiveWatch> watches = new ConcurrentHashMap<>();

    public WatchManager(ThreadPoolManager threadPoolManager, boolean autoRestoreWatches) {
        this(threadPoolManager, autoRestoreWatches,
                INITIAL_RETRY_BACKOFF_MS, MAX_RETRY_BACKOFF_MS, MAX_CONSECUTIVE_RETRIES);
    }

    /**
     * 退避策略可注入的构造器：供测试在**毫秒级**验证"退避上限/放弃"路径。
     *
     * <p>用一个只影响时序的参数化入口，好过在测试里真的等指数退避累积到 30s —— 后者要么让
     * 测试慢到没人跑，要么让"达到上限就放弃并通知"这条路径永远没被测过。
     * 生产装配请用两参数的公共构造器。
     */
    public WatchManager(ThreadPoolManager threadPoolManager, boolean autoRestoreWatches,
                        long initialRetryBackoffMs, long maxRetryBackoffMs,
                        int maxConsecutiveRetries) {
        this.threadPoolManager = threadPoolManager;
        this.autoRestoreWatches = autoRestoreWatches;
        this.initialRetryBackoffMs = initialRetryBackoffMs;
        this.maxRetryBackoffMs = maxRetryBackoffMs;
        this.maxConsecutiveRetries = maxConsecutiveRetries;
    }

    /** 当前活跃订阅数（测试与可观测用）。 */
    public int activeWatchCount() {
        return watches.size();
    }

    /** 自动恢复订阅是否开启（来自 {@code CoordConfig.autoRestoreWatches}）。 */
    public boolean isAutoRestoreWatches() {
        return autoRestoreWatches;
    }

    /**
     * 启动一个订阅：注册到表中，并在一条虚拟线程上跑消费循环。
     */
    public void startWatch(ActiveWatch watch) {
        watches.put(watch.watchId, watch);
        threadPoolManager.getVirtualThreadExecutor().execute(() -> runWatchLoop(watch));
    }

    private void runWatchLoop(ActiveWatch watch) {
        watch.active.set(true);
        long backoffMs = initialRetryBackoffMs;
        int consecutiveFailures = 0;
        Throwable terminal = null;

        try {
            while (watch.active.get()) {
                CancellableStream stream = null;
                try {
                    stream = watch.streamFactory.apply(watch.nextStartRevision());
                    watch.currentStream = stream;

                    while (watch.active.get() && stream.hasNext()) {
                        Object event = stream.next();

                        // 1) 同步推进 revision 水位：必须在派发**之前**、且在消费线程上完成。
                        //    修复前水位是在业务回调里更新的，而回调是异步派发的——流断开时
                        //    水位可能还没前进，重连就会重复投递。
                        long revision = watch.revisionExtractor.applyAsLong(event);
                        if (revision > 0) {
                            watch.setLastRevision(revision);
                        }

                        // 2) 异步派发给业务回调（回调抛异常不得杀死订阅循环）
                        dispatch(watch, event);
                    }

                    if (!watch.active.get()) {
                        return; // 我们主动取消：不是故障，直接退出。
                    }
                    Throwable failure = stream.failure();
                    consecutiveFailures++;
                    if (failure != null) {
                        log.warn("watch stream failed for watchId={} (attempt {}): {}",
                                watch.watchId, consecutiveFailures, failure.toString());
                    } else {
                        log.info("watch stream ended for watchId={} (attempt {}); will reconnect "
                                        + "from revision {}",
                                watch.watchId, consecutiveFailures, watch.getLastRevision() + 1);
                    }
                } catch (Exception e) {
                    if (!watch.active.get()) {
                        return;
                    }
                    consecutiveFailures++;
                    log.warn("watch loop error for watchId={} (attempt {}): {}",
                            watch.watchId, consecutiveFailures, e.toString());
                } finally {
                    watch.currentStream = null;
                    if (stream != null) {
                        stream.close();
                    }
                }

                if (!autoRestoreWatches) {
                    terminal = new IllegalStateException(
                            "watch stream ended and autoRestoreWatches=false");
                    log.warn("watch watchId={} ended; NOT reconnecting because "
                            + "autoRestoreWatches=false", watch.watchId);
                    return;
                }
                if (consecutiveFailures >= maxConsecutiveRetries) {
                    terminal = new IllegalStateException(
                            "watch gave up after " + consecutiveFailures
                                    + " consecutive failures (last revision "
                                    + watch.getLastRevision() + ")");
                    log.error("watch watchId={} gave up after {} consecutive failures; "
                                    + "events are NO LONGER being delivered",
                            watch.watchId, consecutiveFailures);
                    return;
                }
                if (!sleepBackoff(backoffMs)) {
                    return; // 被中断（通常是 close()）
                }
                backoffMs = Math.min(backoffMs * 2, maxRetryBackoffMs);
            }
        } finally {
            // 终态：从注册表移除（修复前不移除 → map 无界增长 + 陈旧条目），
            // 置 active=false，并把终态原因告知订阅方（修复前完全静默）。
            watches.remove(watch.watchId, watch);
            watch.active.set(false);
            if (terminal != null && watch.onTerminated != null) {
                try {
                    watch.onTerminated.accept(terminal);
                } catch (Exception e) {
                    log.warn("watch onTerminated callback threw for watchId={}: {}",
                            watch.watchId, e.toString());
                }
            }
        }
    }

    /**
     * 派发事件到业务回调。回调在虚拟线程上执行，抛出的异常只记录不传播
     * （一条坏事件不能杀死整条订阅）。
     */
    private void dispatch(ActiveWatch watch, Object event) {
        threadPoolManager.getVirtualThreadExecutor().execute(() -> {
            try {
                @SuppressWarnings("unchecked")
                Consumer<Object> handler = (Consumer<Object>) watch.handler;
                handler.accept(event);
            } catch (Exception e) {
                log.warn("Watch handler error for watchId={}", watch.watchId, e);
            }
        });
    }

    /** @return false 表示被中断（应退出循环）。 */
    private static boolean sleepBackoff(long ms) {
        try {
            Thread.sleep(ms);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * 取消并移除指定订阅（立即生效：关闭底层流，唤醒消费线程）。
     */
    public void cancelWatch(String watchId) {
        ActiveWatch watch = watches.remove(watchId);
        if (watch != null) {
            watch.cancel();
        }
    }

    /**
     * 取消并清空全部订阅（{@code CoordClient.close()} 调用）。
     */
    public void shutdown() {
        for (ActiveWatch watch : watches.values()) {
            watch.cancel();
        }
        watches.clear();
    }

    /**
     * Represents an active watch subscription.
     */
    public static class ActiveWatch {
        final String watchId;
        /** 由 startRevision 构造一条**可取消**的事件流。 */
        final LongFunction<CancellableStream> streamFactory;
        final Consumer<?> handler;
        /** 从事件中取出 revision（消费线程同步调用，用于续订水位）。 */
        final ToLongFunction<Object> revisionExtractor;
        final AtomicLong lastRevision;
        final AtomicBoolean active = new AtomicBoolean(false);
        /** 订阅终态（放弃/关闭）时的通知；可为 null。 */
        final Consumer<Throwable> onTerminated;
        /** 首连使用的 start revision（0 = 从最新开始）。 */
        final long initialRevision;

        /** 当前底层流（取消时用它立即唤醒消费线程）。 */
        volatile CancellableStream currentStream;

        public ActiveWatch(String watchId,
                           LongFunction<CancellableStream> streamFactory,
                           Consumer<?> handler,
                           ToLongFunction<Object> revisionExtractor,
                           long startRevision) {
            this(watchId, streamFactory, handler, revisionExtractor, startRevision, null);
        }

        public ActiveWatch(String watchId,
                           LongFunction<CancellableStream> streamFactory,
                           Consumer<?> handler,
                           ToLongFunction<Object> revisionExtractor,
                           long startRevision,
                           Consumer<Throwable> onTerminated) {
            this.watchId = watchId;
            this.streamFactory = streamFactory;
            this.handler = handler;
            this.revisionExtractor = revisionExtractor;
            this.lastRevision = new AtomicLong(startRevision);
            this.initialRevision = startRevision;
            this.onTerminated = onTerminated;
        }

        /**
         * 本次（重）连应请求的起始 revision。
         *
         * <p>首连：{@code initialRevision}（0 = 从最新开始）。
         * 重连：{@code lastRevision + 1} —— 服务端 {@code start_revision} 含起点，
         * 因此 +1 恰好从"下一条未投递事件"开始，既不重复也不丢失。
         */
        long nextStartRevision() {
            long last = lastRevision.get();
            if (last <= 0) {
                return Math.max(initialRevision, 0);
            }
            return last + 1;
        }

        public boolean isActive() {
            return active.get();
        }

        /** 取消订阅：立即关闭底层流以唤醒阻塞中的消费线程。 */
        public void cancel() {
            active.set(false);
            CancellableStream stream = currentStream;
            if (stream != null) {
                stream.close();
            }
        }

        public long getLastRevision() {
            return lastRevision.get();
        }

        public void setLastRevision(long revision) {
            lastRevision.set(revision);
        }

        public String getWatchId() {
            return watchId;
        }
    }
}
