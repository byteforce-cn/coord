package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.workflow.WorkflowStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.CancellationException;
import java.util.function.Function;

/**
 * Polls a workflow instance until it reaches a terminal state, completing a
 * {@link CompletableFuture}.
 *
 * <h2>第四轮 §3.14.4：这里此前会留下**永久挂起**的 future</h2>
 *
 * 修复前的行为：轮询线程用裸 {@code Thread.ofVirtual()} 建立，既不由
 * {@code ThreadPoolManager} 管理，也不注册进任何注册表。于是 {@code CoordClient.close()}
 * 之后：状态查询每次都抛异常，轮询循环按 30s 上限**无限重试**，而 {@code cancelled} 只在
 * future 完成时才被置位 —— 从未完成，也就永不取消。结果是
 * {@code watchInstance()} / {@code startAsync()} 的调用方**永久挂起**（SDK 内部也没有
 * {@code orTimeout}）。同时也解释了"忘记 close() 的进程不退出"。
 *
 * <p>现在：
 * <ul>
 *   <li>轮询跑在 {@code ThreadPoolManager} 的虚拟线程执行器上，{@code close()} 即可
 *       中断它（{@code shutdownNow} → {@code Thread.sleep} 抛 {@code InterruptedException}）；</li>
 *   <li>所有存活的 handler 登记在 {@link #LIVE}，{@link #cancelAll} 由
 *       {@code CoordClient.close()} 调用 → future 立即以异常完成，调用方**不会**挂死；</li>
 *   <li>连续失败有上限（{@link #MAX_CONSECUTIVE_FAILURES}）——客户端已经不可用时，
 *       与其无限重试，不如让调用方知道并自行决定退避策略。</li>
 * </ul>
 */
final class WorkflowWatchHandler {

    private static final Logger log = LoggerFactory.getLogger(WorkflowWatchHandler.class);

    private static final long INITIAL_POLL_INTERVAL_MS = 1_000L;
    private static final long MAX_POLL_INTERVAL_MS = 30_000L;
    private static final double BACKOFF_MULTIPLIER = 2.0;

    /**
     * 连续失败上限。≈ 1s+2s+4s+…+30s 共 20 次 ≈ 5 分钟，与 WatchManager 的额度一致。
     *
     * <p>达到上限即以异常完成 future：无限重试对调用方而言和"挂起"没有区别。
     */
    private static final int MAX_CONSECUTIVE_FAILURES = 20;

    /** 存活的 handler（{@code CoordClient.close()} 通过 {@link #cancelAll} 一次性了结）。 */
    private static final ConcurrentHashMap<WorkflowWatchHandler, Boolean> LIVE =
            new ConcurrentHashMap<>();

    /** 轮询退避策略（可注入，便于在毫秒级验证"放弃"路径）。 */
    static final class PollPolicy {
        static final PollPolicy DEFAULT = new PollPolicy(
                INITIAL_POLL_INTERVAL_MS, MAX_POLL_INTERVAL_MS, MAX_CONSECUTIVE_FAILURES);

        final long initialIntervalMs;
        final long maxIntervalMs;
        final int maxConsecutiveFailures;

        PollPolicy(long initialIntervalMs, long maxIntervalMs, int maxConsecutiveFailures) {
            this.initialIntervalMs = initialIntervalMs;
            this.maxIntervalMs = maxIntervalMs;
            this.maxConsecutiveFailures = maxConsecutiveFailures;
        }
    }

    private final String instanceId;
    private final Function<String, WorkflowStatus> statusFetcher;
    private final ExecutorService executor;
    private final PollPolicy policy;
    private final CompletableFuture<WorkflowStatus> future;
    private volatile boolean cancelled;

    /**
     * 创建 handler 并开始观察实例。
     *
     * @param instanceId    要观察的 workflow 实例
     * @param statusFetcher 调用 {@code getStatus(instanceId)} 的函数
     * @param executor      轮询线程的执行器（由 {@code ThreadPoolManager} 提供，
     *                      以便 {@code close()} 能中断它）
     */
    static CompletableFuture<WorkflowStatus> startWatching(
            String instanceId,
            Function<String, WorkflowStatus> statusFetcher,
            ExecutorService executor) {
        return startWatching(instanceId, statusFetcher, executor, PollPolicy.DEFAULT);
    }

    /** 同上，退避策略可注入（包内可见，供测试）。 */
    static CompletableFuture<WorkflowStatus> startWatching(
            String instanceId,
            Function<String, WorkflowStatus> statusFetcher,
            ExecutorService executor,
            PollPolicy policy) {
        WorkflowWatchHandler handler =
                new WorkflowWatchHandler(instanceId, statusFetcher, executor, policy);

        // Step 1: Immediate status check (avoid race with already-completed instances)
        // Tolerate transient failures — fall through to polling if the first call fails.
        try {
            WorkflowStatus status = statusFetcher.apply(instanceId);
            if (status.state().isTerminal()) {
                handler.future.complete(status);
                return handler.future;
            }
        } catch (Exception e) {
            log.debug("Initial status check failed for instanceId={}, will retry via polling: {}",
                    instanceId, e.getMessage());
            // Fall through to polling — don't fail the future for transient errors
        }

        // Step 2: Start background polling
        handler.startPolling();

        return handler.future;
    }

    /**
     * 了结全部存活观察：以 {@code cause} 完成其 future。
     *
     * <p>由 {@code CoordClient.close()} 调用。这是"关闭后 future 永久挂起"的修复出口。
     */
    static void cancelAll(Throwable cause) {
        int n = 0;
        for (WorkflowWatchHandler handler : LIVE.keySet()) {
            handler.cancel(cause);
            n++;
        }
        if (n > 0) {
            log.info("Cancelled {} pending workflow watch(es) during client shutdown", n);
        }
    }

    private WorkflowWatchHandler(String instanceId,
                                 Function<String, WorkflowStatus> statusFetcher,
                                 ExecutorService executor,
                                 PollPolicy policy) {
        this.instanceId = instanceId;
        this.statusFetcher = statusFetcher;
        this.executor = executor;
        this.policy = policy;
        this.future = new CompletableFuture<>();
        this.cancelled = false;

        // Clean up when future completes (success, failure, or cancellation)
        this.future.whenComplete((result, error) -> {
            cancelled = true;
            LIVE.remove(this);
        });
    }

    private void startPolling() {
        LIVE.put(this, Boolean.TRUE);
        try {
            executor.execute(this::pollLoop);
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // 执行器已关闭（客户端正在/已经 close）：不要把 future 悬在那里。
            LIVE.remove(this);
            cancelled = true;
            future.completeExceptionally(new IllegalStateException(
                    "CoordClient is closed; cannot start polling for instance " + instanceId, e));
        }
    }

    private void pollLoop() {
        long intervalMs = policy.initialIntervalMs;
        int consecutiveFailures = 0;

        while (!cancelled && !future.isDone()) {
            try {
                Thread.sleep(intervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (!future.isDone()) {
                    // 被 close() 中断：必须完成 future，否则调用方永久挂起。
                    future.completeExceptionally(new CancellationException(
                            "CoordClient closed while watching instance " + instanceId));
                }
                return;
            }

            if (cancelled || future.isDone()) {
                return;
            }

            try {
                WorkflowStatus status = statusFetcher.apply(instanceId);
                if (status.state().isTerminal()) {
                    log.debug("Workflow terminal state detected via poll: instanceId={}, state={}",
                            instanceId, status.state());
                    future.complete(status);
                    return;
                }
                // Reset backoff on successful poll (instance still running)
                intervalMs = policy.initialIntervalMs;
                consecutiveFailures = 0;
            } catch (Exception e) {
                consecutiveFailures++;
                if (consecutiveFailures >= policy.maxConsecutiveFailures) {
                    log.warn("Giving up polling instanceId={} after {} consecutive failures: {}",
                            instanceId, consecutiveFailures, e.getMessage());
                    // 不无限重试：让调用方知道，而不是给它一个永远不完成的 future。
                    future.completeExceptionally(new IllegalStateException(
                            "gave up polling workflow instance " + instanceId + " after "
                                    + consecutiveFailures + " consecutive failures", e));
                    return;
                }
                log.debug("Workflow poll failed for instanceId={} ({}/{}): {}",
                        instanceId, consecutiveFailures, policy.maxConsecutiveFailures,
                        e.getMessage());
                intervalMs = Math.min((long) (intervalMs * BACKOFF_MULTIPLIER), policy.maxIntervalMs);
            }
        }
    }

    /**
     * Cancels the watch, completing the future exceptionally.
     */
    void cancel() {
        cancel(new CancellationException("workflow watch cancelled"));
    }

    /**
     * Cancels the watch with an explicit cause.
     */
    void cancel(Throwable cause) {
        cancelled = true;
        LIVE.remove(this);
        if (!future.isDone()) {
            future.completeExceptionally(cause != null
                    ? cause
                    : new CancellationException("workflow watch cancelled"));
        }
    }
}
