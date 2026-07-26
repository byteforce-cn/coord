package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.workflow.WorkflowState;
import cn.byteforce.coord.sdk.workflow.WorkflowStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Manages async workflow completion watching via polling with exponential backoff.
 * <p>
 * On construction, immediately fetches the current status. If already terminal,
 * completes the future synchronously. Otherwise, starts a background polling loop
 * on a virtual thread that queries {@code getStatus} at increasing intervals
 * (1s → 2s → 4s → ... → 30s max).
 * <p>
 * The future can be cancelled externally via {@link #cancel()}, which interrupts
 * the polling loop and completes the future exceptionally with
 * {@link java.util.concurrent.CancellationException}.
 */
final class WorkflowWatchHandler {

    private static final Logger log = LoggerFactory.getLogger(WorkflowWatchHandler.class);

    private static final long INITIAL_POLL_INTERVAL_MS = 1_000L;
    private static final long MAX_POLL_INTERVAL_MS = 30_000L;
    private static final double BACKOFF_MULTIPLIER = 2.0;

    private final String instanceId;
    private final Function<String, WorkflowStatus> statusFetcher;
    private final CompletableFuture<WorkflowStatus> future;
    private volatile boolean cancelled;

    /**
     * Creates a handler and begins watching the given instance.
     * <p>
     * If the instance is already in a terminal state, the returned future
     * is already completed. Otherwise, polling begins on a virtual thread.
     *
     * @param instanceId    the workflow instance ID to watch
     * @param statusFetcher a function that calls {@code getStatus(instanceId)}
     * @return a future that completes when the instance reaches a terminal state
     */
    static CompletableFuture<WorkflowStatus> startWatching(
            String instanceId,
            Function<String, WorkflowStatus> statusFetcher) {
        WorkflowWatchHandler handler = new WorkflowWatchHandler(instanceId, statusFetcher);

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

    private WorkflowWatchHandler(String instanceId,
                                  Function<String, WorkflowStatus> statusFetcher) {
        this.instanceId = instanceId;
        this.statusFetcher = statusFetcher;
        this.future = new CompletableFuture<>();
        this.cancelled = false;

        // Clean up when future completes (success, failure, or cancellation)
        this.future.whenComplete((result, error) -> cancelled = true);
    }

    private void startPolling() {
        Thread.ofVirtual()
                .name("coord-wf-watch-" + instanceId.substring(0, Math.min(8, instanceId.length())))
                .start(() -> pollLoop());
    }

    private void pollLoop() {
        long intervalMs = INITIAL_POLL_INTERVAL_MS;

        while (!cancelled && !future.isDone()) {
            try {
                Thread.sleep(intervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (!future.isDone()) {
                    future.completeExceptionally(e);
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
                intervalMs = INITIAL_POLL_INTERVAL_MS;
            } catch (Exception e) {
                // getStatus failure — back off but keep retrying
                log.debug("Workflow poll failed for instanceId={}: {}", instanceId, e.getMessage());
                intervalMs = Math.min(
                        (long) (intervalMs * BACKOFF_MULTIPLIER),
                        MAX_POLL_INTERVAL_MS);
            }
        }
    }

    /**
     * Cancels the watch, completing the future exceptionally.
     */
    void cancel() {
        cancelled = true;
        if (!future.isDone()) {
            future.cancel(true);
        }
    }
}
