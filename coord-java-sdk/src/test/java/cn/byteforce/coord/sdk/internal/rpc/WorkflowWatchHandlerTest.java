package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.workflow.WorkflowState;
import cn.byteforce.coord.sdk.workflow.WorkflowStatus;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * TDD tests for {@link WorkflowWatchHandler} — async workflow completion watching.
 * <p>
 * Tests the polling-based completion detection with exponential backoff,
 * immediate completion for already-terminal instances, and cancellation.
 */
class WorkflowWatchHandlerTest {

    private static WorkflowStatus runningStatus(String instanceId) {
        return new WorkflowStatus(instanceId, WorkflowState.RUNNING,
                1, null, null, "test-def", null);
    }

    private static WorkflowStatus completedStatus(String instanceId, byte[] output) {
        return new WorkflowStatus(instanceId, WorkflowState.COMPLETED,
                3, output, null, "test-def", null,
                System.currentTimeMillis() - 60_000,
                System.currentTimeMillis(),
                java.util.Collections.emptyList());
    }

    private static WorkflowStatus failedStatus(String instanceId, String errorMsg) {
        return new WorkflowStatus(instanceId, WorkflowState.FAILED,
                2, null, errorMsg, "test-def", null,
                System.currentTimeMillis() - 30_000,
                System.currentTimeMillis(),
                java.util.Collections.emptyList());
    }

    // ──── Immediate completion tests ────

    @Test
    void shouldCompleteImmediatelyWhenAlreadyCompleted() throws Exception {
        String instanceId = "wf-completed-001";
        byte[] output = "{\"result\":\"ok\"}".getBytes();

        CompletableFuture<WorkflowStatus> future = WorkflowWatchHandler.startWatching(
                instanceId, id -> completedStatus(id, output));

        assertThat(future).isDone();
        assertThat(future).isNotCancelled();
        WorkflowStatus status = future.get(1, TimeUnit.SECONDS);
        assertThat(status.state()).isEqualTo(WorkflowState.COMPLETED);
        assertThat(status.output()).isEqualTo(output);
    }

    @Test
    void shouldCompleteImmediatelyWhenAlreadyFailed() throws Exception {
        String instanceId = "wf-failed-001";
        String errorMsg = "Task 'callHttp' failed: connection refused";

        CompletableFuture<WorkflowStatus> future = WorkflowWatchHandler.startWatching(
                instanceId, id -> failedStatus(id, errorMsg));

        assertThat(future).isDone();
        WorkflowStatus status = future.get(1, TimeUnit.SECONDS);
        assertThat(status.state()).isEqualTo(WorkflowState.FAILED);
        assertThat(status.errorMessage()).isEqualTo(errorMsg);
    }

    @Test
    void shouldCompleteImmediatelyWhenAlreadyCancelled() throws Exception {
        String instanceId = "wf-cancelled-001";

        CompletableFuture<WorkflowStatus> future = WorkflowWatchHandler.startWatching(
                instanceId,
                id -> new WorkflowStatus(id, WorkflowState.CANCELLED,
                        0, null, null, "test-def", null));

        assertThat(future).isDone();
        WorkflowStatus status = future.get(1, TimeUnit.SECONDS);
        assertThat(status.state().isTerminal()).isTrue();
    }

    // ──── Polling completion tests ────

    @Test
    void shouldDetectCompletionViaPolling() throws Exception {
        String instanceId = "wf-poll-001";
        byte[] output = "{\"approved\":true}".getBytes();

        AtomicInteger callCount = new AtomicInteger(0);

        CompletableFuture<WorkflowStatus> future = WorkflowWatchHandler.startWatching(
                instanceId, id -> {
                    int count = callCount.incrementAndGet();
                    if (count >= 3) {
                        // Transition to COMPLETED on 3rd poll
                        return completedStatus(id, output);
                    }
                    return runningStatus(id);
                });

        // Should not be done immediately (first call returns RUNNING)
        assertThat(future).isNotDone();

        // Wait for polling to detect completion (3 polls at 1s interval)
        WorkflowStatus status = future.get(10, TimeUnit.SECONDS);

        assertThat(status.state()).isEqualTo(WorkflowState.COMPLETED);
        assertThat(status.output()).isEqualTo(output);
        assertThat(callCount.get()).isGreaterThanOrEqualTo(3);
    }

    @Test
    void shouldHandleGetStatusExceptionGracefully() throws Exception {
        String instanceId = "wf-flaky-001";
        byte[] output = "{\"result\":\"recovered\"}".getBytes();

        AtomicInteger callCount = new AtomicInteger(0);

        CompletableFuture<WorkflowStatus> future = WorkflowWatchHandler.startWatching(
                instanceId, id -> {
                    int count = callCount.incrementAndGet();
                    if (count <= 2) {
                        // Simulate transient failures
                        throw new RuntimeException("Temporary network error");
                    }
                    // Succeed on 3rd attempt
                    return completedStatus(id, output);
                });

        WorkflowStatus status = future.get(10, TimeUnit.SECONDS);
        assertThat(status.state()).isEqualTo(WorkflowState.COMPLETED);
        assertThat(status.output()).isEqualTo(output);
        assertThat(callCount.get()).isGreaterThanOrEqualTo(3);
    }

    // ──── Cancellation test ────

    @Test
    void shouldSupportFutureCancellation() throws Exception {
        String instanceId = "wf-cancel-test-001";

        AtomicInteger callCount = new AtomicInteger(0);

        CompletableFuture<WorkflowStatus> future = WorkflowWatchHandler.startWatching(
                instanceId, id -> {
                    callCount.incrementAndGet();
                    return runningStatus(id);
                });

        assertThat(future).isNotDone();

        // Cancel the future
        boolean cancelled = future.cancel(true);
        assertThat(cancelled).isTrue();
        assertThat(future).isCancelled();

        // Give the poll loop time to notice cancellation
        Thread.sleep(200);
        int snapshot = callCount.get();
        // Should not continue polling after cancellation
        Thread.sleep(1500);
        assertThat(callCount.get()).isLessThanOrEqualTo(snapshot + 2);
    }

    // ──── State validation tests ────

    @Test
    void shouldNotCompleteWhenStillRunning() throws Exception {
        String instanceId = "wf-still-running-001";

        CompletableFuture<WorkflowStatus> future = WorkflowWatchHandler.startWatching(
                instanceId, id -> runningStatus(id));

        assertThat(future).isNotDone();
        // Clean up
        future.cancel(true);
    }

    @Test
    void shouldNotCompleteWhenSuspended() throws Exception {
        String instanceId = "wf-suspended-001";

        CompletableFuture<WorkflowStatus> future = WorkflowWatchHandler.startWatching(
                instanceId,
                id -> new WorkflowStatus(id, WorkflowState.SUSPENDED,
                        2, null, null, "test-def", null));

        assertThat(future).isNotDone();
        future.cancel(true);
    }

    @Test
    void shouldCompleteForAllTerminalStates() throws Exception {
        // Verify all 5 terminal states trigger immediate completion
        WorkflowState[] terminalStates = {
                WorkflowState.COMPLETED, WorkflowState.FAILED,
                WorkflowState.COMPENSATED, WorkflowState.CANCELLED,
                WorkflowState.TIMED_OUT
        };

        for (WorkflowState state : terminalStates) {
            String instanceId = "wf-term-" + state.getProtoName();
            CompletableFuture<WorkflowStatus> future = WorkflowWatchHandler.startWatching(
                    instanceId,
                    id -> new WorkflowStatus(id, state, 0, null, null, "def", null));

            assertThat(future).isDone()
                    .as("State %s should be terminal", state);
            assertThat(future.get().state()).isEqualTo(state);
        }
    }
}
