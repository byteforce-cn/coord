package cn.byteforce.coord.sdk.internal.thread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manages the two required thread pools for the SDK.
 * <ul>
 *   <li>{@code heartbeatScheduler} — fixed pool of 4 platform threads for heartbeat scheduling and reconnection.</li>
 *   <li>{@code virtualThreadExecutor} — unbounded virtual thread pool for Watch stream loops and event callbacks.</li>
 * </ul>
 */
public final class ThreadPoolManager implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ThreadPoolManager.class);

    private final ScheduledExecutorService heartbeatScheduler;
    private final ExecutorService virtualThreadExecutor;

    public ThreadPoolManager(int heartbeatThreads) {
        this.heartbeatScheduler = Executors.newScheduledThreadPool(heartbeatThreads,
                Thread.ofPlatform().name("coord-hb-", 0).factory());
        this.virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    public ScheduledExecutorService getHeartbeatScheduler() {
        return heartbeatScheduler;
    }

    public ExecutorService getVirtualThreadExecutor() {
        return virtualThreadExecutor;
    }

    @Override
    public void close() {
        // 1. Shutdown heartbeat scheduler
        heartbeatScheduler.shutdown();
        // 2. Shutdown virtual thread executor
        virtualThreadExecutor.shutdown();

        try {
            // 3. Await termination
            boolean hbDone = heartbeatScheduler.awaitTermination(5, TimeUnit.SECONDS);
            boolean vtDone = virtualThreadExecutor.awaitTermination(10, TimeUnit.SECONDS);

            // 4. Force shutdown if needed
            if (!hbDone) {
                log.warn("Heartbeat scheduler did not terminate gracefully, forcing shutdown");
                heartbeatScheduler.shutdownNow();
                heartbeatScheduler.awaitTermination(2, TimeUnit.SECONDS);
            }
            if (!vtDone) {
                log.warn("Virtual thread executor did not terminate gracefully, forcing shutdown");
                virtualThreadExecutor.shutdownNow();
                virtualThreadExecutor.awaitTermination(2, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            heartbeatScheduler.shutdownNow();
            virtualThreadExecutor.shutdownNow();
        }
    }
}
