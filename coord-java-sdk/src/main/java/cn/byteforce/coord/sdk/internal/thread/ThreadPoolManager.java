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
 *   <li>{@code heartbeatScheduler} — fixed pool of platform threads for heartbeat scheduling and reconnection.</li>
 *   <li>{@code virtualThreadExecutor} — virtual thread pool for Watch stream loops, workflow polling and event callbacks.</li>
 * </ul>
 *
 * <p><b>第四轮 §3.14.4：心跳线程是 daemon。</b> 此前这里是普通平台线程，于是"忘记调用
 * {@code CoordClient.close()}"的进程**永远退不出**（虚拟线程本身不会阻止 JVM 退出，
 * 平台线程会）。daemon 化之后，忘记 close 的代价是资源泄漏而不是进程挂死；
 * 正确做法仍然是 close()。
 */
public final class ThreadPoolManager implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ThreadPoolManager.class);

    private final ScheduledExecutorService heartbeatScheduler;
    private final ExecutorService virtualThreadExecutor;

    public ThreadPoolManager(int heartbeatThreads) {
        this.heartbeatScheduler = Executors.newScheduledThreadPool(heartbeatThreads,
                Thread.ofPlatform().daemon(true).name("coord-hb-", 0).factory());
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
