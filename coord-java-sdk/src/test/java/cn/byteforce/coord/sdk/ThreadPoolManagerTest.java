package cn.byteforce.coord.sdk;

import cn.byteforce.coord.sdk.internal.thread.ThreadPoolManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class ThreadPoolManagerTest {

    private ThreadPoolManager threadPoolManager;

    @AfterEach
    void tearDown() {
        if (threadPoolManager != null) {
            threadPoolManager.close();
        }
    }

    @Test
    void shouldScheduleHeartbeatTask() throws Exception {
        threadPoolManager = new ThreadPoolManager(4);
        CountDownLatch latch = new CountDownLatch(1);

        threadPoolManager.getHeartbeatScheduler().schedule(
                latch::countDown, 10, TimeUnit.MILLISECONDS);

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void shouldExecuteOnVirtualThread() throws Exception {
        threadPoolManager = new ThreadPoolManager(4);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean isVirtual = new AtomicBoolean(false);

        threadPoolManager.getVirtualThreadExecutor().execute(() -> {
            isVirtual.set(Thread.currentThread().isVirtual());
            latch.countDown();
        });

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(isVirtual.get()).isTrue();
    }

    @Test
    void shouldShutdownGracefully() {
        threadPoolManager = new ThreadPoolManager(4);
        threadPoolManager.close();
        // After close, the pools should be shut down
        assertThat(threadPoolManager.getHeartbeatScheduler().isShutdown()).isTrue();
        assertThat(threadPoolManager.getVirtualThreadExecutor().isShutdown()).isTrue();
    }

    @Test
    void shouldInterruptVirtualThreadOnShutdown() throws Exception {
        threadPoolManager = new ThreadPoolManager(4);
        CountDownLatch started = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean(false);

        threadPoolManager.getVirtualThreadExecutor().execute(() -> {
            started.countDown();
            try {
                Thread.sleep(60_000); // Long sleep
            } catch (InterruptedException e) {
                interrupted.set(true);
            }
        });

        assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();
        threadPoolManager.close();

        assertThat(interrupted.get()).isTrue();
    }
}
