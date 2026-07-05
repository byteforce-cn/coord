package cn.byteforce.coord.sdk;

import cn.byteforce.coord.sdk.internal.thread.ThreadPoolManager;
import cn.byteforce.coord.sdk.internal.watch.WatchManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class WatchManagerTest {

    private ThreadPoolManager threadPoolManager;
    private WatchManager watchManager;

    @BeforeEach
    void setUp() {
        threadPoolManager = new ThreadPoolManager(2);
        watchManager = new WatchManager(threadPoolManager);
    }

    @AfterEach
    void tearDown() {
        if (watchManager != null) {
            watchManager.shutdown();
        }
        if (threadPoolManager != null) {
            threadPoolManager.close();
        }
    }

    @Test
    void shouldDeliverEventsToHandler() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        List<String> received = new ArrayList<>();

        Iterator<String> iter = List.of("event1", "event2").iterator();
        List<Iterator<String>> iterHolder = new ArrayList<>();
        iterHolder.add(iter);

        WatchManager.ActiveWatch watch = new WatchManager.ActiveWatch(
                "watch-1",
                () -> iterHolder.get(0),
                event -> {
                    received.add(event);
                    latch.countDown();
                },
                0
        );

        watchManager.startWatch(watch);

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        // Events may arrive out of order due to virtual thread dispatch
        assertThat(received).containsExactlyInAnyOrder("event1", "event2");
        assertThat(watch.isActive()).isFalse(); // Stream exhausted
    }

    @Test
    void shouldStopWhenCancelled() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        AtomicInteger count = new AtomicInteger(0);

        // Infinite iterator
        Iterator<String> infiniteIter = new Iterator<>() {
            @Override
            public boolean hasNext() { return true; }
            @Override
            public String next() {
                started.countDown();
                return "event-" + count.incrementAndGet();
            }
        };

        WatchManager.ActiveWatch watch = new WatchManager.ActiveWatch(
                "watch-2",
                () -> infiniteIter,
                event -> {},
                0
        );

        watchManager.startWatch(watch);
        assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();

        // Cancel
        watch.cancel();
        Thread.sleep(200);

        int snapshot = count.get();
        Thread.sleep(500);
        // Should have stopped processing
        assertThat(count.get()).isEqualTo(snapshot);
    }

    @Test
    void shouldTrackLastRevision() {
        WatchManager.ActiveWatch watch = new WatchManager.ActiveWatch(
                "watch-3", () -> List.<String>of().iterator(), e -> {}, 42L
        );
        assertThat(watch.getLastRevision()).isEqualTo(42L);

        watch.setLastRevision(100L);
        assertThat(watch.getLastRevision()).isEqualTo(100L);
    }
}
