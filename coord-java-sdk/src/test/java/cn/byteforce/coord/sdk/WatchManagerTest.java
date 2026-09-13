package cn.byteforce.coord.sdk;

import cn.byteforce.coord.sdk.internal.thread.ThreadPoolManager;
import cn.byteforce.coord.sdk.internal.watch.CancellableStream;
import cn.byteforce.coord.sdk.internal.watch.WatchManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * WatchManager 行为测试。
 *
 * <p>第四轮 §3.14.3 的回归卡口集中在四件事上，它们都是"静默死亡"的不同侧面：
 * <ol>
 *   <li><b>断线要重连，并从 {@code lastRevision + 1} 续订</b>（修复前只迭代一次流）;</li>
 *   <li><b>终态要通知调用方</b>（修复前完全静默）;</li>
 *   <li><b>终态要从注册表移除</b>（修复前 map 无界增长 + 陈旧条目残留）;</li>
 *   <li><b>取消要立即生效</b>（修复前取消只置标志位，要等下一条事件才生效 —— 对一个不再
 *       产生事件的订阅等于没做）。</li>
 * </ol>
 */
class WatchManagerTest {

    private ThreadPoolManager threadPoolManager;
    private WatchManager watchManager;

    @BeforeEach
    void setUp() {
        threadPoolManager = new ThreadPoolManager(2);
        watchManager = new WatchManager(threadPoolManager, true);
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
        // WatchManager 在各自的虚拟线程上派发事件，收集器必须线程安全
        List<String> received = new CopyOnWriteArrayList<>();

        // 流交付两个事件后**保持打开**（模拟健康订阅）
        FakeStream stream = new FakeStream().event("event1", 1).event("event2", 2);

        WatchManager.ActiveWatch watch = new WatchManager.ActiveWatch(
                "watch-1",
                rev -> stream,
                event -> {
                    received.add(((FakeEvent) event).payload());
                    latch.countDown();
                },
                e -> ((FakeEvent) e).revision(),
                0
        );

        watchManager.startWatch(watch);

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(received).containsExactlyInAnyOrder("event1", "event2");
        assertThat(watch.isActive()).isTrue();
        assertThat(watchManager.activeWatchCount()).isEqualTo(1);

        watch.cancel();
    }

    /**
     * §3.14.3 核心回归：流断掉后必须重连，且**从 lastRevision + 1 续订**。
     *
     * <p>修复前：循环体只跑一次，第二个流根本不会被建立。
     */
    @Test
    void shouldReconnectAndResumeFromLastRevisionPlusOne() throws Exception {
        AtomicInteger connects = new AtomicInteger();
        List<Long> requestedRevisions = new CopyOnWriteArrayList<>();

        // 第一个流交付 rev=5 后**结束**；第二个流保持打开
        FakeStream first = new FakeStream().event("a", 5).end();
        FakeStream second = new FakeStream();

        WatchManager.ActiveWatch watch = new WatchManager.ActiveWatch(
                "watch-reconnect",
                rev -> {
                    requestedRevisions.add(rev);
                    return connects.incrementAndGet() == 1 ? first : second;
                },
                event -> { },
                e -> ((FakeEvent) e).revision(),
                0
        );

        watchManager.startWatch(watch);

        // 等待重连真正发生（第二次构造流）
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (requestedRevisions.size() < 2 && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }

        assertThat(requestedRevisions)
                .as("流结束后必须重连")
                .hasSizeGreaterThanOrEqualTo(2);
        assertThat(requestedRevisions.get(0)).isEqualTo(0L);  // 首连：从最新开始
        assertThat(requestedRevisions.get(1))
                .as("重连必须从 lastRevision + 1 续订（服务端 start_revision 含起点）")
                .isEqualTo(6L);
    }

    /**
     * 连续失败达到上限后必须放弃并通知（而非永远重试 —— 那对调用方同样等于挂起）。
     *
     * <p>用毫秒级退避（包内可见的策略入口），否则这条路径要等真实退避累积到数分钟，
     * 结果就是"没人跑、也永远没被测过"。
     */
    @Test
    void shouldGiveUpAfterMaxConsecutiveFailuresAndNotify() throws Exception {
        WatchManager fastGiveUp = new WatchManager(threadPoolManager, true, 1L, 5L, 3);
        try {
            CountDownLatch terminated = new CountDownLatch(1);
            AtomicReference<Throwable> cause = new AtomicReference<>();

            WatchManager.ActiveWatch watch = new WatchManager.ActiveWatch(
                    "watch-giveup",
                    rev -> new FakeStream().fail(new RuntimeException("boom")),
                    event -> { },
                    e -> 0L,
                    0,
                    t -> {
                        cause.set(t);
                        terminated.countDown();
                    }
            );

            fastGiveUp.startWatch(watch);

            assertThat(terminated.await(10, TimeUnit.SECONDS))
                    .as("连续失败超过上限后必须通知调用方")
                    .isTrue();
            assertThat(cause.get().getMessage()).contains("gave up after");
            assertThat(fastGiveUp.activeWatchCount()).isZero();
        } finally {
            fastGiveUp.shutdown();
        }
    }

    /**
     * §3.14.3 第二个断言：{@code autoRestoreWatches=false} 时流结束必须**通知**调用方。
     */
    @Test
    void shouldNotifyTerminationWhenAutoRestoreIsDisabled() throws Exception {
        WatchManager noRestore = new WatchManager(threadPoolManager, false);
        try {
            CountDownLatch terminated = new CountDownLatch(1);
            AtomicReference<Throwable> cause = new AtomicReference<>();

            FakeStream stream = new FakeStream().event("only", 1).end();
            WatchManager.ActiveWatch watch = new WatchManager.ActiveWatch(
                    "watch-norestore",
                    rev -> stream,
                    event -> { },
                    e -> ((FakeEvent) e).revision(),
                    0,
                    t -> {
                        cause.set(t);
                        terminated.countDown();
                    }
            );

            noRestore.startWatch(watch);

            assertThat(terminated.await(10, TimeUnit.SECONDS))
                    .as("autoRestoreWatches=false 时流结束必须通知调用方，而不是静默死亡")
                    .isTrue();
            assertThat(cause.get()).isNotNull();
            assertThat(cause.get().getMessage()).contains("autoRestoreWatches=false");
            assertThat(noRestore.activeWatchCount())
                    .as("终态订阅必须从注册表移除（否则 map 无界增长）")
                    .isZero();
        } finally {
            noRestore.shutdown();
        }
    }

    /**
     * §3.14.3 第四个断言：取消必须**立即**唤醒阻塞中的消费线程。
     *
     * <p>这里的流永不产生事件（真实场景：订阅一个一直没有变更的 prefix，然后连接断开）。
     * 修复前 {@code cancel()} 只置 {@code active=false}，而循环阻塞在
     * {@code iterator.hasNext()} 上 —— 永远醒不过来。
     */
    @Test
    void cancelShouldWakeBlockingStreamImmediately() throws Exception {
        CountDownLatch connected = new CountDownLatch(1);
        BlockingStream stream = new BlockingStream();

        WatchManager.ActiveWatch watch = new WatchManager.ActiveWatch(
                "watch-blocking",
                rev -> {
                    connected.countDown();
                    return stream;
                },
                event -> { },
                e -> 0L,
                0
        );

        watchManager.startWatch(watch);
        assertThat(connected.await(5, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(200); // 消费线程此刻阻塞在 hasNext()

        long start = System.nanoTime();
        watch.cancel();
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(elapsedMs)
                .as("cancel() 必须立即生效，而不是等到下一条事件")
                .isLessThan(2_000);
        assertThat(stream.wasClosed()).isTrue();
    }

    @Test
    void shouldTrackLastRevision() {
        WatchManager.ActiveWatch watch = new WatchManager.ActiveWatch(
                "watch-3", rev -> new FakeStream(), e -> { }, e -> 0L, 42L
        );
        assertThat(watch.getLastRevision()).isEqualTo(42L);

        watch.setLastRevision(100L);
        assertThat(watch.getLastRevision()).isEqualTo(100L);
    }

    // ──── 测试夹具 ────

    /** 带 revision 的假事件。 */
    private record FakeEvent(String payload, long revision) {
    }

    /**
     * 脚本化的假流：按顺序交付事件，可显式 {@link #end()} / {@link #fail(Throwable)}。
     */
    private static final class FakeStream implements CancellableStream {
        private static final Object END = new Object();

        private final LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<>();
        private Object pending;
        private volatile boolean closed;
        private volatile Throwable failure;

        FakeStream event(String payload, long revision) {
            queue.add(new FakeEvent(payload, revision));
            return this;
        }

        /** 模拟服务端正常关闭流。 */
        FakeStream end() {
            queue.add(END);
            return this;
        }

        /** 模拟流因错误终止（可重连的故障）。 */
        FakeStream fail(Throwable cause) {
            this.failure = cause;
            queue.add(END);
            return this;
        }

        @Override
        public boolean hasNext() {
            if (pending != null) {
                return true;
            }
            try {
                Object item = queue.take();
                if (item == END) {
                    queue.add(END); // 维持终态
                    return false;
                }
                pending = item;
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        @Override
        public Object next() {
            Object out = pending;
            pending = null;
            if (out == null) {
                throw new java.util.NoSuchElementException("next() without hasNext()");
            }
            return out;
        }

        @Override
        public Throwable failure() {
            return failure;
        }

        @Override
        public void close() {
            closed = true;
            end();
        }
    }

    /** 永不产生事件、永不结束的流 —— 模拟"订阅了一个一直没变化的 prefix"。 */
    private static final class BlockingStream implements CancellableStream {
        private final CountDownLatch end = new CountDownLatch(1);
        private volatile boolean closed;

        @Override
        public boolean hasNext() {
            try {
                end.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return false;
        }

        @Override
        public Object next() {
            throw new java.util.NoSuchElementException("no events");
        }

        @Override
        public Throwable failure() {
            return null;
        }

        @Override
        public void close() {
            closed = true;
            end.countDown();
        }

        boolean wasClosed() {
            return closed;
        }
    }
}
