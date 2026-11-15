package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.contracts.event.v1.CloudEventMessage;
import cn.byteforce.coord.sdk.event.EventClient.CloudEvent;
import cn.byteforce.coord.sdk.event.EventClient.EventListener;
import cn.byteforce.coord.sdk.internal.watch.CancellableStream;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongFunction;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 事件订阅取流循环的回归（#325）。
 *
 * <p><b>缺陷形态</b>：循环只迭代一条流；stream 结束（coord 重启、agent 断连、服务端
 * 关闭）后循环返回，订阅**永久结束**——不重连、不续订、也不通知调用方。本类逐条钉住：
 * <ol>
 *   <li>断流必须重连，且从**最后一条已交付事件的 seq** 续订；</li>
 *   <li>尚未交付任何事件时，重连位点用调用方给的 cursor（进程重启后的续读位点）；</li>
 *   <li>连续失败超过预算 ⇒ 终态必须报告（{@link EventListener#onError}），不能静默；</li>
 *   <li>{@code close()} 后不得再重连（否则 close 之前的那次取消等于没做）；</li>
 *   <li>业务回调抛异常不得杀死订阅（一条坏事件 ≠ 订阅死亡）。</li>
 * </ol>
 */
class EventStreamReconnectTest {

    private static final long FAST_BACKOFF_MS = 1L;
    private static final int FAST_MAX_FAILURES = 3;

    private static EventClientImpl.EventSubscriptionImpl newSubscription(
            LongFunction<CancellableStream> factory,
            long callerCursor,
            EventListener listener) {
        return new EventClientImpl.EventSubscriptionImpl(
                "evt-test", factory, callerCursor, listener,
                FAST_BACKOFF_MS, FAST_BACKOFF_MS, FAST_MAX_FAILURES, sub -> {
        });
    }

    private static CloudEventMessage message(long seq) {
        return CloudEventMessage.newBuilder()
                .setId("id-" + seq)
                .setSpecversion("1.0")
                .setType("test.event")
                .setSource("test")
                .setData(ByteString.copyFrom(("payload-" + seq).getBytes()))
                .setSeq(seq)
                .build();
    }

    @Test
    @DisplayName("断流后从最后一条已交付事件的 seq 续订")
    void shouldResumeFromLastDeliveredSeq() throws Exception {
        List<Long> cursors = new CopyOnWriteArrayList<>();
        List<Long> received = new CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<EventClientImpl.EventSubscriptionImpl> subRef = new AtomicReference<>();

        LongFunction<CancellableStream> factory = cursor -> {
            cursors.add(cursor);
            // 第一条流交付 1、2 后服务端关闭；第二条流交付 3 后由回调关闭订阅。
            return cursors.size() == 1
                    ? new FakeStream(List.of(message(1), message(2)), null)
                    : new FakeStream(List.of(message(3)), null);
        };
        EventListener listener = new EventListener() {
            @Override
            public void onEvent(CloudEvent event) {
                received.add(event.getSeq());
                if (event.getSeq() == 3) {
                    subRef.get().close();
                    done.countDown();
                }
            }
        };
        EventClientImpl.EventSubscriptionImpl sub = newSubscription(factory, -1L, listener);
        subRef.set(sub);

        Thread thread = new Thread(sub::run, "event-loop-test");
        thread.start();
        assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
        thread.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(received).containsExactly(1L, 2L, 3L);
        // 首次不带 cursor（-1），重连带最后一条已交付的 seq（2）——服务端语义是
        // "重放 seq > cursor"，所以传 2 恰好从 3 继续。
        assertThat(cursors).containsExactly(-1L, 2L);
        assertThat(thread.isAlive()).isFalse();
    }

    @Test
    @DisplayName("尚未交付事件时，重连沿用调用方 cursor")
    void shouldKeepCallerCursorWhenNothingDelivered() throws Exception {
        List<Long> cursors = new CopyOnWriteArrayList<>();
        List<Long> received = new CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<EventClientImpl.EventSubscriptionImpl> subRef = new AtomicReference<>();

        LongFunction<CancellableStream> factory = cursor -> {
            cursors.add(cursor);
            return cursors.size() == 1
                    ? new FakeStream(List.of(), null)          // 一上来就断
                    : new FakeStream(List.of(message(43)), null);
        };
        EventListener listener = new EventListener() {
            @Override
            public void onEvent(CloudEvent event) {
                received.add(event.getSeq());
                subRef.get().close();
                done.countDown();
            }
        };
        EventClientImpl.EventSubscriptionImpl sub = newSubscription(factory, 42L, listener);
        subRef.set(sub);

        Thread thread = new Thread(sub::run, "event-loop-test");
        thread.start();
        assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
        thread.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(cursors).containsExactly(42L, 42L);
        assertThat(received).containsExactly(43L);
    }

    @Test
    @DisplayName("连续失败耗尽预算 ⇒ 调用 onError 报告终态")
    void shouldReportTerminalErrorAfterFailureBudget() throws Exception {
        AtomicInteger connects = new AtomicInteger();
        AtomicReference<Throwable> reported = new AtomicReference<>();
        LongFunction<CancellableStream> factory = cursor -> {
            connects.incrementAndGet();
            return new FakeStream(List.of(), new IllegalStateException("connection refused"));
        };
        EventListener listener = new EventListener() {
            @Override
            public void onEvent(CloudEvent event) {
            }

            @Override
            public void onError(Throwable error) {
                reported.set(error);
            }
        };
        EventClientImpl.EventSubscriptionImpl sub = newSubscription(factory, -1L, listener);

        Thread thread = new Thread(sub::run, "event-loop-test");
        thread.start();
        thread.join(TimeUnit.SECONDS.toMillis(10));

        assertThat(thread.isAlive()).isFalse();
        assertThat(connects.get()).isEqualTo(FAST_MAX_FAILURES);
        assertThat(reported.get())
                .as("放弃时必须报告，否则订阅又是一次静默死亡")
                .isNotNull()
                .hasMessageContaining("gave up");
    }

    @Test
    @DisplayName("close() 后不再重连，并立即唤醒阻塞中的取流线程")
    void shouldNotReconnectAfterClose() throws Exception {
        AtomicInteger connects = new AtomicInteger();
        AtomicReference<FakeStream> blocking = new AtomicReference<>();
        LongFunction<CancellableStream> factory = cursor -> {
            connects.incrementAndGet();
            FakeStream stream = new FakeStream(List.of(), null).blockWhenEmpty();
            blocking.set(stream);
            return stream;
        };
        EventClientImpl.EventSubscriptionImpl sub = newSubscription(factory, -1L, event -> {
        });

        Thread thread = new Thread(sub::run, "event-loop-test");
        thread.start();
        // 等到第一轮真的在取流（阻塞在 hasNext 上）
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (blocking.get() == null && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(blocking.get()).isNotNull();

        sub.close();
        thread.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(thread.isAlive()).isFalse();
        assertThat(connects.get()).as("close 之后不得再建立新流").isEqualTo(1);
    }

    @Test
    @DisplayName("业务回调抛异常不杀死订阅")
    void shouldSurviveListenerException() throws Exception {
        AtomicInteger connects = new AtomicInteger();
        List<Long> received = new CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<EventClientImpl.EventSubscriptionImpl> subRef = new AtomicReference<>();
        LongFunction<CancellableStream> factory = cursor -> {
            connects.incrementAndGet();
            return new FakeStream(List.of(message(1), message(2)), null);
        };
        EventListener listener = new EventListener() {
            @Override
            public void onEvent(CloudEvent event) {
                if (event.getSeq() == 1) {
                    throw new IllegalStateException("business handler blew up");
                }
                received.add(event.getSeq());
                subRef.get().close();
                done.countDown();
            }
        };
        EventClientImpl.EventSubscriptionImpl sub = newSubscription(factory, -1L, listener);
        subRef.set(sub);

        Thread thread = new Thread(sub::run, "event-loop-test");
        thread.start();
        assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
        thread.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(received).containsExactly(2L);
        assertThat(connects.get()).as("回调异常不得触发重连（订阅本身是健康的）").isEqualTo(1);
    }

    /**
     * 可注入结束形态的假流：事件按序交付，队列耗尽即"流结束"。
     *
     * <p>{@link #blockWhenEmpty()} 模拟健康但安静的服务端：没有事件时 {@code hasNext()}
     * 阻塞，直到 {@link #close()} 唤醒——用来验证取消真的立即生效。
     */
    private static final class FakeStream implements CancellableStream {
        private static final Object END = new Object();

        private final LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<>();
        private final Throwable failure;
        private final boolean block;
        private volatile boolean closed;

        FakeStream(List<CloudEventMessage> messages, Throwable failure) {
            this.queue.addAll(messages);
            this.failure = failure;
            this.block = false;
            this.queue.add(END);
        }

        private FakeStream(FakeStream base) {
            this.queue.addAll(new ArrayList<>(base.queue));
            this.queue.remove(END);
            this.failure = base.failure;
            this.block = true;
        }

        FakeStream blockWhenEmpty() {
            return new FakeStream(this);
        }

        @Override
        public boolean hasNext() {
            try {
                Object item = queue.take();
                if (item == END) {
                    return false;
                }
                pending = item;
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        private Object pending;

        @Override
        public Object next() {
            Object out = pending;
            pending = null;
            return out;
        }

        @Override
        public Throwable failure() {
            return closed ? null : failure;
        }

        @Override
        public void close() {
            closed = true;
            queue.offer(END);
        }
    }
}
