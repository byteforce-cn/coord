package cn.byteforce.coord.sdk.internal.watch;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NoSuchElementException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link CancellableStream} 的标准实现：手工建立 {@code ClientCall}，把服务端消息推入
 * 有界队列，由订阅循环阻塞取用。
 *
 * <p>与"直接迭代阻塞式 stub"的区别（见 {@link CancellableStream} 的说明）：
 * <ul>
 *   <li><b>取消立即生效</b>：{@link #close()} 调 {@code ClientCall.cancel()} 并投毒丸，
 *       阻塞中的 {@code take()} 立刻返回 —— 不需要等下一次事件；</li>
 *   <li><b>结束原因可区分</b>：{@code onClose(status, trailers)} 保存异常；
 *       主动取消保持 {@code null}；</li>
 *   <li><b>流量控制</b>：每条消息处理完再 {@code request(1)}。</li>
 * </ul>
 *
 * <p><b>队列满 = 流致命错误，而不是静默丢消息。</b> 容量 {@value #QUEUE_CAPACITY}。
 * 若消费者跟不上，静默丢弃事件会让业务永久缺失一次变更（而 watch 的价值恰恰是"不漏"）。
 * 因此溢出时把流判定为失败并结束，由 {@link WatchManager} 从
 * {@code lastRevision + 1} 重连 —— 补回历史（服务端保留 Changelog），
 * 这与 proto 里 {@code BUFFER_OVERFLOW} ⇒ "客户端全量同步"的约定同向。
 */
public final class GrpcWatchStream implements CancellableStream {

    private static final Logger log = LoggerFactory.getLogger(GrpcWatchStream.class);

    /** 队列容量（消息 + 至多一个哨兵）。 */
    private static final int QUEUE_CAPACITY = 1024;

    /** 结束哨兵：用于区分"队列空"与"流已结束"。 */
    private static final Object END = new Object();

    private final LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final ClientCall<Object, Object> call;

    /** 流结束原因：{@code null} = 正常结束 / 主动取消 / 尚未结束。 */
    private volatile Throwable failure;

    /** 队列溢出次数（用于日志与观测；每次溢出都会导致一次重连补历史）。 */
    private final AtomicLong overflows = new AtomicLong();

    /** {@code hasNext()} 预取到的消息。 */
    private Object pending;
    private boolean pendingValid;

    public GrpcWatchStream(ManagedChannel channel,
                           MethodDescriptor<?, ?> method,
                           Object request) {
        @SuppressWarnings("unchecked")
        MethodDescriptor<Object, Object> md = (MethodDescriptor<Object, Object>) method;
        ClientCall<Object, Object> call = channel.newCall(md, CallOptions.DEFAULT);
        this.call = call;

        call.start(new ClientCall.Listener<>() {
            @Override
            public void onMessage(Object message) {
                if (!queue.offer(message)) {
                    // 消费者跟不上：有界队列拒绝。判定为流失败并结束，
                    // 让上层从 lastRevision+1 重连补历史（见类注释）。
                    overflows.incrementAndGet();
                    if (failure == null) {
                        failure = new IllegalStateException(
                                "watch consumer fell behind: queue overflow ("
                                        + QUEUE_CAPACITY + " pending); reconnecting to replay");
                    }
                    log.warn("watch stream queue overflow ({} pending) — ending the stream so the "
                            + "manager can replay from the last revision", QUEUE_CAPACITY);
                    signalEnd();
                    return;
                }
                call.request(1);
            }

            @Override
            public void onClose(Status status, Metadata trailers) {
                if (!status.isOk() && !closed.get() && failure == null) {
                    failure = status.asRuntimeException(trailers);
                }
                signalEnd();
            }
        }, new Metadata());

        call.sendMessage(request);
        call.halfClose();
        call.request(1);
    }

    /**
     * 保证结束哨兵一定能进入队列（队列满时先腾位）。
     *
     * <p>不能用裸 {@code offer}：队列恰好满时 offer 失败，阻塞在 {@code take()} 的
     * 消费者就永远醒不过来——那正是本轮要修掉的"取消无效"形态。
     */
    private void signalEnd() {
        // 有界循环：每次失败先丢弃一条队首消息腾位，最终必然成功。
        while (!queue.offer(END)) {
            queue.poll();
        }
    }

    @Override
    public boolean hasNext() {
        if (pendingValid) {
            return true;
        }
        try {
            Object item = queue.take();
            if (item == END) {
                pendingValid = false;
                return false;
            }
            pending = item;
            pendingValid = true;
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @Override
    public Object next() {
        if (!pendingValid) {
            throw new NoSuchElementException("next() called without a preceding hasNext() == true");
        }
        Object out = pending;
        pending = null;
        pendingValid = false;
        return out;
    }

    @Override
    public Throwable failure() {
        return failure;
    }

    /** 队列溢出次数（每次溢出都会触发一次重连补历史）。 */
    public long overflowCount() {
        return overflows.get();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            call.cancel("watch cancelled by client", null);
        } catch (RuntimeException e) {
            log.debug("cancel() on an already-finished call: {}", e.toString());
        }
        // 唤醒阻塞中的 hasNext()。failure 保持 null —— 主动取消不是故障。
        signalEnd();
    }
}
