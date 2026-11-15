package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.contracts.event.v1.CloudEventMessage;
import cn.byteforce.coord.contracts.event.v1.EventGrpc;
import cn.byteforce.coord.contracts.event.v1.EventPublishRequest;
import cn.byteforce.coord.contracts.event.v1.EventPublishResponse;
import cn.byteforce.coord.contracts.event.v1.EventSubscribeRequest;
import cn.byteforce.coord.contracts.event.v1.EventUnsubscribeRequest;
import cn.byteforce.coord.contracts.event.v1.EventUnsubscribeResponse;
import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.event.EventClient;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.sdk.internal.watch.CancellableStream;
import cn.byteforce.coord.sdk.internal.watch.GrpcWatchStream;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongFunction;

/**
 * Implementation of {@link EventClient} backed by gRPC to the Coord Agent.
 *
 * <p><b>Two implementation choices worth stating</b> (both are the same choices the
 * existing Watch/Config path already made — see {@code CancellableStream}):
 *
 * <ol>
 *   <li><b>订阅用可取消的 {@link CancellableStream}，不用阻塞式 stub iterator。</b>
 *       阻塞式 iterator 的 {@code hasNext()} 会挂在网络上，于是 {@code close()} 必须
 *       等到**下一条事件到达**才生效 —— 对一个已经安静下来的 topic，取消等于没做。
 *       {@link GrpcWatchStream#close()} 真取消 {@code ClientCall} 并唤醒消费者。</li>
 *   <li><b>回调跑在受管理的虚拟线程执行器上</b>（`ThreadPoolManager`），这样
 *       {@code CoordClient.close()} 才能中断它。</li>
 * </ol>
 */
public final class EventClientImpl extends AgentRpcClient implements EventClient {

    private static final Logger log = LoggerFactory.getLogger(EventClientImpl.class);

    /**
     * 重连退避与放弃阈值（与 {@code WatchManager} 同口径：200ms 起、30s 上限、连续 20 次放弃）。
     * 不设上限意味着"永远重试"——那同样是静默：订阅看起来活着，实际永远收不到事件。
     */
    static final long INITIAL_RETRY_BACKOFF_MS = 200L;
    static final long MAX_RETRY_BACKOFF_MS = 30_000L;
    static final int MAX_CONSECUTIVE_FAILURES = 20;

    private final CoordConfig config;
    private final ExecutorService streamExecutor;
    /** 活跃订阅（`CoordClient.close()` 时逐条关闭，避免 shutdown 后重连风暴）。 */
    private final Set<EventSubscriptionImpl> subscriptions = ConcurrentHashMap.newKeySet();

    public EventClientImpl(AgentChannelManager channelManager, ErrorMapper errorMapper,
                           RetryTemplate retryTemplate, ObservabilityProvider observability,
                           CoordConfig config, ExecutorService streamExecutor) {
        super(channelManager, errorMapper, retryTemplate, observability);
        this.config = config;
        this.streamExecutor = streamExecutor;
    }

    @Override
    public String publish(String eventType, String source, byte[] data,
                          String dataContentType, String subject) {
        EventPublishRequest request = EventPublishRequest.newBuilder()
                .setEventType(nullToEmpty(eventType))
                .setSource(nullToEmpty(source))
                .setData(ByteString.copyFrom(data == null ? new byte[0] : data))
                .setDataContentType(nullToEmpty(dataContentType))
                .setSubject(nullToEmpty(subject))
                .build();

        EventPublishResponse response = callWithRetry(
                (ch, req) -> EventGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .publish((EventPublishRequest) req),
                request, "event.publish");

        String eventId = response.getEventId();
        log.debug("Event published: type={} source={} id={}", eventType, source, eventId);
        return eventId;
    }

    @Override
    public EventSubscription subscribe(String eventType, EventListener listener) {
        return subscribe(eventType, -1L, listener);
    }

    @Override
    public EventSubscription subscribe(String eventType, long cursor, EventListener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("listener must not be null");
        }
        String filter = nullToEmpty(eventType);

        // 每次（重）连都现取 channel：agent 重启后 AgentChannelManager 交给我们的仍是
        // 当前可用通道，而不是订阅建立那一刻的快照。
        LongFunction<CancellableStream> streamFactory = resumeCursor -> {
            EventSubscribeRequest.Builder req = EventSubscribeRequest.newBuilder()
                    .setEventType(filter);
            if (resumeCursor >= 0) {
                // 位点是十进制 seq 字符串（uint64 全范围）；-1 = 不携带 cursor（默认实时）。
                req.setCursor(Long.toUnsignedString(resumeCursor));
            }
            return new GrpcWatchStream(
                    channelManager.getChannel(),
                    EventGrpc.getSubscribeMethod(),
                    req.build());
        };

        EventSubscriptionImpl sub = new EventSubscriptionImpl(
                deriveSubscriptionId(filter), streamFactory, cursor, listener,
                INITIAL_RETRY_BACKOFF_MS, MAX_RETRY_BACKOFF_MS, MAX_CONSECUTIVE_FAILURES,
                this::forget);
        subscriptions.add(sub);
        sub.start(streamExecutor);
        return sub;
    }

    /** 关闭全部活跃订阅（{@code CoordClient.close()} 调用）；返回取消条数。 */
    public int closeAllSubscriptions() {
        int cancelled = 0;
        for (EventSubscriptionImpl sub : subscriptions) {
            sub.close();
            cancelled++;
        }
        subscriptions.clear();
        return cancelled;
    }

    private void forget(EventSubscriptionImpl sub) {
        subscriptions.remove(sub);
    }

    @Override
    public void unsubscribe(String subscriptionId) {
        // 契约声明了 Unsubscribe，服务端也接受它 —— 但它**不携带状态**：订阅就是那条
        // gRPC 流本身，服务端没有按 subscription_id 索引的注册表（grpc_handlers.rs 的
        // `unsubscribe` 直接返回空响应）。这里如实照做，而不是假装它取消了什么：
        // 真正取消订阅的方式是 EventSubscription.close()。契约侧的边界声明见
        // event.proto 的 Unsubscribe 注释与 WHITEPAPER.md 的边界声明。
        EventUnsubscribeRequest request = EventUnsubscribeRequest.newBuilder()
                .setSubscriptionId(nullToEmpty(subscriptionId))
                .build();
        EventUnsubscribeResponse ignored = callWithRetry(
                (ch, req) -> EventGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .unsubscribe((EventUnsubscribeRequest) req),
                request, "event.unsubscribe");
        log.debug("Event unsubscribe acknowledged (state-free): {} (ignored={})",
                subscriptionId, ignored != null);
    }

    /**
     * 当前 wire 没有服务端分配的 subscription_id（`Subscribe` 是流，不回传 id），
     * 因此本地**确定性**派生一个：同一 event_type 得同一个 id。确定性是有意的 ——
     * 随机 id 会让调用方以为它指向服务端的某个订阅，而服务端根本没有这张表。
     */
    private static String deriveSubscriptionId(String eventType) {
        return "evt-" + (eventType.isEmpty() ? "*" : eventType) + "-" + UUID.nameUUIDFromBytes(
                eventType.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }

    /**
     * 一条订阅的取流循环 + 重连/报告（#325）。
     *
     * <p><b>缺陷形态</b>：循环只迭代**一条**流，正常结束或异常都只打一条日志就收尾。
     * 于是 coord（agent）重启、连接抖动、服务端关闭流之后，订阅**永久结束**：不重连、
     * 不续订、也不通知调用方——调用方看到的是一个"活着"的订阅对象和一条再也不来的
     * 事件流。这与 {@code WatchManager} 修掉的是同一类"静默死亡"。
     *
     * <p><b>现在</b>：
     * <ul>
     *   <li><b>断流即重连</b>（指数退避 200ms→30s），并从**最后一条已交付事件的 seq**
     *       续订（服务端的 {@code cursor} 语义是"已处理到的位点"，重放 {@code seq > cursor}）
     *       ——断线窗口不再丢事件，重放/实时的交接可能重复，消费方按既有契约保持幂等；</li>
     *   <li><b>连续失败有上限</b>（20 次）并**通知调用方**（{@link EventListener#onError}）：
     *       放弃时必须说出来，这才是"报告"；</li>
     *   <li><b>取消立即生效</b>：{@link #close()} 真取消底层 {@code ClientCall}
     *       并唤醒阻塞中的取流线程，不再等到下一条事件。</li>
     * </ul>
     */
    static final class EventSubscriptionImpl implements EventSubscription {

        private final String subscriptionId;
        private final LongFunction<CancellableStream> streamFactory;
        private final long callerCursor;
        private final EventListener listener;
        private final long initialBackoffMs;
        private final long maxBackoffMs;
        private final int maxConsecutiveFailures;
        private final java.util.function.Consumer<EventSubscriptionImpl> onTerminated;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        /** 最后一条已交付事件的 seq；-1 = 尚未交付（服务端 seq 从 0 起，永不为负）。 */
        private final AtomicLong lastSeq = new AtomicLong(-1L);
        private final AtomicReference<CancellableStream> currentStream = new AtomicReference<>();

        EventSubscriptionImpl(String subscriptionId,
                              LongFunction<CancellableStream> streamFactory,
                              long callerCursor,
                              EventListener listener,
                              long initialBackoffMs,
                              long maxBackoffMs,
                              int maxConsecutiveFailures,
                              java.util.function.Consumer<EventSubscriptionImpl> onTerminated) {
            this.subscriptionId = subscriptionId;
            this.streamFactory = streamFactory;
            this.callerCursor = callerCursor;
            this.listener = listener;
            this.initialBackoffMs = initialBackoffMs;
            this.maxBackoffMs = maxBackoffMs;
            this.maxConsecutiveFailures = maxConsecutiveFailures;
            this.onTerminated = onTerminated;
        }

        /** 重连位点：优先"最后一条已交付"，否则用调用方给的位点（-1 = 实时）。 */
        long resumeCursor() {
            long seen = lastSeq.get();
            return seen >= 0 ? seen : callerCursor;
        }

        void start(ExecutorService executor) {
            executor.submit(this::run);
        }

        void run() {
            long backoffMs = initialBackoffMs;
            int consecutiveFailures = 0;
            try {
                while (!closed.get() && !Thread.currentThread().isInterrupted()) {
                    CancellableStream stream = null;
                    boolean delivered = false;
                    try {
                        stream = streamFactory.apply(resumeCursor());
                        currentStream.set(stream);
                        while (!closed.get() && stream.hasNext()) {
                            Object msg = stream.next();
                            if (!(msg instanceof CloudEventMessage ce)) {
                                continue;
                            }
                            // 水位必须在派发**之前**推进：回调抛异常时重连不能退回旧位点
                            // （否则同一条事件会无限重投）。消费方按契约保持幂等。
                            long seq = ce.getSeq();
                            if (seq > 0) {
                                lastSeq.set(seq);
                            }
                            delivered = true;
                            try {
                                listener.onEvent(toCloudEvent(ce));
                            } catch (RuntimeException e) {
                                // 一条坏事件不能杀死整条订阅（与 Watch 派发同口径）。
                                log.warn("Event listener for '{}' threw on seq={}: {}",
                                        subscriptionId, seq, e.toString());
                            }
                        }
                        if (closed.get() || Thread.currentThread().isInterrupted()) {
                            return;
                        }
                        Throwable failure = stream.failure();
                        consecutiveFailures++;
                        log.warn("Event subscription '{}' stream ended (attempt {}): {}; "
                                        + "reconnecting from seq {}",
                                subscriptionId, consecutiveFailures,
                                failure == null ? "server closed the stream" : failure.toString(),
                                resumeCursor());
                    } catch (RuntimeException e) {
                        if (closed.get() || Thread.currentThread().isInterrupted()) {
                            return;
                        }
                        consecutiveFailures++;
                        log.warn("Event subscription '{}' loop error (attempt {}): {}",
                                subscriptionId, consecutiveFailures, e.toString());
                    } finally {
                        currentStream.compareAndSet(stream, null);
                        if (stream != null) {
                            stream.close();
                        }
                    }

                    // 连接健康过一次就重置预算：长期订阅偶发一次抖动不该攒到放弃。
                    if (delivered) {
                        consecutiveFailures = 0;
                        backoffMs = initialBackoffMs;
                    }
                    if (consecutiveFailures >= maxConsecutiveFailures) {
                        Throwable terminal = new IllegalStateException(
                                "event subscription '" + subscriptionId + "' gave up after "
                                        + consecutiveFailures + " consecutive failures (last seq "
                                        + lastSeq.get() + "); events are NO LONGER being delivered");
                        log.error(terminal.getMessage());
                        notify(listener, terminal);
                        return;
                    }
                    if (!sleepBackoff(backoffMs)) {
                        return;
                    }
                    backoffMs = Math.min(backoffMs * 2, maxBackoffMs);
                }
            } finally {
                CancellableStream stream = currentStream.getAndSet(null);
                if (stream != null) {
                    stream.close();
                }
                if (onTerminated != null) {
                    onTerminated.accept(this);
                }
            }
        }

        /** 终态报告；回调自身抛异常不得把清理路径带崩。 */
        private void notify(EventListener listener, Throwable terminal) {
            try {
                listener.onError(terminal);
            } catch (RuntimeException e) {
                log.warn("Event listener onError for '{}' threw: {}", subscriptionId, e.toString());
            }
        }

        /** @return false 表示被中断（应退出循环）。 */
        private static boolean sleepBackoff(long ms) {
            try {
                Thread.sleep(ms);
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        private static CloudEvent toCloudEvent(CloudEventMessage ce) {
            return new CloudEvent(
                    ce.getId(),
                    ce.getSpecversion(),
                    ce.getType(),
                    ce.getSource(),
                    ce.getData().toByteArray(),
                    ce.getDataContentType(),
                    ce.getSubject(),
                    ce.getTime(),
                    ce.getSeq());
        }

        @Override
        public String subscriptionId() {
            return subscriptionId;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                // 立即取消 RPC 并唤醒阻塞中的取流线程（不需要等下一跳事件）
                CancellableStream stream = currentStream.get();
                if (stream != null) {
                    stream.close();
                }
            }
        }
    }
}
