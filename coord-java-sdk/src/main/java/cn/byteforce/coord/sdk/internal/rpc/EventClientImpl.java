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

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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

    private final CoordConfig config;
    private final ExecutorService streamExecutor;

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
        if (listener == null) {
            throw new IllegalArgumentException("listener must not be null");
        }
        String filter = nullToEmpty(eventType);

        CancellableStream stream = new GrpcWatchStream(
                channelManager.getChannel(),
                EventGrpc.getSubscribeMethod(),
                EventSubscribeRequest.newBuilder().setEventType(filter).build());

        EventSubscriptionImpl sub = new EventSubscriptionImpl(deriveSubscriptionId(filter), stream, listener);
        sub.start(streamExecutor);
        return sub;
    }

    @Override
    public void unsubscribe(String subscriptionId) {
        // 契约声明了 Unsubscribe，服务端也接受它 —— 但它**不携带状态**：订阅就是那条
        // gRPC 流本身，服务端没有按 subscription_id 索引的注册表（grpc_handlers.rs 的
        // `unsubscribe` 直接返回空响应）。这里如实照做，而不是假装它取消了什么：
        // 真正取消订阅的方式是 EventSubscription.close()。契约侧的边界声明见
        // event.proto 的 Unsubscribe 注释与 WHITEPAPER §9.1.1。
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

    /** 取流循环 + 取消句柄。 */
    private static final class EventSubscriptionImpl implements EventSubscription {

        private final String subscriptionId;
        private final CancellableStream stream;
        private final EventListener listener;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        EventSubscriptionImpl(String subscriptionId, CancellableStream stream, EventListener listener) {
            this.subscriptionId = subscriptionId;
            this.stream = stream;
            this.listener = listener;
        }

        void start(ExecutorService executor) {
            executor.submit(() -> {
                try {
                    while (!closed.get() && stream.hasNext()) {
                        Object msg = stream.next();
                        if (msg instanceof CloudEventMessage ce) {
                            listener.onEvent(new CloudEvent(
                                    ce.getId(),
                                    ce.getSpecversion(),
                                    ce.getType(),
                                    ce.getSource(),
                                    ce.getData().toByteArray(),
                                    ce.getDataContentType(),
                                    ce.getSubject(),
                                    ce.getTime()));
                        }
                    }
                } catch (RuntimeException e) {
                    // 连接断了/被拒：这不是"订阅正常结束"，但对调用方而言两者都只是
                    // "不会再有事件了"。cancel() 之后到达的异常属预期，不再上抛。
                    if (!closed.get()) {
                        log.warn("Event subscription '{}' terminated abnormally: {}",
                                subscriptionId, e.toString());
                    }
                } finally {
                    stream.close();
                }
            });
        }

        @Override
        public String subscriptionId() {
            return subscriptionId;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                // 立即取消 RPC 并唤醒阻塞中的取流线程（不需要等下一跳事件）
                stream.close();
            }
        }
    }
}
