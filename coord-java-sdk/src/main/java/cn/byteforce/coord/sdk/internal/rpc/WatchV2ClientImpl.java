package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.contracts.watch.v2.WatchGrpc;
import cn.byteforce.coord.contracts.watch.v2.WatchOuterClass;
import cn.byteforce.coord.sdk.CoordException;
import cn.byteforce.coord.sdk.ErrorCode;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.sdk.kv.v2.KvClient;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import cn.byteforce.coord.sdk.watch.v2.WatchClient;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link WatchClient} backed by {@code coord.watch.v2} to the Coord Agent.
 * <p>
 * The agent owns the server-side {@code watchId}; this client keeps one RPC per
 * subscription and resumes a lost stream from the last cursor it delivered
 * (at-least-once). Acknowledgment-only frames advance the cursor but are not
 * surfaced. {@code HISTORY_UNAVAILABLE} ends the subscription instead of
 * reconnecting forever; any other retryable error backs off and reconnects,
 * bounded like the Rust client's retry budget.
 * <p>
 * Cancellation is implemented by cancelling the RPC: a closed stream drops the
 * server-side watch, so there is no separate cancel frame to race with.
 */
public final class WatchV2ClientImpl extends AgentRpcClient implements WatchClient {

    private static final Logger log = LoggerFactory.getLogger(WatchV2ClientImpl.class);

    private static final int MAX_RECONNECTS = 5;
    private static final long INITIAL_BACKOFF_MS = 100;
    private static final long MAX_BACKOFF_MS = 1600;
    private static final long CLOSE_WAIT_MS = 5000;
    /**
     * Frames buffered between the gRPC callback and the delivery loop. Overflow
     * is not a silent drop: the stream ends and the worker replays from the
     * cursor (the dropped frame's commit_ts is greater than that cursor).
     */
    private static final int QUEUE_CAPACITY = 256;

    private static final Object END = new Object();

    private final ExecutorService executor;

    public WatchV2ClientImpl(AgentChannelManager channelManager, ErrorMapper errorMapper,
                             RetryTemplate retryTemplate, ObservabilityProvider observability,
                             ExecutorService executor) {
        super(channelManager, errorMapper, retryTemplate, observability);
        this.executor = executor;
    }

    @Override
    public Subscription watch(Options options, Listener listener) {
        if (listener == null) {
            throw new CoordException(ErrorCode.INVALID_ARGUMENT, "listener must not be null");
        }
        if (options == null) {
            throw new CoordException(ErrorCode.INVALID_ARGUMENT, "options must not be null");
        }
        if (options.startRevision() < 0) {
            throw new CoordException(ErrorCode.INVALID_ARGUMENT, "startRevision must be >= 0");
        }
        if (options.startCursor() != null && options.startRevision() != 0) {
            throw new CoordException(ErrorCode.INVALID_ARGUMENT,
                    "startRevision and startCursor are mutually exclusive");
        }
        SubscriptionImpl subscription = new SubscriptionImpl(listener, options.startCursor());
        try {
            executor.execute(() -> run(options, subscription));
        } catch (java.util.concurrent.RejectedExecutionException e) {
            throw new CoordException(ErrorCode.AGENT_UNAVAILABLE, "client is closed", e);
        }
        return subscription;
    }

    private WatchOuterClass.WatchCreateRequest createRequest(Options options, Cursor resume) {
        WatchOuterClass.WatchCreateRequest.Builder create = WatchOuterClass.WatchCreateRequest
                .newBuilder()
                .setKey(KvProto.bytes(options.key()))
                .setRangeEnd(KvProto.bytes(options.rangeEnd()))
                .setPrevKv(options.prevKv())
                .setFilter(toProto(options.filter()));
        if (resume != null) {
            create.setStartCursor(WatchOuterClass.WatchCursor.newBuilder()
                    .setShardId(resume.shardId())
                    .setCommitTs(resume.commitTs())
                    .build());
        } else {
            create.setStartRevision(options.startRevision());
        }
        return create.build();
    }

    private static WatchOuterClass.EventFilter toProto(Filter filter) {
        return switch (filter == null ? Filter.ALL : filter) {
            case ALL -> WatchOuterClass.EventFilter.ALL;
            case PUTS_ONLY -> WatchOuterClass.EventFilter.PUTS_ONLY;
            case DELETES_ONLY -> WatchOuterClass.EventFilter.DELETES_ONLY;
        };
    }

    private void run(Options options, SubscriptionImpl sub) {
        sub.runner = Thread.currentThread();
        try {
            int reconnects = 0;
            long backoff = INITIAL_BACKOFF_MS;
            Cursor resume = options.startCursor();
            while (!sub.closed.get()) {
                if (channelManager.isShutdown()) {
                    sub.listener.onError(new CoordException(ErrorCode.AGENT_UNAVAILABLE,
                            "client is closed"));
                    return;
                }
                Attempt attempt = openAndConsume(options, sub, resume);
                resume = sub.cursor.get();
                if (sub.closed.get()) {
                    return;
                }
                if (attempt.delivered) {
                    reconnects = 0;
                    backoff = INITIAL_BACKOFF_MS;
                }
                if (attempt.terminal != null) {
                    sub.listener.onError(attempt.terminal);
                    return;
                }
                if (reconnects >= MAX_RECONNECTS) {
                    sub.listener.onError(new CoordException(ErrorCode.UNAVAILABLE,
                            "watch reconnect attempts exhausted"));
                    return;
                }
                reconnects++;
                if (!sleep(backoff)) {
                    return;
                }
                backoff = Math.min(backoff * 2, MAX_BACKOFF_MS);
            }
        } catch (CoordException e) {
            // 客户端已关闭（channel shutdown）不是订阅的协议失败：结束即可，
            // 但要在订阅仍活着时把原因交给调用方，不能静默吞掉。
            if (!sub.closed.get()) {
                sub.listener.onError(e);
            }
        } catch (RuntimeException e) {
            // listener 抛出的异常不能变成静默的订阅终止。
            log.warn("watch worker ended after an unexpected failure", e);
        } finally {
            sub.call = null;
        }
    }

    /** @return {@code false} when interrupted (the subscription must end) */
    private boolean sleep(long millis) {
        try {
            Thread.sleep(millis);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Open one stream and consume it until it ends. A normal stream completion
     * is retryable too: the server closing an open-ended watch is a transport
     * outcome, not a terminal protocol decision.
     */
    private Attempt openAndConsume(Options options, SubscriptionImpl sub, Cursor resume) {
        LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
        AtomicReference<CoordException> failure = new AtomicReference<>();
        boolean delivered = false;
        CoordException terminal = null;
        ManagedChannel channel = channelManager.getChannel();
        ClientCall<WatchOuterClass.WatchRequest, WatchOuterClass.WatchResponse> call =
                channel.newCall(WatchGrpc.getWatchMethod(), CallOptions.DEFAULT);
        sub.call = call;

        call.start(new ClientCall.Listener<>() {
            @Override
            public void onMessage(WatchOuterClass.WatchResponse message) {
                if (!queue.offer(message)) {
                    failure.compareAndSet(null, new CoordException(ErrorCode.RESOURCE_EXHAUSTED,
                            "watch consumer fell behind; reconnecting to replay"));
                    signalEnd(queue);
                    return;
                }
                call.request(1);
            }

            @Override
            public void onClose(Status status, Metadata trailers) {
                if (!status.isOk()) {
                    failure.compareAndSet(null, errorMapper.map(status.asRuntimeException(trailers)));
                }
                signalEnd(queue);
            }
        }, new Metadata());

        call.sendMessage(WatchOuterClass.WatchRequest.newBuilder()
                .setCreate(createRequest(options, resume))
                .build());
        call.request(1);

        try {
            while (true) {
                Object item = queue.take();
                if (item == END) {
                    break;
                }
                WatchOuterClass.WatchResponse response = (WatchOuterClass.WatchResponse) item;
                if (response.hasCursor()) {
                    WatchOuterClass.WatchCursor cursor = response.getCursor();
                    sub.cursor.set(new Cursor(cursor.getShardId(), cursor.getCommitTs()));
                }
                if (response.getEventsCount() == 0) {
                    // ack-only：只推进游标，不投给调用方。
                    continue;
                }
                List<Event> events;
                try {
                    events = eventsOf(response.getEventsList());
                } catch (CoordException e) {
                    terminal = e;
                    break;
                }
                Batch batch = new Batch(events, sub.cursor.get(), response.getWatermark(),
                        response.getSealed());
                if (!sub.closed.get()) {
                    sub.listener.onBatch(batch);
                    delivered = true;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            // 释放流：正常结束/队列溢出/主动关闭都走同一条路径。
            cancelQuietly(call, "watch stream ended");
        }

        if (terminal == null) {
            CoordException error = failure.get();
            if (error != null && !RetryTemplate.isRetryable(error.getErrorCode())) {
                terminal = error;
            }
        }
        return new Attempt(terminal, delivered);
    }

    /** Queue-full safe end signal: make room instead of blocking the gRPC callback. */
    private static void signalEnd(LinkedBlockingQueue<Object> queue) {
        while (!queue.offer(END)) {
            queue.poll();
        }
    }

    /**
     * Cancel is racy by nature: {@link #close()} may cancel the same call the
     * worker is finishing. A rejected cancel must not break the cleanup path.
     */
    private static void cancelQuietly(ClientCall<?, ?> call, String reason) {
        try {
            call.cancel(reason, null);
        } catch (RuntimeException e) {
            log.debug("watch stream cancel was refused: {}", e.toString());
        }
    }

    private static List<Event> eventsOf(List<WatchOuterClass.WatchEvent> protoEvents) {
        List<Event> events = new ArrayList<>(protoEvents.size());
        for (WatchOuterClass.WatchEvent event : protoEvents) {
            List<Change> changes = new ArrayList<>(event.getChangesCount());
            for (WatchOuterClass.Change change : event.getChangesList()) {
                byte[] value;
                switch (change.getType()) {
                    case PUT -> value = change.getValue().toByteArray();
                    case DELETE -> value = null;
                    case TYPE_UNSPECIFIED, UNRECOGNIZED -> throw new CoordException(
                            ErrorCode.INTERNAL,
                            "watch event carries an unspecified change type");
                    default -> value = change.getValue().toByteArray();
                }
                KvClient.KeyValue prev = change.hasPrevKv()
                        ? KvProto.toSdk(change.getPrevKv()) : null;
                changes.add(new Change(change.getKey().toByteArray(), value, prev));
            }
            events.add(new Event(event.getCommitTs(), List.copyOf(changes)));
        }
        return List.copyOf(events);
    }

    private record Attempt(CoordException terminal, boolean delivered) {}

    private final class SubscriptionImpl implements Subscription {

        private final Listener listener;
        private final AtomicReference<Cursor> cursor;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private volatile ClientCall<WatchOuterClass.WatchRequest, WatchOuterClass.WatchResponse> call;
        private volatile Thread runner;

        private SubscriptionImpl(Listener listener, Cursor startCursor) {
            this.listener = listener;
            this.cursor = new AtomicReference<>(startCursor);
        }

        @Override
        public Cursor cursor() {
            return cursor.get();
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            ClientCall<WatchOuterClass.WatchRequest, WatchOuterClass.WatchResponse> current = call;
            if (current != null) {
                cancelQuietly(current, "watch subscription closed");
            }
            Thread worker = runner;
            if (worker == null || worker == Thread.currentThread()) {
                return;
            }
            try {
                worker.join(CLOSE_WAIT_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
