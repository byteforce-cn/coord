package cn.byteforce.coord.sdk.internal.watch;

import cn.byteforce.coord.sdk.internal.thread.ThreadPoolManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Manages all active Watch subscriptions.
 * <p>
 * Each watch runs its blocking gRPC stream iterator on a virtual thread.
 * On connection recovery, watches are restored from their last known revision.
 */
public final class WatchManager {

    private static final Logger log = LoggerFactory.getLogger(WatchManager.class);

    private final ThreadPoolManager threadPoolManager;
    private final ConcurrentHashMap<String, ActiveWatch> watches = new ConcurrentHashMap<>();

    public WatchManager(ThreadPoolManager threadPoolManager) {
        this.threadPoolManager = threadPoolManager;
    }

    /**
     * Start a watch on a virtual thread. The watch iterates the stream and delivers
     * events to the handler. When the stream ends or fails, the watch becomes inactive.
     */
    public void startWatch(ActiveWatch watch) {
        watches.put(watch.watchId, watch);
        threadPoolManager.getVirtualThreadExecutor().execute(() -> runWatchLoop(watch));
    }

    private <T> void runWatchLoop(ActiveWatch watch) {
        watch.active.set(true);
        try {
            Iterator<?> iter = watch.streamFactory.get();
            while (watch.active.get() && iter.hasNext()) {
                Object event = iter.next();
                @SuppressWarnings("unchecked")
                Consumer<Object> handler = (Consumer<Object>) watch.handler;
                threadPoolManager.getVirtualThreadExecutor().execute(() -> {
                    try {
                        handler.accept(event);
                    } catch (Exception e) {
                        log.warn("Watch handler error for watchId={}", watch.watchId, e);
                    }
                });
            }
        } catch (Exception e) {
            log.info("Watch stream ended for watchId={}: {}", watch.watchId, e.getMessage());
        } finally {
            watch.active.set(false);
        }
    }

    /**
     * Cancel and remove a specific watch.
     */
    public void cancelWatch(String watchId) {
        ActiveWatch watch = watches.remove(watchId);
        if (watch != null) {
            watch.cancel();
        }
    }

    /**
     * Shutdown all watches and clear the registry.
     */
    public void shutdown() {
        for (ActiveWatch watch : watches.values()) {
            watch.cancel();
        }
        watches.clear();
    }

    /**
     * Represents an active watch subscription.
     */
    public static class ActiveWatch {
        final String watchId;
        final Supplier<Iterator<?>> streamFactory;
        final Consumer<?> handler;
        final AtomicLong lastRevision;
        final AtomicBoolean active = new AtomicBoolean(false);

        public <T> ActiveWatch(String watchId, Supplier<Iterator<T>> streamFactory,
                                Consumer<T> handler, long startRevision) {
            this.watchId = watchId;
            this.streamFactory = () -> (Iterator<?>) streamFactory.get();
            this.handler = handler;
            this.lastRevision = new AtomicLong(startRevision);
        }

        public boolean isActive() {
            return active.get();
        }

        public void cancel() {
            active.set(false);
        }

        public long getLastRevision() {
            return lastRevision.get();
        }

        public void setLastRevision(long revision) {
            lastRevision.set(revision);
        }

        public String getWatchId() {
            return watchId;
        }
    }
}
