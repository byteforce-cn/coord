package cn.byteforce.coord.sdk.registry;

import java.io.Closeable;

/** A subscription to a registry watch. Call {@link #close()} to cancel. */
public interface WatchSubscription extends Closeable {
    /** Cancel this watch subscription. */
    @Override
    void close();
}
