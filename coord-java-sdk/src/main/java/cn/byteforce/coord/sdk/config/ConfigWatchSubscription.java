package cn.byteforce.coord.sdk.config;

import java.io.Closeable;

/** A subscription to a configuration watch. Call {@link #close()} to cancel. */
public interface ConfigWatchSubscription extends Closeable {
    @Override
    void close();
}
