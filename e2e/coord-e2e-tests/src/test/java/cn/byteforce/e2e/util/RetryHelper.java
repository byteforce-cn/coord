package cn.byteforce.e2e.util;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;

import java.time.Duration;
import java.util.concurrent.Callable;

public class RetryHelper {

    public static ConditionFactory await(int seconds) {
        return Awaitility.await().atMost(Duration.ofSeconds(seconds))
                .pollInterval(Duration.ofMillis(500));
    }

    public static <T> T poll(int timeoutSeconds, Callable<T> supplier) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L;
        Exception last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                return supplier.call();
            } catch (Exception e) {
                last = e;
                Thread.sleep(500);
            }
        }
        throw last != null ? last : new RuntimeException("Timeout after " + timeoutSeconds + "s");
    }

    public static void waitSeconds(int seconds) {
        try { Thread.sleep(seconds * 1000L); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
