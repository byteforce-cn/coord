package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.CoordException;
import cn.byteforce.coord.sdk.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

/**
 * Retry template for RPC calls.
 * <p>
 * Retry policy:
 * <ul>
 *   <li>Max 3 attempts total (1 initial + 2 retries).</li>
 *   <li>Only retries on {@link ErrorCode#AGENT_UNAVAILABLE} and {@link ErrorCode#RESOURCE_EXHAUSTED}.</li>
 *   <li>Backoff: 100ms, 200ms, 500ms for retry attempts 2, 3.</li>
 * </ul>
 */
public final class RetryTemplate {

    private static final Logger log = LoggerFactory.getLogger(RetryTemplate.class);
    private static final int MAX_ATTEMPTS = 3;
    private static final long[] BACKOFF_MS = {0, 100, 200, 500};

    /**
     * Execute with retry logic.
     *
     * @param call the RPC call function that takes a retry context and returns a result
     * @param <T>  the result type
     * @return the result
     * @throws CoordException if all retries are exhausted
     */
    public <T> T execute(Function<RetryContext, T> call) throws CoordException {
        CoordException lastException = null;

        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            RetryContext ctx = new RetryContext(attempt);
            try {
                T result = call.apply(ctx);
                if (attempt > 1) {
                    log.debug("RPC succeeded on attempt {}", attempt);
                }
                return result;
            } catch (CoordException e) {
                lastException = e;
                if (!isRetryable(e.getErrorCode())) {
                    throw e;
                }
                if (attempt < MAX_ATTEMPTS) {
                    long delay = BACKOFF_MS[attempt];
                    log.debug("RPC attempt {} failed with {}, retrying in {}ms",
                            attempt, e.getErrorCode(), delay);
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new CoordException(ErrorCode.INTERNAL, "Retry interrupted", ie);
                    }
                }
            } catch (RuntimeException e) {
                // Non-Coord exceptions are NOT retried
                throw e;
            }
        }

        throw lastException;
    }

    private boolean isRetryable(ErrorCode code) {
        return code == ErrorCode.AGENT_UNAVAILABLE || code == ErrorCode.RESOURCE_EXHAUSTED;
    }

    /**
     * Context passed to each retry attempt.
     */
    public static final class RetryContext {
        private final int attemptNumber;

        RetryContext(int attemptNumber) {
            this.attemptNumber = attemptNumber;
        }

        /** 1-based attempt number. */
        public int getAttemptNumber() {
            return attemptNumber;
        }
    }
}
