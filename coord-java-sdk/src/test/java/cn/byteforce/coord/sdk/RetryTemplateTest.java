package cn.byteforce.coord.sdk;

import cn.byteforce.coord.sdk.internal.rpc.RetryTemplate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RetryTemplateTest {

    private RetryTemplate retryTemplate;

    @BeforeEach
    void setUp() {
        retryTemplate = new RetryTemplate();
    }

    @Test
    void shouldSucceedOnFirstAttempt() throws Exception {
        AtomicInteger calls = new AtomicInteger(0);
        String result = retryTemplate.execute(ctx -> {
            calls.incrementAndGet();
            return "ok";
        });
        assertThat(result).isEqualTo("ok");
        assertThat(calls.get()).isEqualTo(1);
    }

    @Test
    void shouldRetryOnAgentUnavailable() throws Exception {
        AtomicInteger calls = new AtomicInteger(0);
        String result = retryTemplate.execute(ctx -> {
            int attempt = calls.incrementAndGet();
            if (attempt < 2) {
                throw new CoordException(ErrorCode.AGENT_UNAVAILABLE);
            }
            return "ok";
        });
        assertThat(result).isEqualTo("ok");
        assertThat(calls.get()).isEqualTo(2);
    }

    @Test
    void shouldRetryOnResourceExhausted() throws Exception {
        AtomicInteger calls = new AtomicInteger(0);
        String result = retryTemplate.execute(ctx -> {
            int attempt = calls.incrementAndGet();
            if (attempt < 3) {
                throw new CoordException(ErrorCode.RESOURCE_EXHAUSTED);
            }
            return "ok";
        });
        assertThat(result).isEqualTo("ok");
        assertThat(calls.get()).isEqualTo(3);
    }

    @Test
    void shouldNotRetryOnOtherErrors() {
        AtomicInteger calls = new AtomicInteger(0);
        assertThatThrownBy(() -> retryTemplate.execute(ctx -> {
            calls.incrementAndGet();
            throw new CoordException(ErrorCode.PROTOCOL_MISMATCH);
        })).isInstanceOf(CoordException.class)
                .extracting(e -> ((CoordException) e).getErrorCode())
                .isEqualTo(ErrorCode.PROTOCOL_MISMATCH);

        assertThat(calls.get()).isEqualTo(1); // No retry
    }

    @Test
    void shouldFailAfterMaxRetries() {
        AtomicInteger calls = new AtomicInteger(0);
        assertThatThrownBy(() -> retryTemplate.execute(ctx -> {
            calls.incrementAndGet();
            throw new CoordException(ErrorCode.AGENT_UNAVAILABLE);
        })).isInstanceOf(CoordException.class)
                .extracting(e -> ((CoordException) e).getErrorCode())
                .isEqualTo(ErrorCode.AGENT_UNAVAILABLE);

        assertThat(calls.get()).isEqualTo(3); // 3 attempts total
    }

    @Test
    void shouldPropagateNonCoordExceptionWithoutRetry() {
        AtomicInteger calls = new AtomicInteger(0);
        assertThatThrownBy(() -> retryTemplate.execute(ctx -> {
            calls.incrementAndGet();
            throw new RuntimeException("unexpected");
        })).isInstanceOf(RuntimeException.class)
                .hasMessage("unexpected");

        assertThat(calls.get()).isEqualTo(1);
    }

    @Test
    void shouldTrackAttemptNumber() throws Exception {
        AtomicInteger firstAttempt = new AtomicInteger(0);
        AtomicInteger secondAttempt = new AtomicInteger(0);
        retryTemplate.execute(ctx -> {
            if (ctx.getAttemptNumber() == 1) {
                firstAttempt.set(ctx.getAttemptNumber());
                throw new CoordException(ErrorCode.AGENT_UNAVAILABLE);
            }
            secondAttempt.set(ctx.getAttemptNumber());
            return "ok";
        });
        assertThat(firstAttempt.get()).isEqualTo(1);
        assertThat(secondAttempt.get()).isEqualTo(2);
    }

    @Test
    void shouldNotExceedMaxAttempts() {
        AtomicInteger calls = new AtomicInteger(0);
        assertThatThrownBy(() -> retryTemplate.execute(ctx -> {
            int a = calls.incrementAndGet();
            assertThat(a).isLessThanOrEqualTo(3);
            throw new CoordException(ErrorCode.AGENT_UNAVAILABLE);
        }));
        assertThat(calls.get()).isEqualTo(3);
    }
}
