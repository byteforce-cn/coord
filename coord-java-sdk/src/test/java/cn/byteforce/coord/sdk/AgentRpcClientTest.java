package cn.byteforce.coord.sdk;

import cn.byteforce.coord.sdk.internal.rpc.RetryTemplate;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests verifying the RPC retry infrastructure.
 * RetryTemplate is tested directly; integration with gRPC stubs is covered by integration tests.
 */
class AgentRpcClientTest {

    @Test
    void shouldRetryOnRetryableErrorCodes() {
        RetryTemplate rt = new RetryTemplate();
        AtomicInteger calls = new AtomicInteger(0);

        String result = rt.execute(ctx -> {
            calls.incrementAndGet();
            if (ctx.getAttemptNumber() < 2) {
                throw new CoordException(ErrorCode.AGENT_UNAVAILABLE);
            }
            return "ok";
        });

        assertThat(result).isEqualTo("ok");
        assertThat(calls.get()).isEqualTo(2);
    }

    @Test
    void shouldRetryOnResourceExhausted() {
        RetryTemplate rt = new RetryTemplate();
        AtomicInteger calls = new AtomicInteger(0);

        String result = rt.execute(ctx -> {
            calls.incrementAndGet();
            if (ctx.getAttemptNumber() < 3) {
                throw new CoordException(ErrorCode.RESOURCE_EXHAUSTED);
            }
            return "ok";
        });

        assertThat(result).isEqualTo("ok");
        assertThat(calls.get()).isEqualTo(3);
    }

    @Test
    void shouldNotRetryOnNonRetryableErrorCode() {
        RetryTemplate rt = new RetryTemplate();
        AtomicInteger calls = new AtomicInteger(0);

        assertThatThrownBy(() -> rt.execute(ctx -> {
            calls.incrementAndGet();
            throw new CoordException(ErrorCode.PROTOCOL_MISMATCH);
        })).isInstanceOf(CoordException.class)
                .extracting(e -> ((CoordException) e).getErrorCode())
                .isEqualTo(ErrorCode.PROTOCOL_MISMATCH);

        assertThat(calls.get()).isEqualTo(1);
    }

    @Test
    void shouldThrowAfterMaxRetriesExhausted() {
        RetryTemplate rt = new RetryTemplate();
        AtomicInteger calls = new AtomicInteger(0);

        assertThatThrownBy(() -> rt.execute(ctx -> {
            calls.incrementAndGet();
            throw new CoordException(ErrorCode.AGENT_UNAVAILABLE);
        })).isInstanceOf(CoordException.class);

        assertThat(calls.get()).isEqualTo(3);
    }
}
