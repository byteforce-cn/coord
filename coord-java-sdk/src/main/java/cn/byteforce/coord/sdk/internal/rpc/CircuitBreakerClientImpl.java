package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.circuitbreaker.CircuitBreakerClient;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.contracts.circuitbreaker.v1.*;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Implementation of {@link CircuitBreakerClient} backed by gRPC to the Coord Agent.
 */
public final class CircuitBreakerClientImpl extends AgentRpcClient implements CircuitBreakerClient {

    private static final Logger log = LoggerFactory.getLogger(CircuitBreakerClientImpl.class);
    private final CoordConfig config;

    public CircuitBreakerClientImpl(AgentChannelManager channelManager, ErrorMapper errorMapper,
                                    RetryTemplate retryTemplate, ObservabilityProvider observability,
                                    CoordConfig config) {
        super(channelManager, errorMapper, retryTemplate, observability);
        this.config = config;
    }

    @Override
    public BreakerState getState(String name) {
        CircuitBreakerGetStateRequest request = CircuitBreakerGetStateRequest.newBuilder()
                .setName(name)
                .build();

        CircuitBreakerGetStateResponse response = callWithRetry(
                (ch, req) -> CircuitBreakerGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .getState((CircuitBreakerGetStateRequest) req),
                request, "circuitBreaker.getState");

        return new BreakerState(toState(response.getState()), response.getLastFailureTime());
    }

    @Override
    public void reportSuccess(String name) {
        CircuitBreakerReportSuccessRequest request = CircuitBreakerReportSuccessRequest.newBuilder()
                .setName(name)
                .build();

        callWithRetry(
                (ch, req) -> CircuitBreakerGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .reportSuccess((CircuitBreakerReportSuccessRequest) req),
                request, "circuitBreaker.reportSuccess");
        log.debug("Circuit breaker success reported: {}", name);
    }

    @Override
    public void reportFailure(String name) {
        CircuitBreakerReportFailureRequest request = CircuitBreakerReportFailureRequest.newBuilder()
                .setName(name)
                .build();

        callWithRetry(
                (ch, req) -> CircuitBreakerGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .reportFailure((CircuitBreakerReportFailureRequest) req),
                request, "circuitBreaker.reportFailure");
        log.debug("Circuit breaker failure reported: {}", name);
    }

    @Override
    public void reset(String name) {
        CircuitBreakerResetRequest request = CircuitBreakerResetRequest.newBuilder()
                .setName(name)
                .build();

        callWithRetry(
                (ch, req) -> CircuitBreakerGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .reset((CircuitBreakerResetRequest) req),
                request, "circuitBreaker.reset");
        log.debug("Circuit breaker reset: {}", name);
    }

    /**
     * 契约（`circuitbreaker.proto`）把 state 定成**字符串**：`CLOSED` / `OPEN` /
     * `HALF_OPEN`。未知值不静默当作 `CLOSED`（那会把"看不懂的状态"伪装成"正常放行"），
     * 而是 warn + 归到 `HALF_OPEN`（探测态，宁可多走一次下游也不误判为健康）。
     */
    private static CircuitState toState(String raw) {
        if (raw == null) {
            return CircuitState.HALF_OPEN;
        }
        return switch (raw.trim().toUpperCase(java.util.Locale.ROOT)) {
            case "CLOSED" -> CircuitState.CLOSED;
            case "OPEN" -> CircuitState.OPEN;
            case "HALF_OPEN" -> CircuitState.HALF_OPEN;
            default -> {
                log.warn("Circuit breaker returned unknown state '{}'; treating as HALF_OPEN", raw);
                yield CircuitState.HALF_OPEN;
            }
        };
    }
}
