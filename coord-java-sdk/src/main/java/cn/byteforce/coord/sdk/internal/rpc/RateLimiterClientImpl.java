package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.contracts.ratelimiter.v1.*;
import cn.byteforce.coord.sdk.ratelimiter.RateLimiterClient;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Implementation of {@link RateLimiterClient} backed by gRPC to the Coord Agent.
 */
public final class RateLimiterClientImpl extends AgentRpcClient implements RateLimiterClient {

    private static final Logger log = LoggerFactory.getLogger(RateLimiterClientImpl.class);
    private final CoordConfig config;

    public RateLimiterClientImpl(AgentChannelManager channelManager, ErrorMapper errorMapper,
                                 RetryTemplate retryTemplate, ObservabilityProvider observability,
                                 CoordConfig config) {
        super(channelManager, errorMapper, retryTemplate, observability);
        this.config = config;
    }

    @Override
    public RateLimitDecision allow(String key, int permits) {
        RateLimiterAllowRequest request = RateLimiterAllowRequest.newBuilder()
                .setKey(key)
                .setPermits(permits)
                .build();

        RateLimiterAllowResponse response = callWithRetry(
                (ch, req) -> RateLimiterGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .allow((RateLimiterAllowRequest) req),
                request, "rateLimiter.allow");

        log.debug("Rate limiter allow: key={}, permits={}, allowed={}, remaining={}",
                key, permits, response.getAllowed(), response.getRemaining());
        return new RateLimitDecision(response.getAllowed(), response.getRemaining(), response.getResetTime());
    }
}
