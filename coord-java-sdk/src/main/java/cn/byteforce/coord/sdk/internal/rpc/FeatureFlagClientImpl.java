package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.featureflags.FeatureFlagClient;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.contracts.featureflags.v1.*;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of {@link FeatureFlagClient} backed by gRPC to the Coord Agent.
 */
public final class FeatureFlagClientImpl extends AgentRpcClient implements FeatureFlagClient {

    private static final Logger log = LoggerFactory.getLogger(FeatureFlagClientImpl.class);
    private final CoordConfig config;

    public FeatureFlagClientImpl(AgentChannelManager channelManager, ErrorMapper errorMapper,
                                 RetryTemplate retryTemplate, ObservabilityProvider observability,
                                 CoordConfig config) {
        super(channelManager, errorMapper, retryTemplate, observability);
        this.config = config;
    }

    @Override
    public FlagDecision isEnabled(String flagName, String context) {
        FeatureFlagIsEnabledRequest request = FeatureFlagIsEnabledRequest.newBuilder()
                .setFlagName(flagName)
                .setContext(toContext(context))
                .build();

        FeatureFlagIsEnabledResponse response = callWithRetry(
                (ch, req) -> FeatureFlagsGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .isEnabled((FeatureFlagIsEnabledRequest) req),
                request, "featureFlags.isEnabled");

        log.debug("Feature flag isEnabled: name={}, enabled={}, variant={}",
                flagName, response.getEnabled(), response.getVariant());
        return new FlagDecision(response.getEnabled(),
                response.getVariant().isEmpty() ? null : response.getVariant());
    }

    @Override
    public String evaluate(String flagName, String context) {
        FeatureFlagEvaluateRequest request = FeatureFlagEvaluateRequest.newBuilder()
                .setFlagName(flagName)
                .setContext(toContext(context))
                .build();

        FeatureFlagEvaluateResponse response = callWithRetry(
                (ch, req) -> FeatureFlagsGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .evaluate((FeatureFlagEvaluateRequest) req),
                request, "featureFlags.evaluate");

        String result = response.getResult().toStringUtf8();
        log.debug("Feature flag evaluate: name={}, resultBytes={}", flagName, response.getResult().size());
        return result;
    }

    /** 上下文字段是 bytes（JSON）；null 与空串都表示"无上下文"。 */
    private static ByteString toContext(String context) {
        return context == null
                ? ByteString.EMPTY
                : ByteString.copyFrom(context, StandardCharsets.UTF_8);
    }
}
