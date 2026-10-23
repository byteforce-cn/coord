package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.contracts.tso.v2.TSOGrpc;
import cn.byteforce.coord.contracts.tso.v2.Tso;
import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import cn.byteforce.coord.sdk.tso.v2.TsoClient;

import java.util.concurrent.TimeUnit;

/**
 * {@link TsoClient} backed by {@code coord.tso.v2} calls to the Coord Agent.
 */
public final class TsoV2ClientImpl extends AgentRpcClient implements TsoClient {

    private final CoordConfig config;

    public TsoV2ClientImpl(AgentChannelManager channelManager, ErrorMapper errorMapper,
                           RetryTemplate retryTemplate, ObservabilityProvider observability,
                           CoordConfig config) {
        super(channelManager, errorMapper, retryTemplate, observability);
        this.config = config;
    }

    private long deadlineMs() {
        return config.getRequestTimeout().toMillis();
    }

    @Override
    public long getTimestamp() {
        Tso.GetTsResponse response = callWithRetry(
                (ch, req) -> TSOGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(deadlineMs(), TimeUnit.MILLISECONDS)
                        .getTs((Tso.GetTsRequest) req),
                Tso.GetTsRequest.getDefaultInstance(),
                "tsoV2.getTimestamp");
        return response.getTs();
    }

    @Override
    public TimestampBatch allocate(int batchSize) {
        Tso.AllocateRequest request = Tso.AllocateRequest.newBuilder()
                .setBatchSize(batchSize)
                .build();
        Tso.AllocateResponse response = callWithRetry(
                (ch, req) -> TSOGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(deadlineMs(), TimeUnit.MILLISECONDS)
                        .allocate((Tso.AllocateRequest) req),
                request,
                "tsoV2.allocate");
        return new TimestampBatch(response.getStartTs(), response.getEndTs());
    }
}
