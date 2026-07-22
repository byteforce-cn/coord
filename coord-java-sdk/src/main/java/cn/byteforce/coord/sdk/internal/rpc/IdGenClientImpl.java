package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.CoordException;
import cn.byteforce.coord.sdk.idgen.IdGenClient;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.sdk.internal.proto.*;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Implementation of {@link IdGenClient} backed by gRPC calls to the Coord Agent.
 */
public final class IdGenClientImpl extends AgentRpcClient implements IdGenClient {

    private static final Logger log = LoggerFactory.getLogger(IdGenClientImpl.class);
    private final CoordConfig config;

    public IdGenClientImpl(AgentChannelManager channelManager, ErrorMapper errorMapper,
                           RetryTemplate retryTemplate, ObservabilityProvider observability,
                           CoordConfig config) {
        super(channelManager, errorMapper, retryTemplate, observability);
        this.config = config;
    }

    @Override
    public long nextId(String name) {
        IdGenNextIdRequest request = IdGenNextIdRequest.newBuilder()
                .setName(name)
                .build();

        IdGenNextIdResponse response = callWithRetry(
                (ch, req) -> IdGenGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .nextId((IdGenNextIdRequest) req),
                request, "idgen.nextId");

        log.debug("IdGen nextId: name={}, id={}", name, response.getId());
        return response.getId();
    }

    @Override
    public List<Long> nextBatch(String name, int count) {
        IdGenNextBatchRequest request = IdGenNextBatchRequest.newBuilder()
                .setName(name)
                .setCount(count)
                .build();

        IdGenNextBatchResponse response = callWithRetry(
                (ch, req) -> IdGenGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .nextBatch((IdGenNextBatchRequest) req),
                request, "idgen.nextBatch");

        log.debug("IdGen nextBatch: name={}, count={}", name, response.getIdsCount());
        return response.getIdsList().stream()
                .map(Long::valueOf)
                .collect(Collectors.toList());
    }
}
