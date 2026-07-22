package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.CoordException;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.sdk.internal.proto.*;
import cn.byteforce.coord.sdk.lock.LockClient;
import cn.byteforce.coord.sdk.lock.LockInfo;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Implementation of {@link LockClient} backed by gRPC calls to the Coord Agent.
 */
public final class LockClientImpl extends AgentRpcClient implements LockClient {

    private static final Logger log = LoggerFactory.getLogger(LockClientImpl.class);
    private final CoordConfig config;

    public LockClientImpl(AgentChannelManager channelManager, ErrorMapper errorMapper,
                          RetryTemplate retryTemplate, ObservabilityProvider observability,
                          CoordConfig config) {
        super(channelManager, errorMapper, retryTemplate, observability);
        this.config = config;
    }

    @Override
    public LockInfo acquire(String name, String holderId, long ttlSeconds) {
        LockAcquireRequest request = LockAcquireRequest.newBuilder()
                .setName(name)
                .setHolderId(holderId)
                .setTtlSeconds(ttlSeconds)
                .build();

        LockAcquireResponse response = callWithRetry(
                (ch, req) -> LockGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .acquire((LockAcquireRequest) req),
                request, "lock.acquire");

        log.debug("Lock acquire: name={}, acquired={}", name, response.getAcquired());
        return new LockInfo(name, response.getHolderId(), response.getLeaseId(),
                0, ttlSeconds, response.getAcquired());
    }

    @Override
    public boolean release(String name, String holderId, long leaseId) {
        LockReleaseRequest request = LockReleaseRequest.newBuilder()
                .setName(name)
                .setHolderId(holderId)
                .setLeaseId(leaseId)
                .build();

        LockReleaseResponse response = callWithRetry(
                (ch, req) -> LockGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .release((LockReleaseRequest) req),
                request, "lock.release");

        log.debug("Lock release: name={}, released={}", name, response.getReleased());
        return response.getReleased();
    }

    @Override
    public boolean renew(String name, String holderId, long leaseId) {
        LockRenewRequest request = LockRenewRequest.newBuilder()
                .setName(name)
                .setHolderId(holderId)
                .setLeaseId(leaseId)
                .build();

        try {
            callWithRetry(
                    (ch, req) -> LockGrpc.newBlockingStub(ch)
                            .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                            .renew((LockRenewRequest) req),
                    request, "lock.renew");
            return true;
        } catch (CoordException e) {
            log.debug("Lock renew failed: name={}, error={}", name, e.getMessage());
            return false;
        }
    }

    @Override
    public LockInfo getLockInfo(String name) {
        LockGetInfoRequest request = LockGetInfoRequest.newBuilder()
                .setName(name)
                .build();

        LockGetInfoResponse response = callWithRetry(
                (ch, req) -> LockGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .getLockInfo((LockGetInfoRequest) req),
                request, "lock.getLockInfo");

        if (!response.getExists()) {
            return LockInfo.NOT_FOUND;
        }
        return new LockInfo(response.getName(), response.getHolderId(),
                response.getLeaseId(), response.getAcquiredAt(),
                response.getTtlSeconds(), true);
    }
}
