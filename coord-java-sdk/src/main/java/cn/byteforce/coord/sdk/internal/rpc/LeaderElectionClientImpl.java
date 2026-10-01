package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.CoordException;
import cn.byteforce.coord.sdk.election.LeaderElectionClient;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.contracts.election.v1.*;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Implementation of {@link LeaderElectionClient} backed by gRPC to the Coord Agent.
 */
public final class LeaderElectionClientImpl extends AgentRpcClient implements LeaderElectionClient {

    private static final Logger log = LoggerFactory.getLogger(LeaderElectionClientImpl.class);
    private final CoordConfig config;

    public LeaderElectionClientImpl(AgentChannelManager channelManager, ErrorMapper errorMapper,
                                    RetryTemplate retryTemplate, ObservabilityProvider observability,
                                    CoordConfig config) {
        super(channelManager, errorMapper, retryTemplate, observability);
        this.config = config;
    }

    @Override
    public LeaderLease campaign(String groupName, String candidateId, long ttlSeconds) {
        LeaderCampaignRequest request = LeaderCampaignRequest.newBuilder()
                .setGroupName(groupName)
                .setCandidateId(candidateId)
                .setTtlSeconds(ttlSeconds)
                .build();

        LeaderCampaignResponse response = callWithRetry(
                (ch, req) -> LeaderElectionGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .campaign((LeaderCampaignRequest) req),
                request, "election.campaign");

        log.debug("Election campaign: group={}, candidate={}, elected={}, lease={}",
                groupName, candidateId, response.getElected(), response.getLeaseId());
        return new LeaderLease(response.getElected(), response.getLeaseId(), response.getLeaderId());
    }

    @Override
    public boolean resign(String groupName, String candidateId, long leaseId) {
        LeaderResignRequest request = LeaderResignRequest.newBuilder()
                .setGroupName(groupName)
                .setCandidateId(candidateId)
                .setLeaseId(leaseId)
                .build();

        LeaderResignResponse response = callWithRetry(
                (ch, req) -> LeaderElectionGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .resign((LeaderResignRequest) req),
                request, "election.resign");

        log.debug("Election resign: group={}, candidate={}, resigned={}",
                groupName, candidateId, response.getResigned());
        return response.getResigned();
    }

    @Override
    public Leader currentLeader(String groupName) {
        LeaderGetLeaderRequest request = LeaderGetLeaderRequest.newBuilder()
                .setGroupName(groupName)
                .build();

        LeaderGetLeaderResponse response = callWithRetry(
                (ch, req) -> LeaderElectionGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .getLeader((LeaderGetLeaderRequest) req),
                request, "election.getLeader");

        if (!response.getExists()) {
            return null;
        }
        return new Leader(response.getLeaderId(), response.getLeaseId(), response.getElectedAt());
    }

    @Override
    public AutoCloseable watch(String groupName, Consumer<LeaderChangeEvent> onEvent,
                               Consumer<Throwable> onError) {
        LeaderWatchRequest request = LeaderWatchRequest.newBuilder()
                .setGroupName(groupName)
                .build();

        AtomicBoolean cancelled = new AtomicBoolean(false);
        AtomicReference<Iterator<LeaderWatchEvent>> streamRef = new AtomicReference<>();
        try {
            Iterator<LeaderWatchEvent> stream = LeaderElectionGrpc.newBlockingStub(channelManager.getChannel())
                    .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                    .watch(request);
            streamRef.set(stream);
        } catch (io.grpc.StatusRuntimeException e) {
            throw errorMapper.map(e);
        }

        // 阻塞式流在守护线程里消费：SDK 其余调用都是同步的，watch 返回句柄即可用。
        Thread pump = new Thread(() -> {
            Iterator<LeaderWatchEvent> stream = streamRef.get();
            try {
                while (!cancelled.get() && stream.hasNext()) {
                    LeaderWatchEvent raw = stream.next();
                    try {
                        onEvent.accept(toEvent(raw));
                    } catch (RuntimeException ex) {
                        log.warn("Election watch callback threw: {}", ex.toString());
                    }
                }
            } catch (RuntimeException e) {
                // 取消导致的打断不是错误，不上报
                if (!cancelled.get() && onError != null) {
                    onError.accept(e);
                }
            }
        }, "coord-election-watch-" + groupName);
        pump.setDaemon(true);
        pump.start();

        return () -> {
            cancelled.set(true);
            pump.interrupt();
        };
    }

    private static LeaderChangeEvent toEvent(LeaderWatchEvent event) {
        LeaderChangeKind kind = switch (event.getType()) {
            case LEADER_ELECTED -> LeaderChangeKind.ELECTED;
            case LEADER_RESIGNED -> LeaderChangeKind.RESIGNED;
            case LEADER_EXPIRED -> LeaderChangeKind.EXPIRED;
            default -> LeaderChangeKind.UNKNOWN;
        };
        return new LeaderChangeEvent(kind, event.getGroupName(), event.getLeaderId());
    }
}
