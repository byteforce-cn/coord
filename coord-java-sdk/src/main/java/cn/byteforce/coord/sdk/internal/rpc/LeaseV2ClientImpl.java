package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.contracts.lease.v2.LeaseGrpc;
import cn.byteforce.coord.contracts.lease.v2.LeaseOuterClass;
import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.sdk.lease.v2.LeaseClient;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link LeaseClient} backed by {@code coord.lease.v2} calls to the Coord Agent.
 * <p>
 * Keep-alive is a bidirectional stream on the wire; this SDK sends one request,
 * waits for the first response and closes the stream. Background renewal with a
 * stop lifecycle is intentionally left to the caller (or the agent-migration
 * slice) instead of hiding an unowned daemon thread.
 */
public final class LeaseV2ClientImpl extends AgentRpcClient implements LeaseClient {

    private final CoordConfig config;

    public LeaseV2ClientImpl(AgentChannelManager channelManager, ErrorMapper errorMapper,
                             RetryTemplate retryTemplate, ObservabilityProvider observability,
                             CoordConfig config) {
        super(channelManager, errorMapper, retryTemplate, observability);
        this.config = config;
    }

    private long deadlineMs() {
        return config.getRequestTimeout().toMillis();
    }

    private static ByteString bytes(byte[] value) {
        return value == null || value.length == 0
                ? ByteString.EMPTY : ByteString.copyFrom(value);
    }

    @Override
    public Grant grant(long ttlSeconds, long leaseId, byte[] requestId) {
        LeaseOuterClass.LeaseGrantRequest request = LeaseOuterClass.LeaseGrantRequest.newBuilder()
                .setTtlSeconds(ttlSeconds)
                .setLeaseId(leaseId)
                .setRequestId(bytes(requestId))
                .build();
        LeaseOuterClass.LeaseGrantResponse response = callWithRetry(
                (ch, req) -> LeaseGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(deadlineMs(), TimeUnit.MILLISECONDS)
                        .leaseGrant((LeaseOuterClass.LeaseGrantRequest) req),
                request,
                "leaseV2.grant");
        return new Grant(
                response.getLeaseId(),
                response.getTtlSeconds(),
                response.getGrantedTs());
    }

    @Override
    public long revoke(long leaseId, byte[] requestId) {
        LeaseOuterClass.LeaseRevokeRequest request = LeaseOuterClass.LeaseRevokeRequest.newBuilder()
                .setLeaseId(leaseId)
                .setRequestId(bytes(requestId))
                .build();
        LeaseOuterClass.LeaseRevokeResponse response = callWithRetry(
                (ch, req) -> LeaseGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(deadlineMs(), TimeUnit.MILLISECONDS)
                        .leaseRevoke((LeaseOuterClass.LeaseRevokeRequest) req),
                request,
                "leaseV2.revoke");
        return response.getCommitTs();
    }

    @Override
    public long keepAlive(long leaseId) {
        LeaseOuterClass.LeaseKeepAliveRequest request =
                LeaseOuterClass.LeaseKeepAliveRequest.newBuilder()
                        .setLeaseId(leaseId)
                        .build();
        LeaseOuterClass.LeaseKeepAliveResponse response = callWithRetry(
                (ch, req) -> {
                    CountDownLatch done = new CountDownLatch(1);
                    AtomicReference<LeaseOuterClass.LeaseKeepAliveResponse> responseRef =
                            new AtomicReference<>();
                    AtomicReference<Throwable> errorRef = new AtomicReference<>();

                    LeaseGrpc.LeaseStub stub = LeaseGrpc.newStub(ch)
                            .withDeadlineAfter(deadlineMs(), TimeUnit.MILLISECONDS);
                    StreamObserver<LeaseOuterClass.LeaseKeepAliveResponse> observer =
                            new StreamObserver<>() {
                                @Override
                                public void onNext(
                                        LeaseOuterClass.LeaseKeepAliveResponse value) {
                                    responseRef.set(value);
                                }

                                @Override
                                public void onError(Throwable t) {
                                    errorRef.set(t);
                                    done.countDown();
                                }

                                @Override
                                public void onCompleted() {
                                    done.countDown();
                                }
                            };
                    StreamObserver<LeaseOuterClass.LeaseKeepAliveRequest> requestObserver =
                            stub.leaseKeepAlive(observer);
                    requestObserver.onNext((LeaseOuterClass.LeaseKeepAliveRequest) req);
                    requestObserver.onCompleted();

                    try {
                        if (!done.await(deadlineMs() + 5000, TimeUnit.MILLISECONDS)) {
                            throw Status.DEADLINE_EXCEEDED
                                    .withDescription("lease keep-alive timed out")
                                    .asRuntimeException();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw Status.CANCELLED
                                .withDescription("interrupted during lease keep-alive")
                                .asRuntimeException();
                    }

                    Throwable error = errorRef.get();
                    if (error instanceof StatusRuntimeException sre) {
                        throw sre;
                    }
                    if (error != null) {
                        throw Status.INTERNAL
                                .withDescription("lease keep-alive failed")
                                .withCause(error)
                                .asRuntimeException();
                    }
                    LeaseOuterClass.LeaseKeepAliveResponse value = responseRef.get();
                    if (value == null) {
                        throw Status.INTERNAL
                                .withDescription("lease keep-alive closed before a response")
                                .asRuntimeException();
                    }
                    return value;
                },
                request,
                "leaseV2.keepAlive");
        return response.getTtlSeconds();
    }

    @Override
    public State timeToLive(long leaseId, boolean keys) {
        LeaseOuterClass.LeaseTimeToLiveRequest request =
                LeaseOuterClass.LeaseTimeToLiveRequest.newBuilder()
                        .setLeaseId(leaseId)
                        .setKeys(keys)
                        .build();
        LeaseOuterClass.LeaseTimeToLiveResponse response = callWithRetry(
                (ch, req) -> LeaseGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(deadlineMs(), TimeUnit.MILLISECONDS)
                        .leaseTimeToLive((LeaseOuterClass.LeaseTimeToLiveRequest) req),
                request,
                "leaseV2.timeToLive");
        List<byte[]> boundKeys = new ArrayList<>(response.getKeysCount());
        for (ByteString key : response.getKeysList()) {
            boundKeys.add(key.toByteArray());
        }
        return new State(
                response.getLeaseId(),
                response.getTtlSeconds(),
                response.getGrantedTtlSeconds(),
                List.copyOf(boundKeys));
    }
}
