package cn.byteforce.coord.sdk;

import cn.byteforce.coord.contracts.kv.v2.KVGrpc;
import cn.byteforce.coord.contracts.kv.v2.Kv;
import cn.byteforce.coord.contracts.lease.v2.LeaseGrpc;
import cn.byteforce.coord.contracts.lease.v2.LeaseOuterClass;
import cn.byteforce.coord.contracts.tso.v2.TSOGrpc;
import cn.byteforce.coord.contracts.tso.v2.Tso;
import cn.byteforce.coord.sdk.kv.v2.KvClient;
import cn.byteforce.coord.sdk.lease.v2.LeaseClient;
import cn.byteforce.coord.sdk.tso.v2.TsoClient;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * KV-T v2 SDK surface: a real Netty gRPC server (fake v2 services) is used so the
 * blocking stubs, trailer mapping, and retry template are exercised end to end.
 * <p>
 * No handshake is needed: the v2 methods go straight to the stub, and the fake
 * server is the transport-level agent contract (not the protocol negotiation).
 */
class KvTV2ClientTest {

    private static final Metadata.Key<String> ERROR_CODE =
            Metadata.Key.of("x-coord-error-code", Metadata.ASCII_STRING_MARSHALLER);

    private Server server;
    private CoordClient client;
    private FakeKv kv;

    @BeforeEach
    void setUp() throws IOException {
        kv = new FakeKv();
        server = NettyServerBuilder.forPort(0)
                .addService(new FakeTso())
                .addService(kv)
                .addService(new FakeLease())
                .build()
                .start();
        CoordConfig config = CoordConfig.builder()
                .agentHost("127.0.0.1")
                .agentPort(server.getPort())
                .build();
        client = CoordClient.create(config);
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close(Duration.ZERO);
        }
        if (server != null) {
            server.shutdownNow();
        }
    }

    @Test
    void tsoAllocatesAndKvRoundTrips() {
        long ts = client.tsoV2().getTimestamp();
        assertThat(ts).isPositive();

        TsoClient.TimestampBatch batch = client.tsoV2().allocate(8);
        assertThat(batch.endTs()).isGreaterThan(batch.startTs());

        KvClient.PutResult put = client.kvV2()
                .put(bytes("k"), bytes("v"), ts, false, new byte[0]);
        assertThat(put.commitTs()).isEqualTo(ts);

        KvClient.RangeResult ranged = client.kvV2().range(bytes("k"));
        assertThat(ranged.kvs()).hasSize(1);
        assertThat(text(ranged.kvs().get(0).key())).isEqualTo("k");
        assertThat(text(ranged.kvs().get(0).value())).isEqualTo("v");
        assertThat(ranged.readTs()).isEqualTo(ts);

        KvClient.DeleteResult deleted = client.kvV2().delete(bytes("k"));
        assertThat(deleted.deleted()).isEqualTo(1);
    }

    @Test
    void leaseGrantKeepAliveTimeToLiveAndRevoke() {
        LeaseClient.Grant grant = client.leaseV2().grant(30);
        assertThat(grant.leaseId()).isEqualTo(7);
        assertThat(grant.ttlSeconds()).isEqualTo(30);
        assertThat(grant.grantedTs()).isPositive();

        assertThat(client.leaseV2().keepAlive(7)).isEqualTo(29);

        LeaseClient.State state = client.leaseV2().timeToLive(7, true);
        assertThat(state.ttlSeconds()).isEqualTo(29);
        assertThat(state.keys()).hasSize(1);
        assertThat(text(state.keys().get(0))).isEqualTo("k");

        assertThat(client.leaseV2().revoke(7, new byte[0])).isEqualTo(42);
    }

    @Test
    void commitRejectedKeepsItsWireCode() {
        kv.rejectPut = true;

        CoordException err = catchThrowableOfType(
                () -> client.kvV2().put(bytes("k"), bytes("v"), 1, false, new byte[0]),
                CoordException.class);

        assertThat(err.getErrorCode()).isEqualTo(ErrorCode.COMMIT_REJECTED);
    }

    @Test
    void safeTsLagIsRetried() {
        kv.failFirstRangeWithSafeTsLag = true;

        KvClient.RangeResult ranged = client.kvV2().range(bytes("k"));

        assertThat(kv.rangeCalls.get()).isEqualTo(2);
        assertThat(ranged.readTs()).isPositive();
    }

    // ──── fake v2 services ────

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String text(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }

    private static final class FakeTso extends TSOGrpc.TSOImplBase {
        private final AtomicLong next = new AtomicLong(100);

        @Override
        public void getTs(Tso.GetTsRequest request,
                          StreamObserver<Tso.GetTsResponse> responseObserver) {
            responseObserver.onNext(Tso.GetTsResponse.newBuilder()
                    .setTs(next.incrementAndGet())
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void allocate(Tso.AllocateRequest request,
                             StreamObserver<Tso.AllocateResponse> responseObserver) {
            int size = request.getBatchSize() == 0 ? 1 : request.getBatchSize();
            long start = next.getAndAdd(size);
            responseObserver.onNext(Tso.AllocateResponse.newBuilder()
                    .setStartTs(start)
                    .setEndTs(start + size)
                    .build());
            responseObserver.onCompleted();
        }
    }

    private static final class FakeKv extends KVGrpc.KVImplBase {
        final AtomicInteger rangeCalls = new AtomicInteger();
        volatile boolean failFirstRangeWithSafeTsLag;
        volatile boolean rejectPut;

        @Override
        public void put(Kv.PutRequest request,
                        StreamObserver<Kv.PutResponse> responseObserver) {
            if (rejectPut) {
                responseObserver.onError(error(
                        Status.FAILED_PRECONDITION.withDescription("commit_ts rejected"),
                        "COMMIT_REJECTED"));
                return;
            }
            Kv.PutResponse.Builder response = Kv.PutResponse.newBuilder()
                    .setCommitTs(request.getCommitTs());
            if (request.getPrevKv()) {
                response.setPrevKv(Kv.KeyValue.newBuilder()
                        .setKey(request.getKey())
                        .setValue(com.google.protobuf.ByteString.copyFrom(bytes("old")))
                        .setCreateRevision(request.getCommitTs() - 1)
                        .setModRevision(request.getCommitTs() - 1)
                        .setVersion(1)
                        .build());
            }
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        }

        @Override
        public void range(Kv.RangeRequest request,
                          StreamObserver<Kv.RangeResponse> responseObserver) {
            int call = rangeCalls.incrementAndGet();
            if (failFirstRangeWithSafeTsLag && call == 1) {
                responseObserver.onError(error(
                        Status.UNAVAILABLE.withDescription("safe_ts lagging"),
                        "SAFE_TS_LAG"));
                return;
            }
            responseObserver.onNext(Kv.RangeResponse.newBuilder()
                    .addKvs(Kv.KeyValue.newBuilder()
                            .setKey(request.getKey())
                            .setValue(com.google.protobuf.ByteString.copyFrom(bytes("v")))
                            .setCreateRevision(101)
                            .setModRevision(101)
                            .setVersion(1)
                            .build())
                    .setReadTs(101)
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void delete(Kv.DeleteRequest request,
                           StreamObserver<Kv.DeleteResponse> responseObserver) {
            responseObserver.onNext(Kv.DeleteResponse.newBuilder()
                    .setDeleted(1)
                    .setCommitTs(request.getCommitTs())
                    .build());
            responseObserver.onCompleted();
        }
    }

    private static final class FakeLease extends LeaseGrpc.LeaseImplBase {
        @Override
        public void leaseGrant(LeaseOuterClass.LeaseGrantRequest request,
                               StreamObserver<LeaseOuterClass.LeaseGrantResponse> observer) {
            observer.onNext(LeaseOuterClass.LeaseGrantResponse.newBuilder()
                    .setLeaseId(7)
                    .setTtlSeconds(request.getTtlSeconds())
                    .setGrantedTs(100)
                    .build());
            observer.onCompleted();
        }

        @Override
        public void leaseRevoke(LeaseOuterClass.LeaseRevokeRequest request,
                                StreamObserver<LeaseOuterClass.LeaseRevokeResponse> observer) {
            observer.onNext(LeaseOuterClass.LeaseRevokeResponse.newBuilder()
                    .setCommitTs(42)
                    .build());
            observer.onCompleted();
        }

        @Override
        public StreamObserver<LeaseOuterClass.LeaseKeepAliveRequest> leaseKeepAlive(
                StreamObserver<LeaseOuterClass.LeaseKeepAliveResponse> responseObserver) {
            return new StreamObserver<>() {
                @Override
                public void onNext(LeaseOuterClass.LeaseKeepAliveRequest value) {
                    responseObserver.onNext(LeaseOuterClass.LeaseKeepAliveResponse.newBuilder()
                            .setLeaseId(value.getLeaseId())
                            .setTtlSeconds(29)
                            .build());
                }

                @Override
                public void onError(Throwable t) {
                    responseObserver.onError(t);
                }

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            };
        }

        @Override
        public void leaseTimeToLive(LeaseOuterClass.LeaseTimeToLiveRequest request,
                                    StreamObserver<LeaseOuterClass.LeaseTimeToLiveResponse> observer) {
            observer.onNext(LeaseOuterClass.LeaseTimeToLiveResponse.newBuilder()
                    .setLeaseId(request.getLeaseId())
                    .setTtlSeconds(29)
                    .setGrantedTtlSeconds(30)
                    .addKeys(com.google.protobuf.ByteString.copyFrom(bytes("k")))
                    .build());
            observer.onCompleted();
        }
    }

    private static StatusRuntimeException error(Status status, String code) {
        Metadata metadata = new Metadata();
        metadata.put(ERROR_CODE, code);
        return status.asRuntimeException(metadata);
    }
}
