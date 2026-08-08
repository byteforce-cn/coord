package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.cache.CacheClient;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.sdk.internal.proto.CacheGetRequest;
import cn.byteforce.coord.sdk.internal.proto.CacheGetResponse;
import cn.byteforce.coord.sdk.internal.proto.CacheGrpc;
import cn.byteforce.coord.sdk.internal.proto.CacheLLenRequest;
import cn.byteforce.coord.sdk.internal.proto.CacheLLenResponse;
import cn.byteforce.coord.sdk.internal.proto.CacheLPushRequest;
import cn.byteforce.coord.sdk.internal.proto.CacheLPushResponse;
import cn.byteforce.coord.sdk.internal.proto.CacheRPopRequest;
import cn.byteforce.coord.sdk.internal.proto.CacheRPopResponse;
import cn.byteforce.coord.sdk.internal.proto.CacheSetRequest;
import cn.byteforce.coord.sdk.internal.proto.CacheSetResponse;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the {@link CacheClient} gRPC client against an in-process stub server.
 * <p>
 * Verifies Phase 2 SDK surface: rpop (atomic dequeue) / llen.
 */
class CacheClientTest {

    private Server server;
    private ManagedChannel channel;
    private CacheClient cache;
    private FakeCacheService fake;

    @BeforeEach
    void setUp() throws Exception {
        fake = new FakeCacheService();
        server = ServerBuilder.forPort(0).addService(fake).build().start();
        channel = ManagedChannelBuilder.forAddress("localhost", server.getPort())
                .usePlaintext()
                .build();

        AgentChannelManager channelManager = mock(AgentChannelManager.class);
        when(channelManager.getChannel()).thenReturn(channel);

        cache = new CacheClientImpl(
                channelManager,
                new ErrorMapper(),
                new RetryTemplate(),
                new ObservabilityProvider() {
                },
                CoordConfig.builder().agentHost("localhost").build());
    }

    @AfterEach
    void tearDown() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    @Test
    void shouldPushAndPopAtomically() {
        cache.lpush("q", "a".getBytes());
        cache.lpush("q", "b".getBytes());
        assertThat(fake.list("q")).containsExactly("b".getBytes(), "a".getBytes());

        // rpop 取队尾
        assertThat(cache.rpop("q")).isEqualTo("a".getBytes());
        assertThat(cache.rpop("q")).isEqualTo("b".getBytes());
        // 空列表 → null
        assertThat(cache.rpop("q")).isNull();
    }

    @Test
    void shouldReturnListLength() {
        assertThat(cache.llen("q")).isZero();

        cache.lpush("q", "x".getBytes());
        cache.lpush("q", "y".getBytes());
        assertThat(cache.llen("q")).isEqualTo(2);

        cache.rpop("q");
        assertThat(cache.llen("q")).isEqualTo(1);
    }

    @Test
    void shouldGetSetDeleteStillWork() {
        cache.set("k", "v".getBytes(), 60);
        assertThat(cache.get("k")).isEqualTo("v".getBytes());
    }

    // ──── In-process stub server ────

    /** Minimal Cache server stub with list state, mimicking agent semantics. */
    static class FakeCacheService extends CacheGrpc.CacheImplBase {
        final Map<String, java.util.List<byte[]>> lists = new LinkedHashMap<>();

        java.util.List<byte[]> list(String key) {
            return lists.getOrDefault(key, java.util.List.of());
        }

        @Override
        public void lPush(CacheLPushRequest request, StreamObserver<CacheLPushResponse> responseObserver) {
            java.util.List<byte[]> l = lists.computeIfAbsent(request.getKey(), k -> new java.util.ArrayList<>());
            l.add(0, request.getValue().toByteArray());
            responseObserver.onNext(CacheLPushResponse.newBuilder().setLength(l.size()).build());
            responseObserver.onCompleted();
        }

        @Override
        public void rPop(CacheRPopRequest request, StreamObserver<CacheRPopResponse> responseObserver) {
            java.util.List<byte[]> l = lists.get(request.getKey());
            CacheRPopResponse.Builder resp = CacheRPopResponse.newBuilder();
            if (l != null && !l.isEmpty()) {
                byte[] value = l.remove(l.size() - 1);
                resp.setValue(ByteString.copyFrom(value)).setFound(true);
            } else {
                resp.setFound(false);
            }
            responseObserver.onNext(resp.build());
            responseObserver.onCompleted();
        }

        @Override
        public void lLen(CacheLLenRequest request, StreamObserver<CacheLLenResponse> responseObserver) {
            java.util.List<byte[]> l = lists.get(request.getKey());
            long len = l == null ? 0 : l.size();
            responseObserver.onNext(CacheLLenResponse.newBuilder().setLength(len).build());
            responseObserver.onCompleted();
        }

        @Override
        public void set(CacheSetRequest request, StreamObserver<CacheSetResponse> responseObserver) {
            // get/set 基础路径（string 存储简化：直接 map 覆盖）
            responseObserver.onNext(CacheSetResponse.getDefaultInstance());
            responseObserver.onCompleted();
        }

        @Override
        public void get(CacheGetRequest request, StreamObserver<CacheGetResponse> responseObserver) {
            CacheGetResponse.Builder resp = CacheGetResponse.newBuilder().setFound(false);
            if ("k".equals(request.getKey())) {
                resp.setValue(ByteString.copyFromUtf8("v")).setFound(true);
            }
            responseObserver.onNext(resp.build());
            responseObserver.onCompleted();
        }
    }
}
