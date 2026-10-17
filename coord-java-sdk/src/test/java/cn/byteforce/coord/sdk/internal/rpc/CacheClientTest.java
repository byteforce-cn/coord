package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.CoordException;
import cn.byteforce.coord.sdk.ErrorCode;
import cn.byteforce.coord.sdk.cache.CacheClearResult;
import cn.byteforce.coord.sdk.cache.CacheClient;
import cn.byteforce.coord.sdk.cache.CacheKeyPage;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.contracts.cache.v1.CacheClearRequest;
import cn.byteforce.coord.contracts.cache.v1.CacheClearResponse;
import cn.byteforce.coord.contracts.cache.v1.CacheGetRequest;
import cn.byteforce.coord.contracts.cache.v1.CacheGetResponse;
import cn.byteforce.coord.contracts.cache.v1.CacheGrpc;
import cn.byteforce.coord.contracts.cache.v1.CacheKeysRequest;
import cn.byteforce.coord.contracts.cache.v1.CacheKeysResponse;
import cn.byteforce.coord.contracts.cache.v1.CacheLLenRequest;
import cn.byteforce.coord.contracts.cache.v1.CacheLLenResponse;
import cn.byteforce.coord.contracts.cache.v1.CacheLPushRequest;
import cn.byteforce.coord.contracts.cache.v1.CacheLPushResponse;
import cn.byteforce.coord.contracts.cache.v1.CacheRPopRequest;
import cn.byteforce.coord.contracts.cache.v1.CacheRPopResponse;
import cn.byteforce.coord.contracts.cache.v1.CacheSetIfAbsentRequest;
import cn.byteforce.coord.contracts.cache.v1.CacheSetIfAbsentResponse;
import cn.byteforce.coord.contracts.cache.v1.CacheSetRequest;
import cn.byteforce.coord.contracts.cache.v1.CacheSetResponse;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the {@link CacheClient} gRPC client against an in-process stub server.
 * <p>
 * Verifies SDK surface: rpop (atomic dequeue) / llen.
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

    @Test
    void shouldSetIfAbsentReportWrittenFlag() {
        // 首次写入成功
        assertThat(cache.setIfAbsent("nx", "v1".getBytes(), 30)).isTrue();
        // 已存在：不写入（written=false），原值不动
        assertThat(cache.setIfAbsent("nx", "v2".getBytes(), 30)).isFalse();
        assertThat(fake.strings.get("nx")).isEqualTo("v1".getBytes());
    }

    @Test
    void shouldPageKeysWithCursor() {
        fake.keyPages.add(new String[]{"a.1", "a.2"});
        fake.keyPages.add(new String[]{"a.3"});

        CacheKeyPage page1 = cache.keys("a.", 2, "");
        assertThat(fake.lastKeysPrefix).isEqualTo("a.");
        assertThat(fake.lastKeysLimit).isEqualTo(2);
        assertThat(page1.getKeys()).containsExactly("a.1", "a.2");
        assertThat(page1.getNextCursor()).isEqualTo("a.2");
        assertThat(page1.hasMore()).isTrue();

        CacheKeyPage page2 = cache.keys("a.", 2, page1.getNextCursor());
        assertThat(fake.lastKeysCursor).isEqualTo("a.2");
        assertThat(page2.getKeys()).containsExactly("a.3");
        assertThat(page2.hasMore()).isFalse();
    }

    @Test
    void shouldClearPrefixAndMapCounts() {
        fake.clearRemovedEntries = 5;
        fake.clearBytesReclaimed = 128;

        CacheClearResult result = cache.clear("ns.");

        assertThat(fake.lastClearPrefix).isEqualTo("ns.");
        assertThat(result.getRemovedEntries()).isEqualTo(5);
        assertThat(result.getBytesReclaimed()).isEqualTo(128);
    }

    @Test
    void shouldReturnNullOnMiss() {
        // 未命中是数据类型语义（found=false），不是错误。
        assertThat(cache.get("absent")).isNull();
    }

    @Test
    void shouldPropagatePermissionDeniedInsteadOfReturningNull() {
        // 能力拒绝必须上抛：吞成 null 会让「缺 coord:cache:read」与「键不存在」
        // 无法区分（最小权限无法验证，见 #110）。
        fake.deniedKey = "forbidden";

        assertThatThrownBy(() -> cache.get("forbidden"))
                .isInstanceOf(CoordException.class)
                .extracting(e -> ((CoordException) e).getErrorCode())
                .isEqualTo(ErrorCode.PERMISSION_DENIED);
    }

    // ──── In-process stub server ────

    /** Minimal Cache server stub with list state, mimicking agent semantics. */
    static class FakeCacheService extends CacheGrpc.CacheImplBase {
        final Map<String, java.util.List<byte[]>> lists = new LinkedHashMap<>();
        final Map<String, byte[]> strings = new LinkedHashMap<>();
        final java.util.List<String[]> keyPages = new java.util.ArrayList<>();
        String lastKeysPrefix;
        int lastKeysLimit;
        String lastKeysCursor;
        String lastClearPrefix;
        long clearRemovedEntries;
        long clearBytesReclaimed;
        String deniedKey;

        java.util.List<byte[]> list(String key) {
            return lists.getOrDefault(key, java.util.List.of());
        }

        @Override
        public void setIfAbsent(CacheSetIfAbsentRequest request,
                                StreamObserver<CacheSetIfAbsentResponse> responseObserver) {
            boolean written = !strings.containsKey(request.getKey());
            if (written) {
                strings.put(request.getKey(), request.getValue().toByteArray());
            }
            responseObserver.onNext(CacheSetIfAbsentResponse.newBuilder().setWritten(written).build());
            responseObserver.onCompleted();
        }

        @Override
        public void keys(CacheKeysRequest request, StreamObserver<CacheKeysResponse> responseObserver) {
            lastKeysPrefix = request.getPrefix();
            lastKeysLimit = request.getLimit();
            lastKeysCursor = request.getCursor();
            int pageIndex = request.getCursor().isEmpty() ? 0 : 1;
            String[] page = keyPages.get(pageIndex);
            String next = pageIndex + 1 < keyPages.size() ? page[page.length - 1] : "";
            CacheKeysResponse.Builder resp = CacheKeysResponse.newBuilder().setNextCursor(next);
            for (String k : page) {
                resp.addKeys(k);
            }
            responseObserver.onNext(resp.build());
            responseObserver.onCompleted();
        }

        @Override
        public void clear(CacheClearRequest request, StreamObserver<CacheClearResponse> responseObserver) {
            lastClearPrefix = request.getPrefix();
            responseObserver.onNext(CacheClearResponse.newBuilder()
                    .setRemovedEntries(clearRemovedEntries)
                    .setBytesReclaimed(clearBytesReclaimed)
                    .build());
            responseObserver.onCompleted();
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
            if (request.getKey().equals(deniedKey)) {
                responseObserver.onError(Status.PERMISSION_DENIED
                        .withDescription("role(s) [] do not have capability 'coord:cache:read'")
                        .asRuntimeException());
                return;
            }
            CacheGetResponse.Builder resp = CacheGetResponse.newBuilder().setFound(false);
            if ("k".equals(request.getKey())) {
                resp.setValue(ByteString.copyFromUtf8("v")).setFound(true);
            }
            responseObserver.onNext(resp.build());
            responseObserver.onCompleted();
        }
    }
}
