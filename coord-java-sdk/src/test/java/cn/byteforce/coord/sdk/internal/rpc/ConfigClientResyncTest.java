package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.contracts.config.v1.ConfigGrpc;
import cn.byteforce.coord.contracts.config.v1.ConfigListRequest;
import cn.byteforce.coord.contracts.config.v1.ConfigListResponse;
import cn.byteforce.coord.contracts.config.v1.ConfigPutRequest;
import cn.byteforce.coord.contracts.config.v1.ConfigPutResponse;
import cn.byteforce.coord.contracts.config.v1.ConfigWatchEvent;
import cn.byteforce.coord.contracts.config.v1.ConfigWatchRequest;
import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.config.ConfigClient;
import cn.byteforce.coord.sdk.config.ConfigEvent;
import cn.byteforce.coord.sdk.config.ConfigSnapshot;
import cn.byteforce.coord.sdk.config.ConfigWatchSubscription;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.sdk.internal.thread.ThreadPoolManager;
import cn.byteforce.coord.sdk.internal.watch.WatchManager;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Config 快照 + 位点续订（resync）面的契约测试（#46/#47）。
 *
 * <p>判据：请求侧位点字段真实送达；响应侧删除事件（presence 缺省）映射为
 * {@code Optional.empty()}；快照 revision 与 put revision 逐字段透出。
 */
class ConfigClientResyncTest {

    private Server server;
    private ManagedChannel channel;
    private ThreadPoolManager threadPoolManager;
    private WatchManager watchManager;
    private final FakeConfig fake = new FakeConfig();
    private ConfigClient client;
    private ConfigWatchSubscription subscription;

    @BeforeEach
    void setUp() throws Exception {
        server = ServerBuilder.forPort(0).addService(fake).build().start();
        channel = ManagedChannelBuilder.forAddress("localhost", server.getPort())
                .usePlaintext()
                .build();

        AgentChannelManager channelManager = mock(AgentChannelManager.class);
        when(channelManager.getChannel()).thenReturn(channel);

        threadPoolManager = new ThreadPoolManager(2);
        // 测试内不需要自动重连（fake 流不会断开），关掉以保持终结路径简单
        watchManager = new WatchManager(threadPoolManager, false);
        client = new ConfigClientImpl(
                channelManager,
                new ErrorMapper(),
                new RetryTemplate(),
                new ObservabilityProvider() {
                },
                CoordConfig.builder().agentHost("localhost").build(),
                watchManager);
    }

    @AfterEach
    void tearDown() {
        if (subscription != null) {
            subscription.close();
        }
        channel.shutdownNow();
        server.shutdownNow();
        threadPoolManager.close();
    }

    @Test
    void listWithRevisionMapsSnapshotAndPosition() {
        fake.listRevision = 42L;
        fake.entries.put("app.a", "1");
        fake.entries.put("app.b", "2");

        ConfigSnapshot snapshot = client.listWithRevision("app.");

        assertThat(fake.lastListPrefix).isEqualTo("app.");
        assertThat(snapshot.getRevision()).isEqualTo(42L);
        assertThat(snapshot.getEntries()).containsEntry("app.a", "1").containsEntry("app.b", "2");
    }

    @Test
    void putReturningRevisionExposesGlobalPosition() {
        fake.putRevision = 77L;

        long revision = client.putReturningRevision("k", "v");

        assertThat(fake.lastPutKey).isEqualTo("k");
        assertThat(revision).isEqualTo(77L);
    }

    @Test
    void watchFromResumesAfterStartRevisionAndMapsDeletePresence() throws Exception {
        CountDownLatch eventLatch = new CountDownLatch(1);
        AtomicReference<ConfigEvent> received = new AtomicReference<>();

        subscription = client.watchFrom("app.", 42L, event -> {
            received.set(event);
            eventLatch.countDown();
        });

        // 请求侧：首连位点 = startRevision + 1（与断线续订同一语义：
        // 位点及其之前的内容不重放——它们已在快照 / 已处理集合里）
        assertThat(fake.watchRequestLatch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(fake.lastWatchStartRevision).isEqualTo(43L);
        assertThat(fake.lastWatchPrefix).isEqualTo("app.");

        // 响应侧：fake 下发一条「无 new_value」的事件 = 删除
        assertThat(eventLatch.await(5, TimeUnit.SECONDS)).isTrue();
        ConfigEvent event = received.get();
        assertThat(event.getKey()).isEqualTo("app.k");
        assertThat(event.getNewValue()).isEmpty(); // presence 缺省 = 删除
        assertThat(event.getRevision()).isEqualTo(43L);
    }

    // ──── in-process stub server ────

    /** Minimal Config server stub matching the agent's wire behavior. */
    static class FakeConfig extends ConfigGrpc.ConfigImplBase {
        volatile long listRevision;
        final Map<String, String> entries = new LinkedHashMap<>();
        volatile String lastListPrefix;
        volatile long putRevision;
        volatile String lastPutKey;
        volatile String lastWatchPrefix;
        volatile long lastWatchStartRevision;
        final CountDownLatch watchRequestLatch = new CountDownLatch(1);

        @Override
        public void list(ConfigListRequest req, StreamObserver<ConfigListResponse> o) {
            lastListPrefix = req.getPrefix();
            o.onNext(ConfigListResponse.newBuilder()
                    .putAllEntries(entries)
                    .setRevision(listRevision)
                    .build());
            o.onCompleted();
        }

        @Override
        public void put(ConfigPutRequest req, StreamObserver<ConfigPutResponse> o) {
            lastPutKey = req.getKey();
            o.onNext(ConfigPutResponse.newBuilder().setRevision(putRevision).build());
            o.onCompleted();
        }

        @Override
        public void watch(ConfigWatchRequest req, StreamObserver<ConfigWatchEvent> o) {
            lastWatchPrefix = req.getPrefix();
            lastWatchStartRevision = req.getStartRevision();
            watchRequestLatch.countDown();
            // 删除事件：new_value 缺省（presence 语义）
            o.onNext(ConfigWatchEvent.newBuilder()
                    .setKey("app.k")
                    .setRevision(43L)
                    .build());
            // 刻意不 onCompleted：订阅保持活着（与真实服务端一致）
        }
    }
}
