package cn.byteforce.coord.example;

import com.google.protobuf.ByteString;
import coord.kv.KVGrpc;
import coord.kv.Kv;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;

import java.util.concurrent.TimeUnit;

/**
 * KV 集成测试 — TDD RED 阶段
 *
 * 验证 Java 应用通过 gRPC 连接本地 Agent 的 KV 操作全路径:
 * Put → Range → Delete → Range(verify)
 *
 * 前置条件: 本地 Agent 已启动 (coord agent --agent-addr 127.0.0.1:19527)
 */
@DisplayName("KV Integration Tests (Java → Agent gRPC)")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class KvIntegrationTest {

    private static ManagedChannel channel;
    private static KVGrpc.KVBlockingStub kvStub;

    @BeforeAll
    static void setUp() {
        AgentEndpoint.requireReachable();
        channel = ManagedChannelBuilder
                .forAddress(AgentEndpoint.host(), AgentEndpoint.port())
                .usePlaintext()
                .keepAliveTime(30, TimeUnit.SECONDS)
                .keepAliveTimeout(10, TimeUnit.SECONDS)
                .build();
        kvStub = KVGrpc.newBlockingStub(channel);
    }

    @AfterAll
    static void tearDown() throws InterruptedException {
        if (channel != null) {
            channel.shutdown();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    @Order(1)
    @DisplayName("Put a single key and verify via Range")
    void testPutAndRangeSingleKey() {
        // 每次运行唯一 key：断言 version == 1 需要该 key 在本集群上从未出现过
        // （dev 集群会被反复复用，写死 key 会让第二次运行就变红）。
        String key = Namespace.unique("/test/kv/hello/");
        String value = "world";

        Kv.PutResponse putResp = kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8(value))
                .build());
        assertThat(putResp.getRevision()).isGreaterThan(0);

        Kv.RangeResponse rangeResp = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .build());
        assertThat(rangeResp.getKvsCount()).isEqualTo(1);
        Kv.KeyValue kv = rangeResp.getKvs(0);
        assertThat(kv.getKey().toStringUtf8()).isEqualTo(key);
        assertThat(kv.getValue().toStringUtf8()).isEqualTo(value);
        assertThat(kv.getVersion()).isEqualTo(1);
    }

    @Test
    @Order(2)
    @DisplayName("Put with prev_kv returns previous value")
    void testPutWithPrevKv() {
        String key = "/test/kv/prev-kv-test";
        String v1 = "first";
        String v2 = "second";

        kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8(v1))
                .build());

        Kv.PutResponse r2 = kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8(v2))
                .setPrevKv(true)
                .build());

        assertThat(r2.hasPrevKv()).isTrue();
        assertThat(r2.getPrevKv().getValue().toStringUtf8()).isEqualTo(v1);
        assertThat(r2.getPrevKv().getVersion()).isEqualTo(1);
    }

    @Test
    @Order(3)
    @DisplayName("Delete a key and verify it's gone")
    void testDeleteAndVerify() {
        String key = "/test/kv/to-delete";
        String value = "delete-me";

        kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8(value))
                .build());

        Kv.DeleteResponse delResp = kvStub.delete(Kv.DeleteRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .build());
        assertThat(delResp).isNotNull();

        Kv.RangeResponse rangeResp = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .build());
        assertThat(rangeResp.getKvsCount()).isEqualTo(0);
    }

    @Test
    @Order(4)
    @DisplayName("Range with prefix (range_end = key + 1)")
    void testRangeWithPrefix() {
        String prefix = "/test/kv/prefix/";
        String[] keys = {prefix + "a", prefix + "b", prefix + "c"};

        for (int i = 0; i < keys.length; i++) {
            kvStub.put(Kv.PutRequest.newBuilder()
                    .setKey(ByteString.copyFromUtf8(keys[i]))
                    .setValue(ByteString.copyFromUtf8("val-" + i))
                    .build());
        }

        ByteString keyBytes = ByteString.copyFromUtf8(prefix);
        // 前缀扫描：range_end = 末字节 +1（`prefix + "\0"` 只匹配 prefix 本身）
        ByteString rangeEnd = PrefixScan.end(prefix);

        Kv.RangeResponse resp = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(keyBytes)
                .setRangeEnd(rangeEnd)
                .build());

        assertThat(resp.getKvsCount()).isEqualTo(3);
    }

    @Test
    @Order(5)
    @DisplayName("Range with keys_only returns keys without values")
    void testRangeKeysOnly() {
        String key = "/test/kv/keys-only";
        String value = "secret";

        kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8(value))
                .build());

        Kv.RangeResponse resp = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setKeysOnly(true)
                .build());

        assertThat(resp.getKvsCount()).isEqualTo(1);
        Kv.KeyValue kv = resp.getKvs(0);
        assertThat(kv.getKey().toStringUtf8()).isEqualTo(key);
        assertThat(kv.getValue().isEmpty()).isTrue();
    }
}
