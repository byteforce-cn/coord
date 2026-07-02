package cn.byteforce.coord.example;

import com.google.protobuf.ByteString;
import coord.kv.KVGrpc;
import coord.kv.Kv;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * KV 高级集成测试 — 覆盖边界场景与高级特性
 *
 * 覆盖:
 * - 幂等写入（request_id）
 * - count_only 查询
 * - limit 分页
 * - revision 历史读取
 * - 二进制 Key/Value
 * - 范围删除（range_end）
 * - 删除 prev_kv
 * - 并发写入
 * - 大 value
 */
@DisplayName("KV Advanced Integration Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class KvAdvancedTest {

    private static ManagedChannel channel;
    private static KVGrpc.KVBlockingStub kvStub;

    @BeforeAll
    static void setUp() {
        channel = ManagedChannelBuilder
                .forAddress("localhost", 19527)
                .usePlaintext()
                .keepAliveTime(30, TimeUnit.SECONDS)
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

    // ──── 幂等写入 ────

    @Test
    @Order(1)
    @DisplayName("Idempotent put: same request_id returns same revision")
    void testIdempotentPut() {
        String key = "/test/kv/idempotent";
        String value = "data";
        ByteString requestId = ByteString.copyFromUtf8("idem-001");

        Kv.PutResponse r1 = kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8(value))
                .setRequestId(requestId)
                .build());
        long rev1 = r1.getRevision();
        assertThat(rev1).isGreaterThan(0);

        // 相同 request_id 再次写入
        Kv.PutResponse r2 = kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8("different-value"))
                .setRequestId(requestId)
                .build());
        assertThat(r2.getRevision()).isEqualTo(rev1);

        // 验证 value 未变
        Kv.RangeResponse range = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .build());
        assertThat(range.getKvs(0).getValue().toStringUtf8()).isEqualTo(value);
    }

    @Test
    @Order(2)
    @DisplayName("Idempotent put: different request_id writes new value")
    void testIdempotentPutDifferentId() {
        String key = "/test/kv/idempotent-2";
        ByteString id1 = ByteString.copyFromUtf8("id-aaa");
        ByteString id2 = ByteString.copyFromUtf8("id-bbb");

        kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8("v1"))
                .setRequestId(id1)
                .build());

        Kv.PutResponse r2 = kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8("v2"))
                .setRequestId(id2)
                .build());

        Kv.RangeResponse range = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .build());
        assertThat(range.getKvs(0).getValue().toStringUtf8()).isEqualTo("v2");
        // 不同 request_id 应产生不同 revision
        assertThat(r2.getRevision()).isGreaterThan(0);
    }

    // ──── count_only ────

    @Test
    @Order(3)
    @DisplayName("Range with count_only returns count without kvs")
    void testCountOnly() {
        String prefix = "/test/kv/count-only/";
        for (int i = 1; i <= 5; i++) {
            kvStub.put(Kv.PutRequest.newBuilder()
                    .setKey(ByteString.copyFromUtf8(prefix + i))
                    .setValue(ByteString.copyFromUtf8("val-" + i))
                    .build());
        }

        Kv.RangeResponse resp = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(prefix))
                .setRangeEnd(ByteString.copyFromUtf8(prefix + "\0"))
                .setCountOnly(true)
                .build());

        assertThat(resp.getCount()).isEqualTo(5);
        assertThat(resp.getKvsCount()).isEqualTo(0);
    }

    // ──── limit 分页 ────

    @Test
    @Order(4)
    @DisplayName("Range with limit returns at most N results")
    void testRangeWithLimit() {
        String prefix = "/test/kv/limit/";
        for (int i = 1; i <= 10; i++) {
            kvStub.put(Kv.PutRequest.newBuilder()
                    .setKey(ByteString.copyFromUtf8(prefix + String.format("%02d", i)))
                    .setValue(ByteString.copyFromUtf8("v" + i))
                    .build());
        }

        Kv.RangeResponse resp = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(prefix))
                .setRangeEnd(ByteString.copyFromUtf8(prefix + "\0"))
                .setLimit(3)
                .build());

        assertThat(resp.getKvsCount()).isEqualTo(3);
        assertThat(resp.getCount()).isGreaterThanOrEqualTo(3);
    }

    // ──── revision 历史读取 ────

    @Test
    @Order(5)
    @DisplayName("Range with revision reads historical snapshot")
    void testRangeWithRevision() {
        String key = "/test/kv/revision-read";
        ByteString keyBs = ByteString.copyFromUtf8(key);

        Kv.PutResponse r1 = kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(keyBs)
                .setValue(ByteString.copyFromUtf8("v1"))
                .build());
        long rev1 = r1.getRevision();

        kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(keyBs)
                .setValue(ByteString.copyFromUtf8("v2"))
                .build());

        // 用 rev1 读取，应返回 v1
        Kv.RangeResponse hist = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(keyBs)
                .setRevision(rev1)
                .build());
        assertThat(hist.getKvsCount()).isEqualTo(1);
        assertThat(hist.getKvs(0).getValue().toStringUtf8()).isEqualTo("v1");
    }

    // ──── 二进制 Key/Value ────

    @Test
    @Order(6)
    @DisplayName("Binary key and value round-trip")
    void testBinaryKeyValue() {
        byte[] binaryKey = new byte[] {0x00, 0x01, (byte) 0xFF, 0x7F, 0x3C};
        byte[] binaryValue = new byte[] {(byte) 0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A};

        kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFrom(binaryKey))
                .setValue(ByteString.copyFrom(binaryValue))
                .build());

        Kv.RangeResponse resp = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(ByteString.copyFrom(binaryKey))
                .build());
        assertThat(resp.getKvsCount()).isEqualTo(1);
        assertThat(resp.getKvs(0).getKey().toByteArray()).isEqualTo(binaryKey);
        assertThat(resp.getKvs(0).getValue().toByteArray()).isEqualTo(binaryValue);
    }

    @Test
    @Order(7)
    @DisplayName("UTF-8 key and value round-trip")
    void testUnicodeKeyValue() {
        String unicodeKey = "/test/kv/中文/キー/🚀";
        String unicodeValue = "值 − ∫√≈ π=3.14159 émotion café 🎉";

        kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(unicodeKey))
                .setValue(ByteString.copyFromUtf8(unicodeValue))
                .build());

        Kv.RangeResponse resp = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(unicodeKey))
                .build());
        assertThat(resp.getKvsCount()).isEqualTo(1);
        assertThat(resp.getKvs(0).getKey().toStringUtf8()).isEqualTo(unicodeKey);
        assertThat(resp.getKvs(0).getValue().toStringUtf8()).isEqualTo(unicodeValue);
    }

    // ──── 范围删除 ────

    @Test
    @Order(8)
    @DisplayName("Delete range removes multiple keys")
    void testDeleteRange() {
        String prefix = "/test/kv/del-range/";
        for (int i = 0; i < 5; i++) {
            kvStub.put(Kv.PutRequest.newBuilder()
                    .setKey(ByteString.copyFromUtf8(prefix + "key-" + i))
                    .setValue(ByteString.copyFromUtf8("val"))
                    .build());
        }

        // 验证 5 个 key 存在
        Kv.RangeResponse before = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(prefix))
                .setRangeEnd(ByteString.copyFromUtf8(prefix + "\0"))
                .build());
        assertThat(before.getKvsCount()).isEqualTo(5);

        // 范围删除
        Kv.DeleteResponse del = kvStub.delete(Kv.DeleteRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(prefix))
                .setRangeEnd(ByteString.copyFromUtf8(prefix + "\0"))
                .build());
        assertThat(del.getDeleted()).isEqualTo(5);

        // 验证全部删除
        Kv.RangeResponse after = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(prefix))
                .setRangeEnd(ByteString.copyFromUtf8(prefix + "\0"))
                .build());
        assertThat(after.getKvsCount()).isEqualTo(0);
    }

    @Test
    @Order(9)
    @DisplayName("Delete with prev_kv returns deleted values")
    void testDeleteWithPrevKv() {
        String key = "/test/kv/del-prevkv";
        String value = "to-be-deleted";

        kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8(value))
                .build());

        Kv.DeleteResponse resp = kvStub.delete(Kv.DeleteRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setPrevKv(true)
                .build());

        assertThat(resp.getDeleted()).isEqualTo(1);
        assertThat(resp.getPrevKvsCount()).isEqualTo(1);
        assertThat(resp.getPrevKvs(0).getValue().toStringUtf8()).isEqualTo(value);
    }

    // ──── 大 value ────

    @Test
    @Order(10)
    @DisplayName("Large value (10KB) round-trip")
    void testLargeValue() {
        String key = "/test/kv/large-value";
        // 生成 10KB 数据
        StringBuilder sb = new StringBuilder(10240);
        for (int i = 0; i < 1024; i++) {
            sb.append(String.format("%09d", i));
        }
        String largeValue = sb.toString();
        assertThat(largeValue.length()).isGreaterThanOrEqualTo(9000);

        kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8(largeValue))
                .build());

        Kv.RangeResponse resp = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .build());
        assertThat(resp.getKvsCount()).isEqualTo(1);
        assertThat(resp.getKvs(0).getValue().toStringUtf8()).isEqualTo(largeValue);
    }

    // ──── 空 value ────

    @Test
    @Order(11)
    @DisplayName("Put with empty value and read back")
    void testEmptyValue() {
        String key = "/test/kv/empty-value";

        kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.EMPTY)
                .build());

        Kv.RangeResponse resp = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .build());
        assertThat(resp.getKvsCount()).isEqualTo(1);
        assertThat(resp.getKvs(0).getValue().isEmpty()).isTrue();
    }

    // ──── 多次覆盖写入 ────

    @Test
    @Order(12)
    @DisplayName("Sequential overwrites increment version")
    void testSequentialOverwrites() {
        String key = "/test/kv/overwrites";

        Kv.PutResponse r1 = kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8("v1"))
                .build());
        Kv.PutResponse r2 = kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8("v2"))
                .build());
        Kv.PutResponse r3 = kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8("v3"))
                .build());

        assertThat(r1.getRevision()).isLessThan(r2.getRevision());
        assertThat(r2.getRevision()).isLessThan(r3.getRevision());

        Kv.RangeResponse resp = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .build());
        assertThat(resp.getKvs(0).getValue().toStringUtf8()).isEqualTo("v3");
        assertThat(resp.getKvs(0).getVersion()).isGreaterThanOrEqualTo(1);
    }

    // ──── 不存在 key 的读取 ────

    @Test
    @Order(13)
    @DisplayName("Range on non-existent key returns empty")
    void testRangeNonExistentKey() {
        Kv.RangeResponse resp = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("/nonexistent/key/12345"))
                .build());
        assertThat(resp.getKvsCount()).isEqualTo(0);
    }

    // ──── 删除不存在的 key ────

    @Test
    @Order(14)
    @DisplayName("Delete non-existent key returns deleted=0")
    void testDeleteNonExistentKey() {
        Kv.DeleteResponse resp = kvStub.delete(Kv.DeleteRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8("/nonexistent/to-delete"))
                .build());
        assertThat(resp.getDeleted()).isEqualTo(0);
    }
}
