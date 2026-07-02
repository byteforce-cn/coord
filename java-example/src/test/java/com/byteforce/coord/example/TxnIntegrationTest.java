package cn.byteforce.coord.example;

import com.google.protobuf.ByteString;
import coord.kv.KVGrpc;
import coord.kv.Kv;
import coord.txn.TxnGrpc;
import coord.txn.TxnOuterClass;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;

import java.util.concurrent.TimeUnit;

/**
 * Txn 集成测试 — 原子事务
 *
 * 覆盖:
 * - Compare version EQUAL → success
 * - Compare version NOT_EQUAL → failure
 * - Compare value EQUAL
 * - Compare mod_revision
 * - 多条件 AND 语义
 * - 多操作事务（Put + Delete + Range）
 * - 空条件事务（直接执行）
 * - 幂等事务（request_id）
 *
 * @implNote 当前 TxnProxy 返回占位响应（succeeded=false）。
 *           待 coord-agent proxy.rs 中 TxnProxy 实现完整的 Txn 转发后启用。
 */
@DisplayName("Txn Integration Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TxnIntegrationTest {

    private static ManagedChannel channel;
    private static KVGrpc.KVBlockingStub kvStub;
    private static TxnGrpc.TxnBlockingStub txnStub;

    @BeforeAll
    static void setUp() {
        channel = ManagedChannelBuilder
                .forAddress("localhost", 19527)
                .usePlaintext()
                .keepAliveTime(30, TimeUnit.SECONDS)
                .build();
        kvStub = KVGrpc.newBlockingStub(channel);
        txnStub = TxnGrpc.newBlockingStub(channel);
    }

    @AfterAll
    static void tearDown() throws InterruptedException {
        if (channel != null) {
            channel.shutdown();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    // 辅助方法
    private void put(String key, String value) {
        kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8(value))
                .build());
    }

    private Kv.KeyValue get(String key) {
        Kv.RangeResponse resp = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .build());
        return resp.getKvsCount() > 0 ? resp.getKvs(0) : null;
    }

    // ──── Compare version EQUAL → success ────

    @Test
    @Order(1)
    @DisplayName("CAS: compare version EQUAL → success branch executes")
    void testCasVersionEqualSuccess() {
        String key = "/test/txn/cas-version";
        put(key, "v1");
        Kv.KeyValue kv = get(key);
        assertThat(kv).isNotNull();

        TxnOuterClass.TxnResponse resp = txnStub.txn(TxnOuterClass.TxnRequest.newBuilder()
                .addCompare(TxnOuterClass.Compare.newBuilder()
                        .setResult(TxnOuterClass.Compare.CompareResult.EQUAL)
                        .setTarget(TxnOuterClass.Compare.Target.VERSION)
                        .setKey(ByteString.copyFromUtf8(key))
                        .setVersion(kv.getVersion())
                        .build())
                .addSuccess(TxnOuterClass.RequestOp.newBuilder()
                        .setRequestPut(Kv.PutRequest.newBuilder()
                                .setKey(ByteString.copyFromUtf8(key))
                                .setValue(ByteString.copyFromUtf8("v2"))
                                .build())
                        .build())
                .addFailure(TxnOuterClass.RequestOp.newBuilder()
                        .setRequestPut(Kv.PutRequest.newBuilder()
                                .setKey(ByteString.copyFromUtf8(key))
                                .setValue(ByteString.copyFromUtf8("should-not-write"))
                                .build())
                        .build())
                .build());

        assertThat(resp.getSucceeded()).isTrue();
        assertThat(get(key).getValue().toStringUtf8()).isEqualTo("v2");
    }

    @Test
    @Order(2)
    @DisplayName("CAS: compare version NOT_EQUAL → failure branch executes")
    void testCasVersionNotEqualFailure() {
        String key = "/test/txn/cas-version-fail";
        put(key, "v1");

        // 用错误的 version 做 CAS
        TxnOuterClass.TxnResponse resp = txnStub.txn(TxnOuterClass.TxnRequest.newBuilder()
                .addCompare(TxnOuterClass.Compare.newBuilder()
                        .setResult(TxnOuterClass.Compare.CompareResult.EQUAL)
                        .setTarget(TxnOuterClass.Compare.Target.VERSION)
                        .setKey(ByteString.copyFromUtf8(key))
                        .setVersion(999) // wrong version
                        .build())
                .addSuccess(TxnOuterClass.RequestOp.newBuilder()
                        .setRequestPut(Kv.PutRequest.newBuilder()
                                .setKey(ByteString.copyFromUtf8(key))
                                .setValue(ByteString.copyFromUtf8("should-not-write"))
                                .build())
                        .build())
                .addFailure(TxnOuterClass.RequestOp.newBuilder()
                        .setRequestPut(Kv.PutRequest.newBuilder()
                                .setKey(ByteString.copyFromUtf8(key))
                                .setValue(ByteString.copyFromUtf8("fallback-value"))
                                .build())
                        .build())
                .build());

        assertThat(resp.getSucceeded()).isFalse();
        assertThat(get(key).getValue().toStringUtf8()).isEqualTo("fallback-value");
    }

    // ──── Compare value EQUAL ────

    @Test
    @Order(3)
    @DisplayName("CAS: compare value EQUAL succeeds")
    void testCasValueEqual() {
        String key = "/test/txn/cas-value";
        put(key, "target-value");

        TxnOuterClass.TxnResponse resp = txnStub.txn(TxnOuterClass.TxnRequest.newBuilder()
                .addCompare(TxnOuterClass.Compare.newBuilder()
                        .setResult(TxnOuterClass.Compare.CompareResult.EQUAL)
                        .setTarget(TxnOuterClass.Compare.Target.VALUE)
                        .setKey(ByteString.copyFromUtf8(key))
                        .setValue(ByteString.copyFromUtf8("target-value"))
                        .build())
                .addSuccess(TxnOuterClass.RequestOp.newBuilder()
                        .setRequestPut(Kv.PutRequest.newBuilder()
                                .setKey(ByteString.copyFromUtf8(key))
                                .setValue(ByteString.copyFromUtf8("updated"))
                                .build())
                        .build())
                .build());

        assertThat(resp.getSucceeded()).isTrue();
        assertThat(get(key).getValue().toStringUtf8()).isEqualTo("updated");
    }

    @Test
    @Order(4)
    @DisplayName("CAS: compare value NOT_EQUAL fails")
    void testCasValueNotEqual() {
        String key = "/test/txn/cas-value-ne";
        put(key, "actual");

        TxnOuterClass.TxnResponse resp = txnStub.txn(TxnOuterClass.TxnRequest.newBuilder()
                .addCompare(TxnOuterClass.Compare.newBuilder()
                        .setResult(TxnOuterClass.Compare.CompareResult.EQUAL)
                        .setTarget(TxnOuterClass.Compare.Target.VALUE)
                        .setKey(ByteString.copyFromUtf8(key))
                        .setValue(ByteString.copyFromUtf8("wrong-value"))
                        .build())
                .addSuccess(TxnOuterClass.RequestOp.newBuilder()
                        .setRequestPut(Kv.PutRequest.newBuilder()
                                .setKey(ByteString.copyFromUtf8(key))
                                .setValue(ByteString.copyFromUtf8("should-not-write"))
                                .build())
                        .build())
                .build());

        assertThat(resp.getSucceeded()).isFalse();
        assertThat(get(key).getValue().toStringUtf8()).isEqualTo("actual");
    }

    // ──── 多条件 AND ────

    @Test
    @Order(5)
    @DisplayName("Multiple compares: all must pass for success")
    void testMultiCompareAnd() {
        String key1 = "/test/txn/multi-and-1";
        String key2 = "/test/txn/multi-and-2";
        put(key1, "v1");
        put(key2, "v2");

        TxnOuterClass.TxnResponse resp = txnStub.txn(TxnOuterClass.TxnRequest.newBuilder()
                .addCompare(TxnOuterClass.Compare.newBuilder()
                        .setResult(TxnOuterClass.Compare.CompareResult.EQUAL)
                        .setTarget(TxnOuterClass.Compare.Target.VALUE)
                        .setKey(ByteString.copyFromUtf8(key1))
                        .setValue(ByteString.copyFromUtf8("v1"))
                        .build())
                .addCompare(TxnOuterClass.Compare.newBuilder()
                        .setResult(TxnOuterClass.Compare.CompareResult.EQUAL)
                        .setTarget(TxnOuterClass.Compare.Target.VALUE)
                        .setKey(ByteString.copyFromUtf8(key2))
                        .setValue(ByteString.copyFromUtf8("v2"))
                        .build())
                .addSuccess(TxnOuterClass.RequestOp.newBuilder()
                        .setRequestPut(Kv.PutRequest.newBuilder()
                                .setKey(ByteString.copyFromUtf8(key1))
                                .setValue(ByteString.copyFromUtf8("updated"))
                                .build())
                        .build())
                .build());

        assertThat(resp.getSucceeded()).isTrue();
    }

    @Test
    @Order(6)
    @DisplayName("Multiple compares: any one fails → failure branch")
    void testMultiCompareOneFails() {
        String key1 = "/test/txn/multi-fail-1";
        String key2 = "/test/txn/multi-fail-2";
        put(key1, "v1");
        put(key2, "v2");

        TxnOuterClass.TxnResponse resp = txnStub.txn(TxnOuterClass.TxnRequest.newBuilder()
                .addCompare(TxnOuterClass.Compare.newBuilder()
                        .setResult(TxnOuterClass.Compare.CompareResult.EQUAL)
                        .setTarget(TxnOuterClass.Compare.Target.VALUE)
                        .setKey(ByteString.copyFromUtf8(key1))
                        .setValue(ByteString.copyFromUtf8("v1")) // passes
                        .build())
                .addCompare(TxnOuterClass.Compare.newBuilder()
                        .setResult(TxnOuterClass.Compare.CompareResult.EQUAL)
                        .setTarget(TxnOuterClass.Compare.Target.VALUE)
                        .setKey(ByteString.copyFromUtf8(key2))
                        .setValue(ByteString.copyFromUtf8("WRONG")) // fails
                        .build())
                .addSuccess(TxnOuterClass.RequestOp.newBuilder()
                        .setRequestDelete(Kv.DeleteRequest.newBuilder()
                                .setKey(ByteString.copyFromUtf8(key1))
                                .build())
                        .build())
                .addFailure(TxnOuterClass.RequestOp.newBuilder()
                        .setRequestPut(Kv.PutRequest.newBuilder()
                                .setKey(ByteString.copyFromUtf8("failure-marker"))
                                .setValue(ByteString.copyFromUtf8("yes"))
                                .build())
                        .build())
                .build());

        assertThat(resp.getSucceeded()).isFalse();
        // key1 should NOT be deleted (success not executed)
        assertThat(get(key1)).isNotNull();
        // failure-marker should exist
        assertThat(get("failure-marker").getValue().toStringUtf8()).isEqualTo("yes");
    }

    // ──── 多操作事务 ────

    @Test
    @Order(7)
    @DisplayName("Txn with mixed operations: Put + Delete + Range")
    void testTxnMixedOperations() {
        String putKey = "/test/txn/mixed-put";
        String delKey = "/test/txn/mixed-del";
        put(delKey, "to-delete");

        TxnOuterClass.TxnResponse resp = txnStub.txn(TxnOuterClass.TxnRequest.newBuilder()
                .addSuccess(TxnOuterClass.RequestOp.newBuilder()
                        .setRequestPut(Kv.PutRequest.newBuilder()
                                .setKey(ByteString.copyFromUtf8(putKey))
                                .setValue(ByteString.copyFromUtf8("new-val"))
                                .build())
                        .build())
                .addSuccess(TxnOuterClass.RequestOp.newBuilder()
                        .setRequestDelete(Kv.DeleteRequest.newBuilder()
                                .setKey(ByteString.copyFromUtf8(delKey))
                                .build())
                        .build())
                .addSuccess(TxnOuterClass.RequestOp.newBuilder()
                        .setRequestRange(Kv.RangeRequest.newBuilder()
                                .setKey(ByteString.copyFromUtf8(putKey))
                                .build())
                        .build())
                .build());

        assertThat(resp.getSucceeded()).isTrue();
        assertThat(resp.getResponsesCount()).isEqualTo(3);

        // 验证 put 生效
        assertThat(get(putKey).getValue().toStringUtf8()).isEqualTo("new-val");
        // 验证 delete 生效
        assertThat(get(delKey)).isNull();
    }

    // ──── 空事务 ────

    @Test
    @Order(8)
    @DisplayName("Empty txn (no compares) succeeds and executes success ops")
    void testEmptyTxn() {
        String key = "/test/txn/empty";
        TxnOuterClass.TxnResponse resp = txnStub.txn(TxnOuterClass.TxnRequest.newBuilder()
                .addSuccess(TxnOuterClass.RequestOp.newBuilder()
                        .setRequestPut(Kv.PutRequest.newBuilder()
                                .setKey(ByteString.copyFromUtf8(key))
                                .setValue(ByteString.copyFromUtf8("direct"))
                                .build())
                        .build())
                .build());

        assertThat(resp.getSucceeded()).isTrue();
        assertThat(resp.getResponsesCount()).isEqualTo(1);
        assertThat(get(key).getValue().toStringUtf8()).isEqualTo("direct");
    }

    // ──── 幂等事务 ────

    @Test
    @Order(9)
    @DisplayName("Txn with request_id is idempotent")
    void testTxnIdempotent() {
        String key = "/test/txn/idempotent";
        ByteString reqId = ByteString.copyFromUtf8("txn-idem-001");

        TxnOuterClass.TxnResponse r1 = txnStub.txn(TxnOuterClass.TxnRequest.newBuilder()
                .setRequestId(reqId)
                .addSuccess(TxnOuterClass.RequestOp.newBuilder()
                        .setRequestPut(Kv.PutRequest.newBuilder()
                                .setKey(ByteString.copyFromUtf8(key))
                                .setValue(ByteString.copyFromUtf8("first"))
                                .build())
                        .build())
                .build());
        long rev1 = r1.getRevision();

        // 相同 request_id 再次执行
        TxnOuterClass.TxnResponse r2 = txnStub.txn(TxnOuterClass.TxnRequest.newBuilder()
                .setRequestId(reqId)
                .addSuccess(TxnOuterClass.RequestOp.newBuilder()
                        .setRequestPut(Kv.PutRequest.newBuilder()
                                .setKey(ByteString.copyFromUtf8(key))
                                .setValue(ByteString.copyFromUtf8("different"))
                                .build())
                        .build())
                .build());
        assertThat(r2.getRevision()).isEqualTo(rev1);
        assertThat(get(key).getValue().toStringUtf8()).isEqualTo("first");
    }
}
