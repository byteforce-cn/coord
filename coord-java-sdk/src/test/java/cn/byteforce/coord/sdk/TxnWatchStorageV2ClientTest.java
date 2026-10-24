package cn.byteforce.coord.sdk;

import cn.byteforce.coord.contracts.kv.v2.Kv;
import cn.byteforce.coord.contracts.storage.v2.StorageGrpc;
import cn.byteforce.coord.contracts.storage.v2.StorageOuterClass;
import cn.byteforce.coord.contracts.txn.v2.TxnGrpc;
import cn.byteforce.coord.contracts.txn.v2.TxnOuterClass;
import cn.byteforce.coord.contracts.watch.v2.WatchGrpc;
import cn.byteforce.coord.contracts.watch.v2.WatchOuterClass;
import cn.byteforce.coord.sdk.storage.v2.StorageClient;
import cn.byteforce.coord.sdk.txn.v2.TxnClient;
import cn.byteforce.coord.sdk.watch.v2.WatchClient;
import com.google.protobuf.ByteString;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * KV-T v2 transaction/watch/object SDK surface against a real Netty gRPC server.
 * <p>
 * The fake services are the agent-side contract: they emit protocol codes in the
 * {@code x-coord-error-code} trailer, so trailer mapping, retry classification
 * and stream reconnection are exercised end to end (no handshake needed).
 */
class TxnWatchStorageV2ClientTest {

    private static final Metadata.Key<String> ERROR_CODE =
            Metadata.Key.of("x-coord-error-code", Metadata.ASCII_STRING_MARSHALLER);

    private Server server;
    private CoordClient client;
    private FakeTxn txn;
    private FakeWatch watch;
    private FakeStorage storage;

    @BeforeEach
    void setUp() throws IOException {
        txn = new FakeTxn();
        watch = new FakeWatch();
        storage = new FakeStorage();
        server = NettyServerBuilder.forPort(0)
                .addService(txn)
                .addService(watch)
                .addService(storage)
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

    // ──── txn ────

    @Test
    void txnBeginReadCommitRoundTrip() {
        TxnClient.Session session = client.txnV2().begin();

        assertThat(text(session.txnId())).isEqualTo("t-1");
        assertThat(session.startTs()).isEqualTo(120);

        TxnClient.ReadResult read = session.read(bytes("k"), bytes("k0"), 10, false);
        assertThat(read.readTs()).isEqualTo(120);
        assertThat(read.kvs()).hasSize(1);
        assertThat(text(read.kvs().get(0).value())).isEqualTo("v");

        TxnClient.CommitResult commit = session.commit(
                List.of(TxnClient.Mutation.put(bytes("k"), bytes("v2"))), bytes("req-1"));
        assertThat(commit.committed()).isTrue();
        assertThat(commit.commitTs()).isEqualTo(140);
        assertThat(txn.lastCommit.getMutationsCount()).isEqualTo(1);
        assertThat(txn.lastCommit.getMutations(0).getOp())
                .isEqualTo(TxnOuterClass.Mutation.Op.PUT);
    }

    @Test
    void txnConflictIsReturnedAsAResultNotAnError() {
        TxnClient.Session session = client.txnV2().begin();
        txn.conflict = true;

        TxnClient.CommitResult commit = session.commit(
                List.of(TxnClient.Mutation.delete(bytes("k"))), bytes("req-2"));

        assertThat(commit.committed()).isFalse();
        // 冲突不写决策：没有 commit_ts 可用。
        assertThat(commit.commitTs()).isZero();
        assertThat(txn.commitCalls.get()).isEqualTo(1);
    }

    @Test
    void txnAbortOfAnExpiredSessionIsIdempotent() {
        TxnClient.Session session = client.txnV2().begin();
        txn.abortNotFound = true;

        session.abort();

        assertThat(txn.abortCalls.get()).isEqualTo(1);
    }

    @Test
    void txnCommitRetriesEpochStaleWithTheSameRequestId() {
        TxnClient.Session session = client.txnV2().begin();
        txn.epochStaleOnce = true;

        TxnClient.CommitResult commit = session.commit(
                List.of(TxnClient.Mutation.put(bytes("k"), bytes("v"))), bytes("req-3"));

        assertThat(commit.committed()).isTrue();
        assertThat(txn.commitCalls.get()).isEqualTo(2);
        // 幂等键必须原样重放：换键会把一次重试变成两个决策。
        assertThat(text(txn.commitRequestIds.get(0))).isEqualTo("req-3");
        assertThat(text(txn.commitRequestIds.get(1))).isEqualTo("req-3");
    }

    // ──── watch ────

    @Test
    void watchDeliversBatchesAndSkipsAckOnlyFrames() throws Exception {
        BlockingQueue<WatchClient.Batch> batches = new LinkedBlockingQueue<>();
        watch.script = attempt -> {
            attempt.send(WatchOuterClass.WatchResponse.newBuilder()
                    .setCursor(cursor(1, 40))
                    .setWatermark(40)
                    .build());
            attempt.send(WatchOuterClass.WatchResponse.newBuilder()
                    .addEvents(WatchOuterClass.WatchEvent.newBuilder()
                            .setCommitTs(50)
                            .addChanges(WatchOuterClass.Change.newBuilder()
                                    .setType(WatchOuterClass.Change.Type.PUT)
                                    .setKey(ByteString.copyFrom(bytes("k")))
                                    .setValue(ByteString.copyFrom(bytes("v")))
                                    .build()))
                    .setCursor(cursor(1, 50))
                    .setWatermark(50)
                    .setSealed(true)
                    .build());
        };

        WatchClient.Subscription sub = client.watchV2().watch(
                WatchClient.Options.key(bytes("k")).fromRevision(30),
                listener(batches, new LinkedBlockingQueue<>()));

        WatchClient.Batch batch = batches.poll(5, TimeUnit.SECONDS);
        assertThat(batch).isNotNull();
        assertThat(batch.events()).hasSize(1);
        assertThat(batch.events().get(0).commitTs()).isEqualTo(50);
        assertThat(text(batch.events().get(0).changes().get(0).value())).isEqualTo("v");
        assertThat(batch.watermark()).isEqualTo(50);
        assertThat(batch.sealed()).isTrue();
        // ack-only 帧只推进游标，不投给调用方。
        assertThat(batches.poll(300, TimeUnit.MILLISECONDS)).isNull();
        assertThat(sub.cursor()).isEqualTo(new WatchClient.Cursor(1, 50));

        assertThat(watch.creates.get(0).getStartRevision()).isEqualTo(30);
        assertThat(watch.creates.get(0).hasStartCursor()).isFalse();
        sub.close();
    }

    @Test
    void watchReconnectsFromTheLastCursor() throws Exception {
        BlockingQueue<WatchClient.Batch> batches = new LinkedBlockingQueue<>();
        watch.script = attempt -> {
            if (attempt.number == 1) {
                attempt.send(WatchOuterClass.WatchResponse.newBuilder()
                        .addEvents(WatchOuterClass.WatchEvent.newBuilder()
                                .setCommitTs(50)
                                .addChanges(WatchOuterClass.Change.newBuilder()
                                        .setType(WatchOuterClass.Change.Type.PUT)
                                        .setKey(ByteString.copyFrom(bytes("k")))
                                        .setValue(ByteString.copyFrom(bytes("v1")))
                                        .build()))
                        .setCursor(cursor(1, 50))
                        .build());
                // 断线：传输层可恢复错误。
                attempt.fail(error(Status.UNAVAILABLE.withDescription("agent restarting"),
                        "UNAVAILABLE"));
                return;
            }
            attempt.send(WatchOuterClass.WatchResponse.newBuilder()
                    .addEvents(WatchOuterClass.WatchEvent.newBuilder()
                            .setCommitTs(60)
                            .addChanges(WatchOuterClass.Change.newBuilder()
                                    .setType(WatchOuterClass.Change.Type.DELETE)
                                    .setKey(ByteString.copyFrom(bytes("k")))
                                    .build()))
                    .setCursor(cursor(1, 60))
                    .build());
        };

        WatchClient.Subscription sub = client.watchV2().watch(
                WatchClient.Options.key(bytes("k")),
                listener(batches, new LinkedBlockingQueue<>()));

        assertThat(batches.poll(5, TimeUnit.SECONDS).events().get(0).commitTs()).isEqualTo(50);
        assertThat(batches.poll(5, TimeUnit.SECONDS).events().get(0).commitTs()).isEqualTo(60);

        assertThat(watch.creates).hasSize(2);
        // 重连不是"从当前水位重来"：必须带已消费到的精确游标（至少一次投递）。
        WatchOuterClass.WatchCreateRequest resume = watch.creates.get(1);
        assertThat(resume.getStartRevision()).isZero();
        assertThat(resume.hasStartCursor()).isTrue();
        assertThat(resume.getStartCursor().getShardId()).isEqualTo(1);
        assertThat(resume.getStartCursor().getCommitTs()).isEqualTo(50);
        sub.close();
    }

    @Test
    void watchHistoryUnavailableIsTerminal() throws Exception {
        BlockingQueue<CoordException> errors = new LinkedBlockingQueue<>();
        watch.script = attempt -> attempt.fail(error(
                Status.OUT_OF_RANGE.withDescription("compacted"), "HISTORY_UNAVAILABLE"));

        BlockingQueue<WatchClient.Batch> batches = new LinkedBlockingQueue<>();
        WatchClient.Subscription sub = client.watchV2().watch(
                WatchClient.Options.key(bytes("k")).fromRevision(1),
                listener(batches, errors));

        CoordException err = errors.poll(5, TimeUnit.SECONDS);
        assertThat(err).isNotNull();
        assertThat(err.getErrorCode()).isEqualTo(ErrorCode.HISTORY_UNAVAILABLE);
        // 保留窗口已回收：重连只会永远失败，不得自动重试。
        assertThat(watch.attempts.get()).isEqualTo(1);
        sub.close();
    }

    @Test
    void watchCloseStopsTheStream() throws Exception {
        BlockingQueue<WatchClient.Batch> batches = new LinkedBlockingQueue<>();
        watch.script = attempt -> attempt.send(WatchOuterClass.WatchResponse.newBuilder()
                .addEvents(WatchOuterClass.WatchEvent.newBuilder()
                        .setCommitTs(50)
                        .addChanges(WatchOuterClass.Change.newBuilder()
                                .setType(WatchOuterClass.Change.Type.PUT)
                                .setKey(ByteString.copyFrom(bytes("k")))
                                .setValue(ByteString.copyFrom(bytes("v")))
                                .build()))
                .setCursor(cursor(1, 50))
                .build());

        WatchClient.Subscription sub = client.watchV2().watch(
                WatchClient.Options.key(bytes("k")),
                listener(batches, new LinkedBlockingQueue<>()));
        assertThat(batches.poll(5, TimeUnit.SECONDS)).isNotNull();

        sub.close();
        sub.close();

        assertThat(watch.cancelled.await(5, TimeUnit.SECONDS)).isTrue();
        // 关闭后不得再有重连/投递。
        Thread.sleep(300);
        assertThat(watch.attempts.get()).isEqualTo(1);
        assertThat(batches.poll()).isNull();
    }

    // ──── storage ────

    @Test
    void storagePutGetStatDeleteRoundTrip() {
        byte[] payload = bytes("0123456789");

        StorageClient.PutResult put = client.storageV2().put("bucket", bytes("obj"), payload, 4);
        assertThat(put.commitTs()).isEqualTo(200);
        assertThat(put.size()).isEqualTo(10);
        assertThat(put.chunks()).isEqualTo(3);
        assertThat(storage.lastDeclaredSize).isEqualTo(10);

        StorageClient.ObjectData data = client.storageV2().get("bucket", bytes("obj"), 0);
        assertThat(data.data()).isEqualTo(payload);
        assertThat(data.stat().commitTs()).isEqualTo(200);
        assertThat(data.stat().committed()).isTrue();

        StorageClient.ObjectStat stat = client.storageV2().stat("bucket", bytes("obj"), 0);
        assertThat(stat.exists()).isTrue();
        assertThat(stat.size()).isEqualTo(10);

        StorageClient.DeleteResult deleted =
                client.storageV2().delete("bucket", bytes("obj"), bytes("req-9"));
        assertThat(deleted.deleted()).isTrue();
        assertThat(deleted.commitTs()).isEqualTo(210);
    }

    @Test
    void storageStatOfAMissingObjectIsAZeroStat() {
        StorageClient.ObjectStat stat =
                client.storageV2().stat("bucket", bytes("absent"), 0);

        assertThat(stat.exists()).isFalse();
        assertThat(stat.commitTs()).isZero();
        assertThat(text(stat.objectId())).isEqualTo("absent");
    }

    @Test
    void storageRejectsAnEmptyObjectBeforeSendingIt() {
        CoordException err = catchThrowableOfType(
                () -> client.storageV2().put("bucket", bytes("obj"), new byte[0]),
                CoordException.class);

        assertThat(err.getErrorCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT);
        assertThat(storage.putCalls.get()).isZero();
    }

    // ──── helpers ────

    private static WatchClient.Listener listener(BlockingQueue<WatchClient.Batch> batches,
                                                 BlockingQueue<CoordException> errors) {
        return new WatchClient.Listener() {
            @Override
            public void onBatch(WatchClient.Batch batch) {
                batches.add(batch);
            }

            @Override
            public void onError(CoordException error) {
                errors.add(error);
            }
        };
    }

    private static WatchOuterClass.WatchCursor cursor(long shardId, long commitTs) {
        return WatchOuterClass.WatchCursor.newBuilder()
                .setShardId(shardId)
                .setCommitTs(commitTs)
                .build();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String text(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }

    private static StatusRuntimeException error(Status status, String code) {
        Metadata metadata = new Metadata();
        metadata.put(ERROR_CODE, code);
        return status.asRuntimeException(metadata);
    }

    private static final class FakeTxn extends TxnGrpc.TxnImplBase {
        final AtomicInteger commitCalls = new AtomicInteger();
        final AtomicInteger abortCalls = new AtomicInteger();
        final List<byte[]> commitRequestIds = new CopyOnWriteArrayList<>();
        volatile TxnOuterClass.CommitRequest lastCommit;
        volatile boolean conflict;
        volatile boolean abortNotFound;
        volatile boolean epochStaleOnce;

        @Override
        public void begin(TxnOuterClass.BeginRequest request,
                          StreamObserver<TxnOuterClass.BeginResponse> observer) {
            observer.onNext(TxnOuterClass.BeginResponse.newBuilder()
                    .setTxnId(ByteString.copyFrom(bytes("t-1")))
                    .setStartTs(120)
                    .build());
            observer.onCompleted();
        }

        @Override
        public void read(TxnOuterClass.ReadRequest request,
                         StreamObserver<TxnOuterClass.ReadResponse> observer) {
            observer.onNext(TxnOuterClass.ReadResponse.newBuilder()
                    .addKvs(Kv.KeyValue.newBuilder()
                            .setKey(request.getKey())
                            .setValue(ByteString.copyFrom(bytes("v")))
                            .setCreateRevision(120)
                            .setModRevision(120)
                            .setVersion(1)
                            .build())
                    .setReadTs(120)
                    .build());
            observer.onCompleted();
        }

        @Override
        public void commit(TxnOuterClass.CommitRequest request,
                           StreamObserver<TxnOuterClass.CommitResponse> observer) {
            int call = commitCalls.incrementAndGet();
            lastCommit = request;
            commitRequestIds.add(request.getRequestId().toByteArray());
            if (conflict) {
                observer.onError(error(Status.ABORTED.withDescription("read set changed"),
                        "TXN_CONFLICT"));
                return;
            }
            if (epochStaleOnce && call == 1) {
                observer.onError(error(Status.ABORTED.withDescription("stale epoch"),
                        "EPOCH_STALE"));
                return;
            }
            observer.onNext(TxnOuterClass.CommitResponse.newBuilder()
                    .setCommitTs(140)
                    .build());
            observer.onCompleted();
        }

        @Override
        public void abort(TxnOuterClass.AbortRequest request,
                          StreamObserver<TxnOuterClass.AbortResponse> observer) {
            abortCalls.incrementAndGet();
            if (abortNotFound) {
                observer.onError(error(Status.ABORTED.withDescription("session gone"),
                        "TXN_NOT_FOUND"));
                return;
            }
            observer.onNext(TxnOuterClass.AbortResponse.getDefaultInstance());
            observer.onCompleted();
        }
    }

    @FunctionalInterface
    private interface WatchScript {
        void run(WatchAttempt attempt);
    }

    private static final class WatchAttempt {
        final int number;
        final StreamObserver<WatchOuterClass.WatchResponse> observer;

        WatchAttempt(int number, StreamObserver<WatchOuterClass.WatchResponse> observer) {
            this.number = number;
            this.observer = observer;
        }

        void send(WatchOuterClass.WatchResponse response) {
            observer.onNext(response);
        }

        void fail(StatusRuntimeException error) {
            observer.onError(error);
        }
    }

    private static final class FakeWatch extends WatchGrpc.WatchImplBase {
        final AtomicInteger attempts = new AtomicInteger();
        final List<WatchOuterClass.WatchCreateRequest> creates = new CopyOnWriteArrayList<>();
        final CountDownLatch cancelled = new CountDownLatch(1);
        volatile WatchScript script = attempt -> { };

        @Override
        public StreamObserver<WatchOuterClass.WatchRequest> watch(
                StreamObserver<WatchOuterClass.WatchResponse> observer) {
            return new StreamObserver<>() {
                @Override
                public void onNext(WatchOuterClass.WatchRequest request) {
                    if (!request.hasCreate()) {
                        return;
                    }
                    creates.add(request.getCreate());
                    int number = attempts.incrementAndGet();
                    script.run(new WatchAttempt(number, observer));
                }

                @Override
                public void onError(Throwable t) {
                    cancelled.countDown();
                }

                @Override
                public void onCompleted() {
                    // 客户端保持请求流打开；half-close 也视为取消。
                    cancelled.countDown();
                }
            };
        }
    }

    private static final class FakeStorage extends StorageGrpc.StorageImplBase {
        final AtomicInteger putCalls = new AtomicInteger();
        volatile long lastDeclaredSize;

        @Override
        public StreamObserver<StorageOuterClass.PutRequest> put(
                StreamObserver<StorageOuterClass.PutResponse> observer) {
            return new StreamObserver<>() {
                private final List<byte[]> chunks = new ArrayList<>();
                private long declared;

                @Override
                public void onNext(StorageOuterClass.PutRequest request) {
                    if (request.hasMeta()) {
                        putCalls.incrementAndGet();
                        declared = request.getMeta().getTotalSize();
                        lastDeclaredSize = declared;
                    } else if (request.hasChunk()) {
                        chunks.add(request.getChunk().toByteArray());
                    }
                }

                @Override
                public void onError(Throwable t) {
                    observer.onError(t);
                }

                @Override
                public void onCompleted() {
                    long size = chunks.stream().mapToLong(c -> c.length).sum();
                    if (declared >= 0 && declared != size) {
                        observer.onError(error(Status.INVALID_ARGUMENT
                                .withDescription("declared size mismatch"), "INVALID_ARGUMENT"));
                        return;
                    }
                    observer.onNext(StorageOuterClass.PutResponse.newBuilder()
                            .setCommitTs(200)
                            .setSize(size)
                            .setChunks(chunks.size())
                            .build());
                    observer.onCompleted();
                }
            };
        }

        @Override
        public void get(StorageOuterClass.GetRequest request,
                        StreamObserver<StorageOuterClass.GetResponse> observer) {
            byte[] payload = bytes("0123456789");
            observer.onNext(StorageOuterClass.GetResponse.newBuilder()
                    .setStat(StorageOuterClass.ObjectStat.newBuilder()
                            .setBucket(request.getBucket())
                            .setObjectId(request.getObjectId())
                            .setSize(payload.length)
                            .setChunks(3)
                            .setCommitTs(200)
                            .setCreateTs(200)
                            .setExists(true)
                            .setCommitted(true)
                            .build())
                    .build());
            for (int off = 0; off < payload.length; off += 4) {
                int len = Math.min(4, payload.length - off);
                observer.onNext(StorageOuterClass.GetResponse.newBuilder()
                        .setChunk(ByteString.copyFrom(payload, off, len))
                        .build());
            }
            observer.onCompleted();
        }

        @Override
        public void stat(StorageOuterClass.StatRequest request,
                         StreamObserver<StorageOuterClass.StatResponse> observer) {
            boolean present = !new String(request.getObjectId().toByteArray(),
                    StandardCharsets.UTF_8).equals("absent");
            StorageOuterClass.ObjectStat stat = StorageOuterClass.ObjectStat.newBuilder()
                    .setBucket(request.getBucket())
                    .setObjectId(request.getObjectId())
                    .setExists(present)
                    .setCommitted(present)
                    .setSize(present ? 10 : 0)
                    .setChunks(present ? 3 : 0)
                    .setCommitTs(present ? 200 : 0)
                    .setCreateTs(present ? 200 : 0)
                    .build();
            observer.onNext(StorageOuterClass.StatResponse.newBuilder().setStat(stat).build());
            observer.onCompleted();
        }

        @Override
        public void delete(StorageOuterClass.DeleteRequest request,
                           StreamObserver<StorageOuterClass.DeleteResponse> observer) {
            observer.onNext(StorageOuterClass.DeleteResponse.newBuilder()
                    .setDeleted(true)
                    .setCommitTs(210)
                    .build());
            observer.onCompleted();
        }
    }
}
