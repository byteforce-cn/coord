package cn.byteforce.coord.example;

import cn.byteforce.coord.contracts.kv.v2.KVGrpc;
import cn.byteforce.coord.contracts.kv.v2.Kv;
import cn.byteforce.coord.contracts.tso.v2.TSOGrpc;
import cn.byteforce.coord.contracts.tso.v2.Tso;
import cn.byteforce.coord.contracts.txn.v2.TxnGrpc;
import cn.byteforce.coord.contracts.txn.v2.TxnOuterClass;
import cn.byteforce.coord.contracts.watch.v2.WatchGrpc;
import cn.byteforce.coord.contracts.watch.v2.WatchOuterClass;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * KV-T v2 数据面示例（{@code coord.tso.v2} / {@code coord.kv.v2} /
 * {@code coord.txn.v2} / {@code coord.watch.v2}），同样只用 gRPC + proto stub。
 *
 * <p>与 v1 示例（{@link CoordClient}）的区别就是 v2 的口径：写必须带 TSO 发放的
 * 显式 {@code commit_ts}，读可以指定 {@code read_ts}（0 = 服务端 safe_ts），
 * 跨键原子性只能走 {@code coord.txn.v2}（一个 {@code commit_ts} 覆盖整个写集）。
 *
 * <p><b>为什么必须连真实 v2 集群</b>：v1 集群（{@code [kv_t].enabled=false}）上
 * v2 服务拒绝请求——直连 server 时是 {@code FAILED_PRECONDITION}，经 agent 时码被
 * 折成 {@code INTERNAL} 但保留 {@code kv_t disabled} 文案（见 {@code translate()}）；
 * v1 agent 不转发 v2 服务时是 {@code UNIMPLEMENTED}。这些情况都会让本示例以
 * 退出码 2 **硬失败**——刻意不提供"降级到 v1 API"的分支：静默降级会把
 * "v2 未验证"伪装成"v2 通过"。
 *
 * <p>运行方式见 {@code java-example/README.md}（
 * {@code bash java-example/scripts/run-v2-example.sh}）。退出码：
 * <ul>
 *   <li>0 = 全部断言通过；</li>
 *   <li>1 = 断言失败或 RPC 报错；</li>
 *   <li>2 = 目标集群不是 v2（或 agent 未转发 v2 服务）。</li>
 * </ul>
 */
public final class KvTV2Example {

    /** 目标集群不是 v2（或 agent 未转发 v2）——不是本示例可以继续的场景。 */
    private static final int EXIT_NOT_V2 = 2;
    private static final int EXIT_FAILED = 1;

    /** Watch 批的等待上限（事件在提交后异步投递，但不能无限等）。 */
    private static final long WATCH_EVENT_TIMEOUT_SECS = 15;

    private KvTV2Example() {
    }

    public static void main(String[] args) {
        String host = env("COORD_V2_AGENT_HOST", "localhost");
        int port = Integer.parseInt(env("COORD_V2_AGENT_PORT", "19637"));
        int rc;
        try {
            rc = run(host, port);
        } catch (NotV2ClusterException e) {
            System.err.println("[v2-example] FAIL (cluster is not v2): " + e.getMessage());
            System.err.println("[v2-example] start one with "
                    + "`bash scripts/ci-java-it-cluster.sh start v2` "
                    + "(agent 127.0.0.1:19637) and retry");
            rc = EXIT_NOT_V2;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("[v2-example] FAIL (interrupted): " + e);
            rc = EXIT_FAILED;
        } catch (RuntimeException e) {
            System.err.println("[v2-example] FAIL: " + e);
            rc = EXIT_FAILED;
        }
        System.exit(rc);
    }

    private static int run(String host, int port) throws InterruptedException {
        String runId = env("COORD_TEST_RUN_ID", Long.toHexString(System.nanoTime()));
        String root = "/example/kvt-v2/" + runId + "/";
        String txnPrefix = root + "txn/";
        String directKey = root + "direct/k";

        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        try {
            TSOGrpc.TSOBlockingStub tso = TSOGrpc.newBlockingStub(channel);
            KVGrpc.KVBlockingStub kv = KVGrpc.newBlockingStub(channel);
            TxnGrpc.TxnBlockingStub txn = TxnGrpc.newBlockingStub(channel);

            System.out.println("[v2-example] target agent=" + host + ":" + port + " run=" + runId);

            // ── 1. TSO：先取号，取不到就说明这套集群没有 v2 数据面 ──
            long firstTs = getTs(tso);
            long secondTs = getTs(tso);
            require(secondTs > firstTs,
                    "coord.tso.v2.GetTs must strictly increase: " + firstTs + " -> " + secondTs);

            // ── 2. coord.kv.v2：显式 commit_ts 写 + 显式 read_ts 读 ──
            long putTs = getTs(tso);
            Kv.PutResponse put = kv.put(Kv.PutRequest.newBuilder()
                    .setKey(utf8(directKey))
                    .setValue(utf8("direct-v1"))
                    .setCommitTs(putTs)
                    .build());
            require(put.getCommitTs() == putTs,
                    "Put must echo commit_ts " + putTs + ", got " + put.getCommitTs());

            Kv.KeyValue atCommit = only(kv.range(Kv.RangeRequest.newBuilder()
                    .setKey(utf8(directKey))
                    .setReadTs(putTs)
                    .build()), "range at commit_ts");
            require(text(atCommit.getValue()).equals("direct-v1"),
                    "read_ts=commit_ts must see the value just written, got "
                            + text(atCommit.getValue()));
            require(atCommit.getModRevision() == putTs && atCommit.getCreateRevision() == putTs,
                    "revision == commit_ts (mod=" + atCommit.getModRevision()
                            + ", create=" + atCommit.getCreateRevision() + ")");
            require(atCommit.getVersion() == 1, "first write must be version 1");

            // 覆盖写带 prev_kv：v2 的旧值来自 change record，不依赖 Region 历史。
            long overwriteTs = getTs(tso);
            Kv.PutResponse overwrite = kv.put(Kv.PutRequest.newBuilder()
                    .setKey(utf8(directKey))
                    .setValue(utf8("direct-v2"))
                    .setCommitTs(overwriteTs)
                    .setPrevKv(true)
                    .build());
            require(overwrite.getPrevKv().getValue().equals(utf8("direct-v1")),
                    "prev_kv must carry the overwritten value");
            require(overwrite.getPrevKv().getModRevision() == putTs,
                    "prev_kv.mod_revision must be the previous commit_ts");
            System.out.println("[v2-example] kv.v2 put/range ok: commit_ts=" + overwriteTs
                    + " mod_revision=" + overwriteTs + " prev_mod_revision=" + putTs);

            // ── 3. coord.watch.v2：先订阅，再提交事务，读同一批事件 ──
            long watchBase = getTs(tso);
            WatchProbe watch = WatchProbe.open(channel, txnPrefix, watchBase + 1);
            try {
                // ── 4. coord.txn.v2：Begin -> Read -> Commit（两个 key 同一 commit_ts） ──
                TxnOuterClass.BeginResponse begin = txn.begin(TxnOuterClass.BeginRequest
                        .newBuilder()
                        .build());
                require(begin.getStartTs() > 0, "Begin must allocate a start_ts");
                TxnOuterClass.ReadResponse read = txn.read(TxnOuterClass.ReadRequest
                        .newBuilder()
                        .setTxnId(begin.getTxnId())
                        .setKey(utf8(txnPrefix + "a"))
                        .build());
                require(read.getReadTs() == begin.getStartTs(),
                        "Txn Read must read at start_ts: " + read.getReadTs()
                                + " != " + begin.getStartTs());

                TxnOuterClass.CommitResponse commit = txn.commit(TxnOuterClass.CommitRequest
                        .newBuilder()
                        .setTxnId(begin.getTxnId())
                        .addMutations(putMutation(txnPrefix + "a", "txn-a"))
                        .addMutations(putMutation(txnPrefix + "b", "txn-b"))
                        .build());
                long commitTs = commit.getCommitTs();
                require(commitTs > begin.getStartTs(),
                        "commit_ts must be greater than start_ts: " + commitTs);

                // 读集/写集在 commit_ts 上一致可见：区间读用同一个 read_ts。
                Kv.RangeResponse rangeAtCommit = kv.range(Kv.RangeRequest.newBuilder()
                        .setKey(utf8(txnPrefix))
                        .setRangeEnd(PrefixScan.end(txnPrefix))
                        .setReadTs(commitTs)
                        .build());
                require(rangeAtCommit.getKvsCount() == 2,
                        "range at commit_ts must return the 2 txn keys, got "
                                + rangeAtCommit.getKvsCount());
                for (Kv.KeyValue stored : rangeAtCommit.getKvsList()) {
                    require(stored.getModRevision() == commitTs,
                            "every key of one txn shares commit_ts: "
                                    + text(stored.getKey()) + " mod=" + stored.getModRevision());
                    require(stored.getCreateRevision() == commitTs,
                            "create_revision == commit_ts");
                    require(stored.getVersion() == 1, "first write must be version 1");
                }
                System.out.println("[v2-example] txn.v2 begin/read/commit ok: start_ts="
                        + begin.getStartTs() + " commit_ts=" + commitTs
                        + " keys=" + keysOf(rangeAtCommit.getKvsList()));

                // 同一个事务在 Watch 里是**一个**原子批，commit_ts 与事务一致。
                WatchOuterClass.WatchEvent event = watch.awaitEvent(commitTs);
                List<String> watchedKeys = new ArrayList<>();
                for (WatchOuterClass.Change change : event.getChangesList()) {
                    watchedKeys.add(text(change.getKey()));
                }
                require(watchedKeys.size() == 2,
                        "one txn must be one Watch event with 2 changes, got " + watchedKeys);
                require(watchedKeys.contains(txnPrefix + "a") && watchedKeys.contains(txnPrefix + "b"),
                        "Watch event keys " + watchedKeys + " must be the txn write set");
                watch.cancel();
                System.out.println("[v2-example] watch.v2 create/event/cancel ok: commit_ts="
                        + event.getCommitTs() + " keys=" + watchedKeys);

                // safe_ts 追平后，read_ts=0（服务端 safe_ts）也能读到同一份数据。
                Kv.KeyValue atSafeTs = only(kv.range(Kv.RangeRequest.newBuilder()
                        .setKey(utf8(txnPrefix + "a"))
                        .build()), "range at safe_ts");
                require(text(atSafeTs.getValue()).equals("txn-a"),
                        "read_ts=0 must serve safe_ts >= commit_ts, got "
                                + text(atSafeTs.getValue()));
            } finally {
                watch.close();
            }

            System.out.println("[v2-example] PASS: coord.tso.v2 / coord.kv.v2 / "
                    + "coord.txn.v2 / coord.watch.v2 all verified on a real v2 cluster");
            return 0;
        } finally {
            channel.shutdownNow();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /** 取一个 TSO 时间戳；v2 未开启/未交付在这里就会硬失败。 */
    private static long getTs(TSOGrpc.TSOBlockingStub tso) {
        try {
            return tso.getTs(Tso.GetTsRequest.newBuilder().build()).getTs();
        } catch (StatusRuntimeException e) {
            throw translate("coord.tso.v2.GetTs", e);
        }
    }

    /**
     * 走到 v2 服务却拿到「未开启 / 未交付 / 未实现」时，说明目标集群不是 v2：
     * 这是配置面的事实，不是可重试的传输错误，因此单独成为退出码 2。
     *
     * <p>结构化码优先（直连 server 或 agent 保留原码时是
     * `FAILED_PRECONDITION`/`UNIMPLEMENTED`）。经 agent 时 `FailedPrecondition`
     * 会被折成 `INTERNAL`（见 `coord-agent` 的 `map_core_error` 兜底分支），此时只剩
     * 服务端的稳定诊断文案——`coord-server/src/kvt/mod.rs` 的
     * `kv_t disabled: [kv_t].enabled=false` 与 `kv_t enabled but ... is not
     * implemented yet`；它们是运维/测试共同依赖的锚点，不是临时措辞。
     */
    private static RuntimeException translate(String rpc, StatusRuntimeException e) {
        Status.Code code = e.getStatus().getCode();
        String description = String.valueOf(e.getStatus().getDescription());
        if (code == Status.Code.FAILED_PRECONDITION
                || code == Status.Code.UNIMPLEMENTED
                || code == Status.Code.NOT_FOUND
                || looksLikeV2Disabled(description)) {
            return new NotV2ClusterException(rpc + " -> " + code + ": " + description);
        }
        return new IllegalStateException(rpc + " failed: " + e.getStatus(), e);
    }

    private static boolean looksLikeV2Disabled(String description) {
        return description.contains("kv_t disabled")
                || description.contains("is not implemented yet");
    }

    private static TxnOuterClass.Mutation putMutation(String key, String value) {
        return TxnOuterClass.Mutation.newBuilder()
                .setOp(TxnOuterClass.Mutation.Op.PUT)
                .setKey(utf8(key))
                .setValue(utf8(value))
                .build();
    }

    private static Kv.KeyValue only(Kv.RangeResponse response, String what) {
        require(response.getKvsCount() == 1,
                what + " must return exactly 1 key, got " + response.getKvsCount());
        return response.getKvs(0);
    }

    private static List<String> keysOf(List<Kv.KeyValue> kvs) {
        List<String> keys = new ArrayList<>();
        for (Kv.KeyValue kv : kvs) {
            keys.add(text(kv.getKey()));
        }
        return keys;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    private static ByteString utf8(String value) {
        return ByteString.copyFrom(value, StandardCharsets.UTF_8);
    }

    private static String text(ByteString value) {
        return value.toString(StandardCharsets.UTF_8);
    }

    private static String env(String name, String fallback) {
        String raw = System.getenv(name);
        return (raw == null || raw.isBlank()) ? fallback : raw.trim();
    }

    /** 目标集群不是 v2（`[kv_t].enabled=false` 或 agent 未转发 v2 服务）。 */
    static final class NotV2ClusterException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        NotV2ClusterException(String message) {
            super(message);
        }
    }

    /**
     * `coord.watch.v2` 的一条订阅：create → 收批 → cancel。
     *
     * <p>{@code start_revision} 取一个**尚不存在**的 revision：流建立晚于提交时，
     * 服务端仍会从该位点回放，因此不需要用 sleep 去赌 create 帧先到。也正因为它
     * 停在未来位点上，服务端**不会**先回一个空 ack——所以这里不等待首帧，
     * 直接等目标 {@code commit_ts} 的那一批，{@code watch_id} 从响应里取。
     */
    private static final class WatchProbe {

        private final AtomicReference<Throwable> error = new AtomicReference<>();
        private final AtomicReference<Long> watchId = new AtomicReference<>();
        private final List<WatchOuterClass.WatchEvent> events = new ArrayList<>();
        private final StreamObserver<WatchOuterClass.WatchRequest> requests;
        private final long startRevision;

        private WatchProbe(StreamObserver<WatchOuterClass.WatchRequest> requests, long startRevision) {
            this.requests = requests;
            this.startRevision = startRevision;
        }

        static WatchProbe open(ManagedChannel channel, String prefix, long startRevision) {
            AtomicReference<WatchProbe> probe = new AtomicReference<>();
            StreamObserver<WatchOuterClass.WatchRequest> requests =
                    WatchGrpc.newStub(channel).watch(new StreamObserver<>() {
                        @Override
                        public void onNext(WatchOuterClass.WatchResponse response) {
                            WatchProbe current = probe.get();
                            if (current != null) {
                                current.onResponse(response);
                            }
                        }

                        @Override
                        public void onError(Throwable t) {
                            WatchProbe current = probe.get();
                            if (current != null) {
                                current.onStreamError(t);
                            }
                        }

                        @Override
                        public void onCompleted() {
                            WatchProbe current = probe.get();
                            if (current != null) {
                                current.onStreamCompleted();
                            }
                        }
                    });
            WatchProbe created = new WatchProbe(requests, startRevision);
            probe.set(created);
            requests.onNext(WatchOuterClass.WatchRequest.newBuilder()
                    .setCreate(WatchOuterClass.WatchCreateRequest.newBuilder()
                            .setKey(utf8(prefix))
                            .setRangeEnd(PrefixScan.end(prefix))
                            .setStartRevision(startRevision)
                            .setPrevKv(true)
                            .setFilter(WatchOuterClass.EventFilter.ALL)
                            .build())
                    .build());
            return created;
        }

        private synchronized void onResponse(WatchOuterClass.WatchResponse response) {
            watchId.compareAndSet(null, response.getWatchId());
            events.addAll(response.getEventsList());
            notifyAll();
        }

        private void onStreamError(Throwable t) {
            error.set(t);
            synchronized (this) {
                notifyAll();
            }
        }

        private synchronized void onStreamCompleted() {
            notifyAll();
        }

        /** 等到 commit_ts 对应的那一个原子批。 */
        synchronized WatchOuterClass.WatchEvent awaitEvent(long commitTs) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WATCH_EVENT_TIMEOUT_SECS);
            while (true) {
                for (WatchOuterClass.WatchEvent event : events) {
                    if (event.getCommitTs() == commitTs) {
                        return event;
                    }
                }
                failOnError();
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    throw new IllegalStateException(
                            "watch delivered no batch with commit_ts=" + commitTs
                                    + " within " + WATCH_EVENT_TIMEOUT_SECS + "s (start_revision="
                                    + startRevision + ")");
                }
                TimeUnit.NANOSECONDS.timedWait(this, remaining);
            }
        }

        /** 显式 cancel（`watch_id` 来自已收到的响应），随后关闭请求流。 */
        void cancel() {
            Long id = watchId.get();
            if (id != null) {
                requests.onNext(WatchOuterClass.WatchRequest.newBuilder()
                        .setCancel(WatchOuterClass.WatchCancelRequest.newBuilder()
                                .setWatchId(id)
                                .build())
                        .build());
            }
            requests.onCompleted();
        }

        void close() {
            try {
                requests.onCompleted();
            } catch (RuntimeException ignored) {
                // 流已因错误终止：关流本身没有更多信息可给。
            }
        }

        private void failOnError() {
            Throwable t = error.get();
            if (t != null) {
                if (t instanceof StatusRuntimeException sre) {
                    throw translate("coord.watch.v2.Watch", sre);
                }
                throw new IllegalStateException("coord.watch.v2 stream failed: " + t, t);
            }
        }
    }
}
