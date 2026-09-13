package cn.byteforce.coord.example;

import com.google.protobuf.ByteString;
import coord.kv.KVGrpc;
import coord.kv.Kv;
import coord.lease.LeaseGrpc;
import coord.lease.LeaseOuterClass;
import coord.maintenance.MaintenanceGrpc;
import coord.maintenance.MaintenanceOuterClass;
import coord.watch.WatchGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Coord Agent 客户端封装 — Java 应用接入 Coord 的推荐入口。
 *
 * 提供简化的 KV / Lease / Watch / Txn / Maintenance 操作 API，
 * 封装 gRPC stub 的创建和管理。
 *
 * 用法:
 * <pre>{@code
 *   CoordClient client = CoordClient.connect("localhost", 19527);
 *   client.put("/app/config", "value");
 *   String val = client.get("/app/config");
 *   client.close();
 * }</pre>
 *
 * 与架构文档 §9.2 一致：Java 应用只依赖标准 gRPC + proto stub，无需 Coord 专用 SDK。
 */
public class CoordClient implements AutoCloseable {

    /** 默认 KeepAlive 心跳间隔（秒）。租约 TTL 的 1/3 是仓库惯例。 */
    public static final long DEFAULT_KEEPALIVE_INTERVAL_SECS = 10;

    private final ManagedChannel channel;
    private final KVGrpc.KVBlockingStub kvStub;
    private final LeaseGrpc.LeaseBlockingStub leaseStub;
    private final LeaseGrpc.LeaseStub leaseAsyncStub;
    private final MaintenanceGrpc.MaintenanceBlockingStub maintenanceStub;
    /**
     * KeepAlive 心跳调度器（daemon）。
     *
     * <p>daemon = true 是必须的：非 daemon 平台线程会让忘记调用 {@link #close()}
     * 的进程**永远退不出**（这正是第四轮 §3.14.4 指出的 `ThreadPoolManager` 问题）。
     */
    private final java.util.concurrent.ScheduledExecutorService keepAliveScheduler;

    private CoordClient(String host, int port) {
        this.channel = ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext()
                .keepAliveTime(30, TimeUnit.SECONDS)
                .keepAliveTimeout(10, TimeUnit.SECONDS)
                .build();
        this.kvStub = KVGrpc.newBlockingStub(channel);
        this.leaseStub = LeaseGrpc.newBlockingStub(channel);
        this.leaseAsyncStub = LeaseGrpc.newStub(channel);
        this.maintenanceStub = MaintenanceGrpc.newBlockingStub(channel);
        this.keepAliveScheduler = java.util.concurrent.Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "coord-example-keepalive");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * 创建连接本地 Agent 的客户端。
     *
     * @param host Agent 地址（通常 localhost）
     * @param port Agent gRPC 端口（默认 19527）
     */
    public static CoordClient connect(String host, int port) {
        return new CoordClient(host, port);
    }

    /**
     * 创建连接本地 Agent 的客户端（使用默认端口 19527）。
     */
    public static CoordClient connectToLocalAgent() {
        return new CoordClient("localhost", 19527);
    }

    // ──── KV API ────

    /**
     * 写入键值对。
     *
     * @return 写入后的全局 revision
     */
    public long put(String key, String value) {
        Kv.PutResponse resp = kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8(value))
                .build());
        return resp.getRevision();
    }

    /**
     * 写入键值对并绑定 Lease。
     */
    public long put(String key, String value, long leaseId) {
        Kv.PutResponse resp = kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8(value))
                .setLeaseId(leaseId)
                .build());
        return resp.getRevision();
    }

    /**
     * 写入键值对并返回旧值。
     *
     * @return 旧值（如存在），否则 null
     */
    public String putWithPrevKv(String key, String value) {
        Kv.PutResponse resp = kvStub.put(Kv.PutRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8(value))
                .setPrevKv(true)
                .build());
        if (resp.hasPrevKv()) {
            return resp.getPrevKv().getValue().toStringUtf8();
        }
        return null;
    }

    /**
     * 精确读取单个 key 的值。
     *
     * @return 值（如存在），否则 null
     */
    public String get(String key) {
        Kv.RangeResponse resp = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .build());
        if (resp.getKvsCount() > 0) {
            return resp.getKvs(0).getValue().toStringUtf8();
        }
        return null;
    }

    /**
     * 前缀扫描。
     *
     * <p><b>第四轮 §3.14.6 修复</b>：此前这里写
     * {@code ByteString.copyFromUtf8(prefix + "\0")}——{@code prefix + "\0"} 是
     * {@code prefix} 的**最小后继**（0x00 是最小字节），区间
     * {@code [prefix, prefix + "\0")} 只包含 {@code prefix} 这一个 key，
     * 因此**匹配 0 条**。受害者是它上面的两个调用方：
     * {@code ConfigClient.getAll()} 永远返回空 map，
     * {@code ServiceRegistry.discover()} 永远返回空列表。
     *
     * <p>正确做法是把**最后一个字节 +1**（与 `PrefixScan` / Rust 侧
     * {@code prefix_end()} 同一口径），见 {@link PrefixScan#end(String)}。
     *
     * @param prefix Key 前缀
     * @return 匹配的键值对列表
     */
    public List<Kv.KeyValue> scan(String prefix) {
        ByteString prefixBytes = ByteString.copyFromUtf8(prefix);
        ByteString rangeEnd = PrefixScan.end(prefixBytes);

        Kv.RangeResponse resp = kvStub.range(Kv.RangeRequest.newBuilder()
                .setKey(prefixBytes)
                .setRangeEnd(rangeEnd)
                .build());
        return resp.getKvsList();
    }

    /**
     * 删除单个 key。
     */
    public void delete(String key) {
        kvStub.delete(Kv.DeleteRequest.newBuilder()
                .setKey(ByteString.copyFromUtf8(key))
                .build());
    }

    // ──── Lease API ────

    /**
     * 创建一个 Lease。
     *
     * @param ttlSeconds TTL（秒）
     * @return Lease ID
     */
    public long grantLease(long ttlSeconds) {
        LeaseOuterClass.LeaseGrantResponse resp = leaseStub.leaseGrant(
                LeaseOuterClass.LeaseGrantRequest.newBuilder().setTtl(ttlSeconds).build());
        return resp.getId();
    }

    /**
     * 撤销 Lease（绑定该 Lease 的所有 key 自动删除）。
     */
    public void revokeLease(long leaseId) {
        leaseStub.leaseRevoke(LeaseOuterClass.LeaseRevokeRequest.newBuilder()
                .setId(leaseId).build());
    }

    /**
     * 创建 KeepAlive 流并按间隔**持续**发送心跳（默认 10s）。
     *
     * <p><b>第四轮 §3.14.6 修复</b>：此前本方法只发**一帧**就返回，而 javadoc
     * 声称"异步维持 Lease 心跳"——名字与行为不符（心跳停在第一帧，租约照常过期）。
     *
     * <p>现在：立即发一帧（缩短"授予后首帧"窗口），再以固定间隔续发；
     * 返回的句柄可停止心跳并优雅关闭流。
     *
     * @param leaseId  要维持的 Lease ID
     * @param onExpire Lease 过期回调（流异常/意外关闭时触发一次；
     *                 调用 {@link KeepAliveHandle#cancel()} 不再触发）
     * @return 可取消的 KeepAlive 句柄
     */
    public KeepAliveHandle keepAlive(long leaseId, Runnable onExpire) {
        return keepAlive(leaseId, DEFAULT_KEEPALIVE_INTERVAL_SECS, onExpire);
    }

    /**
     * 同上，心跳间隔可指定。
     *
     * @param intervalSeconds 心跳间隔（秒）；应显著小于 Lease TTL
     *                        （仓库惯例：TTL/3）
     */
    public KeepAliveHandle keepAlive(long leaseId, long intervalSeconds, Runnable onExpire) {
        if (intervalSeconds <= 0) {
            throw new IllegalArgumentException("keep-alive interval must be > 0");
        }
        // 已取消/已结束：用于区分"我们主动关"与"服务端断了"，
        // 后者才应触发 onExpire（否则 cancel() 会被误报为租约过期）。
        final java.util.concurrent.atomic.AtomicBoolean stopped =
                new java.util.concurrent.atomic.AtomicBoolean(false);

        StreamObserver<LeaseOuterClass.LeaseKeepAliveRequest> reqObserver =
                leaseAsyncStub.leaseKeepAlive(new StreamObserver<>() {
                    @Override
                    public void onNext(LeaseOuterClass.LeaseKeepAliveResponse resp) {
                        // Lease 存活中 — TTL 已刷新
                    }

                    @Override
                    public void onError(Throwable t) {
                        if (stopped.compareAndSet(false, true) && onExpire != null) {
                            onExpire.run();
                        }
                    }

                    @Override
                    public void onCompleted() {
                        if (stopped.compareAndSet(false, true) && onExpire != null) {
                            onExpire.run();
                        }
                    }
                });

        // 立即发一帧，随后按间隔续发。
        reqObserver.onNext(LeaseOuterClass.LeaseKeepAliveRequest.newBuilder()
                .setId(leaseId).build());
        java.util.concurrent.ScheduledFuture<?> task = keepAliveScheduler.scheduleAtFixedRate(() -> {
            if (stopped.get()) {
                return;
            }
            try {
                reqObserver.onNext(LeaseOuterClass.LeaseKeepAliveRequest.newBuilder()
                        .setId(leaseId).build());
            } catch (RuntimeException e) {
                // 流已死：停止调度并通知（若尚未通知过）
                if (stopped.compareAndSet(false, true) && onExpire != null) {
                    onExpire.run();
                }
            }
        }, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);

        return () -> {
            task.cancel(false);
            // 先置 stopped 再关流：正常的 cancel() 不应触发 onExpire。
            stopped.set(true);
            try {
                reqObserver.onCompleted();
            } catch (RuntimeException ignored) {
                // 流已关闭：幂等取消
            }
        };
    }

    // ──── Watch API ────

    /**
     * 获取异步 Watch stub（用于创建 Watch 流）。
     */
    public WatchGrpc.WatchStub watchStub() {
        return WatchGrpc.newStub(channel);
    }

    // ──── Maintenance API ────

    /**
     * 查询集群状态。
     */
    public MaintenanceOuterClass.StatusResponse clusterStatus() {
        return maintenanceStub.status(MaintenanceOuterClass.StatusRequest.newBuilder().build());
    }

    /**
     * 查询集群是否已解封。
     */
    public boolean isUnsealed() {
        MaintenanceOuterClass.StatusResponse status = clusterStatus();
        return !"sealed".equalsIgnoreCase(status.getSealStatus());
    }

    // ──── Lifecycle ────

    @Override
    public void close() {
        // 先停心跳调度器：否则它会持续向正在关闭的 channel 发帧。
        keepAliveScheduler.shutdownNow();
        if (channel != null && !channel.isShutdown()) {
            try {
                channel.shutdown();
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                channel.shutdownNow();
            }
        }
    }

    /**
     * KeepAlive 句柄 — 可调用 cancel() 停止心跳。
     */
    @FunctionalInterface
    public interface KeepAliveHandle {
        void cancel();
    }
}
