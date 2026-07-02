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

    private final ManagedChannel channel;
    private final KVGrpc.KVBlockingStub kvStub;
    private final LeaseGrpc.LeaseBlockingStub leaseStub;
    private final LeaseGrpc.LeaseStub leaseAsyncStub;
    private final MaintenanceGrpc.MaintenanceBlockingStub maintenanceStub;

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
     * @param prefix Key 前缀
     * @return 匹配的键值对列表
     */
    public List<Kv.KeyValue> scan(String prefix) {
        ByteString prefixBytes = ByteString.copyFromUtf8(prefix);
        ByteString rangeEnd = ByteString.copyFromUtf8(prefix + "\0");

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
     * 创建 KeepAlive 流并异步维持 Lease 心跳。
     *
     * @param leaseId  要维持的 Lease ID
     * @param onExpire Lease 过期回调（可选）
     * @return 可取消的 KeepAlive 句柄
     */
    public KeepAliveHandle keepAlive(long leaseId, Runnable onExpire) {
        StreamObserver<LeaseOuterClass.LeaseKeepAliveRequest> reqObserver =
                leaseAsyncStub.leaseKeepAlive(new StreamObserver<>() {
                    @Override
                    public void onNext(LeaseOuterClass.LeaseKeepAliveResponse resp) {
                        // Lease 存活中 — TTL 已刷新
                    }

                    @Override
                    public void onError(Throwable t) {
                        if (onExpire != null) onExpire.run();
                    }

                    @Override
                    public void onCompleted() {
                        if (onExpire != null) onExpire.run();
                    }
                });

        // 发送初始 KeepAlive
        reqObserver.onNext(LeaseOuterClass.LeaseKeepAliveRequest.newBuilder()
                .setId(leaseId).build());

        return () -> reqObserver.onCompleted();
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
        if (channel != null && !channel.isShutdown()) {
            try {
                channel.shutdown();
                channel.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
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
