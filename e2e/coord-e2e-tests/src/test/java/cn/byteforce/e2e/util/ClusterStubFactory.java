package cn.byteforce.e2e.util;

import coord.v1.AdminServiceGrpc;
import coord.v1.ConfigServiceGrpc;
import coord.v1.RegistryServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 为集群中每个 Coord 节点维护独立的 gRPC 通道与 stub，并提供"找到任意可用节点"的查询能力。
 *
 * <p>故障转移测试场景（如杀死 Leader）会断开默认 {@code coordChannel} 指向 coord-1 的连接，
 * 因此这些场景需要直接绕过 Spring 注入的单端点 stub，向其他活节点发起 RPC。</p>
 */
@Component
public class ClusterStubFactory {

    private final List<String> endpoints;
    private final AtomicReference<String> token;
    private final Map<String, ManagedChannel> channels = new HashMap<>();

    @Autowired
    public ClusterStubFactory(@Qualifier("coordPeerEndpoints") List<String> endpoints,
                              AtomicReference<String> token) {
        this.endpoints = endpoints;
        this.token = token;
    }

    public synchronized List<String> endpoints() {
        return new ArrayList<>(endpoints);
    }

    public synchronized ManagedChannel channelFor(String endpoint) {
        return channels.computeIfAbsent(endpoint, ep ->
                ManagedChannelBuilder.forTarget(ep)
                        .usePlaintext()
                        .intercept(new BearerAuthInterceptor(token))
                        .build());
    }

    public AdminServiceGrpc.AdminServiceBlockingStub adminFor(String endpoint) {
        return AdminServiceGrpc.newBlockingStub(channelFor(endpoint));
    }

    public ConfigServiceGrpc.ConfigServiceBlockingStub configFor(String endpoint) {
        return ConfigServiceGrpc.newBlockingStub(channelFor(endpoint));
    }

    public RegistryServiceGrpc.RegistryServiceBlockingStub registryFor(String endpoint) {
        return RegistryServiceGrpc.newBlockingStub(channelFor(endpoint));
    }

    /**
     * Returns the first endpoint whose AdminService responds within a short timeout.
     * Throws {@link IllegalStateException} if no endpoint responds.
     */
    public String findAliveEndpoint() {
        Throwable last = null;
        for (String ep : endpoints) {
            try {
                AdminServiceGrpc.AdminServiceBlockingStub stub = adminFor(ep)
                        .withDeadlineAfter(2, TimeUnit.SECONDS);
                stub.clusterStatus(coord.v1.Coord.ClusterStatusRequest.newBuilder().build());
                return ep;
            } catch (Throwable t) {
                last = t;
            }
        }
        throw new IllegalStateException("no live coord endpoint among " + endpoints, last);
    }

    @PreDestroy
    public synchronized void shutdown() {
        for (ManagedChannel ch : channels.values()) {
            try { ch.shutdown(); } catch (Throwable ignored) {}
        }
        channels.clear();
    }
}
