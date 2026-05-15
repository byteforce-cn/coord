package cn.byteforce.e2e;

import coord.v1.*;
import cn.byteforce.e2e.util.BearerAuthInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.atomic.AtomicReference;

@TestConfiguration
public class SpringBootTestConfig {

    @Value("${coord.grpc.address:localhost:9090}")
    private String coordGrpc;

    /**
     * Optional comma-separated list of additional Coord gRPC endpoints used by failover
     * scenarios to construct per-node channels (see {@link #coordPeerEndpoints()}).
     */
    @Value("${coord.grpc.cluster:}")
    private String coordGrpcCluster;

    /**
     * Shared token reference. Updated by SecuritySteps after domain initialization.
     * Read by the gRPC interceptor to inject Bearer auth headers.
     */
    @Bean
    public AtomicReference<String> coordAuthToken() {
        return new AtomicReference<>();
    }

    @Bean(destroyMethod = "shutdown")
    public ManagedChannel coordChannel(AtomicReference<String> coordAuthToken) {
        return ManagedChannelBuilder.forTarget(coordGrpc)
                .usePlaintext()
                .intercept(new BearerAuthInterceptor(coordAuthToken))
                .build();
    }

    /**
     * All Coord gRPC endpoints (bootstrap address + cluster peers) used by failover
     * scenarios. Tests can iterate over this list to find a surviving node when the
     * primary endpoint goes down.
     */
    @Bean
    public java.util.List<String> coordPeerEndpoints() {
        java.util.List<String> endpoints = new java.util.ArrayList<>();
        endpoints.add(coordGrpc);
        if (coordGrpcCluster != null && !coordGrpcCluster.isBlank()) {
            for (String e : coordGrpcCluster.split(",")) {
                String trimmed = e.trim();
                if (!trimmed.isEmpty()) endpoints.add(trimmed);
            }
        }
        return java.util.Collections.unmodifiableList(endpoints);
    }

    @Bean public RegistryServiceGrpc.RegistryServiceBlockingStub registryStub(ManagedChannel ch) {
        return RegistryServiceGrpc.newBlockingStub(ch);
    }
    @Bean public ConfigServiceGrpc.ConfigServiceBlockingStub configStub(ManagedChannel ch) {
        return ConfigServiceGrpc.newBlockingStub(ch);
    }
    @Bean public LockServiceGrpc.LockServiceBlockingStub lockStub(ManagedChannel ch) {
        return LockServiceGrpc.newBlockingStub(ch);
    }
    @Bean public LockServiceGrpc.LockServiceStub lockAsyncStub(ManagedChannel ch) {
        return LockServiceGrpc.newStub(ch);
    }
    @Bean public IdGenServiceGrpc.IdGenServiceBlockingStub idGenStub(ManagedChannel ch) {
        return IdGenServiceGrpc.newBlockingStub(ch);
    }
    @Bean public WorkflowServiceGrpc.WorkflowServiceBlockingStub workflowStub(ManagedChannel ch) {
        return WorkflowServiceGrpc.newBlockingStub(ch);
    }
    @Bean public TransitServiceGrpc.TransitServiceBlockingStub transitStub(ManagedChannel ch) {
        return TransitServiceGrpc.newBlockingStub(ch);
    }
    @Bean public PkiServiceGrpc.PkiServiceBlockingStub pkiStub(ManagedChannel ch) {
        return PkiServiceGrpc.newBlockingStub(ch);
    }
    @Bean public SealServiceGrpc.SealServiceBlockingStub sealStub(ManagedChannel ch) {
        return SealServiceGrpc.newBlockingStub(ch);
    }
    @Bean public AuthServiceGrpc.AuthServiceBlockingStub authStub(ManagedChannel ch) {
        return AuthServiceGrpc.newBlockingStub(ch);
    }
    @Bean public AdminServiceGrpc.AdminServiceBlockingStub adminStub(ManagedChannel ch) {
        return AdminServiceGrpc.newBlockingStub(ch);
    }
    @Bean public PolicyServiceGrpc.PolicyServiceBlockingStub policyStub(ManagedChannel ch) {
        return PolicyServiceGrpc.newBlockingStub(ch);
    }
}
