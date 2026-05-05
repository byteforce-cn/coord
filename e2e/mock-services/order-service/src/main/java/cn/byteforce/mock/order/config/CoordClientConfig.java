package cn.byteforce.mock.order.config;

import coord.v1.AdminServiceGrpc;
import coord.v1.ConfigServiceGrpc;
import coord.v1.IdGenServiceGrpc;
import coord.v1.LockServiceGrpc;
import coord.v1.RegistryServiceGrpc;
import coord.v1.TransitServiceGrpc;
import coord.v1.WorkflowServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CoordClientConfig {

    @Value("${coord.grpc.address:localhost:9090}")
    private String coordAddress;

    @Autowired
    private CoordAuthTokenHolder authTokenHolder;

    @Bean(destroyMethod = "shutdown")
    public ManagedChannel coordChannel() {
        return ManagedChannelBuilder.forTarget(coordAddress)
                .usePlaintext()
                .intercept(new BearerAuthInterceptor(authTokenHolder))
                .build();
    }

    @Bean
    public RegistryServiceGrpc.RegistryServiceBlockingStub registryStub(ManagedChannel channel) {
        return RegistryServiceGrpc.newBlockingStub(channel);
    }

    @Bean
    public ConfigServiceGrpc.ConfigServiceBlockingStub configStub(ManagedChannel channel) {
        return ConfigServiceGrpc.newBlockingStub(channel);
    }

    @Bean
    public LockServiceGrpc.LockServiceBlockingStub lockStub(ManagedChannel channel) {
        return LockServiceGrpc.newBlockingStub(channel);
    }

    @Bean
    public IdGenServiceGrpc.IdGenServiceBlockingStub idGenStub(ManagedChannel channel) {
        return IdGenServiceGrpc.newBlockingStub(channel);
    }

    @Bean
    public TransitServiceGrpc.TransitServiceBlockingStub transitStub(ManagedChannel channel) {
        return TransitServiceGrpc.newBlockingStub(channel);
    }

    @Bean
    public WorkflowServiceGrpc.WorkflowServiceBlockingStub workflowStub(ManagedChannel channel) {
        return WorkflowServiceGrpc.newBlockingStub(channel);
    }

    @Bean
    public AdminServiceGrpc.AdminServiceBlockingStub adminStub(ManagedChannel channel) {
        return AdminServiceGrpc.newBlockingStub(channel);
    }
}
