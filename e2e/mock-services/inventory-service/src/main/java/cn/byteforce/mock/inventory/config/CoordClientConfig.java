package cn.byteforce.mock.inventory.config;

import coord.v1.LockServiceGrpc;
import coord.v1.RegistryServiceGrpc;
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

    @Bean public RegistryServiceGrpc.RegistryServiceBlockingStub registryStub(ManagedChannel ch) {
        return RegistryServiceGrpc.newBlockingStub(ch);
    }
    @Bean public LockServiceGrpc.LockServiceBlockingStub lockStub(ManagedChannel ch) {
        return LockServiceGrpc.newBlockingStub(ch);
    }
}
