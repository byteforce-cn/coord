package cn.byteforce.mock.pay.config;

import coord.v1.IdGenServiceGrpc;
import coord.v1.LockServiceGrpc;
import coord.v1.RegistryServiceGrpc;
import coord.v1.TransitServiceGrpc;
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
    @Bean public IdGenServiceGrpc.IdGenServiceBlockingStub idGenStub(ManagedChannel ch) {
        return IdGenServiceGrpc.newBlockingStub(ch);
    }
    @Bean public TransitServiceGrpc.TransitServiceBlockingStub transitStub(ManagedChannel ch) {
        return TransitServiceGrpc.newBlockingStub(ch);
    }
    @Bean public LockServiceGrpc.LockServiceBlockingStub lockStub(ManagedChannel ch) {
        return LockServiceGrpc.newBlockingStub(ch);
    }
}
