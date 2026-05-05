package cn.byteforce.mock.order.service;

import coord.v1.Coord;
import coord.v1.RegistryServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.net.InetAddress;
import java.util.Map;
import java.util.UUID;

/**
 * 将 order-service 自身注册到 Coord，并维持定时心跳续约。
 */
@Service
public class CoordRegistryService {
    private static final Logger log = LoggerFactory.getLogger(CoordRegistryService.class);
    private static final long TTL_SECONDS = 30L;

    @Autowired
    private RegistryServiceGrpc.RegistryServiceBlockingStub registryStub;

    @Value("${server.port:18080}")
    private int serverPort;

    private String leaseId;
    private final String instanceId = "order-service-" + UUID.randomUUID().toString().substring(0, 8);

    @PostConstruct
    public void register() {
        try {
            String host = InetAddress.getLocalHost().getHostAddress();
            Coord.ServiceInstance instance = Coord.ServiceInstance.newBuilder()
                    .setServiceName("order-service")
                    .setInstanceId(instanceId)
                    .setHost(host)
                    .setPort(serverPort)
                    .putMetadata("version", "v1")
                    .putMetadata("env", "e2e")
                    .build();

            Coord.Lease lease = registryStub.register(
                    Coord.RegisterRequest.newBuilder()
                            .setInstance(instance)
                            .setTtlSeconds(TTL_SECONDS)
                            .build());
            this.leaseId = lease.getLeaseId();
            log.info("Registered order-service as {} lease={}", instanceId, leaseId);
        } catch (Exception e) {
            log.warn("Failed to register with Coord: {}", e.getMessage());
        }
    }

    @Scheduled(fixedDelay = 10_000, initialDelay = 10_000)
    public void heartbeat() {
        if (leaseId == null) { register(); return; }
        try {
            registryStub.heartbeat(Coord.Lease.newBuilder()
                    .setLeaseId(leaseId)
                    .setTtlSeconds(TTL_SECONDS)
                    .build());
            log.debug("Heartbeat sent for lease={}", leaseId);
        } catch (StatusRuntimeException e) {
            log.warn("Heartbeat failed, re-registering: {}", e.getMessage());
            leaseId = null;
        }
    }

    @PreDestroy
    public void deregister() {
        if (leaseId == null) return;
        try {
            registryStub.deregister(Coord.ServiceInstance.newBuilder()
                    .setServiceName("order-service")
                    .setInstanceId(instanceId)
                    .build());
            log.info("Deregistered order-service");
        } catch (Exception e) {
            log.warn("Deregister failed: {}", e.getMessage());
        }
    }

    public String getInstanceId() { return instanceId; }
}
