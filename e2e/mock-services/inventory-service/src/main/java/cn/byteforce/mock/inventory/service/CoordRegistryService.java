package cn.byteforce.mock.inventory.service;

import coord.v1.Coord;
import coord.v1.RegistryServiceGrpc;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.util.UUID;

@Service
public class CoordRegistryService {
    private static final Logger log = LoggerFactory.getLogger(CoordRegistryService.class);
    @Autowired private RegistryServiceGrpc.RegistryServiceBlockingStub registryStub;
    @Value("${server.port:18082}") private int port;
    private String leaseId;
    private final String instanceId = "inventory-service-" + UUID.randomUUID().toString().substring(0, 8);

    @PostConstruct
    public void register() {
        try {
            Coord.Lease lease = registryStub.register(Coord.RegisterRequest.newBuilder()
                    .setInstance(Coord.ServiceInstance.newBuilder()
                            .setServiceName("inventory-service").setInstanceId(instanceId)
                            .setHost(InetAddress.getLocalHost().getHostAddress()).setPort(port).build())
                    .setTtlSeconds(30).build());
            leaseId = lease.getLeaseId();
            log.info("Registered inventory-service lease={}", leaseId);
        } catch (Exception e) { log.warn("Register failed: {}", e.getMessage()); }
    }

    @Scheduled(fixedDelay = 10_000, initialDelay = 10_000)
    public void heartbeat() {
        if (leaseId == null) { register(); return; }
        try {
            registryStub.heartbeat(Coord.Lease.newBuilder().setLeaseId(leaseId).setTtlSeconds(30).build());
        } catch (StatusRuntimeException e) { leaseId = null; }
    }

    @PreDestroy
    public void deregister() {
        try {
            registryStub.deregister(Coord.ServiceInstance.newBuilder()
                    .setServiceName("inventory-service").setInstanceId(instanceId).build());
        } catch (Exception ignored) {}
    }
}
