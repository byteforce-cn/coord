package cn.byteforce.e2e.steps;

import coord.v1.Coord;
import coord.v1.RegistryServiceGrpc;
import cn.byteforce.e2e.util.RetryHelper;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class RegistrySteps {

    @Autowired private RegistryServiceGrpc.RegistryServiceBlockingStub registryStub;
    @Autowired private ScenarioState state;

    private String lastRegisteredInstanceId;
    private String lastRegisteredServiceName;

    // ── Register ──────────────────────────────────────────────

    @When("注册服务 {string} host={string} port={int} ttl={int}s")
    public void registerService(String name, String host, int port, int ttl) {
        lastRegisteredInstanceId = name + "-" + UUID.randomUUID().toString().substring(0, 6);
        lastRegisteredServiceName = name;
        Coord.Lease lease = registryStub.register(Coord.RegisterRequest.newBuilder()
                .setInstance(Coord.ServiceInstance.newBuilder()
                        .setServiceName(name).setInstanceId(lastRegisteredInstanceId)
                        .setHost(host).setPort(port).build())
                .setTtlSeconds(ttl).build());
        state.registeredLeaseId = lease.getLeaseId();
    }

    @When("注册服务 {string} host={string} port={int} metadata=\\{version={word}\\}")
    public void registerServiceWithMetadata(String name, String host, int port, String version) {
        lastRegisteredInstanceId = name + "-" + UUID.randomUUID().toString().substring(0, 6);
        lastRegisteredServiceName = name;
        registryStub.register(Coord.RegisterRequest.newBuilder()
                .setInstance(Coord.ServiceInstance.newBuilder()
                        .setServiceName(name).setInstanceId(lastRegisteredInstanceId)
                        .setHost(host).setPort(port)
                        .putMetadata("version", version).build())
                .setTtlSeconds(30).build());
    }

    @Given("服务 {string} 实例 host={string} port={int} 已注册")
    @Given("服务 {string} host={string} port={int} 已注册")
    public void serviceRegistered(String name, String host, int port) {
        lastRegisteredInstanceId = name + "-" + UUID.randomUUID().toString().substring(0, 6);
        lastRegisteredServiceName = name;
        Coord.Lease lease = registryStub.register(Coord.RegisterRequest.newBuilder()
                .setInstance(Coord.ServiceInstance.newBuilder()
                        .setServiceName(name).setInstanceId(lastRegisteredInstanceId)
                        .setHost(host).setPort(port).build())
                .setTtlSeconds(30).build());
        state.registeredLeaseId = lease.getLeaseId();
    }

    @Given("服务 {string} 实例 host={string} port={int} 已注册 ttl={int}s")
    public void serviceRegisteredWithTtl(String name, String host, int port, int ttl) {
        lastRegisteredInstanceId = name + "-" + UUID.randomUUID().toString().substring(0, 6);
        lastRegisteredServiceName = name;
        Coord.Lease lease = registryStub.register(Coord.RegisterRequest.newBuilder()
                .setInstance(Coord.ServiceInstance.newBuilder()
                        .setServiceName(name).setInstanceId(lastRegisteredInstanceId)
                        .setHost(host).setPort(port).build())
                .setTtlSeconds(ttl).build());
        state.registeredLeaseId = lease.getLeaseId();
    }

    // ── Discover ──────────────────────────────────────────────

    @When("发现服务 {string}")
    @When("通过 Discover 查询 {string}")
    public void discover(String serviceName) {
        state.discoveredInstances = doDiscover(serviceName);
    }

    // ── Then: instance verification ───────────────────────────

    @Then("返回 lease_id 非空")
    public void verifyLeaseId() {
        assertThat(state.registeredLeaseId).isNotBlank();
    }

    @Then("返回实例列表非空")
    public void verifyInstanceListNotEmpty() {
        assertThat(state.discoveredInstances).isNotEmpty();
    }

    @Then("包含 host={string} port={int}")
    public void verifyContainsHostPort(String host, int port) {
        boolean found = state.discoveredInstances.stream()
                .anyMatch(i -> i.getHost().equals(host) && i.getPort() == port);
        assertThat(found).as("instance host=%s port=%d", host, port).isTrue();
    }

    @Then("实例列表不包含已注销实例")
    public void verifyDeregistered() {
        boolean found = state.discoveredInstances.stream()
                .anyMatch(i -> i.getInstanceId().equals(lastRegisteredInstanceId));
        assertThat(found).as("deregistered instance should be absent").isFalse();
    }

    @Then("实例列表为空或不包含该实例")
    public void verifyExpired() {
        boolean found = state.discoveredInstances.stream()
                .anyMatch(i -> i.getInstanceId().equals(lastRegisteredInstanceId));
        assertThat(found).as("expired instance should be absent").isFalse();
    }

    @Then("返回实例数 >= {int}")
    @Then("返回至少 {int} 个实例")
    public void atLeastInstances(int count) {
        assertThat(state.discoveredInstances.size()).isGreaterThanOrEqualTo(count);
    }

    @Then("发现实例元数据包含 version={word}")
    public void verifyMetadataVersion(String version) {
        state.discoveredInstances = doDiscover(lastRegisteredServiceName);
        boolean found = state.discoveredInstances.stream()
                .anyMatch(inst -> version.equals(inst.getMetadataMap().get("version")));
        assertThat(found).as("metadata version=" + version).isTrue();
    }

    @Then("任一实例元数据包含 {word}={word}")
    public void verifyAnyMetadataEntry(String key, String value) {
        boolean found = state.discoveredInstances.stream()
                .anyMatch(inst -> value.equals(inst.getMetadataMap().get(key)));
        assertThat(found).as("metadata %s=%s", key, value).isTrue();
    }

    @When("对未知租约 {string} 发送心跳")
    public void heartbeatUnknownLease(String leaseId) {
        state.lastHeartbeatException = null;
        try {
            registryStub.heartbeat(Coord.Lease.newBuilder()
                    .setLeaseId(leaseId)
                    .setTtlSeconds(30)
                    .build());
        } catch (StatusRuntimeException e) {
            state.lastHeartbeatException = e;
        }
    }

    @Then("应收到 NOT_FOUND 错误")
    public void verifyNotFoundError() {
        assertThat(state.lastHeartbeatException)
                .as("expected a StatusRuntimeException with NOT_FOUND")
                .isNotNull();
        assertThat(state.lastHeartbeatException.getStatus().getCode())
                .isEqualTo(Status.NOT_FOUND.getCode());
    }

    // ── Deregister / Heartbeat ────────────────────────────────

    @When("注销服务 lease_id")
    public void deregisterByLease() {
        registryStub.deregister(Coord.ServiceInstance.newBuilder()
                .setServiceName(lastRegisteredServiceName)
                .setInstanceId(lastRegisteredInstanceId).build());
    }

    @When("等待 {int}s")
    public void waitS(int seconds) {
        RetryHelper.waitSeconds(seconds);
    }

    @When("记录当前发现实例 ID 列表")
    public void snapshotDiscoveredInstanceIds() {
        state.discoveredInstanceIdsSnapshot = state.discoveredInstances.stream()
                .map(Coord.ServiceInstance::getInstanceId)
                .filter(id -> !id.isBlank())
                .collect(Collectors.toList());
    }

    @When("发送 {int} 次心跳续约")
    public void sendHeartbeats(int count) {
        for (int i = 0; i < count; i++) {
            registryStub.heartbeat(Coord.Lease.newBuilder()
                    .setLeaseId(state.registeredLeaseId).setTtlSeconds(30).build());
            try { Thread.sleep(1000); } catch (InterruptedException e) { break; }
        }
    }

    @Then("仍包含先前记录的任一实例")
    public void verifyStillContainsAnySnapshottedInstance() {
        Set<String> currentInstanceIds = state.discoveredInstances.stream()
                .map(Coord.ServiceInstance::getInstanceId)
                .collect(Collectors.toSet());
        boolean found = state.discoveredInstanceIdsSnapshot.stream().anyMatch(currentInstanceIds::contains);
        assertThat(found)
                .as("current discover result should retain at least one previously discovered instance")
                .isTrue();
    }

    // ── Helpers ───────────────────────────────────────────────

    private List<Coord.ServiceInstance> doDiscover(String name) {
        List<Coord.ServiceInstance> result = new ArrayList<>();
        Iterator<Coord.ServiceInstance> it = registryStub.discover(
                Coord.ServiceQuery.newBuilder().setServiceName(name).build());
        while (it.hasNext()) result.add(it.next());
        return result;
    }
}
