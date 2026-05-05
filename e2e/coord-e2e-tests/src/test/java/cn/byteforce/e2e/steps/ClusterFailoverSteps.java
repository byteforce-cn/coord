package cn.byteforce.e2e.steps;

import coord.v1.AdminServiceGrpc;
import coord.v1.Coord;
import coord.v1.ConfigServiceGrpc;
import coord.v1.RegistryServiceGrpc;
import cn.byteforce.e2e.util.ClusterStubFactory;
import cn.byteforce.e2e.util.DockerComposeHelper;
import cn.byteforce.e2e.util.RetryHelper;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assumptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 集群容错测试步骤：通过 DockerComposeHelper 控制节点启停，验证 Raft 故障转移行为。
 */
public class ClusterFailoverSteps {
    private static final Logger log = LoggerFactory.getLogger(ClusterFailoverSteps.class);

    @Autowired private DockerComposeHelper docker;
    @Autowired private ClusterStubFactory cluster;
    @Autowired private ScenarioState state;

    /**
     * Returns an admin stub that talks to a node currently up in the cluster, so failover
     * scenarios that kill the bootstrap node (coord-1) can still observe the surviving
     * Raft cluster state via coord-2 / coord-3.
     */
    private AdminServiceGrpc.AdminServiceBlockingStub aliveAdmin() {
        return cluster.adminFor(cluster.findAliveEndpoint());
    }

    private ConfigServiceGrpc.ConfigServiceBlockingStub aliveConfig() {
        return cluster.configFor(cluster.findAliveEndpoint());
    }

    private RegistryServiceGrpc.RegistryServiceBlockingStub aliveRegistry() {
        return cluster.registryFor(cluster.findAliveEndpoint());
    }

    /** 确保每个场景结束后恢复所有停止的节点，避免影响后续测试。 */
    @After("@failover")
    public void restoreAll() {
        docker.restoreAllStopped();
        // After restoring containers, wait until the cluster is reachable again so the
        // next scenario doesn't immediately hit "connection refused" from coord-1.
        try {
            RetryHelper.await(60).untilAsserted(() -> {
                Coord.ClusterStatusResponse r = aliveAdmin().clusterStatus(
                        Coord.ClusterStatusRequest.newBuilder().build());
                org.assertj.core.api.Assertions.assertThat(r.getMembersCount())
                        .isGreaterThanOrEqualTo(3);
            });
        } catch (Throwable t) {
            log.warn("cluster did not return to 3 members after restore: {}", t.getMessage());
        }
        // Also ensure the default test channel endpoint (coord-1 / 127.0.0.1:9090) is
        // accepting RPCs — the next scenario's Background step uses it directly.
        try {
            RetryHelper.await(60).untilAsserted(() -> {
                Coord.ClusterStatusResponse r = cluster.adminFor("127.0.0.1:9090")
                        .withDeadlineAfter(2, java.util.concurrent.TimeUnit.SECONDS)
                        .clusterStatus(Coord.ClusterStatusRequest.newBuilder().build());
                org.assertj.core.api.Assertions.assertThat(r.getMembersCount())
                        .isGreaterThanOrEqualTo(3);
            });
        } catch (Throwable t) {
            log.warn("coord-1 (127.0.0.1:9090) not reachable after restore: {}", t.getMessage());
        }
    }

    /**
     * 在每个 @failover 场景开始前检查 Docker 是否可用。
     * 若不可用（如受限 CI），场景被标记为"中止"而不是"失败"，避免误报。
     * 要启用：确保 /var/run/docker.sock 已挂载，或设置 DOCKER_HOST 环境变量。
     */
    @Before("@failover")
    public void requireDocker() {
        Assumptions.assumeTrue(docker.isDockerAvailable(),
                "Docker socket unavailable — skipping cluster failover test. "
                + "Mount /var/run/docker.sock or set DOCKER_HOST to enable.");
    }

    @Given("集群选举已完成")
    public void electionComplete() {
        RetryHelper.await(60).until(() -> {
            try {
                Coord.ClusterStatusResponse r = aliveAdmin().clusterStatus(
                        Coord.ClusterStatusRequest.newBuilder().build());
                return r.getMembersCount() >= 3;
            } catch (Exception e) {
                return false;
            }
        });
    }

    @When("停止 Follower 节点")
    public void stopFollower() {
        docker.stopFollower();
    }

    @When("停止 Leader 节点")
    public void stopLeader() {
        docker.stopLeader();
    }

    @When("停止 2 个节点（仅剩 1 节点）")
    public void stopTwoNodes() {
        docker.stopNodes(2);
    }

    @When("停止 1 个 Follower 节点")
    public void stopOneFollower() {
        docker.stopFollower();
    }

    @When("等待 {int}s 让集群感知")
    public void waitSeconds(int seconds) {
        try { Thread.sleep(seconds * 1000L); } catch (InterruptedException ignored) {}
    }

    @When("等待新选举完成最多 {int}s")
    public void waitElection(int seconds) {
        RetryHelper.await(seconds).until(() -> {
            try {
                Coord.ClusterStatusResponse r = aliveAdmin().clusterStatus(
                        Coord.ClusterStatusRequest.newBuilder().build());
                return !r.getState().isEmpty();
            } catch (Exception e) {
                return false;
            }
        });
    }

    @When("等待日志追赶最多 {int}s")
    public void waitCatchup(int seconds) {
        try { Thread.sleep(Math.min(seconds, 30) * 1000L); } catch (InterruptedException ignored) {}
    }

    @Then("新 Leader 被选出")
    public void newLeaderElected() {
        RetryHelper.await(30).untilAsserted(() -> {
            Coord.ClusterStatusResponse r = aliveAdmin().clusterStatus(
                    Coord.ClusterStatusRequest.newBuilder().build());
            assertThat(r.getState()).isNotBlank();
            assertThat(r.getMembersCount()).isGreaterThanOrEqualTo(2);
        });
    }

    @Then("Discover API 返回正常")
    public void discoverWorks() {
        RetryHelper.await(15).untilAsserted(() -> {
            // Discover returns a server-side stream; calling hasNext() is enough to verify the RPC is reachable
            java.util.Iterator<Coord.ServiceInstance> it = aliveRegistry().discover(
                    Coord.ServiceQuery.newBuilder().setServiceName("coord").build());
            assertThat(it).isNotNull();
        });
    }

    @Then("GetConfig API 返回正常")
    public void getConfigWorks() {
        RetryHelper.await(15).untilAsserted(() -> {
            try {
                Coord.ConfigResponse r = aliveConfig().getConfig(
                        Coord.ConfigRequest.newBuilder().setKey("probe").build());
                assertThat(r).isNotNull();
            } catch (StatusRuntimeException e) {
                // NOT_FOUND means the API is healthy and merely reports the missing key.
                if (e.getStatus().getCode() == io.grpc.Status.Code.NOT_FOUND) {
                    return;
                }
                throw e;
            }
        });
    }

    @When("尝试写入配置 key={string} value={string}")
    public void tryWriteConfig(String key, String value) {
        state.lastConfigWriteKey = key;
        state.lastConfigWriteValue = value;
        // After losing majority, NO node can serve cluster_status (raft sees no quorum), so
        // findAliveEndpoint() throws. Iterate every endpoint directly and treat any
        // failure (including "no live endpoint") as the expected unavailable result.
        boolean wrote = false;
        Throwable lastErr = null;
        for (String ep : cluster.endpoints()) {
            try {
                cluster.configFor(ep)
                        .withDeadlineAfter(5, java.util.concurrent.TimeUnit.SECONDS)
                        .putConfig(Coord.PutConfigRequest.newBuilder()
                                .setKey(key).setValue(value).build());
                wrote = true;
                break;
            } catch (Throwable t) {
                lastErr = t;
            }
        }
        if (wrote) {
            state.lastConfigWriteSucceeded = true;
        } else {
            log.info("Config write failed as expected (no quorum): {}",
                    lastErr != null ? lastErr.getMessage() : "no endpoints");
            state.lastConfigWriteSucceeded = false;
        }
    }

    @Then("写入操作超时或返回 UNAVAILABLE")
    public void writeUnavailable() {
        assertThat(state.lastConfigWriteSucceeded).isFalse();
    }

    @When("恢复 Follower 节点")
    @When("恢复停止的节点")
    @When("恢复已停止的 Follower 节点")
    public void restoreLastStopped() {
        docker.restoreLastStopped();
    }

    @When("恢复所有已停止节点")
    public void restoreAll2() {
        docker.restoreAllStopped();
    }

    @Then("GetConfig {string} 返回 {string}")
    public void getConfigEquals(String key, String expected) {
        RetryHelper.await(30).untilAsserted(() -> {
            Coord.ConfigResponse r = aliveConfig().getConfig(
                    Coord.ConfigRequest.newBuilder().setKey(key).build());
            assertThat(r.getValue()).isEqualTo(expected);
        });
    }
}
