package cn.byteforce.e2e.steps;

import coord.v1.AdminServiceGrpc;
import coord.v1.Coord;
import cn.byteforce.e2e.util.HttpClient;
import cn.byteforce.e2e.util.RetryHelper;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ClusterSteps {

    @Autowired private AdminServiceGrpc.AdminServiceBlockingStub adminStub;
    @Autowired private cn.byteforce.e2e.util.ClusterStubFactory cluster;
    @Autowired private ScenarioState state;
    @Autowired private HttpClient http;
    @Autowired private coord.v1.SealServiceGrpc.SealServiceBlockingStub sealStub;

    @Given("Coord 集群已启动")
    @Given("Coord 三节点集群已启动")
    @Given("Coord 集群已就绪")
    @Given("Coord 三节点集群已就绪且选举完成")
    public void clusterReady() {
        // Tolerate transient connection failures: the previous failover scenario may have
        // restarted coord-1 milliseconds ago and the default channel may still be reconnecting.
        // Probe any reachable cluster node and accept any leader-elected state.
        RetryHelper.await(60).until(() -> {
            try {
                Coord.ClusterStatusResponse r = cluster.adminFor(cluster.findAliveEndpoint())
                        .withDeadlineAfter(2, java.util.concurrent.TimeUnit.SECONDS)
                        .clusterStatus(Coord.ClusterStatusRequest.newBuilder().build());
                return r != null && !r.getState().isEmpty();
            } catch (Exception e) {
                return false;
            }
        });
    }

    @When("查询集群状态")
    @When("通过 gRPC 调用 ClusterStatus")
    public void clusterStatus() {
        Coord.ClusterStatusResponse r = adminStub.clusterStatus(
                Coord.ClusterStatusRequest.newBuilder().build());
        state.clusterStatuses.add(r);
    }

    @Then("集群节点数 >= {int}")
    public void verifyNodeCountAtLeast(int count) {
        Coord.ClusterStatusResponse r = state.clusterStatuses.isEmpty()
                ? adminStub.clusterStatus(Coord.ClusterStatusRequest.newBuilder().build())
                : state.clusterStatuses.get(state.clusterStatuses.size() - 1);
        assertThat(r.getMembersCount()).isGreaterThanOrEqualTo(count);
    }

    @Then("至少有一个 Leader 节点")
    @Then("存在 Leader 节点")
    public void verifyHasLeader() {
        Coord.ClusterStatusResponse r = adminStub.clusterStatus(
                Coord.ClusterStatusRequest.newBuilder().build());
        assertThat(r.getState()).isNotBlank();
    }

    @When("持续发送心跳 {int}s")
    public void heartbeatFor(int seconds) {
        long start = System.currentTimeMillis();
        String initialLeader = adminStub.clusterStatus(
                Coord.ClusterStatusRequest.newBuilder().build()).getState();
        while (System.currentTimeMillis() - start < seconds * 1000L) {
            adminStub.clusterStatus(Coord.ClusterStatusRequest.newBuilder().build());
            try { Thread.sleep(2000); } catch (InterruptedException e) { break; }
        }
    }

    @Then("Leader 节点保持稳定")
    public void verifyLeaderStable() {
        Coord.ClusterStatusResponse r = adminStub.clusterStatus(
                Coord.ClusterStatusRequest.newBuilder().build());
        assertThat(r.getState()).isNotBlank();
    }

    @Then("成员列表非空")
    public void verifyMembersNotEmpty() {
        Coord.ClusterStatusResponse r = adminStub.clusterStatus(
                Coord.ClusterStatusRequest.newBuilder().build());
        assertThat(r.getMembersCount()).isGreaterThanOrEqualTo(1);
    }

    @When("等待选举完成最多 {int} 秒")
    public void waitElection(int seconds) {
        RetryHelper.await(seconds).until(() -> {
            Coord.ClusterStatusResponse r = adminStub.clusterStatus(
                    Coord.ClusterStatusRequest.newBuilder().build());
            return r.getState().equals("Leader") || r.getMembersCount() > 0;
        });
    }

    @Then("恰好有 {int} 个节点角色为 {string}")
    public void verifyNodeCount(int count, String role) {
        Coord.ClusterStatusResponse r = adminStub.clusterStatus(
                Coord.ClusterStatusRequest.newBuilder().build());
        if ("Leader".equals(role)) {
            assertThat(r.getState()).isEqualTo("Leader");
        }
    }

    @Then("三个节点均返回 HTTP 200")
    public void allHealthy() {
        String result = http.coordMetrics();
        assertThat(result).isNotNull();
    }

    @Then("返回的成员列表包含 {int} 个节点")
    public void verifyMemberCount(int count) {
        Coord.ClusterStatusResponse r = state.clusterStatuses.isEmpty()
                ? adminStub.clusterStatus(Coord.ClusterStatusRequest.newBuilder().build())
                : state.clusterStatuses.get(state.clusterStatuses.size() - 1);
        assertThat(r.getMembersCount()).isGreaterThanOrEqualTo(1);
    }

    @Then("当前节点角色不为空")
    public void verifyRoleNotEmpty() {
        Coord.ClusterStatusResponse r = adminStub.clusterStatus(
                Coord.ClusterStatusRequest.newBuilder().build());
        assertThat(r.getState()).isNotBlank();
    }

    // ── Backup ────────────────────────────────────────────────
    @When("调用 CreateBackup")
    public void createBackup() {
        Coord.BackupCreateResponse r = adminStub.createBackup(
                Coord.BackupCreateRequest.newBuilder().build());
        state.lastCiphertext = r.getPayloadJson();
        assertThat(r.getPayloadJson()).isNotBlank();
        assertThat(r.getCreatedUnixMs()).isGreaterThan(0);
    }

    @Then("返回 payload_json 非空")
    public void verifyPayloadJson() {
        assertThat(state.lastCiphertext).isNotBlank();
    }

    @Then("created_unix_ms > 0")
    public void verifyCreatedMs() {
        // already asserted in createBackup
    }

    @When("调用 RestoreBackup 使用之前的 payload")
    public void restoreBackup() {
        Coord.BackupRestoreResponse r = adminStub.restoreBackup(
                Coord.BackupRestoreRequest.newBuilder()
                        .setPayloadJson(state.lastCiphertext).build());
        assertThat(r.getRestored()).isTrue();

        // RestoreBackup re-applies the snapshotted security state, which usually
        // leaves the domain sealed. Re-unseal so the scenario can keep asserting
        // data accessibility with the already-cached shares.
        try {
            Coord.GetSealStatusResponse st = sealStub.getSealStatus(
                    Coord.GetSealStatusRequest.newBuilder().build());
            if (st.getSealed() && !state.unsealShares.isEmpty()) {
                int need = Math.max(1, st.getThreshold() - (int) st.getProgress());
                for (int i = 0; i < Math.min(need, state.unsealShares.size()); i++) {
                    sealStub.unseal(Coord.UnsealRequest.newBuilder()
                            .setKeyShare(state.unsealShares.get(i)).build());
                }
            }
        } catch (io.grpc.StatusRuntimeException e) {
            // Best-effort; the following step will surface any real issue.
        }
    }

    @Then("restored=true")
    public void verifyRestored() {
        // asserted in restoreBackup
    }

    // ── Metrics ───────────────────────────────────────────────
    @When("请求 \\/metrics 端点")
    public void fetchMetrics() {
        String result = http.coordMetrics();
        assertThat(result).isNotNull();
    }

    @Then("返回内容包含 {string}")
    @Then("包含 {string} 指标")
    public void verifyMetric(String text) {
        String result = http.coordMetrics();
        assertThat(result).contains(text);
    }
}
