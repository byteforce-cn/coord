package cn.byteforce.e2e.steps;

import coord.v1.Coord;
import coord.v1.PolicyServiceGrpc;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.grpc.StatusRuntimeException;
import org.springframework.beans.factory.annotation.Autowired;

import static org.assertj.core.api.Assertions.assertThat;

public class PolicySteps {

    @Autowired private PolicyServiceGrpc.PolicyServiceBlockingStub policyStub;
    @Autowired private ScenarioState state;

    // ── Write ─────────────────────────────────────────────────

    @When("写入策略 bundle tenant={string} namespace={string} name={string} rego={string}")
    public void putPolicyBundle(String tenant, String namespace, String name, String rego) {
        Coord.PutPolicyBundleResponse resp = policyStub.putPolicyBundle(
                Coord.PutPolicyBundleRequest.newBuilder()
                        .setTenantId(tenant)
                        .setNamespace(namespace)
                        .setName(name)
                        .setRegoSource(rego.replace("\\n", "\n"))
                        .build());
        state.lastPolicyBundle = resp;
        state.lastPolicyBundleId = resp.getId();
    }

    @Given("策略 bundle tenant={string} namespace={string} name={string} rego={string} 已写入")
    public void policyBundleExists(String tenant, String namespace, String name, String rego) {
        putPolicyBundle(tenant, namespace, name, rego);
    }

    // ── Assertions: write ─────────────────────────────────────

    @Then("返回 bundle_id 非空")
    public void verifyBundleIdNotBlank() {
        assertThat(state.lastPolicyBundleId).isNotBlank();
    }

    // ── List ──────────────────────────────────────────────────

    @When("列出 tenant={string} 的策略 bundle")
    public void listPolicyBundles(String tenant) {
        state.lastPolicyBundleList = policyStub.listPolicyBundles(
                Coord.ListPolicyBundlesRequest.newBuilder()
                        .setTenantId(tenant)
                        .build());
    }

    @Then("策略列表包含 name={string}")
    public void verifyBundleListContainsName(String name) {
        assertThat(state.lastPolicyBundleList).isNotNull();
        boolean found = state.lastPolicyBundleList.getBundlesList().stream()
                .anyMatch(b -> b.getName().equals(name));
        assertThat(found).as("policy bundle with name=%s should be present", name).isTrue();
    }

    @Then("策略列表不包含 name={string}")
    public void verifyBundleListNotContainsName(String name) {
        if (state.lastPolicyBundleList == null) {
            return;
        }
        boolean found = state.lastPolicyBundleList.getBundlesList().stream()
                .anyMatch(b -> b.getName().equals(name));
        assertThat(found).as("policy bundle with name=%s should be absent after delete", name).isFalse();
    }

    // ── Enable / Disable ──────────────────────────────────────

    @When("禁用 bundle_id")
    public void disableBundleById() {
        assertThat(state.lastPolicyBundleId).isNotBlank();
        policyStub.setBundleEnabled(Coord.SetBundleEnabledRequest.newBuilder()
                .setId(state.lastPolicyBundleId)
                .setEnabled(false)
                .build());
    }

    // ── Delete ────────────────────────────────────────────────

    @When("删除 bundle_id")
    public void deleteBundleById() {
        assertThat(state.lastPolicyBundleId).isNotBlank();
        policyStub.deletePolicyBundle(Coord.DeletePolicyBundleRequest.newBuilder()
                .setId(state.lastPolicyBundleId)
                .build());
    }

    // ── Evaluate ──────────────────────────────────────────────

    @When("评估策略 bundle_id query={string} input={string}")
    public void evaluatePolicy(String query, String inputJson) {
        state.lastEvaluateResponse = null;
        state.lastHeartbeatException = null;
        try {
            state.lastEvaluateResponse = policyStub.evaluate(Coord.EvaluateRequest.newBuilder()
                    .setBundleId(state.lastPolicyBundleId)
                    .setQuery(query)
                    .setInputJson(inputJson)
                    .build());
        } catch (StatusRuntimeException e) {
            state.lastHeartbeatException = e;
        }
    }

    @Then("评估结果 allowed=true")
    public void verifyAllowedTrue() {
        assertThat(state.lastEvaluateResponse).isNotNull();
        assertThat(state.lastEvaluateResponse.getAllowed()).isTrue();
    }

    @Then("评估结果 allowed=false")
    public void verifyAllowedFalse() {
        assertThat(state.lastEvaluateResponse).isNotNull();
        assertThat(state.lastEvaluateResponse.getAllowed()).isFalse();
    }

    @Then("评估应返回错误")
    public void verifyEvaluateError() {
        assertThat(state.lastHeartbeatException)
                .as("evaluate on disabled bundle should throw a StatusRuntimeException")
                .isNotNull();
    }

    // ── Explain ───────────────────────────────────────────────

    @When("解释策略 bundle_id query={string} input={string}")
    public void explainPolicy(String query, String inputJson) {
        state.lastExplainResponse = policyStub.explain(Coord.EvaluateRequest.newBuilder()
                .setBundleId(state.lastPolicyBundleId)
                .setQuery(query)
                .setInputJson(inputJson)
                .build());
    }

    @Then("返回解释行数 >= {int}")
    public void verifyExplainLines(int min) {
        assertThat(state.lastExplainResponse).isNotNull();
        assertThat(state.lastExplainResponse.getLinesList().size()).isGreaterThanOrEqualTo(min);
    }
}
