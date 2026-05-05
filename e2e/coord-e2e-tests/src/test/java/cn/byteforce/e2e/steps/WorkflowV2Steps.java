package cn.byteforce.e2e.steps;

import coord.v1.Coord;
import coord.v1.WorkflowServiceGrpc;
import cn.byteforce.e2e.util.RetryHelper;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.springframework.beans.factory.annotation.Autowired;

import static org.assertj.core.api.Assertions.assertThat;

public class WorkflowV2Steps {

    @Autowired private WorkflowServiceGrpc.WorkflowServiceBlockingStub workflowStub;
    @Autowired private ScenarioState state;

    // CNCF Serverless Workflow DSL YAML (document/do format)
    private static final String ORDER_WF_YAML = """
            document:
              dsl: "1.0.0"
              namespace: default
              name: order-flow
              version: "1.0"
            do:
              - captureOrder:
                  set:
                    status: captured
              - confirmOrder:
                  set:
                    status: confirmed
            """;

    @When("部署工作流定义 {string} v{string}")
    public void deployDef(String defId, String version) {
        Coord.DeployWorkflowDefinitionResponse r = workflowStub.deployWorkflowDefinition(
                Coord.DeployWorkflowDefinitionRequest.newBuilder()
                        .setDefinitionId(defId)
                        .setVersion(version)
                        .setDefinitionYaml(ORDER_WF_YAML
                                .replace("name: order-flow", "name: " + defId)
                                .replace("version: \"1.0\"", "version: \"" + version + "\""))
                        .build());
        state.workflowDefId = r.getDefinitionId();
        state.workflowDefVersion = r.getVersion();
        assertThat(state.workflowDefId).isNotBlank();
    }

    @Then("返回 definition_id 非空")
    public void verifyDefId() {
        assertThat(state.workflowDefId).isNotBlank();
    }

    @Then("definition_id={string} version={string}")
    public void verifyDefIdVersion(String id, String version) {
        assertThat(state.workflowDefId).isEqualTo(id);
        assertThat(state.workflowDefVersion).isEqualTo(version);
    }

    @Given("已部署工作流定义 {string} v{string}")
    public void givenDeployed(String defId, String version) {
        deployDef(defId, version);
    }

    @When("启动工作流实例 definition={string} version={string} input={string}")
    public void startInstance(String defId, String version, String input) {
        Coord.StartWorkflowV2Response r = workflowStub.startWorkflowV2(
                Coord.StartWorkflowV2Request.newBuilder()
                        .setDefinitionId(defId)
                        .setVersion(version)
                        .setInputJson(input)
                        .build());
        state.workflowInstanceId = r.getInstanceId();
        assertThat(state.workflowInstanceId).isNotBlank();
    }

    /**
     * Regex variant that captures any raw JSON object literal (including "{}") so that
     * scenarios using Gherkin arguments like input={"is_valid":true} or input={} do
     * not need to quote the JSON. The literal `{string}` parameter cannot match these
     * because it expects a double-quoted string.
     */
    @When("^启动工作流实例 definition=\"([^\"]+)\" version=\"([^\"]+)\" input=(\\{.*\\})$")
    public void startInstanceRawJson(String defId, String version, String inputJson) {
        Coord.StartWorkflowV2Response r = workflowStub.startWorkflowV2(
                Coord.StartWorkflowV2Request.newBuilder()
                        .setDefinitionId(defId)
                        .setVersion(version)
                        .setInputJson(inputJson)
                        .build());
        state.workflowInstanceId = r.getInstanceId();
        assertThat(state.workflowInstanceId).isNotBlank();
    }

    @Then("返回 instance_id 非空")
    public void verifyInstanceId() {
        assertThat(state.workflowInstanceId).isNotBlank();
    }

    @Then("实例状态为 {string}")
    public void verifyInstanceStatus(String expected) {
        RetryHelper.await(20).untilAsserted(() -> {
            Coord.GetWorkflowInstanceResponse r = workflowStub.getWorkflowInstance(
                    Coord.GetWorkflowInstanceRequest.newBuilder()
                            .setInstanceId(state.workflowInstanceId).build());
            assertThat(r.getInstance().getStatus()).isEqualTo(expected);
        });
    }

    @When("列出工作流定义")
    public void listDefs() {
        state.workflowDefList = workflowStub.listWorkflowDefinitions(
                Coord.ListWorkflowDefinitionsRequest.newBuilder().build());
    }

    @Then("列表包含 {string} v{string}")
    public void defListContains(String id, String version) {
        assertThat(state.workflowDefList.getDefinitionsList())
                .anyMatch(d -> d.getDefinitionId().equals(id) && d.getVersion().equals(version));
    }

    @When("列出工作流实例 definition={string}")
    public void listInstances(String defId) {
        state.workflowInstanceList = workflowStub.listWorkflowInstances(
                Coord.ListWorkflowInstancesRequest.newBuilder()
                        .setDefinitionId(defId).build());
    }

    @Then("列表包含当前实例")
    public void instanceListContainsCurrent() {
        assertThat(state.workflowInstanceList.getInstancesList())
                .anyMatch(i -> i.getInstanceId().equals(state.workflowInstanceId));
    }

    @When("更新工作流定义 {string} v{string} 修改描述")
    public void updateDef(String defId, String version) {
        String updatedYaml = ORDER_WF_YAML
                .replace("name: order-flow", "name: " + defId)
                .replace("version: \"1.0\"", "version: \"" + version + "\"")
                .replace("namespace: default", "namespace: default\n  summary: Order Flow Updated");
        workflowStub.deployWorkflowDefinition(
                Coord.DeployWorkflowDefinitionRequest.newBuilder()
                        .setDefinitionId(defId)
                        .setVersion(version)
                        .setDefinitionYaml(updatedYaml)
                        .build());
    }

    @Then("定义 {string} v{string} 描述包含 {string}")
    public void defDescContains(String defId, String version, String fragment) {
        Coord.GetWorkflowDefinitionResponse r = workflowStub.getWorkflowDefinition(
                Coord.GetWorkflowDefinitionRequest.newBuilder()
                        .setDefinitionId(defId).setVersion(version).build());
        assertThat(r.getDefinitionYaml()).contains(fragment);
    }

    // ── DSL 控制流场景 ─────────────────────────────────────────────────────────

    @Given("已部署 switch 工作流 {string} v{string}")
    public void deploySwitchWf(String defId, String version) {
        String yaml = """
                document:
                  dsl: "1.0.0"
                  namespace: default
                  name: %s
                  version: "%s"
                do:
                  - routeByValid:
                      switch:
                        - validCase:
                            when: ".is_valid == true"
                            then: setValid
                        - defaultCase:
                            then: setDefault
                  - setValid:
                      set:
                        branch: valid
                      then: end
                  - setDefault:
                      set:
                        branch: default
                      then: end
                """.formatted(defId, version);
        workflowStub.deployWorkflowDefinition(
                Coord.DeployWorkflowDefinitionRequest.newBuilder()
                        .setDefinitionId(defId).setVersion(version).setDefinitionYaml(yaml).build());
        state.workflowDefId = defId;
        state.workflowDefVersion = version;
    }

    @Given("已部署 fork 工作流 {string} v{string}")
    public void deployForkWf(String defId, String version) {
        String yaml = """
                document:
                  dsl: "1.0.0"
                  namespace: default
                  name: %s
                  version: "%s"
                do:
                  - parallelBranches:
                      fork:
                        branches:
                          - branchA:
                              set:
                                resultA: done
                          - branchB:
                              set:
                                resultB: done
                """.formatted(defId, version);
        workflowStub.deployWorkflowDefinition(
                Coord.DeployWorkflowDefinitionRequest.newBuilder()
                        .setDefinitionId(defId).setVersion(version).setDefinitionYaml(yaml).build());
        state.workflowDefId = defId;
        state.workflowDefVersion = version;
    }

    @Given("已部署 for 工作流 {string} v{string}")
    public void deployForWf(String defId, String version) {
        String yaml = """
                document:
                  dsl: "1.0.0"
                  namespace: default
                  name: %s
                  version: "%s"
                do:
                  - iterateItems:
                      for:
                        each: item
                        in: .items
                      do:
                        - processItem:
                            set:
                              processed: true
                """.formatted(defId, version);
        workflowStub.deployWorkflowDefinition(
                Coord.DeployWorkflowDefinitionRequest.newBuilder()
                        .setDefinitionId(defId).setVersion(version).setDefinitionYaml(yaml).build());
        state.workflowDefId = defId;
        state.workflowDefVersion = version;
    }

    @Given("已部署 try-catch 工作流 {string} v{string}")
    public void deployTryCatchWf(String defId, String version) {
        String yaml = """
                document:
                  dsl: "1.0.0"
                  namespace: default
                  name: %s
                  version: "%s"
                do:
                  - guardedStep:
                      try:
                        - raiseError:
                            raise:
                              error:
                                type: "io.coord.simulated"
                                title: "Simulated error"
                                status: 500
                      catch:
                        do:
                          - handleError:
                              set:
                                caught: true
                """.formatted(defId, version);
        workflowStub.deployWorkflowDefinition(
                Coord.DeployWorkflowDefinitionRequest.newBuilder()
                        .setDefinitionId(defId).setVersion(version).setDefinitionYaml(yaml).build());
        state.workflowDefId = defId;
        state.workflowDefVersion = version;
    }

    @Given("已部署 jq 工作流 {string} v{string}")
    public void deployJqWf(String defId, String version) {
        String yaml = """
                document:
                  dsl: "1.0.0"
                  namespace: default
                  name: %s
                  version: "%s"
                do:
                  - countItems:
                      set:
                        itemCount: "${.items | length}"
                """.formatted(defId, version);
        workflowStub.deployWorkflowDefinition(
                Coord.DeployWorkflowDefinitionRequest.newBuilder()
                        .setDefinitionId(defId).setVersion(version).setDefinitionYaml(yaml).build());
        state.workflowDefId = defId;
        state.workflowDefVersion = version;
    }

    @Then("实例上下文中 {string} 为 {string}")
    public void verifyContextString(String key, String expected) {
        RetryHelper.await(20).untilAsserted(() -> {
            Coord.GetWorkflowInstanceResponse r = workflowStub.getWorkflowInstance(
                    Coord.GetWorkflowInstanceRequest.newBuilder()
                            .setInstanceId(state.workflowInstanceId).build());
            String contextJson = r.getInstance().getContextJson();
            assertThat(contextJson)
                    .as("Expected context key '%s' to equal '%s' in: %s", key, expected, contextJson)
                    .contains("\"" + key + "\"")
                    .contains("\"" + expected + "\"");
        });
    }

    @Then("实例上下文中 {string} 为数字 {int}")
    public void verifyContextInt(String key, int expected) {
        RetryHelper.await(20).untilAsserted(() -> {
            Coord.GetWorkflowInstanceResponse r = workflowStub.getWorkflowInstance(
                    Coord.GetWorkflowInstanceRequest.newBuilder()
                            .setInstanceId(state.workflowInstanceId).build());
            String contextJson = r.getInstance().getContextJson();
            assertThat(contextJson)
                    .as("Expected context key '%s' to equal %d in: %s", key, expected, contextJson)
                    .contains("\"" + key + "\":" + expected);
        });
    }
}
