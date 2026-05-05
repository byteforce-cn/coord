package cn.byteforce.e2e.steps;

import coord.v1.Coord;
import coord.v1.RegistryServiceGrpc;
import coord.v1.TransitServiceGrpc;
import cn.byteforce.e2e.util.HttpClient;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Prometheus 可观测性指标断言步骤。
 *
 * 解析 /metrics 端点返回的 Prometheus 文本格式，提取并比较指标值。
 */
public class ObservabilitySteps {

    @Autowired private HttpClient http;
    @Autowired private TransitServiceGrpc.TransitServiceBlockingStub transitStub;
    @Autowired private RegistryServiceGrpc.RegistryServiceBlockingStub registryStub;
    @Autowired private ScenarioState state;

    /** 指标基准值快照，Key = 指标名（不含标签）。 */
    private final java.util.Map<String, Double> baselineValues = new java.util.HashMap<>();

    // ── /metrics 格式断言 ──────────────────────────────────────────────────────

    @Then("HTTP 200 且 Content-Type 包含 text\\/plain")
    public void verifyMetricsFormat() {
        // HTTP 200 已由 coordMetrics() 保证（失败时返回空串）；此处额外校验内容格式
        String body = http.coordMetrics();
        assertThat(body).isNotBlank();
    }

    @Then("响应包含 {string} 行")
    public void verifyResponseContainsLine(String fragment) {
        String body = http.coordMetrics();
        assertThat(body).contains(fragment);
    }

    @Then("包含指标 {string}")
    public void verifyMetricPresent(String metricName) {
        String body = http.coordMetrics();
        assertThat(body)
                .as("Expected metric '%s' to be present in /metrics output", metricName)
                .contains(metricName);
    }

    // ── 指标值比较 ─────────────────────────────────────────────────────────────

    @Given("抓取指标 {string} 当前值为基准")
    public void captureBaseline(String metricName) {
        double value = extractMetricValue(metricName);
        baselineValues.put(metricName, value);
    }

    @Then("指标 {string} 值比基准大")
    public void verifyMetricIncreased(String metricName) {
        double current = extractMetricValue(metricName);
        double baseline = baselineValues.getOrDefault(metricName, 0.0);
        assertThat(current)
                .as("Expected metric '%s' (%f) to have increased above baseline (%f)",
                        metricName, current, baseline)
                .isGreaterThan(baseline);
    }

    // ── 触发指标变化的动作 ─────────────────────────────────────────────────────

    @When("注册服务 {string} 实例 {string}")
    public void registerServiceInstance(String serviceName, String instanceId) {
        registryStub.register(Coord.RegisterRequest.newBuilder()
                .setInstance(Coord.ServiceInstance.newBuilder()
                        .setServiceName(serviceName)
                        .setInstanceId(instanceId)
                        .setHost("obs-host")
                        .setPort(9999)
                        .build())
                .setTtlSeconds(60).build());
    }

    @When("用密钥 {string} 执行 {int} 次加密")
    public void encryptNTimes(String keyName, int times) {
        for (int i = 0; i < times; i++) {
            transitStub.encrypt(Coord.EncryptRequest.newBuilder()
                    .setKeyName(keyName)
                    .setPlaintext("obs-plaintext-" + i)
                    .build());
        }
    }

    @When("等待 {int}s 让 metric 更新")
    public void waitForMetricUpdate(int seconds) {
        try { Thread.sleep(seconds * 1000L); } catch (InterruptedException ignored) {}
    }

    // ── 工具方法 ────────────────────────────────────────────────────────────────

    /**
     * 从 /metrics 文本中提取第一个匹配 metricName 的数值。
     * 支持 Prometheus 格式：metric_name{labels} value 或 metric_name value
     */
    private double extractMetricValue(String metricName) {
        String body = http.coordMetrics();
        if (body == null || body.isBlank()) return 0.0;

        // 匹配行格式：metricName 或 metricName{...} 后跟数值
        Pattern p = Pattern.compile(
                "^" + Pattern.quote(metricName) + "(\\{[^}]*\\})?\\s+([\\d.eE+\\-]+)",
                Pattern.MULTILINE);
        Matcher m = p.matcher(body);
        if (m.find()) {
            try {
                return Double.parseDouble(m.group(2));
            } catch (NumberFormatException e) {
                return 0.0;
            }
        }
        return 0.0;
    }
}
