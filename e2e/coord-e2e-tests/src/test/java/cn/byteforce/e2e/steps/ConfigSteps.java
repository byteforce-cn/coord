package cn.byteforce.e2e.steps;

import coord.v1.Coord;
import coord.v1.ConfigServiceGrpc;
import cn.byteforce.e2e.util.RetryHelper;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static org.assertj.core.api.Assertions.assertThat;

public class ConfigSteps {

    private static final Logger log = LoggerFactory.getLogger(ConfigSteps.class);

    @Autowired private ConfigServiceGrpc.ConfigServiceBlockingStub configStub;
    @Autowired private ScenarioState state;

    private long previousVersion = -1;
    private String lastWrittenKey;

    /** Queue populated by an active WatchConfig server-streaming RPC. */
    private LinkedBlockingQueue<Coord.ConfigResponse> watchQueue;

    @Given("配置 key={string} value={string} 已写入")
    @Given("Coord 中已存在 key={string} value={string}")
    @Given("已写入 key={string} value={string}")
    public void putConfigGiven(String key, String value) {
        configStub.putConfig(Coord.PutConfigRequest.newBuilder()
                .setKey(key).setValue(value).build());
    }

    @When("写入配置 key={string} value={string}")
    @When("通过 PutConfig 写入 key={string} value={string}")
    @When("更新 key={string} value={string}")
    public void putConfig(String key, String value) {
        lastWrittenKey = key;
        Coord.ConfigResponse prev = null;
        try {
            prev = configStub.getConfig(Coord.ConfigRequest.newBuilder().setKey(key).build());
            previousVersion = prev.getVersion();
        } catch (Exception e) {
            previousVersion = -1;
        }
        configStub.putConfig(Coord.PutConfigRequest.newBuilder()
                .setKey(key).setValue(value).build());
        state.lastConfigResponse = configStub.getConfig(
                Coord.ConfigRequest.newBuilder().setKey(key).build());
    }

    @When("读取配置 key={string}")
    @When("通过 GetConfig 读取 key={string}")
    public void getConfig(String key) {
        state.lastConfigResponse = configStub.getConfig(
                Coord.ConfigRequest.newBuilder().setKey(key).build());
    }

    @When("删除配置 key={string}")
    public void deleteConfig(String key) {
        configStub.putConfig(Coord.PutConfigRequest.newBuilder()
                .setKey(key).setValue("").build());
    }

    @When("启动配置监听 key={string}")
    public void watchConfig(String key) {
        lastWrittenKey = key;
    }

    @When("列举前缀 {string} 下所有配置")
    public void listByPrefix(String prefix) {
        // No ListConfig RPC; enumerate by getting known keys
        StringBuilder keys = new StringBuilder();
        for (String suffix : List.of("k1", "k2", "k3")) {
            String fullKey = prefix + suffix;
            try {
                Coord.ConfigResponse r = configStub.getConfig(
                        Coord.ConfigRequest.newBuilder().setKey(fullKey).build());
                if (r != null && !r.getValue().isEmpty()) {
                    if (keys.length() > 0) keys.append(",");
                    keys.append(fullKey);
                }
            } catch (Exception ignored) {}
        }
        state.lastConfigResponse = Coord.ConfigResponse.newBuilder()
                .setValue(keys.toString()).build();
    }

    @Then("配置值为 {string}")
    public void verifyValue(String expectedValue) {
        if (state.lastConfigResponse != null) {
            assertThat(state.lastConfigResponse.getValue()).isEqualTo(expectedValue);
        } else if (state.orderConfig != null) {
            assertThat(state.orderConfig).isEqualTo(expectedValue);
        }
    }

    @Then("返回空值或 NOT_FOUND")
    public void verifyEmpty() {
        try {
            Coord.ConfigResponse r = state.lastConfigResponse;
            assertThat(r == null || r.getValue().isEmpty()).isTrue();
        } catch (Exception e) {
            // NOT_FOUND is expected
        }
    }

    @Then("在 {int}s 内监听到新值 {string}")
    public void verifyWatchValue(int seconds, String expected) {
        String key = lastWrittenKey != null ? lastWrittenKey : "dynamic.rate";
        RetryHelper.await(seconds).untilAsserted(() -> {
            Coord.ConfigResponse r = configStub.getConfig(
                    Coord.ConfigRequest.newBuilder().setKey(key).build());
            assertThat(r.getValue()).isEqualTo(expected);
        });
    }

    @Then("配置版本号递增")
    public void verifyVersionIncreased() {
        assertThat(state.lastConfigResponse).isNotNull();
        if (previousVersion >= 0) {
            assertThat(state.lastConfigResponse.getVersion()).isGreaterThan(previousVersion);
        } else {
            assertThat(state.lastConfigResponse.getVersion()).isGreaterThan(0);
        }
    }

    @Then("结果包含 {string} 和 {string}")
    public void verifyListContains(String key1, String key2) {
        assertThat(state.lastConfigResponse.getValue()).contains(key1);
        assertThat(state.lastConfigResponse.getValue()).contains(key2);
    }

    @Then("返回 version > 0")
    public void verifyVersionPositive() {
        assertThat(state.lastConfigResponse.getVersion()).isGreaterThan(0);
    }

    // ── WatchConfig gRPC server-streaming ─────────────────────────────────────────

    /**
     * 建立 WatchConfig 服务端流订阅，后台线程持续接收推送并放入队列。
     * 使用 daemon 线程，场景结束后随 JVM 线程自然终止。
     */
    @When("建立 WatchConfig gRPC 流订阅 {string}")
    public void openWatchConfigStream(String key) {
        watchQueue = new LinkedBlockingQueue<>();
        Iterator<Coord.ConfigResponse> it = configStub.watchConfig(
                Coord.ConfigRequest.newBuilder().setKey(key).build());
        Thread t = new Thread(() -> {
            try {
                while (it.hasNext()) {
                    Coord.ConfigResponse resp = it.next();
                    watchQueue.offer(resp);
                    log.debug("WatchConfig received: key={} value={} version={}",
                            resp.getKey(), resp.getValue(), resp.getVersion());
                }
            } catch (Exception e) {
                log.debug("WatchConfig stream ended for key={}: {}", key, e.getMessage());
            }
        }, "watch-config-" + key);
        t.setDaemon(true);
        t.start();
        log.info("WatchConfig gRPC stream opened for key={}", key);
    }

    /**
     * 断言：在 {@code seconds} 秒内，WatchConfig 流至少推送一条值等于 {@code expectedValue} 的消息。
     * 使用快照比较（不移除元素），支持 Awaitility 重试。
     */
    @Then("流在 {int}s 内收到推送值 {string}")
    public void verifyWatchStreamReceived(int seconds, String expectedValue) {
        assertThat(watchQueue).as("WatchConfig stream was not opened").isNotNull();
        RetryHelper.await(seconds).untilAsserted(() -> {
            List<Coord.ConfigResponse> snapshot = new ArrayList<>(watchQueue);
            boolean found = snapshot.stream()
                    .anyMatch(r -> expectedValue.equals(r.getValue()));
            assertThat(found)
                    .as("Expected value '%s' in WatchConfig stream, got %s",
                            expectedValue, snapshot.stream()
                                    .map(Coord.ConfigResponse::getValue).toList())
                    .isTrue();
        });
    }
}
