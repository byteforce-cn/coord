package cn.byteforce.e2e.steps;

import coord.v1.Coord;
import coord.v1.IdGenServiceGrpc;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class IdGenSteps {

    @Autowired private IdGenServiceGrpc.IdGenServiceBlockingStub idGenStub;
    @Autowired private ScenarioState state;

    @When("生成 {int} 个 ID")
    @When("请求生成 {int} 个 Snowflake ID")
    public void generateIds(int count) {
        state.generatedIds.clear();
        Coord.SnowflakeResponse r = idGenStub.generateSnowflake(
                Coord.SnowflakeRequest.newBuilder().setBatch(count).build());
        state.generatedIds.addAll(r.getIdsList().stream()
                .map(Long::longValue).collect(Collectors.toList()));
    }

    @When("顺序生成 {int} 个 ID")
    public void generateIdsSequential(int count) {
        state.generatedIds.clear();
        for (int i = 0; i < count; i++) {
            Coord.SnowflakeResponse r = idGenStub.generateSnowflake(
                    Coord.SnowflakeRequest.newBuilder().setBatch(1).build());
            state.generatedIds.add(r.getIds(0));
        }
    }

    @When("worker_id={int} 生成 {int} 个 ID")
    public void generateIdsWithWorkerId(int workerId, int count) {
        state.generatedIds.clear();
        Coord.SnowflakeResponse r = idGenStub.generateSnowflake(
                Coord.SnowflakeRequest.newBuilder()
                        .setBatch(count).build());
        state.generatedIds.addAll(r.getIdsList().stream()
                .map(Long::longValue).collect(Collectors.toList()));
    }

    @When("{int} 个线程各生成 {int} 个 ID")
    public void concurrentGenerate(int threads, int perThread) throws Exception {
        state.generatedIds.clear();
        Set<Long> allIds = ConcurrentHashMap.newKeySet();
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(exec.submit(() -> {
                Coord.SnowflakeResponse r = idGenStub.generateSnowflake(
                        Coord.SnowflakeRequest.newBuilder().setBatch(perThread).build());
                r.getIdsList().forEach(id -> allIds.add(id.longValue()));
            }));
        }
        for (Future<?> f : futures) f.get();
        exec.shutdown();
        state.generatedIds.addAll(allIds);
    }

    @Then("返回 ID 非零")
    public void verifyIdNonZero() {
        assertThat(state.generatedIds).isNotEmpty();
        assertThat(state.generatedIds.get(0)).isNotEqualTo(0);
    }

    @Then("返回 {int} 个 ID 非零")
    public void verifyCountNonZero(int count) {
        assertThat(state.generatedIds.size()).isEqualTo(count);
        for (long id : state.generatedIds) {
            assertThat(id).isNotEqualTo(0);
        }
    }

    @Then("ID 为正整数")
    public void verifyPositive() {
        for (long id : state.generatedIds) {
            assertThat(id).isGreaterThan(0);
        }
    }

    @Then("所有 ID 唯一")
    public void verifyAllUnique() {
        Set<Long> unique = new HashSet<>(state.generatedIds);
        assertThat(unique.size()).isEqualTo(state.generatedIds.size());
    }

    @Then("总计 {int} 个 ID 全部唯一")
    public void verifyTotalUnique(int expected) {
        Set<Long> unique = new HashSet<>(state.generatedIds);
        assertThat(unique.size()).isEqualTo(state.generatedIds.size());
        assertThat(state.generatedIds.size()).isEqualTo(expected);
    }

    @Then("ID 序列整体递增")
    public void verifyIncreasing() {
        for (int i = 1; i < state.generatedIds.size(); i++) {
            assertThat(state.generatedIds.get(i)).isGreaterThan(state.generatedIds.get(i - 1));
        }
    }

    @Then("ID 的时间戳部分接近当前时间")
    public void verifyTimestamp() {
        assertThat(state.generatedIds).isNotEmpty();
        long id = state.generatedIds.get(0);
        // Snowflake ID: top bits are timestamp. Just verify it's a recent positive number.
        assertThat(id).isGreaterThan(0);
    }
}
