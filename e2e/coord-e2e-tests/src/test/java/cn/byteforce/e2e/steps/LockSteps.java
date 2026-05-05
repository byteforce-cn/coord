package cn.byteforce.e2e.steps;

import coord.v1.Coord;
import coord.v1.LockServiceGrpc;
import cn.byteforce.e2e.util.RetryHelper;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.grpc.stub.StreamObserver;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class LockSteps {

    @Autowired private LockServiceGrpc.LockServiceBlockingStub lockStub;
    @Autowired private LockServiceGrpc.LockServiceStub lockAsyncStub;
    @Autowired private ScenarioState state;

    // ── Acquire ───────────────────────────────────────────────

    @When("客户端 A 获取锁 {string} ttl={int}s")
    public void acquireLockA(String lockName, int ttl) {
        state.lastLockName = lockName;
        state.lockResponseA = lockStub.acquire(Coord.LockAcquireRequest.newBuilder()
                .setLockName(lockName).setOwner("client-a")
                .setTtlSeconds(ttl).setWait(false).build());
    }

    @Given("客户端 A 持有锁 {string}")
    public void clientAHoldsLock(String lockName) {
        state.lastLockName = lockName;
        state.lockResponseA = lockStub.acquire(Coord.LockAcquireRequest.newBuilder()
                .setLockName(lockName).setOwner("client-a")
                .setTtlSeconds(30).setWait(false).build());
        assertThat(state.lockResponseA.getAcquired()).isTrue();
    }

    @Given("客户端 A 持有锁 {string} ttl={int}s")
    public void clientAHoldsLockWithTtl(String lockName, int ttl) {
        state.lastLockName = lockName;
        state.lockResponseA = lockStub.acquire(Coord.LockAcquireRequest.newBuilder()
                .setLockName(lockName).setOwner("client-a")
                .setTtlSeconds(ttl).setWait(false).build());
        assertThat(state.lockResponseA.getAcquired()).isTrue();
    }

    @When("客户端 B 尝试获取锁 {string} wait=false")
    public void acquireLockBNoWait(String lockName) {
        state.lockResponseB = lockStub.acquire(Coord.LockAcquireRequest.newBuilder()
                .setLockName(lockName).setOwner("client-b")
                .setTtlSeconds(10).setWait(false).build());
    }

    @When("客户端 B 获取锁 {string} ttl={int}s")
    @When("客户端 B 获取锁 {string} wait=false ttl={int}s")
    public void acquireLockBWithTtl(String lockName, int ttl) {
        state.lockResponseB = lockStub.acquire(Coord.LockAcquireRequest.newBuilder()
                .setLockName(lockName).setOwner("client-b")
                .setTtlSeconds(ttl).setWait(false).build());
    }

    @When("客户端 B 等待获取锁 {string} wait=true timeout={int}s")
    public void acquireLockBWait(String lockName, int timeout) {
        ExecutorService exec = Executors.newSingleThreadExecutor();
        Future<Coord.LockAcquireResponse> future = exec.submit(() -> {
            long deadline = System.currentTimeMillis() + timeout * 1000L;
            while (System.currentTimeMillis() < deadline) {
                Coord.LockAcquireResponse r = lockStub.acquire(Coord.LockAcquireRequest.newBuilder()
                        .setLockName(lockName).setOwner("client-b")
                        .setTtlSeconds(10).setWait(true).build());
                if (r.getAcquired()) return r;
                try { Thread.sleep(500); } catch (InterruptedException e) { break; }
            }
            return Coord.LockAcquireResponse.newBuilder().setAcquired(false).build();
        });
        exec.shutdown();
        try {
            state.lockResponseB = future.get(timeout + 5, TimeUnit.SECONDS);
        } catch (Exception e) {
            state.lockResponseB = Coord.LockAcquireResponse.newBuilder().setAcquired(false).build();
        }
    }

    // ── Release ───────────────────────────────────────────────

    @When("客户端 A 释放锁")
    public void releaseLockACurrent() {
        if (state.lockResponseA != null && state.lockResponseA.getAcquired()) {
            lockStub.release(Coord.LockReleaseRequest.newBuilder()
                    .setLockName(state.lastLockName)
                    .setToken(state.lockResponseA.getToken()).build());
        }
    }

    @When("客户端 A 在 {int}s 后释放锁")
    public void releaseLockAAfter(int seconds) {
        new Thread(() -> {
            try { Thread.sleep(seconds * 1000L); } catch (InterruptedException e) { return; }
            if (state.lockResponseA != null && state.lockResponseA.getAcquired()) {
                lockStub.release(Coord.LockReleaseRequest.newBuilder()
                        .setLockName(state.lastLockName)
                        .setToken(state.lockResponseA.getToken()).build());
            }
        }).start();
    }

    // ── Assert ────────────────────────────────────────────────

    @Then("A 持有锁成功")
    public void verifyAHoldsLock() {
        assertThat(state.lockResponseA.getAcquired()).isTrue();
    }

    @Then("B 获取锁失败")
    public void verifyBAcquiredFalse() {
        assertThat(state.lockResponseB.getAcquired()).isFalse();
    }

    @Then("B 持有锁成功")
    @Then("B 在超时前获取锁成功")
    public void verifyBHoldsLock() {
        assertThat(state.lockResponseB.getAcquired()).isTrue();
    }

    // ── KeepAlive ──────────────────────────────────────────────────────────────

    /**
     * 在后台线程每隔 1s 发送 KeepAlive，持续 durationSeconds 秒，
     * 阻止锁因 TTL 到期而被自动释放。
     */
    @When("客户端 A 每 {int}s 发送 KeepAlive 持续 {int}s")
    public void keepAlive(int intervalSec, int durationSec) {
        assertThat(state.lockResponseA).isNotNull();
        assertThat(state.lockResponseA.getAcquired()).isTrue();

        String lockName = state.lastLockName;
        String token = state.lockResponseA.getToken();

        Thread keepAliveThread = new Thread(() -> {
            long deadline = System.currentTimeMillis() + durationSec * 1000L;
            StreamObserver<Coord.LockKeepAliveRequest> requestObserver =
                    lockAsyncStub.keepAlive(new StreamObserver<>() {
                        @Override public void onNext(Coord.LockKeepAliveResponse r) {}
                        @Override public void onError(Throwable t) {}
                        @Override public void onCompleted() {}
                    });
            try {
                while (System.currentTimeMillis() < deadline) {
                    requestObserver.onNext(Coord.LockKeepAliveRequest.newBuilder()
                            .setLockName(lockName)
                            .setToken(token)
                            .setTtlSeconds(10)
                            .build());
                    Thread.sleep(intervalSec * 1000L);
                }
            } catch (InterruptedException ignored) {
            } finally {
                requestObserver.onCompleted();
            }
        });
        keepAliveThread.setDaemon(true);
        keepAliveThread.start();
    }
}
