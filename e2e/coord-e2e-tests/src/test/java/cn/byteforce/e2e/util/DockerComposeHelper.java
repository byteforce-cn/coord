package cn.byteforce.e2e.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 通过 Docker CLI（挂载 /var/run/docker.sock）控制容器的启停，
 * 供集群容错测试场景使用。
 *
 * <h3>运行环境要求</h3>
 * <ul>
 *   <li>标准 Docker Socket 模式（默认）：e2e-tests 容器挂载 /var/run/docker.sock</li>
 *   <li>Docker-in-Docker（CI）：设置 DOCKER_HOST 环境变量，例如
 *       {@code DOCKER_HOST=tcp://docker:2375}；此时无需挂载 socket 文件</li>
 * </ul>
 *
 * 若 Docker 不可用（如受限 CI 环境），调用 {@link #isDockerAvailable()} 返回 false，
 * 测试步骤应通过 {@code Assumptions.assumeTrue(docker.isDockerAvailable())} 跳过。
 */
@Component
public class DockerComposeHelper {
    private static final Logger log = LoggerFactory.getLogger(DockerComposeHelper.class);

    /** 所有 Coord 节点的容器名（与 docker-compose.yml container_name 保持一致）。 */
    private static final List<String> NODE_CONTAINERS = List.of("coord-1", "coord-2", "coord-3");

    /**
     * DOCKER_HOST 环境变量（可选）。当设置时，覆盖默认 /var/run/docker.sock，
     * 支持 TCP 远程 Docker daemon（用于 Docker-in-Docker CI 场景）。
     */
    private static final String DOCKER_HOST = System.getenv("DOCKER_HOST");

    /** 缓存的 Docker 可用性检测结果（避免每次检查）。 */
    private Boolean dockerAvailableCache;

    @Value("${coord.http.address:http://localhost:8080}")
    private String coordHttpAddress;

    private String lastStoppedContainer;
    private final List<String> stoppedContainers = new ArrayList<>();

    /**
     * 检查 Docker CLI 是否可用（socket 文件存在 或 DOCKER_HOST 已配置）。
     * 结果被缓存以避免重复检测。
     */
    public boolean isDockerAvailable() {
        if (dockerAvailableCache != null) return dockerAvailableCache;
        // If DOCKER_HOST env is set, assume Docker TCP endpoint is available
        if (DOCKER_HOST != null && !DOCKER_HOST.isBlank()) {
            log.info("Docker host configured via DOCKER_HOST={}", DOCKER_HOST);
            dockerAvailableCache = probeDocker();
            return dockerAvailableCache;
        }
        // Check standard Unix socket
        boolean socketExists = new File("/var/run/docker.sock").exists();
        if (!socketExists) {
            log.warn("Docker socket /var/run/docker.sock not found. "
                    + "Cluster failover tests will be skipped. "
                    + "Set DOCKER_HOST or mount the socket to enable them.");
            dockerAvailableCache = false;
            return false;
        }
        dockerAvailableCache = probeDocker();
        return dockerAvailableCache;
    }

    private boolean probeDocker() {
        try {
            ProcessBuilder pb = buildDockerCommand("docker", "info", "--format", "{{.ServerVersion}}");
            pb.redirectErrorStream(true);
            Process p = pb.start();
            int exit = p.waitFor(5, java.util.concurrent.TimeUnit.SECONDS) ? p.exitValue() : -1;
            if (exit == 0) {
                log.info("Docker is available");
                return true;
            }
            log.warn("'docker info' exited {}, Docker may not be available", exit);
            return false;
        } catch (Exception e) {
            log.warn("Docker probe failed: {}", e.getMessage());
            return false;
        }
    }

    /** 停止一个 Follower 节点（非 Leader）。 */
    public String stopFollower() {
        for (String container : NODE_CONTAINERS) {
            if (!isLeader(container)) {
                stopContainer(container);
                lastStoppedContainer = container;
                stoppedContainers.add(container);
                log.info("Stopped follower container: {}", container);
                return container;
            }
        }
        throw new IllegalStateException("No follower found to stop");
    }

    /** 停止 Leader 节点。 */
    public String stopLeader() {
        for (String container : NODE_CONTAINERS) {
            if (isLeader(container)) {
                stopContainer(container);
                lastStoppedContainer = container;
                stoppedContainers.add(container);
                log.info("Stopped leader container: {}", container);
                return container;
            }
        }
        throw new IllegalStateException("No leader found to stop");
    }

    /** 停止指定数量的节点（从后往前，保留 coord-1 最久）。 */
    public List<String> stopNodes(int count) {
        List<String> stopped = new ArrayList<>();
        List<String> candidates = new ArrayList<>(NODE_CONTAINERS);
        java.util.Collections.reverse(candidates);
        for (String c : candidates) {
            if (stopped.size() >= count) break;
            stopContainer(c);
            stopped.add(c);
            stoppedContainers.add(c);
        }
        log.info("Stopped {} nodes: {}", count, stopped);
        return stopped;
    }

    /** 重启最后一个被停止的容器。 */
    public void restoreLastStopped() {
        if (lastStoppedContainer != null) {
            startContainer(lastStoppedContainer);
            stoppedContainers.remove(lastStoppedContainer);
            log.info("Restored container: {}", lastStoppedContainer);
            lastStoppedContainer = null;
        }
    }

    /** 重启所有被停止的容器。 */
    public void restoreAllStopped() {
        for (String c : new ArrayList<>(stoppedContainers)) {
            startContainer(c);
            log.info("Restored container: {}", c);
        }
        stoppedContainers.clear();
        lastStoppedContainer = null;
    }

    public String getLastStoppedContainer() {
        return lastStoppedContainer;
    }

    // ── private helpers ────────────────────────────────────────────────────────

    private boolean isLeader(String containerName) {
        try {
            ProcessBuilder pb = buildDockerCommand(
                    "docker", "exec", containerName,
                    "sh", "-c", "curl -sf http://localhost:8080/api/v1/role | grep -q '^leader '");
            pb.redirectErrorStream(true);
            Process p = pb.start();
            return p.waitFor() == 0;
        } catch (Exception e) {
            log.debug("isLeader check failed for {}: {}", containerName, e.getMessage());
            return false;
        }
    }

    private void stopContainer(String containerName) {
        exec("docker", "stop", containerName);
    }

    private void startContainer(String containerName) {
        exec("docker", "start", containerName);
    }

    /**
     * Builds a ProcessBuilder with DOCKER_HOST injected into the environment
     * when configured (supports both socket and TCP Docker-in-Docker setups).
     */
    private ProcessBuilder buildDockerCommand(String... cmd) {
        ProcessBuilder pb = new ProcessBuilder(cmd);
        if (DOCKER_HOST != null && !DOCKER_HOST.isBlank()) {
            pb.environment().put("DOCKER_HOST", DOCKER_HOST);
        }
        return pb;
    }

    private void exec(String... cmd) {
        try {
            ProcessBuilder pb = buildDockerCommand(cmd);
            pb.redirectErrorStream(true);
            Process p = pb.start();
            int exit = p.waitFor();
            if (exit != 0) {
                String output = new String(p.getInputStream().readAllBytes());
                log.warn("docker command {} exited {}: {}", java.util.Arrays.asList(cmd), exit, output);
            }
        } catch (IOException | InterruptedException e) {
            log.warn("docker command failed: {}", e.getMessage(), e);
        }
    }
}
