package cn.byteforce.coord.example;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * 集成测试用的 Agent 端点解析（D3：集成测试此前把 {@code localhost:19527} 写死）。
 *
 * <p>写死端点有两个问题：
 * <ul>
 *   <li>CI 里必须占用固定端口 19527，任何端口冲突都会让门禁变红；</li>
 *   <li>无法在同一台机器上并行跑多套测试。</li>
 * </ul>
 *
 * <p>现在端点可由环境变量覆盖（默认保持 {@code localhost:19527}，与文档一致）：
 * <pre>
 *   COORD_AGENT_HOST=127.0.0.1
 *   COORD_AGENT_PORT=19527
 * </pre>
 *
 * <p>本类同时提供 {@link #requireReachable()}：集成测试在缺少 Agent 时应当
 * <b>明确失败</b>（提示如何启动 Agent），而不是等待 30s 连接超时。注意这里刻意
 * <b>不</b>做 {@code assumeTrue} 跳过——"静默跳过"会把"没跑"伪装成"通过"，
 * 这正是本项目此前 chaos 套件被诟病的问题。
 */
final class AgentEndpoint {

    private AgentEndpoint() {
    }

    static String host() {
        return env("COORD_AGENT_HOST", "localhost");
    }

    static int port() {
        String raw = env("COORD_AGENT_PORT", "19527");
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalStateException("COORD_AGENT_PORT is not a number: " + raw, e);
        }
    }

    static String describe() {
        return host() + ":" + port();
    }

    /**
     * 校验 Agent 端口可达；不可达时抛出带排查指引的异常（测试失败而非跳过）。
     */
    static void requireReachable() {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host(), port()), 2000);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "coord-agent is not reachable at " + describe()
                            + ". Start a dev cluster first (see "
                            + "scripts/ci-java-it-cluster.sh) or set COORD_AGENT_HOST/COORD_AGENT_PORT.",
                    e);
        }
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? fallback : value.trim();
    }
}
