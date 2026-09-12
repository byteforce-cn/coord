package cn.byteforce.coord.example;

/**
 * 集成测试的**每次运行唯一**命名空间（D3 附带修复）。
 *
 * <p>问题：这些集成测试此前从未被 CI 执行，因此没人发现它们默认自己独占一个空
 * 命名空间——例如断言 {@code version == 1}、断言某前缀下恰好 N 个 key。在一台
 * 复用的 dev 集群上连跑两次就会失败（第二次是 {@code version == 2}、key 累计翻倍）。
 * 这类"只能跑一次"的测试在 CI 上会被当成 flaky。
 *
 * <p>做法：把命名空间前缀用一次运行唯一 ID 隔开，测试之间、运行之间互不干扰：
 * <pre>
 *   Namespace.unique("/test/kv/hello/")  →  "/test/kv/hello/1f3a9c2b/"
 * </pre>
 *
 * <p>可用 {@code COORD_TEST_RUN_ID} 固定该 ID（便于复现/排查同一次运行的残留数据）。
 */
final class Namespace {

    private static final String RUN_ID = resolveRunId();

    private Namespace() {
    }

    static String unique(String base) {
        return base + RUN_ID + "/";
    }

    /** 唯一命名空间编号（拼接在给定前缀之后）。 */
    static String runId() {
        return RUN_ID;
    }

    private static String resolveRunId() {
        String env = System.getenv("COORD_TEST_RUN_ID");
        if (env != null && !env.isBlank()) {
            return env.trim();
        }
        // nanoTime 的十六进制：进程内唯一，且不会与上一次运行的 key 冲突
        return Long.toHexString(System.nanoTime());
    }
}
