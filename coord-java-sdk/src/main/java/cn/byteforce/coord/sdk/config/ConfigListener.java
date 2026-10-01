package cn.byteforce.coord.sdk.config;

/**
 * Listener for configuration change events.
 *
 * <p><b>订阅可能终结，且必须能被获知。</b> watch 断线**不得静默死亡**（不重连、不报错、
 * 不给回调通知会让业务看到一个"看起来正常但永远不再触发"的订阅）：流断开会自动重连
 * 并续订；当订阅真的无法继续时
 * （{@code autoRestoreWatches=false}，或连续重连耗尽），SDK 会调用 {@link #onTerminated}。
 *
 * <p>默认实现只打日志，便于用 lambda 继续当函数式接口用。
 */
@FunctionalInterface
public interface ConfigListener {
    void onEvent(ConfigEvent event);

    /**
     * 订阅已终结（不再有任何事件会被投递）。
     *
     * <p>收到本回调后应重新 {@code watch(...)}，或明确降级为轮询。
     * 默认实现不做事——但"不做事"是调用方的显式选择，而不再是 SDK 替你静默。
     *
     * @param cause 终结原因；{@code null} 表示正常取消
     */
    default void onTerminated(Throwable cause) {
        // no-op by default; override to react (re-subscribe, alert, fall back to polling)
    }
}
