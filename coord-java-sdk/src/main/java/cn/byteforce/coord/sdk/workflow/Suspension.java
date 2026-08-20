package cn.byteforce.coord.sdk.workflow;

/**
 * 挂起元信息 —— 工作流实例当前挂起原因与恢复信息（ISSUE-010 §2）。
 *
 * <p>对应 proto {@code SuspensionMeta}：
 * <ul>
 *   <li>{@code reason}：挂起原因（wait / call / listen / signal / run / retry）</li>
 *   <li>{@code untilMs}：wait / retry 到期时间（Unix ms，可为 0）</li>
 *   <li>{@code expectedSignal}：人工审批挂起时期望的 signal 名（可为空）</li>
 *   <li>{@code eventType}：listen 事件类型（可为空）</li>
 *   <li>{@code service}：call 目标服务（可为空）</li>
 * </ul>
 *
 * <p>宿主可据此把 {@code execution_progress} 与 coord 实例精确对齐（含审批驳回 / 分支路由后的目标状态）。
 */
public final class Suspension {

    private final String reason;
    private final long untilMs;
    private final String expectedSignal;
    private final String eventType;
    private final String service;

    public Suspension(String reason, long untilMs, String expectedSignal,
                      String eventType, String service) {
        this.reason = reason != null ? reason : "";
        this.untilMs = untilMs;
        this.expectedSignal = expectedSignal != null ? expectedSignal : "";
        this.eventType = eventType != null ? eventType : "";
        this.service = service != null ? service : "";
    }

    public String reason() {
        return reason;
    }

    public long untilMs() {
        return untilMs;
    }

    public String expectedSignal() {
        return expectedSignal;
    }

    public String eventType() {
        return eventType;
    }

    public String service() {
        return service;
    }

    @Override
    public String toString() {
        return "Suspension{reason='" + reason + "', untilMs=" + untilMs
                + ", expectedSignal='" + expectedSignal + "', eventType='" + eventType
                + "', service='" + service + "'}";
    }
}
