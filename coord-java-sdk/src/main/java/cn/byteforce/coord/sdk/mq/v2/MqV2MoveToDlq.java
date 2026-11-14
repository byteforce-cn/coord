package cn.byteforce.coord.sdk.mq.v2;

/**
 * Move one main-log offset into the partition's DLQ. {@code reason} is a stable
 * anchor (e.g. {@code poison}, {@code workflow_trigger_max_attempts}) that
 * operators can grep for.
 */
public record MqV2MoveToDlq(String topic, int partition, long topicEpoch, long offset,
                            String reason, String detail) {
    public MqV2MoveToDlq {
        reason = reason == null ? "" : reason;
        detail = detail == null ? "" : detail;
    }
}
