package cn.byteforce.coord.sdk.mq.v2;

/**
 * Move one main-log offset into the partition's DLQ. {@code reason} is a stable
 * anchor (e.g. {@code poison}, {@code workflow_trigger_max_attempts}) that
 * operators can grep for.
 *
 * <p>{@code group} / {@code failureType} / {@code attempts} carry the delivery
 * attribution into the DLQ record: a partition DLQ is shared by every consumer
 * group, so "which group burned this message, after how many deliveries and with
 * what failure class" can only be answered if it travels with the record.
 */
public record MqV2MoveToDlq(String topic, int partition, long topicEpoch, long offset,
                            String reason, String detail, String group, String failureType,
                            int attempts) {
    public MqV2MoveToDlq {
        reason = reason == null ? "" : reason;
        detail = detail == null ? "" : detail;
        group = group == null ? "" : group;
        failureType = failureType == null ? "" : failureType;
        if (attempts < 0) {
            throw new IllegalArgumentException("attempts must be >= 0");
        }
    }

    /** Move without attribution (management action, e.g. manual triage). */
    public MqV2MoveToDlq(String topic, int partition, long topicEpoch, long offset,
                         String reason, String detail) {
        this(topic, partition, topicEpoch, offset, reason, detail, "", "", 0);
    }
}
