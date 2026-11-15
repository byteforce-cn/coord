package cn.byteforce.coord.sdk.mq.v2;

/**
 * One DLQ entry: the original message plus why it was moved and by whom.
 *
 * <p>{@code group} is the consumer group whose deliveries moved it (empty for a
 * management action), {@code failureType} the structured failure class and
 * {@code attempts} the delivery count at the moment it was dead-lettered.
 */
public record MqV2DlqMessage(MqV2Message message, String reason, String detail,
                             String group, String failureType, int attempts) {
    public MqV2DlqMessage {
        reason = reason == null ? "" : reason;
        detail = detail == null ? "" : detail;
        group = group == null ? "" : group;
        failureType = failureType == null ? "" : failureType;
    }

    public MqV2DlqMessage(MqV2Message message, String reason, String detail) {
        this(message, reason, detail, "", "", 0);
    }
}
