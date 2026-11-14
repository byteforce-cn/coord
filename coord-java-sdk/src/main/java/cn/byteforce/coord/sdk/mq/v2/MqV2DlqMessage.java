package cn.byteforce.coord.sdk.mq.v2;

/** One DLQ entry: the original message plus why it was moved. */
public record MqV2DlqMessage(MqV2Message message, String reason, String detail) {
    public MqV2DlqMessage {
        reason = reason == null ? "" : reason;
        detail = detail == null ? "" : detail;
    }
}
