package cn.byteforce.coord.sdk.mq.v2;

import java.util.Map;

/**
 * One message returned by {@code Poll} / {@code PollDlq}.
 *
 * <p>{@code offset} is the main-log offset even for DLQ messages; the DLQ's own
 * cursor is {@link MqV2PollDlqResult#nextDlqOffset()}. {@code key} / {@code payload}
 * are raw bytes (never assumed UTF-8) and are copied on construction.
 * {@code dlqReason} / {@code dlqDetail} are empty for main-log messages.
 */
public record MqV2Message(String topic, int partition, long offset, long timestampMs,
                          byte[] key, byte[] payload, Map<String, String> headers,
                          String producer, String dlqReason, String dlqDetail) {

    public MqV2Message {
        key = key == null ? new byte[0] : key.clone();
        payload = payload == null ? new byte[0] : payload.clone();
        headers = headers == null ? Map.of() : Map.copyOf(headers);
        producer = producer == null ? "" : producer;
        dlqReason = dlqReason == null ? "" : dlqReason;
        dlqDetail = dlqDetail == null ? "" : dlqDetail;
    }

    /**
     * Metadata only: a payload can be a credential or a binary blob, and a log
     * or exception that prints it cannot be scrubbed afterwards.
     */
    @Override
    public String toString() {
        return "MqV2Message{topic='" + topic + "', partition=" + partition
                + ", offset=" + offset + ", timestampMs=" + timestampMs
                + ", payloadBytes=" + payload.length + ", headers=" + headers.size() + "}";
    }
}
