package cn.byteforce.coord.sdk.mq;

import java.util.Arrays;
import java.util.Objects;

/**
 * A single message pulled from the Coord MQ (data-plane, per-agent redb backend).
 * <p>
 * Offsets are monotonically increasing per {@code (topic, partition)} and serve
 * as the consumption cursor. Acknowledge via {@link MqClient#ack} to commit the
 * consumer-group offset (at-least-once semantics).
 */
public final class MqMessage {

    private final String topic;
    private final int partition;
    private final long offset;
    private final byte[] key;
    private final byte[] payload;
    private final long timestamp;
    private final String dlqReason;
    private final String dlqDetail;

    public MqMessage(String topic, int partition, long offset, byte[] key,
                     byte[] payload, long timestamp) {
        this(topic, partition, offset, key, payload, timestamp, "", "");
    }

    /**
     * Full constructor including DLQ metadata (G-MQ-2). {@code dlqReason} /
     * {@code dlqDetail} are only populated for messages returned by
     * {@link MqClient#pollDlq(String, int, int)}; main-log messages carry empty
     * strings.
     */
    public MqMessage(String topic, int partition, long offset, byte[] key,
                     byte[] payload, long timestamp, String dlqReason, String dlqDetail) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.key = key == null ? new byte[0] : key;
        this.payload = payload == null ? new byte[0] : payload;
        this.timestamp = timestamp;
        this.dlqReason = dlqReason == null ? "" : dlqReason;
        this.dlqDetail = dlqDetail == null ? "" : dlqDetail;
    }

    /** Topic name. */
    public String topic() { return topic; }

    /** Partition number. */
    public int partition() { return partition; }

    /** Monotonic offset within (topic, partition) — the consumption cursor. */
    public long offset() { return offset; }

    /** Optional message key (may be empty). */
    public byte[] key() { return key; }

    /** Message payload. */
    public byte[] payload() { return payload; }

    /** Publish timestamp (Unix ms). */
    public long timestamp() { return timestamp; }

    /**
     * Human-readable reason why the message entered the DLQ (e.g. "poison",
     * "decode_error"); empty for main-log messages and for DLQ entries that
     * carry no reason.
     */
    public String dlqReason() { return dlqReason; }

    /** Free-text detail accompanying {@link #dlqReason()}; empty when none. */
    public String dlqDetail() { return dlqDetail; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MqMessage that)) return false;
        return partition == that.partition
                && offset == that.offset
                && timestamp == that.timestamp
                && Objects.equals(topic, that.topic)
                && Objects.equals(dlqReason, that.dlqReason)
                && Objects.equals(dlqDetail, that.dlqDetail)
                && Arrays.equals(key, that.key)
                && Arrays.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(topic, partition, offset, timestamp, dlqReason, dlqDetail);
        result = 31 * result + Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }

    @Override
    public String toString() {
        return "MqMessage{topic='" + topic + "', partition=" + partition
                + ", offset=" + offset + ", payloadBytes=" + payload.length + "}";
    }
}
