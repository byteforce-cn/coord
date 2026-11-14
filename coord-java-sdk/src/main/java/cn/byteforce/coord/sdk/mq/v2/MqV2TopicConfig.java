package cn.byteforce.coord.sdk.mq.v2;

/**
 * Topic-level configuration ({@code coord.mq.v2.TopicConfig}).
 *
 * <p>Every field is {@code 0} = "server default" ({@code [mq_v2]}, ADR-0016).
 * The reserved ranges are checked by the server, not the SDK: silent clamping
 * would hide a rejected request.
 */
public record MqV2TopicConfig(long retentionSecs, long maxBytes, int maxMessageBytes,
                              long segmentBytes, long dlqRetentionSecs) {

    /** All fields unset: the server applies {@code [mq_v2]} defaults. */
    public static MqV2TopicConfig defaults() {
        return new MqV2TopicConfig(0, 0, 0, 0, 0);
    }
}
