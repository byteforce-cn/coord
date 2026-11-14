package cn.byteforce.coord.sdk.mq.v2;

/**
 * Poll request ({@code coord.mq.v2.PollRequest}).
 *
 * <p>{@code startOffset == null} means the field is unset on the wire
 * (server treats it as 0); an explicit value below {@code earliestOffset}
 * fails with {@code OFFSET_OUT_OF_RANGE} instead of silently clamping. An empty
 * list is not "no messages": it means the long poll expired.
 */
public record MqV2Poll(String topic, int partition, long topicEpoch, Long startOffset,
                       int maxMessages, int maxBytes, int waitMs) {}
