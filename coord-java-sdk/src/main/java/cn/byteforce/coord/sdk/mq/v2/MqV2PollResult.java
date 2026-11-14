package cn.byteforce.coord.sdk.mq.v2;

import java.util.List;

/**
 * Poll response. {@code nextOffset} is the suggested cursor for the next poll
 * (= last message + 1; empty batch = requested start).
 */
public record MqV2PollResult(List<MqV2Message> messages, long nextOffset,
                             long earliestOffset, long latestOffset) {
    public MqV2PollResult {
        messages = messages == null ? List.of() : List.copyOf(messages);
    }
}
