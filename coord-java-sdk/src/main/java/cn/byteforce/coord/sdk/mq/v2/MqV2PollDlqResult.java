package cn.byteforce.coord.sdk.mq.v2;

import java.util.List;

/** DLQ page; {@code nextDlqOffset} is the DLQ's own cursor, not the main-log offset. */
public record MqV2PollDlqResult(List<MqV2DlqMessage> messages, long nextDlqOffset) {
    public MqV2PollDlqResult {
        messages = messages == null ? List.of() : List.copyOf(messages);
    }
}
