package cn.byteforce.coord.sdk.mq.v2;

import java.util.List;

/**
 * Join (or rejoin) a consumer group. An empty {@code memberId} registers a new
 * member; a non-empty one resumes a live session without advancing the
 * generation. Every subscribed topic must already exist (fail-closed).
 */
public record MqV2JoinGroup(String group, String memberId, List<String> topics,
                            int sessionTimeoutMs) {
    public MqV2JoinGroup {
        memberId = memberId == null ? "" : memberId;
        topics = topics == null ? List.of() : List.copyOf(topics);
    }
}
