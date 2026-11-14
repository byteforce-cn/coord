package cn.byteforce.coord.sdk.mq.v2;

import java.util.List;

/**
 * Assembly parameters for {@link MqV2Consumer}.
 *
 * <p>{@code sessionTimeoutMs}, {@code maxMessages}, {@code maxBytes} and
 * {@code waitMs} use {@code 0} = server default ({@code [mq_v2]}).
 * {@code heartbeatIntervalMs} overrides the interval the server suggests at
 * Join; {@code 0} keeps the server value (falling back to 10s when the server
 * sends none).
 */
public final class MqV2ConsumerOptions {

    private final String group;
    private final List<String> topics;
    private final String memberId;
    private final int sessionTimeoutMs;
    private final MqV2ResetPolicy resetPolicy;
    private final int maxMessages;
    private final int maxBytes;
    private final int waitMs;
    private final long heartbeatIntervalMs;

    private MqV2ConsumerOptions(Builder builder) {
        this.group = builder.group;
        this.topics = List.copyOf(builder.topics);
        this.memberId = builder.memberId;
        this.sessionTimeoutMs = builder.sessionTimeoutMs;
        this.resetPolicy = builder.resetPolicy;
        this.maxMessages = builder.maxMessages;
        this.maxBytes = builder.maxBytes;
        this.waitMs = builder.waitMs;
        this.heartbeatIntervalMs = builder.heartbeatIntervalMs;
    }

    public static Builder builder(String group, List<String> topics) {
        return new Builder(group, topics);
    }

    public String group() { return group; }

    public List<String> topics() { return topics; }

    public String memberId() { return memberId; }

    public int sessionTimeoutMs() { return sessionTimeoutMs; }

    public MqV2ResetPolicy resetPolicy() { return resetPolicy; }

    public int maxMessages() { return maxMessages; }

    public int maxBytes() { return maxBytes; }

    public int waitMs() { return waitMs; }

    public long heartbeatIntervalMs() { return heartbeatIntervalMs; }

    /** Builder; validates the two fields the server rejects rather than defaulting. */
    public static final class Builder {
        private final String group;
        private final List<String> topics;
        private String memberId = "";
        private int sessionTimeoutMs;
        private MqV2ResetPolicy resetPolicy = MqV2ResetPolicy.FAIL;
        private int maxMessages;
        private int maxBytes;
        private int waitMs;
        private long heartbeatIntervalMs;

        private Builder(String group, List<String> topics) {
            this.group = group;
            this.topics = topics == null ? List.of() : List.copyOf(topics);
        }

        public Builder memberId(String memberId) {
            this.memberId = memberId == null ? "" : memberId;
            return this;
        }

        public Builder sessionTimeoutMs(int sessionTimeoutMs) {
            this.sessionTimeoutMs = sessionTimeoutMs;
            return this;
        }

        public Builder resetPolicy(MqV2ResetPolicy resetPolicy) {
            this.resetPolicy = resetPolicy == null ? MqV2ResetPolicy.FAIL : resetPolicy;
            return this;
        }

        public Builder maxMessages(int maxMessages) {
            this.maxMessages = maxMessages;
            return this;
        }

        public Builder maxBytes(int maxBytes) {
            this.maxBytes = maxBytes;
            return this;
        }

        public Builder waitMs(int waitMs) {
            this.waitMs = waitMs;
            return this;
        }

        public Builder heartbeatIntervalMs(long heartbeatIntervalMs) {
            this.heartbeatIntervalMs = heartbeatIntervalMs;
            return this;
        }

        public MqV2ConsumerOptions build() {
            if (group == null || group.isEmpty()) {
                throw new IllegalArgumentException("consumer group must not be empty");
            }
            if (topics.isEmpty()) {
                throw new IllegalArgumentException("consumer topics must not be empty");
            }
            return new MqV2ConsumerOptions(this);
        }
    }
}
