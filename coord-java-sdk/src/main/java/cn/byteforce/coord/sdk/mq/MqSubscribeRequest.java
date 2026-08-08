package cn.byteforce.coord.sdk.mq;

import java.util.Objects;

/**
 * Parameters for a push-style MQ subscription.
 * <p>
 * The subscription resumes from the consumer group's committed offset:
 * messages already acked by the group are not re-delivered on (re)subscribe.
 */
public final class MqSubscribeRequest {

    private final String topic;
    private final String consumerGroup;

    private MqSubscribeRequest(String topic, String consumerGroup) {
        this.topic = topic;
        this.consumerGroup = consumerGroup;
    }

    /** Topic name. */
    public String topic() {
        return topic;
    }

    /** Consumer group (offset is tracked per group). */
    public String consumerGroup() {
        return consumerGroup;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String topic;
        private String consumerGroup;

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder consumerGroup(String consumerGroup) {
            this.consumerGroup = consumerGroup;
            return this;
        }

        public MqSubscribeRequest build() {
            Objects.requireNonNull(topic, "topic must not be null");
            Objects.requireNonNull(consumerGroup, "consumerGroup must not be null");
            return new MqSubscribeRequest(topic, consumerGroup);
        }
    }

    @Override
    public String toString() {
        return "MqSubscribeRequest{topic='" + topic + "', consumerGroup='" + consumerGroup + "'}";
    }
}
