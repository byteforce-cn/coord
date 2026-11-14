package cn.byteforce.coord.sdk.mq.v2;

import java.util.Map;

/**
 * One publish request. {@code key} / {@code payload} are raw bytes — the SDK
 * never assumes UTF-8; the constructor copies both so a caller reusing its
 * buffers cannot change a request after it was built.
 *
 * <p>{@code topicEpoch} must be the epoch returned by
 * {@link MqV2Client#createTopic}/{@link MqV2Client#describeTopic}; {@code 0} is
 * accepted by the server for read paths only, and a stale epoch fails with
 * {@code TOPIC_EPOCH_STALE} instead of writing into a recreated topic.
 */
public record MqV2Publish(String topic, int partition, long topicEpoch, byte[] key,
                          byte[] payload, String idempotencyKey,
                          Map<String, String> headers, String producer) {

    public MqV2Publish {
        key = key == null ? new byte[0] : key.clone();
        payload = payload == null ? new byte[0] : payload.clone();
        idempotencyKey = idempotencyKey == null ? "" : idempotencyKey;
        headers = headers == null ? Map.of() : Map.copyOf(headers);
        producer = producer == null ? "" : producer;
    }

    @Override
    public String toString() {
        return "MqV2Publish{topic='" + topic + "', partition=" + partition
                + ", topicEpoch=" + topicEpoch + ", keyBytes=" + key.length
                + ", payloadBytes=" + payload.length + "}";
    }
}
