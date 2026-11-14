package cn.byteforce.coord.sdk.mq.v2;

import java.util.Map;

/**
 * One entry of {@code PublishBatch}. The whole batch is atomic on the server:
 * a single oversized entry or quota breach appends nothing.
 */
public record MqV2BatchEntry(byte[] key, byte[] payload, String idempotencyKey,
                             Map<String, String> headers, String producer) {
    public MqV2BatchEntry {
        key = key == null ? new byte[0] : key.clone();
        payload = payload == null ? new byte[0] : payload.clone();
        idempotencyKey = idempotencyKey == null ? "" : idempotencyKey;
        headers = headers == null ? Map.of() : Map.copyOf(headers);
        producer = producer == null ? "" : producer;
    }

    @Override
    public String toString() {
        return "MqV2BatchEntry{keyBytes=" + key.length + ", payloadBytes=" + payload.length + "}";
    }
}
