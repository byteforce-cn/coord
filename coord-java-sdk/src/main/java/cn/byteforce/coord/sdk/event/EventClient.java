package cn.byteforce.coord.sdk.event;

import cn.byteforce.coord.sdk.CoordException;

/**
 * Event notification API ({@code coord.event.v1}).
 *
 * <p>Publish/subscribe over the Coord Agent. Semantics are fixed by the contract
 * ({@code apis/contracts/proto/coord/event/v1/event.proto}); the parts a caller must
 * know are:
 *
 * <ul>
 *   <li><b>Delivery is at-least-once while the stream is alive</b>, in publish order
 *       within one subscription;</li>
 *   <li><b>The disconnect window is not replayed.</b> There is no persisted cursor, so
 *       events published while you were disconnected are lost to you. If that matters,
 *       treat events as a <i>hint</i> and re-read authoritative state after
 *       reconnecting (the same discipline as {@code Watch} in
 *       {@code WHITEPAPER.md} §8.3);</li>
 *   <li><b>{@code Unsubscribe} is a compatibility no-op.</b> Subscriptions live in the
 *       gRPC stream itself, so the way to unsubscribe is to
 *       {@link EventSubscription#close() close the subscription} (which cancels the
 *       underlying RPC). {@link #unsubscribe(String)} exists because the contract
 *       declares it, and is documented in the contract as not carrying state.</li>
 * </ul>
 *
 * <pre>{@code
 * try (CoordClient client = CoordClient.create(config)) {
 *     EventClient events = client.events();
 *     try (EventSubscription sub = events.subscribe("order.created", e ->
 *             System.out.println(e.getType() + " " + new String(e.getData())))) {
 *         events.publish("order.created", "order-svc", "{\"id\":1}", "application/json", "order/1");
 *         Thread.sleep(1000);
 *     }
 * }
 * }</pre>
 */
public interface EventClient {

    /**
     * Publish an event.
     *
     * @param eventType       event type; the key subscriptions match on (exact match)
     * @param source          publisher identity (e.g. service name)
     * @param data            raw payload bytes
     * @param dataContentType payload MIME type (e.g. {@code application/json});
     *                        empty string means "unspecified"
     * @param subject         optional business subject; empty string means "none"
     * @return the server-assigned, globally unique event id
     * @throws CoordException on communication failure
     */
    String publish(String eventType, String source, byte[] data,
                   String dataContentType, String subject);

    /**
     * Publish an event with {@code application/json} payload type and no subject.
     */
    default String publish(String eventType, String source, byte[] data) {
        return publish(eventType, source, data, "application/json", "");
    }

    /**
     * Publish an event with a UTF-8 string payload.
     */
    default String publish(String eventType, String source, String data) {
        return publish(eventType, source,
                data == null ? new byte[0] : data.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    /**
     * Subscribe to an event type. The returned subscription must be closed to stop
     * delivery (closing cancels the server stream).
     *
     * @param eventType event type to match exactly; empty string = no filter (all events)
     * @param listener  callback invoked on the SDK's streaming executor
     * @throws CoordException on communication failure while opening the stream
     */
    EventSubscription subscribe(String eventType, EventListener listener);

    /**
     * Declared by the contract, but <b>state-free</b>: the server accepts it and
     * returns success without doing anything, because a subscription is the gRPC
     * stream itself. Kept so that callers who follow the contract literally still
     * compile; use {@link EventSubscription#close()} to actually unsubscribe.
     *
     * @param subscriptionId value previously observed in
     *                       {@link EventSubscription#subscriptionId()}
     * @throws CoordException on communication failure
     */
    void unsubscribe(String subscriptionId);

    /** Receives delivered events. */
    @FunctionalInterface
    interface EventListener {
        /**
         * Called for each delivered event.
         *
         * <p>Callbacks run on the SDK's streaming executor. A slow listener applies
         * back-pressure to the stream, not to other subscriptions.
         */
        void onEvent(CloudEvent event);
    }

    /**
     * A live subscription. Closing it cancels the RPC and stops delivery; it is
     * idempotent.
     */
    interface EventSubscription extends AutoCloseable {

        /**
         * The subscription id reported to {@link EventClient#unsubscribe(String)}.
         *
         * <p>The current wire does not carry a server-allocated id (that is a known
         * contract gap — see {@code STATUS.md}), so the SDK derives one locally
         * deterministically from the event type. Treat it as an opaque label, not as
         * something the server knows.
         */
        String subscriptionId();

        /** Cancel the subscription (idempotent). */
        @Override
        void close();
    }

    /** A delivered event (CloudEvents 1.0 simplified shape). */
    final class CloudEvent {
        private final String id;
        private final String specversion;
        private final String type;
        private final String source;
        private final byte[] data;
        private final String dataContentType;
        private final String subject;
        private final String time;

        public CloudEvent(String id, String specversion, String type, String source,
                          byte[] data, String dataContentType, String subject, String time) {
            this.id = id;
            this.specversion = specversion;
            this.type = type;
            this.source = source;
            this.data = data == null ? new byte[0] : data.clone();
            this.dataContentType = dataContentType;
            this.subject = subject;
            this.time = time;
        }

        /** Server-assigned, globally unique event id. */
        public String getId() {
            return id;
        }

        /** Always {@code "1.0"} per the contract. */
        public String getSpecversion() {
            return specversion;
        }

        /** Event type (the subscription match key). */
        public String getType() {
            return type;
        }

        /** Publisher identity. */
        public String getSource() {
            return source;
        }

        /** Raw payload bytes (defensive copy). */
        public byte[] getData() {
            return data.clone();
        }

        /** Payload interpreted as UTF-8 text (convenience for JSON/plain payloads). */
        public String getDataAsString() {
            return new String(data, java.nio.charset.StandardCharsets.UTF_8);
        }

        /** Payload MIME type; empty string when unspecified. */
        public String getDataContentType() {
            return dataContentType;
        }

        /** Business subject; empty string when none. */
        public String getSubject() {
            return subject;
        }

        /** Server-generated timestamp (RFC3339), or empty when not set. */
        public String getTime() {
            return time;
        }

        @Override
        public String toString() {
            return "CloudEvent{id=" + id + ", type=" + type + ", source=" + source
                    + ", subject=" + subject + ", time=" + time + "}";
        }
    }
}
