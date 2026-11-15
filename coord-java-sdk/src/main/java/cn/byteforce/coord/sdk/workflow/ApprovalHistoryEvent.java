package cn.byteforce.coord.sdk.workflow;

/**
 * One event of the instance history projection, returned by
 * {@link WorkflowClient#history(String)}.
 *
 * <p>The projection is deterministic: the same instance state always yields the same
 * {@code (eventId, eventType, occurredAtMs)} triples, so repeated reads are comparable.</p>
 *
 * @param eventId        deterministic event ID (stable across repeated reads)
 * @param eventType      stable UPPER_SNAKE label (e.g. {@code TASK_CREATED},
 *                       {@code ROUND_PASSED}, {@code INSTANCE_COMPLETED})
 * @param node           node (step) name; empty for instance-level events
 * @param taskId         seat ID for seat-level events; empty otherwise
 * @param actionId       client action ID — reserved, currently always empty on the server
 * @param occurredAtMs   event time (Unix ms)
 * @param attributesJson event attributes as the server supplied them (JSON object string);
 *                       passed through verbatim
 */
public record ApprovalHistoryEvent(String eventId, String eventType, String node, String taskId,
                                   String actionId, long occurredAtMs, String attributesJson) {
}
