package cn.byteforce.coord.sdk.workflow;

/**
 * One event of an instance's approval history, returned by
 * {@link WorkflowClient#history(String)}.
 *
 * <p>{@code node} mirrors the {@code step_name} wire field; it is empty for
 * instance-level events. {@code attributesJson} is the raw JSON object string
 * produced by the server: the SDK never parses or re-serializes it, so callers may
 * hand it to a JSON parser of their choice without a formatting round trip.</p>
 *
 * @param instanceId    the workflow instance the event belongs to
 * @param eventId       deterministic id (stable across repeated reads of the same state)
 * @param eventType     event type, e.g. {@code STEP_ENTERED} / {@code TASK_URGED}
 * @param node          node (step) name; empty for instance-level events
 * @param taskId        seat id for seat-level events; empty otherwise
 * @param actionId      reserved by coord (currently always empty; derived tasks expose
 *                      {@code spawnedByActionId} on the seat instead)
 * @param occurredAtMs  event time (Unix milliseconds)
 * @param attributesJson event attributes as a JSON object string; opaque to the SDK
 */
public record ApprovalHistoryEvent(String instanceId, String eventId, String eventType,
                                   String node, String taskId, String actionId,
                                   long occurredAtMs, String attributesJson) {
}
