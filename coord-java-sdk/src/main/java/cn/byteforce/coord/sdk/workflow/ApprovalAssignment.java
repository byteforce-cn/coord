package cn.byteforce.coord.sdk.workflow;

/**
 * One resolved assignment (席位分配) of an approval seat.
 *
 * <p>Assignments project the seat's principals and their per-principal state —
 * {@code pending} / {@code scheduled} / {@code suspended} / {@code transferred} /
 * {@code delegated} / {@code completed} / {@code rejected} / {@code cancelled} /
 * {@code expired}. {@code vote} is the matching vote record when the principal
 * already voted (directly or on behalf of someone); {@code null} otherwise.</p>
 */
public record ApprovalAssignment(String principal, String status, String resolvedReason,
                                 ApprovalVote vote) {
}
