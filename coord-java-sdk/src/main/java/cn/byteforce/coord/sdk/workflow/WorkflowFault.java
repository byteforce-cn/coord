package cn.byteforce.coord.sdk.workflow;

/**
 * Structured workflow fault (the wire projection of an RFC 7807 problem document).
 *
 * <p>Returned by {@link WorkflowStatus#fault()} for failed instances. {@code errorMessage()} on
 * the status remains the compatible copy of {@link #title()}.</p>
 *
 * @param type     standard error type URI (e.g. {@code .../errors/timeout})
 * @param title    human-readable summary
 * @param detail   detailed explanation
 * @param status   suggested HTTP status code
 * @param instance error location as a JSON Pointer (may be empty)
 */
public record WorkflowFault(String type, String title, String detail, int status,
                            String instance) {

    public WorkflowFault {
        type = type == null ? "" : type;
        title = title == null ? "" : title;
        detail = detail == null ? "" : detail;
        instance = instance == null ? "" : instance;
    }
}
