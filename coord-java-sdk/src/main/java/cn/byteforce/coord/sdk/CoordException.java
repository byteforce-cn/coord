package cn.byteforce.coord.sdk;

/**
 * Unified exception for all Coord SDK errors.
 * Carries a structured {@link ErrorCode} — callers MUST use {@link #getErrorCode()}
 * rather than parsing {@link #getMessage()} strings.
 */
public class CoordException extends RuntimeException {

    private final ErrorCode errorCode;

    /**
     * Optional leader-address hint (trailer {@code coord-leader-hint}) attached to
     * structured routing errors ({@link ErrorCode#NOT_LEADER} / non-leader
     * {@link ErrorCode#FAILED_PRECONDITION}); {@code null} when absent.
     * <p>
     * Contract (G-MQ-1): re-route to this address — never parse
     * {@link #getMessage()} text to discover the leader.
     */
    private final String leaderHint;

    public CoordException(ErrorCode errorCode) {
        this(errorCode, errorCode.getProtoName(), null, null);
    }

    public CoordException(ErrorCode errorCode, String message) {
        this(errorCode, message, null, null);
    }

    public CoordException(ErrorCode errorCode, Throwable cause) {
        this(errorCode, errorCode.getProtoName(), cause, null);
    }

    public CoordException(ErrorCode errorCode, String message, Throwable cause) {
        this(errorCode, message, cause, null);
    }

    public CoordException(ErrorCode errorCode, String message, Throwable cause, String leaderHint) {
        super(message, cause);
        this.errorCode = errorCode;
        this.leaderHint = (leaderHint == null || leaderHint.isEmpty()) ? null : leaderHint;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    /**
     * Leader address hint, or {@code null} when the server did not attach one.
     * Attached by data-plane routing errors (see {@link ErrorCode#NOT_LEADER}).
     */
    public String getLeaderHint() {
        return leaderHint;
    }
}
