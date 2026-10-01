package cn.byteforce.coord.sdk;

/**
 * Unified exception for all Coord SDK errors.
 * Carries a structured {@link ErrorCode} — callers MUST use {@link #getErrorCode()}
 * rather than parsing {@link #getMessage()} strings.
 */
public class CoordException extends RuntimeException {

    private final ErrorCode errorCode;

    public CoordException(ErrorCode errorCode) {
        super(errorCode.getProtoName());
        this.errorCode = errorCode;
    }

    public CoordException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public CoordException(ErrorCode errorCode, Throwable cause) {
        super(errorCode.getProtoName(), cause);
        this.errorCode = errorCode;
    }

    public CoordException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }
}
