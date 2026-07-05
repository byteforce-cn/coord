package cn.byteforce.coord.sdk.registry;

/**
 * Service discovery filter mode.
 *
 * <p>Controls whether {@link Registry#discover} matches the service name
 * exactly, by prefix, or returns all known services. Default is {@link #EXACT}
 * for backward compatibility.
 *
 * <h3>Proto mapping</h3>
 * <table>
 *   <tr><th>Enum</th><th>Proto value</th><th>Behavior</th></tr>
 *   <tr><td>{@link #EXACT}</td><td>0 ({@code FILTER_MODE_UNSPECIFIED})</td><td>Exact match</td></tr>
 *   <tr><td>{@link #PREFIX}</td><td>2 ({@code FILTER_MODE_PREFIX})</td><td>Prefix match</td></tr>
 *   <tr><td>{@link #ALL}</td><td>3 ({@code FILTER_MODE_ALL})</td><td>All services</td></tr>
 * </table>
 */
public enum FilterMode {
    /** Exact match (default, maps to proto {@code FILTER_MODE_UNSPECIFIED}). */
    EXACT(0),
    /** Prefix match — returns all services whose name starts with the given prefix. */
    PREFIX(2),
    /** All services — returns every registered service instance. */
    ALL(3);

    private final int protoValue;

    FilterMode(int protoValue) {
        this.protoValue = protoValue;
    }

    /** Map to the gRPC proto {@code FilterMode} enum value. */
    public int toProtoValue() {
        return protoValue;
    }

    /** Restore from proto int value (0 → EXACT, 2 → PREFIX, 3 → ALL). */
    public static FilterMode fromProtoValue(int v) {
        return switch (v) {
            case 2 -> PREFIX;
            case 3 -> ALL;
            default -> EXACT;
        };
    }
}
