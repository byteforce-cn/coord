package cn.byteforce.coord.sdk.transit;

/**
 * Result of {@link TransitClient#rewrap(String)} (G-TR-1): the DEK has been
 * unwrapped with its original KEK material and re-wrapped with the current
 * primary material. The old {@code dek_id} is retired by the same operation.
 */
public final class TransitRewrapResult {

    private final String newDekId;
    private final String kekId;

    public TransitRewrapResult(String newDekId, String kekId) {
        this.newDekId = newDekId == null ? "" : newDekId;
        this.kekId = kekId == null ? "" : kekId;
    }

    /** The new {@code dek_id} (persist it if your workflow tracks DEK ids). */
    public String newDekId() {
        return newDekId;
    }

    /** Material identifier the DEK is now wrapped with (the current primary). */
    public String kekId() {
        return kekId;
    }

    @Override
    public String toString() {
        return "TransitRewrapResult{newDekId='" + newDekId + "', kekId='" + kekId + "'}";
    }
}
