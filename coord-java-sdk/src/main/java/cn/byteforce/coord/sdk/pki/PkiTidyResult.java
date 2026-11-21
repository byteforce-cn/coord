package cn.byteforce.coord.sdk.pki;

/** Result of {@link PkiClient#tidy}. */
public final class PkiTidyResult {

    private final long retentionSeconds;
    private final long removedCerts;

    public PkiTidyResult(long retentionSeconds, long removedCerts) {
        this.retentionSeconds = retentionSeconds;
        this.removedCerts = removedCerts;
    }

    /** Retention actually applied (server default when the request used 0). */
    public long retentionSeconds() { return retentionSeconds; }

    /** Certificate records reclaimed (active + history). Revocations are never reclaimed. */
    public long removedCerts() { return removedCerts; }

    @Override
    public String toString() {
        return "PkiTidyResult{retentionSeconds=" + retentionSeconds
                + ", removedCerts=" + removedCerts + "}";
    }
}
