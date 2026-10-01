package cn.byteforce.coord.sdk.pki;

/**
 * Summary of a certificate for a CN — returned by {@link PkiClient#listCerts(String)}.
 * <p>
 * Contains no private key: listing is intended for verification parties to build a
 * multi-key JWKS (using {@code serial} as the {@code kid}) covering both the active
 * and not-yet-expired retired certificates during rotation overlap.
 */
public final class PkiCertSummary {

    private final String commonName;
    private final String certPem;
    private final long notBeforeEpochSec;
    private final long notAfterEpochSec;
    private final String serial;
    /** active / retired */
    private final String status;
    /** rotation chain: serial this cert was rotated from (empty if first issued) */
    private final String parentSerial;

    public PkiCertSummary(String commonName, String certPem, long notBeforeEpochSec,
                          long notAfterEpochSec, String serial, String status, String parentSerial) {
        this.commonName = commonName;
        this.certPem = certPem;
        this.notBeforeEpochSec = notBeforeEpochSec;
        this.notAfterEpochSec = notAfterEpochSec;
        this.serial = serial;
        this.status = status;
        this.parentSerial = parentSerial;
    }

    public String commonName() { return commonName; }
    public String certPem() { return certPem; }
    public long notBeforeEpochSec() { return notBeforeEpochSec; }
    public long notAfterEpochSec() { return notAfterEpochSec; }
    public String serial() { return serial; }
    public String status() { return status; }
    public String parentSerial() { return parentSerial; }

    @Override
    public String toString() {
        return "PkiCertSummary{cn='" + commonName + "', serial=" + serial
                + ", status=" + status
                + ", notAfter=" + java.time.Instant.ofEpochSecond(notAfterEpochSec) + "}";
    }
}
