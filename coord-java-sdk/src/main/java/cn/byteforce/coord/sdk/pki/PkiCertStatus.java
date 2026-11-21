package cn.byteforce.coord.sdk.pki;

/**
 * Result of {@link PkiClient#getCertStatus}.
 * <p>
 * {@code status} is one of {@code active}, {@code retired}, {@code expired} or
 * {@code revoked}. Revocation wins over expiry: an expired serial that was revoked
 * explicitly still reports {@code revoked}. The revocation fields are zero/empty
 * unless {@code status = "revoked"}.
 */
public final class PkiCertStatus {

    private final String serial;
    private final String certSerial;
    private final String commonName;
    private final String status;
    private final long notBeforeEpochSec;
    private final long notAfterEpochSec;
    private final long revokedAtEpochSec;
    private final PkiRevocationReason reason;
    private final String by;

    public PkiCertStatus(String serial, String certSerial, String commonName, String status,
                         long notBeforeEpochSec, long notAfterEpochSec, long revokedAtEpochSec,
                         PkiRevocationReason reason, String by) {
        this.serial = serial;
        this.certSerial = certSerial;
        this.commonName = commonName;
        this.status = status;
        this.notBeforeEpochSec = notBeforeEpochSec;
        this.notAfterEpochSec = notAfterEpochSec;
        this.revokedAtEpochSec = revokedAtEpochSec;
        this.reason = reason;
        this.by = by;
    }

    /** The serial sent in the request (echoed back). */
    public String serial() { return serial; }

    /** Serial embedded in the certificate; empty when only the revocation ledger matched. */
    public String certSerial() { return certSerial; }

    public String commonName() { return commonName; }

    public String status() { return status; }

    public boolean revoked() { return "revoked".equals(status); }

    public long notBeforeEpochSec() { return notBeforeEpochSec; }

    public long notAfterEpochSec() { return notAfterEpochSec; }

    /** 0 unless {@link #revoked()}. */
    public long revokedAtEpochSec() { return revokedAtEpochSec; }

    /** {@link PkiRevocationReason#UNSPECIFIED} unless {@link #revoked()}. */
    public PkiRevocationReason reason() { return reason; }

    /** Empty unless {@link #revoked()}. */
    public String by() { return by; }

    @Override
    public String toString() {
        return "PkiCertStatus{cn='" + commonName + "', serial=" + serial
                + ", status=" + status
                + ", revokedAt=" + (revokedAtEpochSec == 0
                        ? "-" : java.time.Instant.ofEpochSecond(revokedAtEpochSec))
                + "}";
    }
}
