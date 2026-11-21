package cn.byteforce.coord.sdk.pki;

/**
 * Result of {@link PkiClient#revokeCert}.
 * <p>
 * {@code certSerial} is the serial embedded in the X.509 certificate — the value
 * that appears in the CRL and is checked by third-party verifiers. For certificates
 * issued by current Coord versions {@code serial} and {@code certSerial} are equal.
 */
public final class PkiRevocationInfo {

    private final String serial;
    private final String certSerial;
    private final String commonName;
    private final long revokedAtEpochSec;
    private final PkiRevocationReason reason;
    private final String by;

    public PkiRevocationInfo(String serial, String certSerial, String commonName,
                             long revokedAtEpochSec, PkiRevocationReason reason, String by) {
        this.serial = serial;
        this.certSerial = certSerial;
        this.commonName = commonName;
        this.revokedAtEpochSec = revokedAtEpochSec;
        this.reason = reason;
        this.by = by;
    }

    /** The serial sent in the request (echoed back). */
    public String serial() { return serial; }

    /** Serial embedded in the certificate (CRL entry criterion). */
    public String certSerial() { return certSerial; }

    public String commonName() { return commonName; }

    /** Always {@code "revoked"}. */
    public String status() { return "revoked"; }

    public long revokedAtEpochSec() { return revokedAtEpochSec; }

    public PkiRevocationReason reason() { return reason; }

    /** Authenticated subject that performed the revocation ({@code anonymous} when unauthenticated). */
    public String by() { return by; }

    @Override
    public String toString() {
        return "PkiRevocationInfo{cn='" + commonName + "', serial=" + serial
                + ", certSerial=" + certSerial + ", reason=" + reason
                + ", revokedAt=" + java.time.Instant.ofEpochSecond(revokedAtEpochSec)
                + ", by='" + by + "'}";
    }
}
