package cn.byteforce.coord.sdk.pki;

/**
 * Information about an issued certificate — returned by the Coord Agent PKI service.
 */
public final class PkiCertInfo {

    private final String commonName;
    private final String certPem;
    private final String keyPem;
    private final long notBeforeEpochSec;
    private final long notAfterEpochSec;
    private final String serial;

    public PkiCertInfo(String commonName, String certPem, String keyPem,
                       long notBeforeEpochSec, long notAfterEpochSec, String serial) {
        this.commonName = commonName;
        this.certPem = certPem;
        this.keyPem = keyPem;
        this.notBeforeEpochSec = notBeforeEpochSec;
        this.notAfterEpochSec = notAfterEpochSec;
        this.serial = serial;
    }

    public String commonName() { return commonName; }
    public String certPem() { return certPem; }
    public String keyPem() { return keyPem; }
    public long notBeforeEpochSec() { return notBeforeEpochSec; }
    public long notAfterEpochSec() { return notAfterEpochSec; }
    public String serial() { return serial; }

    @Override
    public String toString() {
        return "PkiCertInfo{cn='" + commonName + "', serial=" + serial
                + ", notBefore=" + java.time.Instant.ofEpochSecond(notBeforeEpochSec)
                + ", notAfter=" + java.time.Instant.ofEpochSecond(notAfterEpochSec) + "}";
    }
}
