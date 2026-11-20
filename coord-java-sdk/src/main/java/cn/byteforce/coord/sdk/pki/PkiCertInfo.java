package cn.byteforce.coord.sdk.pki;

/**
 * Information about an issued certificate — returned by the Coord Agent PKI service.
 */
public final class PkiCertInfo {

    private final String commonName;
    private final String certPem;
    /** 托管私钥的命名密钥名（材料在 coord，ADR-0019 D6） */
    private final String keyName;
    /** 签名使用的密钥版本 */
    private final int keyVersion;
    private final long notBeforeEpochSec;
    private final long notAfterEpochSec;
    private final String serial;
    /** active / retired */
    private final String status;
    /** rotation chain: serial this cert was rotated from (empty if first issued) */
    private final String parentSerial;

    public PkiCertInfo(String commonName, String certPem, String keyName, int keyVersion,
                       long notBeforeEpochSec, long notAfterEpochSec, String serial) {
        this(commonName, certPem, keyName, keyVersion, notBeforeEpochSec, notAfterEpochSec, serial,
                "active", "");
    }

    public PkiCertInfo(String commonName, String certPem, String keyName, int keyVersion,
                       long notBeforeEpochSec, long notAfterEpochSec, String serial,
                       String status, String parentSerial) {
        this.commonName = commonName;
        this.certPem = certPem;
        this.keyName = keyName;
        this.keyVersion = keyVersion;
        this.notBeforeEpochSec = notBeforeEpochSec;
        this.notAfterEpochSec = notAfterEpochSec;
        this.serial = serial;
        this.status = status;
        this.parentSerial = parentSerial;
    }

    public String commonName() { return commonName; }
    public String certPem() { return certPem; }
    public String keyName() { return keyName; }
    public int keyVersion() { return keyVersion; }
    public long notBeforeEpochSec() { return notBeforeEpochSec; }
    public long notAfterEpochSec() { return notAfterEpochSec; }
    public String serial() { return serial; }
    public String status() { return status; }
    public String parentSerial() { return parentSerial; }

    @Override
    public String toString() {
        return "PkiCertInfo{cn='" + commonName + "', serial=" + serial
                + ", status=" + status
                + ", notBefore=" + java.time.Instant.ofEpochSecond(notBeforeEpochSec)
                + ", notAfter=" + java.time.Instant.ofEpochSecond(notAfterEpochSec) + "}";
    }
}
