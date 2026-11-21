package cn.byteforce.coord.sdk.pki;

import java.nio.charset.StandardCharsets;

/**
 * A CA-signed certificate revocation list.
 * <p>
 * {@code crlNumber} is monotonically increasing and changes only when the content
 * changes (or on an explicit {@link PkiClient#rotateCrl}). Consumers must refresh
 * before {@code nextUpdateEpochSec}; Coord has no HTTP CRL endpoint, so caching and
 * refresh cadence are the caller's responsibility.
 */
public final class PkiCrl {

    private final byte[] crl;
    private final PkiCrlFormat format;
    private final long crlNumber;
    private final long thisUpdateEpochSec;
    private final long nextUpdateEpochSec;

    public PkiCrl(byte[] crl, PkiCrlFormat format, long crlNumber,
                  long thisUpdateEpochSec, long nextUpdateEpochSec) {
        this.crl = crl.clone();
        this.format = format;
        this.crlNumber = crlNumber;
        this.thisUpdateEpochSec = thisUpdateEpochSec;
        this.nextUpdateEpochSec = nextUpdateEpochSec;
    }

    /** CRL bytes in {@link #format()}. */
    public byte[] crl() { return crl.clone(); }

    public PkiCrlFormat format() { return format; }

    /** PEM text when {@link #format()} is {@link PkiCrlFormat#PEM}, otherwise null. */
    public String pem() {
        return format == PkiCrlFormat.PEM ? new String(crl, StandardCharsets.UTF_8) : null;
    }

    public long crlNumber() { return crlNumber; }

    public long thisUpdateEpochSec() { return thisUpdateEpochSec; }

    public long nextUpdateEpochSec() { return nextUpdateEpochSec; }

    @Override
    public String toString() {
        return "PkiCrl{number=" + crlNumber + ", format=" + format
                + ", thisUpdate=" + java.time.Instant.ofEpochSecond(thisUpdateEpochSec)
                + ", nextUpdate=" + java.time.Instant.ofEpochSecond(nextUpdateEpochSec) + "}";
    }
}
