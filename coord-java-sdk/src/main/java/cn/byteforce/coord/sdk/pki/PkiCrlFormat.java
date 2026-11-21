package cn.byteforce.coord.sdk.pki;

/**
 * Encoding of the CRL returned by {@link PkiClient#getCrl} /
 * {@link PkiClient#rotateCrl}.
 */
public enum PkiCrlFormat {
    /** PEM text ("-----BEGIN X509 CRL-----"). */
    PEM,
    /** Raw DER bytes. */
    DER
}
