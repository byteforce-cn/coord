package cn.byteforce.coord.sdk.pki;

/**
 * Certificate usage profile, mapped to exact KeyUsage / ExtendedKeyUsage.
 * <ul>
 *   <li>{@link #SERVER}: KU = digitalSignature | keyEncipherment, EKU = serverAuth</li>
 *   <li>{@link #CLIENT}: KU = digitalSignature, EKU = clientAuth</li>
 *   <li>{@link #BOTH}: KU = digitalSignature | keyEncipherment, EKU = clientAuth + serverAuth</li>
 * </ul>
 * On {@code issueCert} / {@code signCsr}, {@code null} means {@link #BOTH}.
 * On {@code rotateCert}, {@code null} means "inherit the current certificate's usage".
 */
public enum PkiCertUsage {
    BOTH,
    SERVER,
    CLIENT
}
