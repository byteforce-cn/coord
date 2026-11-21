package cn.byteforce.coord.sdk.pki;

import cn.byteforce.coord.sdk.CoordException;

import java.util.List;

/**
 * PKI CA certificate management API.
 * <p>
 * Provides CA initialization, end-entity certificate issuance, and certificate
 * verification backed by the Coord Agent's PKI service via gRPC.
 *
 * <pre>{@code
 * try (CoordClient client = CoordClient.create(config)) {
 *     PkiClient pki = client.pki();
 *
 *     // Initialize the CA (idempotent)
 *     pki.initCa("MyOrg Root CA");
 *
 *     // Issue a certificate for an agent (idempotent get-or-create by CN)
 *     PkiCertInfo cert = pki.issueCert("agent-1.myorg.local");
 *
 *     // Rotate: new active key, old kept until notAfter (multi-kid verification)
 *     PkiCertInfo rotated = pki.rotateCert("agent-1.myorg.local");
 *
 *     // List current + retired certs for a CN (build multi-kid JWKS by serial)
 *     List<PkiCertSummary> certs = pki.listCerts("agent-1.myorg.local");
 *
 *     // Verify a certificate
 *     boolean valid = pki.verifyCert(cert.certPem());
 *
 *     // Get CA certificate for trust store
 *     String caPem = pki.getCaCert();
 * }
 * }</pre>
 */
public interface PkiClient {

    /**
     * Initialize the CA with a self-signed root certificate.
     * Idempotent: if already initialized, this is a no-op.
     *
     * @param caCommonName the Common Name (CN) for the CA certificate
     * @throws CoordException on communication or initialization failure
     */
    void initCa(String caCommonName);

    /**
     * Issue a short-lived end-entity certificate signed by the CA.
     * <p>
     * <b>Get-or-create</b>: if an unexpired certificate already
     * exists for the CN, the same certificate/key pair is returned. Idempotent.
     *
     * @param commonName the Common Name (CN) for the certificate
     * @return issued certificate information (PEM-encoded)
     * @throws CoordException if the CA is not initialized or issuance fails
     */
    PkiCertInfo issueCert(String commonName);

    /**
     * Issue a short-lived end-entity certificate with a custom TTL.
     * <p>
     * <b>Get-or-create</b>: idempotent per CN.
     *
     * @param commonName the Common Name (CN) for the certificate
     * @param ttlSeconds TTL for the certificate in seconds (0 for default 24h)
     * @return issued certificate information (PEM-encoded)
     * @throws CoordException if the CA is not initialized or issuance fails
     */
    PkiCertInfo issueCert(String commonName, long ttlSeconds);

    /**
     * Issue a certificate with an explicit SAN set (contract fields
     * {@code dns_names} / {@code ip_addresses}).
     * <p>
     * <b>Get-or-create</b> per CN, same as the shorter overloads. Both lists
     * empty ⇒ the server derives a DNS SAN from {@code commonName} and rejects
     * a CN that is not a legal DNS name ({@code INVALID_ARGUMENT}); it never
     * falls back to a SAN-less certificate silently.
     *
     * @param commonName    the Common Name (CN) for the certificate
     * @param ttlSeconds    TTL for the certificate in seconds (0 for default 24h)
     * @param dnsNames      DNS SANs; empty ⇒ derived from {@code commonName}
     * @param ipAddresses   IP SANs; empty ⇒ no IP SAN
     * @return issued certificate information (PEM-encoded)
     * @throws CoordException if the CA is not initialized or issuance fails
     */
    PkiCertInfo issueCert(String commonName, long ttlSeconds, List<String> dnsNames,
                          List<String> ipAddresses);

    /**
     * Issue a certificate with an explicit usage profile (exact KU/EKU).
     * <p>
     * {@code usage == null} means {@link PkiCertUsage#BOTH}. The idempotency key
     * includes the usage, so changing it re-issues (old certificate becomes retired).
     *
     * @param commonName    the Common Name (CN) for the certificate
     * @param ttlSeconds    TTL for the certificate in seconds (0 for default 24h)
     * @param dnsNames      DNS SANs; empty ⇒ derived from {@code commonName}
     * @param ipAddresses   IP SANs; empty ⇒ no IP SAN
     * @param usage         certificate usage; null ⇒ BOTH
     * @return issued certificate information
     * @throws CoordException if the CA is not initialized or issuance fails
     */
    PkiCertInfo issueCert(String commonName, long ttlSeconds, List<String> dnsNames,
                          List<String> ipAddresses, PkiCertUsage usage);

    /**
     * Renew an existing certificate by serial number, issuing a new certificate
     * with a fresh key pair under the <b>original CN</b> (looked up by serial).
     * The old certificate stays valid until {@code notAfter}.
     *
     * @param serialNumber the serial number of the certificate to renew (hex-encoded)
     * @param ttlSeconds   TTL for the new certificate in seconds (0 for default 24h)
     * @return renewed certificate information (PEM-encoded, new key pair, original CN)
     * @throws CoordException if the CA is not initialized or renewal fails
     */
    PkiCertInfo renewCert(String serialNumber, long ttlSeconds);

    /**
     * Explicitly rotate the active certificate of a CN: issues a new active
     * certificate and marks the old one {@code retired} (kept until notAfter
     * for verification).
     *
     * @param commonName the Common Name (CN) to rotate
     * @return the new active certificate information
     * @throws CoordException on rotation failure
     */
    PkiCertInfo rotateCert(String commonName);

    /**
     * Explicitly rotate the active certificate of a CN with a custom TTL.
     *
     * @param commonName the Common Name (CN) to rotate
     * @param ttlSeconds TTL for the new certificate in seconds (0 for default 24h)
     * @return the new active certificate information
     * @throws CoordException on rotation failure
     */
    PkiCertInfo rotateCert(String commonName, long ttlSeconds);

    /**
     * Explicitly rotate the active certificate of a CN with a custom TTL and an
     * explicit SAN set (contract fields {@code dns_names} / {@code ip_addresses}).
     * <p>
     * Both lists empty ⇒ the server inherits the SANs of the current active
     * certificate (deriving from the CN only when no certificate record exists),
     * so a rotation never drops the SANs by accident.
     *
     * @param commonName    the Common Name (CN) to rotate
     * @param ttlSeconds    TTL for the certificate in seconds (0 for default 24h)
     * @param dnsNames      DNS SANs; empty ⇒ inherited from the current active cert
     * @param ipAddresses   IP SANs; empty ⇒ inherited from the current active cert
     * @return the new active certificate information
     * @throws CoordException on rotation failure
     */
    PkiCertInfo rotateCert(String commonName, long ttlSeconds, List<String> dnsNames,
                           List<String> ipAddresses);

    /**
     * Rotate with an explicit usage profile. {@code usage == null} inherits the
     * current certificate's usage (or BOTH when no certificate exists).
     *
     * @param commonName    the Common Name (CN) to rotate
     * @param ttlSeconds    TTL for the certificate in seconds (0 for default 24h)
     * @param dnsNames      DNS SANs; empty ⇒ inherited from the current active cert
     * @param ipAddresses   IP SANs; empty ⇒ inherited from the current active cert
     * @param usage         certificate usage; null ⇒ inherit
     * @return the new active certificate information
     * @throws CoordException on rotation failure
     */
    PkiCertInfo rotateCert(String commonName, long ttlSeconds, List<String> dnsNames,
                           List<String> ipAddresses, PkiCertUsage usage);

    /**
     * List the current (active) and not-yet-expired retired certificates of a CN.
     * <p>
     * Verification parties use the returned {@code serial} as the JWKS {@code kid}
     * to build a multi-key JWKS (old + new keys valid during rotation overlap).
     *
     * @param commonName the Common Name (CN) to list
     * @return current + historical unexpired certificates (no private keys)
     * @throws CoordException on listing failure
     */
    List<PkiCertSummary> listCerts(String commonName);

    /**
     * Get the current active certificate of a CN (certificate + key reference,
     * no private key material; ADR-0019 D6 keeps the key in Coord's transit archive).
     *
     * @param commonName the Common Name (CN) to fetch
     * @return current active certificate and its {@code keyName} / {@code keyVersion}
     * @throws CoordException if no active certificate exists for the CN
     */
    PkiCertInfo getCertByCN(String commonName);

    /**
     * Read a certificate by serial (active, retired, or expired) without any
     * private key material. Unknown serials surface as {@code NOT_FOUND}.
     *
     * @param serialNumber the hex-encoded serial (as returned by issue/rotate)
     * @return certificate summary (no key material)
     * @throws CoordException if the CA is not initialized or the serial is unknown
     */
    PkiCertSummary getCertBySerial(String serialNumber);

    /**
     * Sign a PKCS#10 CSR with this CA (proof-of-possession is verified server-side).
     * <p>
     * The private key never leaves the caller; the returned {@link PkiCertInfo}
     * therefore has an empty {@code keyName} and {@code keyVersion == 0}. CN and
     * SANs come from the CSR subject / extension request. A CSR without SANs
     * derives a DNS SAN from its CN.
     * <p>
     * Idempotent per CN + public key + profile: the same CSR returns the same
     * certificate; a new key for the same CN retires the old certificate.
     *
     * @param csrDer      PKCS#10 CSR in DER form
     * @param ttlSeconds  TTL for the certificate in seconds (0 for default 24h)
     * @param usage       certificate usage; null ⇒ BOTH
     * @return signed certificate (no key material)
     * @throws CoordException on invalid CSR (including PoP failure) or signing failure
     */
    PkiCertInfo signCsr(byte[] csrDer, long ttlSeconds, PkiCertUsage usage);

    /**
     * Sign a PKCS#10 CSR with the default TTL and {@link PkiCertUsage#BOTH}.
     *
     * @param csrDer PKCS#10 CSR in DER form
     * @return signed certificate (no key material)
     * @throws CoordException on invalid CSR (including PoP failure) or signing failure
     */
    PkiCertInfo signCsr(byte[] csrDer);

    /**
     * Verify a certificate against the CA: validity window, issuer, and the CA
     * public-key signature. Malformed PEM is rejected as an invalid argument.
     *
     * @param certPem the PEM-encoded certificate to verify
     * @return true only if the certificate is in its validity window and signed by this CA
     * @throws CoordException on communication or verification failure
     */
    boolean verifyCert(String certPem);

    /**
     * Get the CA certificate in PEM format.
     *
     * @return the CA certificate PEM string
     * @throws CoordException if the CA is not initialized
     */
    String getCaCert();
}
