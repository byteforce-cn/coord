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
     * <b>Get-or-create (ISSUE-000)</b>: if an unexpired certificate already
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
     * <b>Get-or-create (ISSUE-000)</b>: idempotent per CN.
     *
     * @param commonName the Common Name (CN) for the certificate
     * @param ttlSeconds TTL for the certificate in seconds (0 for default 24h)
     * @return issued certificate information (PEM-encoded)
     * @throws CoordException if the CA is not initialized or issuance fails
     */
    PkiCertInfo issueCert(String commonName, long ttlSeconds);

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
     * Get the current active certificate of a CN including its private key
     * (recovery path after restart for the key holder).
     *
     * @param commonName the Common Name (CN) to fetch
     * @return current active certificate (with private key)
     * @throws CoordException if no active certificate exists for the CN
     */
    PkiCertInfo getCertByCN(String commonName);

    /**
     * Verify a certificate against the CA.
     *
     * @param certPem the PEM-encoded certificate to verify
     * @return true if the certificate is valid and signed by this CA
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
