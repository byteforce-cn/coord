package cn.byteforce.coord.sdk.pki;

import cn.byteforce.coord.sdk.CoordException;

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
 *     // Issue a certificate for an agent
 *     PkiCertInfo cert = pki.issueCert("agent-1.myorg.local");
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
     *
     * @param commonName the Common Name (CN) for the certificate
     * @return issued certificate information (PEM-encoded)
     * @throws CoordException if the CA is not initialized or issuance fails
     */
    PkiCertInfo issueCert(String commonName);

    /**
     * Issue a short-lived end-entity certificate with a custom TTL.
     *
     * @param commonName the Common Name (CN) for the certificate
     * @param ttlSeconds TTL for the certificate in seconds (0 for default 24h)
     * @return issued certificate information (PEM-encoded)
     * @throws CoordException if the CA is not initialized or issuance fails
     */
    PkiCertInfo issueCert(String commonName, long ttlSeconds);

    /**
     * Renew an existing certificate, issuing a new certificate with a fresh key pair.
     * <p>
     * The current implementation issues a new certificate with the given serial number
     * used as the common name. Future versions may look up the original certificate
     * by serial number and preserve the original CN.
     *
     * @param serialNumber the serial number of the certificate to renew (hex-encoded)
     * @param ttlSeconds   TTL for the new certificate in seconds (0 for default 24h)
     * @return renewed certificate information (PEM-encoded, new key pair)
     * @throws CoordException if the CA is not initialized or renewal fails
     */
    PkiCertInfo renewCert(String serialNumber, long ttlSeconds);

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
