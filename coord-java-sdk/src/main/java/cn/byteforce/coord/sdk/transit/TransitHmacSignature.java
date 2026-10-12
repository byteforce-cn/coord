package cn.byteforce.coord.sdk.transit;

/**
 * Result of {@link TransitClient#hmacSignWithPurpose} (G-TR-3 / ADR-0012).
 * <p>
 * {@link #keyId()} is the material identifier that produced the signature
 * (primary or historical). Persist it together with the signature: verification
 * requires it, and after material rotation it is what keeps old signatures
 * computable inside the historical-material window.
 */
public final class TransitHmacSignature {

    private final byte[] signature;
    private final String keyId;
    private final String algorithm;

    public TransitHmacSignature(byte[] signature, String keyId, String algorithm) {
        this.signature = signature == null ? new byte[0] : signature;
        this.keyId = keyId == null ? "" : keyId;
        this.algorithm = algorithm == null ? "" : algorithm;
    }

    /** Raw HMAC bytes (hex-encode at the domain layer if a hex field is required). */
    public byte[] signature() {
        return signature;
    }

    /** Material identifier to persist with the signature; pass back on verify. */
    public String keyId() {
        return keyId;
    }

    /** Algorithm echoed by the server (empty means the server default). */
    public String algorithm() {
        return algorithm;
    }

    @Override
    public String toString() {
        return "TransitHmacSignature{signatureLen=" + signature.length
                + ", keyId='" + keyId + "', algorithm='" + algorithm + "'}";
    }
}
