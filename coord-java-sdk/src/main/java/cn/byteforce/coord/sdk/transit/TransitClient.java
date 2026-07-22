package cn.byteforce.coord.sdk.transit;

import cn.byteforce.coord.sdk.CoordException;

/**
 * Envelope encryption API (Transit Service).
 * <p>
 * Provides AES-256-GCM envelope encryption: a local DEK (Data Encryption Key)
 * encrypts the plaintext, and the DEK itself is encrypted with a KEK (Key
 * Encryption Key) stored on the Agent. DEK is used once and discarded.
 *
 * <pre>{@code
 * try (CoordClient client = CoordClient.create(config)) {
 *     TransitClient transit = client.transit();
 *
 *     // Encrypt
 *     byte[] plaintext = "sensitive data".getBytes(StandardCharsets.UTF_8);
 *     byte[] ciphertext = transit.encrypt(plaintext);
 *
 *     // Decrypt
 *     byte[] decrypted = transit.decrypt(ciphertext);
 * }
 * }</pre>
 */
public interface TransitClient {

    /**
     * Encrypt plaintext using envelope encryption.
     * <p>
     * The returned ciphertext contains the encrypted DEK, nonce, and encrypted data
     * in a self-contained packet.
     *
     * @param plaintext the data to encrypt
     * @return encrypted packet (nonce + encrypted DEK + ciphertext)
     * @throws CoordException on encryption or communication failure
     */
    byte[] encrypt(byte[] plaintext);

    /**
     * Encrypt plaintext with additional context binding.
     * <p>
     * Context-bound encryption ensures the ciphertext can only be decrypted
     * with the same context, providing an additional integrity guarantee.
     *
     * @param plaintext the data to encrypt
     * @param context   optional context bytes for additional binding
     * @return encrypted packet
     * @throws CoordException on encryption or communication failure
     */
    byte[] encrypt(byte[] plaintext, byte[] context);

    /**
     * Decrypt a ciphertext packet produced by {@link #encrypt(byte[])}.
     *
     * @param ciphertext the encrypted packet
     * @return decrypted plaintext
     * @throws CoordException on decryption or communication failure
     */
    byte[] decrypt(byte[] ciphertext);

    /**
     * Decrypt a ciphertext packet with context binding.
     *
     * @param ciphertext the encrypted packet
     * @param context    the same context used during encryption
     * @return decrypted plaintext
     * @throws CoordException on decryption or communication failure
     */
    byte[] decrypt(byte[] ciphertext, byte[] context);

    // ──── HMAC 签名与验签 (Phase C.1) ────

    /**
     * Sign data using HMAC-SHA256 (default algorithm).
     *
     * @param data the data to sign
     * @return the signature bytes
     * @throws CoordException on signing or communication failure
     */
    byte[] hmacSign(byte[] data);

    /**
     * Sign data using HMAC with the specified algorithm.
     *
     * @param data      the data to sign
     * @param algorithm the HMAC algorithm ("HMAC-SHA256" or "HMAC-SHA512")
     * @return the signature bytes
     * @throws CoordException on signing or communication failure
     */
    byte[] hmacSign(byte[] data, String algorithm);

    /**
     * Verify an HMAC signature using HMAC-SHA256 (default).
     *
     * @param data      the original data
     * @param signature the signature to verify
     * @return true if the signature is valid
     * @throws CoordException on verification or communication failure
     */
    boolean hmacVerify(byte[] data, byte[] signature);

    /**
     * Verify an HMAC signature with the specified algorithm.
     *
     * @param data      the original data
     * @param signature the signature to verify
     * @param algorithm the HMAC algorithm used ("HMAC-SHA256" or "HMAC-SHA512")
     * @return true if the signature is valid
     * @throws CoordException on verification or communication failure
     */
    boolean hmacVerify(byte[] data, byte[] signature, String algorithm);
}
