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

    // ──── HMAC 签名与验签（G-TR-3 / ADR-0012）────

    /**
     * Sign data with a purpose-bound HMAC key (default algorithm HMAC-SHA256),
     * using the current primary material.
     * <p>
     * The purpose is a required opaque label; different purposes derive different
     * keys and are mutually unverifiable. Persist {@code result.keyId()} with the
     * signature — verification requires it.
     *
     * @param data    the data to sign
     * @param purpose required purpose label (non-empty, no NUL, &le;128 bytes)
     * @return signature bytes plus the signing material id
     * @throws CoordException {@code INVALID_ARGUMENT} when the purpose is missing/invalid
     */
    TransitHmacSignature hmacSignWithPurpose(byte[] data, String purpose);

    /** Same as {@link #hmacSignWithPurpose(byte[], String)} with an explicit algorithm. */
    TransitHmacSignature hmacSignWithPurpose(byte[] data, String purpose, String algorithm);

    /**
     * Sign with an explicit material id (rotation window). Used to recompute
     * hash variants with a historical material for cross-rotation duplicate
     * detection (DRV-005); new writes must leave {@code keyId} empty/primary.
     *
     * @throws CoordException {@code FAILED_PRECONDITION} when the material is not injected
     */
    TransitHmacSignature hmacSignWithPurpose(byte[] data, String purpose, String algorithm, String keyId);

    /**
     * Verify a signature against the given purpose and material id.
     * <p>
     * Both parameters are required: the material id selects the key version used
     * at signing time. A missing (not injected) material fails loudly with
     * {@code FAILED_PRECONDITION} instead of returning {@code false}.
     *
     * @return true when the signature matches
     */
    boolean hmacVerifyWithPurpose(byte[] data, byte[] signature, String purpose, String keyId);

    /** Same as {@link #hmacVerifyWithPurpose(byte[], byte[], String, String)} with an explicit algorithm. */
    boolean hmacVerifyWithPurpose(byte[] data, byte[] signature, String purpose, String keyId, String algorithm);

    // ──── KEK 材料迁移（G-TR-1）────

    /**
     * Re-wrap a persisted DEK with the current primary KEK material (management
     * path; capability {@code coord:transit:crypto}).
     * <p>
     * Used during KEK rotation: while the old material is still injected
     * ({@code COORD_TRANSIT_KEK_OLD} / {@code transit-kek-old.txt}), call this for
     * every still-needed DEK so it no longer depends on the old material; only
     * then may the old material be removed. The reply carries the new
     * {@code dek_id} and the material id it is now wrapped with.
     *
     * @param dekId id of the DEK to migrate ({@code dek_id} as carried in stored
     *              packets / records)
     * @return the new dek id and material id
     * @throws CoordException on communication failure; {@code NOT_FOUND} when the
     *                        DEK is unknown or expired; {@code FAILED_PRECONDITION}
     *                        when the DEK's material is not injected (fail-loud —
     *                        re-inject the historical material first)
     */
    TransitRewrapResult rewrap(String dekId);
}
