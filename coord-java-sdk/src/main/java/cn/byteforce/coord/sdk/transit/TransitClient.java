package cn.byteforce.coord.sdk.transit;

import cn.byteforce.coord.contracts.transit.v1.TransitDecryptBatchItem;
import cn.byteforce.coord.contracts.transit.v1.TransitDecryptRequest;
import cn.byteforce.coord.contracts.transit.v1.TransitDecryptResponse;
import cn.byteforce.coord.contracts.transit.v1.TransitEncryptBatchItem;
import cn.byteforce.coord.contracts.transit.v1.TransitEncryptRequest;
import cn.byteforce.coord.contracts.transit.v1.TransitEncryptResponse;
import cn.byteforce.coord.contracts.transit.v1.TransitExportKeyResponse;
import cn.byteforce.coord.contracts.transit.v1.TransitGenerateDataKeyResponse;
import cn.byteforce.coord.contracts.transit.v1.TransitKeyInfo;
import cn.byteforce.coord.contracts.transit.v1.TransitKeyType;
import cn.byteforce.coord.contracts.transit.v1.TransitRewrapBatchItem;
import cn.byteforce.coord.contracts.transit.v1.TransitRewrapRequest;
import cn.byteforce.coord.contracts.transit.v1.TransitRewrapResponse;
import cn.byteforce.coord.contracts.transit.v1.TransitSignBatchItem;
import cn.byteforce.coord.contracts.transit.v1.TransitSignRequest;
import cn.byteforce.coord.contracts.transit.v1.TransitSignResponse;
import cn.byteforce.coord.contracts.transit.v1.TransitVerifyBatchItem;
import cn.byteforce.coord.contracts.transit.v1.TransitVerifyRequest;

import java.util.List;

/**
 * Transit / KMS API（ADR-0019）：命名密钥 + 版本边界 + 非对称签名 + 按名信封加密。
 *
 * <p>密钥材料留在 coord；调用方只持有 {@code key_name} / {@code key_version}。
 * 密文是自描述的 {@code CCT3} 包（含 key name 与版本），解密不需要服务端 DEK 记录。
 *
 * <pre>{@code
 * try (CoordClient client = CoordClient.create(config)) {
 *     TransitClient transit = client.transit();
 *     transit.createKey("app-key", TransitKeyType.AES256_GCM96);
 *     TransitEncryptResponse ct = transit.encrypt("app-key", plaintext);
 *     byte[] pt = transit.decrypt(ct.getCiphertext()).getPlaintext().toByteArray();
 * }
 * }</pre>
 */
public interface TransitClient {

    // ──── 密钥生命周期（coord:transit:key_manage / key_read）────

    /**
     * 创建密钥；同名同配置幂等，配置冲突抛 {@code ALREADY_EXISTS}。
     *
     * <p>{@code pki/} 前缀保留给 PKI 服务，走此接口会得到 {@code PERMISSION_DENIED}。
     */
    TransitKeyInfo createKey(String name, TransitKeyType type);

    /** 创建密钥；{@code exportable=true} 仅表示允许 ExportKey（仍需独立能力）。 */
    TransitKeyInfo createKey(String name, TransitKeyType type, boolean exportable);

    TransitKeyInfo readKey(String name);

    List<TransitKeyInfo> listKeys(String namePrefix);

    /** 非幂等：每次成功调用产生一个新版本。 */
    TransitKeyInfo rotateKey(String name);

    /**
     * 局部更新配置；{@code null} 表示不修改该字段。
     *
     * <p>版本必须落在合法区间；{@code deletionAllowed=true} 是 DeleteKey 的唯一闸门。
     */
    TransitKeyInfo updateKeyConfig(
            String name,
            Integer minDecryptionVersion,
            Integer minEncryptionVersion,
            Boolean deletionAllowed);

    /** 非幂等：不存在抛 {@code NOT_FOUND}；deletion_allowed=false 抛 {@code FAILED_PRECONDITION}。 */
    void deleteKey(String name);

    /** 非幂等：抬高 min_decryption_version 并删除更早版本材料。 */
    TransitKeyInfo trimKey(String name, int minVersion);

    /**
     * 导入 PEM/DER（PKCS#8）或原始对称密钥材料。
     *
     * @param materialFormat "pem" / "der" / "raw"；空 = 自动
     */
    TransitKeyInfo importKey(
            String name, TransitKeyType type, byte[] material, String materialFormat, boolean exportable);

    /**
     * 导出材料（独立能力 {@code coord:transit:key_export}）；非 exportable 抛
     * {@code FAILED_PRECONDITION}，CA 密钥恒抛 {@code PERMISSION_DENIED}。
     *
     * @param version 0 = latest
     */
    TransitExportKeyResponse exportKey(String name, int version);

    // ──── 按名信封加密（coord:transit:crypto）────

    TransitEncryptResponse encrypt(String name, byte[] plaintext);

    TransitEncryptResponse encrypt(String name, byte[] plaintext, byte[] context);

    /** @param version 0 = latest；显式版本必须落在 [min_encryption_version, latest]。 */
    TransitEncryptResponse encrypt(String name, int version, byte[] plaintext, byte[] context);

    /** 解密；key name/版本来自 CCT3 包头，context 是 AAD。 */
    TransitDecryptResponse decrypt(byte[] ciphertext);

    TransitDecryptResponse decrypt(byte[] ciphertext, byte[] context);

    /** 密文进、密文出：用 latest_version 重包 DEK（不解出明文）。 */
    TransitRewrapResponse rewrap(byte[] ciphertext, byte[] context);

    /**
     * 生成数据密钥；{@code withPlaintext=false} 时只返回包裹后的密文。
     */
    TransitGenerateDataKeyResponse generateDataKey(
            String name, byte[] context, int version, boolean withPlaintext);

    // ──── 批量（非原子、逐条结果、顺序 == 输入顺序）────

    List<TransitEncryptBatchItem> encryptBatch(List<TransitEncryptRequest> items);

    List<TransitDecryptBatchItem> decryptBatch(List<TransitDecryptRequest> items);

    List<TransitRewrapBatchItem> rewrapBatch(List<TransitRewrapRequest> items);

    // ──── 非对称签名（coord:transit:crypto）────

    /** 使用密钥默认算法（RSA 默认 PSS + SHA-256；ECDSA 按曲线默认摘要）。 */
    TransitSignResponse sign(String name, byte[] input);

    TransitSignResponse sign(TransitSignRequest request);

    boolean verify(TransitVerifyRequest request);

    List<TransitSignBatchItem> signBatch(List<TransitSignRequest> items);

    List<TransitVerifyBatchItem> verifyBatch(List<TransitVerifyRequest> items);

    // ──── 用途化 HMAC（ADR-0012；与命名密钥是两套模型）────

    TransitHmacSignature hmacSignWithPurpose(byte[] data, String purpose);

    TransitHmacSignature hmacSignWithPurpose(byte[] data, String purpose, String algorithm);

    TransitHmacSignature hmacSignWithPurpose(byte[] data, String purpose, String algorithm, String keyId);

    boolean hmacVerifyWithPurpose(byte[] data, byte[] signature, String purpose, String keyId);

    boolean hmacVerifyWithPurpose(
            byte[] data, byte[] signature, String purpose, String keyId, String algorithm);
}
