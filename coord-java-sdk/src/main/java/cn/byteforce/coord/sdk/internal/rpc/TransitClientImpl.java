package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.contracts.transit.v1.TransitCreateKeyRequest;
import cn.byteforce.coord.contracts.transit.v1.TransitDecryptBatchItem;
import cn.byteforce.coord.contracts.transit.v1.TransitDecryptBatchRequest;
import cn.byteforce.coord.contracts.transit.v1.TransitDecryptRequest;
import cn.byteforce.coord.contracts.transit.v1.TransitDecryptResponse;
import cn.byteforce.coord.contracts.transit.v1.TransitDeleteKeyRequest;
import cn.byteforce.coord.contracts.transit.v1.TransitEncryptBatchItem;
import cn.byteforce.coord.contracts.transit.v1.TransitEncryptBatchRequest;
import cn.byteforce.coord.contracts.transit.v1.TransitEncryptRequest;
import cn.byteforce.coord.contracts.transit.v1.TransitEncryptResponse;
import cn.byteforce.coord.contracts.transit.v1.TransitExportKeyRequest;
import cn.byteforce.coord.contracts.transit.v1.TransitExportKeyResponse;
import cn.byteforce.coord.contracts.transit.v1.TransitGenerateDataKeyRequest;
import cn.byteforce.coord.contracts.transit.v1.TransitGenerateDataKeyResponse;
import cn.byteforce.coord.contracts.transit.v1.TransitGrpc;
import cn.byteforce.coord.contracts.transit.v1.TransitHmacSignRequest;
import cn.byteforce.coord.contracts.transit.v1.TransitHmacSignResponse;
import cn.byteforce.coord.contracts.transit.v1.TransitHmacVerifyRequest;
import cn.byteforce.coord.contracts.transit.v1.TransitHmacVerifyResponse;
import cn.byteforce.coord.contracts.transit.v1.TransitImportKeyRequest;
import cn.byteforce.coord.contracts.transit.v1.TransitKeyInfo;
import cn.byteforce.coord.contracts.transit.v1.TransitKeyType;
import cn.byteforce.coord.contracts.transit.v1.TransitListKeysRequest;
import cn.byteforce.coord.contracts.transit.v1.TransitReadKeyRequest;
import cn.byteforce.coord.contracts.transit.v1.TransitRewrapBatchItem;
import cn.byteforce.coord.contracts.transit.v1.TransitRewrapBatchRequest;
import cn.byteforce.coord.contracts.transit.v1.TransitRewrapRequest;
import cn.byteforce.coord.contracts.transit.v1.TransitRewrapResponse;
import cn.byteforce.coord.contracts.transit.v1.TransitRotateKeyRequest;
import cn.byteforce.coord.contracts.transit.v1.TransitSignBatchItem;
import cn.byteforce.coord.contracts.transit.v1.TransitSignBatchRequest;
import cn.byteforce.coord.contracts.transit.v1.TransitSignRequest;
import cn.byteforce.coord.contracts.transit.v1.TransitSignResponse;
import cn.byteforce.coord.contracts.transit.v1.TransitTrimKeyRequest;
import cn.byteforce.coord.contracts.transit.v1.TransitUpdateKeyConfigRequest;
import cn.byteforce.coord.contracts.transit.v1.TransitVerifyBatchItem;
import cn.byteforce.coord.contracts.transit.v1.TransitVerifyBatchRequest;
import cn.byteforce.coord.contracts.transit.v1.TransitVerifyRequest;
import cn.byteforce.coord.sdk.CoordConfig;
import io.grpc.ManagedChannel;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import cn.byteforce.coord.sdk.transit.TransitClient;
import cn.byteforce.coord.sdk.transit.TransitHmacSignature;

import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of {@link TransitClient} backed by gRPC calls to the Coord Agent.
 */
public final class TransitClientImpl extends AgentRpcClient implements TransitClient {

    private static final Logger log = LoggerFactory.getLogger(TransitClientImpl.class);
    private final CoordConfig config;

    public TransitClientImpl(AgentChannelManager channelManager, ErrorMapper errorMapper,
                             RetryTemplate retryTemplate, ObservabilityProvider observability,
                             CoordConfig config) {
        super(channelManager, errorMapper, retryTemplate, observability);
        this.config = config;
    }

    private static ByteString bytes(byte[] value) {
        return value == null ? ByteString.EMPTY : ByteString.copyFrom(value);
    }

    private static String text(String value) {
        return value == null ? "" : value;
    }

    private TransitGrpc.TransitBlockingStub stub(ManagedChannel ch) {
        return TransitGrpc.newBlockingStub(ch)
                .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS);
    }

    // ──── 密钥生命周期 ────

    @Override
    public TransitKeyInfo createKey(String name, TransitKeyType type) {
        return createKey(name, type, false);
    }

    @Override
    public TransitKeyInfo createKey(String name, TransitKeyType type, boolean exportable) {
        TransitCreateKeyRequest request = TransitCreateKeyRequest.newBuilder()
                .setName(text(name))
                .setType(type)
                .setExportable(exportable)
                .build();
        return callWithRetry(
                (ch, r) -> stub(ch).createKey((TransitCreateKeyRequest) r),
                request, "transit.createKey").getKey();
    }

    @Override
    public TransitKeyInfo readKey(String name) {
        TransitReadKeyRequest request = TransitReadKeyRequest.newBuilder().setName(text(name)).build();
        return callWithRetry(
                (ch, r) -> stub(ch).readKey((TransitReadKeyRequest) r),
                request, "transit.readKey").getKey();
    }

    @Override
    public List<TransitKeyInfo> listKeys(String namePrefix) {
        TransitListKeysRequest request = TransitListKeysRequest.newBuilder()
                .setNamePrefix(text(namePrefix))
                .build();
        return callWithRetry(
                (ch, r) -> stub(ch).listKeys((TransitListKeysRequest) r),
                request, "transit.listKeys").getKeysList();
    }

    @Override
    public TransitKeyInfo rotateKey(String name) {
        TransitRotateKeyRequest request = TransitRotateKeyRequest.newBuilder().setName(text(name)).build();
        return callWithRetry(
                (ch, r) -> stub(ch).rotateKey((TransitRotateKeyRequest) r),
                request, "transit.rotateKey").getKey();
    }

    @Override
    public TransitKeyInfo updateKeyConfig(String name, Integer minDecryptionVersion,
                                          Integer minEncryptionVersion, Boolean deletionAllowed) {
        TransitUpdateKeyConfigRequest.Builder builder = TransitUpdateKeyConfigRequest.newBuilder()
                .setName(text(name));
        if (minDecryptionVersion != null) {
            builder.setMinDecryptionVersion(minDecryptionVersion);
        }
        if (minEncryptionVersion != null) {
            builder.setMinEncryptionVersion(minEncryptionVersion);
        }
        if (deletionAllowed != null) {
            builder.setDeletionAllowed(deletionAllowed);
        }
        TransitUpdateKeyConfigRequest request = builder.build();
        return callWithRetry(
                (ch, r) -> stub(ch).updateKeyConfig((TransitUpdateKeyConfigRequest) r),
                request, "transit.updateKeyConfig").getKey();
    }

    @Override
    public void deleteKey(String name) {
        TransitDeleteKeyRequest request = TransitDeleteKeyRequest.newBuilder().setName(text(name)).build();
        callWithRetry(
                (ch, r) -> stub(ch).deleteKey((TransitDeleteKeyRequest) r),
                request, "transit.deleteKey");
    }

    @Override
    public TransitKeyInfo trimKey(String name, int minVersion) {
        TransitTrimKeyRequest request = TransitTrimKeyRequest.newBuilder()
                .setName(text(name))
                .setMinVersion(minVersion)
                .build();
        return callWithRetry(
                (ch, r) -> stub(ch).trimKey((TransitTrimKeyRequest) r),
                request, "transit.trimKey").getKey();
    }

    @Override
    public TransitKeyInfo importKey(String name, TransitKeyType type, byte[] material,
                                    String materialFormat, boolean exportable) {
        TransitImportKeyRequest request = TransitImportKeyRequest.newBuilder()
                .setName(text(name))
                .setType(type)
                .setMaterial(bytes(material))
                .setMaterialFormat(text(materialFormat))
                .setExportable(exportable)
                .build();
        return callWithRetry(
                (ch, r) -> stub(ch).importKey((TransitImportKeyRequest) r),
                request, "transit.importKey").getKey();
    }

    @Override
    public TransitExportKeyResponse exportKey(String name, int version) {
        TransitExportKeyRequest request = TransitExportKeyRequest.newBuilder()
                .setName(text(name))
                .setVersion(version)
                .build();
        return callWithRetry(
                (ch, r) -> stub(ch).exportKey((TransitExportKeyRequest) r),
                request, "transit.exportKey");
    }

    // ──── 按名信封加密 ────

    @Override
    public TransitEncryptResponse encrypt(String name, byte[] plaintext) {
        return encrypt(name, 0, plaintext, null);
    }

    @Override
    public TransitEncryptResponse encrypt(String name, byte[] plaintext, byte[] context) {
        return encrypt(name, 0, plaintext, context);
    }

    @Override
    public TransitEncryptResponse encrypt(String name, int version, byte[] plaintext, byte[] context) {
        TransitEncryptRequest request = TransitEncryptRequest.newBuilder()
                .setName(text(name))
                .setVersion(version)
                .setPlaintext(bytes(plaintext))
                .setContext(bytes(context))
                .build();
        TransitEncryptResponse response = callWithRetry(
                (ch, r) -> stub(ch).encrypt((TransitEncryptRequest) r),
                request, "transit.encrypt");
        log.debug("Transit encrypt: key={}, version={}, ciphertext_len={}",
                name, response.getKeyVersion(), response.getCiphertext().size());
        return response;
    }

    @Override
    public TransitDecryptResponse decrypt(byte[] ciphertext) {
        return decrypt(ciphertext, null);
    }

    @Override
    public TransitDecryptResponse decrypt(byte[] ciphertext, byte[] context) {
        TransitDecryptRequest request = TransitDecryptRequest.newBuilder()
                .setCiphertext(bytes(ciphertext))
                .setContext(bytes(context))
                .build();
        return callWithRetry(
                (ch, r) -> stub(ch).decrypt((TransitDecryptRequest) r),
                request, "transit.decrypt");
    }

    @Override
    public TransitRewrapResponse rewrap(byte[] ciphertext, byte[] context) {
        TransitRewrapRequest request = TransitRewrapRequest.newBuilder()
                .setCiphertext(bytes(ciphertext))
                .setContext(bytes(context))
                .build();
        return callWithRetry(
                (ch, r) -> stub(ch).rewrap((TransitRewrapRequest) r),
                request, "transit.rewrap");
    }

    @Override
    public TransitGenerateDataKeyResponse generateDataKey(String name, byte[] context, int version,
                                                          boolean withPlaintext) {
        TransitGenerateDataKeyRequest request = TransitGenerateDataKeyRequest.newBuilder()
                .setName(text(name))
                .setVersion(version)
                .setContext(bytes(context))
                .setType(withPlaintext
                        ? cn.byteforce.coord.contracts.transit.v1.TransitDataKeyType.DATA_KEY_TYPE_PLAINTEXT_AND_WRAPPED
                        : cn.byteforce.coord.contracts.transit.v1.TransitDataKeyType.DATA_KEY_TYPE_WRAPPED)
                .build();
        return callWithRetry(
                (ch, r) -> stub(ch).generateDataKey((TransitGenerateDataKeyRequest) r),
                request, "transit.generateDataKey");
    }

    @Override
    public List<TransitEncryptBatchItem> encryptBatch(List<TransitEncryptRequest> items) {
        TransitEncryptBatchRequest request = TransitEncryptBatchRequest.newBuilder()
                .addAllItems(items)
                .build();
        return callWithRetry(
                (ch, r) -> stub(ch).encryptBatch((TransitEncryptBatchRequest) r),
                request, "transit.encryptBatch").getItemsList();
    }

    @Override
    public List<TransitDecryptBatchItem> decryptBatch(List<TransitDecryptRequest> items) {
        TransitDecryptBatchRequest request = TransitDecryptBatchRequest.newBuilder()
                .addAllItems(items)
                .build();
        return callWithRetry(
                (ch, r) -> stub(ch).decryptBatch((TransitDecryptBatchRequest) r),
                request, "transit.decryptBatch").getItemsList();
    }

    @Override
    public List<TransitRewrapBatchItem> rewrapBatch(List<TransitRewrapRequest> items) {
        TransitRewrapBatchRequest request = TransitRewrapBatchRequest.newBuilder()
                .addAllItems(items)
                .build();
        return callWithRetry(
                (ch, r) -> stub(ch).rewrapBatch((TransitRewrapBatchRequest) r),
                request, "transit.rewrapBatch").getItemsList();
    }

    // ──── 非对称签名 ────

    @Override
    public TransitSignResponse sign(String name, byte[] input) {
        return sign(TransitSignRequest.newBuilder()
                .setName(text(name))
                .setInput(bytes(input))
                .build());
    }

    @Override
    public TransitSignResponse sign(TransitSignRequest request) {
        return callWithRetry(
                (ch, r) -> stub(ch).sign((TransitSignRequest) r),
                request, "transit.sign");
    }

    @Override
    public boolean verify(TransitVerifyRequest request) {
        return callWithRetry(
                (ch, r) -> stub(ch).verify((TransitVerifyRequest) r),
                request, "transit.verify").getValid();
    }

    @Override
    public List<TransitSignBatchItem> signBatch(List<TransitSignRequest> items) {
        TransitSignBatchRequest request = TransitSignBatchRequest.newBuilder()
                .addAllItems(items)
                .build();
        return callWithRetry(
                (ch, r) -> stub(ch).signBatch((TransitSignBatchRequest) r),
                request, "transit.signBatch").getItemsList();
    }

    @Override
    public List<TransitVerifyBatchItem> verifyBatch(List<TransitVerifyRequest> items) {
        TransitVerifyBatchRequest request = TransitVerifyBatchRequest.newBuilder()
                .addAllItems(items)
                .build();
        return callWithRetry(
                (ch, r) -> stub(ch).verifyBatch((TransitVerifyBatchRequest) r),
                request, "transit.verifyBatch").getItemsList();
    }

    // ──── 用途化 HMAC（ADR-0012）────

    @Override
    public TransitHmacSignature hmacSignWithPurpose(byte[] data, String purpose) {
        return hmacSignWithPurpose(data, purpose, "HMAC-SHA256", "");
    }

    @Override
    public TransitHmacSignature hmacSignWithPurpose(byte[] data, String purpose, String algorithm) {
        return hmacSignWithPurpose(data, purpose, algorithm, "");
    }

    @Override
    public TransitHmacSignature hmacSignWithPurpose(byte[] data, String purpose, String algorithm,
                                                    String keyId) {
        TransitHmacSignRequest request = TransitHmacSignRequest.newBuilder()
                .setData(bytes(data))
                .setAlgorithm(algorithm != null ? algorithm : "HMAC-SHA256")
                .setPurpose(text(purpose))
                .setKeyId(text(keyId))
                .build();
        TransitHmacSignResponse response = callWithRetry(
                (ch, r) -> stub(ch).hmacSign((TransitHmacSignRequest) r),
                request, "transit.hmacSign");
        return new TransitHmacSignature(
                response.getSignature().toByteArray(), response.getKeyId(), response.getAlgorithm());
    }

    @Override
    public boolean hmacVerifyWithPurpose(byte[] data, byte[] signature, String purpose, String keyId) {
        return hmacVerifyWithPurpose(data, signature, purpose, keyId, "HMAC-SHA256");
    }

    @Override
    public boolean hmacVerifyWithPurpose(byte[] data, byte[] signature, String purpose,
                                         String keyId, String algorithm) {
        TransitHmacVerifyRequest request = TransitHmacVerifyRequest.newBuilder()
                .setData(bytes(data))
                .setSignature(bytes(signature))
                .setAlgorithm(algorithm != null ? algorithm : "HMAC-SHA256")
                .setPurpose(text(purpose))
                .setKeyId(text(keyId))
                .build();
        TransitHmacVerifyResponse response = callWithRetry(
                (ch, r) -> stub(ch).hmacVerify((TransitHmacVerifyRequest) r),
                request, "transit.hmacVerify");
        return response.getValid();
    }
}
