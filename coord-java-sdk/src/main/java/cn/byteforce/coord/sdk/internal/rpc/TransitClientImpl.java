package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.CoordException;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.sdk.internal.proto.TransitDecryptRequest;
import cn.byteforce.coord.sdk.internal.proto.TransitDecryptResponse;
import cn.byteforce.coord.sdk.internal.proto.TransitEncryptRequest;
import cn.byteforce.coord.sdk.internal.proto.TransitEncryptResponse;
import cn.byteforce.coord.sdk.internal.proto.TransitGrpc;
import cn.byteforce.coord.sdk.internal.proto.TransitHmacSignRequest;
import cn.byteforce.coord.sdk.internal.proto.TransitHmacSignResponse;
import cn.byteforce.coord.sdk.internal.proto.TransitHmacVerifyRequest;
import cn.byteforce.coord.sdk.internal.proto.TransitHmacVerifyResponse;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import cn.byteforce.coord.sdk.transit.TransitClient;

import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    @Override
    public byte[] encrypt(byte[] plaintext) {
        return encrypt(plaintext, null);
    }

    @Override
    public byte[] encrypt(byte[] plaintext, byte[] context) {
        TransitEncryptRequest.Builder req = TransitEncryptRequest.newBuilder()
                .setPlaintext(ByteString.copyFrom(plaintext));
        if (context != null && context.length > 0) {
            req.setContext(ByteString.copyFrom(context));
        }

        TransitEncryptResponse response = callWithRetry(
                (ch, r) -> TransitGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .encrypt((TransitEncryptRequest) r),
                req.build(), "transit.encrypt");

        byte[] ciphertext = response.getCiphertext().toByteArray();
        log.debug("Transit encrypt: plaintext_len={}, ciphertext_len={}", plaintext.length, ciphertext.length);
        return ciphertext;
    }

    @Override
    public byte[] decrypt(byte[] ciphertext) {
        return decrypt(ciphertext, null);
    }

    @Override
    public byte[] decrypt(byte[] ciphertext, byte[] context) {
        TransitDecryptRequest.Builder req = TransitDecryptRequest.newBuilder()
                .setCiphertext(ByteString.copyFrom(ciphertext));
        if (context != null && context.length > 0) {
            req.setContext(ByteString.copyFrom(context));
        }

        TransitDecryptResponse response = callWithRetry(
                (ch, r) -> TransitGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .decrypt((TransitDecryptRequest) r),
                req.build(), "transit.decrypt");

        byte[] plaintext = response.getPlaintext().toByteArray();
        log.debug("Transit decrypt: ciphertext_len={}, plaintext_len={}", ciphertext.length, plaintext.length);
        return plaintext;
    }

    // ──── HMAC 签名与验签 (Phase C.1) ────

    @Override
    public byte[] hmacSign(byte[] data) {
        return hmacSign(data, "HMAC-SHA256");
    }

    @Override
    public byte[] hmacSign(byte[] data, String algorithm) {
        TransitHmacSignRequest request = TransitHmacSignRequest.newBuilder()
                .setData(ByteString.copyFrom(data))
                .setAlgorithm(algorithm != null ? algorithm : "HMAC-SHA256")
                .build();

        TransitHmacSignResponse response = callWithRetry(
                (ch, r) -> TransitGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .hmacSign((TransitHmacSignRequest) r),
                request, "transit.hmacSign");

        byte[] signature = response.getSignature().toByteArray();
        log.debug("Transit hmacSign: data_len={}, algo={}, sig_len={}", data.length, algorithm, signature.length);
        return signature;
    }

    @Override
    public boolean hmacVerify(byte[] data, byte[] signature) {
        return hmacVerify(data, signature, "HMAC-SHA256");
    }

    @Override
    public boolean hmacVerify(byte[] data, byte[] signature, String algorithm) {
        TransitHmacVerifyRequest request = TransitHmacVerifyRequest.newBuilder()
                .setData(ByteString.copyFrom(data))
                .setSignature(ByteString.copyFrom(signature))
                .setAlgorithm(algorithm != null ? algorithm : "HMAC-SHA256")
                .build();

        TransitHmacVerifyResponse response = callWithRetry(
                (ch, r) -> TransitGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .hmacVerify((TransitHmacVerifyRequest) r),
                request, "transit.hmacVerify");

        boolean valid = response.getValid();
        log.debug("Transit hmacVerify: data_len={}, valid={}", data.length, valid);
        return valid;
    }
}
