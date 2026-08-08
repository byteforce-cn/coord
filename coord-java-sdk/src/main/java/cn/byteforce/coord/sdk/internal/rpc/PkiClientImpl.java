package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.CoordException;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.sdk.internal.proto.PkiGetCaCertRequest;
import cn.byteforce.coord.sdk.internal.proto.PkiGetCaCertResponse;
import cn.byteforce.coord.sdk.internal.proto.PkiGetCertByCNRequest;
import cn.byteforce.coord.sdk.internal.proto.PkiGetCertByCNResponse;
import cn.byteforce.coord.sdk.internal.proto.PkiGrpc;
import cn.byteforce.coord.sdk.internal.proto.PkiInitCaRequest;
import cn.byteforce.coord.sdk.internal.proto.PkiInitCaResponse;
import cn.byteforce.coord.sdk.internal.proto.PkiIssueCertRequest;
import cn.byteforce.coord.sdk.internal.proto.PkiIssueCertResponse;
import cn.byteforce.coord.sdk.internal.proto.PkiListCertsRequest;
import cn.byteforce.coord.sdk.internal.proto.PkiListCertsResponse;
import cn.byteforce.coord.sdk.internal.proto.PkiRenewCertRequest;
import cn.byteforce.coord.sdk.internal.proto.PkiRenewCertResponse;
import cn.byteforce.coord.sdk.internal.proto.PkiRotateCertRequest;
import cn.byteforce.coord.sdk.internal.proto.PkiRotateCertResponse;
import cn.byteforce.coord.sdk.internal.proto.PkiVerifyCertRequest;
import cn.byteforce.coord.sdk.internal.proto.PkiVerifyCertResponse;
import cn.byteforce.coord.sdk.pki.PkiCertInfo;
import cn.byteforce.coord.sdk.pki.PkiCertSummary;
import cn.byteforce.coord.sdk.pki.PkiClient;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of {@link PkiClient} backed by gRPC calls to the Coord Agent.
 */
public final class PkiClientImpl extends AgentRpcClient implements PkiClient {

    private static final Logger log = LoggerFactory.getLogger(PkiClientImpl.class);
    private final CoordConfig config;

    public PkiClientImpl(AgentChannelManager channelManager, ErrorMapper errorMapper,
                         RetryTemplate retryTemplate, ObservabilityProvider observability,
                         CoordConfig config) {
        super(channelManager, errorMapper, retryTemplate, observability);
        this.config = config;
    }

    @Override
    public void initCa(String caCommonName) {
        PkiInitCaRequest request = PkiInitCaRequest.newBuilder()
                .setCaCommonName(caCommonName)
                .build();

        callWithRetry(
                (ch, r) -> PkiGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .initCa((PkiInitCaRequest) r),
                request, "pki.initCa");

        log.debug("PKI CA initialized: cn={}", caCommonName);
    }

    @Override
    public PkiCertInfo issueCert(String commonName) {
        return issueCert(commonName, 0);
    }

    @Override
    public PkiCertInfo issueCert(String commonName, long ttlSeconds) {
        PkiIssueCertRequest request = PkiIssueCertRequest.newBuilder()
                .setCommonName(commonName)
                .setTtlSeconds(ttlSeconds)
                .build();

        PkiIssueCertResponse response = callWithRetry(
                (ch, r) -> PkiGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .issueCert((PkiIssueCertRequest) r),
                request, "pki.issueCert");

        log.debug("PKI cert issued: cn={}, serial={}", response.getCommonName(), response.getSerial());
        return new PkiCertInfo(
                response.getCommonName(),
                response.getCertPem(),
                response.getKeyPem(),
                response.getNotBefore(),
                response.getNotAfter(),
                response.getSerial());
    }

    @Override
    public PkiCertInfo renewCert(String serialNumber, long ttlSeconds) {
        PkiRenewCertRequest request = PkiRenewCertRequest.newBuilder()
                .setSerialNumber(serialNumber)
                .setTtlSeconds(ttlSeconds)
                .build();

        PkiRenewCertResponse response = callWithRetry(
                (ch, r) -> PkiGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .renewCert((PkiRenewCertRequest) r),
                request, "pki.renewCert");

        log.debug("PKI cert renewed: cn={}, serial={}", response.getCommonName(), response.getSerial());
        return new PkiCertInfo(
                response.getCommonName(),
                response.getCertPem(),
                response.getKeyPem(),
                response.getNotBefore(),
                response.getNotAfter(),
                response.getSerial(),
                response.getStatus(),
                response.getParentSerial());
    }

    @Override
    public PkiCertInfo rotateCert(String commonName) {
        return rotateCert(commonName, 0);
    }

    @Override
    public PkiCertInfo rotateCert(String commonName, long ttlSeconds) {
        PkiRotateCertRequest request = PkiRotateCertRequest.newBuilder()
                .setCommonName(commonName)
                .setTtlSeconds(ttlSeconds)
                .build();

        PkiRotateCertResponse response = callWithRetry(
                (ch, r) -> PkiGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .rotateCert((PkiRotateCertRequest) r),
                request, "pki.rotateCert");

        log.debug("PKI cert rotated: cn={}, newSerial={}, parent={}",
                response.getCommonName(), response.getSerial(), response.getParentSerial());
        return new PkiCertInfo(
                response.getCommonName(),
                response.getCertPem(),
                response.getKeyPem(),
                response.getNotBefore(),
                response.getNotAfter(),
                response.getSerial(),
                response.getStatus(),
                response.getParentSerial());
    }

    @Override
    public List<PkiCertSummary> listCerts(String commonName) {
        PkiListCertsRequest request = PkiListCertsRequest.newBuilder()
                .setCommonName(commonName)
                .build();

        PkiListCertsResponse response = callWithRetry(
                (ch, r) -> PkiGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .listCerts((PkiListCertsRequest) r),
                request, "pki.listCerts");

        List<PkiCertSummary> summaries = new ArrayList<>();
        for (cn.byteforce.coord.sdk.internal.proto.PkiCertSummary s : response.getCertsList()) {
            summaries.add(new PkiCertSummary(
                    s.getCommonName(),
                    s.getCertPem(),
                    s.getNotBefore(),
                    s.getNotAfter(),
                    s.getSerial(),
                    s.getStatus(),
                    s.getParentSerial()));
        }
        log.debug("PKI cert list: cn={}, count={}", commonName, summaries.size());
        return summaries;
    }

    @Override
    public PkiCertInfo getCertByCN(String commonName) {
        PkiGetCertByCNRequest request = PkiGetCertByCNRequest.newBuilder()
                .setCommonName(commonName)
                .build();

        PkiGetCertByCNResponse response = callWithRetry(
                (ch, r) -> PkiGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .getCertByCN((PkiGetCertByCNRequest) r),
                request, "pki.getCertByCN");

        log.debug("PKI cert fetched by CN: cn={}, serial={}", response.getCommonName(), response.getSerial());
        return new PkiCertInfo(
                response.getCommonName(),
                response.getCertPem(),
                response.getKeyPem(),
                response.getNotBefore(),
                response.getNotAfter(),
                response.getSerial(),
                response.getStatus(),
                response.getParentSerial());
    }

    @Override
    public boolean verifyCert(String certPem) {
        PkiVerifyCertRequest request = PkiVerifyCertRequest.newBuilder()
                .setCertPem(certPem)
                .build();

        PkiVerifyCertResponse response = callWithRetry(
                (ch, r) -> PkiGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .verifyCert((PkiVerifyCertRequest) r),
                request, "pki.verifyCert");

        log.debug("PKI cert verification: valid={}", response.getValid());
        return response.getValid();
    }

    @Override
    public String getCaCert() {
        PkiGetCaCertRequest request = PkiGetCaCertRequest.getDefaultInstance();

        PkiGetCaCertResponse response = callWithRetry(
                (ch, r) -> PkiGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .getCaCert((PkiGetCaCertRequest) r),
                request, "pki.getCaCert");

        return response.getCaCertPem();
    }
}
