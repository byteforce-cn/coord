package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.CoordException;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.contracts.pki.v1.PkiGetCaCertRequest;
import cn.byteforce.coord.contracts.pki.v1.PkiGetCaCertResponse;
import cn.byteforce.coord.contracts.pki.v1.PkiGetCertByCNRequest;
import cn.byteforce.coord.contracts.pki.v1.PkiGetCertByCNResponse;
import cn.byteforce.coord.contracts.pki.v1.PkiGetCertBySerialRequest;
import cn.byteforce.coord.contracts.pki.v1.PkiGetCertBySerialResponse;
import cn.byteforce.coord.contracts.pki.v1.PkiGetCertStatusRequest;
import cn.byteforce.coord.contracts.pki.v1.PkiGetCertStatusResponse;
import cn.byteforce.coord.contracts.pki.v1.PkiGetCrlRequest;
import cn.byteforce.coord.contracts.pki.v1.PkiGetCrlResponse;
import cn.byteforce.coord.contracts.pki.v1.PkiRevokeCertRequest;
import cn.byteforce.coord.contracts.pki.v1.PkiRevokeCertResponse;
import cn.byteforce.coord.contracts.pki.v1.PkiRotateCrlRequest;
import cn.byteforce.coord.contracts.pki.v1.PkiRotateCrlResponse;
import cn.byteforce.coord.contracts.pki.v1.PkiTidyRequest;
import cn.byteforce.coord.contracts.pki.v1.PkiTidyResponse;
import cn.byteforce.coord.contracts.pki.v1.PkiCertificateUsage;
import cn.byteforce.coord.contracts.pki.v1.PkiSignCsrRequest;
import cn.byteforce.coord.contracts.pki.v1.PkiSignCsrResponse;
import cn.byteforce.coord.contracts.pki.v1.PkiGrpc;
import cn.byteforce.coord.contracts.pki.v1.PkiInitCaRequest;
import cn.byteforce.coord.contracts.pki.v1.PkiInitCaResponse;
import cn.byteforce.coord.contracts.pki.v1.PkiIssueCertRequest;
import cn.byteforce.coord.contracts.pki.v1.PkiIssueCertResponse;
import cn.byteforce.coord.contracts.pki.v1.PkiListCertsRequest;
import cn.byteforce.coord.contracts.pki.v1.PkiListCertsResponse;
import cn.byteforce.coord.contracts.pki.v1.PkiRenewCertRequest;
import cn.byteforce.coord.contracts.pki.v1.PkiRenewCertResponse;
import cn.byteforce.coord.contracts.pki.v1.PkiRotateCertRequest;
import cn.byteforce.coord.contracts.pki.v1.PkiRotateCertResponse;
import cn.byteforce.coord.contracts.pki.v1.PkiVerifyCertRequest;
import cn.byteforce.coord.contracts.pki.v1.PkiVerifyCertResponse;
import cn.byteforce.coord.sdk.pki.PkiCertInfo;
import cn.byteforce.coord.sdk.pki.PkiCertStatus;
import cn.byteforce.coord.sdk.pki.PkiCertSummary;
import cn.byteforce.coord.sdk.pki.PkiCertUsage;
import cn.byteforce.coord.sdk.pki.PkiClient;
import cn.byteforce.coord.sdk.pki.PkiCrl;
import cn.byteforce.coord.sdk.pki.PkiCrlFormat;
import cn.byteforce.coord.sdk.pki.PkiRevocationInfo;
import cn.byteforce.coord.sdk.pki.PkiRevocationReason;
import cn.byteforce.coord.sdk.pki.PkiTidyResult;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import com.google.protobuf.ByteString;

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

    private static PkiCertificateUsage toProtoUsage(PkiCertUsage usage) {
        if (usage == null) {
            return PkiCertificateUsage.PKI_CERTIFICATE_USAGE_UNSPECIFIED;
        }
        return switch (usage) {
            case BOTH -> PkiCertificateUsage.PKI_CERTIFICATE_USAGE_BOTH;
            case SERVER -> PkiCertificateUsage.PKI_CERTIFICATE_USAGE_SERVER;
            case CLIENT -> PkiCertificateUsage.PKI_CERTIFICATE_USAGE_CLIENT;
        };
    }

    private static cn.byteforce.coord.contracts.pki.v1.PkiKeyMaterial keyMaterialOf(
            cn.byteforce.coord.contracts.pki.v1.PkiKeyMaterial material) {
        return material == null
                ? cn.byteforce.coord.contracts.pki.v1.PkiKeyMaterial.getDefaultInstance()
                : material;
    }

    private static cn.byteforce.coord.contracts.pki.v1.PkiRevocationReason toProtoRevocation(
            PkiRevocationReason reason) {
        if (reason == null) {
            return cn.byteforce.coord.contracts.pki.v1.PkiRevocationReason
                    .PKI_REVOCATION_REASON_UNSPECIFIED;
        }
        return switch (reason) {
            case UNSPECIFIED ->
                    cn.byteforce.coord.contracts.pki.v1.PkiRevocationReason
                            .PKI_REVOCATION_REASON_UNSPECIFIED;
            case KEY_COMPROMISE -> cn.byteforce.coord.contracts.pki.v1.PkiRevocationReason
                            .PKI_REVOCATION_REASON_KEY_COMPROMISE;
            case CA_COMPROMISE -> cn.byteforce.coord.contracts.pki.v1.PkiRevocationReason
                            .PKI_REVOCATION_REASON_CA_COMPROMISE;
            case AFFILIATION_CHANGED ->
                    cn.byteforce.coord.contracts.pki.v1.PkiRevocationReason
                            .PKI_REVOCATION_REASON_AFFILIATION_CHANGED;
            case SUPERSEDED -> cn.byteforce.coord.contracts.pki.v1.PkiRevocationReason
                            .PKI_REVOCATION_REASON_SUPERSEDED;
            case CESSATION_OF_OPERATION ->
                    cn.byteforce.coord.contracts.pki.v1.PkiRevocationReason
                            .PKI_REVOCATION_REASON_CESSATION_OF_OPERATION;
            case CERTIFICATE_HOLD -> cn.byteforce.coord.contracts.pki.v1.PkiRevocationReason
                            .PKI_REVOCATION_REASON_CERTIFICATE_HOLD;
            case PRIVILEGE_WITHDRAWN ->
                    cn.byteforce.coord.contracts.pki.v1.PkiRevocationReason
                            .PKI_REVOCATION_REASON_PRIVILEGE_WITHDRAWN;
            case AA_COMPROMISE -> cn.byteforce.coord.contracts.pki.v1.PkiRevocationReason
                            .PKI_REVOCATION_REASON_AA_COMPROMISE;
        };
    }

    private static cn.byteforce.coord.sdk.pki.PkiRevocationReason fromProtoRevocation(
            cn.byteforce.coord.contracts.pki.v1.PkiRevocationReason reason) {
        return switch (reason) {
            case PKI_REVOCATION_REASON_KEY_COMPROMISE ->
                    cn.byteforce.coord.sdk.pki.PkiRevocationReason.KEY_COMPROMISE;
            case PKI_REVOCATION_REASON_CA_COMPROMISE ->
                    cn.byteforce.coord.sdk.pki.PkiRevocationReason.CA_COMPROMISE;
            case PKI_REVOCATION_REASON_AFFILIATION_CHANGED ->
                    cn.byteforce.coord.sdk.pki.PkiRevocationReason.AFFILIATION_CHANGED;
            case PKI_REVOCATION_REASON_SUPERSEDED ->
                    cn.byteforce.coord.sdk.pki.PkiRevocationReason.SUPERSEDED;
            case PKI_REVOCATION_REASON_CESSATION_OF_OPERATION ->
                    cn.byteforce.coord.sdk.pki.PkiRevocationReason.CESSATION_OF_OPERATION;
            case PKI_REVOCATION_REASON_CERTIFICATE_HOLD ->
                    cn.byteforce.coord.sdk.pki.PkiRevocationReason.CERTIFICATE_HOLD;
            case PKI_REVOCATION_REASON_PRIVILEGE_WITHDRAWN ->
                    cn.byteforce.coord.sdk.pki.PkiRevocationReason.PRIVILEGE_WITHDRAWN;
            case PKI_REVOCATION_REASON_AA_COMPROMISE ->
                    cn.byteforce.coord.sdk.pki.PkiRevocationReason.AA_COMPROMISE;
            case PKI_REVOCATION_REASON_UNSPECIFIED, UNRECOGNIZED ->
                    cn.byteforce.coord.sdk.pki.PkiRevocationReason.UNSPECIFIED;
        };
    }

    private static cn.byteforce.coord.contracts.pki.v1.PkiCrlFormat toProtoCrlFormat(
            PkiCrlFormat format) {
        if (format == PkiCrlFormat.DER) {
            return cn.byteforce.coord.contracts.pki.v1.PkiCrlFormat.PKI_CRL_FORMAT_DER;
        }
        return cn.byteforce.coord.contracts.pki.v1.PkiCrlFormat.PKI_CRL_FORMAT_PEM;
    }

    private static cn.byteforce.coord.sdk.pki.PkiCrlFormat fromProtoCrlFormat(
            cn.byteforce.coord.contracts.pki.v1.PkiCrlFormat format) {
        return format == cn.byteforce.coord.contracts.pki.v1.PkiCrlFormat.PKI_CRL_FORMAT_DER
                ? cn.byteforce.coord.sdk.pki.PkiCrlFormat.DER
                : cn.byteforce.coord.sdk.pki.PkiCrlFormat.PEM;
    }

    private static PkiCrl crlOf(cn.byteforce.coord.contracts.pki.v1.PkiCrlFormat requested,
                                int responseFormat, byte[] crl,
                                long crlNumber, long thisUpdate, long nextUpdate) {
        // 服务端回显实际编码；缺失/未知时按请求口径解释（forNumber 对未知值返回 null）。
        cn.byteforce.coord.contracts.pki.v1.PkiCrlFormat echoed =
                cn.byteforce.coord.contracts.pki.v1.PkiCrlFormat.forNumber(responseFormat);
        cn.byteforce.coord.sdk.pki.PkiCrlFormat format =
                echoed == null
                                || echoed
                                        == cn.byteforce.coord.contracts.pki.v1.PkiCrlFormat
                                                .PKI_CRL_FORMAT_UNSPECIFIED
                        ? fromProtoCrlFormat(requested)
                        : fromProtoCrlFormat(echoed);
        return new PkiCrl(crl, format, crlNumber, thisUpdate, nextUpdate);
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
        return issueCert(commonName, ttlSeconds, List.of(), List.of());
    }

    @Override
    public PkiCertInfo issueCert(String commonName, long ttlSeconds, List<String> dnsNames,
                                 List<String> ipAddresses) {
        return issueCert(commonName, ttlSeconds, dnsNames, ipAddresses, null);
    }

    @Override
    public PkiCertInfo issueCert(String commonName, long ttlSeconds, List<String> dnsNames,
                                 List<String> ipAddresses, PkiCertUsage usage) {
        PkiIssueCertRequest request = PkiIssueCertRequest.newBuilder()
                .setCommonName(commonName)
                .setTtlSeconds(ttlSeconds)
                .addAllDnsNames(dnsNames == null ? List.of() : dnsNames)
                .addAllIpAddresses(ipAddresses == null ? List.of() : ipAddresses)
                .setUsage(toProtoUsage(usage))
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
                keyMaterialOf(response.getKeyMaterial()).getKeyName(),
                keyMaterialOf(response.getKeyMaterial()).getKeyVersion(),
                response.getNotBefore(),
                response.getNotAfter(),
                response.getSerial(),
                response.getStatus(),
                response.getParentSerial());
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
                keyMaterialOf(response.getKeyMaterial()).getKeyName(),
                keyMaterialOf(response.getKeyMaterial()).getKeyVersion(),
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
        return rotateCert(commonName, ttlSeconds, List.of(), List.of());
    }

    @Override
    public PkiCertInfo rotateCert(String commonName, long ttlSeconds, List<String> dnsNames,
                                  List<String> ipAddresses) {
        return rotateCert(commonName, ttlSeconds, dnsNames, ipAddresses, null);
    }

    @Override
    public PkiCertInfo rotateCert(String commonName, long ttlSeconds, List<String> dnsNames,
                                  List<String> ipAddresses, PkiCertUsage usage) {
        PkiRotateCertRequest request = PkiRotateCertRequest.newBuilder()
                .setCommonName(commonName)
                .setTtlSeconds(ttlSeconds)
                .addAllDnsNames(dnsNames == null ? List.of() : dnsNames)
                .addAllIpAddresses(ipAddresses == null ? List.of() : ipAddresses)
                .setUsage(toProtoUsage(usage))
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
                keyMaterialOf(response.getKeyMaterial()).getKeyName(),
                keyMaterialOf(response.getKeyMaterial()).getKeyVersion(),
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
        for (cn.byteforce.coord.contracts.pki.v1.PkiCertSummary s : response.getCertsList()) {
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
                keyMaterialOf(response.getKeyMaterial()).getKeyName(),
                keyMaterialOf(response.getKeyMaterial()).getKeyVersion(),
                response.getNotBefore(),
                response.getNotAfter(),
                response.getSerial(),
                response.getStatus(),
                response.getParentSerial());
    }

    @Override
    public PkiCertSummary getCertBySerial(String serialNumber) {
        PkiGetCertBySerialRequest request = PkiGetCertBySerialRequest.newBuilder()
                .setSerial(serialNumber)
                .build();

        PkiGetCertBySerialResponse response = callWithRetry(
                (ch, r) -> PkiGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .getCertBySerial((PkiGetCertBySerialRequest) r),
                request, "pki.getCertBySerial");

        log.debug("PKI cert fetched by serial: cn={}, serial={}",
                response.getCommonName(), response.getSerial());
        return new PkiCertSummary(
                response.getCommonName(),
                response.getCertPem(),
                response.getNotBefore(),
                response.getNotAfter(),
                response.getSerial(),
                response.getStatus(),
                response.getParentSerial());
    }

    @Override
    public PkiCertInfo signCsr(byte[] csrDer) {
        return signCsr(csrDer, 0, null);
    }

    @Override
    public PkiCertInfo signCsr(byte[] csrDer, long ttlSeconds, PkiCertUsage usage) {
        PkiSignCsrRequest request = PkiSignCsrRequest.newBuilder()
                .setCsrDer(ByteString.copyFrom(csrDer))
                .setTtlSeconds(ttlSeconds)
                .setUsage(toProtoUsage(usage))
                .build();

        PkiSignCsrResponse response = callWithRetry(
                (ch, r) -> PkiGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .signCsr((PkiSignCsrRequest) r),
                request, "pki.signCsr");

        log.debug("PKI CSR signed: cn={}, serial={}", response.getCommonName(), response.getSerial());
        // 外部密钥：没有 coord 侧 key archive 引用。
        return new PkiCertInfo(
                response.getCommonName(),
                response.getCertPem(),
                "",
                0,
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

    @Override
    public PkiRevocationInfo revokeCert(String serialNumber, PkiRevocationReason reason) {
        PkiRevokeCertRequest request = PkiRevokeCertRequest.newBuilder()
                .setSerial(serialNumber)
                .setReason(toProtoRevocation(reason))
                .build();

        PkiRevokeCertResponse response = callWithRetry(
                (ch, r) -> PkiGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .revokeCert((PkiRevokeCertRequest) r),
                request, "pki.revokeCert");

        log.debug("PKI cert revoked: serial={}, certSerial={}, by={}",
                response.getSerial(), response.getCertSerial(), response.getBy());
        return new PkiRevocationInfo(
                response.getSerial(),
                response.getCertSerial(),
                response.getCommonName(),
                response.getRevokedAt(),
                fromProtoRevocation(response.getReason()),
                response.getBy());
    }

    @Override
    public PkiCertStatus getCertStatus(String serialNumber) {
        PkiGetCertStatusRequest request = PkiGetCertStatusRequest.newBuilder()
                .setSerial(serialNumber)
                .build();

        PkiGetCertStatusResponse response = callWithRetry(
                (ch, r) -> PkiGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .getCertStatus((PkiGetCertStatusRequest) r),
                request, "pki.getCertStatus");

        log.debug("PKI cert status: serial={}, status={}", response.getSerial(), response.getStatus());
        return new PkiCertStatus(
                response.getSerial(),
                response.getCertSerial(),
                response.getCommonName(),
                response.getStatus(),
                response.getNotBefore(),
                response.getNotAfter(),
                response.getRevokedAt(),
                fromProtoRevocation(response.getReason()),
                response.getBy());
    }

    @Override
    public PkiCrl getCrl(PkiCrlFormat format) {
        PkiCrlFormat effective = format == null ? PkiCrlFormat.PEM : format;
        PkiGetCrlRequest request = PkiGetCrlRequest.newBuilder()
                .setFormat(toProtoCrlFormat(effective))
                .build();

        PkiGetCrlResponse response = callWithRetry(
                (ch, r) -> PkiGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .getCrl((PkiGetCrlRequest) r),
                request, "pki.getCrl");

        return crlOf(toProtoCrlFormat(effective), response.getFormatValue(),
                response.getCrl().toByteArray(), response.getCrlNumber(),
                response.getThisUpdate(), response.getNextUpdate());
    }

    @Override
    public PkiCrl rotateCrl(PkiCrlFormat format) {
        PkiCrlFormat effective = format == null ? PkiCrlFormat.PEM : format;
        PkiRotateCrlRequest request = PkiRotateCrlRequest.newBuilder()
                .setFormat(toProtoCrlFormat(effective))
                .build();

        PkiRotateCrlResponse response = callWithRetry(
                (ch, r) -> PkiGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .rotateCrl((PkiRotateCrlRequest) r),
                request, "pki.rotateCrl");

        log.debug("PKI CRL rotated: number={}", response.getCrlNumber());
        return crlOf(toProtoCrlFormat(effective), response.getFormatValue(),
                response.getCrl().toByteArray(), response.getCrlNumber(),
                response.getThisUpdate(), response.getNextUpdate());
    }

    @Override
    public PkiTidyResult tidy(long retentionSeconds) {
        PkiTidyRequest request = PkiTidyRequest.newBuilder()
                .setRetentionSeconds(retentionSeconds)
                .build();

        PkiTidyResponse response = callWithRetry(
                (ch, r) -> PkiGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .tidy((PkiTidyRequest) r),
                request, "pki.tidy");

        log.debug("PKI tidy: retention={}s, removed={}",
                response.getRetentionSeconds(), response.getRemovedCerts());
        return new PkiTidyResult(response.getRetentionSeconds(), response.getRemovedCerts());
    }
}
