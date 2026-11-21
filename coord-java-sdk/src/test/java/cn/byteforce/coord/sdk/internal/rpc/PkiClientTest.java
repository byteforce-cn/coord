package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.CoordException;
import cn.byteforce.coord.sdk.ErrorCode;
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
import cn.byteforce.coord.contracts.pki.v1.PkiCrlFormat;
import cn.byteforce.coord.contracts.pki.v1.PkiRevocationReason;
import cn.byteforce.coord.contracts.pki.v1.PkiRevokeCertRequest;
import cn.byteforce.coord.contracts.pki.v1.PkiRevokeCertResponse;
import cn.byteforce.coord.contracts.pki.v1.PkiRotateCrlRequest;
import cn.byteforce.coord.contracts.pki.v1.PkiRotateCrlResponse;
import cn.byteforce.coord.contracts.pki.v1.PkiTidyRequest;
import cn.byteforce.coord.contracts.pki.v1.PkiTidyResponse;
import cn.byteforce.coord.contracts.pki.v1.PkiCertificateUsage;
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
import cn.byteforce.coord.contracts.pki.v1.PkiSignCsrRequest;
import cn.byteforce.coord.contracts.pki.v1.PkiSignCsrResponse;
import cn.byteforce.coord.contracts.pki.v1.PkiVerifyCertRequest;
import cn.byteforce.coord.contracts.pki.v1.PkiVerifyCertResponse;
import cn.byteforce.coord.sdk.pki.PkiCertInfo;
import cn.byteforce.coord.sdk.pki.PkiCertStatus;
import cn.byteforce.coord.sdk.pki.PkiCertSummary;
import cn.byteforce.coord.sdk.pki.PkiCrl;
import cn.byteforce.coord.sdk.pki.PkiRevocationInfo;
import cn.byteforce.coord.sdk.pki.PkiTidyResult;
import cn.byteforce.coord.sdk.pki.PkiCertUsage;
import cn.byteforce.coord.sdk.pki.PkiClient;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the {@link PkiClient} gRPC client against an in-process netty server
 * with a stub {@link PkiGrpc.PkiImplBase}.
 * <p>
 * Covers SDK surface: rotateCert / listCerts / getCertByCN.
 */
class PkiClientTest {

    private Server server;
    private ManagedChannel channel;
    private PkiClient pki;
    private FakePkiService fake;

    @BeforeEach
    void setUp() throws Exception {
        fake = new FakePkiService();
        server = ServerBuilder.forPort(0).addService(fake).build().start();
        channel = ManagedChannelBuilder.forAddress("localhost", server.getPort())
                .usePlaintext()
                .build();

        AgentChannelManager channelManager = mock(AgentChannelManager.class);
        when(channelManager.getChannel()).thenReturn(channel);

        pki = new PkiClientImpl(
                channelManager,
                new ErrorMapper(),
                new RetryTemplate(),
                new ObservabilityProvider() {
                },
                CoordConfig.builder().agentHost("localhost").build());
    }

    @AfterEach
    void tearDown() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    @Test
    void issueCertReturnsCertInfo() {
        PkiCertInfo cert = pki.issueCert("svc-a.local");
        assertThat(cert.commonName()).isEqualTo("svc-a.local");
        assertThat(cert.serial()).isEqualTo("0x1");
        assertThat(cert.certPem()).contains("BEGIN CERTIFICATE");
        assertThat(cert.keyName()).isNotEmpty();
        assertThat(cert.keyVersion()).isEqualTo(1);
        assertThat(cert.status()).isEqualTo("active");
        assertThat(fake.issueCalls).isEqualTo(1);
    }

    @Test
    void issueCertSendsUsageProfile() {
        pki.issueCert("svc-a.local", 3600, List.of("svc-a.local"), List.of(),
                PkiCertUsage.SERVER);

        assertThat(fake.lastIssue.getUsage())
                .isEqualTo(PkiCertificateUsage.PKI_CERTIFICATE_USAGE_SERVER);
    }

    @Test
    void rotateCertWithoutUsageAsksServerToInherit() {
        pki.rotateCert("svc-a.local", 3600, List.of(), List.of());

        assertThat(fake.lastRotate.getUsage())
                .isEqualTo(PkiCertificateUsage.PKI_CERTIFICATE_USAGE_UNSPECIFIED);
    }

    @Test
    void signCsrReturnsExternalCertWithoutKeyReference() {
        byte[] csr = "fake-csr".getBytes(StandardCharsets.UTF_8);
        PkiCertInfo cert = pki.signCsr(csr, 3600, PkiCertUsage.CLIENT);

        assertThat(cert.commonName()).isEqualTo("svc-csr.local");
        assertThat(cert.serial()).isEqualTo("0x3");
        assertThat(cert.status()).isEqualTo("active");
        assertThat(cert.keyName()).isEmpty();
        assertThat(cert.keyVersion()).isZero();
        assertThat(fake.lastSignCsr.getCsrDer().toByteArray()).isEqualTo(csr);
        assertThat(fake.lastSignCsr.getUsage())
                .isEqualTo(PkiCertificateUsage.PKI_CERTIFICATE_USAGE_CLIENT);
    }

    @Test
    void signCsrInvalidCsrPropagatesInvalidArgument() {
        fake.signCsrValid = false;

        assertThatThrownBy(() -> pki.signCsr("bad".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(CoordException.class)
                .extracting(e -> ((CoordException) e).getErrorCode())
                .isEqualTo(ErrorCode.INVALID_ARGUMENT);
    }

    @Test
    void issueCertSendsExplicitSans() {
        pki.issueCert("svc-a.local", 3600, List.of("svc-a.local", "alt.example"),
                List.of("10.0.0.7", "::1"));

        assertThat(fake.lastIssue.getDnsNamesList()).containsExactly("svc-a.local", "alt.example");
        assertThat(fake.lastIssue.getIpAddressesList()).containsExactly("10.0.0.7", "::1");
    }

    @Test
    void issueCertWithoutSansLeavesThemForTheServerToDerive() {
        pki.issueCert("svc-a.local", 3600);

        assertThat(fake.lastIssue.getDnsNamesList()).isEmpty();
        assertThat(fake.lastIssue.getIpAddressesList()).isEmpty();
    }

    @Test
    void rotateCertSendsExplicitSans() {
        pki.rotateCert("svc-a.local", 3600, List.of("svc-a.local"), List.of("10.0.0.7"));

        assertThat(fake.lastRotate.getDnsNamesList()).containsExactly("svc-a.local");
        assertThat(fake.lastRotate.getIpAddressesList()).containsExactly("10.0.0.7");
    }

    @Test
    void rotateCertWithoutSansLeavesThemForTheServerToInherit() {
        pki.rotateCert("svc-a.local", 3600);

        assertThat(fake.lastRotate.getDnsNamesList()).isEmpty();
        assertThat(fake.lastRotate.getIpAddressesList()).isEmpty();
    }

    @Test
    void rotateCertReturnsNewSerialWithParentChain() {
        PkiCertInfo rotated = pki.rotateCert("svc-a.local", 3600);
        assertThat(rotated.serial()).isEqualTo("0x2");
        assertThat(rotated.status()).isEqualTo("active");
        assertThat(rotated.parentSerial()).isEqualTo("0x1");
        assertThat(fake.rotateCalls).isEqualTo(1);
    }

    @Test
    void listCertsReturnsActiveAndRetiredSummaries() {
        List<PkiCertSummary> certs = pki.listCerts("svc-a.local");
        assertThat(certs).hasSize(2);
        assertThat(certs.get(0).serial()).isEqualTo("0x2");
        assertThat(certs.get(0).status()).isEqualTo("active");
        assertThat(certs.get(1).serial()).isEqualTo("0x1");
        assertThat(certs.get(1).status()).isEqualTo("retired");
        // ListCerts 不返回私钥（仅公钥/证书）
        assertThat(certs.get(1).certPem()).contains("BEGIN CERTIFICATE");
    }

    @Test
    void verifyCertReturnsServerDecision() {
        assertThat(pki.verifyCert("pem")).isTrue();

        fake.verifyValid = false;
        assertThat(pki.verifyCert("forged-pem")).isFalse();
    }

    @Test
    void getCertBySerialReturnsSummary() {
        PkiCertSummary cert = pki.getCertBySerial("0x2");
        assertThat(cert.commonName()).isEqualTo("svc-a.local");
        assertThat(cert.serial()).isEqualTo("0x2");
        assertThat(cert.status()).isEqualTo("active");
        assertThat(cert.parentSerial()).isEqualTo("0x1");
        assertThat(cert.certPem()).contains("BEGIN CERTIFICATE");
    }

    @Test
    void getCertBySerialUnknownSerialPropagatesNotFound() {
        assertThatThrownBy(() -> pki.getCertBySerial("missing"))
                .isInstanceOf(CoordException.class)
                .extracting(e -> ((CoordException) e).getErrorCode())
                .isEqualTo(ErrorCode.NOT_FOUND);
    }

    @Test
    void getCertByCNReturnsActiveCertificateWithoutKeyMaterial() {

        PkiCertInfo cert = pki.getCertByCN("svc-a.local");
        assertThat(cert.serial()).isEqualTo("0x2");
        assertThat(cert.keyName()).isNotEmpty();
        assertThat(cert.keyVersion()).isEqualTo(2);
        assertThat(cert.status()).isEqualTo("active");
    }

    @Test
    void revokeCertMapsReasonAndReturnsRecord() {
        PkiRevocationInfo revoked = pki.revokeCert("0x2", cn.byteforce.coord.sdk.pki.PkiRevocationReason.KEY_COMPROMISE);

        assertThat(fake.lastRevoke.getSerial()).isEqualTo("0x2");
        assertThat(fake.lastRevoke.getReason())
                .isEqualTo(PkiRevocationReason.PKI_REVOCATION_REASON_KEY_COMPROMISE);
        assertThat(revoked.serial()).isEqualTo("0x2");
        assertThat(revoked.certSerial()).isEqualTo("0x2");
        assertThat(revoked.commonName()).isEqualTo("svc-a.local");
        assertThat(revoked.status()).isEqualTo("revoked");
        assertThat(revoked.reason()).isEqualTo(cn.byteforce.coord.sdk.pki.PkiRevocationReason.KEY_COMPROMISE);
        assertThat(revoked.by()).isEqualTo("ops@example");
        assertThat(revoked.revokedAtEpochSec()).isEqualTo(1_700_000_100L);
    }

    @Test
    void revokeCertDefaultsToUnspecifiedReason() {
        pki.revokeCert("0x2");
        assertThat(fake.lastRevoke.getReason())
                .isEqualTo(PkiRevocationReason.PKI_REVOCATION_REASON_UNSPECIFIED);
    }

    @Test
    void revokeCertUnknownSerialPropagatesNotFound() {
        assertThatThrownBy(() -> pki.revokeCert("missing"))
                .isInstanceOf(CoordException.class)
                .extracting(e -> ((CoordException) e).getErrorCode())
                .isEqualTo(ErrorCode.NOT_FOUND);
    }

    @Test
    void getCertStatusExposesRevocation() {
        PkiCertStatus status = pki.getCertStatus("0x2");
        assertThat(status.status()).isEqualTo("revoked");
        assertThat(status.revoked()).isTrue();
        assertThat(status.reason()).isEqualTo(cn.byteforce.coord.sdk.pki.PkiRevocationReason.KEY_COMPROMISE);
        assertThat(status.by()).isEqualTo("ops@example");
        assertThat(status.revokedAtEpochSec()).isEqualTo(1_700_000_100L);
    }

    @Test
    void getCrlDefaultsToPemAndRotateForcesNewNumber() {
        PkiCrl crl = pki.getCrl();
        assertThat(fake.lastGetCrl.getFormat()).isEqualTo(PkiCrlFormat.PKI_CRL_FORMAT_PEM);
        assertThat(crl.format()).isEqualTo(cn.byteforce.coord.sdk.pki.PkiCrlFormat.PEM);
        assertThat(crl.pem()).startsWith("-----BEGIN X509 CRL-----");
        assertThat(crl.crlNumber()).isEqualTo(1);

        PkiCrl rotated = pki.rotateCrl(cn.byteforce.coord.sdk.pki.PkiCrlFormat.DER);
        assertThat(fake.lastRotateCrl.getFormat()).isEqualTo(PkiCrlFormat.PKI_CRL_FORMAT_DER);
        assertThat(rotated.format()).isEqualTo(cn.byteforce.coord.sdk.pki.PkiCrlFormat.DER);
        assertThat(rotated.pem()).isNull();
        assertThat(rotated.crlNumber()).isEqualTo(2);
    }

    @Test
    void tidyPropagatesRetentionAndRemovedCount() {
        PkiTidyResult result = pki.tidy(3600);
        assertThat(fake.lastTidy.getRetentionSeconds()).isEqualTo(3600);
        assertThat(result.retentionSeconds()).isEqualTo(3600);
        assertThat(result.removedCerts()).isEqualTo(3);
    }

    // ──── Stub 实现 ────

    private static final class FakePkiService extends PkiGrpc.PkiImplBase {
        int issueCalls;
        int rotateCalls;
        boolean verifyValid = true;
        boolean signCsrValid = true;
        PkiIssueCertRequest lastIssue;
        PkiRotateCertRequest lastRotate;
        PkiSignCsrRequest lastSignCsr;
        PkiRevokeCertRequest lastRevoke;
        PkiGetCrlRequest lastGetCrl;
        PkiRotateCrlRequest lastRotateCrl;
        PkiTidyRequest lastTidy;

        @Override
        public void initCa(PkiInitCaRequest request, StreamObserver<PkiInitCaResponse> observer) {
            observer.onNext(PkiInitCaResponse.newBuilder().build());
            observer.onCompleted();
        }

        @Override
        public void issueCert(PkiIssueCertRequest request, StreamObserver<PkiIssueCertResponse> observer) {
            issueCalls++;
            lastIssue = request;
            observer.onNext(PkiIssueCertResponse.newBuilder()
                    .setCommonName(request.getCommonName())
                    .setCertPem("-----BEGIN CERTIFICATE-----\n" + request.getCommonName() + "\n-----END CERTIFICATE-----")
                    .setKeyMaterial(cn.byteforce.coord.contracts.pki.v1.PkiKeyMaterial.newBuilder()
                            .setKind("coord_managed")
                            .setKeyName("pki/leaf/svc-a.local")
                            .setKeyVersion(1))
                    .setNotBefore(1_700_000_000L)
                    .setNotAfter(1_700_086_400L)
                    .setSerial("0x1")
                    .setStatus("active")
                    .build());
            observer.onCompleted();
        }

        @Override
        public void renewCert(PkiRenewCertRequest request, StreamObserver<PkiRenewCertResponse> observer) {
            observer.onNext(PkiRenewCertResponse.newBuilder()
                    .setCommonName("svc-a.local")
                    .setCertPem("-----BEGIN CERTIFICATE-----\nrenewed\n-----END CERTIFICATE-----")
                    .setKeyMaterial(cn.byteforce.coord.contracts.pki.v1.PkiKeyMaterial.newBuilder()
                            .setKind("coord_managed")
                            .setKeyName("pki/leaf/svc-a.local")
                            .setKeyVersion(2))
                    .setSerial("0x3")
                    .setStatus("active")
                    .setParentSerial("0x2")
                    .build());
            observer.onCompleted();
        }

        @Override
        public void rotateCert(PkiRotateCertRequest request, StreamObserver<PkiRotateCertResponse> observer) {
            rotateCalls++;
            lastRotate = request;
            observer.onNext(PkiRotateCertResponse.newBuilder()
                    .setCommonName(request.getCommonName())
                    .setCertPem("-----BEGIN CERTIFICATE-----\nnew\n-----END CERTIFICATE-----")
                    .setKeyMaterial(cn.byteforce.coord.contracts.pki.v1.PkiKeyMaterial.newBuilder()
                            .setKind("coord_managed")
                            .setKeyName("pki/leaf/svc-a.local")
                            .setKeyVersion(2))
                    .setNotBefore(1_700_000_000L)
                    .setNotAfter(1_700_086_400L)
                    .setSerial("0x2")
                    .setStatus("active")
                    .setParentSerial("0x1")
                    .build());
            observer.onCompleted();
        }

        @Override
        public void listCerts(PkiListCertsRequest request, StreamObserver<PkiListCertsResponse> observer) {
            observer.onNext(PkiListCertsResponse.newBuilder()
                    .addCerts(cn.byteforce.coord.contracts.pki.v1.PkiCertSummary.newBuilder()
                            .setCommonName(request.getCommonName())
                            .setCertPem("-----BEGIN CERTIFICATE-----\nnew\n-----END CERTIFICATE-----")
                            .setSerial("0x2")
                            .setStatus("active")
                            .setParentSerial("0x1"))
                    .addCerts(cn.byteforce.coord.contracts.pki.v1.PkiCertSummary.newBuilder()
                            .setCommonName(request.getCommonName())
                            .setCertPem("-----BEGIN CERTIFICATE-----\nold\n-----END CERTIFICATE-----")
                            .setSerial("0x1")
                            .setStatus("retired"))
                    .build());
            observer.onCompleted();
        }

        @Override
        public void getCertByCN(PkiGetCertByCNRequest request, StreamObserver<PkiGetCertByCNResponse> observer) {
            observer.onNext(PkiGetCertByCNResponse.newBuilder()
                    .setCommonName(request.getCommonName())
                    .setCertPem("-----BEGIN CERTIFICATE-----\nnew\n-----END CERTIFICATE-----")
                    .setKeyMaterial(cn.byteforce.coord.contracts.pki.v1.PkiKeyMaterial.newBuilder()
                            .setKind("coord_managed")
                            .setKeyName("pki/leaf/svc-a.local")
                            .setKeyVersion(2))
                    .setNotBefore(1_700_000_000L)
                    .setNotAfter(1_700_086_400L)
                    .setSerial("0x2")
                    .setStatus("active")
                    .setParentSerial("0x1")
                    .build());
            observer.onCompleted();
        }

        @Override
        public void signCsr(PkiSignCsrRequest request, StreamObserver<PkiSignCsrResponse> observer) {
            lastSignCsr = request;
            if (!signCsrValid) {
                observer.onError(io.grpc.Status.INVALID_ARGUMENT
                        .withDescription("CSR proof-of-possession signature is invalid")
                        .asRuntimeException());
                return;
            }
            observer.onNext(PkiSignCsrResponse.newBuilder()
                    .setCommonName("svc-csr.local")
                    .setCertPem("-----BEGIN CERTIFICATE-----\ncsr\n-----END CERTIFICATE-----")
                    .setNotBefore(1_700_000_000L)
                    .setNotAfter(1_700_086_400L)
                    .setSerial("0x3")
                    .setStatus("active")
                    .setParentSerial("")
                    .build());
            observer.onCompleted();
        }

        @Override
        public void getCertBySerial(PkiGetCertBySerialRequest request,
                                    StreamObserver<PkiGetCertBySerialResponse> observer) {
            if (!"0x2".equals(request.getSerial())) {
                observer.onError(io.grpc.Status.NOT_FOUND
                        .withDescription("certificate not found by serial: " + request.getSerial())
                        .asRuntimeException());
                return;
            }
            observer.onNext(PkiGetCertBySerialResponse.newBuilder()
                    .setCommonName("svc-a.local")
                    .setCertPem("-----BEGIN CERTIFICATE-----\nnew\n-----END CERTIFICATE-----")
                    .setNotBefore(1_700_000_000L)
                    .setNotAfter(1_700_086_400L)
                    .setSerial("0x2")
                    .setStatus("active")
                    .setParentSerial("0x1")
                    .build());
            observer.onCompleted();
        }

        @Override
        public void revokeCert(PkiRevokeCertRequest request,
                               StreamObserver<PkiRevokeCertResponse> observer) {
            lastRevoke = request;
            if (!"0x2".equals(request.getSerial())) {
                observer.onError(io.grpc.Status.NOT_FOUND
                        .withDescription("certificate not found by serial: " + request.getSerial())
                        .asRuntimeException());
                return;
            }
            observer.onNext(PkiRevokeCertResponse.newBuilder()
                    .setSerial(request.getSerial())
                    .setCertSerial(request.getSerial())
                    .setCommonName("svc-a.local")
                    .setStatus("revoked")
                    .setRevokedAt(1_700_000_100L)
                    .setReason(request.getReason())
                    .setBy("ops@example")
                    .build());
            observer.onCompleted();
        }

        @Override
        public void getCertStatus(PkiGetCertStatusRequest request,
                                  StreamObserver<PkiGetCertStatusResponse> observer) {
            if (!"0x2".equals(request.getSerial())) {
                observer.onError(io.grpc.Status.NOT_FOUND
                        .withDescription("certificate not found by serial: " + request.getSerial())
                        .asRuntimeException());
                return;
            }
            observer.onNext(PkiGetCertStatusResponse.newBuilder()
                    .setSerial(request.getSerial())
                    .setCertSerial(request.getSerial())
                    .setCommonName("svc-a.local")
                    .setStatus("revoked")
                    .setNotBefore(1_700_000_000L)
                    .setNotAfter(1_700_086_400L)
                    .setRevokedAt(1_700_000_100L)
                    .setReason(PkiRevocationReason.PKI_REVOCATION_REASON_KEY_COMPROMISE)
                    .setBy("ops@example")
                    .build());
            observer.onCompleted();
        }

        @Override
        public void getCrl(PkiGetCrlRequest request, StreamObserver<PkiGetCrlResponse> observer) {
            lastGetCrl = request;
            byte[] body = request.getFormat() == PkiCrlFormat.PKI_CRL_FORMAT_DER
                    ? new byte[] {1, 2, 3}
                    : "-----BEGIN X509 CRL-----\ncrl\n-----END X509 CRL-----".getBytes(StandardCharsets.UTF_8);
            observer.onNext(PkiGetCrlResponse.newBuilder()
                    .setCrl(com.google.protobuf.ByteString.copyFrom(body))
                    .setFormat(request.getFormat())
                    .setCrlNumber(1)
                    .setThisUpdate(1_700_000_000L)
                    .setNextUpdate(1_700_086_400L)
                    .build());
            observer.onCompleted();
        }

        @Override
        public void rotateCrl(PkiRotateCrlRequest request,
                              StreamObserver<PkiRotateCrlResponse> observer) {
            lastRotateCrl = request;
            byte[] body = request.getFormat() == PkiCrlFormat.PKI_CRL_FORMAT_DER
                    ? new byte[] {4, 5, 6}
                    : "-----BEGIN X509 CRL-----\nnew\n-----END X509 CRL-----".getBytes(StandardCharsets.UTF_8);
            observer.onNext(PkiRotateCrlResponse.newBuilder()
                    .setCrl(com.google.protobuf.ByteString.copyFrom(body))
                    .setFormat(request.getFormat())
                    .setCrlNumber(2)
                    .setThisUpdate(1_700_000_100L)
                    .setNextUpdate(1_700_086_400L)
                    .build());
            observer.onCompleted();
        }

        @Override
        public void tidy(PkiTidyRequest request, StreamObserver<PkiTidyResponse> observer) {
            lastTidy = request;
            long retention = request.getRetentionSeconds() == 0 ? 86_400 : request.getRetentionSeconds();
            observer.onNext(PkiTidyResponse.newBuilder()
                    .setRetentionSeconds(retention)
                    .setRemovedCerts(3)
                    .build());
            observer.onCompleted();
        }

        @Override
        public void verifyCert(PkiVerifyCertRequest request, StreamObserver<PkiVerifyCertResponse> observer) {
            observer.onNext(PkiVerifyCertResponse.newBuilder().setValid(verifyValid).build());
            observer.onCompleted();
        }

        @Override
        public void getCaCert(PkiGetCaCertRequest request, StreamObserver<PkiGetCaCertResponse> observer) {
            observer.onNext(PkiGetCaCertResponse.newBuilder().setCaCertPem("-----BEGIN CERTIFICATE-----\nca\n-----END CERTIFICATE-----").build());
            observer.onCompleted();
        }
    }
}
