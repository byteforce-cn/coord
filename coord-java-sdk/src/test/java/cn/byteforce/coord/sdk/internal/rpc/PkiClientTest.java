package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.CoordConfig;
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
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the {@link PkiClient} gRPC client against an in-process netty server
 * with a stub {@link PkiGrpc.PkiImplBase}.
 * <p>
 * Covers ISSUE-000 Phase 2 SDK surface: rotateCert / listCerts / getCertByCN.
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
        assertThat(cert.keyPem()).isNotEmpty();
        assertThat(cert.status()).isEqualTo("active");
        assertThat(fake.issueCalls).isEqualTo(1);
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
    void getCertByCNReturnsActiveWithPrivateKey() {
        PkiCertInfo cert = pki.getCertByCN("svc-a.local");
        assertThat(cert.serial()).isEqualTo("0x2");
        assertThat(cert.keyPem()).isNotEmpty();
        assertThat(cert.status()).isEqualTo("active");
    }

    // ──── Stub 实现 ────

    private static final class FakePkiService extends PkiGrpc.PkiImplBase {
        int issueCalls;
        int rotateCalls;

        @Override
        public void initCa(PkiInitCaRequest request, StreamObserver<PkiInitCaResponse> observer) {
            observer.onNext(PkiInitCaResponse.newBuilder().build());
            observer.onCompleted();
        }

        @Override
        public void issueCert(PkiIssueCertRequest request, StreamObserver<PkiIssueCertResponse> observer) {
            issueCalls++;
            observer.onNext(PkiIssueCertResponse.newBuilder()
                    .setCommonName(request.getCommonName())
                    .setCertPem("-----BEGIN CERTIFICATE-----\n" + request.getCommonName() + "\n-----END CERTIFICATE-----")
                    .setKeyPem("-----BEGIN PRIVATE KEY-----\nkey\n-----END PRIVATE KEY-----")
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
                    .setKeyPem("-----BEGIN PRIVATE KEY-----\nrenewed-key\n-----END PRIVATE KEY-----")
                    .setSerial("0x3")
                    .setStatus("active")
                    .setParentSerial("0x2")
                    .build());
            observer.onCompleted();
        }

        @Override
        public void rotateCert(PkiRotateCertRequest request, StreamObserver<PkiRotateCertResponse> observer) {
            rotateCalls++;
            observer.onNext(PkiRotateCertResponse.newBuilder()
                    .setCommonName(request.getCommonName())
                    .setCertPem("-----BEGIN CERTIFICATE-----\nnew\n-----END CERTIFICATE-----")
                    .setKeyPem("-----BEGIN PRIVATE KEY-----\nnew-key\n-----END PRIVATE KEY-----")
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
                    .addCerts(cn.byteforce.coord.sdk.internal.proto.PkiCertSummary.newBuilder()
                            .setCommonName(request.getCommonName())
                            .setCertPem("-----BEGIN CERTIFICATE-----\nnew\n-----END CERTIFICATE-----")
                            .setSerial("0x2")
                            .setStatus("active")
                            .setParentSerial("0x1"))
                    .addCerts(cn.byteforce.coord.sdk.internal.proto.PkiCertSummary.newBuilder()
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
                    .setKeyPem("-----BEGIN PRIVATE KEY-----\nnew-key\n-----END PRIVATE KEY-----")
                    .setNotBefore(1_700_000_000L)
                    .setNotAfter(1_700_086_400L)
                    .setSerial("0x2")
                    .setStatus("active")
                    .setParentSerial("0x1")
                    .build());
            observer.onCompleted();
        }

        @Override
        public void verifyCert(PkiVerifyCertRequest request, StreamObserver<PkiVerifyCertResponse> observer) {
            observer.onNext(PkiVerifyCertResponse.newBuilder().setValid(true).build());
            observer.onCompleted();
        }

        @Override
        public void getCaCert(PkiGetCaCertRequest request, StreamObserver<PkiGetCaCertResponse> observer) {
            observer.onNext(PkiGetCaCertResponse.newBuilder().setCaCertPem("-----BEGIN CERTIFICATE-----\nca\n-----END CERTIFICATE-----").build());
            observer.onCompleted();
        }
    }
}
