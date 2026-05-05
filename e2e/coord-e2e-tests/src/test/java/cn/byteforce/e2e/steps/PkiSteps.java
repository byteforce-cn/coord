package cn.byteforce.e2e.steps;

import coord.v1.Coord;
import coord.v1.PkiServiceGrpc;
import cn.byteforce.e2e.util.RetryHelper;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequestBuilder;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.StringReader;
import java.io.StringWriter;
import java.security.KeyPair;
import java.security.KeyPairGenerator;

import static org.assertj.core.api.Assertions.assertThat;

public class PkiSteps {

    @Autowired private PkiServiceGrpc.PkiServiceBlockingStub pkiStub;
    @Autowired private ScenarioState state;

    // ── PKI Role ──────────────────────────────────────────────────────────────────

    @Given("PKI角色 {string} 已创建 allowed_domains={string} max_ttl={string}")
    public void ensureRole(String role, String domains, String maxTtl) {
        try {
            pkiStub.createPkiRole(Coord.CreatePkiRoleRequest.newBuilder()
                    .setRoleName(role)
                    .setAllowedDomains(domains)
                    .setMaxTtl(maxTtl)
                    .build());
        } catch (io.grpc.StatusRuntimeException e) {
            if (e.getStatus().getCode() != io.grpc.Status.Code.ALREADY_EXISTS) throw e;
        }
        state.pkiRole = role;
    }

    @When("创建 PKI 角色 {string} allowed_domains={string} max_ttl={string}")
    public void createRole(String role, String domains, String maxTtl) {
        pkiStub.createPkiRole(Coord.CreatePkiRoleRequest.newBuilder()
                .setRoleName(role)
                .setAllowedDomains(domains)
                .setMaxTtl(maxTtl)
                .build());
        state.pkiRole = role;
    }

    // ── Issue cert ────────────────────────────────────────────────────────────────

    @When("颁发证书 common_name={string} ttl={string}")
    public void issueCert(String cn, String ttl) {
        Coord.IssueCertificateResponse r = pkiStub.issueCertificate(
                Coord.IssueCertificateRequest.newBuilder()
                        .setRoleName(state.pkiRole)
                        .setCommonName(cn)
                        .setTtl(ttl)
                        .build());
        state.pkiCertPem = r.getCertificatePem();
        state.pkiSerialNumber = r.getSerialNumber();
        assertThat(state.pkiCertPem).isNotBlank();
    }

    @Then("返回证书 PEM 非空")
    public void verifyCertPem() {
        assertThat(state.pkiCertPem).isNotBlank();
    }

    @Then("serial_number 非空")
    public void verifySerial() {
        assertThat(state.pkiSerialNumber).isNotBlank();
    }

    @Then("PEM 包含 {string}")
    public void pemContains(String fragment) {
        assertThat(state.pkiCertPem).contains(fragment);
    }

    // ── Renew cert ────────────────────────────────────────────────────────────────

    @When("续签证书 serial={string} ttl={string}")
    public void renewCert(String serial, String ttl) {
        state.pkiCertPem = pkiStub.renewCertificate(
                Coord.RenewCertificateRequest.newBuilder()
                        .setSerialNumber(serial.isEmpty() ? state.pkiSerialNumber : serial)
                        .setTtl(ttl)
                        .build()).getCertificatePem();
    }

    @When("续签当前证书 ttl={string}")
    public void renewCurrentCert(String ttl) {
        state.pkiCertPem = pkiStub.renewCertificate(
                Coord.RenewCertificateRequest.newBuilder()
                        .setSerialNumber(state.pkiSerialNumber)
                        .setTtl(ttl)
                        .build()).getCertificatePem();
    }

    @Then("新 PEM 非空")
    public void newPemNotBlank() {
        assertThat(state.pkiCertPem).isNotBlank();
    }

    // ── Revoke cert ───────────────────────────────────────────────────────────────

    @When("吊销证书 serial={string}")
    public void revokeCert(String serial) {
        pkiStub.revokeCertificate(Coord.RevokeCertificateRequest.newBuilder()
                .setSerialNumber(serial.isEmpty() ? state.pkiSerialNumber : serial)
                .build());
    }

    @When("吊销当前证书")
    public void revokeCurrentCert() {
        pkiStub.revokeCertificate(Coord.RevokeCertificateRequest.newBuilder()
                .setSerialNumber(state.pkiSerialNumber)
                .build());
    }

    // ── CRL / OCSP ────────────────────────────────────────────────────────────────

    @When("获取 CA Chain")
    public void getCaChain() {
        state.pkiCaChain = pkiStub.getCaChain(Coord.GetCaChainRequest.newBuilder().build())
                .getCaCertPem();
    }

    @Then("CA Chain 非空")
    public void caChainNotBlank() {
        assertThat(state.pkiCaChain).isNotBlank();
    }

    @When("获取 CRL")
    public void getCrl() {
        state.pkiCrl = pkiStub.getCrl(Coord.GetCrlRequest.newBuilder().build()).getCrlPem();
    }

    @Then("CRL 包含当前 serial")
    public void crlContainsSerial() {
        assertThat(state.pkiCrl).contains(state.pkiSerialNumber);
    }

    @When("检查证书状态 serial={string}")
    public void checkStatus(String serial) {
        state.pkiCertStatus = pkiStub.checkCertificateStatus(
                Coord.CheckCertificateStatusRequest.newBuilder()
                        .setSerialNumber(serial.isEmpty() ? state.pkiSerialNumber : serial)
                        .build()).getStatus();
    }

    @When("检查当前证书状态")
    public void checkCurrentStatus() {
        state.pkiCertStatus = pkiStub.checkCertificateStatus(
                Coord.CheckCertificateStatusRequest.newBuilder()
                        .setSerialNumber(state.pkiSerialNumber)
                        .build()).getStatus();
    }

    @Then("证书状态为 {string}")
    public void verifyCertStatus(String expected) {
        assertThat(state.pkiCertStatus).isEqualTo(expected);
    }

    // ── ACME ──────────────────────────────────────────────────────────────────────

    @When("创建 ACME Order domain={string}")
    public void createAcmeOrder(String domain) {
        Coord.CreateAcmeOrderResponse r = pkiStub.createAcmeOrder(
                Coord.CreateAcmeOrderRequest.newBuilder().setDomain(domain).build());
        state.acmeOrderId = r.getOrderId();
        state.acmeChallengeToken = r.getChallengeToken();
        state.acmeDomain = domain;
        assertThat(state.acmeOrderId).isNotBlank();
    }

    @Then("返回 order_id 和 challenge_token")
    public void verifyAcmeOrderAndToken() {
        assertThat(state.acmeOrderId).isNotBlank();
        assertThat(state.acmeChallengeToken).isNotBlank();
    }

    @When("完成 ACME Challenge")
    public void completeChallenge() {
        pkiStub.completeAcmeChallenge(Coord.CompleteAcmeChallengeRequest.newBuilder()
                .setOrderId(state.acmeOrderId)
                .setDomain(state.acmeDomain)
                .setChallengeToken(state.acmeChallengeToken)
                .build());
    }

    @When("更新自动续期策略 enabled={word} renew_before_seconds={int}")
    public void updateAutoRenewPolicy(String enabled, int renewBefore) {
        pkiStub.updateAutoRenewPolicy(Coord.UpdateAutoRenewPolicyRequest.newBuilder()
                .setSerialNumber(state.pkiSerialNumber)
                .setEnabled(Boolean.parseBoolean(enabled))
                .setRenewBeforeSeconds(renewBefore)
                .build());
    }

    @When("运行 RunAutoRenew")
    public void runAutoRenew() {
        state.autoRenewResponse = pkiStub.runAutoRenew(
                Coord.RunAutoRenewRequest.newBuilder().build());
    }

    @Then("RunAutoRenew 已处理至少 {int} 条（策略按需运行）")
    public void verifyAutoRenewCount(int minCount) {
        assertThat(state.autoRenewResponse).isNotNull();
        // Generated SDK uses field-number suffix: getRenewedCount1() for field 1 (renewed_count)
        assertThat(state.autoRenewResponse.getRenewedCount1()).isGreaterThanOrEqualTo(minCount);
    }

    @When("Finalize ACME Order csr={string}")
    public void finalizeOrder(String csr) {
        state.pkiCertPem = pkiStub.finalizeAcmeOrder(
                Coord.FinalizeAcmeOrderRequest.newBuilder()
                        .setOrderId(state.acmeOrderId)
                        .setCommonName(state.acmeDomain)
                        .setCsrPem(csr)
                        .build()).getCertificatePem();
    }

    @When("Finalize ACME Order 使用真实 CSR domain={string}")
    public void finalizeOrderRealCsr(String domain) throws Exception {
        // Generate RSA-2048 key pair
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        KeyPair kp = kpg.generateKeyPair();

        // Build PKCS#10 CSR
        X500Name subject = new X500Name("CN=" + domain);
        JcaPKCS10CertificationRequestBuilder builder =
                new JcaPKCS10CertificationRequestBuilder(subject, kp.getPublic());
        ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA").build(kp.getPrivate());
        PKCS10CertificationRequest csr = builder.build(signer);

        // Encode to PEM
        StringWriter sw = new StringWriter();
        try (JcaPEMWriter pw = new JcaPEMWriter(sw)) {
            pw.writeObject(csr);
        }
        String csrPem = sw.toString();

        // Finalize ACME order with the real CSR
        state.pkiCertPem = pkiStub.finalizeAcmeOrder(
                Coord.FinalizeAcmeOrderRequest.newBuilder()
                        .setOrderId(state.acmeOrderId)
                        .setCommonName(domain)
                        .setCsrPem(csrPem)
                        .build()).getCertificatePem();
    }

    @Then("证书 Subject CN 为 {string}")
    public void verifyCertSubjectCn(String expectedCn) throws Exception {
        try (PEMParser parser = new PEMParser(new StringReader(state.pkiCertPem))) {
            X509CertificateHolder holder = (X509CertificateHolder) parser.readObject();
            String cn = holder.getSubject()
                    .getRDNs(org.bouncycastle.asn1.x500.style.BCStyle.CN)[0]
                    .getFirst().getValue().toString();
            assertThat(cn).isEqualTo(expectedCn);
        }
    }

    @Then("返回最终证书 PEM 非空")
    public void finalCertNotBlank() {
        assertThat(state.pkiCertPem).isNotBlank();
    }
}
