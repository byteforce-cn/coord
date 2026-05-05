package cn.byteforce.e2e.steps;

import coord.v1.Coord;
import coord.v1.TransitServiceGrpc;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.springframework.beans.factory.annotation.Autowired;

import static org.assertj.core.api.Assertions.assertThat;

public class TransitSteps {

    @Autowired private TransitServiceGrpc.TransitServiceBlockingStub transitStub;
    @Autowired private ScenarioState state;

    // ── Key management ──────────────────────────────────────────────────────────

    @Given("Transit密钥 {string} 已创建 algorithm={string}")
    public void ensureKey(String keyName, String algorithm) {
        try {
            transitStub.createKey(Coord.CreateKeyRequest.newBuilder()
                    .setKeyName(keyName)
                    .setAlgorithm(algorithm)
                    .build());
        } catch (io.grpc.StatusRuntimeException e) {
            // Server may return INVALID_ARGUMENT or ALREADY_EXISTS for duplicate keys
            String msg = e.getStatus().getDescription();
            if (msg == null || !msg.contains("already exists")) throw e;
        }
        state.transitKeyName = keyName;
    }

    @When("创建 Transit 密钥 {string} algorithm={string}")
    public void createKey(String keyName, String algorithm) {
        transitStub.createKey(Coord.CreateKeyRequest.newBuilder()
                .setKeyName(keyName)
                .setAlgorithm(algorithm)
                .build());
        state.transitKeyName = keyName;
    }

    // ── Encrypt / Decrypt ────────────────────────────────────────────────────────

    @When("加密明文 {string}")
    public void encrypt(String plaintext) {
        Coord.EncryptResponse r = transitStub.encrypt(
                Coord.EncryptRequest.newBuilder()
                        .setKeyName(state.transitKeyName)
                        .setPlaintext(plaintext)
                        .build());
        state.transitCiphertext = r.getCiphertext();
        state.transitPlaintext = plaintext;
        assertThat(state.transitCiphertext).isNotBlank();
    }

    @Then("返回密文非空")
    public void verifyCiphertext() {
        assertThat(state.transitCiphertext).isNotBlank();
    }

    @Then("密文不等于明文")
    public void ciphertextNotEqualsPlaintext() {
        assertThat(state.transitCiphertext).isNotEqualTo(state.transitPlaintext);
    }

    @When("解密该密文")
    public void decrypt() {
        Coord.DecryptResponse r = transitStub.decrypt(
                Coord.DecryptRequest.newBuilder()
                        .setKeyName(state.transitKeyName)
                        .setCiphertext(state.transitCiphertext)
                        .build());
        state.transitDecrypted = r.getPlaintext();
    }

    @Then("解密结果等于原明文")
    public void decryptedEqualsOriginal() {
        assertThat(state.transitDecrypted).isEqualTo(state.transitPlaintext);
    }

    // ── HMAC ─────────────────────────────────────────────────────────────────────

    @When("对 {string} 签名")
    public void hmacSign(String message) {
        Coord.HmacSignResponse r = transitStub.hmacSign(
                Coord.HmacSignRequest.newBuilder()
                        .setKeyName(state.transitKeyName)
                        .setInput(message)
                        .build());
        state.transitHmac = r.getHmac();
        state.transitPlaintext = message;
    }

    @Then("返回 HMAC 非空")
    public void verifyHmac() {
        assertThat(state.transitHmac).isNotBlank();
    }

    @When("验证签名")
    public void verifySignature() {
        Coord.HmacVerifyResponse r = transitStub.hmacVerify(
                Coord.HmacVerifyRequest.newBuilder()
                        .setKeyName(state.transitKeyName)
                        .setInput(state.transitPlaintext)
                        .setHmac(state.transitHmac)
                        .build());
        state.transitVerifyValid = r.getValid();
    }

    @Then("验证结果为 true")
    public void verifyResultTrue() {
        assertThat(state.transitVerifyValid).isTrue();
    }

    @When("用错误 HMAC 验证")
    public void verifyBadHmac() {
        // Use a properly-formatted HMAC (v<version>:<base64>) with wrong content
        Coord.HmacVerifyResponse r = transitStub.hmacVerify(
                Coord.HmacVerifyRequest.newBuilder()
                        .setKeyName(state.transitKeyName)
                        .setInput(state.transitPlaintext)
                        .setHmac("v1:aW52YWxpZC1obWFj")
                        .build());
        state.transitVerifyValid = r.getValid();
    }

    @Then("验证结果为 false")
    public void verifyResultFalse() {
        assertThat(state.transitVerifyValid).isFalse();
    }

    // ── Key rotation ──────────────────────────────────────────────────────────────

    @When("轮换密钥 {string}")
    public void rotateKey(String keyName) {
        transitStub.rotateKey(Coord.RotateKeyRequest.newBuilder()
                .setKeyName(keyName).build());
    }

    @Then("旧密文仍可解密")
    public void oldCiphertextDecryptable() {
        Coord.DecryptResponse r = transitStub.decrypt(
                Coord.DecryptRequest.newBuilder()
                        .setKeyName(state.transitKeyName)
                        .setCiphertext(state.transitCiphertext)
                        .build());
        assertThat(r.getPlaintext()).isEqualTo(state.transitPlaintext);
    }

    @When("用新密钥加密 {string}")
    public void encryptWithNew(String plaintext) {
        state.transitCiphertextAfterRotation = transitStub.encrypt(
                Coord.EncryptRequest.newBuilder()
                        .setKeyName(state.transitKeyName)
                        .setPlaintext(plaintext)
                        .build()).getCiphertext();
    }

    @Then("新密文和旧密文不同")
    public void ciphertextsDiffer() {
        assertThat(state.transitCiphertextAfterRotation).isNotEqualTo(state.transitCiphertext);
    }

    // ── Key info ──────────────────────────────────────────────────────────────────

    @When("查询密钥信息 {string}")
    public void getKeyInfo(String keyName) {
        state.transitKeyInfo = transitStub.getTransitKey(
                Coord.GetTransitKeyRequest.newBuilder().setKeyName(keyName).build());
    }

    @Then("key_name={string}")
    public void verifyKeyName(String expected) {
        assertThat(state.transitKeyInfo.getKeyName()).isEqualTo(expected);
    }

    @Then("algorithm={string}")
    public void verifyAlgorithm(String expected) {
        assertThat(state.transitKeyInfo.getAlgorithm()).isEqualTo(expected);
    }
}
