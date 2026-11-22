// TDD: Agent 端 PKI 服务测试（ADR-0019 D6：私钥留库）
//
// PKI — CA 与 leaf 私钥都由 transit 命名密钥档案托管（KEK 包裹）；
// 证书记录只保存 `key_name` / `key_version`，不保存任何 PEM 私钥。
//
// 覆盖验收标准：
// 1. 同一 CN 连续 issueCert → 相同 serial / 证书
// 2. agent 重启（store + 密钥档案保留）→ issueCert 仍返回同一证书；CA 不重建
// 3. 两 agent 并发首次 issueCert(同 CN) → 仅一份证书（CAS 单写）
// 4. 多 agent：A 签发的证书可在 B 验签（同一 CA 根）
// 5. renewCert(serial) → 返回原 CN、新 serial，旧证书保留可验签
// 6. rotateCert(CN) 后 listCerts(CN) → active + retired 两份
// 7. 到期 = 换新触发；GetCertByCN 对过期记录返回 None（G-PKI-1）
// 8. 私钥不出域：PKI store 记录不含 PEM；CA 密钥 ExportKey 恒拒绝；
//    leaf 密钥经 transit Sign 远程签名，签名能被证书公钥验证。

use std::sync::Arc;

use coord_agent::pki::{CertInfo, PkiConfig, PkiError, PkiService};
use coord_agent::pki_store::{CertUsage, MemoryPkiStore, PkiStore};
use coord_agent::{KeyType, SignSpec, TransitKeyRegistry, VerifySpec};

fn new_keys() -> Arc<TransitKeyRegistry> {
    Arc::new(TransitKeyRegistry::in_memory().expect("in-memory key registry"))
}

/// 同一集群的多个 PKI 实例必须共享密钥档案（生产是 KvTransitKeyStore）
fn make_pki_with_store(store: Arc<MemoryPkiStore>, keys: &Arc<TransitKeyRegistry>) -> PkiService {
    let store: Arc<dyn PkiStore> = store;
    PkiService::with_store_and_keys(PkiConfig::default(), store, keys.clone())
}

fn cert_public_key(cert_pem: &str) -> Vec<u8> {
    let der_b64: String = cert_pem
        .lines()
        .filter(|line| !line.starts_with("-----"))
        .collect();
    use base64::Engine as _;
    let der = base64::engine::general_purpose::STANDARD
        .decode(der_b64)
        .expect("certificate base64");
    let (_, cert) = x509_parser::parse_x509_certificate(&der).expect("parse certificate");
    cert.public_key().subject_public_key.data.to_vec()
}

/// 生成 CSR（DER）：CN + DNS/IP SAN；私钥由调用方持有。
fn csr_der(
    common_name: &str,
    dns_names: &[&str],
    ip_addresses: &[&str],
    key: &rcgen::KeyPair,
) -> Vec<u8> {
    use rcgen::{CertificateParams, DnType, SanType};

    let mut params = CertificateParams::default();
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    params.subject_alt_names = dns_names
        .iter()
        .map(|dns| SanType::DnsName((*dns).try_into().expect("dns ia5")))
        .chain(
            ip_addresses
                .iter()
                .map(|ip| SanType::IpAddress(ip.parse().expect("ip san"))),
        )
        .collect();
    params
        .serialize_request(key)
        .expect("serialize CSR")
        .der()
        .to_vec()
}

fn cert_der(cert_pem: &str) -> Vec<u8> {
    let der_b64: String = cert_pem
        .lines()
        .filter(|line| !line.starts_with("-----"))
        .collect();
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(der_b64)
        .expect("certificate base64")
}

/// 验证 PKI 配置默认值
#[test]
fn test_pki_config_defaults() {
    let config = PkiConfig::default();
    assert_eq!(config.cert_ttl_hours, 24, "证书默认 24h TTL");
    assert_eq!(
        config.expiry_warn_hours, 6,
        "到期告警窗口默认 6h（G-PKI-2）"
    );
    assert_eq!(
        config.crl_ttl_hours, 24,
        "CRL next_update 窗口默认 24h（ADR-0020）"
    );
    assert_eq!(
        config.history_retention_hours, 24,
        "Tidy 证书历史保留期默认 24h"
    );
    assert_eq!(config.ca_cert_path, None);
}

/// 验证 PKI 配置从 TOML 反序列化
#[test]
fn test_pki_config_from_toml() {
    let toml_str = r#"
cert_ttl_hours = 48
ca_cert_path = "/etc/coord-agent/ca.crt"
ca_key_path = "/etc/coord-agent/ca.key"
"#;
    let config: PkiConfig = toml::from_str(toml_str).expect("TOML 解析失败");
    assert_eq!(config.cert_ttl_hours, 48);
    assert_eq!(config.ca_cert_path, Some("/etc/coord-agent/ca.crt".into()));
    assert_eq!(config.ca_key_path, Some("/etc/coord-agent/ca.key".into()));
}

/// 验证 PkiService 能初始化 CA 并签发证书（响应只带 key 引用）
#[tokio::test]
async fn test_pki_service_init_and_issue() {
    let keys = new_keys();
    let pki = make_pki_with_store(Arc::new(MemoryPkiStore::new()), &keys);

    pki.init_ca("Coord Test CA").await.expect("初始化 CA 失败");

    let cert = pki
        .issue_cert("agent-001.coord.local", 0)
        .await
        .expect("签发证书失败");

    assert_eq!(cert.common_name, "agent-001.coord.local");
    assert!(!cert.cert_pem.is_empty(), "证书 PEM 不应为空");
    assert_eq!(cert.key_name, "pki/leaf/agent-001.coord.local");
    assert_eq!(cert.key_version, 1);
    assert!(cert.not_after > cert.not_before, "有效期应合法");
}

/// 验证签发的证书可被 CA 验证
#[tokio::test]
async fn test_pki_service_cert_chain_validation() {
    let keys = new_keys();
    let pki = make_pki_with_store(Arc::new(MemoryPkiStore::new()), &keys);
    pki.init_ca("Coord Chain CA").await.expect("初始化 CA 失败");

    let cert = pki
        .issue_cert("test.coord.local", 0)
        .await
        .expect("签发证书失败");

    let valid = pki.verify_cert(&cert.cert_pem).await.expect("验证失败");
    assert!(valid, "CA 应能验证自己签发的证书");
}

/// 与真实 CA 同 issuer CN、但由另一把私钥签出的叶子证书。
///
/// `VerifyCert` 只比较 issuer CN 时这条会被误判为有效；验签实现必须以
/// CA 公钥验证签名后才返回 true。
fn forged_cert_with_issuer_cn(issuer_cn: &str) -> String {
    use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, KeyUsagePurpose};

    let now = time::OffsetDateTime::now_utc();
    let forged_ca_key =
        KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("forged CA key");
    let mut ca_params = CertificateParams::default();
    ca_params
        .distinguished_name
        .push(DnType::CommonName, issuer_cn);
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
    ca_params.not_before = now - time::Duration::hours(1);
    ca_params.not_after = now + time::Duration::hours(1);
    let forged_ca = ca_params
        .self_signed(&forged_ca_key)
        .expect("forged CA cert");
    let issuer = rcgen::Issuer::from_ca_cert_pem(&forged_ca.pem(), forged_ca_key).expect("issuer");

    let leaf_key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("leaf key");
    let mut leaf_params = CertificateParams::default();
    leaf_params
        .distinguished_name
        .push(DnType::CommonName, "forged.coord.local");
    leaf_params.not_before = now - time::Duration::hours(1);
    leaf_params.not_after = now + time::Duration::hours(1);
    leaf_params
        .signed_by(&leaf_key, &issuer)
        .expect("forged leaf cert")
        .pem()
}

/// `VerifyCert` 必须用 CA 公钥验签：同 issuer CN 的伪造证书 ⇒ false。
#[tokio::test]
async fn test_pki_service_verify_rejects_forged_issuer_cn() {
    let keys = new_keys();
    let pki = make_pki_with_store(Arc::new(MemoryPkiStore::new()), &keys);
    pki.init_ca("Coord Chain CA").await.expect("初始化 CA 失败");

    let forged = forged_cert_with_issuer_cn("Coord Chain CA");
    assert!(
        !pki.verify_cert(&forged).await.expect("验证失败"),
        "issuer CN 相同但签名不是本 CA 的证书必须被拒绝"
    );
}

/// 非法 PEM 是调用方参数错误，不能与「验签不过」混成 false。
#[tokio::test]
async fn test_pki_service_verify_rejects_malformed_pem() {
    let keys = new_keys();
    let pki = make_pki_with_store(Arc::new(MemoryPkiStore::new()), &keys);
    pki.init_ca("Coord Chain CA").await.expect("初始化 CA 失败");

    let err = pki
        .verify_cert("not a certificate")
        .await
        .expect_err("非法 PEM 必须返回错误");
    assert!(matches!(err, PkiError::CertParse(_)), "unexpected: {err}");
}

/// 验收 1: 同一 CN 连续 issueCert → 相同 serial / 相同证书
#[tokio::test]
async fn test_issue_cert_idempotent_get_or_create() {
    let store = Arc::new(MemoryPkiStore::new());
    let keys = new_keys();
    let pki = make_pki_with_store(store.clone(), &keys);
    pki.init_ca("Idem CA").await.expect("初始化 CA 失败");

    let cert1 = pki
        .issue_cert("svc-a.coord.local", 0)
        .await
        .expect("首次签发");
    let cert2 = pki
        .issue_cert("svc-a.coord.local", 0)
        .await
        .expect("再次签发");

    assert_eq!(cert1.serial, cert2.serial, "同一 CN 必须返回相同 serial");
    assert_eq!(cert1.cert_pem, cert2.cert_pem, "同一 CN 必须返回相同证书");
    assert_eq!(cert1.key_name, cert2.key_name);
    assert_eq!(cert1.key_version, cert2.key_version);
    assert_eq!(
        cert1.not_after, cert2.not_after,
        "同一 CN 必须返回相同有效期"
    );
}

/// 验收 2: agent 重启（store + 密钥档案保留）→ issueCert 仍返回同一证书；CA 不重建
#[tokio::test]
async fn test_issue_cert_after_restart_returns_same_cert() {
    let store = Arc::new(MemoryPkiStore::new());
    let keys = new_keys();

    let pki_a = make_pki_with_store(store.clone(), &keys);
    pki_a.init_ca("Restart CA").await.expect("初始化 CA 失败");
    let ca_a = pki_a.ca_cert_pem().expect("导出 CA");
    let cert_a = pki_a
        .issue_cert("svc-restart.coord.local", 0)
        .await
        .expect("签发");

    // 模拟重启：新建 PkiService（同一 store + 同一密钥档案），重新 init_ca
    let pki_b = make_pki_with_store(store.clone(), &keys);
    pki_b
        .init_ca("Restart CA")
        .await
        .expect("重启后初始化 CA 失败");

    let ca_b = pki_b.ca_cert_pem().expect("重启后导出 CA");
    assert_eq!(ca_a, ca_b, "CA 根证书不得重建");

    let cert_b = pki_b
        .issue_cert("svc-restart.coord.local", 0)
        .await
        .expect("重启后签发");
    assert_eq!(cert_a.serial, cert_b.serial, "重启后必须取回同一证书");
    assert_eq!(
        cert_public_key(&cert_a.cert_pem),
        cert_public_key(&cert_b.cert_pem),
        "重启后必须取回同一公钥"
    );
}

/// 验收 3: 两个 agent 并发首次 issueCert(同 CN) → 仅一份证书（Txn CAS 单写）
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_first_issue_single_key() {
    let store = Arc::new(MemoryPkiStore::new());
    let keys = new_keys();

    let pki_a = make_pki_with_store(store.clone(), &keys);
    let pki_b = make_pki_with_store(store.clone(), &keys);
    pki_a
        .init_ca("Concurrent CA")
        .await
        .expect("A 初始化 CA 失败");
    pki_b
        .init_ca("Concurrent CA")
        .await
        .expect("B 初始化 CA 失败");

    let a = tokio::spawn(async move {
        pki_a
            .issue_cert("svc-concurrent.coord.local", 0)
            .await
            .expect("A 签发失败")
    });
    let b = tokio::spawn(async move {
        pki_b
            .issue_cert("svc-concurrent.coord.local", 0)
            .await
            .expect("B 签发失败")
    });

    let cert_a = a.await.expect("join A");
    let cert_b = b.await.expect("join B");

    assert_eq!(cert_a.serial, cert_b.serial, "并发首次签发必须只有一份证书");
    assert_eq!(
        cert_public_key(&cert_a.cert_pem),
        cert_public_key(&cert_b.cert_pem)
    );

    let stored = store
        .get_cert("svc-concurrent.coord.local")
        .await
        .expect("读取 store");
    let stored = stored.expect("store 应有 active 记录");
    assert_eq!(stored.serial, cert_a.serial);
}

/// 验收 4: 多 agent：A 签发的证书可在 B 验签（同一 CA 根）
#[tokio::test]
async fn test_cross_agent_verify_same_ca() {
    let store = Arc::new(MemoryPkiStore::new());
    let keys = new_keys();

    let pki_a = make_pki_with_store(store.clone(), &keys);
    let pki_b = make_pki_with_store(store.clone(), &keys);
    pki_a.init_ca("Shared CA").await.expect("A 初始化 CA 失败");
    pki_b.init_ca("Shared CA").await.expect("B 初始化 CA 失败");

    let cert = pki_a
        .issue_cert("svc-a.coord.local", 0)
        .await
        .expect("A 签发");
    assert!(
        pki_b.verify_cert(&cert.cert_pem).await.expect("B 验签"),
        "A 签发的证书必须在 B 下可验签（同一 CA 根）"
    );
}

/// 验收 5: renewCert(serial) → 返回原 CN、新 serial，旧证书保留至 not_after 可验签
#[tokio::test]
async fn test_renew_cert_returns_original_cn() {
    let store = Arc::new(MemoryPkiStore::new());
    let keys = new_keys();
    let pki = make_pki_with_store(store.clone(), &keys);
    pki.init_ca("Renew CA").await.expect("初始化 CA 失败");

    let old = pki
        .issue_cert("svc-renew.coord.local", 0)
        .await
        .expect("首次签发");
    let renewed = pki.renew_cert(&old.serial, 0).await.expect("续期失败");

    assert_eq!(
        renewed.common_name, "svc-renew.coord.local",
        "renew 必须返回原 CN"
    );
    assert_ne!(renewed.serial, old.serial, "新证书必须换新 serial");
    assert_ne!(
        cert_public_key(&renewed.cert_pem),
        cert_public_key(&old.cert_pem),
        "新证书必须换新密钥"
    );
    assert_eq!(
        renewed.key_version,
        old.key_version + 1,
        "leaf 密钥版本随轮换代次递增"
    );

    assert!(
        pki.verify_cert(&old.cert_pem).await.expect("验签旧证书"),
        "旧证书保留期内应可验签"
    );
    assert!(pki
        .verify_cert(&renewed.cert_pem)
        .await
        .expect("验签新证书"));
}

/// 验收 6: rotateCert(CN) 后 listCerts(CN) → active + retired 两份（多 kid 验签）
#[tokio::test]
async fn test_rotate_then_list_returns_active_and_retired() {
    let store = Arc::new(MemoryPkiStore::new());
    let keys = new_keys();
    let pki = make_pki_with_store(store.clone(), &keys);
    pki.init_ca("Rotate CA").await.expect("初始化 CA 失败");

    let first = pki
        .issue_cert("svc-rotate.coord.local", 0)
        .await
        .expect("首次签发");
    let rotated = pki
        .rotate_cert("svc-rotate.coord.local", 0)
        .await
        .expect("轮换失败");

    assert_ne!(rotated.serial, first.serial);
    assert_eq!(rotated.status, coord_agent::pki_store::CertStatus::Active);

    let list = pki
        .list_certs("svc-rotate.coord.local")
        .await
        .expect("列表失败");
    assert_eq!(list.len(), 2, "rotate 后应返回 active + retired 两份");
    let serials: Vec<&str> = list.iter().map(|c| c.serial.as_str()).collect();
    assert!(
        serials.contains(&first.serial.as_str()),
        "旧证书应保留在列表"
    );
    assert!(serials.contains(&rotated.serial.as_str()), "新证书应在列表");

    assert!(pki.verify_cert(&first.cert_pem).await.expect("旧证书可验"));
    assert!(pki
        .verify_cert(&rotated.cert_pem)
        .await
        .expect("新证书可验"));

    let again = pki
        .issue_cert("svc-rotate.coord.local", 0)
        .await
        .expect("取回失败");
    assert_eq!(
        again.serial, rotated.serial,
        "rotate 后 issue 应返回 active 证书"
    );
}

/// get_cert_by_cn 只返回证书与 key 引用（不含材料）
#[tokio::test]
async fn test_get_cert_by_cn_returns_key_reference_only() {
    let store = Arc::new(MemoryPkiStore::new());
    let keys = new_keys();
    let pki = make_pki_with_store(store.clone(), &keys);
    pki.init_ca("GetCN CA").await.expect("初始化 CA 失败");

    assert!(pki
        .get_cert_by_cn("missing.coord.local")
        .await
        .expect("读取")
        .is_none());

    let issued = pki
        .issue_cert("svc-getcn.coord.local", 0)
        .await
        .expect("签发");
    let got = pki
        .get_cert_by_cn("svc-getcn.coord.local")
        .await
        .expect("取回")
        .expect("应有证书");
    assert_eq!(got.serial, issued.serial);
    assert_eq!(got.key_name, "pki/leaf/svc-getcn.coord.local");
    assert_eq!(got.key_version, issued.key_version);
}

/// 验收 8a: PKI store 的记录里没有任何私钥 PEM（CA 与 leaf 都是）
#[tokio::test]
async fn test_private_keys_are_not_stored_in_pki_records() {
    let store = Arc::new(MemoryPkiStore::new());
    let keys = new_keys();
    let pki = make_pki_with_store(store.clone(), &keys);
    pki.init_ca("Custody CA").await.expect("初始化 CA 失败");
    pki.issue_cert("svc-custody.coord.local", 0)
        .await
        .expect("签发");

    let ca = store.get_ca().await.expect("ca").expect("CA record");
    assert_eq!(ca.key_name, "pki/ca");
    assert!(!serde_json::to_string(&ca)
        .expect("serialize ca")
        .contains("PRIVATE KEY"));

    let cert = store
        .get_cert("svc-custody.coord.local")
        .await
        .expect("cert")
        .expect("cert record");
    let encoded = serde_json::to_string(&cert).expect("serialize cert");
    assert!(
        !encoded.contains("PRIVATE KEY"),
        "CertRecord 不得含私钥 PEM"
    );
    assert_eq!(cert.key_name, "pki/leaf/svc-custody.coord.local");
}

/// 验收 8b: CA key 恒不可导出；leaf key 只能经 ExportKey（exportable）导出
#[tokio::test]
async fn test_ca_key_is_never_exportable() {
    let store = Arc::new(MemoryPkiStore::new());
    let keys = new_keys();
    let pki = make_pki_with_store(store.clone(), &keys);
    pki.init_ca("NoExport CA").await.expect("初始化 CA 失败");

    let err = keys.export_key("pki/ca", 0).await.unwrap_err();
    assert_eq!(err.code(), "PERMISSION_DENIED");
}

/// 验收 8c: leaf 私钥留在 coord —— 用 transit Sign 远程签名，证书公钥可验
#[tokio::test]
async fn test_leaf_key_signs_remotely_and_cert_verifies() {
    use p256::ecdsa::{signature::Verifier as _, Signature, VerifyingKey};

    let store = Arc::new(MemoryPkiStore::new());
    let keys = new_keys();
    let pki = make_pki_with_store(store.clone(), &keys);
    pki.init_ca("Remote Sign CA").await.expect("初始化 CA 失败");

    let cert = pki
        .issue_cert("svc-remote.coord.local", 0)
        .await
        .expect("签发");
    assert_eq!(cert.key_version, 1);

    let message = b"tls CertificateVerify payload";
    let signed = keys
        .sign(SignSpec {
            name: cert.key_name.clone(),
            version: cert.key_version,
            input: message.to_vec(),
            prehashed: false,
            hash_algorithm: None,
            signature_algorithm: None,
        })
        .await
        .expect("remote sign");
    assert_eq!(signed.key_version, cert.key_version);

    let point = cert_public_key(&cert.cert_pem);
    let verifying = VerifyingKey::from_sec1_bytes(&point).expect("P-256 public key");
    let signature = Signature::from_der(&signed.signature).expect("DER signature");
    verifying
        .verify(message, &signature)
        .expect("签名必须能被证书公钥验证");

    // 同一把 key 的 verify 也必须过（服务端路径）
    assert!(
        keys.verify(VerifySpec {
            name: cert.key_name.clone(),
            version: cert.key_version,
            input: message.to_vec(),
            signature: signed.signature,
            prehashed: false,
            hash_algorithm: None,
            signature_algorithm: None,
        })
        .await
        .expect("server verify")
        .valid
    );

    // 类型一致性：leaf key 是 ECDSA P-256
    assert_eq!(
        keys.read_key(&cert.key_name)
            .await
            .expect("read key")
            .key_type,
        KeyType::EcdsaP256.canonical()
    );
}

/// 验证按 serial 续期不存在时返回 CertNotFound
#[tokio::test]
async fn test_renew_unknown_serial_returns_not_found() {
    let store = Arc::new(MemoryPkiStore::new());
    let keys = new_keys();
    let pki = make_pki_with_store(store.clone(), &keys);
    pki.init_ca("NotFound CA").await.expect("初始化 CA 失败");

    let result = pki.renew_cert("deadbeef", 0).await;
    assert!(result.is_err(), "未知 serial 必须报错");
    assert!(
        matches!(
            result.unwrap_err(),
            coord_agent::pki::PkiError::CertNotFound(_)
        ),
        "应返回 CertNotFound"
    );
}

/// 按 serial 只读查询：active / retired 都可查，未知 ⇒ None（不含私钥材料）。
#[tokio::test]
async fn test_get_cert_by_serial_active_retired_and_unknown() {
    use coord_agent::pki_store::CertStatus;

    let keys = new_keys();
    let pki = make_pki_with_store(Arc::new(MemoryPkiStore::new()), &keys);
    pki.init_ca("Serial Lookup CA")
        .await
        .expect("初始化 CA 失败");
    let cn = "svc-serial.coord.local";

    let active = pki.issue_cert(cn, 0).await.expect("issue");
    let found = pki
        .get_cert_by_serial(&active.serial)
        .await
        .expect("query")
        .expect("active record");
    assert_eq!(found.serial, active.serial);
    assert_eq!(found.common_name, cn);
    assert_eq!(found.status, CertStatus::Active);
    assert_eq!(found.cert_pem, active.cert_pem);

    let rotated = pki.rotate_cert(cn, 0).await.expect("rotate");
    let old = pki
        .get_cert_by_serial(&active.serial)
        .await
        .expect("query")
        .expect("retired record");
    assert_eq!(old.status, CertStatus::Retired);
    assert_eq!(old.cert_pem, active.cert_pem);
    assert_ne!(rotated.serial, active.serial);

    assert!(
        pki.get_cert_by_serial("deadbeef")
            .await
            .expect("query")
            .is_none(),
        "未知 serial 必须返回 None"
    );
}

/// gRPC 响应：过期记录按 not_after 报 `expired`；未知 serial ⇒ NOT_FOUND。
#[tokio::test]
async fn test_get_cert_by_serial_rpc_reports_expired_and_not_found() {
    use coord_agent::pki_store::{CertRecord, CertStatus};
    use coord_proto::pki::v1::pki_server::Pki as PkiRpc;
    use coord_proto::pki::v1::PkiGetCertBySerialRequest;

    let store = Arc::new(MemoryPkiStore::new());
    let keys = new_keys();
    let pki = make_pki_with_store(store.clone(), &keys);
    pki.init_ca("Serial RPC CA").await.expect("初始化 CA 失败");

    let cn = "svc-serial-expired.coord.local";
    let expired = CertRecord {
        common_name: cn.into(),
        cert_pem: "-----BEGIN CERTIFICATE-----\nEXPIRED\n-----END CERTIFICATE-----".into(),
        key_name: format!("pki/leaf/{cn}"),
        key_version: 1,
        not_before: 1_700_000_000,
        not_after: 1_700_000_001,
        serial: "0e11ed5e71a1".into(),
        status: CertStatus::Active,
        parent_serial: None,
        dns_names: Vec::new(),
        ip_addresses: Vec::new(),
        spki_sha256: String::new(),
        usage: coord_agent::pki_store::CertUsage::Both,
    };
    store.create_cert(cn, &expired).await.expect("seed expired");

    let response = PkiRpc::get_cert_by_serial(
        &pki,
        tonic::Request::new(PkiGetCertBySerialRequest {
            serial: expired.serial.clone(),
        }),
    )
    .await
    .expect("GetCertBySerial")
    .into_inner();
    assert_eq!(response.common_name, cn);
    assert_eq!(response.serial, expired.serial);
    assert_eq!(response.cert_pem, expired.cert_pem);
    assert_eq!(response.status, "expired");

    let err = PkiRpc::get_cert_by_serial(
        &pki,
        tonic::Request::new(PkiGetCertBySerialRequest {
            serial: "deadbeef".into(),
        }),
    )
    .await
    .expect_err("unknown serial");
    assert_eq!(err.code(), tonic::Code::NotFound, "{err:?}");
}

/// 反复签发直到拿到首 nibble 为 `0` 的 serial（概率 12.5%）——
/// coord 签出的 serial 恒为定长 32 hex，但首个 nibble 可以是 `0`。
async fn issue_with_leading_zero_serial(pki: &PkiService) -> CertInfo {
    for attempt in 0..200 {
        let cn = format!("svc-serial-{attempt}.coord.local");
        let cert = pki.issue_cert(&cn, 0).await.expect("issue");
        if cert.serial.starts_with('0') {
            assert_eq!(cert.serial.len(), 32, "coord 签发 serial 恒为定长 32 hex");
            return cert;
        }
    }
    panic!("200 次签发未出现首 nibble 为 0 的 serial（(7/8)^200 ≈ 3e-12）");
}

/// serial 入参按 X.509 INTEGER 数值口径匹配：忽略大小写与前导零，
/// `GetCertBySerial` / `GetCertStatus` / `RevokeCert` / `RenewCert` 四个 RPC 同一判据。
#[tokio::test]
async fn test_serial_lookup_ignores_leading_zeros_across_rpcs() {
    use coord_proto::pki::v1::pki_server::Pki as PkiRpc;
    use coord_proto::pki::v1::{
        PkiGetCertBySerialRequest, PkiGetCertStatusRequest, PkiRenewCertRequest,
        PkiRevocationReason, PkiRevokeCertRequest,
    };

    let keys = new_keys();
    let pki = make_pki_with_store(Arc::new(MemoryPkiStore::new()), &keys);
    pki.init_ca("Serial Spelling CA")
        .await
        .expect("初始化 CA 失败");

    let cert = issue_with_leading_zero_serial(&pki).await;
    let padded = cert.serial.clone();
    let unpadded = padded.trim_start_matches('0').to_string();
    assert_eq!(unpadded.len(), padded.len() - 1);

    let by_serial = PkiRpc::get_cert_by_serial(
        &pki,
        tonic::Request::new(PkiGetCertBySerialRequest {
            serial: format!(" {} ", unpadded.to_uppercase()),
        }),
    )
    .await
    .expect("GetCertBySerial(去零 + 大写 + 空白)")
    .into_inner();
    assert_eq!(by_serial.serial, padded, "回显仍是定长 32 hex");
    assert_eq!(by_serial.common_name, cert.common_name);

    let status = PkiRpc::get_cert_status(
        &pki,
        tonic::Request::new(PkiGetCertStatusRequest {
            serial: unpadded.clone(),
        }),
    )
    .await
    .expect("GetCertStatus(去零)")
    .into_inner();
    assert_eq!(status.status, "active");

    // RenewCert(去零写法) 命中同一记录：新证书，旧记录转 retired
    let renewed = pki.renew_cert(&unpadded, 0).await.expect("RenewCert(去零)");
    assert_eq!(renewed.common_name, cert.common_name);
    assert_ne!(renewed.serial, padded);

    let revoked = PkiRpc::revoke_cert(
        &pki,
        tonic::Request::new(PkiRevokeCertRequest {
            serial: unpadded.clone(),
            reason: PkiRevocationReason::Unspecified as i32,
        }),
    )
    .await
    .expect("RevokeCert(去零)")
    .into_inner();
    assert_eq!(revoked.cert_serial, padded, "吊销账本键仍是定长口径");
    assert_eq!(revoked.status, "revoked");

    let status = PkiRpc::get_cert_status(
        &pki,
        tonic::Request::new(PkiGetCertStatusRequest {
            serial: unpadded.clone(),
        }),
    )
    .await
    .expect("GetCertStatus(吊销后)")
    .into_inner();
    assert_eq!(status.status, "revoked");

    // 已吊销 + 去零写法：必须明确 FAILED_PRECONDITION，而不是静默 NOT_FOUND
    let err = PkiRpc::renew_cert(
        &pki,
        tonic::Request::new(PkiRenewCertRequest {
            serial_number: unpadded,
            ttl_seconds: 0,
        }),
    )
    .await
    .expect_err("renew revoked");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition, "{err:?}");

    // 非 hex ⇒ INVALID_ARGUMENT（不是「不存在」）
    let err = PkiRpc::get_cert_by_serial(
        &pki,
        tonic::Request::new(PkiGetCertBySerialRequest {
            serial: "not-hex".into(),
        }),
    )
    .await
    .expect_err("non-hex serial");
    assert_eq!(err.code(), tonic::Code::InvalidArgument, "{err:?}");
}

/// CSR 签发：私钥留在调用方；记录只有公钥指纹；usage=SERVER 的 KU/EKU 精确。
#[tokio::test]
async fn test_sign_csr_external_key_and_server_profile() {
    let store = Arc::new(MemoryPkiStore::new());
    let keys = new_keys();
    let pki = make_pki_with_store(store.clone(), &keys);
    pki.init_ca("CSR CA").await.expect("初始化 CA 失败");

    let leaf_key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("leaf key");
    let csr = csr_der(
        "svc-csr.coord.local",
        &["svc-csr.coord.local"],
        &[],
        &leaf_key,
    );
    let cert = pki
        .sign_csr(&csr, 0, CertUsage::Server)
        .await
        .expect("sign CSR");

    assert_eq!(cert.common_name, "svc-csr.coord.local");
    assert!(cert.key_name.is_empty(), "外部密钥不得有 coord key 引用");
    assert_eq!(cert.key_version, 0);
    assert_eq!(cert.usage, CertUsage::Server);
    assert!(cert.cert_pem.contains("BEGIN CERTIFICATE"));

    let record = store
        .get_cert("svc-csr.coord.local")
        .await
        .expect("read record")
        .expect("active record");
    assert!(record.is_external());
    assert!(!record.spki_sha256.is_empty(), "必须记录公钥指纹");
    assert_eq!(record.usage, CertUsage::Server);
    assert!(
        record.cert_pem.contains("BEGIN CERTIFICATE"),
        "store 只存证书，不存私钥"
    );

    let der = cert_der(&cert.cert_pem);
    let (_, parsed) = x509_parser::parse_x509_certificate(&der).expect("parse cert");
    let ku = parsed.key_usage().expect("ku extension").expect("ku value");
    assert!(ku.value.digital_signature());
    assert!(ku.value.key_encipherment());
    let eku = parsed
        .extended_key_usage()
        .expect("eku extension")
        .expect("eku value");
    assert!(eku.value.server_auth, "SERVER 必须含 serverAuth");
    assert!(!eku.value.client_auth, "SERVER 不得含 clientAuth");

    let ca_der = cert_der(&pki.ca_cert_pem().expect("ca pem"));
    let (_, ca_cert) = x509_parser::parse_x509_certificate(&ca_der).expect("parse CA");
    parsed
        .verify_signature(Some(ca_cert.public_key()))
        .expect("CSR cert must carry a real CA signature");
}

/// usage=CLIENT ⇒ KU 仅 digitalSignature、EKU 仅 clientAuth。
#[tokio::test]
async fn test_sign_csr_client_profile_is_exact() {
    let keys = new_keys();
    let pki = make_pki_with_store(Arc::new(MemoryPkiStore::new()), &keys);
    pki.init_ca("CSR Client CA").await.expect("初始化 CA 失败");

    let leaf_key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("leaf key");
    let csr = csr_der("svc-client.coord.local", &[], &["10.1.2.3"], &leaf_key);
    let cert = pki
        .sign_csr(&csr, 0, CertUsage::Client)
        .await
        .expect("sign CSR");
    let der = cert_der(&cert.cert_pem);
    let (_, parsed) = x509_parser::parse_x509_certificate(&der).expect("parse cert");
    let ku = parsed.key_usage().expect("ku").expect("ku value");
    assert!(ku.value.digital_signature());
    assert!(
        !ku.value.key_encipherment(),
        "CLIENT 不得含 keyEncipherment"
    );
    let eku = parsed
        .extended_key_usage()
        .expect("eku")
        .expect("eku value");
    assert!(eku.value.client_auth);
    assert!(!eku.value.server_auth);
}

/// PoP 失败（CSR 签名被篡改）⇒ INVALID_ARGUMENT，不签发。
#[tokio::test]
async fn test_sign_csr_rejects_invalid_proof_of_possession() {
    let keys = new_keys();
    let pki = make_pki_with_store(Arc::new(MemoryPkiStore::new()), &keys);
    pki.init_ca("CSR PoP CA").await.expect("初始化 CA 失败");

    let leaf_key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("leaf key");
    let mut csr = csr_der(
        "svc-pop.coord.local",
        &["svc-pop.coord.local"],
        &[],
        &leaf_key,
    );
    let last = csr.len() - 1;
    csr[last] ^= 0xff;

    let err = pki
        .sign_csr(&csr, 0, CertUsage::Both)
        .await
        .expect_err("tampered CSR must be rejected");
    assert!(
        matches!(err, PkiError::InvalidArgument(_)),
        "unexpected: {err}"
    );
}

/// 同一 CSR 幂等；同 CN 新公钥 ⇒ 新 serial，旧证书 retired。
#[tokio::test]
async fn test_sign_csr_idempotent_and_new_key_rotates() {
    use coord_agent::pki_store::CertStatus;

    let keys = new_keys();
    let pki = make_pki_with_store(Arc::new(MemoryPkiStore::new()), &keys);
    pki.init_ca("CSR Idem CA").await.expect("初始化 CA 失败");

    let key1 = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("key1");
    let csr1 = csr_der(
        "svc-idem.coord.local",
        &["svc-idem.coord.local"],
        &[],
        &key1,
    );
    let first = pki
        .sign_csr(&csr1, 0, CertUsage::Both)
        .await
        .expect("first");
    let again = pki
        .sign_csr(&csr1, 0, CertUsage::Both)
        .await
        .expect("idempotent");
    assert_eq!(first.serial, again.serial);

    let key2 = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("key2");
    let csr2 = csr_der(
        "svc-idem.coord.local",
        &["svc-idem.coord.local"],
        &[],
        &key2,
    );
    let rotated = pki
        .sign_csr(&csr2, 0, CertUsage::Both)
        .await
        .expect("new key");
    assert_ne!(rotated.serial, first.serial);
    let old = pki
        .get_cert_by_serial(&first.serial)
        .await
        .expect("query")
        .expect("old");
    assert_eq!(old.status, CertStatus::Retired);
}

/// 外部密钥证书没有 coord 侧私钥：Renew/Rotate 必须要求新 CSR。
#[tokio::test]
async fn test_external_cert_renew_and_rotate_require_csr() {
    let keys = new_keys();
    let pki = make_pki_with_store(Arc::new(MemoryPkiStore::new()), &keys);
    pki.init_ca("CSR Renew CA").await.expect("初始化 CA 失败");

    let leaf_key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("leaf key");
    let csr = csr_der(
        "svc-external.coord.local",
        &["svc-external.coord.local"],
        &[],
        &leaf_key,
    );
    let cert = pki
        .sign_csr(&csr, 0, CertUsage::Both)
        .await
        .expect("sign CSR");

    let err = pki
        .renew_cert(&cert.serial, 0)
        .await
        .expect_err("renew external must fail");
    assert!(
        matches!(err, PkiError::FailedPrecondition(_)),
        "unexpected: {err}"
    );

    let err = pki
        .rotate_cert("svc-external.coord.local", 0)
        .await
        .expect_err("rotate external must fail");
    assert!(
        matches!(err, PkiError::FailedPrecondition(_)),
        "unexpected: {err}"
    );
}

/// managed IssueCert 的 usage 精确生效；Rotation 未指定 usage 时继承；usage 变化触发换新。
#[tokio::test]
async fn test_issue_cert_usage_profile_and_rotate_inherits() {
    let keys = new_keys();
    let pki = make_pki_with_store(Arc::new(MemoryPkiStore::new()), &keys);
    pki.init_ca("Usage CA").await.expect("初始化 CA 失败");
    let cn = "svc-server.coord.local";

    let server = pki
        .issue_cert_with_profile(
            cn,
            0,
            coord_agent::pki::SanSpec::default(),
            CertUsage::Server,
        )
        .await
        .expect("issue server");
    assert_eq!(server.usage, CertUsage::Server);
    let der = cert_der(&server.cert_pem);
    let (_, parsed) = x509_parser::parse_x509_certificate(&der).expect("parse cert");
    let eku = parsed
        .extended_key_usage()
        .expect("eku")
        .expect("eku value");
    assert!(eku.value.server_auth);
    assert!(!eku.value.client_auth);

    let rotated = pki.rotate_cert(cn, 0).await.expect("rotate");
    assert_eq!(rotated.usage, CertUsage::Server, "Rotate 必须继承 usage");

    let both = pki.issue_cert(cn, 0).await.expect("issue both");
    assert_ne!(both.serial, rotated.serial, "usage 变化必须换新");
    assert_eq!(both.usage, CertUsage::Both);
}

/// 外部证书存在时，managed IssueCert 不得把它当作幂等命中。
#[tokio::test]
async fn test_issue_cert_after_external_replaces_with_managed() {
    let keys = new_keys();
    let pki = make_pki_with_store(Arc::new(MemoryPkiStore::new()), &keys);
    pki.init_ca("CSR Issue CA").await.expect("初始化 CA 失败");

    let leaf_key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("leaf key");
    let csr = csr_der(
        "svc-switch.coord.local",
        &["svc-switch.coord.local"],
        &[],
        &leaf_key,
    );
    let external = pki
        .sign_csr(&csr, 0, CertUsage::Both)
        .await
        .expect("external");

    let managed = pki
        .issue_cert("svc-switch.coord.local", 0)
        .await
        .expect("managed issue");
    assert!(managed.key_name.starts_with("pki/leaf/"));
    assert_ne!(managed.serial, external.serial);
}

/// G-PKI-1 验收：到期 = 换新触发
#[tokio::test]
async fn test_expired_current_record_is_reissued_not_returned() {
    use coord_agent::pki_store::{CertRecord, CertStatus};

    let store = Arc::new(MemoryPkiStore::new());
    let keys = new_keys();
    let pki = make_pki_with_store(store.clone(), &keys);
    pki.init_ca("Expiry Semantics CA")
        .await
        .expect("初始化 CA 失败");

    let cn = "svc-expiry.coord.local";
    let expired = CertRecord {
        common_name: cn.into(),
        cert_pem: "-----BEGIN CERTIFICATE-----\nEXPIRED\n-----END CERTIFICATE-----".into(),
        key_name: format!("pki/leaf/{cn}"),
        key_version: 1,
        not_before: 1_700_000_000,
        not_after: 1_700_000_001,
        serial: "0e1".into(),
        status: CertStatus::Active,
        parent_serial: None,
        dns_names: Vec::new(),
        ip_addresses: Vec::new(),
        spki_sha256: String::new(),
        usage: coord_agent::pki_store::CertUsage::Both,
    };
    store.create_cert(cn, &expired).await.expect("seed expired");

    assert!(
        pki.get_cert_by_cn(cn).await.expect("get").is_none(),
        "过期记录不通过 GetCertByCN 返回"
    );

    let issued = pki.issue_cert(cn, 0).await.expect("issue over expired");
    assert_ne!(issued.serial, expired.serial, "IssueCert 必须换新 serial");
    assert!(pki.verify_cert(&issued.cert_pem).await.expect("verify"));

    let rotated = pki.rotate_cert(cn, 0).await.expect("rotate");
    assert_ne!(rotated.serial, issued.serial, "RotateCert 必须换新 serial");
    assert!(pki.verify_cert(&rotated.cert_pem).await.expect("verify"));

    let recovered = pki
        .renew_cert("0e1", 0)
        .await
        .expect("renew via expired serial");
    assert_eq!(recovered.common_name, cn);
    assert_ne!(recovered.serial, "0e1");
    assert!(pki.verify_cert(&recovered.cert_pem).await.expect("verify"));

    assert_eq!(
        recovered.parent_serial.as_deref(),
        Some(rotated.serial.as_str())
    );
}

// ──── 吊销面（ADR-0020）────

/// 验收：A 吊销 ⇒ B 的 VerifyCert 拒绝、状态与 CRL 同步；IssueCert 不静默重签；
/// 重启（同 store + 同密钥档案）后吊销事实仍在。
#[tokio::test]
async fn test_revocation_visible_across_agents_and_across_restart() {
    let store = Arc::new(MemoryPkiStore::new());
    let keys = new_keys();
    let pki_a = make_pki_with_store(store.clone(), &keys);
    let pki_b = make_pki_with_store(store.clone(), &keys);
    pki_a.init_ca("Revocation CA").await.expect("init A");
    pki_b.init_ca("Revocation CA").await.expect("init B");

    let cert = pki_a
        .issue_cert("revoked.coord.local", 0)
        .await
        .expect("issue");
    assert!(pki_b.verify_cert(&cert.cert_pem).await.expect("B verify"));

    pki_a
        .revoke_cert(
            &cert.serial,
            coord_agent::pki_store::RevocationReason::KeyCompromise,
            "ops-a",
        )
        .await
        .expect("A revoke");

    assert!(
        !pki_b
            .verify_cert(&cert.cert_pem)
            .await
            .expect("B verify revoked"),
        "B 必须拒绝 A 吊销的 serial"
    );
    let status = pki_b.get_cert_status(&cert.serial).await.expect("B status");
    assert_eq!(status.status, "revoked");
    assert_eq!(status.revocation.expect("revocation").by, "ops-a");

    // 负控制：revoke 后 IssueCert(同 CN) 不返回新证书
    assert!(
        matches!(
            pki_b.issue_cert("revoked.coord.local", 0).await,
            Err(PkiError::FailedPrecondition(_))
        ),
        "已吊销 CN 的 IssueCert 必须显式失败"
    );

    // 重启：store + 密钥档案保留 ⇒ 吊销事实与 CRL 都不丢
    let pki_restart = make_pki_with_store(store.clone(), &keys);
    pki_restart
        .init_ca("Revocation CA")
        .await
        .expect("restart init");
    assert!(!pki_restart
        .verify_cert(&cert.cert_pem)
        .await
        .expect("restart verify"));
    let crl = pki_restart.get_crl().await.expect("restart CRL");
    let (_rest, parsed) = x509_parser::parse_x509_crl(&crl.der).expect("parse CRL");
    let serials: Vec<String> = parsed
        .iter_revoked_certificates()
        .map(|entry| hex::encode(entry.serial().to_bytes_be()))
        .collect();
    assert!(
        serials.contains(&cert.serial),
        "CRL 必须包含被吊销的 serial: {serials:?}"
    );
}
