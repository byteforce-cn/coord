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

use coord_agent::pki::{PkiConfig, PkiService};
use coord_agent::pki_store::{MemoryPkiStore, PkiStore};
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

/// 验证 PKI 配置默认值
#[test]
fn test_pki_config_defaults() {
    let config = PkiConfig::default();
    assert_eq!(config.cert_ttl_hours, 24, "证书默认 24h TTL");
    assert_eq!(
        config.expiry_warn_hours, 6,
        "到期告警窗口默认 6h（G-PKI-2）"
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

    let valid = pki.verify_cert(&cert.cert_pem).expect("验证失败");
    assert!(valid, "CA 应能验证自己签发的证书");
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
        pki_b.verify_cert(&cert.cert_pem).expect("B 验签"),
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
        pki.verify_cert(&old.cert_pem).expect("验签旧证书"),
        "旧证书保留期内应可验签"
    );
    assert!(pki.verify_cert(&renewed.cert_pem).expect("验签新证书"));
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

    assert!(pki.verify_cert(&first.cert_pem).expect("旧证书可验"));
    assert!(pki.verify_cert(&rotated.cert_pem).expect("新证书可验"));

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
        serial: "expired-0x1".into(),
        status: CertStatus::Active,
        parent_serial: None,
        dns_names: Vec::new(),
        ip_addresses: Vec::new(),
    };
    store.create_cert(cn, &expired).await.expect("seed expired");

    assert!(
        pki.get_cert_by_cn(cn).await.expect("get").is_none(),
        "过期记录不通过 GetCertByCN 返回"
    );

    let issued = pki.issue_cert(cn, 0).await.expect("issue over expired");
    assert_ne!(issued.serial, expired.serial, "IssueCert 必须换新 serial");
    assert!(pki.verify_cert(&issued.cert_pem).expect("verify"));

    let rotated = pki.rotate_cert(cn, 0).await.expect("rotate");
    assert_ne!(rotated.serial, issued.serial, "RotateCert 必须换新 serial");
    assert!(pki.verify_cert(&rotated.cert_pem).expect("verify"));

    let recovered = pki
        .renew_cert("expired-0x1", 0)
        .await
        .expect("renew via expired serial");
    assert_eq!(recovered.common_name, cn);
    assert_ne!(recovered.serial, "expired-0x1");
    assert!(pki.verify_cert(&recovered.cert_pem).expect("verify"));

    assert_eq!(
        recovered.parent_serial.as_deref(),
        Some(rotated.serial.as_str())
    );
}
