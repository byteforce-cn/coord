// TDD: Agent 端 PKI 服务测试（Phase F / ISSUE-000 整改）
//
// v8.2 §4.12: PKI — CA 私钥受根密钥保护，为 mTLS 签发短期证书
// ISSUE-000: 按 CN 幂等取回（get-or-create）+ CA/证书共享存储持久化
//
// 覆盖验收标准（docs/issue/ISSUE-000-test-评估与方案.md §8）：
// 1. 同一 CN 连续 issueCert → 相同 serial / 公钥 / 私钥
// 2. agent 重启（store 保留）→ issueCert 仍返回同一证书；CA 不重建
// 3. 两 agent 并发首次 issueCert(同 CN) → 仅一份密钥（CAS 单写）
// 4. 多 agent：A 签发的证书可在 B 验签（同一 CA 根）
// 5. renewCert(serial) → 返回原 CN、新 serial，旧证书保留可验签
// 6. rotateCert(CN) 后 listCerts(CN) → active + retired 两份

use std::sync::Arc;

use coord_agent::pki::{PkiConfig, PkiService};
use coord_agent::pki_store::{MemoryPkiStore, PkiStore};

/// 创建共享 store 的 PkiService（模拟同一集群下的 agent 实例）
fn make_pki_with_store(store: Arc<MemoryPkiStore>) -> PkiService {
    let store: Arc<dyn PkiStore> = store;
    PkiService::with_store(PkiConfig::default(), store)
}

/// 验证 PKI 配置默认值
#[test]
fn test_pki_config_defaults() {
    let config = PkiConfig::default();
    assert_eq!(config.cert_ttl_hours, 24, "证书默认 24h TTL");
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

/// 验证 PkiService 能初始化 CA 并签发证书
#[tokio::test]
async fn test_pki_service_init_and_issue() {
    let pki = make_pki_with_store(Arc::new(MemoryPkiStore::new()));

    // 初始化 CA
    pki.init_ca("Coord Test CA").await.expect("初始化 CA 失败");

    // 签发证书
    let cert = pki.issue_cert("agent-001.coord.local", 0).await.expect("签发证书失败");

    assert_eq!(cert.common_name, "agent-001.coord.local");
    assert!(!cert.cert_pem.is_empty(), "证书 PEM 不应为空");
    assert!(!cert.key_pem.is_empty(), "私钥 PEM 不应为空");
    assert!(cert.not_after > cert.not_before, "有效期应合法");
}

/// 验证签发的证书可被 CA 验证
#[tokio::test]
async fn test_pki_service_cert_chain_validation() {
    let pki = make_pki_with_store(Arc::new(MemoryPkiStore::new()));
    pki.init_ca("Coord Chain CA").await.expect("初始化 CA 失败");

    let cert = pki.issue_cert("test.coord.local", 0).await.expect("签发证书失败");

    // 用 CA 证书验证签发的证书链
    let valid = pki.verify_cert(&cert.cert_pem).expect("验证失败");
    assert!(valid, "CA 应能验证自己签发的证书");
}

/// 验收 1: 同一 CN 连续 issueCert → 相同 serial / 相同公钥 / 相同私钥
#[tokio::test]
async fn test_issue_cert_idempotent_get_or_create() {
    let store = Arc::new(MemoryPkiStore::new());
    let pki = make_pki_with_store(store.clone());
    pki.init_ca("Idem CA").await.expect("初始化 CA 失败");

    let cert1 = pki.issue_cert("svc-a.coord.local", 0).await.expect("首次签发");
    let cert2 = pki.issue_cert("svc-a.coord.local", 0).await.expect("再次签发");

    assert_eq!(cert1.serial, cert2.serial, "同一 CN 必须返回相同 serial");
    assert_eq!(cert1.key_pem, cert2.key_pem, "同一 CN 必须返回相同私钥");
    assert_eq!(cert1.cert_pem, cert2.cert_pem, "同一 CN 必须返回相同证书");
    assert_eq!(cert1.not_after, cert2.not_after, "同一 CN 必须返回相同有效期");
}

/// 验收 2: agent 重启（store 保留）→ issueCert 仍返回同一证书；CA 不重建
#[tokio::test]
async fn test_issue_cert_after_restart_returns_same_cert() {
    let store = Arc::new(MemoryPkiStore::new());

    // 第一个 agent 实例：初始化 CA + 签发
    let pki_a = make_pki_with_store(store.clone());
    pki_a.init_ca("Restart CA").await.expect("初始化 CA 失败");
    let ca_a = pki_a.ca_cert_pem().expect("导出 CA");
    let cert_a = pki_a.issue_cert("svc-restart.coord.local", 0).await.expect("签发");

    // 模拟重启：新建 PkiService（同一 store），重新 init_ca
    let pki_b = make_pki_with_store(store.clone());
    pki_b.init_ca("Restart CA").await.expect("重启后初始化 CA 失败");

    // CA 不重建：导出的根证书一致
    let ca_b = pki_b.ca_cert_pem().expect("重启后导出 CA");
    assert_eq!(ca_a, ca_b, "CA 根证书不得重建");

    // 重启后仍取回同一证书
    let cert_b = pki_b.issue_cert("svc-restart.coord.local", 0).await.expect("重启后签发");
    assert_eq!(cert_a.serial, cert_b.serial, "重启后必须取回同一证书");
    assert_eq!(cert_a.key_pem, cert_b.key_pem, "重启后必须取回同一私钥");
}

/// 验收 3: 两个 agent 并发首次 issueCert(同 CN) → 仅一份密钥（Txn CAS 单写）
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_first_issue_single_key() {
    let store = Arc::new(MemoryPkiStore::new());

    let pki_a = make_pki_with_store(store.clone());
    let pki_b = make_pki_with_store(store.clone());
    pki_a.init_ca("Concurrent CA").await.expect("A 初始化 CA 失败");
    pki_b.init_ca("Concurrent CA").await.expect("B 初始化 CA 失败");

    let a = tokio::spawn(async move {
        pki_a.issue_cert("svc-concurrent.coord.local", 0).await.expect("A 签发失败")
    });
    let b = tokio::spawn(async move {
        pki_b.issue_cert("svc-concurrent.coord.local", 0).await.expect("B 签发失败")
    });

    let cert_a = a.await.expect("join A");
    let cert_b = b.await.expect("join B");

    // 并发首次签发：两份结果必须一致（仅一份密钥，无竞态双签发）
    assert_eq!(cert_a.serial, cert_b.serial, "并发首次签发必须只有一份密钥");
    assert_eq!(cert_a.key_pem, cert_b.key_pem);

    // 且 store 中只有一份 active 记录
    let stored = store.get_cert("svc-concurrent.coord.local").await.expect("读取 store");
    let stored = stored.expect("store 应有 active 记录");
    assert_eq!(stored.serial, cert_a.serial);
}

/// 验收 4: 多 agent：A 签发的证书可在 B 验签（同一 CA 根）
#[tokio::test]
async fn test_cross_agent_verify_same_ca() {
    let store = Arc::new(MemoryPkiStore::new());

    let pki_a = make_pki_with_store(store.clone());
    let pki_b = make_pki_with_store(store.clone());
    pki_a.init_ca("Shared CA").await.expect("A 初始化 CA 失败");
    pki_b.init_ca("Shared CA").await.expect("B 初始化 CA 失败");

    // A 签发，B 验签
    let cert = pki_a.issue_cert("svc-a.coord.local", 0).await.expect("A 签发");
    assert!(
        pki_b.verify_cert(&cert.cert_pem).expect("B 验签"),
        "A 签发的证书必须在 B 下可验签（同一 CA 根）"
    );
}

/// 验收 5: renewCert(serial) → 返回原 CN、新 serial，旧证书保留至 not_after 可验签
#[tokio::test]
async fn test_renew_cert_returns_original_cn() {
    let store = Arc::new(MemoryPkiStore::new());
    let pki = make_pki_with_store(store.clone());
    pki.init_ca("Renew CA").await.expect("初始化 CA 失败");

    let old = pki.issue_cert("svc-renew.coord.local", 0).await.expect("首次签发");
    let renewed = pki.renew_cert(&old.serial, 0).await.expect("续期失败");

    // 返回原 CN（修复 serial-as-CN bug）
    assert_eq!(renewed.common_name, "svc-renew.coord.local", "renew 必须返回原 CN");
    assert_ne!(renewed.serial, old.serial, "新证书必须换新 serial");
    assert_ne!(renewed.key_pem, old.key_pem, "新证书必须换新密钥");

    // 旧证书保留至 not_after 仍可验签
    assert!(pki.verify_cert(&old.cert_pem).expect("验签旧证书"), "旧证书保留期内应可验签");
    assert!(pki.verify_cert(&renewed.cert_pem).expect("验签新证书"));
}

/// 验收 6: rotateCert(CN) 后 listCerts(CN) → active + retired 两份（多 kid 验签）
#[tokio::test]
async fn test_rotate_then_list_returns_active_and_retired() {
    let store = Arc::new(MemoryPkiStore::new());
    let pki = make_pki_with_store(store.clone());
    pki.init_ca("Rotate CA").await.expect("初始化 CA 失败");

    let first = pki.issue_cert("svc-rotate.coord.local", 0).await.expect("首次签发");
    let rotated = pki.rotate_cert("svc-rotate.coord.local", 0).await.expect("轮换失败");

    assert_ne!(rotated.serial, first.serial);
    assert_eq!(rotated.status, coord_agent::pki_store::CertStatus::Active);

    let list = pki.list_certs("svc-rotate.coord.local").await.expect("列表失败");
    assert_eq!(list.len(), 2, "rotate 后应返回 active + retired 两份");
    let serials: Vec<&str> = list.iter().map(|c| c.serial.as_str()).collect();
    assert!(serials.contains(&first.serial.as_str()), "旧证书应保留在列表");
    assert!(serials.contains(&rotated.serial.as_str()), "新证书应在列表");

    // 验签方按 kid（serial）双密钥可验：新旧证书都有效
    assert!(pki.verify_cert(&first.cert_pem).expect("旧证书可验"));
    assert!(pki.verify_cert(&rotated.cert_pem).expect("新证书可验"));

    // 幂等取回仍返回当前 active（轮换后的新证书）
    let again = pki.issue_cert("svc-rotate.coord.local", 0).await.expect("取回失败");
    assert_eq!(again.serial, rotated.serial, "rotate 后 issue 应返回 active 证书");
}

/// 验证 get_cert_by_cn 幂等取回当前有效证书（含私钥）
#[tokio::test]
async fn test_get_cert_by_cn_returns_active_with_key() {
    let store = Arc::new(MemoryPkiStore::new());
    let pki = make_pki_with_store(store.clone());
    pki.init_ca("GetCN CA").await.expect("初始化 CA 失败");

    // 未签发 → None
    assert!(pki.get_cert_by_cn("missing.coord.local").await.expect("读取").is_none());

    let issued = pki.issue_cert("svc-getcn.coord.local", 0).await.expect("签发");
    let got = pki.get_cert_by_cn("svc-getcn.coord.local").await.expect("取回").expect("应有证书");
    assert_eq!(got.serial, issued.serial);
    assert_eq!(got.key_pem, issued.key_pem, "getCertByCN 应返回私钥供持有方恢复签名");
}

/// 验证按 serial 续期不存在时返回 CertNotFound
#[tokio::test]
async fn test_renew_unknown_serial_returns_not_found() {
    let store = Arc::new(MemoryPkiStore::new());
    let pki = make_pki_with_store(store.clone());
    pki.init_ca("NotFound CA").await.expect("初始化 CA 失败");

    let result = pki.renew_cert("deadbeef", 0).await;
    assert!(result.is_err(), "未知 serial 必须报错");
    assert!(
        matches!(result.unwrap_err(), coord_agent::pki::PkiError::CertNotFound(_)),
        "应返回 CertNotFound"
    );
}

