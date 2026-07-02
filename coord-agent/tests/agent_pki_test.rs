// TDD: Agent 端 PKI 服务测试 (Phase F — 待实施)
//
// v8.2 §4.12: PKI — CA 私钥受根密钥保护，为 mTLS 签发短期证书
// - CA 自动签发
// - 短期证书（默认 24h）
// - 证书轮换
//
// RED stage: PkiService 尚未定义

use coord_agent::pki::{PkiService, PkiConfig};

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
#[test]
fn test_pki_service_init_and_issue() {
    let config = PkiConfig::default();
    let pki = PkiService::new(config).expect("创建 PkiService 失败");

    // 初始化 CA
    pki.init_ca("Coord Test CA").expect("初始化 CA 失败");

    // 签发证书
    let cert = pki.issue_cert("agent-001.coord.local").expect("签发证书失败");

    assert_eq!(cert.common_name, "agent-001.coord.local");
    assert!(!cert.cert_pem.is_empty(), "证书 PEM 不应为空");
    assert!(!cert.key_pem.is_empty(), "私钥 PEM 不应为空");
    assert!(cert.not_after > cert.not_before, "有效期应合法");
}

/// 验证签发的证书可被 CA 验证
#[test]
fn test_pki_service_cert_chain_validation() {
    let config = PkiConfig::default();
    let pki = PkiService::new(config).expect("创建 PkiService 失败");
    pki.init_ca("Coord Chain CA").expect("初始化 CA 失败");

    let cert = pki.issue_cert("test.coord.local").expect("签发证书失败");

    // 用 CA 证书验证签发的证书链
    let valid = pki.verify_cert(&cert.cert_pem).expect("验证失败");
    assert!(valid, "CA 应能验证自己签发的证书");
}

/// 验证证书轮换：签发新证书，旧证书仍有效直到过期
#[test]
fn test_pki_service_cert_rotation() {
    let config = PkiConfig::default();
    let pki = PkiService::new(config).expect("创建 PkiService 失败");
    pki.init_ca("Coord Rotation CA").expect("初始化 CA 失败");

    // 签发初始证书
    let cert1 = pki.issue_cert("agent-001.coord.local").expect("签发 v1 失败");

    // 轮换：签发新证书
    let cert2 = pki.issue_cert("agent-001.coord.local").expect("签发 v2 失败");

    // 两个证书应都有效
    assert!(pki.verify_cert(&cert1.cert_pem).expect("验证 v1 失败"));
    assert!(pki.verify_cert(&cert2.cert_pem).expect("验证 v2 失败"));

    // 新证书应晚于旧证书
    assert!(cert2.not_before >= cert1.not_before);
}

/// 验证 CA 证书可导出
#[test]
fn test_pki_service_export_ca() {
    let config = PkiConfig::default();
    let pki = PkiService::new(config).expect("创建 PkiService 失败");
    pki.init_ca("Coord Export CA").expect("初始化 CA 失败");

    let ca_cert = pki.ca_cert_pem().expect("导出 CA 证书失败");
    assert!(!ca_cert.is_empty());
    assert!(ca_cert.contains("BEGIN CERTIFICATE"));
}
