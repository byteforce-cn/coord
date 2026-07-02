// TLS/mTLS 传输安全模块
//
// 提供 gRPC Server 和 Raft Network 的 TLS 配置。
// ADP §14.1 安全分层第一层：传输层安全（TLS），mTLS 双向证书验证。
//
// 使用 tonic 内置 TLS 集成，支持：
// - 服务端 TLS（server.crt + server.key）
// - 客户端 mTLS（CA 证书验证客户端身份）
// - Raft 节点间 TLS

use std::fs;
use std::path::{Path, PathBuf};

// ──── TLS 配置 ────

/// TLS 配置
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// 服务端证书路径（PEM 格式）
    pub cert_path: PathBuf,
    /// 服务端私钥路径（PEM 格式）
    pub key_path: PathBuf,
    /// CA 证书路径（mTLS 客户端验证，PEM 格式）
    pub ca_path: Option<PathBuf>,
}

impl TlsConfig {
    /// 从路径创建 TLS 配置
    pub fn new(cert_path: PathBuf, key_path: PathBuf, ca_path: Option<PathBuf>) -> Self {
        Self {
            cert_path,
            key_path,
            ca_path,
        }
    }

    /// 检查 TLS 证书文件是否都存在
    pub fn is_configured(&self) -> bool {
        self.cert_path.exists() && self.key_path.exists()
    }

    /// 加载服务端证书（PEM 格式）
    pub fn load_cert(&self) -> Result<Vec<u8>, std::io::Error> {
        fs::read(&self.cert_path)
    }

    /// 加载服务端私钥（PEM 格式）
    pub fn load_key(&self) -> Result<Vec<u8>, std::io::Error> {
        fs::read(&self.key_path)
    }

    /// 加载 CA 证书（PEM 格式，用于 mTLS）
    pub fn load_ca(&self) -> Result<Option<Vec<u8>>, std::io::Error> {
        match &self.ca_path {
            Some(path) => fs::read(path).map(Some),
            None => Ok(None),
        }
    }
}

// ──── Tonic TLS 配置构建 ────

/// 构建 tonic gRPC Server 的 TLS 配置
///
/// 如果提供了 CA 证书，则启用 mTLS（要求客户端提供证书）。
pub fn build_server_tls(
    config: &TlsConfig,
) -> Result<tonic::transport::server::ServerTlsConfig, Box<dyn std::error::Error + Send + Sync>> {
    let cert_pem = config.load_cert()?;
    let key_pem = config.load_key()?;

    let identity = tonic::transport::Identity::from_pem(&cert_pem, &key_pem);

    let mut tls_config = tonic::transport::server::ServerTlsConfig::new()
        .identity(identity);

    // mTLS: 添加客户端证书验证
    if let Some(ca_pem) = config.load_ca()? {
        let ca = tonic::transport::Certificate::from_pem(&ca_pem);
        tls_config = tls_config.client_ca_root(ca);
        tracing::info!("mTLS enabled: client certificate verification active");
    }

    Ok(tls_config)
}

/// 构建 tonic gRPC Client 的 TLS 配置（用于 Raft 节点间通信）
///
/// 返回可选的 ClientTlsConfig，当证书配置缺失时返回 None。
pub fn build_client_tls(
    cert_path: Option<&Path>,
    key_path: Option<&Path>,
    ca_path: Option<&Path>,
) -> Option<tonic::transport::channel::ClientTlsConfig> {
    // 如果没有配置 CA，不启用 TLS
    let ca_path = ca_path?;
    let ca_pem = fs::read(ca_path).ok()?;
    let ca = tonic::transport::Certificate::from_pem(&ca_pem);

    let mut tls = tonic::transport::channel::ClientTlsConfig::new()
        .ca_certificate(ca);

    // mTLS：客户端也提供证书
    if let (Some(cert), Some(key)) = (cert_path, key_path) {
        if let (Ok(cert_pem), Ok(key_pem)) = (fs::read(cert), fs::read(key)) {
            let identity = tonic::transport::Identity::from_pem(&cert_pem, &key_pem);
            tls = tls.identity(identity);
        }
    }

    Some(tls)
}

// ──── 测试 ────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tls_config_not_configured() {
        let config = TlsConfig::new(
            PathBuf::from("/nonexistent/cert.pem"),
            PathBuf::from("/nonexistent/key.pem"),
            None,
        );
        assert!(!config.is_configured());
    }

    #[test]
    fn test_build_server_tls_missing_files() {
        let config = TlsConfig::new(
            PathBuf::from("/nonexistent/cert.pem"),
            PathBuf::from("/nonexistent/key.pem"),
            None,
        );
        let result = build_server_tls(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_client_tls_no_config() {
        let result = build_client_tls(None, None, None);
        assert!(result.is_none());
    }
}

