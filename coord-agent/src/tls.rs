// coord-agent: TLS/mTLS 传输安全模块 (Phase A-mTLS)
//
// 提供 Agent ↔ Server 的 TLS/mTLS 配置与 Channel 构建。
// v8.2 §2.2: Agent ↔ Server 强制 mTLS，证书由 PKI 服务自动签发轮换。
//
// 职责：
// - AgentTlsConfig: 证书路径配置，支持 TOML 反序列化
// - build_agent_tls_channel(): 构建 TLS 加密的 tonic Channel
// - build_agent_tls_server_config(): 构建 mTLS 服务端配置（测试用）

use std::path::{Path, PathBuf};
use std::fs;

use tonic::transport::{Certificate, ClientTlsConfig, Identity};

// ──── AgentTlsConfig ────

/// Agent TLS/mTLS 配置
///
/// 支持从 TOML 配置文件反序列化。
/// 当 `ca_path` 为 Some 时启用 mTLS（双向证书验证）。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct AgentTlsConfig {
    /// Agent 客户端证书路径（PEM 格式）
    pub cert_path: PathBuf,
    /// Agent 客户端私钥路径（PEM 格式）
    pub key_path: PathBuf,
    /// CA 证书路径（PEM 格式，用于验证服务端 + mTLS 客户端身份）
    /// None = 仅 TLS（验证服务端），Some = mTLS（双向验证）
    #[serde(default)]
    pub ca_path: Option<PathBuf>,
}

impl AgentTlsConfig {
    /// 检查 TLS 证书文件是否都存在
    pub fn is_configured(&self) -> bool {
        self.cert_path.exists() && self.key_path.exists()
    }

    /// 加载客户端证书（PEM 格式）
    pub fn load_cert(&self) -> Result<Vec<u8>, std::io::Error> {
        fs::read(&self.cert_path)
    }

    /// 加载客户端私钥（PEM 格式）
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

// ──── TLS Channel 构建 ────

/// 构建到 Server 的 TLS/mTLS Channel
///
/// # Arguments
/// * `endpoint_url` - Server gRPC 地址，格式 `https://host:port`
/// * `tls_config` - Agent TLS 配置
///
/// # Errors
/// 当证书加载失败或 Channel 构建失败时返回错误。
pub async fn build_agent_tls_channel(
    endpoint_url: &str,
    tls_config: &AgentTlsConfig,
) -> Result<tonic::transport::Channel, Box<dyn std::error::Error + Send + Sync>> {
    let ca_pem = match tls_config.load_ca()? {
        Some(ca) => ca,
        None => return Err("CA certificate is required for TLS connection".into()),
    };

    let ca = Certificate::from_pem(&ca_pem);

    let mut tls = ClientTlsConfig::new()
        .ca_certificate(ca)
        .domain_name("localhost"); // Allow localhost connections for dev

    // mTLS: 提供客户端证书
    if tls_config.is_configured() {
        let cert_pem = tls_config.load_cert()?;
        let key_pem = tls_config.load_key()?;
        let identity = Identity::from_pem(&cert_pem, &key_pem);
        tls = tls.identity(identity);
    }

    let channel = tonic::transport::Channel::from_shared(endpoint_url.to_string())?
        .tls_config(tls)?
        .connect()
        .await?;

    Ok(channel)
}

/// 构建测试用的 TLS Server 配置
///
/// 用于集成测试，启动 mTLS 服务端。
/// 当 `ca_path` 为 Some 时启用客户端证书验证（mTLS）。
pub fn build_agent_tls_server_config(
    cert_path: &Path,
    key_path: &Path,
    ca_path: Option<&Path>,
) -> Result<tonic::transport::server::ServerTlsConfig, Box<dyn std::error::Error + Send + Sync>> {
    let cert_pem = fs::read(cert_path)?;
    let key_pem = fs::read(key_path)?;

    let identity = Identity::from_pem(&cert_pem, &key_pem);

    let mut tls_config = tonic::transport::server::ServerTlsConfig::new()
        .identity(identity);

    if let Some(ca_path) = ca_path {
        let ca_pem = fs::read(ca_path)?;
        let ca = Certificate::from_pem(&ca_pem);
        tls_config = tls_config.client_ca_root(ca);
    }

    Ok(tls_config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tls_config_missing_files() {
        let config = AgentTlsConfig {
            cert_path: PathBuf::from("/nonexistent/cert.pem"),
            key_path: PathBuf::from("/nonexistent/key.pem"),
            ca_path: None,
        };
        assert!(!config.is_configured());
    }
}
