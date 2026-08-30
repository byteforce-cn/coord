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

use sha2::{Digest, Sha256};

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

    /// P2-05：证书文件指纹快照（mtime + 长度 + 内容 SHA-256，用于热加载变更检测）。
    pub fn fingerprint(&self) -> Vec<FileFingerprint> {
        let mut paths = vec![(&self.cert_path, true), (&self.key_path, true)];
        if let Some(ca) = &self.ca_path {
            paths.push((ca, false));
        }
        paths
            .into_iter()
            .filter_map(|(p, _)| FileFingerprint::of(p))
            .collect()
    }

    /// P2-05：与上一快照对比，任一证书文件（内容/长度/存在性）变化返回 `true`。
    /// 首次调用（`previous == None`）返回 `false`（仅记录基线，不触发重载）。
    pub fn files_changed(&self, previous: &mut Option<Vec<FileFingerprint>>) -> bool {
        let current = self.fingerprint();
        let changed = match previous {
            None => false,
            Some(prev) => current != *prev,
        };
        *previous = Some(current);
        changed
    }
}

/// P2-05：单个证书文件的指纹（路径 + 修改时间 + 长度 + 内容 SHA-256）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileFingerprint {
    pub path: PathBuf,
    pub modified: Option<std::time::SystemTime>,
    pub len: u64,
    /// 内容 SHA-256：变更检测不能只依赖 mtime/长度——在粗粒度时间戳
    /// 文件系统（如 1s 粒度）上，同长度内容在同一秒内改写时 mtime 与
    /// 长度均不变，会漏检热更新。
    pub hash: [u8; 32],
}

impl FileFingerprint {
    fn of(path: &Path) -> Option<Self> {
        let bytes = fs::read(path).ok()?;
        let meta = fs::metadata(path).ok()?;
        Some(Self {
            path: path.to_path_buf(),
            modified: meta.modified().ok(),
            len: meta.len(),
            hash: Sha256::digest(&bytes).into(),
        })
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

    let mut tls_config = tonic::transport::server::ServerTlsConfig::new().identity(identity);

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

    let mut tls = tonic::transport::channel::ClientTlsConfig::new().ca_certificate(ca);

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

    // ──── P2-05：证书热加载变更检测 ────

    #[test]
    fn test_files_changed_baseline_and_change() {
        let tmpdir = tempfile::tempdir().unwrap();
        let cert = tmpdir.path().join("server.crt");
        let key = tmpdir.path().join("server.key");
        fs::write(&cert, b"cert-v1").unwrap();
        fs::write(&key, b"key-v1").unwrap();
        let config = TlsConfig::new(cert.clone(), key.clone(), None);

        let mut prev: Option<Vec<FileFingerprint>> = None;
        // 首次调用仅建立基线
        assert!(!config.files_changed(&mut prev));
        // 未变化
        assert!(!config.files_changed(&mut prev));
        // 证书内容变化（mtime/长度变化）
        fs::write(&cert, b"cert-v2-longer").unwrap();
        assert!(
            config.files_changed(&mut prev),
            "cert change must be detected"
        );
        // 变更后再查：回到稳定
        assert!(!config.files_changed(&mut prev));
        // 私钥变化
        fs::write(&key, b"key-v2").unwrap();
        assert!(
            config.files_changed(&mut prev),
            "key change must be detected"
        );
    }

    #[test]
    fn test_files_changed_detects_removed_file() {
        let tmpdir = tempfile::tempdir().unwrap();
        let cert = tmpdir.path().join("server.crt");
        let key = tmpdir.path().join("server.key");
        fs::write(&cert, b"cert").unwrap();
        fs::write(&key, b"key").unwrap();
        let config = TlsConfig::new(cert.clone(), key.clone(), None);

        let mut prev: Option<Vec<FileFingerprint>> = None;
        let _ = config.files_changed(&mut prev);
        // 删除证书文件 → 指纹集合变化
        fs::remove_file(&cert).unwrap();
        assert!(
            config.files_changed(&mut prev),
            "removed cert must be detected"
        );
    }
}
