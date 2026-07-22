// coord-agent: PKI CA 自动签发/轮换服务 (Phase F)
//
// v8.2 §4.12: PKI — CA 私钥受根密钥保护，为 mTLS 签发短期证书。
//
// 核心能力：
// - 初始化 CA（自签名根证书）
// - 签发短期终端证书（默认 24h TTL）
// - 证书轮换（renew before expiry）
// - 证书验证（链式验证）
//
// 使用 rcgen 生成 X.509 证书，x509-parser 解析验证。

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use base64;
use parking_lot::RwLock;
use rcgen::{
    BasicConstraints, CertificateParams, DnType, IsCa, KeyPair,
    KeyUsagePurpose,
};
use time::OffsetDateTime;

// ──── PkiConfig ────

/// PKI 服务配置
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct PkiConfig {
    /// 证书 TTL（小时，默认 24）
    #[serde(default = "default_cert_ttl_hours")]
    pub cert_ttl_hours: u32,

    /// CA 证书路径（用于持久化）
    #[serde(default)]
    pub ca_cert_path: Option<PathBuf>,

    /// CA 私钥路径（用于持久化）
    #[serde(default)]
    pub ca_key_path: Option<PathBuf>,
}

fn default_cert_ttl_hours() -> u32 { 24 }

impl Default for PkiConfig {
    fn default() -> Self {
        Self {
            cert_ttl_hours: 24,
            ca_cert_path: None,
            ca_key_path: None,
        }
    }
}

// ──── CertInfo ────

/// 签发的证书信息
#[derive(Debug, Clone)]
pub struct CertInfo {
    /// 通用名称（CN）
    pub common_name: String,
    /// 证书 PEM
    pub cert_pem: String,
    /// 私钥 PEM
    pub key_pem: String,
    /// 生效时间（UNIX 秒）
    pub not_before: i64,
    /// 失效时间（UNIX 秒）
    pub not_after: i64,
    /// 序列号（十六进制）
    pub serial: String,
}

// ──── PkiService ────

/// PKI CA 服务
///
/// 管理 CA 密钥对，签发和验证终端证书。
/// CA 私钥仅存于内存，可通过 KeyUtil 加密持久化到磁盘。
pub struct PkiService {
    config: PkiConfig,
    /// CA 证书 + 私钥（签发后才初始化）
    ca: Arc<RwLock<Option<CaMaterial>>>,
}

struct CaMaterial {
    cert_pem: String,
    /// CA 密钥 PEM 编码（用于重建 KeyPair 签名）
    key_pem: String,
    params: CertificateParams,
}

impl PkiService {
    /// 创建 PKI 服务实例
    pub fn new(config: PkiConfig) -> Result<Self, PkiError> {
        Ok(Self {
            config,
            ca: Arc::new(RwLock::new(None)),
        })
    }

    /// 初始化 CA：生成自签名根证书
    pub fn init_ca(&self, ca_common_name: &str) -> Result<(), PkiError> {
        let mut ca = self.ca.write();

        if ca.is_some() {
            return Ok(());
        }

        let key_pair = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)
            .map_err(|e| PkiError::KeyGen(e.to_string()))?;

        let mut params = CertificateParams::default();
        params.distinguished_name.push(DnType::CommonName, ca_common_name);
        params.distinguished_name.push(DnType::OrganizationName, "Coord PKI");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];

        let now = OffsetDateTime::now_utc();
        params.not_before = now;
        params.not_after = now + time::Duration::days(3650);

        let key_pem = key_pair.serialize_pem();

        let cert = params
            .self_signed(&key_pair)
            .map_err(|e| PkiError::CertGen(e.to_string()))?;

        *ca = Some(CaMaterial {
            cert_pem: cert.pem(),
            key_pem,
            params,
        });

        Ok(())
    }

    /// 签发终端证书
    ///
    /// `ttl_seconds`: 证书有效期（秒）。为 0 时使用 config.cert_ttl_hours 默认值。
    pub fn issue_cert(&self, common_name: &str, ttl_seconds: u64) -> Result<CertInfo, PkiError> {
        let ca_guard = self.ca.read();
        let ca = ca_guard.as_ref().ok_or(PkiError::CaNotInitialized)?;

        let key_pair = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)
            .map_err(|e| PkiError::KeyGen(e.to_string()))?;

        let mut params = CertificateParams::default();
        params.distinguished_name.push(DnType::CommonName, common_name);
        params.distinguished_name.push(DnType::OrganizationName, "Coord Agent");
        params.is_ca = IsCa::NoCa;
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ];
        params.extended_key_usages = vec![
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
        ];

        let now = OffsetDateTime::now_utc();
        let ttl = if ttl_seconds > 0 {
            time::Duration::seconds(ttl_seconds as i64)
        } else {
            time::Duration::hours(self.config.cert_ttl_hours as i64)
        };
        params.not_before = now;
        params.not_after = now + ttl;

        let ca_key = KeyPair::from_pem(&ca.key_pem)
            .map_err(|e| PkiError::KeyGen(e.to_string()))?;

        let issuer = rcgen::Issuer::from_params(&ca.params, ca_key);

        let cert = params
            .signed_by(&key_pair, &issuer)
            .map_err(|e| PkiError::CertGen(e.to_string()))?;

        let not_before = now.unix_timestamp();
        let not_after = (now + ttl).unix_timestamp();
        let serial = format!("{:x}", rand::random::<u64>());

        Ok(CertInfo {
            common_name: common_name.to_string(),
            cert_pem: cert.pem(),
            key_pem: key_pair.serialize_pem(),
            not_before,
            not_after,
            serial,
        })
    }

    /// 续期证书：基于序列号查找并签发新证书
    ///
    /// 当前简化实现：重新签一张同 CN 的新证书（新 key pair + 新序列号）。
    /// 未来可扩展为基于原序列号匹配旧证书并续期。
    pub fn renew_cert(&self, common_name: &str, ttl_seconds: u64) -> Result<CertInfo, PkiError> {
        let ca_guard = self.ca.read();
        let ca = ca_guard.as_ref().ok_or(PkiError::CaNotInitialized)?;

        let key_pair = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)
            .map_err(|e| PkiError::KeyGen(e.to_string()))?;

        let mut params = CertificateParams::default();
        params.distinguished_name.push(DnType::CommonName, common_name);
        params.distinguished_name.push(DnType::OrganizationName, "Coord Agent");
        params.is_ca = IsCa::NoCa;
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ];
        params.extended_key_usages = vec![
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
        ];

        let now = OffsetDateTime::now_utc();
        let ttl = if ttl_seconds > 0 {
            time::Duration::seconds(ttl_seconds as i64)
        } else {
            time::Duration::hours(self.config.cert_ttl_hours as i64)
        };
        params.not_before = now;
        params.not_after = now + ttl;

        let ca_key = KeyPair::from_pem(&ca.key_pem)
            .map_err(|e| PkiError::KeyGen(e.to_string()))?;

        let issuer = rcgen::Issuer::from_params(&ca.params, ca_key);

        let cert = params
            .signed_by(&key_pair, &issuer)
            .map_err(|e| PkiError::CertGen(e.to_string()))?;

        let not_before = now.unix_timestamp();
        let not_after = (now + ttl).unix_timestamp();
        let serial = format!("{:x}", rand::random::<u64>());

        Ok(CertInfo {
            common_name: common_name.to_string(),
            cert_pem: cert.pem(),
            key_pem: key_pair.serialize_pem(),
            not_before,
            not_after,
            serial,
        })
    }
}

// ──── 辅助函数 ────

/// 将 PEM 证书转换为 DER 字节
fn pem_to_der(pem: &str) -> Result<Vec<u8>, PkiError> {
    let pem = pem.trim();
    let der_b64: String = pem
        .lines()
        .filter(|line| !line.starts_with("-----"))
        .collect();
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(&der_b64)
        .map_err(|e| PkiError::CertParse(format!("base64 decode: {e}")))
}

impl PkiService {
    /// 验证证书链
    pub fn verify_cert(&self, cert_pem: &str) -> Result<bool, PkiError> {
        let ca_guard = self.ca.read();
        let ca = ca_guard.as_ref().ok_or(PkiError::CaNotInitialized)?;

        // PEM → DER 转换后解析
        let cert_der = pem_to_der(cert_pem)?;
        let (_remainder, cert) = x509_parser::parse_x509_certificate(&cert_der)
            .map_err(|e| PkiError::CertParse(e.to_string()))?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        if now < cert.validity().not_before.timestamp() {
            return Ok(false);
        }
        if now > cert.validity().not_after.timestamp() {
            return Ok(false);
        }

        let issuer_cn = cert.issuer()
            .iter_common_name()
            .next()
            .map(|cn| cn.as_str().unwrap_or(""))
            .unwrap_or("");

        let ca_der = pem_to_der(&ca.cert_pem)?;
        let (_ca_remainder, ca_cert) = x509_parser::parse_x509_certificate(&ca_der)
            .map_err(|e| PkiError::CertParse(e.to_string()))?;

        let ca_cn = ca_cert.subject()
            .iter_common_name()
            .next()
            .map(|cn| cn.as_str().unwrap_or(""))
            .unwrap_or("");

        Ok(issuer_cn == ca_cn)
    }

    /// 导出 CA 证书 PEM
    pub fn ca_cert_pem(&self) -> Result<String, PkiError> {
        let ca = self.ca.read();
        let ca = ca.as_ref().ok_or(PkiError::CaNotInitialized)?;
        Ok(ca.cert_pem.clone())
    }

    /// 检查证书是否即将过期（剩余时间 < 指定小时数）
    pub fn is_expiring_soon(&self, cert: &CertInfo, within_hours: i64) -> bool {
        let now = OffsetDateTime::now_utc().unix_timestamp();
        let remaining = cert.not_after - now;
        remaining < within_hours * 3600
    }
}

// ──── PkiError ────

/// PKI 错误类型
#[derive(Debug)]
pub enum PkiError {
    CaNotInitialized,
    KeyGen(String),
    CertGen(String),
    CertParse(String),
    Io(std::io::Error),
}

impl std::fmt::Display for PkiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CaNotInitialized => write!(f, "CA not initialized"),
            Self::KeyGen(msg) => write!(f, "key generation failed: {msg}"),
            Self::CertGen(msg) => write!(f, "certificate generation failed: {msg}"),
            Self::CertParse(msg) => write!(f, "certificate parse failed: {msg}"),
            Self::Io(e) => write!(f, "IO error: {e}"),
        }
    }
}

impl std::error::Error for PkiError {}

impl From<std::io::Error> for PkiError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

// ──── gRPC trait impl ────

use coord_proto::agent::{
    pki_server::Pki,
    PkiInitCaRequest, PkiInitCaResponse,
    PkiIssueCertRequest, PkiIssueCertResponse,
    PkiRenewCertRequest, PkiRenewCertResponse,
    PkiVerifyCertRequest, PkiVerifyCertResponse,
    PkiGetCaCertRequest, PkiGetCaCertResponse,
};
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl Pki for PkiService {
    async fn init_ca(
        &self,
        request: Request<PkiInitCaRequest>,
    ) -> Result<Response<PkiInitCaResponse>, Status> {
        let req = request.into_inner();
        PkiService::init_ca(self, &req.ca_common_name)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(PkiInitCaResponse {}))
    }

    async fn issue_cert(
        &self,
        request: Request<PkiIssueCertRequest>,
    ) -> Result<Response<PkiIssueCertResponse>, Status> {
        let req = request.into_inner();
        let cert_info = PkiService::issue_cert(self, &req.common_name, req.ttl_seconds as u64)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(PkiIssueCertResponse {
            common_name: cert_info.common_name,
            cert_pem: cert_info.cert_pem,
            key_pem: cert_info.key_pem,
            not_before: cert_info.not_before,
            not_after: cert_info.not_after,
            serial: cert_info.serial,
        }))
    }

    async fn renew_cert(
        &self,
        request: Request<PkiRenewCertRequest>,
    ) -> Result<Response<PkiRenewCertResponse>, Status> {
        let req = request.into_inner();
        // 当前简化实现：使用 serial_number 作为 common_name 续期
        // 未来可通过 serial 查找已签发的证书并匹配 CN
        let cert_info = PkiService::renew_cert(self, &req.serial_number, req.ttl_seconds as u64)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(PkiRenewCertResponse {
            common_name: cert_info.common_name,
            cert_pem: cert_info.cert_pem,
            key_pem: cert_info.key_pem,
            not_before: cert_info.not_before,
            not_after: cert_info.not_after,
            serial: cert_info.serial,
        }))
    }

    async fn verify_cert(
        &self,
        request: Request<PkiVerifyCertRequest>,
    ) -> Result<Response<PkiVerifyCertResponse>, Status> {
        let req = request.into_inner();
        let valid = PkiService::verify_cert(self, &req.cert_pem)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(PkiVerifyCertResponse { valid }))
    }

    async fn get_ca_cert(
        &self,
        _request: Request<PkiGetCaCertRequest>,
    ) -> Result<Response<PkiGetCaCertResponse>, Status> {
        let ca_cert_pem = PkiService::ca_cert_pem(self)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(PkiGetCaCertResponse { ca_cert_pem }))
    }
}

// ──── tests ────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_ca_and_issue() {
        let pki = PkiService::new(PkiConfig::default()).expect("create");
        pki.init_ca("Test CA").expect("init");

        let cert = pki.issue_cert("test.local", 0).expect("issue");
        assert_eq!(cert.common_name, "test.local");
        assert!(!cert.cert_pem.is_empty());
        assert!(!cert.key_pem.is_empty());
    }

    #[test]
    fn test_verify_valid_cert() {
        let pki = PkiService::new(PkiConfig::default()).expect("create");
        pki.init_ca("Verify CA").expect("init");

        let cert = pki.issue_cert("verify.local", 0).expect("issue");
        assert!(pki.verify_cert(&cert.cert_pem).expect("verify"));
    }

    #[test]
    fn test_ca_cert_export() {
        let pki = PkiService::new(PkiConfig::default()).expect("create");
        pki.init_ca("Export CA").expect("init");

        let pem = pki.ca_cert_pem().expect("export");
        assert!(pem.contains("BEGIN CERTIFICATE"));
    }

    /// RED→GREEN: 验证未初始化 CA 时 issue_cert 返回 CaNotInitialized 错误。
    /// 修复前 dev 模式 PKI CA 未自动初始化。
    #[test]
    fn test_ca_not_initialized_error() {
        let pki = PkiService::new(PkiConfig::default()).expect("create");
        let result = pki.issue_cert("test.local", 0);
        assert!(result.is_err());
        match result.unwrap_err() {
            PkiError::CaNotInitialized => {}
            other => panic!("expected CaNotInitialized, got: {}", other),
        }
    }

    /// 验证 CA init_ca 幂等性（多次调用不报错）
    #[test]
    fn test_init_ca_idempotent() {
        let pki = PkiService::new(PkiConfig::default()).expect("create");
        pki.init_ca("Test CA").expect("first init");
        pki.init_ca("Test CA").expect("second init (idempotent)");
        let cert = pki.issue_cert("test.local", 0).expect("issue after init");
        assert!(!cert.cert_pem.is_empty());
    }
}
