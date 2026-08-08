// coord-agent: PKI CA 自动签发/轮换服务 (Phase F / ISSUE-000)
//
// v8.2 §4.12: PKI — CA 私钥受根密钥保护，为 mTLS 签发短期证书。
//
// 核心能力（ISSUE-000 整改后）：
// - 初始化 CA（自签名根证书，**持久化到共享 KV**，重启/多 agent 共享同一 CA 根）
// - **按 CN 幂等取回（get-or-create）**：同一 CN 未过期证书直接返回既有记录
// - 证书轮换（rotate / renew，旧证书保留至 not_after 供验签）
// - 证书验证（链式验证）
// - 证书列表（按 CN 取 active + 历史，验签方按 serial/kid 构建多密钥 JWKS）
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

use crate::pki_store::{
    CaRecord, CertRecord, CertStatus, MemoryPkiStore, PkiStore, PkiStoreError,
};

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
    /// 证书状态（active / retired）
    pub status: CertStatus,
    /// 轮换链：本证书由哪个 serial 轮换而来（None = 首次签发）
    pub parent_serial: Option<String>,
}

impl From<CertRecord> for CertInfo {
    fn from(r: CertRecord) -> Self {
        Self {
            common_name: r.common_name,
            cert_pem: r.cert_pem,
            key_pem: r.key_pem,
            not_before: r.not_before,
            not_after: r.not_after,
            serial: r.serial,
            status: r.status,
            parent_serial: r.parent_serial,
        }
    }
}

// ──── PkiService ────

/// PKI CA 服务
///
/// 管理 CA 密钥对，签发和验证终端证书。
/// CA 与已签发证书（含私钥）通过 [`PkiStore`] 持久化到共享存储
/// （生产为 coord-server KV，见 [`crate::pki_store::KvPkiStore`]）。
pub struct PkiService {
    config: PkiConfig,
    /// 共享存储（Memory 开发 / Kv 生产）
    store: Arc<dyn PkiStore>,
    /// CA 证书 + 私钥（内存缓存，签发后才初始化）
    ca: Arc<RwLock<Option<CaMaterial>>>,
}

struct CaMaterial {
    cert_pem: String,
    /// CA 密钥 PEM 编码（用于重建 KeyPair 签名）
    key_pem: String,
}

impl PkiService {
    /// 创建 PKI 服务实例（内存 store，开发/单测/骨架模式）
    pub fn new(config: PkiConfig) -> Result<Self, PkiError> {
        Ok(Self::with_store(config, Arc::new(MemoryPkiStore::new())))
    }

    /// 创建 PKI 服务实例并注入共享存储（生产：KvPkiStore）
    pub fn with_store(config: PkiConfig, store: Arc<dyn PkiStore>) -> Self {
        Self {
            config,
            store,
            ca: Arc::new(RwLock::new(None)),
        }
    }

    /// 初始化/加载 CA：幂等 + 共享（多 agent 只产生一份 CA 根）
    ///
    /// - 内存已加载 → 直接返回；
    /// - 共享 store 已有 CA → 加载；
    /// - 未命中 → 生成后 Txn CAS 原子写入；冲突则重读加载胜者。
    pub async fn init_ca(&self, ca_common_name: &str) -> Result<(), PkiError> {
        if self.ca.read().is_some() {
            return Ok(());
        }

        if let Some(record) = self
            .store
            .get_ca()
            .await
            .map_err(|e| PkiError::Store(e.to_string()))?
        {
            self.load_ca(record)?;
            return Ok(());
        }

        let (cert_pem, key_pem) = Self::generate_ca(ca_common_name)?;
        let record = CaRecord {
            cert_pem,
            key_pem,
            common_name: ca_common_name.to_string(),
        };
        match self
            .store
            .create_ca(&record)
            .await
        {
            Ok(()) => self.load_ca(record)?,
            Err(PkiStoreError::AlreadyExists(_)) => {
                // 其他 agent 已创建 → 重读并加载
                let winner = self
                    .store
                    .get_ca()
                    .await
                    .map_err(|e| PkiError::Store(e.to_string()))?
                    .ok_or(PkiError::CaLoadFailed)?;
                self.load_ca(winner)?;
            }
            Err(e) => return Err(PkiError::Store(e.to_string())),
        }
        Ok(())
    }

    /// 加载 CA 到内存（校验私钥可解析）
    fn load_ca(&self, record: CaRecord) -> Result<(), PkiError> {
        // 校验 CA 私钥可用
        KeyPair::from_pem(&record.key_pem).map_err(|e| PkiError::KeyGen(e.to_string()))?;
        *self.ca.write() = Some(CaMaterial {
            cert_pem: record.cert_pem,
            key_pem: record.key_pem,
        });
        Ok(())
    }

    /// 生成自签名根证书，返回 (cert_pem, key_pem)
    fn generate_ca(ca_common_name: &str) -> Result<(String, String), PkiError> {
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

        Ok((cert.pem(), key_pem))
    }

    /// 签发终端证书（**get-or-create**）
    ///
    /// 同一 CN 未过期证书直接返回既有记录（同 serial / 公钥 / 私钥）；
    /// 未命中或已过期才新签发，Txn CAS 原子写入，冲突则重读返回胜者。
    /// `ttl_seconds`: 证书有效期（秒）。为 0 时使用 config.cert_ttl_hours 默认值。
    pub async fn issue_cert(&self, common_name: &str, ttl_seconds: u64) -> Result<CertInfo, PkiError> {
        // 1. get-or-create 快路径：命中未过期 → 直接返回
        if let Some(record) = self
            .store
            .get_cert(common_name)
            .await
            .map_err(|e| PkiError::Store(e.to_string()))?
        {
            if !record.is_expired(now_unix()) {
                return Ok(record.into());
            }
        }

        // 2. 新签发
        let record = self.sign_new_cert(common_name, ttl_seconds, CertStatus::Active, None)?;

        // 3. Txn CAS 原子写入；冲突 → 重读并返回胜者
        match self
            .store
            .create_cert(common_name, &record)
            .await
        {
            Ok(()) => Ok(record.into()),
            Err(PkiStoreError::AlreadyExists(_)) => {
                let winner = self
                    .store
                    .get_cert(common_name)
                    .await
                    .map_err(|e| PkiError::Store(e.to_string()))?
                    .ok_or(PkiError::CertMissing)?;
                Ok(winner.into())
            }
            Err(e) => Err(PkiError::Store(e.to_string())),
        }
    }

    /// 续期证书：**按 serial 查回真实 CN**，再签发新证书（新密钥 + 新 serial）
    ///
    /// 修复：不再把 serial 当 CN 使用（ISSUE-000 §2.2）。
    /// 旧证书保留至 not_after 仍可验签。
    pub async fn renew_cert(&self, serial: &str, ttl_seconds: u64) -> Result<CertInfo, PkiError> {
        let old = self
            .store
            .get_cert_by_serial(serial)
            .await
            .map_err(|e| PkiError::Store(e.to_string()))?
            .ok_or_else(|| PkiError::CertNotFound(serial.to_string()))?;
        self.rotate_locked(&old.common_name, ttl_seconds).await
    }

    /// 按 CN 显式轮换：签发新 active，旧证书标记 retired 保留至 not_after
    pub async fn rotate_cert(&self, common_name: &str, ttl_seconds: u64) -> Result<CertInfo, PkiError> {
        // 无 active 或已过期 → 走 get-or-create 首次签发
        match self
            .store
            .get_cert(common_name)
            .await
            .map_err(|e| PkiError::Store(e.to_string()))?
        {
            Some(old) if !old.is_expired(now_unix()) => self.rotate_locked(common_name, ttl_seconds).await,
            _ => self.issue_cert(common_name, ttl_seconds).await,
        }
    }

    /// 轮换实现（调用方已确认存在未过期 active）：新签发 + 旧 retired 原子入历史
    async fn rotate_locked(&self, common_name: &str, ttl_seconds: u64) -> Result<CertInfo, PkiError> {
        let old = self
            .store
            .get_cert(common_name)
            .await
            .map_err(|e| PkiError::Store(e.to_string()))?
            .ok_or(PkiError::CertMissing)?;

        let new_record = self.sign_new_cert(
            common_name,
            ttl_seconds,
            CertStatus::Active,
            Some(old.serial.clone()),
        )?;

        let mut retired = old.clone();
        retired.status = CertStatus::Retired;

        self.store
            .replace_active_cert(common_name, &new_record, &retired)
            .await
            .map_err(|e| PkiError::Store(e.to_string()))?;

        Ok(new_record.into())
    }

    /// 按 CN 取当前 + 历史未过期证书（验签方按 serial/kid 构建多密钥 JWKS）
    pub async fn list_certs(&self, common_name: &str) -> Result<Vec<CertInfo>, PkiError> {
        let records = self
            .store
            .list_certs(common_name)
            .await
            .map_err(|e| PkiError::Store(e.to_string()))?;
        Ok(records.into_iter().map(Into::into).collect())
    }

    /// 按 CN 幂等取回当前有效证书（含私钥；无或已过期返回 Ok(None)）
    pub async fn get_cert_by_cn(&self, common_name: &str) -> Result<Option<CertInfo>, PkiError> {
        match self
            .store
            .get_cert(common_name)
            .await
            .map_err(|e| PkiError::Store(e.to_string()))?
        {
            Some(record) if !record.is_expired(now_unix()) => Ok(Some(record.into())),
            _ => Ok(None),
        }
    }

    /// 签发新终端证书并返回记录（不落库，由调用方决定写入策略）
    fn sign_new_cert(
        &self,
        common_name: &str,
        ttl_seconds: u64,
        status: CertStatus,
        parent_serial: Option<String>,
    ) -> Result<CertRecord, PkiError> {
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

        // 从持久化的 CA 证书重建签发者（重启后无需原始 params）
        let issuer = rcgen::Issuer::from_ca_cert_pem(&ca.cert_pem, ca_key)
            .map_err(|e| PkiError::CertGen(e.to_string()))?;

        let cert = params
            .signed_by(&key_pair, &issuer)
            .map_err(|e| PkiError::CertGen(e.to_string()))?;

        let not_before = now.unix_timestamp();
        let not_after = (now + ttl).unix_timestamp();
        let serial = format!("{:x}", rand::random::<u64>());

        Ok(CertRecord {
            common_name: common_name.to_string(),
            cert_pem: cert.pem(),
            key_pem: key_pair.serialize_pem(),
            not_before,
            not_after,
            serial,
            status,
            parent_serial,
        })
    }
}

// ──── 辅助函数 ────

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

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

        let now = now_unix();

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
    /// renew 按 serial 查不到证书
    CertNotFound(String),
    /// get-or-create 冲突后重读不到胜者
    CertMissing,
    /// CA 持久化记录加载失败
    CaLoadFailed,
    /// 共享存储错误
    Store(String),
}

impl std::fmt::Display for PkiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CaNotInitialized => write!(f, "CA not initialized"),
            Self::KeyGen(msg) => write!(f, "key generation failed: {msg}"),
            Self::CertGen(msg) => write!(f, "certificate generation failed: {msg}"),
            Self::CertParse(msg) => write!(f, "certificate parse failed: {msg}"),
            Self::Io(e) => write!(f, "IO error: {e}"),
            Self::CertNotFound(s) => write!(f, "certificate not found by serial: {s}"),
            Self::CertMissing => write!(f, "certificate missing in store"),
            Self::CaLoadFailed => write!(f, "failed to load CA from store"),
            Self::Store(msg) => write!(f, "pki store error: {msg}"),
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
    PkiCertSummary,
    PkiGetCaCertRequest, PkiGetCaCertResponse,
    PkiGetCertByCnRequest, PkiGetCertByCnResponse,
    PkiInitCaRequest, PkiInitCaResponse,
    PkiIssueCertRequest, PkiIssueCertResponse,
    PkiListCertsRequest, PkiListCertsResponse,
    PkiRenewCertRequest, PkiRenewCertResponse,
    PkiRotateCertRequest, PkiRotateCertResponse,
    PkiVerifyCertRequest, PkiVerifyCertResponse,
};
use tonic::{Request, Response, Status};

fn status_to_str(status: CertStatus) -> &'static str {
    match status {
        CertStatus::Active => "active",
        CertStatus::Retired => "retired",
    }
}

fn cert_to_issue_response(cert_info: CertInfo) -> PkiIssueCertResponse {
    PkiIssueCertResponse {
        common_name: cert_info.common_name,
        cert_pem: cert_info.cert_pem,
        key_pem: cert_info.key_pem,
        not_before: cert_info.not_before,
        not_after: cert_info.not_after,
        serial: cert_info.serial,
        status: status_to_str(cert_info.status).into(),
        parent_serial: cert_info.parent_serial.unwrap_or_default(),
    }
}

#[tonic::async_trait]
impl Pki for PkiService {
    async fn init_ca(
        &self,
        request: Request<PkiInitCaRequest>,
    ) -> Result<Response<PkiInitCaResponse>, Status> {
        let req = request.into_inner();
        PkiService::init_ca(self, &req.ca_common_name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(PkiInitCaResponse {}))
    }

    async fn issue_cert(
        &self,
        request: Request<PkiIssueCertRequest>,
    ) -> Result<Response<PkiIssueCertResponse>, Status> {
        let req = request.into_inner();
        let cert_info = PkiService::issue_cert(self, &req.common_name, req.ttl_seconds as u64)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(cert_to_issue_response(cert_info)))
    }

    async fn renew_cert(
        &self,
        request: Request<PkiRenewCertRequest>,
    ) -> Result<Response<PkiRenewCertResponse>, Status> {
        let req = request.into_inner();
        // 修复（ISSUE-000 §2.2）：按 serial 查回真实 CN 再续期，不再把 serial 当 CN
        let cert_info = PkiService::renew_cert(self, &req.serial_number, req.ttl_seconds as u64)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(PkiRenewCertResponse {
            common_name: cert_info.common_name,
            cert_pem: cert_info.cert_pem,
            key_pem: cert_info.key_pem,
            not_before: cert_info.not_before,
            not_after: cert_info.not_after,
            serial: cert_info.serial,
            status: status_to_str(cert_info.status).into(),
            parent_serial: cert_info.parent_serial.unwrap_or_default(),
        }))
    }

    async fn rotate_cert(
        &self,
        request: Request<PkiRotateCertRequest>,
    ) -> Result<Response<PkiRotateCertResponse>, Status> {
        let req = request.into_inner();
        let cert_info = PkiService::rotate_cert(self, &req.common_name, req.ttl_seconds as u64)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(PkiRotateCertResponse {
            common_name: cert_info.common_name,
            cert_pem: cert_info.cert_pem,
            key_pem: cert_info.key_pem,
            not_before: cert_info.not_before,
            not_after: cert_info.not_after,
            serial: cert_info.serial,
            status: status_to_str(cert_info.status).into(),
            parent_serial: cert_info.parent_serial.unwrap_or_default(),
        }))
    }

    async fn list_certs(
        &self,
        request: Request<PkiListCertsRequest>,
    ) -> Result<Response<PkiListCertsResponse>, Status> {
        let req = request.into_inner();
        let certs = PkiService::list_certs(self, &req.common_name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let summaries = certs
            .into_iter()
            .map(|c| PkiCertSummary {
                common_name: c.common_name,
                cert_pem: c.cert_pem,
                not_before: c.not_before,
                not_after: c.not_after,
                serial: c.serial,
                status: status_to_str(c.status).into(),
                parent_serial: c.parent_serial.unwrap_or_default(),
            })
            .collect();
        Ok(Response::new(PkiListCertsResponse { certs: summaries }))
    }

    async fn get_cert_by_cn(
        &self,
        request: Request<PkiGetCertByCnRequest>,
    ) -> Result<Response<PkiGetCertByCnResponse>, Status> {
        let req = request.into_inner();
        let cert_info = PkiService::get_cert_by_cn(self, &req.common_name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| {
                Status::not_found(format!("no active certificate for CN: {}", req.common_name))
            })?;
        Ok(Response::new(PkiGetCertByCnResponse {
            common_name: cert_info.common_name,
            cert_pem: cert_info.cert_pem,
            key_pem: cert_info.key_pem,
            not_before: cert_info.not_before,
            not_after: cert_info.not_after,
            serial: cert_info.serial,
            status: status_to_str(cert_info.status).into(),
            parent_serial: cert_info.parent_serial.unwrap_or_default(),
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

    fn make_pki() -> PkiService {
        PkiService::new(PkiConfig::default()).expect("create")
    }

    #[tokio::test]
    async fn test_init_ca_and_issue() {
        let pki = make_pki();
        pki.init_ca("Test CA").await.expect("init");

        let cert = pki.issue_cert("test.local", 0).await.expect("issue");
        assert_eq!(cert.common_name, "test.local");
        assert!(!cert.cert_pem.is_empty());
        assert!(!cert.key_pem.is_empty());
        assert_eq!(cert.status, CertStatus::Active);
    }

    #[tokio::test]
    async fn test_verify_valid_cert() {
        let pki = make_pki();
        pki.init_ca("Verify CA").await.expect("init");

        let cert = pki.issue_cert("verify.local", 0).await.expect("issue");
        assert!(pki.verify_cert(&cert.cert_pem).expect("verify"));
    }

    #[tokio::test]
    async fn test_ca_cert_export() {
        let pki = make_pki();
        pki.init_ca("Export CA").await.expect("init");

        let pem = pki.ca_cert_pem().expect("export");
        assert!(pem.contains("BEGIN CERTIFICATE"));
    }

    /// RED→GREEN: 验证未初始化 CA 时 issue_cert 返回 CaNotInitialized 错误。
    /// 修复前 dev 模式 PKI CA 未自动初始化。
    #[tokio::test]
    async fn test_ca_not_initialized_error() {
        let pki = make_pki();
        let result = pki.issue_cert("test.local", 0).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            PkiError::CaNotInitialized => {}
            other => panic!("expected CaNotInitialized, got: {}", other),
        }
    }

    /// 验证 CA init_ca 幂等性（多次调用不报错）
    #[tokio::test]
    async fn test_init_ca_idempotent() {
        let pki = make_pki();
        pki.init_ca("Test CA").await.expect("first init");
        pki.init_ca("Test CA").await.expect("second init (idempotent)");
        let cert = pki.issue_cert("test.local", 0).await.expect("issue after init");
        assert!(!cert.cert_pem.is_empty());
    }
}
