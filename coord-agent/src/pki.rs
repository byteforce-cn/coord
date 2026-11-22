// coord-agent: PKI CA 自动签发/轮换服务
//
// PKI — CA 私钥受根密钥保护，为 mTLS 签发短期证书。
//
// 核心能力：
// - 初始化 CA（自签名根证书，**持久化到共享 KV**，重启/多 agent 共享同一 CA 根）
// - **按 CN 幂等取回（get-or-create）**：同一 CN 未过期证书直接返回既有记录
// - **到期即换新**：IssueCert / RotateCert 对过期记录走替换路径重签（新 serial / 新密钥），
//   不返回过期记录；RenewCert(serial) 为按序列号恢复入口
// - 证书轮换（rotate / renew，旧证书保留至 not_after 供验签）
// - 证书验证（链式验证）
// - 证书列表（按 CN 取 active + 历史，验签方按 serial/kid 构建多密钥 JWKS）
//
// 使用 rcgen 生成 X.509 证书，x509-parser 解析验证。

use std::net::IpAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use base64;
use parking_lot::RwLock;
use rcgen::string::Ia5String;
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, KeyUsagePurpose, SanType};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;

use crate::pki_store::{
    CaRecord, CertRecord, CertStatus, CertUsage, CrlState, MemoryPkiStore, PkiStore, PkiStoreError,
    RevocationReason, RevocationRecord,
};
use crate::services::transit_key::{
    CreateKeySpec, ImportKeySpec, KeyError, KeyType, MaterialFormat, TransitKeyRegistry,
};

// ──── PkiConfig ────

/// PKI 服务配置
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct PkiConfig {
    /// 证书 TTL（小时，默认 24）
    #[serde(default = "default_cert_ttl_hours")]
    pub cert_ttl_hours: u32,

    /// 到期告警窗口（小时，默认 6）：剩余有效期 < 该窗口的 active 证书计入
    /// `coord_agent_pki_certs_expiring_soon`（G-PKI-2）
    #[serde(default = "default_expiry_warn_hours")]
    pub expiry_warn_hours: i64,

    /// CRL 的 next_update 窗口（小时，默认 24）：`GetCrl` 返回的 CRL 只承诺到
    /// this_update + 该值；消费方须在此之前重新拉取。
    #[serde(default = "default_crl_ttl_hours")]
    pub crl_ttl_hours: u32,

    /// 证书到期后的历史保留期（小时，默认 24）：`Tidy` 回收更早的记录；
    /// 吊销记录不回收（见 ADR-0020）。
    #[serde(default = "default_history_retention_hours")]
    pub history_retention_hours: u32,

    /// CA 证书路径（用于持久化）
    #[serde(default)]
    pub ca_cert_path: Option<PathBuf>,

    /// CA 私钥路径（用于持久化）
    #[serde(default)]
    pub ca_key_path: Option<PathBuf>,
}

fn default_cert_ttl_hours() -> u32 {
    24
}

fn default_expiry_warn_hours() -> i64 {
    6
}

fn default_crl_ttl_hours() -> u32 {
    24
}

fn default_history_retention_hours() -> u32 {
    24
}

impl Default for PkiConfig {
    fn default() -> Self {
        Self {
            cert_ttl_hours: 24,
            expiry_warn_hours: 6,
            crl_ttl_hours: 24,
            history_retention_hours: 24,
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
    /// 托管私钥的命名密钥名（材料不在 PKI 记录里，ADR-0019 D6）
    pub key_name: String,
    /// 签名使用的密钥版本
    pub key_version: u32,
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
    /// 证书用途（KU/EKU profile）
    pub usage: CertUsage,
}

impl From<CertRecord> for CertInfo {
    fn from(r: CertRecord) -> Self {
        Self {
            common_name: r.common_name,
            cert_pem: r.cert_pem,
            key_name: r.key_name,
            key_version: r.key_version,
            not_before: r.not_before,
            not_after: r.not_after,
            serial: r.serial,
            status: r.status,
            parent_serial: r.parent_serial,
            usage: r.usage,
        }
    }
}

/// 按 serial 的状态查询结果（`GetCertStatus`）。
#[derive(Debug, Clone)]
pub struct CertStatusInfo {
    /// 请求的 serial（原样回显）
    pub serial: String,
    /// 证书内嵌的 X.509 serial
    pub cert_serial: String,
    pub common_name: String,
    /// active / retired / expired / revoked
    pub status: String,
    pub not_before: i64,
    pub not_after: i64,
    /// 吊销记录（未吊销为 None）
    pub revocation: Option<RevocationRecord>,
}

/// 生成好的 CRL 及编号/时间窗口。
#[derive(Debug, Clone)]
pub struct CrlMaterial {
    pub der: Vec<u8>,
    pub pem: String,
    pub crl_number: u64,
    pub this_update: i64,
    pub next_update: i64,
}

/// `Tidy` 的执行结果。
#[derive(Debug, Clone)]
pub struct TidyResult {
    pub retention_seconds: i64,
    pub removed_certs: u64,
}

// ──── SanSpec ────

/// 证书 SAN 集合（已归一化：排序 + 去重，作为幂等比较口径）。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SanSpec {
    /// DNS SAN（IA5String 字符集）
    pub dns_names: Vec<String>,
    /// IP SAN（标准字面量：IPv4 / IPv6）
    pub ip_addresses: Vec<String>,
}

impl SanSpec {
    pub fn new(dns_names: Vec<String>, ip_addresses: Vec<String>) -> Self {
        Self {
            dns_names,
            ip_addresses,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.dns_names.is_empty() && self.ip_addresses.is_empty()
    }

    /// 校验并归一化（排序 + 去重）。
    ///
    /// 校验在签发前完成：非法 DNS/IP 必须返回 `INVALID_ARGUMENT`，
    /// 不能等 rcgen 在 `spawn_blocking` 里报一个与调用方参数无关的
    /// `certificate generation failed`。
    fn normalized(mut self) -> Result<Self, PkiError> {
        for name in &self.dns_names {
            validate_dns_name(name)?;
            // 提前验证 IA5String 可构造，确保签发阶段不会再遇到参数错误
            Ia5String::try_from(name.as_str())
                .map_err(|_| PkiError::InvalidArgument(format!("invalid DNS name: {name:?}")))?;
        }
        for ip in &self.ip_addresses {
            IpAddr::from_str(ip)
                .map_err(|_| PkiError::InvalidArgument(format!("invalid IP address: {ip:?}")))?;
        }
        self.dns_names.sort();
        self.dns_names.dedup();
        self.ip_addresses.sort();
        self.ip_addresses.dedup();
        Ok(self)
    }
}

/// CN 派生为 DNS SAN 时的语法约束。
///
/// 这是「默认开箱即用」的判据：CN 非法时**不得**静默签出无 SAN 证书；
/// 调用方须显式传 `dns_names`（CN 仅作 subject）。
fn validate_dns_name(name: &str) -> Result<(), PkiError> {
    let valid = !name.is_empty()
        && name.len() <= 253
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '.' | '_' | '*'));
    if valid {
        Ok(())
    } else {
        Err(PkiError::InvalidArgument(format!(
            "common_name {name:?} is not a valid DNS name; pass dns_names explicitly"
        )))
    }
}

fn derive_san_from_cn(common_name: &str) -> Result<SanSpec, PkiError> {
    validate_dns_name(common_name)?;
    Ok(SanSpec {
        dns_names: vec![common_name.to_string()],
        ip_addresses: Vec::new(),
    })
}

/// 选定本次签发的生效 SAN：
/// - 显式请求非空 → 用请求（IssueCert / RotateCert）；
/// - 否则若可继承且记录已有 SAN → 继承（RenewCert / RotateCert 未传 SAN）；
/// - 否则由 CN 派生（含升级前的无 SAN 旧记录）。
fn effective_san(
    common_name: &str,
    requested: &SanSpec,
    inherit_existing: bool,
    existing: Option<&CertRecord>,
) -> Result<SanSpec, PkiError> {
    if !requested.is_empty() {
        return Ok(requested.clone());
    }
    if inherit_existing {
        if let Some(record) = existing {
            if !record.dns_names.is_empty() || !record.ip_addresses.is_empty() {
                return Ok(SanSpec {
                    dns_names: record.dns_names.clone(),
                    ip_addresses: record.ip_addresses.clone(),
                });
            }
        }
    }
    derive_san_from_cn(common_name)
}

fn effective_ttl_seconds(ttl_seconds: u64, default_ttl_hours: u32) -> u64 {
    if ttl_seconds > 0 {
        ttl_seconds
    } else {
        default_ttl_hours as u64 * 3600
    }
}

fn record_ttl_seconds(record: &CertRecord) -> u64 {
    record.not_after.saturating_sub(record.not_before) as u64
}

// ──── PkiService ────

/// CA 密钥档案名（与 transit 共用同一注册表，ADR-0019 D1/D2）
pub const CA_KEY_NAME: &str = "pki/ca";

/// PKI CA 服务
///
/// 管理 CA 与终端证书；私钥材料**不落 PKI store**，而是由 transit 命名密钥
/// 档案托管（KEK 包裹，ADR-0019 D6）。证书记录只保留 `key_name` / `key_version`。
pub struct PkiService {
    config: PkiConfig,
    /// 共享存储（Memory 开发 / Kv 生产）
    store: Arc<dyn PkiStore>,
    /// 命名密钥档案（与 transit 共用同一实例）
    keys: Arc<TransitKeyRegistry>,
    /// CA 证书 + 密钥引用（内存缓存，签发后才初始化）
    ca: Arc<RwLock<Option<CaMaterial>>>,
}

struct CaMaterial {
    cert_pem: String,
    key_name: String,
    key_version: u32,
}

impl Clone for CaMaterial {
    fn clone(&self) -> Self {
        Self {
            cert_pem: self.cert_pem.clone(),
            key_name: self.key_name.clone(),
            key_version: self.key_version,
        }
    }
}

impl PkiService {
    /// 创建 PKI 服务实例（内存 store + 内存密钥档案，开发/单测/骨架模式）
    pub fn new(config: PkiConfig) -> Result<Self, PkiError> {
        Self::with_store(config, Arc::new(MemoryPkiStore::new()))
    }

    /// 注入共享存储；密钥档案使用进程内随机 KEK（**非生产**，重启即丢）
    pub fn with_store(config: PkiConfig, store: Arc<dyn PkiStore>) -> Result<Self, PkiError> {
        let keys = TransitKeyRegistry::in_memory().map_err(|e| PkiError::Key(e.to_string()))?;
        Ok(Self::with_store_and_keys(config, store, Arc::new(keys)))
    }

    /// 生产装配：PKI store + 与 transit 共用的命名密钥档案
    pub fn with_store_and_keys(
        config: PkiConfig,
        store: Arc<dyn PkiStore>,
        keys: Arc<TransitKeyRegistry>,
    ) -> Self {
        Self {
            config,
            store,
            keys,
            ca: Arc::new(RwLock::new(None)),
        }
    }

    /// 初始化/加载 CA：幂等 + 共享（多 agent 只产生一份 CA 根）
    ///
    /// - 内存已加载 → 直接返回；
    /// - 共享 store 已有 CA → 加载（校验密钥档案里的 CA 材料可解）；
    /// - 未命中 → 生成 CA 密钥并导入 `pki/ca`（never_exportable），再 CAS 写 CaRecord；
    ///   并发时重读胜者。密钥已存在但 CaRecord 缺失时，用既有密钥重建自签证书。
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
            self.load_ca(record).await?;
            return Ok(());
        }

        let (generated_cert, key_der) = {
            let cn = ca_common_name.to_string();
            tokio::task::spawn_blocking(move || Self::generate_ca(&cn))
                .await
                .map_err(|e| PkiError::KeyGen(format!("ca generate task join: {e}")))??
        };

        let (archive, cert_pem) = match self
            .keys
            .import_key_with_flags(
                ImportKeySpec {
                    name: CA_KEY_NAME.to_string(),
                    key_type: KeyType::EcdsaP256,
                    material: key_der,
                    format: Some(MaterialFormat::Der),
                    exportable: false,
                },
                true,
            )
            .await
        {
            Ok(archive) => (archive, generated_cert),
            Err(KeyError::AlreadyExists(_)) => {
                // 密钥已存在（并发 init / 上次崩溃）：证书必须由既有密钥重签，
                // 否则 CaRecord 指向的公钥与证书不匹配。
                let archive = self
                    .keys
                    .read_key(CA_KEY_NAME)
                    .await
                    .map_err(|e| PkiError::Key(e.to_string()))?;
                let cn = ca_common_name.to_string();
                let pem = self
                    .keys
                    .private_key_pem(CA_KEY_NAME, archive.latest_version)
                    .await
                    .map_err(|e| PkiError::Key(e.to_string()))?;
                let cert = tokio::task::spawn_blocking(move || Self::self_signed_ca(&cn, &pem))
                    .await
                    .map_err(|e| PkiError::KeyGen(format!("ca self-sign task join: {e}")))??;
                (archive, cert)
            }
            Err(e) => return Err(PkiError::Key(e.to_string())),
        };
        let key_version = archive.latest_version;

        let record = CaRecord {
            cert_pem,
            key_name: CA_KEY_NAME.to_string(),
            key_version,
            common_name: ca_common_name.to_string(),
        };
        match self.store.create_ca(&record).await {
            Ok(()) => self.load_ca(record).await?,
            Err(PkiStoreError::AlreadyExists(_)) => {
                let winner = self
                    .store
                    .get_ca()
                    .await
                    .map_err(|e| PkiError::Store(e.to_string()))?
                    .ok_or(PkiError::CaLoadFailed)?;
                self.load_ca(winner).await?;
            }
            Err(e) => return Err(PkiError::Store(e.to_string())),
        }
        Ok(())
    }

    /// 加载 CA 到内存（校验密钥档案中的 CA 材料可解）
    async fn load_ca(&self, record: CaRecord) -> Result<(), PkiError> {
        let pem = self
            .keys
            .private_key_pem(&record.key_name, record.key_version)
            .await
            .map_err(|e| PkiError::Key(e.to_string()))?;
        KeyPair::from_pem(&pem).map_err(|e| PkiError::KeyGen(e.to_string()))?;
        *self.ca.write() = Some(CaMaterial {
            cert_pem: record.cert_pem,
            key_name: record.key_name,
            key_version: record.key_version,
        });
        Ok(())
    }

    /// 生成自签名根证书，返回 (cert_pem, PKCS#8 DER)
    fn generate_ca(ca_common_name: &str) -> Result<(String, Vec<u8>), PkiError> {
        let key_pair = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)
            .map_err(|e| PkiError::KeyGen(e.to_string()))?;
        let cert = Self::ca_params(ca_common_name)?
            .self_signed(&key_pair)
            .map_err(|e| PkiError::CertGen(e.to_string()))?;
        Ok((cert.pem(), key_pair.serialize_der()))
    }

    /// 用既有 CA 私钥重建自签证书（并发 init / CaRecord 缺失时）
    fn self_signed_ca(ca_common_name: &str, key_pem: &str) -> Result<String, PkiError> {
        let key_pair = KeyPair::from_pem(key_pem).map_err(|e| PkiError::KeyGen(e.to_string()))?;
        let cert = Self::ca_params(ca_common_name)?
            .self_signed(&key_pair)
            .map_err(|e| PkiError::CertGen(e.to_string()))?;
        Ok(cert.pem())
    }

    fn ca_params(ca_common_name: &str) -> Result<CertificateParams, PkiError> {
        let mut params = CertificateParams::default();
        params
            .distinguished_name
            .push(DnType::CommonName, ca_common_name);
        params
            .distinguished_name
            .push(DnType::OrganizationName, "Coord PKI");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let now = OffsetDateTime::now_utc();
        params.not_before = now;
        params.not_after = now + time::Duration::days(3650);
        Ok(params)
    }

    /// 版本化 CAS 替换的最大重试次数（仅在并发替换/并发创建冲突时消耗）。
    const MAX_REPLACE_ATTEMPTS: usize = 5;

    /// 签发终端证书（**get-or-create，到期即换新，参数一致才幂等**）
    ///
    /// 同一 CN 且 SAN / TTL 与当前 active 记录一致 → 返回既有记录（同 serial / 私钥）；
    /// 参数不一致或已过期 → 走替换路径重签（新 serial / 新密钥），旧记录 retired。
    /// `ttl_seconds`: 有效期（秒）。为 0 时使用 config.cert_ttl_hours。
    pub async fn issue_cert(
        &self,
        common_name: &str,
        ttl_seconds: u64,
    ) -> Result<CertInfo, PkiError> {
        self.issue_cert_with_profile(
            common_name,
            ttl_seconds,
            SanSpec::default(),
            CertUsage::Both,
        )
        .await
    }

    /// 同 [`Self::issue_cert`]，显式指定 SAN；usage 默认 `Both`。
    ///
    /// SAN 两者皆空 ⇒ 将 CN 派生为 DNS SAN；CN 不是合法 DNS 名时返回
    /// `InvalidArgument`（不静默签出无 SAN 证书）。
    pub async fn issue_cert_with_san(
        &self,
        common_name: &str,
        ttl_seconds: u64,
        san: SanSpec,
    ) -> Result<CertInfo, PkiError> {
        self.issue_cert_with_profile(common_name, ttl_seconds, san, CertUsage::Both)
            .await
    }

    /// 显式指定 SAN 与证书用途（KU/EKU 精确写入）。
    pub async fn issue_cert_with_profile(
        &self,
        common_name: &str,
        ttl_seconds: u64,
        san: SanSpec,
        usage: CertUsage,
    ) -> Result<CertInfo, PkiError> {
        self.ensure_cert(
            common_name,
            ttl_seconds,
            false,
            san.normalized()?,
            false,
            Some(usage),
        )
        .await
    }

    /// 续期证书：**按 serial 查回真实 CN**，再替换出一个新证书（新密钥 + 新 serial）
    ///
    /// 按序列号恢复入口：serial 可来自 active 或历史（含已过期）记录。
    /// 旧证书保留至 not_after 供验签方按 serial/kid 取用。
    pub async fn renew_cert(&self, serial: &str, ttl_seconds: u64) -> Result<CertInfo, PkiError> {
        let normalized = normalize_serial(serial)?;
        let old = self
            .store
            .get_cert_by_serial(&normalized)
            .await
            .map_err(|e| PkiError::Store(e.to_string()))?
            .ok_or_else(|| PkiError::CertNotFound(serial.to_string()))?;
        if old.is_external() {
            return Err(PkiError::FailedPrecondition(format!(
                "certificate {serial} uses a caller-held key; renew requires a new CSR via Pki/SignCsr"
            )));
        }
        // 吊销是永久事实：按已吊销 serial 续期等于复活该身份。
        if self.is_record_revoked(&old).await? {
            return Err(PkiError::FailedPrecondition(format!(
                "certificate {serial} is revoked; renew would resurrect it — RotateCert issues a \
                 new identity explicitly"
            )));
        }
        // 同一个 CN 的「当前身份」已吊销时，续期任意历史 serial 也会隐式换新身份；
        // 必须走显式 RotateCert。
        if let Some(current) = self
            .store
            .get_cert(&old.common_name)
            .await
            .map_err(|e| PkiError::Store(e.to_string()))?
        {
            if self.is_record_revoked(&current).await? {
                return Err(PkiError::FailedPrecondition(format!(
                    "current certificate for CN {} is revoked (serial {}); renew would resurrect \
                     the identity — use RotateCert",
                    current.common_name, current.serial
                )));
            }
        }
        // RenewCert 无 SAN / usage 入参：继承旧记录（无 SAN 的旧记录由 CN 派生）。
        self.ensure_cert(
            &old.common_name,
            ttl_seconds,
            true,
            SanSpec::default(),
            true,
            None,
        )
        .await
    }

    /// 按 CN 显式轮换：签发新 active，旧记录（含已过期）retired 入历史
    pub async fn rotate_cert(
        &self,
        common_name: &str,
        ttl_seconds: u64,
    ) -> Result<CertInfo, PkiError> {
        self.rotate_cert_with_san(common_name, ttl_seconds, SanSpec::default())
            .await
    }

    /// 同 [`Self::rotate_cert`]，显式指定 SAN。
    ///
    /// SAN 两者皆空 ⇒ 继承当前 active 记录的 SAN（无既有记录时由 CN 派生），
    /// 避免「轮换后丢 SAN」。
    pub async fn rotate_cert_with_san(
        &self,
        common_name: &str,
        ttl_seconds: u64,
        san: SanSpec,
    ) -> Result<CertInfo, PkiError> {
        self.rotate_cert_with_profile(common_name, ttl_seconds, san, None)
            .await
    }

    /// 显式轮换；`usage = None` 继承当前记录的用途（无记录时 `Both`）。
    pub async fn rotate_cert_with_profile(
        &self,
        common_name: &str,
        ttl_seconds: u64,
        san: SanSpec,
        usage: Option<CertUsage>,
    ) -> Result<CertInfo, PkiError> {
        self.ensure_cert(
            common_name,
            ttl_seconds,
            true,
            san.normalized()?,
            true,
            usage,
        )
        .await
    }

    /// 统一的签发/轮换实现。
    ///
    /// - `force_rotate = false`（IssueCert）：当前记录未过期**且 SAN/TTL 与本次请求一致**
    ///   → 幂等返回；参数不一致或已过期 → 替换重签。
    /// - `force_rotate = true`（RotateCert / RenewCert）：无条件替换出新 active。
    /// - `inherit_existing_san`：请求未给 SAN 时是否继承现有记录（Renew/未显式传 SAN 的
    ///   Rotate）；false 时由 CN 派生（IssueCert 的默认语义）。
    ///
    /// 三条路径的并发安全：创建用 CAS Version==0；替换用「读到的 key version」做
    /// CAS 比较（见 [`PkiStore::replace_active_cert_cas`]）。冲突即重读，按最新状态
    /// 重新决策（未过期则返回胜者；仍过期/仍缺失则重试），保证不返回过期记录、无丢更新。
    async fn ensure_cert(
        &self,
        common_name: &str,
        ttl_seconds: u64,
        force_rotate: bool,
        requested_san: SanSpec,
        inherit_existing_san: bool,
        usage: Option<CertUsage>,
    ) -> Result<CertInfo, PkiError> {
        let want_ttl = effective_ttl_seconds(ttl_seconds, self.config.cert_ttl_hours);
        for _ in 0..Self::MAX_REPLACE_ATTEMPTS {
            match self
                .store
                .get_cert_with_version(common_name)
                .await
                .map_err(|e| PkiError::Store(e.to_string()))?
            {
                Some((record, version)) => {
                    // 外部密钥证书没有 coord 侧私钥可轮换：Renew/Rotate 必须改走 SignCsr。
                    if force_rotate && record.is_external() {
                        return Err(PkiError::FailedPrecondition(format!(
                            "certificate for CN {common_name} uses a caller-held key; \
                             renew/rotate requires a new CSR via Pki/SignCsr"
                        )));
                    }
                    // 已吊销的当前身份不得被 get-or-create 隐式重签（ADR-0020）；
                    // RotateCert（force_rotate=true）是显式换新身份的路径。
                    if !force_rotate && self.is_record_revoked(&record).await? {
                        return Err(PkiError::FailedPrecondition(format!(
                            "certificate for CN {common_name} is revoked (serial {}); \
                             IssueCert will not re-issue implicitly — use RotateCert for a new \
                             identity or SignCsr with a new CN",
                            record.serial
                        )));
                    }
                    let want_usage = usage.unwrap_or(record.usage);
                    // 幂等快路径：未过期 + 非强制轮换 + 同一密钥归属 + SAN/TTL/usage 一致。
                    // 外部密钥记录不能作为 managed 签发的幂等命中。
                    if !force_rotate && !record.is_expired(now_unix()) && !record.is_external() {
                        let san = effective_san(common_name, &requested_san, false, Some(&record))?;
                        if san.dns_names == record.dns_names
                            && san.ip_addresses == record.ip_addresses
                            && record_ttl_seconds(&record) == want_ttl
                            && record.usage == want_usage
                        {
                            return Ok(record.into());
                        }
                    }
                    // 替换路径：参数不一致 / 过期 / 强制轮换 → 新签 + 版本化 CAS 替换
                    let san = effective_san(
                        common_name,
                        &requested_san,
                        inherit_existing_san,
                        Some(&record),
                    )?;
                    let (key_name, key_version) = self.ensure_leaf_key(common_name, false).await?;
                    let new_record = self
                        .sign_new_cert_blocking(
                            common_name,
                            ttl_seconds,
                            san,
                            want_usage,
                            CertStatus::Active,
                            Some(record.serial.clone()),
                            key_name,
                            key_version,
                        )
                        .await?;
                    let mut retired = record.clone();
                    retired.status = CertStatus::Retired;
                    match self
                        .store
                        .replace_active_cert_cas(common_name, version, &new_record, &retired)
                        .await
                    {
                        Ok(()) => return Ok(new_record.into()),
                        // 并发替换：重读后按新状态重试（不丢更新）
                        Err(PkiStoreError::VersionConflict(_)) => continue,
                        Err(e) => return Err(PkiError::Store(e.to_string())),
                    }
                }
                // 首次签发：CAS Version==0 原子创建
                None => {
                    let san =
                        effective_san(common_name, &requested_san, inherit_existing_san, None)?;
                    let want_usage = usage.unwrap_or(CertUsage::Both);
                    let (key_name, key_version) = self.ensure_leaf_key(common_name, true).await?;
                    let new_record = self
                        .sign_new_cert_blocking(
                            common_name,
                            ttl_seconds,
                            san,
                            want_usage,
                            CertStatus::Active,
                            None,
                            key_name,
                            key_version,
                        )
                        .await?;
                    match self.store.create_cert(common_name, &new_record).await {
                        Ok(()) => return Ok(new_record.into()),
                        // 并发创建：重读（未过期即返回胜者；过期则转替换路径）
                        Err(PkiStoreError::AlreadyExists(_)) => continue,
                        Err(e) => return Err(PkiError::Store(e.to_string())),
                    }
                }
            }
        }
        Err(PkiError::Store(format!(
            "replace attempts exhausted for CN {common_name} (persistent concurrent replace)"
        )))
    }

    /// 按 CN 取当前 + 历史未过期证书（验签方按 serial/kid 构建多密钥 JWKS）
    pub async fn list_certs(&self, common_name: &str) -> Result<Vec<CertInfo>, PkiError> {
        let records = self
            .store
            .list_certs(common_name)
            .await
            .map_err(|e| PkiError::Store(e.to_string()))?;
        let revoked: std::collections::HashSet<String> = self
            .store
            .list_revocations()
            .await
            .map_err(|e| PkiError::Store(e.to_string()))?
            .into_iter()
            .map(|r| r.cert_serial)
            .collect();
        let mut seen = std::collections::HashSet::new();
        let mut out = Vec::with_capacity(records.len());
        for record in records {
            if !seen.insert(record.serial.clone()) {
                continue;
            }
            let mut info: CertInfo = record.clone().into();
            if revoked.contains(&record_cert_serial(&record)) {
                info.status = CertStatus::Revoked;
            }
            out.push(info);
        }
        Ok(out)
    }

    /// 按 CN 幂等取回当前有效证书（无 / 已过期 / 已吊销 ⇒ Ok(None)）
    pub async fn get_cert_by_cn(&self, common_name: &str) -> Result<Option<CertInfo>, PkiError> {
        match self
            .store
            .get_cert(common_name)
            .await
            .map_err(|e| PkiError::Store(e.to_string()))?
        {
            Some(record) if !record.is_expired(now_unix()) => {
                if self.is_record_revoked(&record).await? {
                    return Ok(None);
                }
                Ok(Some(record.into()))
            }
            _ => Ok(None),
        }
    }

    /// 按 serial 只读取回（含 retired / expired；无记录 ⇒ None，不含私钥材料）。
    ///
    /// 与 `RenewCert` 的「按 serial 还原真实 CN」共用同一存储查找；本方法没有副作用。
    pub async fn get_cert_by_serial(&self, serial: &str) -> Result<Option<CertInfo>, PkiError> {
        let normalized = normalize_serial(serial)?;
        let record = self
            .store
            .get_cert_by_serial(&normalized)
            .await
            .map_err(|e| PkiError::Store(e.to_string()))?;
        match record {
            Some(record) => {
                let mut info: CertInfo = record.clone().into();
                if self.is_record_revoked(&record).await? {
                    info.status = CertStatus::Revoked;
                }
                Ok(Some(info))
            }
            None => Ok(None),
        }
    }

    /// 证书当前是否已被吊销（吊销账本按证书内嵌 serial 索引）。
    async fn is_record_revoked(&self, record: &CertRecord) -> Result<bool, PkiError> {
        let cert_serial = record_cert_serial(record);
        Ok(self
            .store
            .get_revocation(&cert_serial)
            .await
            .map_err(|e| PkiError::Store(e.to_string()))?
            .is_some())
    }

    /// 吊销证书（幂等）。
    ///
    /// `serial` 接受签发记录里的 serial（IssueCert 响应）或证书内嵌的 X.509 serial。
    /// 账本以**证书内嵌 serial** 为键：CRL 与第三方验签方看到的是同一个值。
    /// 重复吊销返回首次记录（保留原 reason / revoked_at / by）。
    pub async fn revoke_cert(
        &self,
        serial: &str,
        reason: RevocationReason,
        by: &str,
    ) -> Result<RevocationRecord, PkiError> {
        let normalized = normalize_serial(serial)?;
        // 传入的就是证书 serial（或已吊销）：幂等返回首次记录。
        if let Some(existing) = self
            .store
            .get_revocation(&normalized)
            .await
            .map_err(|e| PkiError::Store(e.to_string()))?
        {
            return Ok(existing);
        }
        let record = self
            .store
            .get_cert_by_serial(&normalized)
            .await
            .map_err(|e| PkiError::Store(e.to_string()))?
            .ok_or_else(|| PkiError::CertNotFound(serial.to_string()))?;
        let cert_serial = record_cert_serial(&record);
        if let Some(existing) = self
            .store
            .get_revocation(&cert_serial)
            .await
            .map_err(|e| PkiError::Store(e.to_string()))?
        {
            return Ok(existing);
        }
        let by = by.trim();
        let revocation = RevocationRecord {
            cert_serial: cert_serial.clone(),
            record_serial: record.serial.clone(),
            common_name: record.common_name.clone(),
            reason,
            revoked_at: now_unix(),
            by: if by.is_empty() {
                "anonymous".to_string()
            } else {
                by.to_string()
            },
        };
        match self.store.create_revocation(&revocation).await {
            Ok(()) => Ok(revocation),
            // 并发吊销：返回胜者（幂等）
            Err(PkiStoreError::AlreadyExists(_)) => self
                .store
                .get_revocation(&cert_serial)
                .await
                .map_err(|e| PkiError::Store(e.to_string()))?
                .ok_or_else(|| PkiError::Store("revocation vanished after CAS conflict".into())),
            Err(e) => Err(PkiError::Store(e.to_string())),
        }
    }

    /// 按 serial 查状态（active / retired / expired / revoked）。
    ///
    /// 吊销优先于过期：过期且已吊销的 serial 返回 revoked。
    pub async fn get_cert_status(&self, serial: &str) -> Result<CertStatusInfo, PkiError> {
        let normalized = normalize_serial(serial)?;
        let record = self
            .store
            .get_cert_by_serial(&normalized)
            .await
            .map_err(|e| PkiError::Store(e.to_string()))?;
        let cert_serial = record
            .as_ref()
            .map(record_cert_serial)
            .unwrap_or_else(|| normalized.clone());
        let mut revocation = self
            .store
            .get_revocation(&normalized)
            .await
            .map_err(|e| PkiError::Store(e.to_string()))?;
        if revocation.is_none() && cert_serial != normalized {
            revocation = self
                .store
                .get_revocation(&cert_serial)
                .await
                .map_err(|e| PkiError::Store(e.to_string()))?;
        }
        if let Some(revocation) = revocation {
            return Ok(CertStatusInfo {
                serial: serial.to_string(),
                cert_serial: revocation.cert_serial.clone(),
                common_name: revocation.common_name.clone(),
                status: status_to_str(CertStatus::Revoked).to_string(),
                not_before: record.as_ref().map(|r| r.not_before).unwrap_or(0),
                not_after: record.as_ref().map(|r| r.not_after).unwrap_or(0),
                revocation: Some(revocation),
            });
        }
        let record = record.ok_or_else(|| PkiError::CertNotFound(serial.to_string()))?;
        Ok(CertStatusInfo {
            serial: serial.to_string(),
            cert_serial,
            common_name: record.common_name.clone(),
            status: status_to_str_at(record.status, record.not_after).to_string(),
            not_before: record.not_before,
            not_after: record.not_after,
            revocation: None,
        })
    }

    /// 当前 CRL（内容未变化时复用已持久化的 CRL number）。
    pub async fn get_crl(&self) -> Result<CrlMaterial, PkiError> {
        self.crl_material(false).await
    }

    /// 强制分配新 CRL number 并重签（即使吊销集合未变）。
    pub async fn rotate_crl(&self) -> Result<CrlMaterial, PkiError> {
        self.crl_material(true).await
    }

    async fn crl_material(&self, force_new: bool) -> Result<CrlMaterial, PkiError> {
        let (state, revocations) = self.ensure_crl_state(force_new).await?;
        let ca = {
            let guard = self.ca.read();
            guard.as_ref().cloned().ok_or(PkiError::CaNotInitialized)?
        };
        let ca_key_pem = self
            .keys
            .private_key_pem(&ca.key_name, ca.key_version)
            .await
            .map_err(|e| PkiError::Key(e.to_string()))?;
        let crl_state = state.clone();
        let crl_cert_pem = ca.cert_pem;
        let (der, pem) = tokio::task::spawn_blocking(move || {
            build_crl(&crl_cert_pem, &ca_key_pem, &crl_state, &revocations)
        })
        .await
        .map_err(|e| PkiError::CertGen(format!("crl blocking task join: {e}")))??;
        Ok(CrlMaterial {
            der,
            pem,
            crl_number: state.crl_number,
            this_update: state.this_update,
            next_update: state.next_update,
        })
    }

    /// 决定本次 CRL 的编号与时间窗口。
    ///
    /// - 内容（吊销集合）未变且不强制轮换 → 复用持久化状态（编号稳定）；
    /// - 否则分配 `number + 1` 并 CAS 持久化（并发冲突重读重试）。
    async fn ensure_crl_state(
        &self,
        force_new: bool,
    ) -> Result<(CrlState, Vec<RevocationRecord>), PkiError> {
        let revocations = self
            .store
            .list_revocations()
            .await
            .map_err(|e| PkiError::Store(e.to_string()))?;
        let content_hash = revocation_content_hash(&revocations);
        for _ in 0..Self::MAX_REPLACE_ATTEMPTS {
            let current = self
                .store
                .get_crl_state()
                .await
                .map_err(|e| PkiError::Store(e.to_string()))?;
            let version = current.as_ref().map(|(_, v)| *v).unwrap_or(0);
            if let Some((state, _)) = &current {
                // 内容未变且仍在承诺窗口内才复用；next_update 已过必须换号重签，
                // 否则消费方拿到的是过期 CRL（没有后台发布任务可依赖）。
                if !force_new
                    && state.content_hash == content_hash
                    && now_unix() < state.next_update
                {
                    return Ok((state.clone(), revocations));
                }
            }
            let crl_number = current
                .as_ref()
                .map(|(state, _)| state.crl_number + 1)
                .unwrap_or(1);
            let this_update = now_unix();
            let state = CrlState {
                crl_number,
                content_hash: content_hash.clone(),
                this_update,
                next_update: this_update + self.config.crl_ttl_hours as i64 * 3600,
            };
            match self.store.cas_crl_state(version, &state).await {
                Ok(()) => return Ok((state, revocations)),
                Err(PkiStoreError::VersionConflict(_)) => continue,
                Err(e) => return Err(PkiError::Store(e.to_string())),
            }
        }
        Err(PkiError::Store(
            "crl number allocation attempts exhausted (persistent concurrent rotate)".to_string(),
        ))
    }

    /// 回收历史证书记录（`not_after + retention` 之前到期）。
    ///
    /// 吊销账本**不回收**：serial 的 revoked 状态是永久事实。被吊销的证书记录
    /// 也保留（active 槽位上的吊销记录同时承担「同 CN 不得静默重签」的阻断职责）。
    pub async fn tidy(&self, retention_seconds: i64) -> Result<TidyResult, PkiError> {
        let retention = if retention_seconds > 0 {
            retention_seconds
        } else {
            self.config.history_retention_hours as i64 * 3600
        };
        let cutoff = now_unix() - retention;
        let revoked: std::collections::HashSet<String> = self
            .store
            .list_revocations()
            .await
            .map_err(|e| PkiError::Store(e.to_string()))?
            .into_iter()
            .map(|r| r.cert_serial)
            .collect();
        let mut removed_certs = 0u64;
        for record in self
            .store
            .list_history_certs()
            .await
            .map_err(|e| PkiError::Store(e.to_string()))?
        {
            if record.not_after < cutoff && !revoked.contains(&record_cert_serial(&record)) {
                self.store
                    .delete_history_cert(&record.common_name, &record.serial)
                    .await
                    .map_err(|e| PkiError::Store(e.to_string()))?;
                removed_certs += 1;
            }
        }
        for record in self
            .store
            .list_active_certs()
            .await
            .map_err(|e| PkiError::Store(e.to_string()))?
        {
            if record.not_after >= cutoff || revoked.contains(&record_cert_serial(&record)) {
                continue;
            }
            // 读版本后 CAS 删除：并发轮换（已写入新记录）时跳过，不误删新证书。
            let Some((_current, version)) = self
                .store
                .get_cert_with_version(&record.common_name)
                .await
                .map_err(|e| PkiError::Store(e.to_string()))?
            else {
                continue;
            };
            match self
                .store
                .delete_active_cert_cas(&record.common_name, version)
                .await
            {
                Ok(()) => removed_certs += 1,
                Err(PkiStoreError::VersionConflict(_)) => {}
                Err(e) => return Err(PkiError::Store(e.to_string())),
            }
        }
        Ok(TidyResult {
            retention_seconds: retention,
            removed_certs,
        })
    }

    /// 用调用方 CSR 的公钥签发（PoP 必须通过；私钥不进入 coord）。
    ///
    /// 幂等：同 CN + 同 SPKI + SAN/TTL/usage 一致 ⇒ 返回既有证书；否则替换出新证书，
    /// 旧记录 retired。CN / SAN 取自 CSR；CSR 无 SAN 时由 CN 派生。
    pub async fn sign_csr(
        &self,
        csr_der: &[u8],
        ttl_seconds: u64,
        usage: CertUsage,
    ) -> Result<CertInfo, PkiError> {
        let csr = parse_csr(csr_der)?;
        let common_name = csr.common_name.clone();
        let want_ttl = effective_ttl_seconds(ttl_seconds, self.config.cert_ttl_hours);
        for _ in 0..Self::MAX_REPLACE_ATTEMPTS {
            match self
                .store
                .get_cert_with_version(&common_name)
                .await
                .map_err(|e| PkiError::Store(e.to_string()))?
            {
                Some((record, version)) => {
                    // 当前身份已吊销：CSR 签发会隐式复活同一 CN（ADR-0020）。
                    if self.is_record_revoked(&record).await? {
                        return Err(PkiError::FailedPrecondition(format!(
                            "certificate for CN {common_name} is revoked (serial {}); sign with a \
                             new CN instead of resurrecting the revoked identity",
                            record.serial
                        )));
                    }
                    if !record.is_expired(now_unix())
                        && record.is_external()
                        && record.spki_sha256 == csr.spki_sha256
                        && record.dns_names == csr.san.dns_names
                        && record.ip_addresses == csr.san.ip_addresses
                        && record_ttl_seconds(&record) == want_ttl
                        && record.usage == usage
                    {
                        return Ok(record.into());
                    }
                    let new_record = self
                        .sign_external_cert_blocking(
                            &common_name,
                            ttl_seconds,
                            &csr,
                            usage,
                            CertStatus::Active,
                            Some(record.serial.clone()),
                        )
                        .await?;
                    let mut retired = record.clone();
                    retired.status = CertStatus::Retired;
                    match self
                        .store
                        .replace_active_cert_cas(&common_name, version, &new_record, &retired)
                        .await
                    {
                        Ok(()) => return Ok(new_record.into()),
                        Err(PkiStoreError::VersionConflict(_)) => continue,
                        Err(e) => return Err(PkiError::Store(e.to_string())),
                    }
                }
                None => {
                    let new_record = self
                        .sign_external_cert_blocking(
                            &common_name,
                            ttl_seconds,
                            &csr,
                            usage,
                            CertStatus::Active,
                            None,
                        )
                        .await?;
                    match self.store.create_cert(&common_name, &new_record).await {
                        Ok(()) => return Ok(new_record.into()),
                        Err(PkiStoreError::AlreadyExists(_)) => continue,
                        Err(e) => return Err(PkiError::Store(e.to_string())),
                    }
                }
            }
        }
        Err(PkiError::Store(format!(
            "replace attempts exhausted for CN {common_name} (persistent concurrent replace)"
        )))
    }

    /// 取得 leaf 密钥版本：首次创建（version 1）或轮换代次（latest + 1）。
    ///
    /// PKI leaf key 走 transit 命名密钥档案（`pki/leaf/{cn}`），与 CA 共用同一 KEK；
    /// `exportable=true` 保留受控恢复路径（导出仍需 `coord:transit:key_export`）。
    async fn ensure_leaf_key(
        &self,
        common_name: &str,
        create: bool,
    ) -> Result<(String, u32), PkiError> {
        let name = format!("pki/leaf/{common_name}");
        let archive = if create {
            self.keys
                .create_key_internal(CreateKeySpec {
                    name: name.clone(),
                    key_type: KeyType::EcdsaP256,
                    exportable: true,
                })
                .await
        } else {
            match self.keys.rotate_key_internal(&name).await {
                Ok(archive) => Ok(archive),
                Err(KeyError::NotFound(_)) => {
                    self.keys
                        .create_key_internal(CreateKeySpec {
                            name: name.clone(),
                            key_type: KeyType::EcdsaP256,
                            exportable: true,
                        })
                        .await
                }
                Err(e) => Err(e),
            }
        }
        .map_err(|e| PkiError::Key(e.to_string()))?;
        Ok((name, archive.latest_version))
    }

    /// 异步签发：CPU 密集的 rcgen 密钥生成/证书签名移到阻塞线程池。
    ///
    /// CA 与 leaf 的私钥材料从命名密钥档案解出（`Zeroizing`），随 `spawn_blocking`
    /// 闭包移动；函数返回即释放，不进入 PKI store 或 CertRecord。
    #[allow(clippy::too_many_arguments)]
    async fn sign_new_cert_blocking(
        &self,
        common_name: &str,
        ttl_seconds: u64,
        san: SanSpec,
        usage: CertUsage,
        status: CertStatus,
        parent_serial: Option<String>,
        key_name: String,
        key_version: u32,
    ) -> Result<CertRecord, PkiError> {
        let ca = {
            let guard = self.ca.read();
            guard.as_ref().cloned().ok_or(PkiError::CaNotInitialized)?
        };
        let ca_key_pem = self
            .keys
            .private_key_pem(&ca.key_name, ca.key_version)
            .await
            .map_err(|e| PkiError::Key(e.to_string()))?;
        let leaf_key_pem = self
            .keys
            .private_key_pem(&key_name, key_version)
            .await
            .map_err(|e| PkiError::Key(e.to_string()))?;
        let default_ttl_hours = self.config.cert_ttl_hours;
        let cn = common_name.to_string();
        let ca_cert_pem = ca.cert_pem;

        tokio::task::spawn_blocking(move || {
            sign_cert_with_ca(
                &ca_cert_pem,
                &ca_key_pem,
                &leaf_key_pem,
                &key_name,
                key_version,
                &cn,
                ttl_seconds,
                default_ttl_hours,
                &san,
                usage,
                status,
                parent_serial,
            )
        })
        .await
        .map_err(|e| PkiError::CertGen(format!("sign blocking task join: {e}")))?
    }
    /// 异步签发外部密钥证书：CA 私钥解出后放进阻塞线程池；调用方私钥不参与。
    async fn sign_external_cert_blocking(
        &self,
        common_name: &str,
        ttl_seconds: u64,
        csr: &CsrSpec,
        usage: CertUsage,
        status: CertStatus,
        parent_serial: Option<String>,
    ) -> Result<CertRecord, PkiError> {
        let ca = {
            let guard = self.ca.read();
            guard.as_ref().cloned().ok_or(PkiError::CaNotInitialized)?
        };
        let ca_key_pem = self
            .keys
            .private_key_pem(&ca.key_name, ca.key_version)
            .await
            .map_err(|e| PkiError::Key(e.to_string()))?;
        let default_ttl_hours = self.config.cert_ttl_hours;
        let cn = common_name.to_string();
        let ca_cert_pem = ca.cert_pem;
        let spki_der = csr.spki_der.clone();
        let san = csr.san.clone();
        let spki_sha256 = csr.spki_sha256.clone();

        tokio::task::spawn_blocking(move || {
            sign_external_cert_with_ca(
                &ca_cert_pem,
                &ca_key_pem,
                &spki_der,
                &cn,
                ttl_seconds,
                default_ttl_hours,
                &san,
                &spki_sha256,
                usage,
                status,
                parent_serial,
            )
        })
        .await
        .map_err(|e| PkiError::CertGen(format!("sign blocking task join: {e}")))?
    }
}

// ──── 辅助函数 ────

/// 纯函数：使用给定 CA 材料签发新终端证书（不读 self，可安全放入 spawn_blocking）。
#[allow(clippy::too_many_arguments)]
fn sign_cert_with_ca(
    ca_cert_pem: &str,
    ca_key_pem: &str,
    leaf_key_pem: &str,
    leaf_key_name: &str,
    leaf_key_version: u32,
    common_name: &str,
    ttl_seconds: u64,
    default_ttl_hours: u32,
    san: &SanSpec,
    usage: CertUsage,
    status: CertStatus,
    parent_serial: Option<String>,
) -> Result<CertRecord, PkiError> {
    let key_pair =
        KeyPair::from_pem(leaf_key_pem).map_err(|e| PkiError::KeyGen(format!("leaf key: {e}")))?;

    let mut params = CertificateParams::default();
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    params
        .distinguished_name
        .push(DnType::OrganizationName, "Coord Agent");
    params.is_ca = IsCa::NoCa;
    params.key_usages = key_usages(usage);
    params.extended_key_usages = extended_key_usages(usage);

    // rcgen 不会把 CN 自动写入 SAN；只认 SAN 的校验方（Go ≥1.15 / rustls /
    // 浏览器 / Envoy）会拒绝无 SAN 证书，故 SAN 必须显式写入。
    let mut subject_alt_names = Vec::with_capacity(san.dns_names.len() + san.ip_addresses.len());
    for name in &san.dns_names {
        let ia5 = Ia5String::try_from(name.as_str())
            .map_err(|_| PkiError::InvalidArgument(format!("invalid DNS name: {name:?}")))?;
        subject_alt_names.push(SanType::DnsName(ia5));
    }
    for ip in &san.ip_addresses {
        let addr = IpAddr::from_str(ip)
            .map_err(|_| PkiError::InvalidArgument(format!("invalid IP address: {ip:?}")))?;
        subject_alt_names.push(SanType::IpAddress(addr));
    }
    params.subject_alt_names = subject_alt_names;

    let now = OffsetDateTime::now_utc();
    let ttl = time::Duration::seconds(effective_ttl_seconds(ttl_seconds, default_ttl_hours) as i64);
    params.not_before = now;
    params.not_after = now + ttl;
    let (serial, serial_number) = new_serial();
    params.serial_number = Some(serial_number);

    let ca_key = KeyPair::from_pem(ca_key_pem).map_err(|e| PkiError::KeyGen(e.to_string()))?;

    // 从持久化的 CA 证书重建签发者（重启后无需原始 params）
    let issuer = rcgen::Issuer::from_ca_cert_pem(ca_cert_pem, ca_key)
        .map_err(|e| PkiError::CertGen(e.to_string()))?;

    let cert = params
        .signed_by(&key_pair, &issuer)
        .map_err(|e| PkiError::CertGen(e.to_string()))?;
    let cert_pem = cert.pem();
    let spki_sha256 = cert_spki_sha256(&cert_pem)?;

    let not_before = now.unix_timestamp();
    let not_after = (now + ttl).unix_timestamp();

    Ok(CertRecord {
        common_name: common_name.to_string(),
        cert_pem,
        key_name: leaf_key_name.to_string(),
        key_version: leaf_key_version,
        not_before,
        not_after,
        serial,
        status,
        parent_serial,
        dns_names: san.dns_names.clone(),
        ip_addresses: san.ip_addresses.clone(),
        spki_sha256,
        usage,
    })
}

/// KU 按 usage 精确写入（与 seedj `CertificateUsage` 对齐）。
fn key_usages(usage: CertUsage) -> Vec<KeyUsagePurpose> {
    match usage {
        CertUsage::Client => vec![KeyUsagePurpose::DigitalSignature],
        CertUsage::Server | CertUsage::Both => vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ],
    }
}

/// EKU 按 usage 精确写入；BOTH 保留既有「双 EKU」行为。
fn extended_key_usages(usage: CertUsage) -> Vec<rcgen::ExtendedKeyUsagePurpose> {
    match usage {
        CertUsage::Server => vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth],
        CertUsage::Client => vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth],
        CertUsage::Both => vec![
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
        ],
    }
}

/// 证书公钥 SPKI DER 的 SHA-256（小写 hex），作为 CSR 幂等口径。
fn cert_spki_sha256(cert_pem: &str) -> Result<String, PkiError> {
    let der = pem_to_der(cert_pem)?;
    let (_rest, cert) = x509_parser::parse_x509_certificate(&der)
        .map_err(|e| PkiError::CertParse(e.to_string()))?;
    Ok(hex::encode(Sha256::digest(cert.public_key().raw)))
}

/// CSR 解析结果（PoP 已通过；私钥从未出现）。
struct CsrSpec {
    common_name: String,
    san: SanSpec,
    /// PKIX SubjectPublicKeyInfo DER（用于 rcgen 签证书）
    spki_der: Vec<u8>,
    /// SPKI DER 的 SHA-256（小写 hex，幂等判据）
    spki_sha256: String,
}

/// 解析并验证 CSR：签名（PoP）+ CN + DNS/IP SAN。
///
/// 不支持的 SAN 类型（email/URI/DirectoryName 等）显式拒绝，避免静默丢弃身份。
fn parse_csr(csr_der: &[u8]) -> Result<CsrSpec, PkiError> {
    use x509_parser::certification_request::X509CertificationRequest;
    use x509_parser::extensions::{GeneralName, ParsedExtension};
    use x509_parser::prelude::FromDer;

    let (rest, csr) = X509CertificationRequest::from_der(csr_der)
        .map_err(|e| PkiError::InvalidArgument(format!("invalid CSR: {e}")))?;
    if !rest.is_empty() {
        return Err(PkiError::InvalidArgument(
            "trailing bytes after CSR".to_string(),
        ));
    }
    csr.verify_signature().map_err(|_| {
        PkiError::InvalidArgument("CSR proof-of-possession signature is invalid".to_string())
    })?;

    let common_name = csr
        .certification_request_info
        .subject
        .iter_common_name()
        .next()
        .and_then(|cn| cn.as_str().ok())
        .filter(|cn| !cn.is_empty())
        .ok_or_else(|| {
            PkiError::InvalidArgument("CSR must contain a non-empty subject CN".to_string())
        })?
        .to_string();

    let mut san = SanSpec::default();
    if let Some(extensions) = csr.requested_extensions() {
        for extension in extensions {
            let ParsedExtension::SubjectAlternativeName(san_ext) = extension else {
                continue;
            };
            for name in &san_ext.general_names {
                match name {
                    GeneralName::DNSName(dns) => san.dns_names.push((*dns).to_string()),
                    GeneralName::IPAddress(bytes) => {
                        let ip = ip_from_bytes(bytes).ok_or_else(|| {
                            PkiError::InvalidArgument(format!("invalid IP SAN in CSR: {bytes:?}"))
                        })?;
                        san.ip_addresses.push(ip.to_string());
                    }
                    other => {
                        return Err(PkiError::InvalidArgument(format!(
                            "unsupported CSR SAN type: {other:?}"
                        )));
                    }
                }
            }
        }
    }
    let san = if san.is_empty() {
        derive_san_from_cn(&common_name)?
    } else {
        san.normalized()?
    };
    let spki_der = csr.certification_request_info.subject_pki.raw.to_vec();
    let spki_sha256 = hex::encode(Sha256::digest(&spki_der));
    Ok(CsrSpec {
        common_name,
        san,
        spki_der,
        spki_sha256,
    })
}

fn ip_from_bytes(bytes: &[u8]) -> Option<IpAddr> {
    match bytes.len() {
        4 => <[u8; 4]>::try_from(bytes).ok().map(IpAddr::from),
        16 => <[u8; 16]>::try_from(bytes).ok().map(IpAddr::from),
        _ => None,
    }
}

/// 纯函数：用 CA 材料给 CSR 的公钥签证书（私钥不参与）。
#[allow(clippy::too_many_arguments)]
fn sign_external_cert_with_ca(
    ca_cert_pem: &str,
    ca_key_pem: &str,
    spki_der: &[u8],
    common_name: &str,
    ttl_seconds: u64,
    default_ttl_hours: u32,
    san: &SanSpec,
    spki_sha256: &str,
    usage: CertUsage,
    status: CertStatus,
    parent_serial: Option<String>,
) -> Result<CertRecord, PkiError> {
    let subject_spki = rcgen::SubjectPublicKeyInfo::from_der(spki_der)
        .map_err(|e| PkiError::InvalidArgument(format!("unsupported CSR public key: {e}")))?;

    let mut params = CertificateParams::default();
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    params
        .distinguished_name
        .push(DnType::OrganizationName, "Coord Agent");
    params.is_ca = IsCa::NoCa;
    params.key_usages = key_usages(usage);
    params.extended_key_usages = extended_key_usages(usage);

    let mut subject_alt_names = Vec::with_capacity(san.dns_names.len() + san.ip_addresses.len());
    for name in &san.dns_names {
        let ia5 = Ia5String::try_from(name.as_str())
            .map_err(|_| PkiError::InvalidArgument(format!("invalid DNS name: {name:?}")))?;
        subject_alt_names.push(SanType::DnsName(ia5));
    }
    for ip in &san.ip_addresses {
        let addr = IpAddr::from_str(ip)
            .map_err(|_| PkiError::InvalidArgument(format!("invalid IP address: {ip:?}")))?;
        subject_alt_names.push(SanType::IpAddress(addr));
    }
    params.subject_alt_names = subject_alt_names;

    let now = OffsetDateTime::now_utc();
    let ttl = time::Duration::seconds(effective_ttl_seconds(ttl_seconds, default_ttl_hours) as i64);
    params.not_before = now;
    params.not_after = now + ttl;
    let (serial, serial_number) = new_serial();
    params.serial_number = Some(serial_number);

    let ca_key = KeyPair::from_pem(ca_key_pem).map_err(|e| PkiError::KeyGen(e.to_string()))?;
    let issuer = rcgen::Issuer::from_ca_cert_pem(ca_cert_pem, ca_key)
        .map_err(|e| PkiError::CertGen(e.to_string()))?;
    let cert = params
        .signed_by(&subject_spki, &issuer)
        .map_err(|e| PkiError::CertGen(e.to_string()))?;

    Ok(CertRecord {
        common_name: common_name.to_string(),
        cert_pem: cert.pem(),
        // 外部密钥：私钥从未进入 coord，没有 key archive 引用。
        key_name: String::new(),
        key_version: 0,
        not_before: now.unix_timestamp(),
        not_after: (now + ttl).unix_timestamp(),
        serial,
        status,
        parent_serial,
        dns_names: san.dns_names.clone(),
        ip_addresses: san.ip_addresses.clone(),
        spki_sha256: spki_sha256.to_string(),
        usage,
    })
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// 新证书的 X.509 serial：16 字节随机、首字节非零（无前导零）、最高位清零（正数）。
///
/// 记录 serial 与证书内嵌 serial 逐字一致：吊销账本与 CRL 条目都按这个值定位
/// （旧记录两者可能不一致，读取时以证书内嵌值为准）。
fn new_serial() -> (String, rcgen::SerialNumber) {
    let mut bytes: [u8; 16] = rand::random();
    bytes[0] = (bytes[0] & 0x7f) | 0x01;
    (
        hex::encode(bytes),
        rcgen::SerialNumber::from(bytes.to_vec()),
    )
}

/// 证书内嵌的 X.509 serial（小写 hex）；PEM 无法解析时返回 None。
fn cert_serial_hex(cert_pem: &str) -> Option<String> {
    let der = pem_to_der(cert_pem).ok()?;
    let (_rest, cert) = x509_parser::parse_x509_certificate(&der).ok()?;
    Some(hex::encode(cert.raw_serial()))
}

/// 记录对应的证书 serial：以证书内嵌值为准，解析失败回退记录字段。
fn record_cert_serial(record: &CertRecord) -> String {
    cert_serial_hex(&record.cert_pem).unwrap_or_else(|| record.serial.to_lowercase())
}

/// 入参 serial 的规范形态（`trim` + 小写）；空 / 非 hex ⇒ `INVALID_ARGUMENT`。
///
/// X.509 serial 是 INTEGER：`0abc…` 与 `abc…` 是同一个值，两种写法都必须命中
/// 同一记录。数值口径的比较在 store 层（`pki_store::serial_key`），这里只负责
/// 校验与规范化，保留调用方的 padding 以便账本键精确命中。
fn normalize_serial(raw: &str) -> Result<String, PkiError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(PkiError::InvalidArgument("serial is empty".to_string()));
    }
    if !trimmed.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(PkiError::InvalidArgument(format!(
            "serial must be hex-encoded (case and leading zeros do not matter): {trimmed}"
        )));
    }
    Ok(trimmed.to_ascii_lowercase())
}

/// `NOT_FOUND` 的单一文案：明确是 serial 未知，不暗示权限问题。
fn not_found_status(serial: &str) -> Status {
    Status::not_found(format!(
        "unknown serial {serial}: no certificate or revocation record (hex, case and leading \
         zeros ignored)"
    ))
}

/// 吊销集合的内容指纹（排序后的 cert_serial + RFC reason）。
///
/// CRL 只在内容变化时分配新编号：同一编号必须对应同一份条目集。
fn revocation_content_hash(revocations: &[RevocationRecord]) -> String {
    let mut entries: Vec<&RevocationRecord> = revocations.iter().collect();
    entries.sort_by(|a, b| a.cert_serial.cmp(&b.cert_serial));
    let mut hasher = Sha256::new();
    for entry in entries {
        hasher.update(entry.cert_serial.as_bytes());
        hasher.update([entry.reason.rfc_code()]);
    }
    hex::encode(hasher.finalize())
}

/// CRL number 的最小大端字节（去掉前导零；`0` 不会出现）。
fn u64_serial_number(value: u64) -> rcgen::SerialNumber {
    let bytes = value.max(1).to_be_bytes();
    let start = bytes
        .iter()
        .position(|b| *b != 0)
        .unwrap_or(bytes.len() - 1);
    rcgen::SerialNumber::from(bytes[start..].to_vec())
}

fn rcgen_revocation_reason(reason: RevocationReason) -> rcgen::RevocationReason {
    match reason {
        RevocationReason::Unspecified => rcgen::RevocationReason::Unspecified,
        RevocationReason::KeyCompromise => rcgen::RevocationReason::KeyCompromise,
        RevocationReason::CaCompromise => rcgen::RevocationReason::CaCompromise,
        RevocationReason::AffiliationChanged => rcgen::RevocationReason::AffiliationChanged,
        RevocationReason::Superseded => rcgen::RevocationReason::Superseded,
        RevocationReason::CessationOfOperation => rcgen::RevocationReason::CessationOfOperation,
        RevocationReason::CertificateHold => rcgen::RevocationReason::CertificateHold,
        RevocationReason::PrivilegeWithdrawn => rcgen::RevocationReason::PrivilegeWithdrawn,
        RevocationReason::AaCompromise => rcgen::RevocationReason::AaCompromise,
    }
}

/// 用 CA 材料签发 CRL（阻塞线程池内执行；纯函数）。
fn build_crl(
    ca_cert_pem: &str,
    ca_key_pem: &str,
    state: &CrlState,
    revocations: &[RevocationRecord],
) -> Result<(Vec<u8>, String), PkiError> {
    let ca_key = KeyPair::from_pem(ca_key_pem).map_err(|e| PkiError::KeyGen(e.to_string()))?;
    let issuer = rcgen::Issuer::from_ca_cert_pem(ca_cert_pem, ca_key)
        .map_err(|e| PkiError::CertGen(e.to_string()))?;

    let mut entries: Vec<&RevocationRecord> = revocations.iter().collect();
    entries.sort_by(|a, b| a.cert_serial.cmp(&b.cert_serial));
    let mut revoked_certs = Vec::with_capacity(entries.len());
    for entry in entries {
        let serial = hex::decode(&entry.cert_serial).map_err(|e| {
            PkiError::Store(format!(
                "revocation record serial {} is not hex: {e}",
                entry.cert_serial
            ))
        })?;
        revoked_certs.push(rcgen::RevokedCertParams {
            serial_number: rcgen::SerialNumber::from(serial),
            revocation_time: OffsetDateTime::from_unix_timestamp(entry.revoked_at)
                .unwrap_or_else(|_| OffsetDateTime::now_utc()),
            reason_code: Some(rcgen_revocation_reason(entry.reason)),
            invalidity_date: None,
        });
    }

    let this_update = OffsetDateTime::from_unix_timestamp(state.this_update)
        .unwrap_or_else(|_| OffsetDateTime::now_utc());
    let next_update = OffsetDateTime::from_unix_timestamp(state.next_update)
        .unwrap_or_else(|_| OffsetDateTime::now_utc() + time::Duration::hours(24));
    let params = rcgen::CertificateRevocationListParams {
        this_update,
        next_update,
        crl_number: u64_serial_number(state.crl_number),
        issuing_distribution_point: None,
        revoked_certs,
        key_identifier_method: rcgen::KeyIdMethod::Sha256,
    };
    let crl = params
        .signed_by(&issuer)
        .map_err(|e| PkiError::CertGen(format!("crl signing failed: {e}")))?;
    let pem = crl
        .pem()
        .map_err(|e| PkiError::CertGen(format!("crl pem encode failed: {e}")))?;
    Ok((crl.der().to_vec(), pem))
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
    /// 验证证书：有效期窗口 + issuer CN + **CA 公钥验签** + **未被吊销**。
    ///
    /// 签名不匹配（含同 issuer CN 的伪造 CA）、不在有效期内或 serial 已在吊销账本
    /// 中 ⇒ `Ok(false)`；PEM 非法 ⇒ `CertParse`（gRPC 映射为 `INVALID_ARGUMENT`）。
    pub async fn verify_cert(&self, cert_pem: &str) -> Result<bool, PkiError> {
        // 不在 await 上持有锁：先取 CA 证书 PEM 再释放 guard。
        let ca_cert_pem = {
            let ca_guard = self.ca.read();
            ca_guard
                .as_ref()
                .ok_or(PkiError::CaNotInitialized)?
                .cert_pem
                .clone()
        };

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

        let issuer_cn = cert
            .issuer()
            .iter_common_name()
            .next()
            .map(|cn| cn.as_str().unwrap_or(""))
            .unwrap_or("");

        let ca_der = pem_to_der(&ca_cert_pem)?;
        let (_ca_remainder, ca_cert) = x509_parser::parse_x509_certificate(&ca_der)
            .map_err(|e| PkiError::CertParse(e.to_string()))?;

        let ca_cn = ca_cert
            .subject()
            .iter_common_name()
            .next()
            .map(|cn| cn.as_str().unwrap_or(""))
            .unwrap_or("");

        if issuer_cn != ca_cn {
            return Ok(false);
        }

        // 只有 issuer 名字匹配不够：必须用 CA 公钥验证签名，否则任何知道 CA CN
        // 的主体都能用自签证书通过。验签失败（含不支持的算法）一律按「不通过」处理。
        if cert.verify_signature(Some(ca_cert.public_key())).is_err() {
            return Ok(false);
        }

        // 吊销账本按证书内嵌 serial 索引（与 CRL 条目同一口径）。
        let serial = hex::encode(cert.raw_serial());
        let revoked = self
            .store
            .get_revocation(&serial)
            .await
            .map_err(|e| PkiError::Store(e.to_string()))?
            .is_some();
        Ok(!revoked)
    }

    /// 导出 CA 证书 PEM
    pub fn ca_cert_pem(&self) -> Result<String, PkiError> {
        let ca = self.ca.read();
        let ca = ca.as_ref().ok_or(PkiError::CaNotInitialized)?;
        Ok(ca.cert_pem.clone())
    }

    /// CA 证书 DER 的 SHA-256 指纹（大写十六进制、冒号分隔）。
    ///
    /// 与 `openssl x509 -in ca.pem -noout -fingerprint -sha256` 的输出逐字可比，
    /// 便于运维在启动日志与导出的根证书之间做一致性复核。
    pub fn ca_fingerprint_sha256(&self) -> Result<String, PkiError> {
        let ca = self.ca.read();
        let ca = ca.as_ref().ok_or(PkiError::CaNotInitialized)?;
        let der = pem_to_der(&ca.cert_pem)?;
        let digest = Sha256::digest(&der);
        Ok(digest
            .iter()
            .map(|byte| format!("{byte:02X}"))
            .collect::<Vec<_>>()
            .join(":"))
    }

    /// 检查证书是否即将过期（剩余时间 < 指定小时数）
    pub fn is_expiring_soon(&self, cert: &CertInfo, within_hours: i64) -> bool {
        let now = OffsetDateTime::now_utc().unix_timestamp();
        let remaining = cert.not_after - now;
        remaining < within_hours * 3600
    }

    /// 到期观测快照（G-PKI-2）：(active 证书总数, 窗口内即将过期数)。
    ///
    /// 「即将过期」= 仍有效（now <= not_after）且剩余有效期 < `expiry_warn_hours`
    /// （基于 `is_expiring_soon` 语义，排除已过期——已过期由「换新触发」处理）。
    pub async fn cert_expiry_snapshot(&self) -> Result<(u64, u64), PkiError> {
        let records = self
            .store
            .list_active_certs()
            .await
            .map_err(|e| PkiError::Store(e.to_string()))?;
        // 吊销账本里的 active 记录不是「在用证书」，不计入到期观测。
        let revoked: std::collections::HashSet<String> = self
            .store
            .list_revocations()
            .await
            .map_err(|e| PkiError::Store(e.to_string()))?
            .into_iter()
            .map(|r| r.cert_serial)
            .collect();
        let records: Vec<_> = records
            .into_iter()
            .filter(|r| !revoked.contains(&record_cert_serial(r)))
            .collect();
        let now = now_unix();
        let window = self.config.expiry_warn_hours;
        let total = records.len() as u64;
        let expiring = records
            .iter()
            .filter(|r| !r.is_expired(now) && r.not_after - now < window * 3600)
            .count() as u64;
        Ok((total, expiring))
    }

    /// 到期告警窗口（小时；G-PKI-2 指标口径与文档共用）
    pub fn expiry_warn_hours(&self) -> i64 {
        self.config.expiry_warn_hours
    }
}

// ──── PkiError ────

/// PKI 错误类型
#[derive(Debug)]
pub enum PkiError {
    /// 命名密钥档案错误（材料解不开 / 版本越界等）
    Key(String),
    CaNotInitialized,
    /// 调用方参数非法（非法 DNS SAN / IP / CN 无法派生 SAN）
    InvalidArgument(String),
    KeyGen(String),
    CertGen(String),
    CertParse(String),
    Io(std::io::Error),
    /// renew 按 serial 查不到证书
    CertNotFound(String),
    /// 状态不允许该操作（如外部密钥证书的 Renew/Rotate）
    FailedPrecondition(String),
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
            Self::Key(msg) => write!(f, "transit key archive error: {msg}"),
            Self::CaNotInitialized => write!(f, "CA not initialized"),
            Self::InvalidArgument(msg) => write!(f, "invalid argument: {msg}"),
            Self::KeyGen(msg) => write!(f, "key generation failed: {msg}"),
            Self::CertGen(msg) => write!(f, "certificate generation failed: {msg}"),
            Self::CertParse(msg) => write!(f, "certificate parse failed: {msg}"),
            Self::Io(e) => write!(f, "IO error: {e}"),
            Self::CertNotFound(s) => write!(f, "unknown serial: {s}"),
            Self::FailedPrecondition(msg) => write!(f, "failed precondition: {msg}"),
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
    pki_server::Pki, PkiCertSummary, PkiCertificateUsage, PkiCrlFormat, PkiGetCaCertRequest,
    PkiGetCaCertResponse, PkiGetCertByCnRequest, PkiGetCertByCnResponse, PkiGetCertBySerialRequest,
    PkiGetCertBySerialResponse, PkiGetCertStatusRequest, PkiGetCertStatusResponse,
    PkiGetCrlRequest, PkiGetCrlResponse, PkiInitCaRequest, PkiInitCaResponse, PkiIssueCertRequest,
    PkiIssueCertResponse, PkiKeyMaterial, PkiListCertsRequest, PkiListCertsResponse,
    PkiRenewCertRequest, PkiRenewCertResponse, PkiRevocationReason, PkiRevokeCertRequest,
    PkiRevokeCertResponse, PkiRotateCertRequest, PkiRotateCertResponse, PkiRotateCrlRequest,
    PkiRotateCrlResponse, PkiSignCsrRequest, PkiSignCsrResponse, PkiTidyRequest, PkiTidyResponse,
    PkiVerifyCertRequest, PkiVerifyCertResponse,
};
use tonic::{Request, Response, Status};

use crate::plugin::GatewayIdentity;

/// `PkiError → tonic::Status` 的单一映射点。
///
/// 参数错误必须是 `INVALID_ARGUMENT`（调用方可重试/修正），其余保持
/// `INTERNAL`；不得让所有失败都退化成无法区分的内部错误。
fn status_from_pki_error(e: PkiError) -> Status {
    match e {
        PkiError::InvalidArgument(msg) => Status::invalid_argument(msg),
        // 调用方给的 PEM 无法解析：参数错误，不是服务端内部错误。
        PkiError::CertParse(msg) => Status::invalid_argument(msg),
        PkiError::CertNotFound(serial) => not_found_status(&serial),
        PkiError::FailedPrecondition(msg) => Status::failed_precondition(msg),
        other => Status::internal(other.to_string()),
    }
}

fn status_to_str(status: CertStatus) -> &'static str {
    match status {
        CertStatus::Active => "active",
        CertStatus::Retired => "retired",
        CertStatus::Revoked => "revoked",
    }
}

/// 对外状态：吊销优先于过期，过期优先于存储态 active/retired（服务端单一判定点）。
fn status_to_str_at(status: CertStatus, not_after: i64) -> &'static str {
    if status == CertStatus::Revoked {
        "revoked"
    } else if now_unix() > not_after {
        "expired"
    } else {
        status_to_str(status)
    }
}

/// 私钥引用（材料在 coord 内；外部密钥证书没有引用）
fn key_material_of(cert: &CertInfo) -> Option<PkiKeyMaterial> {
    if cert.key_name.is_empty() {
        return Some(PkiKeyMaterial {
            kind: "external".into(),
            key_name: String::new(),
            key_version: 0,
        });
    }
    Some(PkiKeyMaterial {
        kind: "coord_managed".into(),
        key_name: cert.key_name.clone(),
        key_version: cert.key_version,
    })
}

/// proto usage → 生效 profile；`UNSPECIFIED` 按 BOTH（IssueCert / SignCsr）。
fn cert_usage_from_proto(value: i32) -> Result<CertUsage, Status> {
    match PkiCertificateUsage::try_from(value) {
        Ok(PkiCertificateUsage::Unspecified) | Ok(PkiCertificateUsage::Both) => Ok(CertUsage::Both),
        Ok(PkiCertificateUsage::Server) => Ok(CertUsage::Server),
        Ok(PkiCertificateUsage::Client) => Ok(CertUsage::Client),
        Err(_) => Err(Status::invalid_argument(format!(
            "unknown certificate usage: {value}"
        ))),
    }
}

/// proto usage → 可选 profile；`UNSPECIFIED` = 继承（RotateCert）。
fn optional_cert_usage_from_proto(value: i32) -> Result<Option<CertUsage>, Status> {
    match PkiCertificateUsage::try_from(value) {
        Ok(PkiCertificateUsage::Unspecified) => Ok(None),
        Ok(PkiCertificateUsage::Server) => Ok(Some(CertUsage::Server)),
        Ok(PkiCertificateUsage::Client) => Ok(Some(CertUsage::Client)),
        Ok(PkiCertificateUsage::Both) => Ok(Some(CertUsage::Both)),
        Err(_) => Err(Status::invalid_argument(format!(
            "unknown certificate usage: {value}"
        ))),
    }
}

/// proto 吊销原因 → RFC 5280 CRLReason；未知数值 fail-closed。
fn revocation_reason_from_proto(value: i32) -> Result<RevocationReason, Status> {
    RevocationReason::from_rfc_code(value).ok_or_else(|| {
        Status::invalid_argument(format!(
            "unknown revocation reason (RFC 5280 CRLReason): {value}"
        ))
    })
}

fn revocation_reason_to_proto(reason: RevocationReason) -> PkiRevocationReason {
    match reason {
        RevocationReason::Unspecified => PkiRevocationReason::Unspecified,
        RevocationReason::KeyCompromise => PkiRevocationReason::KeyCompromise,
        RevocationReason::CaCompromise => PkiRevocationReason::CaCompromise,
        RevocationReason::AffiliationChanged => PkiRevocationReason::AffiliationChanged,
        RevocationReason::Superseded => PkiRevocationReason::Superseded,
        RevocationReason::CessationOfOperation => PkiRevocationReason::CessationOfOperation,
        RevocationReason::CertificateHold => PkiRevocationReason::CertificateHold,
        RevocationReason::PrivilegeWithdrawn => PkiRevocationReason::PrivilegeWithdrawn,
        RevocationReason::AaCompromise => PkiRevocationReason::AaCompromise,
    }
}

/// proto CRL 格式 → 响应编码；UNSPECIFIED = PEM。
fn crl_format_from_proto(value: i32) -> Result<PkiCrlFormat, Status> {
    match PkiCrlFormat::try_from(value) {
        Ok(PkiCrlFormat::Unspecified) | Ok(PkiCrlFormat::Pem) => Ok(PkiCrlFormat::Pem),
        Ok(PkiCrlFormat::Der) => Ok(PkiCrlFormat::Der),
        Err(_) => Err(Status::invalid_argument(format!(
            "unknown CRL format: {value}"
        ))),
    }
}

fn crl_bytes(format: PkiCrlFormat, material: &CrlMaterial) -> Vec<u8> {
    match format {
        PkiCrlFormat::Der => material.der.clone(),
        _ => material.pem.as_bytes().to_vec(),
    }
}

fn cert_to_issue_response(cert_info: CertInfo) -> PkiIssueCertResponse {
    let key_material = key_material_of(&cert_info);
    PkiIssueCertResponse {
        common_name: cert_info.common_name,
        cert_pem: cert_info.cert_pem,
        not_before: cert_info.not_before,
        not_after: cert_info.not_after,
        serial: cert_info.serial,
        status: status_to_str_at(cert_info.status, cert_info.not_after).into(),
        parent_serial: cert_info.parent_serial.unwrap_or_default(),
        key_material,
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
            .map_err(status_from_pki_error)?;
        Ok(Response::new(PkiInitCaResponse {}))
    }

    async fn issue_cert(
        &self,
        request: Request<PkiIssueCertRequest>,
    ) -> Result<Response<PkiIssueCertResponse>, Status> {
        let req = request.into_inner();
        let usage = cert_usage_from_proto(req.usage)?;
        let cert_info = PkiService::issue_cert_with_profile(
            self,
            &req.common_name,
            req.ttl_seconds as u64,
            SanSpec::new(req.dns_names, req.ip_addresses),
            usage,
        )
        .await
        .map_err(status_from_pki_error)?;
        Ok(Response::new(cert_to_issue_response(cert_info)))
    }

    async fn renew_cert(
        &self,
        request: Request<PkiRenewCertRequest>,
    ) -> Result<Response<PkiRenewCertResponse>, Status> {
        let req = request.into_inner();
        // 修复：按 serial 查回真实 CN 再续期，不再把 serial 当 CN
        let cert_info = PkiService::renew_cert(self, &req.serial_number, req.ttl_seconds as u64)
            .await
            .map_err(status_from_pki_error)?;
        let key_material = key_material_of(&cert_info);
        Ok(Response::new(PkiRenewCertResponse {
            common_name: cert_info.common_name,
            cert_pem: cert_info.cert_pem,
            not_before: cert_info.not_before,
            not_after: cert_info.not_after,
            serial: cert_info.serial,
            status: status_to_str_at(cert_info.status, cert_info.not_after).into(),
            parent_serial: cert_info.parent_serial.unwrap_or_default(),
            key_material,
        }))
    }

    async fn rotate_cert(
        &self,
        request: Request<PkiRotateCertRequest>,
    ) -> Result<Response<PkiRotateCertResponse>, Status> {
        let req = request.into_inner();
        let usage = optional_cert_usage_from_proto(req.usage)?;
        let cert_info = PkiService::rotate_cert_with_profile(
            self,
            &req.common_name,
            req.ttl_seconds as u64,
            SanSpec::new(req.dns_names, req.ip_addresses),
            usage,
        )
        .await
        .map_err(status_from_pki_error)?;
        let key_material = key_material_of(&cert_info);
        Ok(Response::new(PkiRotateCertResponse {
            common_name: cert_info.common_name,
            cert_pem: cert_info.cert_pem,
            not_before: cert_info.not_before,
            not_after: cert_info.not_after,
            serial: cert_info.serial,
            status: status_to_str_at(cert_info.status, cert_info.not_after).into(),
            parent_serial: cert_info.parent_serial.unwrap_or_default(),
            key_material,
        }))
    }

    async fn list_certs(
        &self,
        request: Request<PkiListCertsRequest>,
    ) -> Result<Response<PkiListCertsResponse>, Status> {
        let req = request.into_inner();
        let certs = PkiService::list_certs(self, &req.common_name)
            .await
            .map_err(status_from_pki_error)?;
        let summaries = certs
            .into_iter()
            .map(|c| PkiCertSummary {
                common_name: c.common_name,
                cert_pem: c.cert_pem,
                not_before: c.not_before,
                not_after: c.not_after,
                serial: c.serial,
                status: status_to_str_at(c.status, c.not_after).into(),
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
            .map_err(status_from_pki_error)?
            .ok_or_else(|| {
                Status::not_found(format!("no active certificate for CN: {}", req.common_name))
            })?;
        let key_material = key_material_of(&cert_info);
        Ok(Response::new(PkiGetCertByCnResponse {
            common_name: cert_info.common_name,
            cert_pem: cert_info.cert_pem,
            not_before: cert_info.not_before,
            not_after: cert_info.not_after,
            serial: cert_info.serial,
            status: status_to_str_at(cert_info.status, cert_info.not_after).into(),
            parent_serial: cert_info.parent_serial.unwrap_or_default(),
            key_material,
        }))
    }

    async fn get_cert_by_serial(
        &self,
        request: Request<PkiGetCertBySerialRequest>,
    ) -> Result<Response<PkiGetCertBySerialResponse>, Status> {
        let req = request.into_inner();
        let cert_info = PkiService::get_cert_by_serial(self, &req.serial)
            .await
            .map_err(status_from_pki_error)?
            .ok_or_else(|| not_found_status(&req.serial))?;
        Ok(Response::new(PkiGetCertBySerialResponse {
            common_name: cert_info.common_name,
            cert_pem: cert_info.cert_pem,
            not_before: cert_info.not_before,
            not_after: cert_info.not_after,
            serial: cert_info.serial,
            status: status_to_str_at(cert_info.status, cert_info.not_after).into(),
            parent_serial: cert_info.parent_serial.unwrap_or_default(),
        }))
    }

    async fn sign_csr(
        &self,
        request: Request<PkiSignCsrRequest>,
    ) -> Result<Response<PkiSignCsrResponse>, Status> {
        let req = request.into_inner();
        let usage = cert_usage_from_proto(req.usage)?;
        let cert_info = PkiService::sign_csr(self, &req.csr_der, req.ttl_seconds as u64, usage)
            .await
            .map_err(status_from_pki_error)?;
        Ok(Response::new(PkiSignCsrResponse {
            common_name: cert_info.common_name,
            cert_pem: cert_info.cert_pem,
            not_before: cert_info.not_before,
            not_after: cert_info.not_after,
            serial: cert_info.serial,
            status: status_to_str_at(cert_info.status, cert_info.not_after).into(),
            parent_serial: cert_info.parent_serial.unwrap_or_default(),
        }))
    }

    async fn verify_cert(
        &self,
        request: Request<PkiVerifyCertRequest>,
    ) -> Result<Response<PkiVerifyCertResponse>, Status> {
        let req = request.into_inner();
        let valid = PkiService::verify_cert(self, &req.cert_pem)
            .await
            .map_err(status_from_pki_error)?;
        Ok(Response::new(PkiVerifyCertResponse { valid }))
    }

    async fn get_ca_cert(
        &self,
        _request: Request<PkiGetCaCertRequest>,
    ) -> Result<Response<PkiGetCaCertResponse>, Status> {
        let ca_cert_pem = PkiService::ca_cert_pem(self).map_err(status_from_pki_error)?;
        Ok(Response::new(PkiGetCaCertResponse { ca_cert_pem }))
    }

    async fn revoke_cert(
        &self,
        request: Request<PkiRevokeCertRequest>,
    ) -> Result<Response<PkiRevokeCertResponse>, Status> {
        // `by` 是审计事实，取自鉴权主体（CCT sub），不接受调用方自报。
        let by = request
            .extensions()
            .get::<GatewayIdentity>()
            .map(|identity| identity.subject.clone())
            .filter(|subject| !subject.is_empty())
            .unwrap_or_else(|| "anonymous".to_string());
        let req = request.into_inner();
        let reason = revocation_reason_from_proto(req.reason)?;
        let revocation = PkiService::revoke_cert(self, &req.serial, reason, &by)
            .await
            .map_err(status_from_pki_error)?;
        Ok(Response::new(PkiRevokeCertResponse {
            serial: req.serial,
            cert_serial: revocation.cert_serial,
            common_name: revocation.common_name,
            status: status_to_str(CertStatus::Revoked).to_string(),
            revoked_at: revocation.revoked_at,
            reason: revocation_reason_to_proto(revocation.reason) as i32,
            by: revocation.by,
        }))
    }

    async fn get_cert_status(
        &self,
        request: Request<PkiGetCertStatusRequest>,
    ) -> Result<Response<PkiGetCertStatusResponse>, Status> {
        let req = request.into_inner();
        let status = PkiService::get_cert_status(self, &req.serial)
            .await
            .map_err(status_from_pki_error)?;
        let (revoked_at, reason, by) = match &status.revocation {
            Some(revocation) => (
                revocation.revoked_at,
                revocation_reason_to_proto(revocation.reason) as i32,
                revocation.by.clone(),
            ),
            None => (0, PkiRevocationReason::Unspecified as i32, String::new()),
        };
        Ok(Response::new(PkiGetCertStatusResponse {
            serial: status.serial,
            cert_serial: status.cert_serial,
            common_name: status.common_name,
            status: status.status,
            not_before: status.not_before,
            not_after: status.not_after,
            revoked_at,
            reason,
            by,
        }))
    }

    async fn get_crl(
        &self,
        request: Request<PkiGetCrlRequest>,
    ) -> Result<Response<PkiGetCrlResponse>, Status> {
        let req = request.into_inner();
        let format = crl_format_from_proto(req.format)?;
        let material = PkiService::get_crl(self)
            .await
            .map_err(status_from_pki_error)?;
        Ok(Response::new(PkiGetCrlResponse {
            crl: crl_bytes(format, &material),
            format: format as i32,
            crl_number: material.crl_number,
            this_update: material.this_update,
            next_update: material.next_update,
        }))
    }

    async fn rotate_crl(
        &self,
        request: Request<PkiRotateCrlRequest>,
    ) -> Result<Response<PkiRotateCrlResponse>, Status> {
        let req = request.into_inner();
        let format = crl_format_from_proto(req.format)?;
        let material = PkiService::rotate_crl(self)
            .await
            .map_err(status_from_pki_error)?;
        Ok(Response::new(PkiRotateCrlResponse {
            crl: crl_bytes(format, &material),
            format: format as i32,
            crl_number: material.crl_number,
            this_update: material.this_update,
            next_update: material.next_update,
        }))
    }

    async fn tidy(
        &self,
        request: Request<PkiTidyRequest>,
    ) -> Result<Response<PkiTidyResponse>, Status> {
        let req = request.into_inner();
        let result = PkiService::tidy(self, req.retention_seconds)
            .await
            .map_err(status_from_pki_error)?;
        Ok(Response::new(PkiTidyResponse {
            retention_seconds: result.retention_seconds,
            removed_certs: result.removed_certs,
        }))
    }
}

// ──── BaseService：插件生命周期（把「CA get-or-create」变成启动动作）────
//
// 迁移前：CA 的 get-or-create 在 serve 装配期内联 await —— 与服务生命周期
// 无关（服务没启动也会执行），失败只有一行 `warn!`，外界看不到。
// 迁移后：它是本服务 `start()` 的动作：
// - 成功 → `health_check()` 真（CA 已就绪）；
// - 失败 → 记 warn 并保持服务可达（与历史行为一致），但 `health_check()` 为假，
//   由统一健康面（`PluginManager::health_check_all` / `coord.plugin.Plugin/List`）
//   暴露出去。

impl PkiService {
    /// 默认 CA Common Name（与历史 serve 装配期使用的值一致）。
    pub const DEFAULT_CA_COMMON_NAME: &'static str = "coord-agent-ca";
}

#[async_trait::async_trait]
impl crate::service::BaseService for PkiService {
    fn name(&self) -> &'static str {
        "pki"
    }

    async fn start(&self) -> crate::service::ServiceResult<()> {
        match self.init_ca(Self::DEFAULT_CA_COMMON_NAME).await {
            Ok(()) => {
                // 指纹与取根证书方式在**启动日志里各出现一次**（不进采样循环）：
                // 开发者/运维据此把日志与导出的 ca.pem 对上。
                match self.ca_fingerprint_sha256() {
                    Ok(fingerprint) => tracing::info!(
                        "PKI CA auto-initialized: CN={} sha256={fingerprint}",
                        Self::DEFAULT_CA_COMMON_NAME
                    ),
                    Err(e) => tracing::warn!(
                        "PKI CA auto-initialized: CN={} (fingerprint unavailable: {e})",
                        Self::DEFAULT_CA_COMMON_NAME
                    ),
                }
                tracing::info!(
                    "PKI root certificate: `coord pki ca --out ca.pem` (or GetCaCert); \
                     PKI is for dev/test/internal verification only"
                );
                Ok(())
            }
            Err(e) => {
                // 保持服务可达（历史行为：装配失败不阻塞 agent 启动），
                // 但健康检查会如实反映「CA 未就绪」。
                tracing::warn!(
                    "PKI CA auto-init failed (service stays reachable but reports unhealthy): {e}"
                );
                Ok(())
            }
        }
    }

    async fn stop(&self) -> crate::service::ServiceResult<()> {
        // CA 材料只驻留内存，无外部资源需释放（共享 store 归调用方）。
        Ok(())
    }

    fn health_check(&self) -> bool {
        self.ca.read().is_some()
    }
}

// ──── tests ────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_pki() -> PkiService {
        PkiService::new(PkiConfig::default()).expect("create")
    }

    fn shared_keys() -> Arc<TransitKeyRegistry> {
        Arc::new(TransitKeyRegistry::in_memory().expect("in-memory keys"))
    }

    /// 同一集群的多个 PKI 实例必须共享密钥档案（生产是 KvTransitKeyStore）
    fn make_pki_shared(store: Arc<MemoryPkiStore>, keys: &Arc<TransitKeyRegistry>) -> PkiService {
        PkiService::with_store_and_keys(PkiConfig::default(), store, keys.clone())
    }

    #[tokio::test]
    async fn test_init_ca_and_issue() {
        let pki = make_pki();
        pki.init_ca("Test CA").await.expect("init");

        let cert = pki.issue_cert("test.local", 0).await.expect("issue");
        assert_eq!(cert.common_name, "test.local");
        assert!(!cert.cert_pem.is_empty());
        assert!(cert.key_name.starts_with("pki/"));
        assert!(cert.key_version >= 1);
        assert_eq!(cert.status, CertStatus::Active);
    }

    /// 非法 PEM 必须映射 `INVALID_ARGUMENT`（调用方可修正），不能是 `INTERNAL`。
    #[test]
    fn test_cert_parse_error_maps_to_invalid_argument() {
        let status = status_from_pki_error(PkiError::CertParse("bad pem".into()));
        assert_eq!(status.code(), tonic::Code::InvalidArgument, "{status:?}");
    }

    /// 由本 CA 真实签名、但已过期的证书 ⇒ false。
    #[tokio::test]
    async fn test_verify_rejects_expired_ca_signed_cert() {
        let pki = make_pki();
        pki.init_ca("Expired Verify CA").await.expect("init");
        let (ca_key_name, ca_key_version, ca_cert_pem) = {
            let ca = pki.ca.read();
            let ca = ca.as_ref().expect("ca loaded");
            (ca.key_name.clone(), ca.key_version, ca.cert_pem.clone())
        };
        let ca_key_pem = pki
            .keys
            .private_key_pem(&ca_key_name, ca_key_version)
            .await
            .expect("ca key");
        let cert_pem = tokio::task::spawn_blocking(move || {
            let ca_key = KeyPair::from_pem(&ca_key_pem).expect("ca key pair");
            let issuer = rcgen::Issuer::from_ca_cert_pem(&ca_cert_pem, ca_key).expect("issuer");
            let leaf_key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("leaf key");
            let now = OffsetDateTime::now_utc();
            let mut params = CertificateParams::default();
            params
                .distinguished_name
                .push(DnType::CommonName, "expired.verify.local");
            params.not_before = now - time::Duration::hours(2);
            params.not_after = now - time::Duration::hours(1);
            params
                .signed_by(&leaf_key, &issuer)
                .expect("sign expired cert")
                .pem()
        })
        .await
        .expect("join");

        assert!(
            !pki.verify_cert(&cert_pem).await.expect("verify"),
            "过期证书必须被拒绝"
        );
    }

    #[test]
    fn test_external_status_marks_expired_over_stored_state() {
        let now = now_unix();
        assert_eq!(status_to_str_at(CertStatus::Active, now + 60), "active");
        assert_eq!(status_to_str_at(CertStatus::Retired, now + 60), "retired");
        assert_eq!(status_to_str_at(CertStatus::Active, now - 1), "expired");
        assert_eq!(status_to_str_at(CertStatus::Retired, now - 1), "expired");
    }

    #[tokio::test]
    async fn test_verify_valid_cert() {
        let pki = make_pki();
        pki.init_ca("Verify CA").await.expect("init");

        let cert = pki.issue_cert("verify.local", 0).await.expect("issue");
        assert!(pki.verify_cert(&cert.cert_pem).await.expect("verify"));
    }

    #[tokio::test]
    async fn test_ca_fingerprint_format_and_stability() {
        let pki = make_pki();
        pki.init_ca("Fingerprint CA").await.expect("init");
        let fp = pki.ca_fingerprint_sha256().expect("fingerprint");
        let parts: Vec<&str> = fp.split(':').collect();
        assert_eq!(parts.len(), 32, "SHA-256 = 32 bytes: {fp}");
        assert!(
            parts
                .iter()
                .all(|p| p.len() == 2 && p.chars().all(|c| c.is_ascii_hexdigit())),
            "uppercase hex pairs: {fp}"
        );
        assert_eq!(
            fp,
            pki.ca_fingerprint_sha256().expect("stable"),
            "same CA material => same fingerprint"
        );
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
        pki.init_ca("Test CA")
            .await
            .expect("second init (idempotent)");
        let cert = pki
            .issue_cert("test.local", 0)
            .await
            .expect("issue after init");
        assert!(!cert.cert_pem.is_empty());
    }

    // ──── G-PKI-1：过期语义（到期 = 换新触发）────

    /// 合成一条已过期的当前记录（PEM 仅为占位——替换路径不校验旧证书）
    fn expired_record(cn: &str, serial: &str) -> CertRecord {
        CertRecord {
            common_name: cn.to_string(),
            cert_pem: "-----BEGIN CERTIFICATE-----\nEXPIRED\n-----END CERTIFICATE-----".into(),
            key_name: "pki/expired".into(),
            key_version: 1,
            not_before: 1_700_000_000,
            not_after: 1_700_000_001,
            serial: serial.to_string(),
            status: CertStatus::Active,
            parent_serial: None,
            dns_names: Vec::new(),
            ip_addresses: Vec::new(),
            spki_sha256: String::new(),
            usage: CertUsage::Both,
        }
    }

    /// G-PKI-1：IssueCert 对过期记录走替换路径——新 serial / 新密钥，绝不返回过期记录
    #[tokio::test]
    async fn test_issue_cert_replaces_expired_record() {
        let store = Arc::new(MemoryPkiStore::new());
        let pki = PkiService::with_store(PkiConfig::default(), store.clone()).expect("pki");
        pki.init_ca("Expiry CA").await.expect("init");

        let expired = expired_record("svc-expired.local", "expired-0x1");
        store
            .create_cert("svc-expired.local", &expired)
            .await
            .expect("seed expired");

        let cert = pki
            .issue_cert("svc-expired.local", 0)
            .await
            .expect("issue over expired");

        assert_ne!(cert.serial, expired.serial, "必须换新 serial");
        assert!(cert.key_version >= 1);
        assert!(
            pki.verify_cert(&cert.cert_pem).await.expect("verify"),
            "换新后的证书必须可验签"
        );
        assert_eq!(
            cert.parent_serial.as_deref(),
            Some("expired-0x1"),
            "轮换链指向被替换的 serial"
        );

        // store 中当前 active 已是新证书；过期记录进入历史（按 serial 仍可寻回）
        let (active, version) = store
            .get_cert_with_version("svc-expired.local")
            .await
            .expect("read")
            .expect("active");
        assert_eq!(active.serial, cert.serial);
        assert!(version >= 2, "替换后版本递增");
        let old = store
            .get_cert_by_serial("expired-0x1")
            .await
            .expect("by serial")
            .expect("history keeps expired record");
        assert_eq!(old.status, CertStatus::Retired);
    }

    /// G-PKI-1：RotateCert 对过期记录同样换新（不返回过期记录）
    #[tokio::test]
    async fn test_rotate_cert_replaces_expired_record() {
        let store = Arc::new(MemoryPkiStore::new());
        let pki = PkiService::with_store(PkiConfig::default(), store.clone()).expect("pki");
        pki.init_ca("Expiry CA").await.expect("init");

        store
            .create_cert(
                "svc-expired.local",
                &expired_record("svc-expired.local", "expired-0x2"),
            )
            .await
            .expect("seed expired");

        let cert = pki
            .rotate_cert("svc-expired.local", 0)
            .await
            .expect("rotate over expired");
        assert_ne!(cert.serial, "expired-0x2");
        assert!(cert.key_name.starts_with("pki/"));
        assert!(pki.verify_cert(&cert.cert_pem).await.expect("verify"));
    }

    /// G-PKI-1：RenewCert(serial) 为按序列号恢复入口——过期 serial 也能恢复出新证书
    #[tokio::test]
    async fn test_renew_cert_recovers_from_expired_serial() {
        let store = Arc::new(MemoryPkiStore::new());
        let pki = PkiService::with_store(PkiConfig::default(), store.clone()).expect("pki");
        pki.init_ca("Recover CA").await.expect("init");

        store
            .create_cert(
                "svc-recover.local",
                &expired_record("svc-recover.local", "0e3"),
            )
            .await
            .expect("seed expired");

        let recovered = pki
            .renew_cert("0e3", 0)
            .await
            .expect("renew by expired serial");
        assert_eq!(recovered.common_name, "svc-recover.local");
        assert_ne!(recovered.serial, "0e3");
        assert!(pki.verify_cert(&recovered.cert_pem).await.expect("verify"));

        // 恢复后旧 serial 已入历史，再次按同一 serial 恢复仍可用（恢复到最新）
        let again = pki
            .renew_cert("0e3", 0)
            .await
            .expect("renew again from history");
        assert_ne!(again.serial, recovered.serial);
    }

    /// G-PKI-1 验收锚点：短 TTL——过期前幂等；过期后 issue 得到新 serial/新密钥
    #[tokio::test]
    async fn test_issue_cert_short_ttl_expiry_reissues() {
        let pki = make_pki();
        pki.init_ca("Short TTL CA").await.expect("init");

        let first = pki.issue_cert("svc-ttl.local", 1).await.expect("issue");

        // 过期前 + 同 TTL：幂等返回同一证书（参数一致才幂等）
        let same = pki
            .issue_cert("svc-ttl.local", 1)
            .await
            .expect("issue again");
        assert_eq!(first.serial, same.serial, "过期前同参数必须幂等");

        // 越过 TTL（1s）→ 再次 issue 必须换新
        // 留出 2 秒级余量：not_after 为秒级截断，1s TTL 在跨秒边界上最多需 ~2s 才判过期
        tokio::time::sleep(std::time::Duration::from_millis(2200)).await;
        let renewed = pki
            .issue_cert("svc-ttl.local", 0)
            .await
            .expect("issue after expiry");
        assert_ne!(renewed.serial, first.serial, "过期后必须换新 serial");
        assert!(renewed.key_version >= 1);
        assert!(pki.verify_cert(&renewed.cert_pem).await.expect("verify"));
    }

    /// G-PKI-1：并发替换（版本化 CAS）——双 agent 同时 rotate：都成功、无丢更新
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_rotate_no_lost_update() {
        let store = Arc::new(MemoryPkiStore::new());
        let keys = shared_keys();

        let seed_svc = make_pki_shared(store.clone(), &keys);
        seed_svc
            .init_ca("Concurrent Rotate CA")
            .await
            .expect("init");
        let first = seed_svc
            .issue_cert("svc-conc.local", 0)
            .await
            .expect("seed");

        let pki_a = make_pki_shared(store.clone(), &keys);
        let pki_b = make_pki_shared(store.clone(), &keys);
        pki_a.init_ca("Concurrent Rotate CA").await.expect("init A");
        pki_b.init_ca("Concurrent Rotate CA").await.expect("init B");

        let a = tokio::spawn(async move { pki_a.rotate_cert("svc-conc.local", 0).await });
        let b = tokio::spawn(async move { pki_b.rotate_cert("svc-conc.local", 0).await });
        let cert_a = a.await.expect("join A").expect("rotate A");
        let cert_b = b.await.expect("join B").expect("rotate B");

        assert_ne!(cert_a.serial, cert_b.serial, "并发轮换产生两个不同新证书");

        // 无丢更新：两个新证书都在 store 中（一个 active、另一个 retired），旧证书也在
        let check_svc = make_pki_shared(store.clone(), &keys);
        let all = check_svc.list_certs("svc-conc.local").await.expect("list");
        let serials: Vec<&str> = all.iter().map(|c| c.serial.as_str()).collect();
        assert!(serials.contains(&first.serial.as_str()), "旧证书保留");
        assert!(serials.contains(&cert_a.serial.as_str()), "A 的证书保留");
        assert!(serials.contains(&cert_b.serial.as_str()), "B 的证书保留");

        // 重启一致性：新实例（同一 store）取回当前 active，且为本次轮换之一
        let pki_restart = make_pki_shared(store.clone(), &keys);
        pki_restart
            .init_ca("Concurrent Rotate CA")
            .await
            .expect("restart init");
        let current = pki_restart
            .issue_cert("svc-conc.local", 0)
            .await
            .expect("restart issue");
        assert!(
            current.serial == cert_a.serial || current.serial == cert_b.serial,
            "重启后 active 必须为本次轮换之一"
        );
    }

    /// G-PKI-2：到期快照——窗口内即将到期计数（排除已过期）与 active 总数
    #[tokio::test]
    async fn test_cert_expiry_snapshot_counts_window() {
        let store = Arc::new(MemoryPkiStore::new());
        let config = PkiConfig {
            expiry_warn_hours: 6,
            ..Default::default()
        };
        let pki = PkiService::with_store(config, store.clone()).expect("pki");
        pki.init_ca("Snapshot CA").await.expect("init");

        // 1h TTL → 窗口内；48h → 不在窗口
        pki.issue_cert("short.local", 3600)
            .await
            .expect("issue short");
        pki.issue_cert("long.local", 48 * 3600)
            .await
            .expect("issue long");
        // 已过期（合成）→ 计入总数、不计入 expiring_soon
        store
            .create_cert("gone.local", &expired_record("gone.local", "expired-0x5"))
            .await
            .expect("seed");

        let (total, expiring) = pki.cert_expiry_snapshot().await.expect("snapshot");
        assert_eq!(total, 3, "active 总数含已过期记录");
        assert_eq!(expiring, 1, "仅 1h TTL 计入窗口（已过期不算）");
        assert_eq!(pki.expiry_warn_hours(), 6);
    }

    /// G-PKI-1：并发过期替换——双 agent 同时 issue(已过期)：都拿到未过期证书
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_issue_over_expired_returns_unexpired() {
        let store = Arc::new(MemoryPkiStore::new());
        let keys = shared_keys();
        let pki_a = make_pki_shared(store.clone(), &keys);
        let pki_b = make_pki_shared(store.clone(), &keys);
        pki_a.init_ca("Concurrent Expired CA").await.expect("init");
        pki_b.init_ca("Concurrent Expired CA").await.expect("init");

        store
            .create_cert(
                "svc-exp.local",
                &expired_record("svc-exp.local", "expired-0x9"),
            )
            .await
            .expect("seed expired");

        let a = tokio::spawn(async move { pki_a.issue_cert("svc-exp.local", 0).await });
        let b = tokio::spawn(async move { pki_b.issue_cert("svc-exp.local", 0).await });
        let cert_a = a.await.expect("join A").expect("issue A");
        let cert_b = b.await.expect("join B").expect("issue B");

        // 双方都不得拿到过期记录；两方各自拿到的是未过期的新证书（同一胜者或先后替换）
        assert_ne!(cert_a.serial, "expired-0x9");
        assert_ne!(cert_b.serial, "expired-0x9");
        assert!(cert_a.key_version >= 1);
        assert!(cert_b.key_version >= 1);
    }

    // ──── SAN 签发（#117）────

    /// 解析证书 SAN 集合（x509-parser 直接读取扩展，不经 OpenSSL 的 CN 回退）。
    fn cert_sans(cert_pem: &str) -> (Vec<String>, Vec<String>) {
        use x509_parser::extensions::GeneralName;

        let der = pem_to_der(cert_pem).expect("pem -> der");
        let (_rest, cert) = x509_parser::parse_x509_certificate(&der).expect("parse x509");
        let mut dns = Vec::new();
        let mut ips = Vec::new();
        if let Some(ext) = cert.subject_alternative_name().expect("san extension") {
            for name in &ext.value.general_names {
                match name {
                    GeneralName::DNSName(name) => dns.push((*name).to_string()),
                    GeneralName::IPAddress(bytes) => match bytes.len() {
                        4 => ips.push(
                            IpAddr::from([bytes[0], bytes[1], bytes[2], bytes[3]]).to_string(),
                        ),
                        16 => {
                            let mut octets = [0u8; 16];
                            octets.copy_from_slice(bytes);
                            ips.push(IpAddr::from(octets).to_string());
                        }
                        other => panic!("unexpected IP SAN length: {other}"),
                    },
                    _ => {}
                }
            }
        }
        dns.sort();
        ips.sort();
        (dns, ips)
    }

    /// 只认 SAN 的校验方（与 Go ≥1.15 / rustls / 浏览器 / Envoy 同口径）。
    fn webpki_verify(
        ca_pem: &str,
        leaf_pem: &str,
        server_name: &str,
        at_unix: u64,
    ) -> Result<(), String> {
        use rustls_pki_types::{CertificateDer, ServerName, UnixTime};

        let ca_der = CertificateDer::from(pem_to_der(ca_pem).expect("ca der"));
        let leaf_der = CertificateDer::from(pem_to_der(leaf_pem).expect("leaf der"));
        let anchor = webpki::anchor_from_trusted_cert(&ca_der).expect("trust anchor");
        let anchors = [anchor];
        let cert = webpki::EndEntityCert::try_from(&leaf_der).expect("end entity");
        let name = ServerName::try_from(server_name).expect("server name");
        cert.verify_for_usage(
            webpki::ALL_VERIFICATION_ALGS,
            &anchors,
            &[],
            UnixTime::since_unix_epoch(std::time::Duration::from_secs(at_unix)),
            webpki::KeyUsage::server_auth(),
            None,
            None,
        )
        .map_err(|e| e.to_string())?;
        cert.verify_is_valid_for_subject_name(&name)
            .map_err(|e| e.to_string())
    }

    #[tokio::test]
    async fn test_issue_cert_defaults_san_from_cn() {
        let pki = make_pki();
        pki.init_ca("SAN CA").await.expect("init");

        let cert = pki.issue_cert("orders-api", 0).await.expect("issue");
        let (dns, ips) = cert_sans(&cert.cert_pem);
        assert_eq!(dns, vec!["orders-api"], "CN 必须派生为 DNS SAN");
        assert!(ips.is_empty());
    }

    #[tokio::test]
    async fn test_issue_cert_explicit_san_matches_request() {
        let pki = make_pki();
        pki.init_ca("SAN CA").await.expect("init");

        let requested = SanSpec::new(
            vec!["orders-api".into(), "orders-api.svc".into()],
            vec!["127.0.0.1".into(), "::1".into()],
        );
        let cert = pki
            .issue_cert_with_san("orders-api", 3600, requested.clone())
            .await
            .expect("issue with san");

        let (dns, ips) = cert_sans(&cert.cert_pem);
        let mut want_dns = requested.dns_names.clone();
        want_dns.sort();
        let mut want_ips = requested.ip_addresses.clone();
        want_ips.sort();
        assert_eq!(
            (dns, ips),
            (want_dns, want_ips),
            "SAN 集合必须与请求精确相等"
        );
    }

    #[tokio::test]
    async fn test_issue_cert_rejects_non_ascii_cn_without_san() {
        let pki = make_pki();
        pki.init_ca("SAN CA").await.expect("init");

        match pki.issue_cert("订单-api", 0).await {
            Err(PkiError::InvalidArgument(msg)) => {
                assert!(msg.contains("dns_names"), "错误提示必须指向显式 SAN: {msg}");
            }
            other => panic!("expected InvalidArgument, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_issue_cert_rejects_invalid_ip_san() {
        let pki = make_pki();
        pki.init_ca("SAN CA").await.expect("init");

        match pki
            .issue_cert_with_san(
                "orders-api",
                0,
                SanSpec::new(vec![], vec!["999.0.0.1".into()]),
            )
            .await
        {
            Err(PkiError::InvalidArgument(_)) => {}
            other => panic!("expected InvalidArgument, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_rotate_cert_preserves_san_when_not_specified() {
        let pki = make_pki();
        pki.init_ca("SAN Rotate CA").await.expect("init");

        let requested = SanSpec::new(vec!["orders-api".into()], vec!["127.0.0.1".into()]);
        pki.issue_cert_with_san("orders-api", 0, requested)
            .await
            .expect("issue");
        let rotated = pki.rotate_cert("orders-api", 0).await.expect("rotate");

        let (dns, ips) = cert_sans(&rotated.cert_pem);
        assert_eq!(dns, vec!["orders-api"], "轮换后 DNS SAN 不得丢");
        assert_eq!(ips, vec!["127.0.0.1"], "轮换后 IP SAN 不得丢");
    }

    #[tokio::test]
    async fn test_renew_cert_preserves_san() {
        let pki = make_pki();
        pki.init_ca("SAN Renew CA").await.expect("init");

        let issued = pki
            .issue_cert_with_san(
                "orders-api",
                0,
                SanSpec::new(vec!["orders-api.svc".into()], vec![]),
            )
            .await
            .expect("issue");
        let renewed = pki.renew_cert(&issued.serial, 0).await.expect("renew");

        let (dns, _) = cert_sans(&renewed.cert_pem);
        assert_eq!(dns, vec!["orders-api.svc"], "RenewCert 必须继承 SAN");
    }

    #[tokio::test]
    async fn test_issued_cert_passes_webpki_positive_and_fails_other_name() {
        let pki = make_pki();
        pki.init_ca("WebPKI CA").await.expect("init");
        let cert = pki
            .issue_cert_with_san(
                "orders-api",
                3600,
                SanSpec::new(
                    vec!["orders-api".into(), "orders-api.svc".into()],
                    vec!["127.0.0.1".into()],
                ),
            )
            .await
            .expect("issue");
        let ca = pki.ca_cert_pem().expect("ca");
        let now = now_unix() as u64;

        webpki_verify(&ca, &cert.cert_pem, "orders-api", now)
            .expect("rustls-webpki must accept the requested DNS name");
        webpki_verify(&ca, &cert.cert_pem, "orders-api.svc", now)
            .expect("additional DNS SAN must verify");
        webpki_verify(&ca, &cert.cert_pem, "127.0.0.1", now).expect("IP SAN must verify");

        // 负控制：错误名字必须失败——否则"通过"不能证明 SAN 生效。
        assert!(
            webpki_verify(&ca, &cert.cert_pem, "orders-api.evil", now).is_err(),
            "wrong name must fail"
        );
    }

    // ──── 参数一致才幂等（#119）────

    #[tokio::test]
    async fn test_issue_cert_same_params_stays_idempotent() {
        let pki = make_pki();
        pki.init_ca("Param Idem CA").await.expect("init");
        let san = SanSpec::new(vec!["orders-api".into()], vec!["127.0.0.1".into()]);

        let first = pki
            .issue_cert_with_san("orders-api", 3600, san.clone())
            .await
            .expect("first");
        let second = pki
            .issue_cert_with_san("orders-api", 3600, san)
            .await
            .expect("second");

        assert_eq!(first.serial, second.serial, "同 SAN/TTL 必须幂等");
        assert_eq!(first.cert_pem, second.cert_pem);
    }

    #[tokio::test]
    async fn test_issue_cert_different_ttl_reissues_and_keeps_history() {
        let store = Arc::new(MemoryPkiStore::new());
        let pki = PkiService::with_store(PkiConfig::default(), store.clone()).expect("pki");
        pki.init_ca("Param TTL CA").await.expect("init");

        let first = pki.issue_cert("orders-api", 3600).await.expect("first");
        let second = pki.issue_cert("orders-api", 7200).await.expect("second");

        assert_ne!(first.serial, second.serial, "TTL 变化必须换新证书");
        assert_eq!(
            second.not_after - second.not_before,
            7200,
            "新证书 TTL 必须与本次请求一致"
        );

        let old = store
            .get_cert_by_serial(&first.serial)
            .await
            .expect("by serial")
            .expect("retired history keeps old cert");
        assert_eq!(old.status, CertStatus::Retired);
    }

    #[tokio::test]
    async fn test_issue_cert_different_san_reissues() {
        let store = Arc::new(MemoryPkiStore::new());
        let pki = PkiService::with_store(PkiConfig::default(), store.clone()).expect("pki");
        pki.init_ca("Param SAN CA").await.expect("init");

        let first = pki
            .issue_cert_with_san(
                "orders-api",
                0,
                SanSpec::new(vec!["old.svc".into()], vec![]),
            )
            .await
            .expect("first");
        let second = pki
            .issue_cert_with_san(
                "orders-api",
                0,
                SanSpec::new(vec!["new.svc".into()], vec![]),
            )
            .await
            .expect("second");

        assert_ne!(first.serial, second.serial, "SAN 变化必须换新证书");
        let (dns, _) = cert_sans(&second.cert_pem);
        assert_eq!(dns, vec!["new.svc"], "SAN 必须与本次请求一致");

        let old = store
            .get_cert_by_serial(&first.serial)
            .await
            .expect("by serial")
            .expect("retired history keeps old cert");
        assert_eq!(old.status, CertStatus::Retired);
    }

    #[tokio::test]
    async fn test_issue_cert_upgrades_legacy_record_without_san() {
        let store = Arc::new(MemoryPkiStore::new());
        let pki = PkiService::with_store(PkiConfig::default(), store.clone()).expect("pki");
        pki.init_ca("Legacy CA").await.expect("init");

        // 升级前的记录：无 SAN 字段（反序列化为空），未过期、TTL 与默认值一致。
        let now = now_unix();
        let legacy = CertRecord {
            common_name: "legacy-svc".into(),
            cert_pem: "-----BEGIN CERTIFICATE-----\nLEGACY\n-----END CERTIFICATE-----".into(),
            key_name: "pki/legacy-svc".into(),
            key_version: 1,
            not_before: now,
            not_after: now + 86_400,
            serial: "legacy-0x1".into(),
            status: CertStatus::Active,
            parent_serial: None,
            dns_names: Vec::new(),
            ip_addresses: Vec::new(),
            spki_sha256: String::new(),
            usage: CertUsage::Both,
        };
        store
            .create_cert("legacy-svc", &legacy)
            .await
            .expect("seed legacy");

        let upgraded = pki.issue_cert("legacy-svc", 0).await.expect("issue");
        assert_ne!(upgraded.serial, legacy.serial, "无 SAN 旧记录必须被替换");
        let (dns, _) = cert_sans(&upgraded.cert_pem);
        assert_eq!(dns, vec!["legacy-svc"], "替换后必须带 CN 派生 SAN");
    }

    // ──── 吊销面（ADR-0020）────

    fn parse_crl(
        material: &CrlMaterial,
    ) -> x509_parser::revocation_list::CertificateRevocationList<'_> {
        let (_rest, crl) = x509_parser::parse_x509_crl(&material.der).expect("parse generated CRL");
        crl
    }

    fn crl_serials(material: &CrlMaterial) -> Vec<String> {
        parse_crl(material)
            .iter_revoked_certificates()
            .map(|entry| hex::encode(entry.serial().to_bytes_be()))
            .collect()
    }

    async fn csr_for(common_name: &str) -> Vec<u8> {
        let key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("csr key");
        let mut params = CertificateParams::default();
        params
            .distinguished_name
            .push(DnType::CommonName, common_name);
        params
            .serialize_request(&key)
            .expect("serialize CSR")
            .der()
            .to_vec()
    }

    /// 记录 serial = 证书内嵌 serial（CRL / 第三方验签的同一口径）
    #[tokio::test]
    async fn test_record_serial_matches_certificate_embedded_serial() {
        let pki = make_pki();
        pki.init_ca("Serial CA").await.expect("init");
        let cert = pki.issue_cert("serial.local", 0).await.expect("issue");
        let embedded = cert_serial_hex(&cert.cert_pem).expect("parse cert");
        assert_eq!(embedded, cert.serial, "记录 serial 必须可写入 CRL");
    }

    /// 吊销阻断隐式重签（IssueCert/SignCsr/Renew），显式 RotateCert 仍可用
    #[tokio::test]
    async fn test_revoke_blocks_implicit_reissue_and_allows_explicit_rotate() {
        let pki = make_pki();
        pki.init_ca("Revoke CA").await.expect("init");
        let cert = pki.issue_cert("revoke.local", 0).await.expect("issue");
        assert!(pki.verify_cert(&cert.cert_pem).await.expect("verify"));

        let revocation = pki
            .revoke_cert(&cert.serial, RevocationReason::KeyCompromise, "ops@example")
            .await
            .expect("revoke");
        assert_eq!(revocation.cert_serial, cert.serial);
        assert_eq!(revocation.reason, RevocationReason::KeyCompromise);
        assert_eq!(revocation.by, "ops@example");

        // 幂等：重复吊销保留首次的 reason / revoked_at / by
        let again = pki
            .revoke_cert(&cert.serial, RevocationReason::Superseded, "another")
            .await
            .expect("revoke again");
        assert_eq!(again.reason, RevocationReason::KeyCompromise);
        assert_eq!(again.by, "ops@example");
        assert_eq!(again.revoked_at, revocation.revoked_at);

        // 读面与验签
        let status = pki.get_cert_status(&cert.serial).await.expect("status");
        assert_eq!(status.status, "revoked");
        assert_eq!(status.cert_serial, cert.serial);
        assert_eq!(
            status.revocation.as_ref().expect("revocation").by,
            "ops@example"
        );
        assert!(
            pki.get_cert_by_cn("revoke.local")
                .await
                .expect("by cn")
                .is_none(),
            "已吊销的 active 证书不是「当前有效证书」"
        );
        let listed = pki.list_certs("revoke.local").await.expect("list");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].status, CertStatus::Revoked);
        assert_eq!(
            pki.get_cert_by_serial(&cert.serial)
                .await
                .expect("by serial")
                .expect("record")
                .status,
            CertStatus::Revoked
        );
        assert!(
            !pki.verify_cert(&cert.cert_pem)
                .await
                .expect("verify revoked"),
            "VerifyCert 必须拒绝已吊销证书"
        );

        // 负控制：IssueCert 不得静默重签
        match pki.issue_cert("revoke.local", 0).await {
            Err(PkiError::FailedPrecondition(msg)) => {
                assert!(msg.contains("revoked"), "错误必须点名吊销: {msg}");
            }
            other => panic!("expected FailedPrecondition, got {other:?}"),
        }
        // 负控制：SignCsr 不得复活同一 CN
        let csr = csr_for("revoke.local").await;
        assert!(matches!(
            pki.sign_csr(&csr, 0, CertUsage::Both).await,
            Err(PkiError::FailedPrecondition(_))
        ));
        // 负控制：RenewCert 不得复活已吊销 serial
        assert!(matches!(
            pki.renew_cert(&cert.serial, 0).await,
            Err(PkiError::FailedPrecondition(_))
        ));

        // 显式路径：RotateCert 换新身份，旧 serial 仍永久 revoked
        let rotated = pki
            .rotate_cert("revoke.local", 0)
            .await
            .expect("explicit rotate");
        assert_ne!(rotated.serial, cert.serial);
        assert!(pki
            .verify_cert(&rotated.cert_pem)
            .await
            .expect("new cert verifies"));
        assert_eq!(
            pki.get_cert_status(&cert.serial)
                .await
                .expect("old status")
                .status,
            "revoked"
        );
        assert_eq!(
            pki.get_cert_status(&rotated.serial)
                .await
                .expect("new status")
                .status,
            "active"
        );
        // 轮换后 get-or-create 恢复幂等
        let same = pki
            .issue_cert("revoke.local", 0)
            .await
            .expect("issue after rotate");
        assert_eq!(same.serial, rotated.serial);
    }

    /// 续期任意历史 serial 也不能绕过「当前身份已吊销」
    #[tokio::test]
    async fn test_renew_history_serial_cannot_bypass_revoked_current_identity() {
        let pki = make_pki();
        pki.init_ca("Renew Bypass CA").await.expect("init");
        let first = pki.issue_cert("bypass.local", 0).await.expect("issue");
        let second = pki.rotate_cert("bypass.local", 0).await.expect("rotate");
        pki.revoke_cert(&second.serial, RevocationReason::KeyCompromise, "ops")
            .await
            .expect("revoke");

        // 续期历史 serial 会替换掉已吊销的当前身份 ⇒ 必须拒绝
        assert!(
            matches!(
                pki.renew_cert(&first.serial, 0).await,
                Err(PkiError::FailedPrecondition(_))
            ),
            "续期历史 serial 不得隐式复活已吊销的当前身份"
        );

        // 显式轮换仍可用
        let third = pki
            .rotate_cert("bypass.local", 0)
            .await
            .expect("explicit rotate");
        assert_ne!(third.serial, second.serial);
        assert_eq!(
            pki.get_cert_status(&second.serial)
                .await
                .expect("status")
                .status,
            "revoked"
        );
    }

    #[tokio::test]
    async fn test_revoke_unknown_serial_is_not_found() {
        let pki = make_pki();
        pki.init_ca("Revoke Unknown CA").await.expect("init");
        assert!(matches!(
            pki.revoke_cert("deadbeef", RevocationReason::Unspecified, "ops")
                .await,
            Err(PkiError::CertNotFound(_))
        ));
        assert!(matches!(
            pki.get_cert_status("deadbeef").await,
            Err(PkiError::CertNotFound(_))
        ));
    }

    /// 双实例共享 store + 密钥档案：A 吊销 ⇒ B 的 VerifyCert 与 CRL 立即生效
    #[tokio::test]
    async fn test_dual_instance_revocation_visible_across_agents() {
        let store = Arc::new(MemoryPkiStore::new());
        let keys = shared_keys();
        let pki_a = make_pki_shared(store.clone(), &keys);
        let pki_b = make_pki_shared(store.clone(), &keys);
        pki_a.init_ca("Shared Revoke CA").await.expect("init A");
        pki_b.init_ca("Shared Revoke CA").await.expect("init B");

        let cert = pki_a.issue_cert("shared.local", 0).await.expect("issue");
        assert!(pki_b.verify_cert(&cert.cert_pem).await.expect("B verify"));

        pki_a
            .revoke_cert(&cert.serial, RevocationReason::KeyCompromise, "ops-a")
            .await
            .expect("A revoke");

        assert!(
            !pki_b.verify_cert(&cert.cert_pem).await.expect("B verify"),
            "B 必须拒绝 A 吊销的 serial"
        );
        assert_eq!(
            pki_b
                .get_cert_status(&cert.serial)
                .await
                .expect("B status")
                .status,
            "revoked"
        );
        let crl = pki_b.get_crl().await.expect("B crl");
        assert!(crl_serials(&crl).contains(&cert.serial));

        // 重启（同 store + 同密钥档案）后吊销事实仍在
        let pki_restart = make_pki_shared(store.clone(), &keys);
        pki_restart
            .init_ca("Shared Revoke CA")
            .await
            .expect("restart init");
        assert!(!pki_restart
            .verify_cert(&cert.cert_pem)
            .await
            .expect("restart verify"));
        assert_eq!(
            pki_restart
                .get_cert_status(&cert.serial)
                .await
                .expect("restart status")
                .status,
            "revoked"
        );
    }

    /// CRL：CA 签名、条目与 reason 正确、编号单调且「编号 ↔ 内容」一致
    #[tokio::test]
    async fn test_crl_entries_signature_and_monotonic_numbers() {
        let pki = make_pki();
        pki.init_ca("CRL CA").await.expect("init");
        let a = pki.issue_cert("crl-a.local", 0).await.expect("issue a");
        let b = pki.issue_cert("crl-b.local", 0).await.expect("issue b");

        let empty = pki.get_crl().await.expect("crl");
        assert_eq!(empty.crl_number, 1);
        assert!(empty.pem.contains("BEGIN X509 CRL"));
        assert_eq!(crl_serials(&empty).len(), 0);
        let second = pki.get_crl().await.expect("same crl");
        assert_eq!(
            (second.crl_number, second.this_update, second.next_update),
            (empty.crl_number, empty.this_update, empty.next_update),
            "内容未变 ⇒ 复用同一编号与时间窗口（ECDSA 签名每次不同，不比较字节）"
        );
        assert_eq!(crl_serials(&second), crl_serials(&empty));
        assert!(empty.next_update > empty.this_update);

        // CRL 由本 CA 公钥验签
        let ca_der = pem_to_der(&pki.ca_cert_pem().expect("ca")).expect("ca der");
        let (_rest, ca) = x509_parser::parse_x509_certificate(&ca_der).expect("parse ca");
        parse_crl(&empty)
            .verify_signature(ca.public_key())
            .expect("CRL signature must verify with the CA key");

        pki.revoke_cert(&a.serial, RevocationReason::CessationOfOperation, "ops")
            .await
            .expect("revoke");
        let after = pki.get_crl().await.expect("crl after revoke");
        assert_eq!(after.crl_number, 2, "吊销集合变化 ⇒ 新编号");
        let serials = crl_serials(&after);
        assert!(serials.contains(&a.serial));
        assert!(!serials.contains(&b.serial));
        let crl_after = parse_crl(&after);
        let entry = crl_after
            .iter_revoked_certificates()
            .find(|entry| hex::encode(entry.serial().to_bytes_be()) == a.serial)
            .expect("entry for revoked serial");
        let (_critical, reason) = entry.reason_code().expect("reason code");
        assert_eq!(
            reason.0,
            RevocationReason::CessationOfOperation.rfc_code(),
            "CRL 条目 reason 必须是 RFC 5280 数值"
        );

        let rotated = pki.rotate_crl().await.expect("rotate crl");
        assert_eq!(rotated.crl_number, 3, "强制轮换必增编号");
        assert_eq!(
            pki.get_crl().await.expect("after rotate").crl_number,
            3,
            "无变化 ⇒ 复用最新编号"
        );
    }

    /// Tidy：回收过期历史/active 记录，但保留吊销记录与已吊销证书
    #[tokio::test]
    async fn test_tidy_reclaims_expired_records_but_keeps_revocations() {
        let store = Arc::new(MemoryPkiStore::new());
        let pki = PkiService::with_store(PkiConfig::default(), store.clone()).expect("pki");
        pki.init_ca("Tidy CA").await.expect("init");

        let live = pki.issue_cert("live.local", 0).await.expect("issue live");

        // 已吊销的过期记录：Tidy 必须保留它（同 CN 的隐式重签阻断依赖它）
        let mut revoked = expired_record("revoked.local", "0a0b0c");
        revoked.not_after = now_unix() - 86_400 * 2;
        store
            .create_cert("revoked.local", &revoked)
            .await
            .expect("seed revoked");
        pki.revoke_cert(&revoked.serial, RevocationReason::KeyCompromise, "ops")
            .await
            .expect("revoke");

        // 过期历史记录（未吊销）：seed 一条过期 active，再替换出新 active ⇒ 旧记录进历史
        let mut old_history = expired_record("gone-history.local", "gone-history");
        old_history.not_after = now_unix() - 86_400 * 2;
        store
            .create_cert("gone-history.local", &old_history)
            .await
            .expect("seed history source");
        let mut fresh = expired_record("gone-history.local", "fresh-active");
        fresh.not_after = now_unix() + 86_400;
        store
            .replace_active_cert_cas("gone-history.local", 1, &fresh, &old_history)
            .await
            .expect("seed history");

        // 过期 active 记录（未吊销）：Tidy 直接回收
        let mut old_active = expired_record("gone-active.local", "gone-active");
        old_active.not_after = now_unix() - 86_400 * 2;
        store
            .create_cert("gone-active.local", &old_active)
            .await
            .expect("seed expired active");

        let result = pki.tidy(3600).await.expect("tidy");
        assert_eq!(result.retention_seconds, 3600);
        assert_eq!(result.removed_certs, 2, "过期历史 + 过期 active 各一条");
        assert!(
            store
                .get_cert_by_serial("gone-history")
                .await
                .expect("by serial")
                .is_none(),
            "过期历史必须被回收"
        );
        assert!(
            store
                .get_cert("gone-active.local")
                .await
                .expect("active")
                .is_none(),
            "过期 active 必须被回收"
        );
        // 吊销记录与已吊销证书跨 Tidy 保留（永久事实）
        assert_eq!(
            pki.get_cert_status(&revoked.serial)
                .await
                .expect("status after tidy")
                .status,
            "revoked"
        );
        assert!(
            store
                .get_cert("revoked.local")
                .await
                .expect("revoked active")
                .is_some(),
            "已吊销记录必须保留（阻断隐式重签）"
        );
        assert!(crl_serials(&pki.get_crl().await.expect("crl")).contains(&revoked.serial));
        // 未过期证书不受影响
        assert!(pki.verify_cert(&live.cert_pem).await.expect("verify live"));
    }

    // ──── serial 入参按 X.509 INTEGER 数值口径匹配 ────

    /// 自签一张 serial 首 nibble 为 `0` 的证书（DER INTEGER 无需补零字节）。
    fn cert_with_leading_zero_serial() -> (String, String) {
        let bytes = [0x0a, 0x1b, 0x2c, 0x3d, 0x4e, 0x5f, 0x60, 0x71];
        let mut params = CertificateParams::default();
        params
            .distinguished_name
            .push(DnType::CommonName, "legacy.coord.local");
        params.serial_number = Some(rcgen::SerialNumber::from(bytes.to_vec()));
        params.not_after = OffsetDateTime::now_utc() + time::Duration::days(365);
        let key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("leaf key");
        let cert = params.self_signed(&key).expect("leaf cert");
        (cert.pem(), hex::encode(bytes))
    }

    fn serial_record(pem: String, serial: &str) -> CertRecord {
        CertRecord {
            common_name: "legacy.coord.local".into(),
            cert_pem: pem,
            key_name: "pki/leaf/legacy.coord.local".into(),
            key_version: 1,
            not_before: now_unix() - 60,
            not_after: now_unix() + 86_400,
            serial: serial.to_string(),
            status: CertStatus::Active,
            parent_serial: None,
            dns_names: vec!["legacy.coord.local".into()],
            ip_addresses: Vec::new(),
            spki_sha256: String::new(),
            usage: CertUsage::Both,
        }
    }

    /// 首 nibble 为 `0` 的 serial：定长 / 去零 / 大写 / 带空白都能查、能吊、能续期。
    #[tokio::test]
    async fn test_serial_lookup_ignores_case_and_leading_zeros() {
        let store = Arc::new(MemoryPkiStore::new());
        let pki = make_pki_shared(store.clone(), &shared_keys());
        pki.init_ca("Serial CA").await.expect("init");

        let (pem, padded) = cert_with_leading_zero_serial();
        assert!(padded.starts_with('0'), "夹具 serial 必须以 0 开头");
        let record = serial_record(pem, &padded);
        store
            .create_cert("legacy.coord.local", &record)
            .await
            .expect("seed");

        let unpadded = padded.trim_start_matches('0').to_string();
        assert_eq!(unpadded.len(), padded.len() - 1, "仅省掉一个前导零 nibble");
        let spellings = [
            padded.clone(),
            unpadded.clone(),
            padded.to_uppercase(),
            format!("  {unpadded}\t"),
        ];
        for spelling in &spellings {
            let found = pki
                .get_cert_by_serial(spelling)
                .await
                .expect("query")
                .unwrap_or_else(|| panic!("GetCertBySerial 未命中 {spelling:?}"));
            assert_eq!(found.serial, padded, "{spelling:?}");
            assert_eq!(
                pki.get_cert_status(spelling).await.expect("status").status,
                "active",
                "{spelling:?}"
            );
        }

        // 去零写法吊销成功，账本回显仍是定长口径；再次吊销（定长写法）幂等
        let revoked = pki
            .revoke_cert(&unpadded, RevocationReason::KeyCompromise, "ops")
            .await
            .expect("revoke by unpadded serial");
        assert_eq!(revoked.cert_serial, padded);
        let again = pki
            .revoke_cert(&padded, RevocationReason::Superseded, "another")
            .await
            .expect("idempotent revoke");
        assert_eq!(again.cert_serial, padded);
        assert_eq!(again.revoked_at, revoked.revoked_at, "保留首次记录");
        assert_eq!(again.reason, RevocationReason::KeyCompromise);

        let status = pki
            .get_cert_status(&unpadded)
            .await
            .expect("status after revoke");
        assert_eq!(status.status, "revoked");
        assert_eq!(status.cert_serial, padded);

        // 吊销后按去零写法续期：必须明确拒绝（命中同一记录），而不是静默 NOT_FOUND
        assert!(
            matches!(
                pki.renew_cert(&unpadded, 0).await,
                Err(PkiError::FailedPrecondition(_))
            ),
            "已吊销 serial 的续期必须按数值口径命中并拒绝复活"
        );
    }

    /// 续期用去零写法能还原真实 CN（RenewCert 与按 serial 查共用同一数值判据）。
    #[tokio::test]
    async fn test_renew_accepts_unpadded_serial() {
        let store = Arc::new(MemoryPkiStore::new());
        let pki = make_pki_shared(store.clone(), &shared_keys());
        pki.init_ca("Serial Renew CA").await.expect("init");

        let (pem, padded) = cert_with_leading_zero_serial();
        store
            .create_cert("legacy.coord.local", &serial_record(pem, &padded))
            .await
            .expect("seed");

        let unpadded = padded.trim_start_matches('0').to_string();
        let renewed = pki
            .renew_cert(&unpadded, 0)
            .await
            .expect("renew by unpadded serial");
        assert_eq!(renewed.common_name, "legacy.coord.local");
        assert_ne!(renewed.serial, padded);
        assert!(pki.verify_cert(&renewed.cert_pem).await.expect("verify"));
        // 旧 serial 仍可按去零写法取回（历史记录，同一判据）
        let old = pki
            .get_cert_by_serial(&unpadded)
            .await
            .expect("query")
            .expect("history hit");
        assert_eq!(old.serial, padded);
        assert_eq!(old.status, CertStatus::Retired);
    }

    /// 非 hex / 空 serial ⇒ INVALID_ARGUMENT（调用方参数错，不是“不存在”）。
    #[tokio::test]
    async fn test_serial_must_be_hex() {
        let pki = make_pki();
        for bad in ["", "   ", "0x0a1b", "deadbeefg", "a b"] {
            assert!(
                matches!(
                    pki.get_cert_by_serial(bad).await,
                    Err(PkiError::InvalidArgument(_))
                ),
                "GetCertBySerial {bad:?}"
            );
            assert!(
                matches!(
                    pki.get_cert_status(bad).await,
                    Err(PkiError::InvalidArgument(_))
                ),
                "GetCertStatus {bad:?}"
            );
            assert!(
                matches!(
                    pki.revoke_cert(bad, RevocationReason::Unspecified, "ops")
                        .await,
                    Err(PkiError::InvalidArgument(_))
                ),
                "RevokeCert {bad:?}"
            );
            assert!(
                matches!(
                    pki.renew_cert(bad, 0).await,
                    Err(PkiError::InvalidArgument(_))
                ),
                "RenewCert {bad:?}"
            );
        }
    }
}
