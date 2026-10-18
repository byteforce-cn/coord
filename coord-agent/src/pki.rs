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

use crate::pki_store::{CaRecord, CertRecord, CertStatus, MemoryPkiStore, PkiStore, PkiStoreError};

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

impl Default for PkiConfig {
    fn default() -> Self {
        Self {
            cert_ttl_hours: 24,
            expiry_warn_hours: 6,
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

impl Clone for CaMaterial {
    fn clone(&self) -> Self {
        Self {
            cert_pem: self.cert_pem.clone(),
            key_pem: self.key_pem.clone(),
        }
    }
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

        let (cert_pem, key_pem) = {
            let cn = ca_common_name.to_string();
            // CPU 密集的 CA 密钥生成 + 自签移到阻塞线程池
            tokio::task::spawn_blocking(move || Self::generate_ca(&cn))
                .await
                .map_err(|e| PkiError::KeyGen(format!("ca generate task join: {e}")))??
        };
        let record = CaRecord {
            cert_pem,
            key_pem,
            common_name: ca_common_name.to_string(),
        };
        match self.store.create_ca(&record).await {
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

        let key_pem = key_pair.serialize_pem();

        let cert = params
            .self_signed(&key_pair)
            .map_err(|e| PkiError::CertGen(e.to_string()))?;

        Ok((cert.pem(), key_pem))
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
        self.issue_cert_with_san(common_name, ttl_seconds, SanSpec::default())
            .await
    }

    /// 同 [`Self::issue_cert`]，显式指定 SAN。
    ///
    /// SAN 两者皆空 ⇒ 将 CN 派生为 DNS SAN；CN 不是合法 DNS 名时返回
    /// `InvalidArgument`（不静默签出无 SAN 证书）。
    pub async fn issue_cert_with_san(
        &self,
        common_name: &str,
        ttl_seconds: u64,
        san: SanSpec,
    ) -> Result<CertInfo, PkiError> {
        self.ensure_cert(common_name, ttl_seconds, false, san.normalized()?, false)
            .await
    }

    /// 续期证书：**按 serial 查回真实 CN**，再替换出一个新证书（新密钥 + 新 serial）
    ///
    /// 按序列号恢复入口：serial 可来自 active 或历史（含已过期）记录。
    /// 旧证书保留至 not_after 供验签方按 serial/kid 取用。
    pub async fn renew_cert(&self, serial: &str, ttl_seconds: u64) -> Result<CertInfo, PkiError> {
        let old = self
            .store
            .get_cert_by_serial(serial)
            .await
            .map_err(|e| PkiError::Store(e.to_string()))?
            .ok_or_else(|| PkiError::CertNotFound(serial.to_string()))?;
        // RenewCert 无 SAN 入参：继承旧记录 SAN（无 SAN 的旧记录由 CN 派生）。
        self.ensure_cert(
            &old.common_name,
            ttl_seconds,
            true,
            SanSpec::default(),
            true,
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
        self.ensure_cert(common_name, ttl_seconds, true, san.normalized()?, true)
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
                    // 幂等快路径：未过期 + 非强制轮换 + SAN/TTL 与请求一致
                    if !force_rotate && !record.is_expired(now_unix()) {
                        let san = effective_san(common_name, &requested_san, false, Some(&record))?;
                        if san.dns_names == record.dns_names
                            && san.ip_addresses == record.ip_addresses
                            && record_ttl_seconds(&record) == want_ttl
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
                    let new_record = self
                        .sign_new_cert_blocking(
                            common_name,
                            ttl_seconds,
                            san,
                            CertStatus::Active,
                            Some(record.serial.clone()),
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
                    let new_record = self
                        .sign_new_cert_blocking(
                            common_name,
                            ttl_seconds,
                            san,
                            CertStatus::Active,
                            None,
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

    /// 异步签发：CPU 密集的 rcgen 密钥生成/证书签名移到阻塞线程池。
    ///
    /// 运行时隔离注意：`issue_cert` / `rotate_locked` 等 async 路径
    /// 原先在 worker 上内联执行 ECDSA 密钥生成 + 签名；此处先短锁 clone CA 材料与
    /// TTL 配置（纯数据），再 `spawn_blocking` 执行纯函数，返回语义与同步版一致。
    async fn sign_new_cert_blocking(
        &self,
        common_name: &str,
        ttl_seconds: u64,
        san: SanSpec,
        status: CertStatus,
        parent_serial: Option<String>,
    ) -> Result<CertRecord, PkiError> {
        let ca = {
            let guard = self.ca.read();
            guard.as_ref().cloned().ok_or(PkiError::CaNotInitialized)?
        };
        let default_ttl_hours = self.config.cert_ttl_hours;
        let cn = common_name.to_string();

        tokio::task::spawn_blocking(move || {
            sign_cert_with_ca(
                &ca,
                &cn,
                ttl_seconds,
                default_ttl_hours,
                &san,
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
fn sign_cert_with_ca(
    ca: &CaMaterial,
    common_name: &str,
    ttl_seconds: u64,
    default_ttl_hours: u32,
    san: &SanSpec,
    status: CertStatus,
    parent_serial: Option<String>,
) -> Result<CertRecord, PkiError> {
    let key_pair = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)
        .map_err(|e| PkiError::KeyGen(e.to_string()))?;

    let mut params = CertificateParams::default();
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    params
        .distinguished_name
        .push(DnType::OrganizationName, "Coord Agent");
    params.is_ca = IsCa::NoCa;
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    params.extended_key_usages = vec![
        rcgen::ExtendedKeyUsagePurpose::ClientAuth,
        rcgen::ExtendedKeyUsagePurpose::ServerAuth,
    ];

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

    let ca_key = KeyPair::from_pem(&ca.key_pem).map_err(|e| PkiError::KeyGen(e.to_string()))?;

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
        dns_names: san.dns_names.clone(),
        ip_addresses: san.ip_addresses.clone(),
    })
}

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

        let issuer_cn = cert
            .issuer()
            .iter_common_name()
            .next()
            .map(|cn| cn.as_str().unwrap_or(""))
            .unwrap_or("");

        let ca_der = pem_to_der(&ca.cert_pem)?;
        let (_ca_remainder, ca_cert) = x509_parser::parse_x509_certificate(&ca_der)
            .map_err(|e| PkiError::CertParse(e.to_string()))?;

        let ca_cn = ca_cert
            .subject()
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
    CaNotInitialized,
    /// 调用方参数非法（非法 DNS SAN / IP / CN 无法派生 SAN）
    InvalidArgument(String),
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
            Self::InvalidArgument(msg) => write!(f, "invalid argument: {msg}"),
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
    pki_server::Pki, PkiCertSummary, PkiGetCaCertRequest, PkiGetCaCertResponse,
    PkiGetCertByCnRequest, PkiGetCertByCnResponse, PkiInitCaRequest, PkiInitCaResponse,
    PkiIssueCertRequest, PkiIssueCertResponse, PkiListCertsRequest, PkiListCertsResponse,
    PkiRenewCertRequest, PkiRenewCertResponse, PkiRotateCertRequest, PkiRotateCertResponse,
    PkiVerifyCertRequest, PkiVerifyCertResponse,
};
use tonic::{Request, Response, Status};

/// `PkiError → tonic::Status` 的单一映射点。
///
/// 参数错误必须是 `INVALID_ARGUMENT`（调用方可重试/修正），其余保持
/// `INTERNAL`；不得让所有失败都退化成无法区分的内部错误。
fn status_from_pki_error(e: PkiError) -> Status {
    match e {
        PkiError::InvalidArgument(msg) => Status::invalid_argument(msg),
        PkiError::CertNotFound(serial) => {
            Status::not_found(format!("certificate not found by serial: {serial}"))
        }
        other => Status::internal(other.to_string()),
    }
}

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
            .map_err(status_from_pki_error)?;
        Ok(Response::new(PkiInitCaResponse {}))
    }

    async fn issue_cert(
        &self,
        request: Request<PkiIssueCertRequest>,
    ) -> Result<Response<PkiIssueCertResponse>, Status> {
        let req = request.into_inner();
        let cert_info = PkiService::issue_cert_with_san(
            self,
            &req.common_name,
            req.ttl_seconds as u64,
            SanSpec::new(req.dns_names, req.ip_addresses),
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
        let cert_info = PkiService::rotate_cert_with_san(
            self,
            &req.common_name,
            req.ttl_seconds as u64,
            SanSpec::new(req.dns_names, req.ip_addresses),
        )
        .await
        .map_err(status_from_pki_error)?;
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
            .map_err(status_from_pki_error)?;
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
            .map_err(status_from_pki_error)?
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
        let valid = PkiService::verify_cert(self, &req.cert_pem).map_err(status_from_pki_error)?;
        Ok(Response::new(PkiVerifyCertResponse { valid }))
    }

    async fn get_ca_cert(
        &self,
        _request: Request<PkiGetCaCertRequest>,
    ) -> Result<Response<PkiGetCaCertResponse>, Status> {
        let ca_cert_pem = PkiService::ca_cert_pem(self).map_err(status_from_pki_error)?;
        Ok(Response::new(PkiGetCaCertResponse { ca_cert_pem }))
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
            key_pem: "-----BEGIN PRIVATE KEY-----\nEXPIRED\n-----END PRIVATE KEY-----".into(),
            not_before: 1_700_000_000,
            not_after: 1_700_000_001,
            serial: serial.to_string(),
            status: CertStatus::Active,
            parent_serial: None,
            dns_names: Vec::new(),
            ip_addresses: Vec::new(),
        }
    }

    /// G-PKI-1：IssueCert 对过期记录走替换路径——新 serial / 新密钥，绝不返回过期记录
    #[tokio::test]
    async fn test_issue_cert_replaces_expired_record() {
        let store = Arc::new(MemoryPkiStore::new());
        let pki = PkiService::with_store(PkiConfig::default(), store.clone());
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
        assert_ne!(cert.key_pem, expired.key_pem, "必须换新密钥");
        assert!(
            pki.verify_cert(&cert.cert_pem).expect("verify"),
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
        let pki = PkiService::with_store(PkiConfig::default(), store.clone());
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
        assert!(!cert
            .key_pem
            .starts_with("-----BEGIN PRIVATE KEY-----\nEXPIRED"));
        assert!(pki.verify_cert(&cert.cert_pem).expect("verify"));
    }

    /// G-PKI-1：RenewCert(serial) 为按序列号恢复入口——过期 serial 也能恢复出新证书
    #[tokio::test]
    async fn test_renew_cert_recovers_from_expired_serial() {
        let store = Arc::new(MemoryPkiStore::new());
        let pki = PkiService::with_store(PkiConfig::default(), store.clone());
        pki.init_ca("Recover CA").await.expect("init");

        store
            .create_cert(
                "svc-recover.local",
                &expired_record("svc-recover.local", "expired-0x3"),
            )
            .await
            .expect("seed expired");

        let recovered = pki
            .renew_cert("expired-0x3", 0)
            .await
            .expect("renew by expired serial");
        assert_eq!(recovered.common_name, "svc-recover.local");
        assert_ne!(recovered.serial, "expired-0x3");
        assert!(pki.verify_cert(&recovered.cert_pem).expect("verify"));

        // 恢复后旧 serial 已入历史，再次按同一 serial 恢复仍可用（恢复到最新）
        let again = pki
            .renew_cert("expired-0x3", 0)
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
        assert_ne!(renewed.key_pem, first.key_pem, "过期后必须换新密钥");
        assert!(pki.verify_cert(&renewed.cert_pem).expect("verify"));
    }

    /// G-PKI-1：并发替换（版本化 CAS）——双 agent 同时 rotate：都成功、无丢更新
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_rotate_no_lost_update() {
        let store = Arc::new(MemoryPkiStore::new());

        let seed_svc = PkiService::with_store(PkiConfig::default(), store.clone());
        seed_svc
            .init_ca("Concurrent Rotate CA")
            .await
            .expect("init");
        let first = seed_svc
            .issue_cert("svc-conc.local", 0)
            .await
            .expect("seed");

        let pki_a = PkiService::with_store(PkiConfig::default(), store.clone());
        let pki_b = PkiService::with_store(PkiConfig::default(), store.clone());
        pki_a.init_ca("Concurrent Rotate CA").await.expect("init A");
        pki_b.init_ca("Concurrent Rotate CA").await.expect("init B");

        let a = tokio::spawn(async move { pki_a.rotate_cert("svc-conc.local", 0).await });
        let b = tokio::spawn(async move { pki_b.rotate_cert("svc-conc.local", 0).await });
        let cert_a = a.await.expect("join A").expect("rotate A");
        let cert_b = b.await.expect("join B").expect("rotate B");

        assert_ne!(cert_a.serial, cert_b.serial, "并发轮换产生两个不同新证书");

        // 无丢更新：两个新证书都在 store 中（一个 active、另一个 retired），旧证书也在
        let check_svc = PkiService::with_store(PkiConfig::default(), store.clone());
        let all = check_svc.list_certs("svc-conc.local").await.expect("list");
        let serials: Vec<&str> = all.iter().map(|c| c.serial.as_str()).collect();
        assert!(serials.contains(&first.serial.as_str()), "旧证书保留");
        assert!(serials.contains(&cert_a.serial.as_str()), "A 的证书保留");
        assert!(serials.contains(&cert_b.serial.as_str()), "B 的证书保留");

        // 重启一致性：新实例（同一 store）取回当前 active，且为本次轮换之一
        let pki_restart = PkiService::with_store(PkiConfig::default(), store.clone());
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
        let pki = PkiService::with_store(config, store.clone());
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
        let pki_a = PkiService::with_store(PkiConfig::default(), store.clone());
        let pki_b = PkiService::with_store(PkiConfig::default(), store.clone());
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
        assert_ne!(
            cert_a.key_pem,
            "-----BEGIN PRIVATE KEY-----\nEXPIRED\n-----END PRIVATE KEY-----"
        );
        assert_ne!(
            cert_b.key_pem,
            "-----BEGIN PRIVATE KEY-----\nEXPIRED\n-----END PRIVATE KEY-----"
        );
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
        let pki = PkiService::with_store(PkiConfig::default(), store.clone());
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
        let pki = PkiService::with_store(PkiConfig::default(), store.clone());
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
        let pki = PkiService::with_store(PkiConfig::default(), store.clone());
        pki.init_ca("Legacy CA").await.expect("init");

        // 升级前的记录：无 SAN 字段（反序列化为空），未过期、TTL 与默认值一致。
        let now = now_unix();
        let legacy = CertRecord {
            common_name: "legacy-svc".into(),
            cert_pem: "-----BEGIN CERTIFICATE-----\nLEGACY\n-----END CERTIFICATE-----".into(),
            key_pem: "-----BEGIN PRIVATE KEY-----\nLEGACY\n-----END PRIVATE KEY-----".into(),
            not_before: now,
            not_after: now + 86_400,
            serial: "legacy-0x1".into(),
            status: CertStatus::Active,
            parent_serial: None,
            dns_names: Vec::new(),
            ip_addresses: Vec::new(),
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
}
