// coord-agent: Transit 命名密钥档案与密码学操作（ADR-0019）
//
// 本模块是 KMS 面的核心：
// - 命名密钥的创建/轮换/配置/裁剪/删除/导入/导出（版本三边界：latest /
//   min_decryption / min_encryption）；
// - 非对称签名/验签（Ed25519 / ECDSA P-256/P-384/P-521 / RSA PKCS#1 v1.5 + PSS）；
// - 按名信封加密（CCT3：key name + version 自描述，DEK 用该版本材料包裹）。
//
// 材料保护：版本材料（PKCS#8 DER 或 32 字节对称密钥）只以
// `nonce(12) || AES-256-GCM(material, KEK)` 形态落盘；明文材料只在单次操作内
// 存在（`Zeroizing`）。KEK 供给/多材料窗口见 `transit.rs` 的 `TransitKekKeyring`。
//
// 并发：所有写操作走档案 key version 的 CAS（见 `transit_key_store.rs`），冲突即
// 重读重试；`latest_version` 单调递增，不丢更新。读操作返回的版本必须落在
// [min_decryption_version, latest_version]（加密/签名另加 min_encryption_version）。
//
// 格式与错误口径见 `apis/contracts/proto/coord/transit/v1/transit.proto` 头注。

use std::sync::Arc;

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use ed25519_dalek::pkcs8::DecodePrivateKey as _;
use ed25519_dalek::{
    Signer as _, SigningKey as Ed25519SigningKey, VerifyingKey as Ed25519VerifyingKey,
};
use p256::ecdsa::signature::hazmat::{PrehashSigner, PrehashVerifier};
use p256::ecdsa::{
    Signature as P256Signature, SigningKey as P256SigningKey, VerifyingKey as P256VerifyingKey,
};
use p256::elliptic_curve::Generate as _;
use p256::pkcs8::{DecodePrivateKey as _, EncodePrivateKey as _};
use p384::ecdsa::{
    Signature as P384Signature, SigningKey as P384SigningKey, VerifyingKey as P384VerifyingKey,
};
use p521::ecdsa::{
    Signature as P521Signature, SigningKey as P521SigningKey, VerifyingKey as P521VerifyingKey,
};
use rand::RngCore;
use rsa::pkcs8::LineEnding;
use rsa::traits::PublicKeyParts as _;
use rsa::{PrivateKey as RsaPrivateKey, PublicKey as RsaPublicKey};
use sha2::{Digest, Sha256, Sha384, Sha512};
use sha2_11::{Sha256 as RsaSha256, Sha384 as RsaSha384, Sha512 as RsaSha512};
use zeroize::Zeroizing;

use super::transit::TransitKekKeyring;
use super::transit_key_store::{
    now_unix, KeyArchive, KeyVersionMeta, KeyVersionRecord, TransitKeyStore, TransitKeyStoreError,
};

/// 单次 CAS 重试上限（并发轮换/配置的持久冲突才会耗尽）
const MAX_CAS_ATTEMPTS: usize = 8;

/// 对称密钥长度（AES-256 / DEK）
pub const SYMMETRIC_KEY_LEN: usize = 32;

/// CCT3 魔数
pub const CCT3_MAGIC: &[u8; 4] = b"CCT3";

const NONCE_LEN: usize = 12;
const DEK_PACKET_LEN: usize = NONCE_LEN + SYMMETRIC_KEY_LEN + 16;

/// 当前密文格式可携带的最大 key name 字节数（`name_len` 是 u8）
pub const KEY_NAME_MAX_LEN: usize = 255;

// ──── KeyType ────

/// 命名密钥类型（wire 值见 proto 的 `TransitKeyType`）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyType {
    Aes256Gcm96,
    Ed25519,
    EcdsaP256,
    EcdsaP384,
    EcdsaP521,
    Rsa2048,
    Rsa3072,
    Rsa4096,
}

/// 服务端权威能力矩阵
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyCapabilities {
    pub supports_encryption: bool,
    pub supports_signing: bool,
    pub supports_hmac: bool,
    pub supports_derivation: bool,
}

impl KeyType {
    pub fn canonical(self) -> &'static str {
        match self {
            Self::Aes256Gcm96 => "aes256-gcm96",
            Self::Ed25519 => "ed25519",
            Self::EcdsaP256 => "ecdsa-p256",
            Self::EcdsaP384 => "ecdsa-p384",
            Self::EcdsaP521 => "ecdsa-p521",
            Self::Rsa2048 => "rsa-2048",
            Self::Rsa3072 => "rsa-3072",
            Self::Rsa4096 => "rsa-4096",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "aes256-gcm96" => Some(Self::Aes256Gcm96),
            "ed25519" => Some(Self::Ed25519),
            "ecdsa-p256" => Some(Self::EcdsaP256),
            "ecdsa-p384" => Some(Self::EcdsaP384),
            "ecdsa-p521" => Some(Self::EcdsaP521),
            "rsa-2048" => Some(Self::Rsa2048),
            "rsa-3072" => Some(Self::Rsa3072),
            "rsa-4096" => Some(Self::Rsa4096),
            _ => None,
        }
    }

    pub fn capabilities(self) -> KeyCapabilities {
        match self {
            Self::Aes256Gcm96 => KeyCapabilities {
                supports_encryption: true,
                supports_signing: false,
                supports_hmac: false,
                supports_derivation: false,
            },
            _ => KeyCapabilities {
                supports_encryption: false,
                supports_signing: true,
                supports_hmac: false,
                supports_derivation: false,
            },
        }
    }

    fn is_asymmetric(self) -> bool {
        !matches!(self, Self::Aes256Gcm96)
    }

    fn rsa_bits(self) -> Option<usize> {
        match self {
            Self::Rsa2048 => Some(2048),
            Self::Rsa3072 => Some(3072),
            Self::Rsa4096 => Some(4096),
            _ => None,
        }
    }

    /// ECDSA 全量签名的默认摘要（与曲线匹配；预哈希路径可由调用方显式指定）
    fn ecdsa_default_hash(self) -> Option<HashAlg> {
        match self {
            Self::EcdsaP256 => Some(HashAlg::Sha256),
            Self::EcdsaP384 => Some(HashAlg::Sha384),
            Self::EcdsaP521 => Some(HashAlg::Sha512),
            _ => None,
        }
    }
}

// ──── 错误 ────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyError {
    InvalidArgument(String),
    NotFound(String),
    AlreadyExists(String),
    FailedPrecondition(String),
    PermissionDenied(String),
    Conflict(String),
    Internal(String),
}

impl KeyError {
    /// gRPC 状态名（批量逐条结果也用它）
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidArgument(_) => "INVALID_ARGUMENT",
            Self::NotFound(_) => "NOT_FOUND",
            Self::AlreadyExists(_) => "ALREADY_EXISTS",
            Self::FailedPrecondition(_) => "FAILED_PRECONDITION",
            Self::PermissionDenied(_) => "PERMISSION_DENIED",
            Self::Conflict(_) => "ABORTED",
            Self::Internal(_) => "INTERNAL",
        }
    }

    pub fn message(&self) -> &str {
        match self {
            Self::InvalidArgument(m)
            | Self::NotFound(m)
            | Self::AlreadyExists(m)
            | Self::FailedPrecondition(m)
            | Self::PermissionDenied(m)
            | Self::Conflict(m)
            | Self::Internal(m) => m,
        }
    }
}

impl std::fmt::Display for KeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code(), self.message())
    }
}

impl std::error::Error for KeyError {}

impl From<TransitKeyStoreError> for KeyError {
    fn from(e: TransitKeyStoreError) -> Self {
        match e {
            TransitKeyStoreError::AlreadyExists(k) => Self::AlreadyExists(k),
            TransitKeyStoreError::VersionConflict(k) => Self::Conflict(k),
            TransitKeyStoreError::Kv(m) => Self::Internal(m),
            TransitKeyStoreError::Serialization(m) => Self::Internal(m),
        }
    }
}

// ──── 请求/结果类型 ────

#[derive(Debug, Clone)]
pub struct CreateKeySpec {
    pub name: String,
    pub key_type: KeyType,
    pub exportable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaterialFormat {
    Pem,
    Der,
    Raw,
}

#[derive(Debug, Clone)]
pub struct ImportKeySpec {
    pub name: String,
    pub key_type: KeyType,
    pub material: Vec<u8>,
    pub format: Option<MaterialFormat>,
    pub exportable: bool,
}

#[derive(Debug, Clone, Default)]
pub struct KeyConfigPatch {
    pub min_decryption_version: Option<u32>,
    pub min_encryption_version: Option<u32>,
    pub deletion_allowed: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct SignSpec {
    pub name: String,
    pub version: u32,
    pub input: Vec<u8>,
    pub prehashed: bool,
    pub hash_algorithm: Option<String>,
    pub signature_algorithm: Option<String>,
}

#[derive(Debug, Clone)]
pub struct VerifySpec {
    pub name: String,
    pub version: u32,
    pub input: Vec<u8>,
    pub signature: Vec<u8>,
    pub prehashed: bool,
    pub hash_algorithm: Option<String>,
    pub signature_algorithm: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignOutput {
    pub signature: Vec<u8>,
    pub key_version: u32,
    pub algorithm: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyOutput {
    pub valid: bool,
    pub key_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportedKey {
    pub key_type: KeyType,
    pub key_version: u32,
    pub material: Vec<u8>,
    pub format: &'static str,
    pub contains_private_key: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataKeyKind {
    Plaintext,
    Wrapped,
    PlaintextAndWrapped,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratedDataKey {
    pub plaintext: Option<Vec<u8>>,
    pub ciphertext: Option<Vec<u8>>,
    pub key_version: u32,
}

// ──── 摘要算法 ────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HashAlg {
    Sha256,
    Sha384,
    Sha512,
}

impl HashAlg {
    fn parse(raw: &str) -> Option<Self> {
        let compact: String = raw
            .chars()
            .filter(|c| *c != '-' && *c != '_')
            .collect::<String>()
            .to_ascii_uppercase();
        match compact.as_str() {
            "SHA256" => Some(Self::Sha256),
            "SHA384" => Some(Self::Sha384),
            "SHA512" => Some(Self::Sha512),
            _ => None,
        }
    }

    fn canonical(self) -> &'static str {
        match self {
            Self::Sha256 => "sha256",
            Self::Sha384 => "sha384",
            Self::Sha512 => "sha512",
        }
    }

    fn digest_len(self) -> usize {
        match self {
            Self::Sha256 => 32,
            Self::Sha384 => 48,
            Self::Sha512 => 64,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RsaScheme {
    Pkcs1v15,
    Pss,
}

// ──── 归档校验 ────

/// 密钥名规则：非空、≤255 字节（CCT3 的 name_len 是 u8）、UTF-8、无 NUL/控制字符、
/// 无首尾空白。允许 `/`（PKI 的 `pki/leaf/{cn}` 形态）与常见符号。
fn validate_key_name(name: &str) -> Result<(), KeyError> {
    if name.is_empty() {
        return Err(KeyError::InvalidArgument(
            "key name must not be empty".into(),
        ));
    }
    if name.len() > KEY_NAME_MAX_LEN {
        return Err(KeyError::InvalidArgument(format!(
            "key name must be at most {KEY_NAME_MAX_LEN} bytes"
        )));
    }
    if name.trim() != name {
        return Err(KeyError::InvalidArgument(
            "key name must not have leading/trailing whitespace".into(),
        ));
    }
    if name.chars().any(|c| c == '\0' || c.is_control()) {
        return Err(KeyError::InvalidArgument(
            "key name must not contain NUL or control characters".into(),
        ));
    }
    Ok(())
}

/// PKI 密钥由 PKI 服务独占 `pki/` 前缀（ADR-0019 D6）：
/// 若允许 Transit 的写面创建/导入/轮换/裁剪这些名字，调用方就能预置一把
/// 自己知道材料的 `pki/leaf/{cn}`（PKI 会沿用），或裁剪掉 CA 的历史版本。
/// 读取与导出不在此列：CA 由 `never_exportable` 挡住，leaf 导出是受控恢复路径。
fn reject_reserved_pki_name(name: &str, action: &str) -> Result<(), KeyError> {
    if name.starts_with("pki/") {
        return Err(KeyError::PermissionDenied(format!(
            "key names under 'pki/' are managed by the PKI service; {action} is not allowed via Transit"
        )));
    }
    Ok(())
}

fn version_meta(version: u32, created_at: i64) -> KeyVersionMeta {
    KeyVersionMeta {
        version,
        created_at,
    }
}

// ──── 注册表 ────

/// 命名密钥注册表：档案 + 版本材料 + 密码学操作
pub struct TransitKeyRegistry {
    store: Arc<dyn TransitKeyStore>,
    keyring: Arc<TransitKekKeyring>,
}

impl TransitKeyRegistry {
    pub fn new(store: Arc<dyn TransitKeyStore>, keyring: Arc<TransitKekKeyring>) -> Self {
        Self { store, keyring }
    }

    /// 开发/单测：内存 store + 随机 KEK（不用于生产；生产必须注入材料）
    pub fn in_memory() -> Result<Self, KeyError> {
        let mut material = [0u8; SYMMETRIC_KEY_LEN];
        aes_gcm::aead::OsRng.fill_bytes(&mut material);
        let kek = super::transit::TransitKekMaterial::from_bytes(&material)
            .map_err(KeyError::Internal)?;
        let keyring =
            TransitKekKeyring::single("in-memory-kek", kek).map_err(KeyError::Internal)?;
        Ok(Self::new(
            Arc::new(super::transit_key_store::MemoryTransitKeyStore::new()),
            Arc::new(keyring),
        ))
    }

    pub fn keyring(&self) -> &Arc<TransitKekKeyring> {
        &self.keyring
    }

    // ── 生命周期 ──

    /// 创建密钥（同名同配置幂等；配置冲突 ⇒ ALREADY_EXISTS）
    pub async fn create_key(&self, spec: CreateKeySpec) -> Result<KeyArchive, KeyError> {
        reject_reserved_pki_name(&spec.name, "CreateKey")?;
        self.create_key_internal(spec).await
    }

    /// PKI 内部创建（`pki/leaf/{cn}`、`pki/ca`）；绕过保留前缀检查，仅供 `pki.rs` 调用。
    pub(crate) async fn create_key_internal(
        &self,
        spec: CreateKeySpec,
    ) -> Result<KeyArchive, KeyError> {
        validate_key_name(&spec.name)?;
        for _ in 0..MAX_CAS_ATTEMPTS {
            match self.store.get_key(&spec.name).await? {
                Some((archive, _version)) => {
                    if archive.key_type == spec.key_type.canonical()
                        && archive.exportable == spec.exportable
                    {
                        return Ok(archive);
                    }
                    return Err(KeyError::AlreadyExists(format!(
                        "key '{}' already exists with a different configuration",
                        spec.name
                    )));
                }
                None => {
                    let material = generate_material(spec.key_type).await?;
                    let archive = KeyArchive {
                        name: spec.name.clone(),
                        key_type: spec.key_type.canonical().to_string(),
                        latest_version: 1,
                        min_decryption_version: 1,
                        min_encryption_version: 1,
                        deletion_allowed: false,
                        exportable: spec.exportable,
                        never_exportable: false,
                        created_at: now_unix(),
                        versions: vec![version_meta(1, now_unix())],
                    };
                    let record = self.wrap_material(&archive, 1, &material)?;
                    match self.store.create_key(&archive, &record).await {
                        Ok(()) => return Ok(archive),
                        Err(TransitKeyStoreError::AlreadyExists(_)) => continue,
                        Err(e) => return Err(e.into()),
                    }
                }
            }
        }
        Err(KeyError::Conflict(format!(
            "create key '{}' exhausted CAS retries",
            spec.name
        )))
    }

    pub async fn read_key(&self, name: &str) -> Result<KeyArchive, KeyError> {
        self.store
            .get_key(name)
            .await?
            .map(|(archive, _)| archive)
            .ok_or_else(|| KeyError::NotFound(format!("key '{name}' not found")))
    }

    pub async fn list_keys(&self, name_prefix: &str) -> Result<Vec<KeyArchive>, KeyError> {
        let mut keys = self.store.list_keys().await?;
        if !name_prefix.is_empty() {
            keys.retain(|archive| archive.name.starts_with(name_prefix));
        }
        Ok(keys)
    }

    /// 轮换：新版本 = latest + 1（CAS 串行化，并发轮换不丢更新）
    pub async fn rotate_key(&self, name: &str) -> Result<KeyArchive, KeyError> {
        reject_reserved_pki_name(name, "RotateKey")?;
        self.rotate_key_internal(name).await
    }

    /// PKI 内部轮换（leaf key 换代）；绕过保留前缀检查，仅供 `pki.rs` 调用。
    pub(crate) async fn rotate_key_internal(&self, name: &str) -> Result<KeyArchive, KeyError> {
        for _ in 0..MAX_CAS_ATTEMPTS {
            let (mut archive, version) = self
                .store
                .get_key(name)
                .await?
                .ok_or_else(|| KeyError::NotFound(format!("key '{name}' not found")))?;
            let key_type = KeyType::parse(&archive.key_type).ok_or_else(|| {
                KeyError::Internal(format!("unknown stored key type '{}'", archive.key_type))
            })?;
            let next = archive.latest_version + 1;
            let material = generate_material(key_type).await?;
            let record = self.wrap_material(&archive, next, &material)?;
            archive.latest_version = next;
            archive.versions.push(version_meta(next, now_unix()));
            archive.min_encryption_version = archive
                .min_encryption_version
                .max(archive.min_decryption_version);
            match self
                .store
                .replace_key(version, &archive, Some(&record), &[])
                .await
            {
                Ok(()) => return Ok(archive),
                Err(TransitKeyStoreError::VersionConflict(_)) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Err(KeyError::Conflict(format!(
            "rotate key '{name}' exhausted CAS retries"
        )))
    }

    /// 更新配置（min_decryption / min_encryption / deletion_allowed）
    pub async fn update_key_config(
        &self,
        name: &str,
        patch: KeyConfigPatch,
    ) -> Result<KeyArchive, KeyError> {
        reject_reserved_pki_name(name, "UpdateKeyConfig")?;
        if patch.min_decryption_version.is_none()
            && patch.min_encryption_version.is_none()
            && patch.deletion_allowed.is_none()
        {
            return Err(KeyError::InvalidArgument(
                "at least one configuration field must be provided".into(),
            ));
        }
        for _ in 0..MAX_CAS_ATTEMPTS {
            let (mut archive, version) = self
                .store
                .get_key(name)
                .await?
                .ok_or_else(|| KeyError::NotFound(format!("key '{name}' not found")))?;
            if let Some(min) = patch.min_decryption_version {
                if min < 1 || min > archive.latest_version {
                    return Err(KeyError::InvalidArgument(format!(
                        "min_decryption_version must be in [1, {}]",
                        archive.latest_version
                    )));
                }
                archive.min_decryption_version = archive.min_decryption_version.max(min);
            }
            if let Some(min) = patch.min_encryption_version {
                if min < 1 || min > archive.latest_version {
                    return Err(KeyError::InvalidArgument(format!(
                        "min_encryption_version must be in [1, {}]",
                        archive.latest_version
                    )));
                }
                if min < archive.min_decryption_version {
                    return Err(KeyError::InvalidArgument(
                        "min_encryption_version must be >= min_decryption_version".into(),
                    ));
                }
                archive.min_encryption_version = min;
            } else if archive.min_encryption_version < archive.min_decryption_version {
                archive.min_encryption_version = archive.min_decryption_version;
            }
            if let Some(allowed) = patch.deletion_allowed {
                if allowed && archive.never_exportable {
                    return Err(KeyError::FailedPrecondition(
                        "protected CA key cannot be marked deletion_allowed".into(),
                    ));
                }
                archive.deletion_allowed = allowed;
            }
            match self.store.replace_key(version, &archive, None, &[]).await {
                Ok(()) => return Ok(archive),
                Err(TransitKeyStoreError::VersionConflict(_)) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Err(KeyError::Conflict(format!(
            "update key '{name}' exhausted CAS retries"
        )))
    }

    /// 删除（deletion_allowed=true 是唯一闸门；不存在 ⇒ NOT_FOUND）
    pub async fn delete_key(&self, name: &str) -> Result<(), KeyError> {
        reject_reserved_pki_name(name, "DeleteKey")?;
        for _ in 0..MAX_CAS_ATTEMPTS {
            let (archive, version) = self
                .store
                .get_key(name)
                .await?
                .ok_or_else(|| KeyError::NotFound(format!("key '{name}' not found")))?;
            if !archive.deletion_allowed {
                return Err(KeyError::FailedPrecondition(format!(
                    "key '{name}' is not marked deletion_allowed; refusing to delete"
                )));
            }
            match self.store.delete_key(version, &archive).await {
                Ok(()) => return Ok(()),
                Err(TransitKeyStoreError::VersionConflict(_)) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Err(KeyError::Conflict(format!(
            "delete key '{name}' exhausted CAS retries"
        )))
    }

    /// 裁剪：抬高 min_decryption_version 并删除更早版本材料
    pub async fn trim_key(&self, name: &str, min_version: u32) -> Result<KeyArchive, KeyError> {
        reject_reserved_pki_name(name, "TrimKey")?;
        if min_version == 0 {
            return Err(KeyError::InvalidArgument("min_version must be >= 1".into()));
        }
        for _ in 0..MAX_CAS_ATTEMPTS {
            let (mut archive, version) = self
                .store
                .get_key(name)
                .await?
                .ok_or_else(|| KeyError::NotFound(format!("key '{name}' not found")))?;
            if min_version > archive.latest_version {
                return Err(KeyError::InvalidArgument(format!(
                    "min_version must be <= latest_version ({})",
                    archive.latest_version
                )));
            }
            let new_min = archive.min_decryption_version.max(min_version);
            let mut delete_versions = Vec::new();
            archive.versions.retain(|meta| {
                if meta.version < new_min {
                    delete_versions.push(meta.version);
                    false
                } else {
                    true
                }
            });
            archive.min_decryption_version = new_min;
            archive.min_encryption_version = archive.min_encryption_version.max(new_min);
            match self
                .store
                .replace_key(version, &archive, None, &delete_versions)
                .await
            {
                Ok(()) => return Ok(archive),
                Err(TransitKeyStoreError::VersionConflict(_)) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Err(KeyError::Conflict(format!(
            "trim key '{name}' exhausted CAS retries"
        )))
    }

    // ── 导入/导出 ──

    pub async fn import_key(&self, spec: ImportKeySpec) -> Result<KeyArchive, KeyError> {
        reject_reserved_pki_name(&spec.name, "ImportKey")?;
        self.import_key_with_flags(spec, false).await
    }

    /// `never_exportable=true` 用于 CA 密钥（任何能力都不可导出，ADR-0019 D6）；
    /// 仅供 `pki.rs` 的内部调用，绕过保留前缀检查。
    pub(crate) async fn import_key_with_flags(
        &self,
        spec: ImportKeySpec,
        never_exportable: bool,
    ) -> Result<KeyArchive, KeyError> {
        validate_key_name(&spec.name)?;
        let material = normalize_import(spec.key_type, &spec.material, spec.format)?;
        let archive = KeyArchive {
            name: spec.name.clone(),
            key_type: spec.key_type.canonical().to_string(),
            latest_version: 1,
            min_decryption_version: 1,
            min_encryption_version: 1,
            deletion_allowed: false,
            exportable: spec.exportable && !never_exportable,
            never_exportable,
            created_at: now_unix(),
            versions: vec![version_meta(1, now_unix())],
        };
        let record = self.wrap_material(&archive, 1, &material)?;
        match self.store.create_key(&archive, &record).await {
            Ok(()) => Ok(archive),
            Err(TransitKeyStoreError::AlreadyExists(_)) => Err(KeyError::AlreadyExists(format!(
                "key '{}' already exists; import is not idempotent",
                spec.name
            ))),
            Err(e) => Err(e.into()),
        }
    }

    /// 导出（`exportable` + 非 CA 密钥；能力门禁在 gRPC 层）
    pub async fn export_key(
        &self,
        name: &str,
        requested_version: u32,
    ) -> Result<ExportedKey, KeyError> {
        let archive = self.read_key(name).await?;
        if archive.never_exportable {
            return Err(KeyError::PermissionDenied(format!(
                "key '{name}' is protected and can never be exported"
            )));
        }
        if !archive.exportable {
            return Err(KeyError::FailedPrecondition(format!(
                "key '{name}' is not marked exportable"
            )));
        }
        let version = self.resolve_decryption_version(&archive, requested_version)?;
        let key_type = KeyType::parse(&archive.key_type).ok_or_else(|| {
            KeyError::Internal(format!("unknown stored key type '{}'", archive.key_type))
        })?;
        let material = self.material_for(&archive, version).await?;
        let (bytes, format, contains_private_key) = export_material(key_type, &material)?;
        Ok(ExportedKey {
            key_type,
            key_version: version,
            material: bytes,
            format,
            contains_private_key,
        })
    }

    // ── 签名/验签 ──

    pub async fn sign(&self, spec: SignSpec) -> Result<SignOutput, KeyError> {
        let archive = self.read_key(&spec.name).await?;
        let key_type = KeyType::parse(&archive.key_type).ok_or_else(|| {
            KeyError::Internal(format!("unknown stored key type '{}'", archive.key_type))
        })?;
        if !key_type.capabilities().supports_signing {
            return Err(KeyError::InvalidArgument(format!(
                "key '{}' of type {} does not support signing",
                spec.name,
                key_type.canonical()
            )));
        }
        let version = self.resolve_encryption_version(&archive, spec.version)?;
        let algorithms = resolve_algorithms(
            key_type,
            spec.prehashed,
            spec.hash_algorithm.as_deref(),
            spec.signature_algorithm.as_deref(),
        )?;
        validate_prehashed_input(&algorithms, &spec.input)?;
        let material = self.material_for(&archive, version).await?;
        let signature = sign_with_material(key_type, &material, &spec.input, &algorithms)?;
        Ok(SignOutput {
            signature,
            key_version: version,
            algorithm: algorithms.canonical,
        })
    }

    pub async fn verify(&self, spec: VerifySpec) -> Result<VerifyOutput, KeyError> {
        let archive = self.read_key(&spec.name).await?;
        let key_type = KeyType::parse(&archive.key_type).ok_or_else(|| {
            KeyError::Internal(format!("unknown stored key type '{}'", archive.key_type))
        })?;
        if !key_type.capabilities().supports_signing {
            return Err(KeyError::InvalidArgument(format!(
                "key '{}' of type {} does not support verification",
                spec.name,
                key_type.canonical()
            )));
        }
        let version = self.resolve_decryption_version(&archive, spec.version)?;
        let algorithms = resolve_algorithms(
            key_type,
            spec.prehashed,
            spec.hash_algorithm.as_deref(),
            spec.signature_algorithm.as_deref(),
        )?;
        validate_prehashed_input(&algorithms, &spec.input)?;
        let material = self.material_for(&archive, version).await?;
        let valid = verify_with_material(
            key_type,
            &material,
            &spec.input,
            &spec.signature,
            &algorithms,
        )?;
        Ok(VerifyOutput {
            valid,
            key_version: version,
        })
    }

    // ── 按名加密（CCT3）──

    pub async fn encrypt(
        &self,
        name: &str,
        requested_version: u32,
        plaintext: &[u8],
        context: &[u8],
    ) -> Result<(Vec<u8>, u32), KeyError> {
        let archive = self.read_key(name).await?;
        let key_type = self.encryption_key_type(&archive)?;
        let version = self.resolve_encryption_version(&archive, requested_version)?;
        let material = self.material_for(&archive, version).await?;
        let ciphertext =
            encrypt_packet(&archive, key_type, version, &material, plaintext, context)?;
        Ok((ciphertext, version))
    }

    pub async fn decrypt(
        &self,
        name: Option<&str>,
        ciphertext: &[u8],
        context: &[u8],
    ) -> Result<(Vec<u8>, u32), KeyError> {
        let packet = parse_packet(ciphertext)?;
        if let Some(requested) = name {
            if !requested.is_empty() && requested != packet.name {
                return Err(KeyError::InvalidArgument(format!(
                    "ciphertext key name '{}' does not match request '{}'",
                    packet.name, requested
                )));
            }
        }
        let archive = self.read_key(packet.name).await?;
        let key_type = self.encryption_key_type(&archive)?;
        let version = self.resolve_decryption_version(&archive, packet.version)?;
        let material = self.material_for(&archive, version).await?;
        decrypt_packet(&archive, key_type, &packet, &material, context)
    }

    /// 密文进、密文出：解包 DEK 后用 latest_version 重包（不触碰明文）
    pub async fn rewrap(
        &self,
        name: Option<&str>,
        ciphertext: &[u8],
        _context: &[u8],
    ) -> Result<(Vec<u8>, u32), KeyError> {
        let packet = parse_packet(ciphertext)?;
        if let Some(requested) = name {
            if !requested.is_empty() && requested != packet.name {
                return Err(KeyError::InvalidArgument(format!(
                    "ciphertext key name '{}' does not match request '{}'",
                    packet.name, requested
                )));
            }
        }
        let archive = self.read_key(packet.name).await?;
        let key_type = self.encryption_key_type(&archive)?;
        let old_version = self.resolve_decryption_version(&archive, packet.version)?;
        let new_version = self.resolve_encryption_version(&archive, 0)?;
        let old_material = self.material_for(&archive, old_version).await?;
        let dek = unwrap_dek(&archive, old_version, &old_material, packet.dek_packet)?;
        let new_material = self.material_for(&archive, new_version).await?;
        let new_dek_packet = wrap_dek(&archive, key_type, new_version, &new_material, &dek)?;
        let rewrapped = build_packet(
            packet.name,
            new_version,
            packet.nonce,
            &new_dek_packet,
            packet.ciphertext,
        );
        Ok((rewrapped, new_version))
    }

    pub async fn generate_data_key(
        &self,
        name: &str,
        requested_version: u32,
        context: &[u8],
        kind: DataKeyKind,
    ) -> Result<GeneratedDataKey, KeyError> {
        let archive = self.read_key(name).await?;
        let key_type = self.encryption_key_type(&archive)?;
        let version = self.resolve_encryption_version(&archive, requested_version)?;
        let material = self.material_for(&archive, version).await?;

        let mut dek = Zeroizing::new(vec![0u8; SYMMETRIC_KEY_LEN]);
        aes_gcm::aead::OsRng.fill_bytes(&mut dek);
        let wrapped = if matches!(
            kind,
            DataKeyKind::Wrapped | DataKeyKind::PlaintextAndWrapped
        ) {
            Some(encrypt_packet(
                &archive, key_type, version, &material, &dek, context,
            )?)
        } else {
            None
        };
        let plaintext = if matches!(
            kind,
            DataKeyKind::Plaintext | DataKeyKind::PlaintextAndWrapped
        ) {
            Some(dek.to_vec())
        } else {
            None
        };
        Ok(GeneratedDataKey {
            plaintext,
            ciphertext: wrapped,
            key_version: version,
        })
    }

    // ── PKI 内部材料访问（不经导出闸门；pki.rs 是可信调用方）──

    /// 读取某版本私钥 PEM（内部面：PKI 用它重建 rcgen KeyPair）
    pub(crate) async fn private_key_pem(
        &self,
        name: &str,
        version: u32,
    ) -> Result<Zeroizing<String>, KeyError> {
        let archive = self.read_key(name).await?;
        let key_type = KeyType::parse(&archive.key_type).ok_or_else(|| {
            KeyError::Internal(format!("unknown stored key type '{}'", archive.key_type))
        })?;
        if !key_type.is_asymmetric() {
            return Err(KeyError::Internal(format!(
                "key '{name}' is not an asymmetric key"
            )));
        }
        let version = self.resolve_decryption_version(&archive, version)?;
        let material = self.material_for(&archive, version).await?;
        let (bytes, _, _) = export_material(key_type, &material)?;
        let pem = String::from_utf8(bytes)
            .map_err(|e| KeyError::Internal(format!("private key PEM is not UTF-8: {e}")))?;
        Ok(Zeroizing::new(pem))
    }

    // ── 内部 ──

    fn encryption_key_type(&self, archive: &KeyArchive) -> Result<KeyType, KeyError> {
        let key_type = KeyType::parse(&archive.key_type).ok_or_else(|| {
            KeyError::Internal(format!("unknown stored key type '{}'", archive.key_type))
        })?;
        if !key_type.capabilities().supports_encryption {
            return Err(KeyError::InvalidArgument(format!(
                "key '{}' of type {} does not support encryption",
                archive.name,
                key_type.canonical()
            )));
        }
        Ok(key_type)
    }

    fn resolve_encryption_version(
        &self,
        archive: &KeyArchive,
        requested: u32,
    ) -> Result<u32, KeyError> {
        let version = if requested == 0 {
            archive.latest_version
        } else {
            requested
        };
        if version > archive.latest_version {
            return Err(KeyError::FailedPrecondition(format!(
                "key '{}' version {version} is above latest_version {} (KeyVersionException)",
                archive.name, archive.latest_version
            )));
        }
        if version < archive.min_decryption_version {
            return Err(KeyError::FailedPrecondition(format!(
                "key '{}' version {version} is below min_decryption_version {} (KeyVersionException)",
                archive.name, archive.min_decryption_version
            )));
        }
        if version < archive.min_encryption_version {
            return Err(KeyError::FailedPrecondition(format!(
                "key '{}' version {version} is below min_encryption_version {} (KeyVersionException)",
                archive.name, archive.min_encryption_version
            )));
        }
        if !archive.versions.iter().any(|meta| meta.version == version) {
            return Err(KeyError::NotFound(format!(
                "key '{}' version {version} material is not available",
                archive.name
            )));
        }
        Ok(version)
    }

    fn resolve_decryption_version(
        &self,
        archive: &KeyArchive,
        requested: u32,
    ) -> Result<u32, KeyError> {
        let version = if requested == 0 {
            archive.latest_version
        } else {
            requested
        };
        if version > archive.latest_version {
            return Err(KeyError::FailedPrecondition(format!(
                "key '{}' version {version} is above latest_version {} (KeyVersionException)",
                archive.name, archive.latest_version
            )));
        }
        if version < archive.min_decryption_version {
            return Err(KeyError::FailedPrecondition(format!(
                "key '{}' version {version} is below min_decryption_version {} (KeyVersionException)",
                archive.name, archive.min_decryption_version
            )));
        }
        if !archive.versions.iter().any(|meta| meta.version == version) {
            return Err(KeyError::NotFound(format!(
                "key '{}' version {version} material is not available",
                archive.name
            )));
        }
        Ok(version)
    }

    async fn material_for(
        &self,
        archive: &KeyArchive,
        version: u32,
    ) -> Result<Zeroizing<Vec<u8>>, KeyError> {
        let record = self
            .store
            .get_material(&archive.name, version)
            .await?
            .ok_or_else(|| {
                KeyError::FailedPrecondition(format!(
                    "key '{}' version {version} material is unavailable",
                    archive.name
                ))
            })?;
        self.unwrap_material(archive, version, &record)
    }

    fn wrap_material(
        &self,
        archive: &KeyArchive,
        version: u32,
        material: &[u8],
    ) -> Result<KeyVersionRecord, KeyError> {
        let kek = self.keyring.primary();
        let cipher = Aes256Gcm::new_from_slice(kek.as_bytes())
            .map_err(|e| KeyError::Internal(format!("invalid KEK material: {e}")))?;
        let mut nonce_bytes = [0u8; NONCE_LEN];
        aes_gcm::aead::OsRng.fill_bytes(&mut nonce_bytes);
        let aad = material_aad(archive, version);
        let ciphertext = cipher
            .encrypt(
                Nonce::from_slice(&nonce_bytes),
                Payload {
                    msg: material,
                    aad: &aad,
                },
            )
            .map_err(|_| KeyError::Internal("failed to wrap key material".into()))?;
        let mut wrapped = Vec::with_capacity(NONCE_LEN + ciphertext.len());
        wrapped.extend_from_slice(&nonce_bytes);
        wrapped.extend_from_slice(&ciphertext);
        Ok(KeyVersionRecord {
            wrapped,
            created_at: now_unix(),
            kek_id: self.keyring.primary_id().to_string(),
        })
    }

    fn unwrap_material(
        &self,
        archive: &KeyArchive,
        version: u32,
        record: &KeyVersionRecord,
    ) -> Result<Zeroizing<Vec<u8>>, KeyError> {
        let kek = self.keyring.material_for(&record.kek_id).ok_or_else(|| {
            KeyError::FailedPrecondition(format!(
                "KEK material '{}' is not injected; cannot read key '{}' version {version}",
                record.kek_id, archive.name
            ))
        })?;
        if record.wrapped.len() < NONCE_LEN + 16 {
            return Err(KeyError::FailedPrecondition(format!(
                "wrapped material for key '{}' version {version} is malformed",
                archive.name
            )));
        }
        let cipher = Aes256Gcm::new_from_slice(kek.as_bytes())
            .map_err(|e| KeyError::Internal(format!("invalid KEK material: {e}")))?;
        let (nonce_bytes, ciphertext) = record.wrapped.split_at(NONCE_LEN);
        let aad = material_aad(archive, version);
        let plaintext = cipher
            .decrypt(
                Nonce::from_slice(nonce_bytes),
                Payload {
                    msg: ciphertext,
                    aad: &aad,
                },
            )
            .map_err(|_| {
                KeyError::FailedPrecondition(format!(
                    "key '{}' version {version} material cannot be unwrapped with KEK material '{}'",
                    archive.name, record.kek_id
                ))
            })?;
        Ok(Zeroizing::new(plaintext))
    }
}

// ──── 材料生成/导入/导出 ────

fn material_aad(archive: &KeyArchive, version: u32) -> Vec<u8> {
    let mut aad = Vec::with_capacity(32 + archive.name.len() + archive.key_type.len());
    aad.extend_from_slice(b"coord-transit-keymaterial-v1");
    aad.extend_from_slice(&(archive.name.len() as u32).to_be_bytes());
    aad.extend_from_slice(archive.name.as_bytes());
    aad.extend_from_slice(&version.to_be_bytes());
    aad.extend_from_slice(archive.key_type.as_bytes());
    aad
}

async fn generate_material(key_type: KeyType) -> Result<Zeroizing<Vec<u8>>, KeyError> {
    if let Some(bits) = key_type.rsa_bits() {
        return tokio::task::spawn_blocking(move || generate_material_blocking(key_type, bits))
            .await
            .map_err(|e| KeyError::Internal(format!("RSA key generation task join: {e}")))?;
    }
    generate_material_blocking(key_type, 0)
}

fn generate_material_blocking(
    key_type: KeyType,
    _rsa_bits: usize,
) -> Result<Zeroizing<Vec<u8>>, KeyError> {
    let mut rng = aes_gcm::aead::OsRng;
    match key_type {
        KeyType::Aes256Gcm96 => {
            let mut key = Zeroizing::new(vec![0u8; SYMMETRIC_KEY_LEN]);
            rng.fill_bytes(&mut key);
            Ok(key)
        }
        KeyType::Ed25519 => {
            // 材料 = 32 字节 seed（导出时再包成 OpenSSL 3.0 可读的 PKCS#8 v1）
            let mut seed = Zeroizing::new(vec![0u8; 32]);
            rng.fill_bytes(&mut seed);
            Ok(seed)
        }
        KeyType::EcdsaP256 => {
            let key = p256::SecretKey::try_generate_from_rng(&mut getrandom::SysRng)
                .map_err(|e| KeyError::Internal(format!("p256 key generation: {e}")))?;
            let der = key
                .to_pkcs8_der()
                .map_err(|e| KeyError::Internal(format!("p256 pkcs8 encode: {e}")))?;
            Ok(Zeroizing::new(der.as_bytes().to_vec()))
        }
        KeyType::EcdsaP384 => {
            let key = p384::SecretKey::try_generate_from_rng(&mut getrandom::SysRng)
                .map_err(|e| KeyError::Internal(format!("p384 key generation: {e}")))?;
            let der = key
                .to_pkcs8_der()
                .map_err(|e| KeyError::Internal(format!("p384 pkcs8 encode: {e}")))?;
            Ok(Zeroizing::new(der.as_bytes().to_vec()))
        }
        KeyType::EcdsaP521 => {
            let key = p521::SecretKey::try_generate_from_rng(&mut getrandom::SysRng)
                .map_err(|e| KeyError::Internal(format!("p521 key generation: {e}")))?;
            let der = key
                .to_pkcs8_der()
                .map_err(|e| KeyError::Internal(format!("p521 pkcs8 encode: {e}")))?;
            Ok(Zeroizing::new(der.as_bytes().to_vec()))
        }
        KeyType::Rsa2048 | KeyType::Rsa3072 | KeyType::Rsa4096 => {
            let bits = key_type.rsa_bits().unwrap_or(2048);
            let key = RsaPrivateKey::new(&mut getrandom::SysRng, bits)
                .map_err(|e| KeyError::Internal(format!("RSA key generation: {e}")))?;
            let der = key
                .to_pkcs8_der()
                .map_err(|e| KeyError::Internal(format!("RSA pkcs8 encode: {e}")))?;
            Ok(Zeroizing::new(der.as_bytes().to_vec()))
        }
    }
}

fn normalize_import(
    key_type: KeyType,
    material: &[u8],
    format: Option<MaterialFormat>,
) -> Result<Zeroizing<Vec<u8>>, KeyError> {
    let looks_pem = material.starts_with(b"-----BEGIN");
    let format = format.unwrap_or(if looks_pem {
        MaterialFormat::Pem
    } else if key_type.is_asymmetric() {
        MaterialFormat::Der
    } else {
        MaterialFormat::Raw
    });

    match key_type {
        KeyType::Aes256Gcm96 => {
            if format != MaterialFormat::Raw || material.len() != SYMMETRIC_KEY_LEN {
                return Err(KeyError::InvalidArgument(format!(
                    "AES-256 key material must be raw {SYMMETRIC_KEY_LEN} bytes"
                )));
            }
            Ok(Zeroizing::new(material.to_vec()))
        }
        KeyType::Ed25519 => {
            let key = decode_asymmetric_pem_or_der(
                material,
                format,
                |pem| Ed25519SigningKey::from_pkcs8_pem(pem).map_err(|e| e.to_string()),
                |der| Ed25519SigningKey::from_pkcs8_der(der).map_err(|e| e.to_string()),
            )?;
            Ok(Zeroizing::new(key.to_bytes().to_vec()))
        }
        KeyType::EcdsaP256 => {
            let key = decode_asymmetric_pem_or_der(
                material,
                format,
                |pem| p256::SecretKey::from_pkcs8_pem(pem).map_err(|e| e.to_string()),
                |der| p256::SecretKey::from_pkcs8_der(der).map_err(|e| e.to_string()),
            )?;
            let der = key
                .to_pkcs8_der()
                .map_err(|e| KeyError::InvalidArgument(format!("p256 pkcs8 encode: {e}")))?;
            Ok(Zeroizing::new(der.as_bytes().to_vec()))
        }
        KeyType::EcdsaP384 => {
            let key = decode_asymmetric_pem_or_der(
                material,
                format,
                |pem| p384::SecretKey::from_pkcs8_pem(pem).map_err(|e| e.to_string()),
                |der| p384::SecretKey::from_pkcs8_der(der).map_err(|e| e.to_string()),
            )?;
            let der = key
                .to_pkcs8_der()
                .map_err(|e| KeyError::InvalidArgument(format!("p384 pkcs8 encode: {e}")))?;
            Ok(Zeroizing::new(der.as_bytes().to_vec()))
        }
        KeyType::EcdsaP521 => {
            let key = decode_asymmetric_pem_or_der(
                material,
                format,
                |pem| p521::SecretKey::from_pkcs8_pem(pem).map_err(|e| e.to_string()),
                |der| p521::SecretKey::from_pkcs8_der(der).map_err(|e| e.to_string()),
            )?;
            let der = key
                .to_pkcs8_der()
                .map_err(|e| KeyError::InvalidArgument(format!("p521 pkcs8 encode: {e}")))?;
            Ok(Zeroizing::new(der.as_bytes().to_vec()))
        }
        KeyType::Rsa2048 | KeyType::Rsa3072 | KeyType::Rsa4096 => {
            let key = decode_asymmetric_pem_or_der(
                material,
                format,
                |pem| RsaPrivateKey::from_pkcs8_pem(pem).map_err(|e| e.to_string()),
                |der| RsaPrivateKey::from_pkcs8_der(der).map_err(|e| e.to_string()),
            )?;
            let bits = key.n().bit_width();
            let expected = key_type.rsa_bits().unwrap_or(2048) as u32;
            if bits != expected {
                return Err(KeyError::InvalidArgument(format!(
                    "RSA key has {bits} bits but type {} requires {expected}",
                    key_type.canonical()
                )));
            }
            let der = key
                .to_pkcs8_der()
                .map_err(|e| KeyError::InvalidArgument(format!("RSA pkcs8 encode: {e}")))?;
            Ok(Zeroizing::new(der.as_bytes().to_vec()))
        }
    }
}

fn decode_asymmetric_pem_or_der<T>(
    material: &[u8],
    format: MaterialFormat,
    from_pem: impl FnOnce(&str) -> Result<T, String>,
    from_der: impl FnOnce(&[u8]) -> Result<T, String>,
) -> Result<T, KeyError> {
    let parsed = match format {
        MaterialFormat::Pem => {
            let pem = std::str::from_utf8(material)
                .map_err(|_| KeyError::InvalidArgument("PEM material is not valid UTF-8".into()))?;
            from_pem(pem)
        }
        MaterialFormat::Der => from_der(material),
        MaterialFormat::Raw => {
            return Err(KeyError::InvalidArgument(
                "asymmetric key material must be PKCS#8 PEM or DER".into(),
            ))
        }
    };
    parsed.map_err(|e| KeyError::InvalidArgument(format!("invalid PKCS#8 private key: {e}")))
}

/// Ed25519 PKCS#8 v1（无公钥字段）：OpenSSL 3.0 不接受 dalek 默认的 v2 编码。
fn ed25519_pkcs8_v1_der(seed: &[u8; 32]) -> Vec<u8> {
    let mut der = Vec::with_capacity(48);
    der.extend_from_slice(&[
        0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70,
    ]);
    der.extend_from_slice(&[0x04, 0x22, 0x04, 0x20]);
    der.extend_from_slice(seed);
    der
}

fn der_to_pem(der: &[u8]) -> String {
    use base64::Engine as _;
    let body = base64::engine::general_purpose::STANDARD.encode(der);
    let mut pem = String::with_capacity(body.len() + 64);
    pem.push_str("-----BEGIN PRIVATE KEY-----\n");
    for chunk in body.as_bytes().chunks(64) {
        pem.push_str(std::str::from_utf8(chunk).unwrap_or_default());
        pem.push('\n');
    }
    pem.push_str("-----END PRIVATE KEY-----\n");
    pem
}

fn export_material(
    key_type: KeyType,
    der: &[u8],
) -> Result<(Vec<u8>, &'static str, bool), KeyError> {
    if key_type == KeyType::Aes256Gcm96 {
        return Ok((der.to_vec(), "raw", false));
    }
    let pem = match key_type {
        KeyType::Ed25519 => {
            let seed: &[u8; 32] = der.try_into().map_err(|_| {
                KeyError::Internal("stored ed25519 material is not a 32-byte seed".into())
            })?;
            der_to_pem(&ed25519_pkcs8_v1_der(seed))
        }
        KeyType::EcdsaP256 => p256::SecretKey::from_pkcs8_der(der)
            .map_err(|e| KeyError::Internal(format!("stored p256 material is invalid: {e}")))?
            .to_pkcs8_pem(LineEnding::LF)
            .map_err(|e| KeyError::Internal(format!("p256 pkcs8 pem encode: {e}")))?
            .to_string(),
        KeyType::EcdsaP384 => p384::SecretKey::from_pkcs8_der(der)
            .map_err(|e| KeyError::Internal(format!("stored p384 material is invalid: {e}")))?
            .to_pkcs8_pem(LineEnding::LF)
            .map_err(|e| KeyError::Internal(format!("p384 pkcs8 pem encode: {e}")))?
            .to_string(),
        KeyType::EcdsaP521 => p521::SecretKey::from_pkcs8_der(der)
            .map_err(|e| KeyError::Internal(format!("stored p521 material is invalid: {e}")))?
            .to_pkcs8_pem(LineEnding::LF)
            .map_err(|e| KeyError::Internal(format!("p521 pkcs8 pem encode: {e}")))?
            .to_string(),
        KeyType::Rsa2048 | KeyType::Rsa3072 | KeyType::Rsa4096 => {
            RsaPrivateKey::from_pkcs8_der(der)
                .map_err(|e| KeyError::Internal(format!("stored RSA material is invalid: {e}")))?
                .to_pkcs8_pem(LineEnding::LF)
                .map_err(|e| KeyError::Internal(format!("RSA pkcs8 pem encode: {e}")))?
                .to_string()
        }
        KeyType::Aes256Gcm96 => {
            return Err(KeyError::Internal(
                "symmetric key reached asymmetric export".into(),
            ))
        }
    };
    Ok((pem.into_bytes(), "pem", true))
}

// ──── 签名 ────

struct ResolvedAlgorithms {
    prehashed: bool,
    hash: Option<HashAlg>,
    rsa_scheme: Option<RsaScheme>,
    canonical: String,
}

fn resolve_algorithms(
    key_type: KeyType,
    prehashed: bool,
    hash_algorithm: Option<&str>,
    signature_algorithm: Option<&str>,
) -> Result<ResolvedAlgorithms, KeyError> {
    let raw_sig = signature_algorithm.unwrap_or("").trim();
    let sig = if raw_sig.is_empty() {
        None
    } else {
        Some(raw_sig.to_ascii_lowercase())
    };

    match key_type {
        KeyType::Ed25519 => {
            if prehashed {
                return Err(KeyError::InvalidArgument(
                    "Ed25519 does not support prehashed signing (EdDSA)".into(),
                ));
            }
            if hash_algorithm
                .map(|h| !h.trim().is_empty())
                .unwrap_or(false)
            {
                return Err(KeyError::InvalidArgument(
                    "Ed25519 signs the full message; hash_algorithm is not applicable".into(),
                ));
            }
            match sig.as_deref() {
                None | Some("ed25519") => {}
                Some(other) => {
                    return Err(KeyError::InvalidArgument(format!(
                        "Ed25519 key cannot use signature_algorithm '{other}'"
                    )))
                }
            }
            Ok(ResolvedAlgorithms {
                prehashed: false,
                hash: None,
                rsa_scheme: None,
                canonical: "ed25519".into(),
            })
        }
        KeyType::EcdsaP256 | KeyType::EcdsaP384 | KeyType::EcdsaP521 => {
            match sig.as_deref() {
                None | Some("ecdsa") => {}
                Some(other) => {
                    return Err(KeyError::InvalidArgument(format!(
                        "ECDSA key cannot use signature_algorithm '{other}'"
                    )))
                }
            }
            let explicit = match hash_algorithm.filter(|h| !h.trim().is_empty()) {
                Some(raw) => Some(HashAlg::parse(raw).ok_or_else(|| {
                    KeyError::InvalidArgument(format!(
                        "unsupported hash_algorithm '{raw}' (expected SHA256/SHA384/SHA512)"
                    ))
                })?),
                None => None,
            };
            if prehashed && explicit.is_none() {
                return Err(KeyError::InvalidArgument(
                    "prehashed=true requires hash_algorithm (SHA256/SHA384/SHA512)".into(),
                ));
            }
            let hash = explicit.or_else(|| key_type.ecdsa_default_hash());
            Ok(ResolvedAlgorithms {
                prehashed,
                hash,
                rsa_scheme: None,
                canonical: format!("ecdsa-{}", hash.unwrap_or(HashAlg::Sha256).canonical()),
            })
        }
        KeyType::Rsa2048 | KeyType::Rsa3072 | KeyType::Rsa4096 => {
            let scheme = match sig.as_deref() {
                None | Some("pss") => RsaScheme::Pss,
                Some("pkcs1v15") => RsaScheme::Pkcs1v15,
                Some(other) => {
                    return Err(KeyError::InvalidArgument(format!(
                        "unknown RSA signature_algorithm '{other}' (expected pss or pkcs1v15)"
                    )))
                }
            };
            let explicit = match hash_algorithm.filter(|h| !h.trim().is_empty()) {
                Some(raw) => Some(HashAlg::parse(raw).ok_or_else(|| {
                    KeyError::InvalidArgument(format!(
                        "unsupported hash_algorithm '{raw}' (expected SHA256/SHA384/SHA512)"
                    ))
                })?),
                None => None,
            };
            if prehashed && explicit.is_none() {
                return Err(KeyError::InvalidArgument(
                    "prehashed=true requires hash_algorithm (SHA256/SHA384/SHA512)".into(),
                ));
            }
            let hash = explicit.unwrap_or(HashAlg::Sha256);
            let canonical = match scheme {
                RsaScheme::Pkcs1v15 => format!("rsa-pkcs1v15-{}", hash.canonical()),
                RsaScheme::Pss => format!("rsa-pss-{}", hash.canonical()),
            };
            Ok(ResolvedAlgorithms {
                prehashed,
                hash: Some(hash),
                rsa_scheme: Some(scheme),
                canonical,
            })
        }
        KeyType::Aes256Gcm96 => Err(KeyError::InvalidArgument(
            "symmetric keys do not support signing".into(),
        )),
    }
}

impl HashAlg {
    fn digest(self, data: &[u8]) -> Vec<u8> {
        match self {
            Self::Sha256 => Sha256::digest(data).to_vec(),
            Self::Sha384 => Sha384::digest(data).to_vec(),
            Self::Sha512 => Sha512::digest(data).to_vec(),
        }
    }
}

/// `prehashed=true` 的 input 是摘要本身：长度必须与声明的 hash 匹配，
/// 否则签名会被错误长度的输入静默签出（同一个签名无法对应任何明文摘要）。
fn validate_prehashed_input(algorithms: &ResolvedAlgorithms, input: &[u8]) -> Result<(), KeyError> {
    if !algorithms.prehashed {
        return Ok(());
    }
    let hash = algorithms
        .hash
        .ok_or_else(|| KeyError::Internal("prehashed signing requires a hash".into()))?;
    if input.len() != hash.digest_len() {
        return Err(KeyError::InvalidArgument(format!(
            "prehashed=true input must be exactly {} bytes for {} (got {})",
            hash.digest_len(),
            hash.canonical(),
            input.len()
        )));
    }
    Ok(())
}

fn require_prehash(prehash: &Option<Vec<u8>>) -> Result<&[u8], KeyError> {
    prehash.as_deref().ok_or_else(|| {
        KeyError::Internal("missing digest for a signature algorithm that requires one".into())
    })
}

fn sign_with_material(
    key_type: KeyType,
    material: &[u8],
    input: &[u8],
    algorithms: &ResolvedAlgorithms,
) -> Result<Vec<u8>, KeyError> {
    let prehash = if algorithms.prehashed {
        Some(input.to_vec())
    } else {
        algorithms.hash.map(|hash| hash.digest(input))
    };
    match key_type {
        KeyType::Ed25519 => {
            let seed: &[u8; 32] = material.try_into().map_err(|_| {
                KeyError::Internal("stored ed25519 material is not a 32-byte seed".into())
            })?;
            let key = Ed25519SigningKey::from_bytes(seed);
            Ok(key.sign(input).to_bytes().to_vec())
        }
        KeyType::EcdsaP256 => {
            let key = P256SigningKey::from_pkcs8_der(material)
                .map_err(|e| KeyError::Internal(format!("stored p256 material is invalid: {e}")))?;
            let signature: P256Signature = key
                .sign_prehash(require_prehash(&prehash)?)
                .map_err(|e| KeyError::Internal(format!("p256 sign: {e}")))?;
            Ok(signature.to_der().as_bytes().to_vec())
        }
        KeyType::EcdsaP384 => {
            let key = P384SigningKey::from_pkcs8_der(material)
                .map_err(|e| KeyError::Internal(format!("stored p384 material is invalid: {e}")))?;
            let signature: P384Signature = key
                .sign_prehash(require_prehash(&prehash)?)
                .map_err(|e| KeyError::Internal(format!("p384 sign: {e}")))?;
            Ok(signature.to_der().as_bytes().to_vec())
        }
        KeyType::EcdsaP521 => {
            let secret = p521::SecretKey::from_pkcs8_der(material)
                .map_err(|e| KeyError::Internal(format!("stored p521 material is invalid: {e}")))?;
            let key = P521SigningKey::from_slice(&secret.to_bytes())
                .map_err(|e| KeyError::Internal(format!("stored p521 material is invalid: {e}")))?;
            let signature: P521Signature = key
                .sign_prehash(require_prehash(&prehash)?)
                .map_err(|e| KeyError::Internal(format!("p521 sign: {e}")))?;
            Ok(signature.to_der().as_bytes().to_vec())
        }
        KeyType::Rsa2048 | KeyType::Rsa3072 | KeyType::Rsa4096 => {
            let key = RsaPrivateKey::from_pkcs8_der(material)
                .map_err(|e| KeyError::Internal(format!("stored RSA material is invalid: {e}")))?;
            let digest = require_prehash(&prehash)?;
            rsa_sign(key, digest, algorithms)
        }
        KeyType::Aes256Gcm96 => Err(KeyError::Internal("symmetric key reached sign".into())),
    }
}

fn rsa_sign(
    key: RsaPrivateKey,
    digest: &[u8],
    algorithms: &ResolvedAlgorithms,
) -> Result<Vec<u8>, KeyError> {
    let hash = algorithms.hash.unwrap_or(HashAlg::Sha256);
    let scheme = algorithms.rsa_scheme.unwrap_or(RsaScheme::Pss);
    let signed = match (scheme, hash) {
        (RsaScheme::Pkcs1v15, HashAlg::Sha256) => key.pkcs1v15_sign_prehash::<RsaSha256>(digest),
        (RsaScheme::Pkcs1v15, HashAlg::Sha384) => key.pkcs1v15_sign_prehash::<RsaSha384>(digest),
        (RsaScheme::Pkcs1v15, HashAlg::Sha512) => key.pkcs1v15_sign_prehash::<RsaSha512>(digest),
        (RsaScheme::Pss, HashAlg::Sha256) => key.pss_sign_prehash::<RsaSha256>(digest),
        (RsaScheme::Pss, HashAlg::Sha384) => key.pss_sign_prehash::<RsaSha384>(digest),
        (RsaScheme::Pss, HashAlg::Sha512) => key.pss_sign_prehash::<RsaSha512>(digest),
    };
    signed.map_err(|e| KeyError::Internal(format!("rsa sign: {e}")))
}

fn verify_with_material(
    key_type: KeyType,
    material: &[u8],
    input: &[u8],
    signature: &[u8],
    algorithms: &ResolvedAlgorithms,
) -> Result<bool, KeyError> {
    let prehash = if algorithms.prehashed {
        Some(input.to_vec())
    } else {
        algorithms.hash.map(|hash| hash.digest(input))
    };
    match key_type {
        KeyType::Ed25519 => {
            let seed: &[u8; 32] = material.try_into().map_err(|_| {
                KeyError::Internal("stored ed25519 material is not a 32-byte seed".into())
            })?;
            let key = Ed25519SigningKey::from_bytes(seed);
            let verifying = Ed25519VerifyingKey::from(&key);
            let signature = ed25519_dalek::Signature::from_slice(signature)
                .map_err(|_| KeyError::InvalidArgument("malformed Ed25519 signature".into()))?;
            Ok(verifying.verify_strict(input, &signature).is_ok())
        }
        KeyType::EcdsaP256 => {
            let key = P256SigningKey::from_pkcs8_der(material)
                .map_err(|e| KeyError::Internal(format!("stored p256 material is invalid: {e}")))?;
            let verifying = P256VerifyingKey::from(&key);
            let signature = P256Signature::from_der(signature).map_err(|_| {
                KeyError::InvalidArgument("malformed ECDSA P-256 DER signature".into())
            })?;
            Ok(verifying
                .verify_prehash(require_prehash(&prehash)?, &signature)
                .is_ok())
        }
        KeyType::EcdsaP384 => {
            let key = P384SigningKey::from_pkcs8_der(material)
                .map_err(|e| KeyError::Internal(format!("stored p384 material is invalid: {e}")))?;
            let verifying = P384VerifyingKey::from(&key);
            let signature = P384Signature::from_der(signature).map_err(|_| {
                KeyError::InvalidArgument("malformed ECDSA P-384 DER signature".into())
            })?;
            Ok(verifying
                .verify_prehash(require_prehash(&prehash)?, &signature)
                .is_ok())
        }
        KeyType::EcdsaP521 => {
            let secret = p521::SecretKey::from_pkcs8_der(material)
                .map_err(|e| KeyError::Internal(format!("stored p521 material is invalid: {e}")))?;
            let key = P521SigningKey::from_slice(&secret.to_bytes())
                .map_err(|e| KeyError::Internal(format!("stored p521 material is invalid: {e}")))?;
            let verifying = P521VerifyingKey::from(&key);
            let signature = P521Signature::from_der(signature).map_err(|_| {
                KeyError::InvalidArgument("malformed ECDSA P-521 DER signature".into())
            })?;
            Ok(verifying
                .verify_prehash(require_prehash(&prehash)?, &signature)
                .is_ok())
        }
        KeyType::Rsa2048 | KeyType::Rsa3072 | KeyType::Rsa4096 => {
            let key = RsaPrivateKey::from_pkcs8_der(material)
                .map_err(|e| KeyError::Internal(format!("stored RSA material is invalid: {e}")))?;
            Ok(rsa_verify(
                RsaPublicKey::from(&key),
                require_prehash(&prehash)?,
                signature,
                algorithms,
            )?)
        }
        KeyType::Aes256Gcm96 => Err(KeyError::Internal("symmetric key reached verify".into())),
    }
}

fn rsa_verify(
    key: RsaPublicKey,
    digest: &[u8],
    signature: &[u8],
    algorithms: &ResolvedAlgorithms,
) -> Result<bool, KeyError> {
    if signature.len() != key.size() {
        return Err(KeyError::InvalidArgument(
            "malformed RSA signature length".into(),
        ));
    }
    let hash = algorithms.hash.unwrap_or(HashAlg::Sha256);
    let scheme = algorithms.rsa_scheme.unwrap_or(RsaScheme::Pss);
    let valid = match (scheme, hash) {
        (RsaScheme::Pkcs1v15, HashAlg::Sha256) => {
            key.pkcs1v15_verify_prehash::<RsaSha256>(digest, signature)
        }
        (RsaScheme::Pkcs1v15, HashAlg::Sha384) => {
            key.pkcs1v15_verify_prehash::<RsaSha384>(digest, signature)
        }
        (RsaScheme::Pkcs1v15, HashAlg::Sha512) => {
            key.pkcs1v15_verify_prehash::<RsaSha512>(digest, signature)
        }
        (RsaScheme::Pss, HashAlg::Sha256) => key.pss_verify_prehash::<RsaSha256>(digest, signature),
        (RsaScheme::Pss, HashAlg::Sha384) => key.pss_verify_prehash::<RsaSha384>(digest, signature),
        (RsaScheme::Pss, HashAlg::Sha512) => key.pss_verify_prehash::<RsaSha512>(digest, signature),
    };
    Ok(valid.is_ok())
}

// ──── CCT3 包 ────

pub struct ParsedPacket<'a> {
    pub name: &'a str,
    pub version: u32,
    pub nonce: &'a [u8],
    pub dek_packet: &'a [u8],
    pub ciphertext: &'a [u8],
    pub header_len: usize,
}

fn parse_packet(bytes: &[u8]) -> Result<ParsedPacket<'_>, KeyError> {
    let invalid =
        |reason: &str| KeyError::InvalidArgument(format!("malformed CCT3 ciphertext: {reason}"));
    if bytes.len() < CCT3_MAGIC.len() + 1 {
        return Err(invalid("truncated header"));
    }
    if &bytes[..CCT3_MAGIC.len()] != CCT3_MAGIC {
        return Err(invalid(
            "bad magic (CCT2 and anonymous packets are not supported)",
        ));
    }
    let name_len = bytes[4] as usize;
    let mut offset = 5;
    let header_len = offset + name_len + 4 + NONCE_LEN + DEK_PACKET_LEN;
    if bytes.len() < header_len {
        return Err(invalid("truncated header"));
    }
    let name = std::str::from_utf8(&bytes[offset..offset + name_len])
        .map_err(|_| invalid("key name is not UTF-8"))?;
    offset += name_len;
    let version = u32::from_be_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .map_err(|_| invalid("truncated version"))?,
    );
    offset += 4;
    let nonce = &bytes[offset..offset + NONCE_LEN];
    offset += NONCE_LEN;
    let dek_packet = &bytes[offset..offset + DEK_PACKET_LEN];
    offset += DEK_PACKET_LEN;
    let ciphertext = &bytes[offset..];
    if ciphertext.len() < 16 {
        return Err(invalid("ciphertext shorter than the AES-GCM tag"));
    }
    Ok(ParsedPacket {
        name,
        version,
        nonce,
        dek_packet,
        ciphertext,
        header_len,
    })
}

fn build_packet(
    name: &str,
    version: u32,
    nonce: &[u8],
    dek_packet: &[u8],
    ciphertext: &[u8],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(
        CCT3_MAGIC.len() + 1 + name.len() + 4 + nonce.len() + dek_packet.len() + ciphertext.len(),
    );
    out.extend_from_slice(CCT3_MAGIC);
    out.push(name.len() as u8);
    out.extend_from_slice(name.as_bytes());
    out.extend_from_slice(&version.to_be_bytes());
    out.extend_from_slice(nonce);
    out.extend_from_slice(dek_packet);
    out.extend_from_slice(ciphertext);
    out
}

fn dek_aad(archive: &KeyArchive, version: u32) -> Vec<u8> {
    let mut aad = Vec::with_capacity(32 + archive.name.len() + archive.key_type.len());
    aad.extend_from_slice(b"coord-cct3-dek-v1");
    aad.push(archive.name.len() as u8);
    aad.extend_from_slice(archive.name.as_bytes());
    aad.extend_from_slice(&version.to_be_bytes());
    aad.extend_from_slice(archive.key_type.as_bytes());
    aad
}

fn wrap_dek(
    archive: &KeyArchive,
    _key_type: KeyType,
    version: u32,
    material: &[u8],
    dek: &[u8],
) -> Result<Vec<u8>, KeyError> {
    let cipher = Aes256Gcm::new_from_slice(material)
        .map_err(|_| KeyError::Internal("key material is not a valid AES-256 key".into()))?;
    let mut nonce_bytes = [0u8; NONCE_LEN];
    aes_gcm::aead::OsRng.fill_bytes(&mut nonce_bytes);
    let aad = dek_aad(archive, version);
    let ciphertext = cipher
        .encrypt(
            Nonce::from_slice(&nonce_bytes),
            Payload {
                msg: dek,
                aad: &aad,
            },
        )
        .map_err(|_| KeyError::Internal("failed to wrap DEK".into()))?;
    let mut packet = Vec::with_capacity(NONCE_LEN + ciphertext.len());
    packet.extend_from_slice(&nonce_bytes);
    packet.extend_from_slice(&ciphertext);
    Ok(packet)
}

fn unwrap_dek(
    archive: &KeyArchive,
    version: u32,
    material: &[u8],
    dek_packet: &[u8],
) -> Result<Zeroizing<Vec<u8>>, KeyError> {
    let cipher = Aes256Gcm::new_from_slice(material)
        .map_err(|_| KeyError::Internal("key material is not a valid AES-256 key".into()))?;
    let (nonce_bytes, ciphertext) = dek_packet.split_at(NONCE_LEN);
    let aad = dek_aad(archive, version);
    let dek = cipher
        .decrypt(
            Nonce::from_slice(nonce_bytes),
            Payload {
                msg: ciphertext,
                aad: &aad,
            },
        )
        .map_err(|_| {
            KeyError::InvalidArgument(
                "ciphertext authentication failed (wrong key/version or tampered header)".into(),
            )
        })?;
    Ok(Zeroizing::new(dek))
}

fn encrypt_packet(
    archive: &KeyArchive,
    key_type: KeyType,
    version: u32,
    material: &[u8],
    plaintext: &[u8],
    context: &[u8],
) -> Result<Vec<u8>, KeyError> {
    let mut dek = Zeroizing::new(vec![0u8; SYMMETRIC_KEY_LEN]);
    aes_gcm::aead::OsRng.fill_bytes(&mut dek);
    let dek_packet = wrap_dek(archive, key_type, version, material, &dek)?;

    let cipher = Aes256Gcm::new_from_slice(&dek)
        .map_err(|_| KeyError::Internal("generated DEK is not a valid AES-256 key".into()))?;
    let mut nonce_bytes = [0u8; NONCE_LEN];
    aes_gcm::aead::OsRng.fill_bytes(&mut nonce_bytes);
    let ciphertext = cipher
        .encrypt(
            Nonce::from_slice(&nonce_bytes),
            Payload {
                msg: plaintext,
                aad: context,
            },
        )
        .map_err(|_| KeyError::Internal("failed to encrypt plaintext".into()))?;
    Ok(build_packet(
        &archive.name,
        version,
        &nonce_bytes,
        &dek_packet,
        &ciphertext,
    ))
}

fn decrypt_packet(
    archive: &KeyArchive,
    _key_type: KeyType,
    packet: &ParsedPacket<'_>,
    material: &[u8],
    context: &[u8],
) -> Result<(Vec<u8>, u32), KeyError> {
    let dek = unwrap_dek(archive, packet.version, material, packet.dek_packet)?;
    let cipher = Aes256Gcm::new_from_slice(&dek)
        .map_err(|_| KeyError::Internal("unwrapped DEK is not a valid AES-256 key".into()))?;
    // `header_len` 已经确保字段边界；这里重算 ciphertext 起点，避免依赖调用方切片。
    let ciphertext = &packet.ciphertext;
    let plaintext = cipher
        .decrypt(
            Nonce::from_slice(packet.nonce),
            Payload {
                msg: ciphertext,
                aad: context,
            },
        )
        .map_err(|_| {
            KeyError::InvalidArgument(
                "ciphertext authentication failed (wrong context or tampered ciphertext)".into(),
            )
        })?;
    Ok((plaintext, packet.version))
}

// ──── 单元测试 ────

#[cfg(test)]
mod tests {
    use super::*;

    async fn registry() -> TransitKeyRegistry {
        TransitKeyRegistry::in_memory().expect("in-memory registry")
    }

    fn create(name: &str, key_type: KeyType) -> CreateKeySpec {
        CreateKeySpec {
            name: name.into(),
            key_type,
            exportable: false,
        }
    }

    fn sign_spec(name: &str, input: &[u8]) -> SignSpec {
        SignSpec {
            name: name.into(),
            version: 0,
            input: input.to_vec(),
            prehashed: false,
            hash_algorithm: None,
            signature_algorithm: None,
        }
    }

    fn verify_spec(name: &str, input: &[u8], signature: &[u8]) -> VerifySpec {
        VerifySpec {
            name: name.into(),
            version: 0,
            input: input.to_vec(),
            signature: signature.to_vec(),
            prehashed: false,
            hash_algorithm: None,
            signature_algorithm: None,
        }
    }

    #[tokio::test]
    async fn create_is_idempotent_and_conflict_rejected() {
        let reg = registry().await;
        let first = reg
            .create_key(create("k", KeyType::Aes256Gcm96))
            .await
            .unwrap();
        let second = reg
            .create_key(create("k", KeyType::Aes256Gcm96))
            .await
            .unwrap();
        assert_eq!(first.latest_version, second.latest_version);

        let err = reg
            .create_key(CreateKeySpec {
                name: "k".into(),
                key_type: KeyType::Ed25519,
                exportable: false,
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), "ALREADY_EXISTS");
    }

    #[tokio::test]
    async fn pki_namespace_is_reserved_for_internal_management() {
        let reg = registry().await;
        let name = "pki/ca";
        assert_eq!(
            reg.create_key(create(name, KeyType::EcdsaP256))
                .await
                .unwrap_err()
                .code(),
            "PERMISSION_DENIED"
        );
        assert_eq!(
            reg.import_key(ImportKeySpec {
                name: name.into(),
                key_type: KeyType::EcdsaP256,
                material: vec![0u8; 32],
                format: None,
                exportable: false,
            })
            .await
            .unwrap_err()
            .code(),
            "PERMISSION_DENIED"
        );

        // PKI 内部路径可管理该前缀；公开写面一律拒绝。
        let archive = reg
            .create_key_internal(create("pki/leaf/test", KeyType::EcdsaP256))
            .await
            .unwrap();
        assert_eq!(archive.name, "pki/leaf/test");
        for code in [
            reg.rotate_key("pki/leaf/test").await.unwrap_err().code(),
            reg.update_key_config("pki/leaf/test", KeyConfigPatch::default())
                .await
                .unwrap_err()
                .code(),
            reg.trim_key("pki/leaf/test", 1).await.unwrap_err().code(),
            reg.delete_key("pki/leaf/test").await.unwrap_err().code(),
        ] {
            assert_eq!(code, "PERMISSION_DENIED");
        }

        assert_eq!(
            reg.rotate_key_internal("pki/leaf/test")
                .await
                .unwrap()
                .latest_version,
            2
        );
        // 读/导出仍可用（leaf 的受控恢复路径）。
        assert_eq!(
            reg.read_key("pki/leaf/test").await.unwrap().latest_version,
            2
        );
    }

    #[tokio::test]
    async fn encrypt_decrypt_roundtrip_and_key_name_binding() {
        let reg = registry().await;
        reg.create_key(create("a", KeyType::Aes256Gcm96))
            .await
            .unwrap();
        reg.create_key(create("b", KeyType::Aes256Gcm96))
            .await
            .unwrap();

        let (ct, version) = reg.encrypt("a", 0, b"hello", b"ctx").await.unwrap();
        assert_eq!(version, 1);
        let (pt, v) = reg.decrypt(None, &ct, b"ctx").await.unwrap();
        assert_eq!(pt, b"hello");
        assert_eq!(v, 1);

        // 错误 context ⇒ 认证失败
        assert!(reg.decrypt(None, &ct, b"other").await.is_err());
        // 请求名与内嵌名不一致 ⇒ INVALID_ARGUMENT
        let err = reg.decrypt(Some("b"), &ct, b"ctx").await.unwrap_err();
        assert_eq!(err.code(), "INVALID_ARGUMENT");
    }

    #[tokio::test]
    async fn rotate_bounds_and_rewrap() {
        let reg = registry().await;
        reg.create_key(create("k", KeyType::Aes256Gcm96))
            .await
            .unwrap();
        let (v1_ct, v1) = reg.encrypt("k", 0, b"payload", b"").await.unwrap();
        assert_eq!(v1, 1);

        reg.rotate_key("k").await.unwrap();
        let (v2_ct, v2) = reg.rewrap(None, &v1_ct, b"").await.unwrap();
        assert_eq!(v2, 2);
        assert_ne!(v1_ct, v2_ct);
        let (pt, version) = reg.decrypt(None, &v2_ct, b"").await.unwrap();
        assert_eq!(pt, b"payload");
        assert_eq!(version, 2);

        // 旧密文仍在 min_decryption 窗口内可解
        let (pt, _) = reg.decrypt(None, &v1_ct, b"").await.unwrap();
        assert_eq!(pt, b"payload");

        // trim 后旧版本不可解
        reg.trim_key("k", 2).await.unwrap();
        let err = reg.decrypt(None, &v1_ct, b"").await.unwrap_err();
        assert_eq!(err.code(), "FAILED_PRECONDITION");
    }

    #[tokio::test]
    async fn sign_verify_ed25519_and_reject_prehashed() {
        let reg = registry().await;
        reg.create_key(create("ed", KeyType::Ed25519))
            .await
            .unwrap();
        let out = reg.sign(sign_spec("ed", b"message")).await.unwrap();
        assert_eq!(out.algorithm, "ed25519");
        assert_eq!(out.key_version, 1);
        let verified = reg
            .verify(verify_spec("ed", b"message", &out.signature))
            .await
            .unwrap();
        assert!(verified.valid);
        let tampered = reg
            .verify(verify_spec("ed", b"other", &out.signature))
            .await
            .unwrap();
        assert!(!tampered.valid);

        let err = reg
            .sign(SignSpec {
                prehashed: true,
                hash_algorithm: Some("SHA256".into()),
                ..sign_spec("ed", b"x")
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), "INVALID_ARGUMENT");
    }

    #[tokio::test]
    async fn prehashed_equivalence_ecdsa() {
        let reg = registry().await;
        reg.create_key(create("ec", KeyType::EcdsaP256))
            .await
            .unwrap();
        let out_full = reg.sign(sign_spec("ec", b"message")).await.unwrap();
        let digest = Sha256::digest(b"message").to_vec();
        let out_pre = reg
            .sign(SignSpec {
                prehashed: true,
                hash_algorithm: Some("SHA256".into()),
                ..sign_spec("ec", &digest)
            })
            .await
            .unwrap();
        // ECDSA 用确定性 nonce（RFC6979）⇒ 同一 (key, digest) 逐字节相等
        assert_eq!(out_full.signature, out_pre.signature);

        let verified = reg
            .verify(VerifySpec {
                prehashed: true,
                hash_algorithm: Some("SHA256".into()),
                ..verify_spec("ec", &digest, &out_full.signature)
            })
            .await
            .unwrap();
        assert!(verified.valid);
    }

    #[tokio::test]
    async fn rsa_default_is_pss_and_unknown_algorithm_rejected() {
        let reg = registry().await;
        reg.create_key(create("rsa", KeyType::Rsa2048))
            .await
            .unwrap();
        let out = reg.sign(sign_spec("rsa", b"message")).await.unwrap();
        assert_eq!(out.algorithm, "rsa-pss-sha256");
        let verified = reg
            .verify(verify_spec("rsa", b"message", &out.signature))
            .await
            .unwrap();
        assert!(verified.valid);

        let err = reg
            .sign(SignSpec {
                signature_algorithm: Some("bogus".into()),
                ..sign_spec("rsa", b"x")
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), "INVALID_ARGUMENT");

        // PSS 是随机化签名：同一输入两次签名不同，但都能验过。
        let out2 = reg.sign(sign_spec("rsa", b"message")).await.unwrap();
        assert_ne!(out.signature, out2.signature);
        assert!(
            reg.verify(verify_spec("rsa", b"message", &out2.signature))
                .await
                .unwrap()
                .valid
        );
    }

    #[tokio::test]
    async fn missing_hash_for_prehashed_is_invalid_argument() {
        let reg = registry().await;
        reg.create_key(create("rsa", KeyType::Rsa2048))
            .await
            .unwrap();
        let err = reg
            .sign(SignSpec {
                prehashed: true,
                ..sign_spec("rsa", b"x")
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), "INVALID_ARGUMENT");
    }

    #[tokio::test]
    async fn prehashed_input_must_match_hash_length() {
        let reg = registry().await;
        reg.create_key(create("ec", KeyType::EcdsaP256))
            .await
            .unwrap();

        let err = reg
            .sign(SignSpec {
                prehashed: true,
                hash_algorithm: Some("SHA256".into()),
                ..sign_spec("ec", b"not-a-32-byte-digest")
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), "INVALID_ARGUMENT");

        let digest = Sha256::digest(b"message").to_vec();
        let out = reg
            .sign(SignSpec {
                prehashed: true,
                hash_algorithm: Some("SHA256".into()),
                ..sign_spec("ec", &digest)
            })
            .await
            .unwrap();
        let err = reg
            .verify(VerifySpec {
                prehashed: true,
                hash_algorithm: Some("SHA256".into()),
                ..verify_spec("ec", b"short", &out.signature)
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), "INVALID_ARGUMENT");
    }

    #[tokio::test]
    async fn delete_requires_deletion_allowed() {
        let reg = registry().await;
        reg.create_key(create("k", KeyType::Aes256Gcm96))
            .await
            .unwrap();
        let err = reg.delete_key("k").await.unwrap_err();
        assert_eq!(err.code(), "FAILED_PRECONDITION");

        reg.update_key_config(
            "k",
            KeyConfigPatch {
                deletion_allowed: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        reg.delete_key("k").await.unwrap();
        assert_eq!(reg.read_key("k").await.unwrap_err().code(), "NOT_FOUND");
    }

    #[tokio::test]
    async fn export_gate_and_never_exportable() {
        let reg = registry().await;
        reg.create_key(CreateKeySpec {
            name: "e".into(),
            key_type: KeyType::Aes256Gcm96,
            exportable: true,
        })
        .await
        .unwrap();
        let exported = reg.export_key("e", 0).await.unwrap();
        assert_eq!(exported.material.len(), SYMMETRIC_KEY_LEN);
        assert!(!exported.contains_private_key);

        reg.create_key(create("n", KeyType::Aes256Gcm96))
            .await
            .unwrap();
        assert_eq!(
            reg.export_key("n", 0).await.unwrap_err().code(),
            "FAILED_PRECONDITION"
        );

        reg.import_key_with_flags(
            ImportKeySpec {
                name: "ca".into(),
                key_type: KeyType::EcdsaP256,
                material: generate_material(KeyType::EcdsaP256)
                    .await
                    .unwrap()
                    .to_vec(),
                format: Some(MaterialFormat::Der),
                exportable: true,
            },
            true,
        )
        .await
        .unwrap();
        assert_eq!(
            reg.export_key("ca", 0).await.unwrap_err().code(),
            "PERMISSION_DENIED"
        );
    }

    #[tokio::test]
    async fn generated_datakey_wrapped_part_decrypts() {
        let reg = registry().await;
        reg.create_key(create("k", KeyType::Aes256Gcm96))
            .await
            .unwrap();
        let generated = reg
            .generate_data_key("k", 0, b"ctx", DataKeyKind::PlaintextAndWrapped)
            .await
            .unwrap();
        let plaintext = generated.plaintext.unwrap();
        assert_eq!(plaintext.len(), SYMMETRIC_KEY_LEN);
        let (decrypted, version) = reg
            .decrypt(None, generated.ciphertext.as_ref().unwrap(), b"ctx")
            .await
            .unwrap();
        assert_eq!(decrypted, plaintext);
        assert_eq!(version, generated.key_version);
    }

    #[tokio::test]
    async fn concurrent_rotates_do_not_lose_updates() {
        let reg = Arc::new(registry().await);
        reg.create_key(create("k", KeyType::Aes256Gcm96))
            .await
            .unwrap();
        let mut handles = Vec::new();
        for _ in 0..4 {
            let reg = Arc::clone(&reg);
            handles.push(tokio::spawn(async move { reg.rotate_key("k").await }));
        }
        for handle in handles {
            handle.await.unwrap().unwrap();
        }
        let archive = reg.read_key("k").await.unwrap();
        assert_eq!(archive.latest_version, 5);
        assert_eq!(archive.versions.len(), 5);
    }
}
