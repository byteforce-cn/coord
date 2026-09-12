// Token Signing Key — HKDF-derived signing key for CCT tokens
//
// Derives a signing key from the Root Key via HKDF-SHA256 with a distinct
// info string. Supports key versioning and rotation:
// - Active key used for signing new CCTs
// - Previous keys retained for verification (2x Max TTL = 2 hours)
// - Rotation period: 7 days

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ed25519_dalek::SigningKey;
use hkdf::Hkdf;
use parking_lot::RwLock;
use sha2::Sha256;
use zeroize::Zeroizing;

use coord_core::auth::cct::{decode_cct_any, CctToken};
use coord_core::error::{Error, Result};

/// HMAC CCT 验证宽限期的**截止时刻**（Unix 秒，UTC 2026-12-31T00:00:00Z）。
///
/// 第三轮 §3.6：HMAC 分支此前"无 deadline、无开关、无告警"——注释里写着
/// "宽限期结束后应删除"，但没有任何机制让这件事发生或被发现。"临时兼容"
/// 不设终点就会变成**永久攻击面**（对称密钥 = 任一 agent 可自签 root）。
pub const HMAC_GRACE_DEADLINE_UNIX: u64 = 1_798_675_200;

/// 判断当前是否已过 HMAC 宽限期。时钟异常（早于 UNIX_EPOCH）按"未过期"处理
/// —— 宁可少报，也不要因时钟问题误报安全告警。
pub fn hmac_grace_expired() -> bool {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() >= HMAC_GRACE_DEADLINE_UNIX)
        .unwrap_or(false)
}

/// 进程内只告警一次（避免每次验签刷屏，同时保证**一定**会在日志里出现一次）。
fn warn_if_hmac_grace_expired() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    if !hmac_grace_expired() {
        return;
    }
    ONCE.call_once(|| {
        tracing::error!(
            deadline_unix = HMAC_GRACE_DEADLINE_UNIX,
            "HMAC CCT verification is PAST its grace deadline (2026-12-31): delete the HMAC \
             branch in TokenSigningKeyring::decode_any — a symmetric key means any agent \
             holding it can self-sign roles:[\"root\"]"
        );
    });
}

// ──── Constants ────

/// Token signing key length (256-bit for HMAC-SHA256)
pub const SIGNING_KEY_LEN: usize = 32;

/// HKDF info string for token signing key derivation
/// 历史 v1 派生 info（仅兼容性测试引用；生产用 `format!("coord-token-signing-v{version}")`）
#[cfg(test)]
const TOKEN_SIGNING_INFO: &[u8] = b"coord-token-signing-v1";

/// Default key rotation period (7 days in seconds)
const DEFAULT_ROTATION_PERIOD_SECS: u64 = 7 * 24 * 60 * 60;

/// How long to retain old keys for verification after rotation (2 hours = 2x Max TTL)
const KEY_RETENTION_SECS: u64 = 2 * 60 * 60;

/// Maximum number of previous keys to retain
const MAX_PREVIOUS_KEYS: usize = 8;

// ──── Token Signing Key ────

/// A versioned token signing key used to sign or verify CCT tokens.
#[derive(Clone)]
pub struct TokenSigningKey {
    /// Monotonically increasing key version
    pub version: u32,
    /// Key bytes (256-bit, zeroized on drop)
    pub key_bytes: Zeroizing<Vec<u8>>,
    /// When this key was created (Unix seconds)
    pub created_at: i64,
    /// Human-readable key ID (e.g., "token-signing-key-v1")
    pub key_id: String,
}

impl TokenSigningKey {
    /// Create a new signing key with the given version.
    pub fn new(version: u32, key_bytes: Vec<u8>, created_at: i64) -> Self {
        let key_id = format!("token-signing-key-v{version}");
        Self {
            version,
            key_bytes: Zeroizing::new(key_bytes),
            created_at,
            key_id,
        }
    }

    /// Derive a signing key from root key material via HKDF-SHA256.
    ///
    /// 修复：info 串混入 key version（`coord-token-signing-v{version}`），
    /// 不同版本派生不同密钥 —— 此前常量 info 导致轮换无效（所有版本同钥）。
    pub fn derive(version: u32, root_key_material: &[u8]) -> Result<Self> {
        let hkdf = Hkdf::<Sha256>::new(None, root_key_material);
        let mut key_bytes = vec![0u8; SIGNING_KEY_LEN];
        let info = format!("coord-token-signing-v{version}");
        hkdf.expand(info.as_bytes(), &mut key_bytes)
            .map_err(|e| Error::Crypto(format!("HKDF expand for token signing key failed: {e}")))?;

        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        Ok(Self::new(version, key_bytes, created_at))
    }

    /// Sign data using HMAC-SHA256.
    pub fn sign(&self, data: &[u8]) -> Result<Vec<u8>> {
        use hmac::{Hmac, Mac};
        type HmacSha256 = Hmac<Sha256>;

        let mut mac = HmacSha256::new_from_slice(&self.key_bytes)
            .map_err(|e| Error::Crypto(format!("HMAC key invalid: {e}")))?;
        mac.update(data);
        Ok(mac.finalize().into_bytes().to_vec())
    }

    /// Verify a signature using HMAC-SHA256.
    pub fn verify(&self, data: &[u8], signature: &[u8]) -> Result<()> {
        use hmac::{Hmac, Mac};
        type HmacSha256 = Hmac<Sha256>;

        let mut mac = HmacSha256::new_from_slice(&self.key_bytes)
            .map_err(|e| Error::Crypto(format!("HMAC key invalid: {e}")))?;
        mac.update(data);
        mac.verify_slice(signature)
            .map_err(|_| Error::InvalidToken("signature verification failed".to_string()))
    }
}

// ──── Token Signing Keyring ────

/// Manages token signing keys with versioning and rotation.
///
/// Holds the active key (for signing) and previous keys (for verification).
/// Rotation generates a new active key and moves the old one to the previous list.
pub struct TokenSigningKeyring {
    /// Active key for signing new tokens
    active: RwLock<TokenSigningKey>,
    /// Previous keys retained for verification
    previous: RwLock<Vec<TokenSigningKey>>,
    /// Root key material for deriving new keys
    root_key_material: Zeroizing<Vec<u8>>,
    /// Current key version (monotonically increasing)
    next_version: RwLock<u32>,
    /// Last rotation timestamp
    last_rotation: RwLock<Instant>,
    /// Rotation period
    rotation_period: Duration,
}

impl TokenSigningKeyring {
    /// Create a new keyring, deriving the initial signing key from root key material.
    pub fn new(root_key_material: Vec<u8>) -> Result<Self> {
        let v1_key = TokenSigningKey::derive(1, &root_key_material)?;

        Ok(Self {
            active: RwLock::new(v1_key),
            previous: RwLock::new(Vec::new()),
            root_key_material: Zeroizing::new(root_key_material),
            next_version: RwLock::new(2),
            last_rotation: RwLock::new(Instant::now()),
            rotation_period: Duration::from_secs(DEFAULT_ROTATION_PERIOD_SECS),
        })
    }

    /// Create a new keyring with a custom rotation period.
    pub fn with_rotation_period(
        root_key_material: Vec<u8>,
        rotation_period_secs: u64,
    ) -> Result<Self> {
        let mut keyring = Self::new(root_key_material)?;
        keyring.rotation_period = Duration::from_secs(rotation_period_secs);
        Ok(keyring)
    }

    /// Get the active signing key (for signing new tokens).
    pub fn active_key(&self) -> TokenSigningKey {
        self.active.read().clone()
    }

    /// Get the active signing key bytes.
    pub fn active_key_bytes(&self) -> Zeroizing<Vec<u8>> {
        self.active.read().key_bytes.clone()
    }

    /// 从 root key 派生 Ed25519 签名私钥（HKDF info `coord-cct-ed25519-v1`）。
    ///
    /// server 持有私钥签发，agent 仅持 `verifying_key()` 公钥验证，
    /// 任一 agent 被控无法伪造 token（与 HMAC 对称方案的根因修复）。
    pub fn ed25519_signing_key(&self) -> Result<SigningKey> {
        let hkdf = Hkdf::<Sha256>::new(None, &self.root_key_material);
        let mut seed = [0u8; 32];
        hkdf.expand(b"coord-cct-ed25519-v1", &mut seed)
            .map_err(|e| Error::Crypto(format!("HKDF expand for Ed25519 CCT key failed: {e}")))?;
        Ok(SigningKey::from_bytes(&seed))
    }

    /// 验证任意算法签发的 CCT。
    ///
    /// - `Ed25519` → 用派生的签名密钥对应公钥验证（**当前唯一签发算法**）；
    /// - `HMAC-SHA256` → 依次尝试 active + previous 密钥。
    ///
    /// # ⚠️ 已弃用分支（A3 标注 / 第三轮 §3.6 加期限）
    ///
    /// HMAC 是对称方案：**任何持有密钥的 agent 都能自签任意 CCT**（含
    /// `roles:["root"]`）。因此 HMAC 仅作**存量 token 宽限期验证**保留，新签发
    /// 一律 Ed25519（见 [`Self::ed25519_signing_key`]）。
    ///
    /// 宽限期**有明确终点**：[`HMAC_GRACE_DEADLINE_UNIX`]。到期后每次进入本函数
    /// 都会以 ERROR 级别告警（进程内只报一次，避免刷屏），提示把 HMAC 分支删掉。
    /// 之所以不自动失效：按墙钟日期静默改变认证行为本身就是一个可用性风险；
    /// 删除该分支应当是一次**显式、可测试**的代码变更。
    pub fn decode_any(&self, token: &str) -> Result<CctToken> {
        warn_if_hmac_grace_expired();

        let pub_key = self
            .ed25519_signing_key()
            .ok()
            .map(|sk| sk.verifying_key().to_bytes().to_vec());

        let mut hmac_keys: Vec<Vec<u8>> = Vec::new();
        hmac_keys.push(self.active_key_bytes().to_vec());
        for key_id in self.all_key_ids() {
            if let Some(key) = self.find_key(&key_id) {
                hmac_keys.push(key.key_bytes.to_vec());
            }
        }
        let key_refs: Vec<&[u8]> = hmac_keys.iter().map(|k| k.as_slice()).collect();
        decode_cct_any(token, &key_refs, pub_key.as_deref())
    }

    /// Find a key by key_id for verification.
    ///
    /// Searches the active key first, then previous keys.
    pub fn find_key(&self, key_id: &str) -> Option<TokenSigningKey> {
        // Check active key
        {
            let active = self.active.read();
            if active.key_id == key_id {
                return Some(active.clone());
            }
        }

        // Check previous keys
        let previous = self.previous.read();
        for key in previous.iter().rev() {
            if key.key_id == key_id {
                return Some(key.clone());
            }
        }

        None
    }

    /// Find a key by version number.
    pub fn find_key_by_version(&self, version: u32) -> Option<TokenSigningKey> {
        {
            let active = self.active.read();
            if active.version == version {
                return Some(active.clone());
            }
        }

        let previous = self.previous.read();
        for key in previous.iter() {
            if key.version == version {
                return Some(key.clone());
            }
        }

        None
    }

    /// Check if key rotation is due and rotate if needed.
    ///
    /// Returns `true` if rotation occurred, `false` otherwise.
    pub fn maybe_rotate(&self) -> Result<bool> {
        let now = Instant::now();
        let last = *self.last_rotation.read();

        if now.duration_since(last) < self.rotation_period {
            return Ok(false);
        }

        self.rotate()
    }

    /// Force a key rotation regardless of the rotation period.
    ///
    /// Generates a new signing key, moves the current active to the previous list,
    /// and evicts expired previous keys.
    pub fn rotate(&self) -> Result<bool> {
        let mut last = self.last_rotation.write();
        let mut next_ver = self.next_version.write();
        let version = *next_ver;

        // Derive new key
        let new_key = TokenSigningKey::derive(version, &self.root_key_material)?;

        // Move current active to previous
        let old_active = {
            let mut active = self.active.write();
            std::mem::replace(&mut *active, new_key)
        };

        // Add old active to previous keys
        {
            let mut previous = self.previous.write();
            previous.push(old_active);
        }

        // Evict expired previous keys
        self.evict_expired_keys();

        // Update state
        *next_ver = version + 1;
        *last = Instant::now();

        tracing::info!(
            "Token signing key rotated to version {} (key_id: {})",
            version,
            format!("token-signing-key-v{version}")
        );

        Ok(true)
    }

    /// Evict previous keys older than KEY_RETENTION_SECS.
    fn evict_expired_keys(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let cutoff = now - KEY_RETENTION_SECS as i64;

        let mut previous = self.previous.write();
        previous.retain(|key| key.created_at > cutoff);

        // Enforce max previous keys limit
        while previous.len() > MAX_PREVIOUS_KEYS {
            previous.remove(0);
        }

        if !previous.is_empty() {
            tracing::debug!(
                "Retained {} previous token signing keys for verification",
                previous.len()
            );
        }
    }

    /// Number of previous keys currently retained.
    pub fn previous_key_count(&self) -> usize {
        self.previous.read().len()
    }

    /// Current active key version.
    pub fn active_version(&self) -> u32 {
        self.active.read().version
    }

    /// Get all key IDs (active + previous) for the "kid" header.
    pub fn all_key_ids(&self) -> Vec<String> {
        let mut ids = vec![self.active.read().key_id.clone()];
        for key in self.previous.read().iter() {
            ids.push(key.key_id.clone());
        }
        ids
    }
}

// ──── Tests ────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_root_material() -> Vec<u8> {
        // 256-bit test root key material
        let mut key = vec![0u8; 32];
        key[0] = 0x42;
        key[31] = 0xFF;
        key
    }

    // ──── Token Signing Key derivation ────

    #[test]
    fn test_derive_signing_key_from_root() {
        let root = make_test_root_material();
        let key = TokenSigningKey::derive(1, &root).expect("derivation should succeed");

        assert_eq!(key.version, 1);
        assert_eq!(key.key_bytes.len(), SIGNING_KEY_LEN);
        assert_eq!(key.key_id, "token-signing-key-v1");
        assert!(key.created_at > 0);
    }

    #[test]
    fn test_derive_different_keys_for_different_versions() {
        let root = make_test_root_material();
        let key1 = TokenSigningKey::derive(1, &root).unwrap();
        let key2 = TokenSigningKey::derive(2, &root).unwrap();

        // 修复：不同版本必须派生不同密钥（轮换才有效）
        assert_eq!(key1.key_id, "token-signing-key-v1");
        assert_eq!(key2.key_id, "token-signing-key-v2");
        assert_ne!(
            &*key1.key_bytes, &*key2.key_bytes,
            "different versions must derive different key bytes (HKDF info mixes version)"
        );

        // v1 派生保持历史兼容（info = "coord-token-signing-v1"，与修复前一致）
        use hkdf::Hkdf;
        let hkdf_legacy = Hkdf::<sha2::Sha256>::new(None, &root);
        let mut legacy = vec![0u8; SIGNING_KEY_LEN];
        hkdf_legacy.expand(TOKEN_SIGNING_INFO, &mut legacy).unwrap();
        assert_eq!(&*key1.key_bytes, legacy.as_slice());
    }

    #[test]
    fn test_sign_and_verify_roundtrip() {
        let root = make_test_root_material();
        let key = TokenSigningKey::derive(1, &root).unwrap();

        let data = b"test signing data";
        let signature = key.sign(data).expect("sign should succeed");

        // Verify with same key
        key.verify(data, &signature).expect("verify should succeed");
    }

    #[test]
    fn test_verify_tampered_signature_fails() {
        let root = make_test_root_material();
        let key = TokenSigningKey::derive(1, &root).unwrap();

        let data = b"test signing data";
        let mut signature = key.sign(data).expect("sign should succeed");

        // Tamper with signature
        if !signature.is_empty() {
            signature[0] ^= 0xFF;
        }

        let result = key.verify(data, &signature);
        assert!(
            result.is_err(),
            "tampered signature should fail verification"
        );
    }

    #[test]
    fn test_verify_different_data_fails() {
        let root = make_test_root_material();
        let key = TokenSigningKey::derive(1, &root).unwrap();

        let data = b"original data";
        let signature = key.sign(data).unwrap();

        let result = key.verify(b"different data", &signature);
        assert!(result.is_err(), "different data should fail verification");
    }

    #[test]
    fn test_verify_different_key_fails() {
        let root1 = make_test_root_material();
        let mut root2 = root1.clone();
        root2[0] ^= 0x01; // Slightly different root

        let key1 = TokenSigningKey::derive(1, &root1).unwrap();
        let key2 = TokenSigningKey::derive(1, &root2).unwrap();

        let data = b"test data";
        let signature = key1.sign(data).unwrap();

        let result = key2.verify(data, &signature);
        assert!(result.is_err(), "different key should fail verification");
    }

    // ──── Keyring tests ────

    #[test]
    fn test_keyring_initialization() {
        let root = make_test_root_material();
        let keyring = TokenSigningKeyring::new(root).expect("keyring init should succeed");

        assert_eq!(keyring.active_version(), 1);
        assert_eq!(keyring.previous_key_count(), 0);

        let active = keyring.active_key();
        assert_eq!(active.key_id, "token-signing-key-v1");
    }

    #[test]
    fn test_keyring_find_key_by_id() {
        let root = make_test_root_material();
        let keyring = TokenSigningKeyring::new(root).unwrap();

        let found = keyring.find_key("token-signing-key-v1");
        assert!(found.is_some());
        assert_eq!(found.unwrap().version, 1);

        let not_found = keyring.find_key("token-signing-key-v99");
        assert!(not_found.is_none());
    }

    #[test]
    fn test_keyring_rotate_creates_new_active_and_retains_old() {
        let root = make_test_root_material();
        let keyring = TokenSigningKeyring::with_rotation_period(root, 0).unwrap(); // 0 = immediate rotation allowed

        let v1 = keyring.active_version();
        assert_eq!(v1, 1);

        let rotated = keyring.rotate().expect("rotation should succeed");
        assert!(rotated);

        let v2 = keyring.active_version();
        assert_eq!(v2, 2);

        // Previous key should be retained
        assert_eq!(keyring.previous_key_count(), 1);

        // Should be able to find both keys
        assert!(keyring.find_key("token-signing-key-v1").is_some());
        assert!(keyring.find_key("token-signing-key-v2").is_some());
    }

    #[test]
    fn test_keyring_maybe_rotate_respects_period() {
        let root = make_test_root_material();
        // Long rotation period — won't auto-rotate
        let keyring = TokenSigningKeyring::with_rotation_period(root, 86400 * 365).unwrap();

        let result = keyring.maybe_rotate().expect("maybe_rotate should succeed");
        assert!(!result, "should not rotate within long period");
        assert_eq!(keyring.active_version(), 1);
    }

    #[test]
    fn test_sign_with_active_key_and_verify_with_previous() {
        let root = make_test_root_material();
        let keyring = TokenSigningKeyring::new(root).unwrap();

        // Sign with v1 key
        let data = b"test data for key rotation";
        let v1_key = keyring.active_key();
        let signature = v1_key.sign(data).unwrap();

        // Rotate to v2
        keyring.rotate().unwrap();

        // Verify v1 signature with retained v1 key
        let v1_retained = keyring.find_key("token-signing-key-v1").unwrap();
        v1_retained
            .verify(data, &signature)
            .expect("v1 key should still verify");

        // Sign with v2 and verify
        let v2_key = keyring.active_key();
        let sig_v2 = v2_key.sign(data).unwrap();
        v2_key.verify(data, &sig_v2).expect("v2 key should verify");
    }

    #[test]
    fn test_keyring_all_key_ids() {
        let root = make_test_root_material();
        let keyring = TokenSigningKeyring::new(root).unwrap();

        let ids = keyring.all_key_ids();
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], "token-signing-key-v1");

        keyring.rotate().unwrap();
        let ids = keyring.all_key_ids();
        assert_eq!(ids.len(), 2);
    }
}
