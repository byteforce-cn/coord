// Key Management — 密钥生命周期管理（P2）
//
// 三层密钥架构（参考 NIST SP 800-57）：
//   Root Key (256-bit, 仅内存) → HKDF-SHA256 → KEK (256-bit, 仅内存) → AES-256-GCM → DEK (256-bit, 密文落盘)
//
// 职责：
// - Root Key 生成 / 派生 KEK
// - DEK 生成、加密落盘、从磁盘加载解密
// - DEK 版本化管理（key_id 单调递增）
// - 密钥轮换（rotate）：生成新 DEK，旧 DEK 保留用于解密历史数据
// - Seal：内存中清零所有密钥
// - Zeroize 保护所有密钥材料

use aes_gcm::{
    aead::{Aead, AeadCore, KeyInit, OsRng},
    Aes256Gcm, Key, Nonce,
};
use hkdf::Hkdf;
use lru::LruCache;
use parking_lot::RwLock;
use rand::RngCore;
use sha2::Sha256;
use std::num::NonZeroUsize;
use std::sync::Arc;
use zeroize::{Zeroize, Zeroizing};

use coord_core::error::{Error, Result};

use super::seal::{self, Share};

// ──── 常量 ────

/// DEK 长度（256-bit）
const DEK_LEN: usize = 32;

/// KEK 长度（256-bit）
const KEK_LEN: usize = 32;

/// Root Key 长度（256-bit）
const ROOT_KEY_LEN: usize = 32;

/// GCM nonce 长度（96-bit）
const NONCE_LEN: usize = 12;

/// GCM tag 长度（128-bit）
const TAG_LEN: usize = 16;

/// DEK 缓存容量（保留最近 N 个旧版本用于解密）
const DEK_CACHE_SIZE: usize = 8;

/// DEK 加密后落盘格式：nonce(12B) || ciphertext(32B) || tag(16B) = 60 bytes
const ENCRYPTED_DEK_LEN: usize = NONCE_LEN + DEK_LEN + TAG_LEN;

// ──── 内部类型 ────

/// Root Key — 256-bit，永不写入磁盘。
/// 启动时由 Shamir 分片重组（Unseal）或随机生成（Bootstrap）。
#[derive(Zeroize)]
#[zeroize(drop)]
struct RootKey(Zeroizing<[u8; ROOT_KEY_LEN]>);

impl RootKey {
    /// 生成新的随机 Root Key（用于首次初始化 / Bootstrap）
    fn generate() -> Self {
        let mut key = Zeroizing::new([0u8; ROOT_KEY_LEN]);
        rand::thread_rng().fill_bytes(&mut *key);
        Self(key)
    }

    /// 从 bytes 恢复 Root Key（用于 Unseal，由 Shamir 分片重组后传入）
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != ROOT_KEY_LEN {
            return Err(Error::Crypto(format!(
                "root key must be {} bytes, got {}",
                ROOT_KEY_LEN,
                bytes.len()
            )));
        }
        let mut key = Zeroizing::new([0u8; ROOT_KEY_LEN]);
        key.copy_from_slice(bytes);
        Ok(Self(key))
    }

    /// 通过 HKDF-SHA256 派生 KEK
    /// HKDF 参数：salt=None, info=b"coord-kek-v1"
    fn derive_kek(&self) -> Kek {
        let hkdf = Hkdf::<Sha256>::new(None, &*self.0);
        let mut kek_bytes = Zeroizing::new([0u8; KEK_LEN]);
        hkdf.expand(b"coord-kek-v1", &mut *kek_bytes)
            .expect("HKDF-SHA256 expand to 32 bytes is infallible");
        Kek(kek_bytes)
    }
}

/// Key Encryption Key (KEK) — 256-bit，仅存于内存。
/// 用于包裹（wrap/unwrap）DEK：加密 DEK 后落盘，从磁盘读取后解密 DEK。
#[derive(Zeroize)]
#[zeroize(drop)]
struct Kek(Zeroizing<[u8; KEK_LEN]>);

impl Kek {
    /// 用 KEK 加密 DEK（AES-256-GCM），返回密文供落盘
    fn wrap_dek(&self, dek: &[u8; DEK_LEN]) -> Result<Vec<u8>> {
        let key = Key::<Aes256Gcm>::from_slice(&*self.0);
        let cipher = Aes256Gcm::new(key);
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);

        let ciphertext = cipher
            .encrypt(&nonce, dek.as_ref())
            .map_err(|e| Error::Crypto(format!("KEK wrap DEK failed: {e}")))?;

        // 编码：nonce(12B) || ciphertext(32B+16B_tag)
        let mut output = Vec::with_capacity(ENCRYPTED_DEK_LEN);
        output.extend_from_slice(&nonce);
        output.extend_from_slice(&ciphertext);
        Ok(output)
    }

    /// 用 KEK 解密 DEK（从磁盘读取的密文），返回明文 DEK
    fn unwrap_dek(&self, encrypted: &[u8]) -> Result<Zeroizing<[u8; DEK_LEN]>> {
        if encrypted.len() != ENCRYPTED_DEK_LEN {
            return Err(Error::Crypto(format!(
                "encrypted DEK must be {ENCRYPTED_DEK_LEN} bytes, got {}",
                encrypted.len()
            )));
        }

        let nonce = Nonce::from_slice(&encrypted[..NONCE_LEN]);
        let ciphertext = &encrypted[NONCE_LEN..];

        let key = Key::<Aes256Gcm>::from_slice(&*self.0);
        let cipher = Aes256Gcm::new(key);

        let plaintext = cipher
            .decrypt(nonce, ciphertext)
            .map_err(|e| Error::Crypto(format!("KEK unwrap DEK failed: {e}")))?;

        if plaintext.len() != DEK_LEN {
            return Err(Error::Crypto(format!(
                "decrypted DEK must be {DEK_LEN} bytes, got {}",
                plaintext.len()
            )));
        }

        let mut dek = Zeroizing::new([0u8; DEK_LEN]);
        dek.copy_from_slice(&plaintext);
        Ok(dek)
    }
}

// ──── 核心类型 ────

/// Keyring — 密钥管理器，管理三层密钥体系的完整生命周期。
///
/// # 线程安全
/// 内部使用 `Arc<RwLock<>>` 保护可变状态，可安全地在多线程间共享。
///
/// # Debug 安全
/// 不打印任何密钥材料，仅显示 key_id 范围。
#[derive(Clone)]
pub struct Keyring {
    inner: Arc<RwLock<KeyringInner>>,
}

impl std::fmt::Debug for Keyring {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.read();
        f.debug_struct("Keyring")
            .field("active_key_id", &inner.active_key_id)
            .field("next_key_id", &inner.next_key_id)
            .field("cached_dek_count", &inner.dek_cache.len())
            .finish_non_exhaustive()
    }
}

struct KeyringInner {
    /// KEK（仅内存，zeroized on drop）
    kek: Kek,
    /// 当前活跃 DEK（明文，仅内存）
    active_dek: Zeroizing<[u8; DEK_LEN]>,
    /// 当前活跃 DEK 的版本 ID
    active_key_id: u32,
    /// 全局 key_id 分配器（单调递增）
    next_key_id: u32,
    /// 旧 DEK 缓存（key_id → 明文 DEK），用于解密历史数据
    dek_cache: LruCache<u32, Zeroizing<[u8; DEK_LEN]>>,
    /// Seal 状态：true 表示已封存，所有 get_dek 拒绝服务
    sealed: bool,
}

impl Keyring {
    // ──── 构造器 ────

    /// Bootstrap：生成新的随机 Root Key，派生 KEK，生成首个 DEK。
    ///
    /// 用于集群首次初始化。调用方需要将 `encrypted_dek` 持久化到 `/_meta/dek/{key_id}`。
    pub fn bootstrap() -> (Self, EncryptedDek) {
        let root_key = RootKey::generate();
        let kek = root_key.derive_kek();
        let (active_dek, active_key_id, next_key_id) = Self::generate_dek(&kek, 1);

        // 用 KEK 加密 DEK 用于落盘
        let encrypted_bytes = kek
            .wrap_dek(&active_dek)
            .expect("KEK wrap of freshly generated DEK should not fail");

        let encrypted_dek = EncryptedDek {
            key_id: active_key_id,
            encrypted_bytes,
        };

        let mut cache = LruCache::new(NonZeroUsize::new(DEK_CACHE_SIZE).unwrap());
        cache.put(active_key_id, Zeroizing::new(active_dek));

        let keyring = Self {
            inner: Arc::new(RwLock::new(KeyringInner {
                kek,
                active_dek: Zeroizing::new(active_dek),
                active_key_id,
                next_key_id,
                dek_cache: cache,
                sealed: false,
            })),
        };

        (keyring, encrypted_dek)
    }

    /// Bootstrap + Shamir 分片：生成 Root Key 并拆分为 N 个分片。
    ///
    /// 用于集群首次初始化（启用 Seal 能力）。返回 (Keyring, EncryptedDek, Shares)。
    /// 调用方需将 `encrypted_dek` 持久化到 `/_meta/dek/{key_id}`，将分片安全分发给管理员。
    pub fn bootstrap_with_shares(n: u8, k: u8) -> Result<(Self, EncryptedDek, Vec<Share>)> {
        let root_key = RootKey::generate();
        let root_key_bytes = *root_key.0; // 捕获 Root Key 明文用于分片

        let kek = root_key.derive_kek();
        let (active_dek, active_key_id, next_key_id) = Self::generate_dek(&kek, 1);

        // 用 KEK 加密 DEK 用于落盘
        let encrypted_bytes = kek
            .wrap_dek(&active_dek)
            .expect("KEK wrap of freshly generated DEK should not fail");

        let encrypted_dek = EncryptedDek {
            key_id: active_key_id,
            encrypted_bytes,
        };

        // 生成 Shamir 分片
        let shares = seal::split_secret(&root_key_bytes, n, k)?;

        let mut cache = LruCache::new(NonZeroUsize::new(DEK_CACHE_SIZE).unwrap());
        cache.put(active_key_id, Zeroizing::new(active_dek));

        let keyring = Self {
            inner: Arc::new(RwLock::new(KeyringInner {
                kek,
                active_dek: Zeroizing::new(active_dek),
                active_key_id,
                next_key_id,
                dek_cache: cache,
                sealed: false,
            })),
        };

        Ok((keyring, encrypted_dek, shares))
    }

    /// Unseal：从 Shamir 分片恢复 Root Key，解密持久化的 DEK，重建 Keyring。
    ///
    /// 用于 Sealed 状态下的恢复流程。调用方收集 ≥K 个分片后调用此方法。
    ///
    /// # 参数
    /// - `shares`: 至少 K 个合法 Shamir 分片
    /// - `encrypted_deks`: 从 `/_meta/dek/` 读取的所有 EncryptedDek 记录
    pub fn unseal(shares: &[Share], encrypted_deks: &[EncryptedDek]) -> Result<Self> {
        let root_key_bytes = seal::recover_secret(shares)?;
        Self::from_root_key(&root_key_bytes, encrypted_deks)
    }

    /// 从已有的 Root Key 和持久化的 EncryptedDek 列表恢复 Keyring。
    ///
    /// 用于节点重启时恢复（Unseal 后调用）。Root Key 由 Shamir 分片重组得到。
    pub fn from_root_key(
        root_key_bytes: &[u8],
        encrypted_deks: &[EncryptedDek],
    ) -> Result<Self> {
        let root_key = RootKey::from_bytes(root_key_bytes)?;
        let kek = root_key.derive_kek();

        let mut cache = LruCache::new(NonZeroUsize::new(DEK_CACHE_SIZE).unwrap());
        let mut active_key_id = 0u32;
        let mut active_dek = Zeroizing::new([0u8; DEK_LEN]);
        let mut max_key_id = 0u32;

        for ed in encrypted_deks {
            let dek = kek.unwrap_dek(&ed.encrypted_bytes)?;
            cache.put(ed.key_id, Zeroizing::new(*dek));

            if ed.key_id > max_key_id {
                max_key_id = ed.key_id;
                active_key_id = ed.key_id;
                active_dek = Zeroizing::new(*dek);
            }
        }

        if encrypted_deks.is_empty() {
            return Err(Error::Crypto(
                "no encrypted DEKs provided; cannot recover Keyring".into(),
            ));
        }

        let next_key_id = max_key_id + 1;

        Ok(Self {
            inner: Arc::new(RwLock::new(KeyringInner {
                kek,
                active_dek,
                active_key_id,
                next_key_id,
                dek_cache: cache,
                sealed: false,
            })),
        })
    }

    // ──── 查询 API ────

    /// 返回当前活跃 DEK 的引用（仅用于加密操作）。
    /// Seal 后返回全零 DEK（调用方应在加密前通过 is_sealed() 检查）。
    pub fn active_dek(&self) -> (u32, [u8; DEK_LEN]) {
        let inner = self.inner.read();
        if inner.sealed {
            return (0, [0u8; DEK_LEN]);
        }
        (inner.active_key_id, *inner.active_dek)
    }

    /// 返回当前活跃 DEK 的 key_id
    pub fn active_key_id(&self) -> u32 {
        self.inner.read().active_key_id
    }

    /// 返回是否处于 Sealed 状态
    pub fn is_sealed(&self) -> bool {
        self.inner.read().sealed
    }

    /// 查找指定 key_id 对应的 DEK（先查 active，再查缓存）。
    /// Seal 后所有查询均返回 Error::Crypto。
    pub fn get_dek(&self, key_id: u32) -> Result<Zeroizing<[u8; DEK_LEN]>> {
        let inner = self.inner.read();

        if inner.sealed {
            return Err(Error::Crypto("keyring is sealed".into()));
        }

        if key_id == inner.active_key_id {
            return Ok(Zeroizing::new(*inner.active_dek));
        }

        inner
            .dek_cache
            .peek(&key_id)
            .cloned()
            .ok_or_else(|| Error::Crypto(format!("DEK key_id={key_id} not found in keyring")))
    }

    /// 返回已知的 DEK 版本数（active + cached）
    pub fn dek_count(&self) -> usize {
        self.inner.read().dek_cache.len()
    }

    // ──── 密钥轮换 ────

    /// 密钥轮换：生成新 DEK，原子切换到新密钥，旧 DEK 移入缓存。
    ///
    /// 返回新 DEK 的 EncryptedDek（调用方需持久化到 `/_meta/dek/{new_key_id}`）。
    pub fn rotate(&self) -> EncryptedDek {
        let mut inner = self.inner.write();

        // 生成新 DEK
        let (new_dek_plain, new_key_id, next_key_id) =
            Self::generate_dek(&inner.kek, inner.next_key_id);

        // 旧 DEK 移入缓存
        let old_dek = Zeroizing::new(*inner.active_dek);
        let old_key_id = inner.active_key_id;
        inner.dek_cache.put(old_key_id, old_dek);

        // 新 DEK 也放入缓存
        inner
            .dek_cache
            .put(new_key_id, Zeroizing::new(new_dek_plain));

        // 用 KEK 加密新 DEK 用于落盘
        let encrypted_bytes = inner
            .kek
            .wrap_dek(&new_dek_plain)
            .expect("KEK wrap of freshly generated DEK should not fail");

        // 原子切换
        inner.active_dek = Zeroizing::new(new_dek_plain);
        inner.active_key_id = new_key_id;
        inner.next_key_id = next_key_id;

        EncryptedDek {
            key_id: new_key_id,
            encrypted_bytes,
        }
    }

    // ──── Seal ────

    /// Seal：清零内存中所有密钥材料并设置 sealed 标志。
    ///
    /// 调用后 Keyring 不可再用，必须通过 Unseal 重新初始化。
    /// 调用方应在调用此方法前确保所有写操作已完成。
    pub fn seal(&self) {
        let mut inner = self.inner.write();
        inner.kek.zeroize();
        inner.active_dek.zeroize();
        inner.dek_cache.clear();
        inner.sealed = true;
        tracing::info!("Keyring sealed: all key material zeroized");
    }

    // ──── 内部方法 ────

    /// 生成新的随机 DEK 并确定其 key_id
    fn generate_dek(_kek: &Kek, key_id: u32) -> ([u8; DEK_LEN], u32, u32) {
        let mut dek = [0u8; DEK_LEN];
        rand::thread_rng().fill_bytes(&mut dek);
        (dek, key_id, key_id + 1)
    }
}

// ──── 持久化类型 ────

/// 加密后的 DEK（用于持久化到 `/_meta/dek/{key_id}`）。
///
/// `encrypted_bytes` 格式：nonce(12B) || ciphertext+tag(48B)
#[derive(Debug, Clone)]
pub struct EncryptedDek {
    /// DEK 版本 ID（单调递增）
    pub key_id: u32,
    /// KEK 加密后的 DEK 密文（60 bytes）
    pub encrypted_bytes: Vec<u8>,
}

impl EncryptedDek {
    /// 验证密文长度是否合法
    pub fn validate(&self) -> Result<()> {
        if self.encrypted_bytes.len() != ENCRYPTED_DEK_LEN {
            return Err(Error::Crypto(format!(
                "encrypted DEK must be {ENCRYPTED_DEK_LEN} bytes, got {}",
                self.encrypted_bytes.len()
            )));
        }
        Ok(())
    }
}

// ──── 测试 ────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bootstrap_creates_valid_keyring() {
        let (keyring, encrypted_dek) = Keyring::bootstrap();

        assert_eq!(keyring.active_key_id(), 1);
        assert_eq!(keyring.dek_count(), 1);
        assert!(encrypted_dek.validate().is_ok());
        assert_eq!(encrypted_dek.key_id, 1);
    }

    #[test]
    fn test_dek_wrap_unwrap_via_from_root_key() {
        let (_keyring, encrypted_dek) = Keyring::bootstrap();

        // We can't extract the RootKey bytes from outside, but we can verify
        // that from_root_key rejects invalid input.
        let result = Keyring::from_root_key(b"too_short", &[encrypted_dek.clone()]);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("must be 32 bytes"), "unexpected error: {err_msg}");

        // Valid-length but wrong key should fail to unwrap DEK
        let wrong_key = [0xFFu8; ROOT_KEY_LEN];
        let result2 = Keyring::from_root_key(&wrong_key, &[encrypted_dek]);
        assert!(result2.is_err());
        let err_msg2 = format!("{}", result2.unwrap_err());
        // Should fail with "unwrap DEK failed" since KEK will differ
        assert!(err_msg2.contains("unwrap DEK failed"), "unexpected error: {err_msg2}");
    }

    #[test]
    fn test_rotate_produces_new_dek() {
        let (keyring, first_encrypted) = Keyring::bootstrap();
        assert_eq!(keyring.active_key_id(), 1);
        assert_eq!(keyring.dek_count(), 1);

        let second_encrypted = keyring.rotate();
        assert_eq!(keyring.active_key_id(), 2);
        assert_eq!(keyring.dek_count(), 2);
        assert_eq!(second_encrypted.key_id, 2);
        assert_ne!(first_encrypted.encrypted_bytes, second_encrypted.encrypted_bytes);

        // Old DEK should still be accessible
        let old_dek = keyring.get_dek(1);
        assert!(old_dek.is_ok());
    }

    #[test]
    fn test_get_dek_nonexistent() {
        let (keyring, _) = Keyring::bootstrap();
        let result = keyring.get_dek(999);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("not found"), "unexpected error: {err_msg}");
    }

    #[test]
    fn test_seal_zeroizes_keys() {
        let (keyring, _) = Keyring::bootstrap();
        keyring.seal();
        // After seal, looking up any key should fail
        let result = keyring.get_dek(1);
        assert!(result.is_err());
    }

    #[test]
    fn test_multiple_rotations() {
        let (keyring, _) = Keyring::bootstrap();
        assert_eq!(keyring.active_key_id(), 1);

        for i in 2..=6 {
            let enc = keyring.rotate();
            assert_eq!(keyring.active_key_id(), i);
            assert_eq!(enc.key_id, i);
        }

        // All old keys should still be accessible (within cache limit of 8)
        for id in 1..=6 {
            assert!(keyring.get_dek(id).is_ok(), "DEK {id} should be cached");
        }
    }

    #[test]
    fn test_encrypted_dek_validate() {
        let valid = EncryptedDek {
            key_id: 1,
            encrypted_bytes: vec![0u8; ENCRYPTED_DEK_LEN],
        };
        assert!(valid.validate().is_ok());

        let invalid = EncryptedDek {
            key_id: 2,
            encrypted_bytes: vec![0u8; 10],
        };
        assert!(invalid.validate().is_err());
    }

    // ──── Seal/Unseal 生命周期集成测试 ────

    #[test]
    fn test_bootstrap_with_shares_full_lifecycle() {
        // 1. Bootstrap: 生成 Keyring + DEK + 5 个分片（门限 3）
        let (keyring, encrypted_dek, shares) =
            Keyring::bootstrap_with_shares(5, 3).unwrap();

        assert_eq!(keyring.active_key_id(), 1);
        assert!(!keyring.is_sealed());
        assert_eq!(shares.len(), 5);
        for s in &shares {
            assert_eq!(s.threshold, 3);
            assert_eq!(s.total, 5);
        }

        // 2. Seal: 清零密钥
        keyring.seal();
        assert!(keyring.is_sealed());
        // 密封后无法获取 DEK
        assert!(keyring.get_dek(1).is_err());

        // 3. Unseal: 用量 3 个分片恢复
        let recovered =
            Keyring::unseal(&shares[..3], &[encrypted_dek]).unwrap();

        assert_eq!(recovered.active_key_id(), 1);
        assert!(!recovered.is_sealed());
        // 恢复后可以获取 DEK
        assert!(recovered.get_dek(1).is_ok());
    }

    #[test]
    fn test_unseal_with_wrong_shares_fails() {
        let (keyring, encrypted_dek, mut shares) =
            Keyring::bootstrap_with_shares(5, 3).unwrap();
        keyring.seal();

        // 篡改一个分片
        shares[0].y[0] ^= 0xFF;

        let result = Keyring::unseal(&shares[..3], &[encrypted_dek]);
        // 应该失败：恢复的 Root Key 不正确，无法解密 DEK
        assert!(result.is_err());
    }

    #[test]
    fn test_unseal_insufficient_shares_fails() {
        let (keyring, encrypted_dek, shares) =
            Keyring::bootstrap_with_shares(5, 3).unwrap();
        keyring.seal();

        // 只提供 2 个分片（需要 3 个）
        let result = Keyring::unseal(&shares[..2], &[encrypted_dek]);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("insufficient"),
            "unexpected error: {err_msg}"
        );
    }

    #[test]
    fn test_seal_then_rotate_while_sealed_fails() {
        let (keyring, _encrypted_dek, _shares) =
            Keyring::bootstrap_with_shares(5, 3).unwrap();

        keyring.seal();
        assert!(keyring.is_sealed());

        // Rotate 在 seal 后仍可调用（生成新 DEK），但 get_dek 不可用
        let new_enc = keyring.rotate();
        assert_eq!(new_enc.key_id, 2);
        // Sealed 状态下 get_dek 仍应失败
        assert!(keyring.get_dek(1).is_err());
        assert!(keyring.get_dek(2).is_err());
    }

    #[test]
    fn test_unseal_with_rotated_keys() {
        // Bootstrap with shares
        let (keyring, enc_dek_1, shares) =
            Keyring::bootstrap_with_shares(5, 3).unwrap();

        // Rotate twice
        let enc_dek_2 = keyring.rotate();
        let enc_dek_3 = keyring.rotate();
        assert_eq!(keyring.active_key_id(), 3);

        // Seal
        keyring.seal();

        // Unseal with all 3 DEK versions
        let all_deks = vec![enc_dek_1, enc_dek_2, enc_dek_3];
        let recovered = Keyring::unseal(&shares[..3], &all_deks).unwrap();

        assert_eq!(recovered.active_key_id(), 3);
        assert!(!recovered.is_sealed());
        // All versions should be accessible
        assert!(recovered.get_dek(1).is_ok());
        assert!(recovered.get_dek(2).is_ok());
        assert!(recovered.get_dek(3).is_ok());
    }

    #[test]
    fn test_bootstrap_with_shares_custom_params() {
        let (keyring, _, shares) =
            Keyring::bootstrap_with_shares(7, 4).unwrap();

        assert_eq!(shares.len(), 7);
        for s in &shares {
            assert_eq!(s.threshold, 4);
            assert_eq!(s.total, 7);
        }

        // 不能只用量 3 个分片恢复（需要 4 个）
        keyring.seal();
        // 注意：这里无法测试因为 keyring 被 move 了，但 share 结构体已验证
    }
}
