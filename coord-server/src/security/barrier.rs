// Storage Barrier — AES-256-GCM 加密/解密屏障（P2）
//
// 职责：
// - encrypt: 使用当前活跃 DEK 加密 plaintext Value，返回 `key_id || nonce || ciphertext || tag`
// - decrypt: 解析密文头部的 key_id，查找对应 DEK 后解密
// - 对上层协调层/MVCC 透明：StateMachine 在写入/读取时调用 Barrier
//
// 密文格式（Big-Endian）：
//   [key_id: 4 bytes] [nonce: 12 bytes] [ciphertext: N bytes] [tag: 16 bytes]
//   总计开销：32 bytes/Value

use aes_gcm::{
    aead::{Aead, AeadCore, KeyInit, OsRng},
    Aes256Gcm, Key, Nonce,
};
use std::sync::Arc;

use coord_core::error::{Error, Result};

use super::key_management::Keyring;

// ──── 常量 ────

/// key_id 编码长度（u32 BE）
const KEY_ID_LEN: usize = 4;

/// GCM nonce 长度（96-bit）
const NONCE_LEN: usize = 12;

/// GCM tag 长度（128-bit）
const TAG_LEN: usize = 16;

/// 密文头部长度：key_id(4) + nonce(12) = 16 bytes
const HEADER_LEN: usize = KEY_ID_LEN + NONCE_LEN;

/// 密文最小长度：header(16) + tag(16) = 32 bytes（空 plaintext 场景）
const MIN_CIPHERTEXT_LEN: usize = HEADER_LEN + TAG_LEN;

// ──── Barrier ────

/// Storage Barrier — 加密/解密屏障。
///
/// 所有写入 Redb 的 KV Value 必须经过 Barrier 加密，所有读取的 Value 必须经过 Barrier 解密。
/// Barrier 位于 StateMachine 内部，对 Raft 层和协调层透明。
///
/// # 线程安全
/// `Barrier` 内部只读访问 `Keyring`（通过 `Arc`），可安全地在多线程间共享。
#[derive(Clone)]
pub struct Barrier {
    keyring: Arc<Keyring>,
}

impl Barrier {
    /// 创建 Barrier，绑定到给定的 Keyring
    pub fn new(keyring: Arc<Keyring>) -> Self {
        Self { keyring }
    }

    /// 加密 plaintext Value。
    ///
    /// 返回密文，格式：`key_id(4B BE) || nonce(12B) || ciphertext || tag(16B)`
    ///
    /// # 性能
    /// AES-256-GCM 在现代 CPU（AES-NI）上 < 0.05ms/KB。
    pub fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>> {
        let (key_id, dek) = self.keyring.active_dek();

        let key = Key::<Aes256Gcm>::from_slice(&dek[..]);
        let cipher = Aes256Gcm::new(key);
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);

        let ciphertext = cipher
            .encrypt(&nonce, plaintext)
            .map_err(|e| Error::Crypto(format!("Barrier encrypt failed: {e}")))?;

        // 编码：key_id(4B BE) || nonce(12B) || ciphertext+tag
        let mut output = Vec::with_capacity(KEY_ID_LEN + NONCE_LEN + ciphertext.len());
        output.extend_from_slice(&key_id.to_be_bytes());
        output.extend_from_slice(&nonce);
        output.extend_from_slice(&ciphertext);
        Ok(output)
    }

    /// 解密 ciphertext Value。
    ///
    /// 从密文头部解析 key_id 和 nonce，在 Keyring 中查找对应 DEK 后解密。
    ///
    /// # 错误
    /// - `Error::Crypto` — 密文格式错误、key_id 对应 DEK 不存在、认证失败
    pub fn decrypt(&self, encrypted: &[u8]) -> Result<Vec<u8>> {
        if encrypted.len() < MIN_CIPHERTEXT_LEN {
            return Err(Error::Crypto(format!(
                "ciphertext too short: {} bytes (min {MIN_CIPHERTEXT_LEN})",
                encrypted.len()
            )));
        }

        // 解析头部
        let key_id = u32::from_be_bytes(
            encrypted[..KEY_ID_LEN]
                .try_into()
                .expect("slice len checked above"),
        );

        let nonce = Nonce::from_slice(&encrypted[KEY_ID_LEN..HEADER_LEN]);

        // ciphertext + tag
        let ciphertext = &encrypted[HEADER_LEN..];

        // 查找对应 DEK
        let dek = self.keyring.get_dek(key_id)?;

        let key = Key::<Aes256Gcm>::from_slice(&dek[..]);
        let cipher = Aes256Gcm::new(key);

        let plaintext = cipher
            .decrypt(nonce, ciphertext)
            .map_err(|e| Error::Crypto(format!("Barrier decrypt failed (key_id={key_id}): {e}")))?;

        Ok(plaintext)
    }

    /// 尝试解密，但返回 `None` 而非错误（用于探测性读取）。
    pub fn try_decrypt(&self, encrypted: &[u8]) -> Option<Vec<u8>> {
        self.decrypt(encrypted).ok()
    }
}

// ──── 测试 ────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_barrier() -> (Barrier, crate::security::key_management::EncryptedDek) {
        let (keyring, encrypted_dek) = Keyring::bootstrap();
        let barrier = Barrier::new(Arc::new(keyring));
        (barrier, encrypted_dek)
    }

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let (barrier, _) = make_barrier();

        let plaintext = b"hello coord barrier";
        let encrypted = barrier.encrypt(plaintext).unwrap();

        // 密文长度 = header(16) + plaintext_len + tag(16)
        assert_eq!(encrypted.len(), 16 + plaintext.len() + 16);

        let decrypted = barrier.decrypt(&encrypted).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_encrypt_empty_value() {
        let (barrier, _) = make_barrier();

        let encrypted = barrier.encrypt(b"").unwrap();
        assert_eq!(encrypted.len(), MIN_CIPHERTEXT_LEN);

        let decrypted = barrier.decrypt(&encrypted).unwrap();
        assert!(decrypted.is_empty());
    }

    #[test]
    fn test_encrypt_different_nonces() {
        let (barrier, _) = make_barrier();

        let e1 = barrier.encrypt(b"same value").unwrap();
        let e2 = barrier.encrypt(b"same value").unwrap();

        // Same plaintext should produce different ciphertexts (random nonces)
        assert_ne!(e1, e2);
    }

    #[test]
    fn test_decrypt_with_old_key_after_rotation() {
        let (keyring, _) = Keyring::bootstrap();
        let barrier = Barrier::new(Arc::new(keyring.clone()));

        // Encrypt with key_id=1
        let encrypted_old = barrier.encrypt(b"data under key 1").unwrap();
        assert_eq!(encrypted_old[0..4], 1u32.to_be_bytes());

        // Rotate to key_id=2
        keyring.rotate();
        assert_eq!(keyring.active_key_id(), 2);

        // Encrypt with key_id=2
        let encrypted_new = barrier.encrypt(b"data under key 2").unwrap();
        assert_eq!(encrypted_new[0..4], 2u32.to_be_bytes());

        // Both should decrypt correctly
        let decrypted_old = barrier.decrypt(&encrypted_old).unwrap();
        assert_eq!(decrypted_old, b"data under key 1");

        let decrypted_new = barrier.decrypt(&encrypted_new).unwrap();
        assert_eq!(decrypted_new, b"data under key 2");
    }

    #[test]
    fn test_decrypt_corrupted_tag_fails() {
        let (barrier, _) = make_barrier();

        let plaintext = b"sensitive data";
        let mut encrypted = barrier.encrypt(plaintext).unwrap();

        // Corrupt the tag (last 16 bytes)
        let last = encrypted.len() - 1;
        encrypted[last] ^= 0xFF;

        let result = barrier.decrypt(&encrypted);
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("decrypt failed"));
    }

    #[test]
    fn test_decrypt_too_short_input() {
        let (barrier, _) = make_barrier();

        let result = barrier.decrypt(b"short");
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("too short"));
    }

    #[test]
    fn test_decrypt_unknown_key_id() {
        let (keyring, _) = Keyring::bootstrap();
        let barrier = Barrier::new(Arc::new(keyring.clone()));

        let mut encrypted = barrier.encrypt(b"test").unwrap();

        // Tamper the key_id to a non-existent one
        encrypted[0..4].copy_from_slice(&999u32.to_be_bytes());

        let result = barrier.decrypt(&encrypted);
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("not found"));
    }

    #[test]
    fn test_large_value_encrypt_decrypt() {
        let (barrier, _) = make_barrier();

        // 100KB plaintext
        let plaintext = vec![0xAB; 100_000];
        let encrypted = barrier.encrypt(&plaintext).unwrap();

        assert_eq!(encrypted.len(), HEADER_LEN + plaintext.len() + TAG_LEN);

        let decrypted = barrier.decrypt(&encrypted).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_try_decrypt_graceful_failure() {
        let (barrier, _) = make_barrier();

        // Corrupt ciphertext
        let result = barrier.try_decrypt(b"not valid ciphertext at all");
        assert!(result.is_none());
    }

    #[test]
    fn test_key_id_encoding_is_big_endian() {
        let (barrier, _) = make_barrier();

        let encrypted = barrier.encrypt(b"test").unwrap();
        let parsed_key_id = u32::from_be_bytes(
            encrypted[0..4].try_into().unwrap(),
        );
        assert_eq!(parsed_key_id, 1);
    }
}
