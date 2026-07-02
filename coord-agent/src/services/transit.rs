// coord-agent: Transit 信封加密服务 (Transit Service)
//
// 实现信封加密模式：DEK 本地生成（AES-256-GCM），KEK 存 Server，DEK 用后即焚。
//
// 架构（v8.2 §4.12）:
// - DEK（Data Encryption Key）本地随机生成
// - KEK（Key Encryption Key）存储在 Server，永不离开
// - 加密数据：ciphertext = AES-256-GCM(plaintext, DEK) || AES-256-GCM(DEK, KEK)
// - DEK 使用后立即从内存销毁（zeroize）
// - 支持上下文绑定（context-dependent encryption，在数据层实现）
// - 支持密钥轮换（rewrap：用 KEK 重新加密 DEK）
//
// 参见 docs/client-agent-architecture.v8.2.md §4.12。

use std::collections::HashMap;

use aes_gcm::aead::{Aead, KeyInit, OsRng, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use parking_lot::RwLock;
use rand::RngCore;
use sha2::{Digest, Sha256};
use zeroize::Zeroize;

// ──── 公共类型 ────

/// Transit 服务配置
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TransitConfig {
    /// DEK 有效期（秒），默认 3600
    pub dek_ttl_secs: u64,
    /// KEK 标识符
    pub kek_id: String,
}

impl Default for TransitConfig {
    fn default() -> Self {
        Self {
            dek_ttl_secs: 3600,
            kek_id: "default-kek".into(),
        }
    }
}

/// 常量
const NONCE_LEN: usize = 12;
const DEK_LEN: usize = 32; // AES-256 key
const TAG_LEN: usize = 16; // GCM authentication tag
const DEK_PACKET_LEN: usize = NONCE_LEN + DEK_LEN + TAG_LEN; // 60 bytes: nonce + encrypted_dek

// ──── TransitService ────

/// 信封加密服务
pub struct TransitService {
    config: TransitConfig,
    /// KEK（从 Server 获取，简化版使用 SHA-256 派生）
    kek: [u8; DEK_LEN],
    /// DEK 注册表：dek_id → (encrypted_packet, created_at)
    /// 解密后 DEK 立即移除（用后即焚）
    dek_store: RwLock<HashMap<String, Vec<u8>>>,
}

impl TransitService {
    pub fn new(config: TransitConfig) -> Result<Self, String> {
        let mut hasher = Sha256::new();
        hasher.update(b"coord-transit-kek:");
        hasher.update(config.kek_id.as_bytes());
        let kek_hash = hasher.finalize();
        let mut kek = [0u8; DEK_LEN];
        kek.copy_from_slice(&kek_hash);
        Ok(Self {
            config,
            kek,
            dek_store: RwLock::new(HashMap::new()),
        })
    }

    // ──── 加密 ────

    pub fn encrypt(&self, plaintext: &[u8]) -> Result<(Vec<u8>, String), String> {
        self.encrypt_inner(plaintext, &HashMap::new())
    }

    pub fn encrypt_with_context(
        &self,
        plaintext: &[u8],
        context: &HashMap<String, String>,
    ) -> Result<(Vec<u8>, String), String> {
        self.encrypt_inner(plaintext, context)
    }

    fn encrypt_inner(
        &self,
        plaintext: &[u8],
        context: &HashMap<String, String>,
    ) -> Result<(Vec<u8>, String), String> {
        // 1. 生成随机 DEK
        let mut dek = [0u8; DEK_LEN];
        OsRng.fill_bytes(&mut dek);

        // 2. 用 KEK 加密 DEK（固定 AAD，不使用上下文）
        let kek_cipher = Aes256Gcm::new_from_slice(&self.kek)
            .map_err(|e| format!("invalid KEK: {e}"))?;
        let mut dek_nonce_bytes = [0u8; NONCE_LEN];
        OsRng.fill_bytes(&mut dek_nonce_bytes);
        let dek_nonce = Nonce::from_slice(&dek_nonce_bytes);

        let fixed_aad = b"coord-transit-dek-v1";
        let mut encrypted_dek_body = kek_cipher
            .encrypt(dek_nonce, Payload { msg: dek.as_ref(), aad: fixed_aad.as_ref() })
            .map_err(|e| format!("DEK encrypt failed: {e}"))?;

        // DEK 存储格式: nonce(12) || encrypted_body(48)
        let mut dek_packet = Vec::with_capacity(DEK_PACKET_LEN);
        dek_packet.extend_from_slice(&dek_nonce_bytes);
        dek_packet.append(&mut encrypted_dek_body);

        // 3. 用 DEK 加密数据（可选上下文绑定）
        let data_cipher = Aes256Gcm::new_from_slice(&dek)
            .map_err(|e| format!("invalid DEK: {e}"))?;
        let mut data_nonce_bytes = [0u8; NONCE_LEN];
        OsRng.fill_bytes(&mut data_nonce_bytes);
        let data_nonce = Nonce::from_slice(&data_nonce_bytes);

        let data_aad = build_context_aad(context);
        let mut ciphertext = data_cipher
            .encrypt(data_nonce, Payload { msg: plaintext, aad: &data_aad })
            .map_err(|e| format!("data encrypt failed: {e}"))?;

        // 4. 销毁 DEK
        dek.zeroize();

        // 5. 生成 DEK ID
        let dek_id = compute_dek_id(&dek_packet);

        // 6. 存储加密后的 DEK
        self.dek_store.write().insert(dek_id.clone(), dek_packet.clone());

        // 7. 组装数据包: data_nonce(12) || dek_packet(60) || ciphertext
        let mut packet = Vec::with_capacity(NONCE_LEN + DEK_PACKET_LEN + ciphertext.len());
        packet.extend_from_slice(&data_nonce_bytes);
        packet.extend_from_slice(&dek_packet);
        packet.append(&mut ciphertext);

        dek_packet.zeroize();
        Ok((packet, dek_id))
    }

    // ──── 解密 ────

    pub fn decrypt(&self, packet: &[u8], dek_id: &str) -> Result<Vec<u8>, String> {
        self.decrypt_inner(packet, dek_id, &HashMap::new())
    }

    pub fn decrypt_with_context(
        &self,
        packet: &[u8],
        dek_id: &str,
        context: &HashMap<String, String>,
    ) -> Result<Vec<u8>, String> {
        self.decrypt_inner(packet, dek_id, context)
    }

    fn decrypt_inner(
        &self,
        packet: &[u8],
        dek_id: &str,
        context: &HashMap<String, String>,
    ) -> Result<Vec<u8>, String> {
        if packet.len() < NONCE_LEN + DEK_PACKET_LEN + TAG_LEN {
            return Err("packet too short".into());
        }

        // 1. 获取加密的 DEK packet
        let dek_packet = {
            let store = self.dek_store.read();
            store.get(dek_id).cloned().ok_or_else(|| {
                format!("DEK '{dek_id}' not found (already used or not created)")
            })?
        };
        if dek_packet.len() < DEK_PACKET_LEN {
            return Err("invalid DEK packet".into());
        }

        // 2. 用 KEK 解密 DEK
        let kek_cipher = Aes256Gcm::new_from_slice(&self.kek)
            .map_err(|e| format!("invalid KEK: {e}"))?;
        let dek_nonce = Nonce::from_slice(&dek_packet[..NONCE_LEN]);
        let fixed_aad = b"coord-transit-dek-v1";

        let mut dek_bytes = kek_cipher
            .decrypt(dek_nonce, Payload { msg: &dek_packet[NONCE_LEN..], aad: fixed_aad.as_ref() })
            .map_err(|e| format!("DEK decrypt failed: {e}"))?;

        if dek_bytes.len() != DEK_LEN {
            return Err("invalid DEK length".into());
        }
        let mut dek = [0u8; DEK_LEN];
        dek.copy_from_slice(&dek_bytes);
        dek_bytes.zeroize();

        // 3. 销毁存储中的 DEK（用后即焚）
        {
            let mut store = self.dek_store.write();
            let mut removed = store.remove(dek_id);
            if let Some(ref mut buf) = removed {
                buf.zeroize();
            }
        }

        // 4. 用 DEK 解密数据
        let data_cipher = Aes256Gcm::new_from_slice(&dek)
            .map_err(|e| format!("invalid DEK: {e}"))?;
        let data_nonce = Nonce::from_slice(&packet[..NONCE_LEN]);
        let ciphertext = &packet[NONCE_LEN + DEK_PACKET_LEN..];

        let data_aad = build_context_aad(context);
        let plaintext = data_cipher
            .decrypt(data_nonce, Payload { msg: ciphertext, aad: &data_aad })
            .map_err(|e| format!("data decrypt failed (wrong context?): {e}"))?;

        dek.zeroize();
        Ok(plaintext)
    }

    // ──── 密钥轮换 ────

    pub fn rewrap(&self, old_dek_id: &str) -> Result<String, String> {
        let dek_packet = {
            let store = self.dek_store.read();
            store.get(old_dek_id).cloned().ok_or_else(|| {
                format!("DEK '{old_dek_id}' not found for rewrap")
            })?
        };

        // 解密旧 DEK
        let kek_cipher = Aes256Gcm::new_from_slice(&self.kek)
            .map_err(|e| format!("invalid KEK: {e}"))?;
        let dek_nonce = Nonce::from_slice(&dek_packet[..NONCE_LEN]);
        let fixed_aad = b"coord-transit-dek-v1";

        let mut dek_bytes = kek_cipher
            .decrypt(dek_nonce, Payload { msg: &dek_packet[NONCE_LEN..], aad: fixed_aad.as_ref() })
            .map_err(|e| format!("DEK decrypt for rewrap failed: {e}"))?;

        let mut dek = [0u8; DEK_LEN];
        dek.copy_from_slice(&dek_bytes);
        dek_bytes.zeroize();

        // 重新加密 DEK（新 nonce）
        let mut new_nonce_bytes = [0u8; NONCE_LEN];
        OsRng.fill_bytes(&mut new_nonce_bytes);
        let new_nonce = Nonce::from_slice(&new_nonce_bytes);

        let mut new_body = kek_cipher
            .encrypt(new_nonce, Payload { msg: dek.as_ref(), aad: fixed_aad.as_ref() })
            .map_err(|e| format!("DEK re-encrypt failed: {e}"))?;

        let mut new_packet = Vec::with_capacity(DEK_PACKET_LEN);
        new_packet.extend_from_slice(&new_nonce_bytes);
        new_packet.append(&mut new_body);

        let new_dek_id = compute_dek_id(&new_packet);

        // 替换旧 DEK
        {
            let mut store = self.dek_store.write();
            store.insert(new_dek_id.clone(), new_packet.clone());
            let mut removed = store.remove(old_dek_id);
            if let Some(ref mut buf) = removed {
                buf.zeroize();
            }
        }

        dek.zeroize();
        new_packet.zeroize();
        Ok(new_dek_id)
    }

    pub fn config(&self) -> &TransitConfig {
        &self.config
    }
}

// ──── 工具函数 ────

fn build_context_aad(context: &HashMap<String, String>) -> Vec<u8> {
    if context.is_empty() {
        return b"coord-transit-data-v1".to_vec();
    }
    let mut aad = b"coord-transit-data-v1:".to_vec();
    let mut keys: Vec<&String> = context.keys().collect();
    keys.sort();
    for k in keys {
        aad.extend_from_slice(k.as_bytes());
        aad.push(b'=');
        aad.extend_from_slice(context[k].as_bytes());
        aad.push(b';');
    }
    aad
}

fn compute_dek_id(encrypted_packet: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(encrypted_packet);
    hex::encode(&hasher.finalize()[..8])
}

// ──── tests ────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let c = TransitConfig::default();
        assert_eq!(c.dek_ttl_secs, 3600);
        assert_eq!(c.kek_id, "default-kek");
    }

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let svc = TransitService::new(TransitConfig::default()).expect("create");
        let pt = b"hello world";
        let (ct, id) = svc.encrypt(pt).expect("encrypt");
        assert_ne!(ct, pt);
        let dec = svc.decrypt(&ct, &id).expect("decrypt");
        assert_eq!(dec, pt);
    }

    #[test]
    fn test_dek_single_use() {
        let svc = TransitService::new(TransitConfig::default()).expect("create");
        let (ct, id) = svc.encrypt(b"secret").expect("encrypt");
        assert!(svc.decrypt(&ct, &id).is_ok());
        assert!(svc.decrypt(&ct, &id).is_err(), "DEK should be single-use");
    }

    #[test]
    fn test_context_binding() {
        let svc = TransitService::new(TransitConfig::default()).expect("create");
        let mut ctx = HashMap::new();
        ctx.insert("tenant".into(), "acme".into());

        let (ct, id) = svc.encrypt_with_context(b"data", &ctx).expect("encrypt");

        // Correct context works
        assert!(svc.decrypt_with_context(&ct, &id, &ctx).is_ok());

        // Wrong context fails (need new encryption since DEK was consumed)
        let (ct2, id2) = svc.encrypt_with_context(b"data", &ctx).expect("encrypt");
        let mut wrong_ctx = HashMap::new();
        wrong_ctx.insert("tenant".into(), "evil".into());
        assert!(
            svc.decrypt_with_context(&ct2, &id2, &wrong_ctx).is_err(),
            "wrong context should fail"
        );
    }

    #[test]
    fn test_rewrap() {
        let svc = TransitService::new(TransitConfig::default()).expect("create");
        let pt = b"rotate me";
        let (ct, old_id) = svc.encrypt(pt).expect("encrypt");

        let new_id = svc.rewrap(&old_id).expect("rewrap");
        assert_ne!(new_id, old_id);
        assert!(svc.decrypt(&ct, &new_id).is_ok());
        assert!(svc.decrypt(&ct, &old_id).is_err());
    }
}
