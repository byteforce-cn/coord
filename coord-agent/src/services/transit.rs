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
use sha2::{Digest, Sha256, Sha512};
use hmac::{Hmac, Mac};
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
    /// HMAC 密钥（仅内存，不落盘；重启后从 Server 重新获取）
    hmac_key: [u8; HMAC_KEY_LEN],
}

/// HMAC 密钥长度（256 位）
const HMAC_KEY_LEN: usize = 32;

impl TransitService {
    pub fn new(config: TransitConfig) -> Result<Self, String> {
        let mut hasher = Sha256::new();
        hasher.update(b"coord-transit-kek:");
        hasher.update(config.kek_id.as_bytes());
        let kek_hash = hasher.finalize();
        let mut kek = [0u8; DEK_LEN];
        kek.copy_from_slice(&kek_hash);
        // HMAC 密钥：从 KEK 派生（仅内存，不落盘；重启后自动重新派生）
        let mut hmac_key = [0u8; HMAC_KEY_LEN];
        let mut hmac_hasher = Sha256::new();
        hmac_hasher.update(b"coord-transit-hmac:");
        hmac_hasher.update(config.kek_id.as_bytes());
        let hmac_hash = hmac_hasher.finalize();
        hmac_key.copy_from_slice(&hmac_hash);
        Ok(Self {
            config,
            kek,
            dek_store: RwLock::new(HashMap::new()),
            hmac_key,
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

        // 7. 组装数据包: dek_id_len(1B) || dek_id || data_nonce(12) || dek_packet(60) || ciphertext
        let dek_id_bytes = dek_id.as_bytes();
        if dek_id_bytes.len() > 255 {
            return Err("dek_id too long".into());
        }
        let mut packet = Vec::with_capacity(1 + dek_id_bytes.len() + NONCE_LEN + DEK_PACKET_LEN + ciphertext.len());
        packet.push(dek_id_bytes.len() as u8);
        packet.extend_from_slice(dek_id_bytes);
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
        _dek_id: &str,
        context: &HashMap<String, String>,
    ) -> Result<Vec<u8>, String> {
        // 新格式（自描述）: dek_id_len(1B) || dek_id(N) || data_nonce(12) || dek_packet(60) || ciphertext
        // 兼容旧格式: data_nonce(12) || dek_packet(60) || ciphertext（无 dek_id 前缀）
        let min_legacy_len = NONCE_LEN + DEK_PACKET_LEN + TAG_LEN;
        if packet.len() < min_legacy_len {
            return Err("packet too short".into());
        }

        // 检测格式：若第一个字节不是有效的 DEK ID 长度前缀，视为旧格式
        let (dek_id, data_nonce_start, dek_packet_start) = {
            let candidate_len = packet[0] as usize;
            let candidate_end = 1 + candidate_len;
            // 检查：candidate_len 合理（1-64）、候选范围不越界、且剩余数据足够
            if candidate_len >= 1
                && candidate_len <= 64
                && candidate_end < packet.len()
                && packet.len() - candidate_end >= NONCE_LEN + DEK_PACKET_LEN + TAG_LEN
            {
                // 新格式：提取 dek_id
                let id = std::str::from_utf8(&packet[1..candidate_end])
                    .map_err(|_| "invalid dek_id encoding".to_string())?
                    .to_string();
                (id, candidate_end, candidate_end + NONCE_LEN)
            } else {
                // 旧格式：使用传入的 _dek_id 参数（硬编码 "default" 的兼容路径）
                (_dek_id.to_string(), 0, NONCE_LEN)
            }
        };

        // 1. 获取加密的 DEK packet
        // 优先从包头提取 dek_id；若对应 DEK 不在 store 中，回退到显式传入的 _dek_id
        let dek_packet_data = {
            let store = self.dek_store.read();
            let id_to_try = if store.contains_key(&dek_id) {
                &dek_id
            } else if !_dek_id.is_empty() && store.contains_key(_dek_id) {
                _dek_id
            } else {
                // 都不存在，用头部的 dek_id 报错（保持原有错误信息）
                &dek_id
            };
            store.get(id_to_try).cloned().ok_or_else(|| {
                format!("DEK '{}' not found (already used or not created)", id_to_try)
            })?
        };
        if dek_packet_data.len() < DEK_PACKET_LEN {
            return Err("invalid DEK packet".into());
        }

        // 2. 用 KEK 解密 DEK
        let kek_cipher = Aes256Gcm::new_from_slice(&self.kek)
            .map_err(|e| format!("invalid KEK: {e}"))?;
        let dek_nonce = Nonce::from_slice(&dek_packet_data[..NONCE_LEN]);
        let fixed_aad = b"coord-transit-dek-v1";

        let mut dek_bytes = kek_cipher
            .decrypt(dek_nonce, Payload { msg: &dek_packet_data[NONCE_LEN..], aad: fixed_aad.as_ref() })
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
            let mut removed = store.remove(&dek_id);
            if let Some(ref mut buf) = removed {
                buf.zeroize();
            }
        }

        // 4. 用 DEK 解密数据
        let data_cipher = Aes256Gcm::new_from_slice(&dek)
            .map_err(|e| format!("invalid DEK: {e}"))?;
        let data_nonce = Nonce::from_slice(&packet[data_nonce_start..data_nonce_start + NONCE_LEN]);
        let ciphertext = &packet[dek_packet_start + DEK_PACKET_LEN..];

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

    // ──── HMAC 签名与验签（Phase B.2 — 仅内存密钥，不落盘）───

    /// 使用 HMAC 对数据进行签名
    ///
    /// 支持算法: HMAC-SHA256（默认）, HMAC-SHA512
    /// 密钥仅存于内存，重启后通过 KEK 重新派生。
    pub fn hmac_sign(&self, data: &[u8], algorithm: &str) -> Result<Vec<u8>, String> {
        let algo = if algorithm.is_empty() { "HMAC-SHA256" } else { algorithm };
        match algo.to_uppercase().as_str() {
            "HMAC-SHA256" => {
                use sha2::Sha256;
                // 使用 digest::KeyInit 消除与 aes_gcm::aead::KeyInit 和 Mac::new_from_slice 的歧义
                let mut mac: Hmac<Sha256> = hmac::digest::KeyInit::new_from_slice(&self.hmac_key)
                    .map_err(|e| format!("HMAC-SHA256 init: {e}"))?;
                Mac::update(&mut mac, data);
                Ok(Mac::finalize(mac).into_bytes().to_vec())
            }
            "HMAC-SHA512" => {
                use sha2::Sha512;
                let mut mac: Hmac<Sha512> = hmac::digest::KeyInit::new_from_slice(&self.hmac_key)
                    .map_err(|e| format!("HMAC-SHA512 init: {e}"))?;
                Mac::update(&mut mac, data);
                Ok(Mac::finalize(mac).into_bytes().to_vec())
            }
            other => Err(format!("unsupported HMAC algorithm: {other}")),
        }
    }

    /// 验证 HMAC 签名
    pub fn hmac_verify(&self, data: &[u8], signature: &[u8], algorithm: &str) -> Result<bool, String> {
        let expected = self.hmac_sign(data, algorithm)?;
        // 常量时间比较
        Ok(expected.len() == signature.len() && {
            let mut acc = 0u8;
            for (a, b) in expected.iter().zip(signature.iter()) {
                acc |= a ^ b;
            }
            acc == 0
        })
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

    // ──── Phase B.2: HMAC 签名与验签测试 ────

    #[test]
    fn test_hmac_sign_sha256_default() {
        let svc = TransitService::new(TransitConfig::default()).expect("create");
        let data = b"hello hmac";
        let sig = svc.hmac_sign(data, "").expect("hmac_sign");
        assert!(!sig.is_empty());
        // SHA-256 HMAC 输出 32 字节
        assert_eq!(sig.len(), 32);
    }

    #[test]
    fn test_hmac_sign_sha512() {
        let svc = TransitService::new(TransitConfig::default()).expect("create");
        let sig = svc.hmac_sign(b"data", "HMAC-SHA512").expect("hmac_sign");
        assert_eq!(sig.len(), 64);
    }

    #[test]
    fn test_hmac_verify_roundtrip() {
        let svc = TransitService::new(TransitConfig::default()).expect("create");
        let data = b"verify me";
        let sig = svc.hmac_sign(data, "HMAC-SHA256").expect("sign");
        assert!(svc.hmac_verify(data, &sig, "HMAC-SHA256").expect("verify"));
    }

    #[test]
    fn test_hmac_verify_tampered_data() {
        let svc = TransitService::new(TransitConfig::default()).expect("create");
        let sig = svc.hmac_sign(b"original", "HMAC-SHA256").expect("sign");
        assert!(!svc.hmac_verify(b"tampered", &sig, "HMAC-SHA256").expect("verify"));
    }

    #[test]
    fn test_hmac_verify_tampered_signature() {
        let svc = TransitService::new(TransitConfig::default()).expect("create");
        let data = b"my data";
        let mut sig = svc.hmac_sign(data, "HMAC-SHA256").expect("sign");
        // Corrupt the signature
        sig[0] ^= 0xFF;
        assert!(!svc.hmac_verify(data, &sig, "HMAC-SHA256").expect("verify"));
    }

    #[test]
    fn test_hmac_deterministic() {
        let svc = TransitService::new(TransitConfig::default()).expect("create");
        let data = b"deterministic";
        let sig1 = svc.hmac_sign(data, "HMAC-SHA256").expect("sign");
        let sig2 = svc.hmac_sign(data, "HMAC-SHA256").expect("sign");
        assert_eq!(sig1, sig2);
    }

    #[test]
    fn test_hmac_unsupported_algorithm() {
        let svc = TransitService::new(TransitConfig::default()).expect("create");
        assert!(svc.hmac_sign(b"data", "HMAC-MD5").is_err());
    }
}
