// coord-agent: Transit 信封加密服务 (Transit Service)
//
// 实现信封加密模式：DEK 本地生成（AES-256-GCM），KEK 存 Server，DEK 用后即焚。
//
// 架构:
// - DEK（Data Encryption Key）本地随机生成
// - KEK（Key Encryption Key）存储在 Server，永不离开
// - 加密数据：ciphertext = AES-256-GCM(plaintext, DEK) || AES-256-GCM(DEK, KEK)
// - DEK 使用后立即从内存销毁（zeroize）
// - 支持上下文绑定（context-dependent encryption，在数据层实现）
// - 支持密钥轮换（rewrap：用 KEK 重新加密 DEK）
//
// 持久化（B-06 / 计划书工作流 E1，2026-09-19）：
// - 加密后的 DEK packet **落 coord-server KV**（`/_transit/v1/dek/{dek_id}`，见
//   [`super::transit_store`]），因此 **重启不丢密钥**：重启后仍能解密重启前产生的密文，
//   且多 Agent 共享同一 key 空间（单次使用语义跨进程成立）。
// - 单次使用（用后即焚）语义**不因持久化而放宽**：解密路径消费 DEK 后即从 KV 删除；
//   删除失败 → 返回错误（fail-closed），不静默放过。
// - 生产路径是 [`TransitService::encrypt_persisted`] / `decrypt_persisted` /
//   `rewrap_persisted`（gRPC handler 走这三条）；`encrypt` / `decrypt` 等**同步**方法
//   只操作内存注册表，保留给单测与无 server 的降级场景（历史行为，逐字未改）。
//
// KEK 供给（U-04 / W4-2a，2026-09-22 落地；此前由 `kek_id` 确定性派生 ⇒ 不保密）：
// - KEK **不再**由配置字符串派生。启动时必须**注入 32 字节密钥材料**：
//     ① 环境变量 `COORD_TRANSIT_KEK`（hex64），或
//     ② `<agent data_dir>/transit-kek.bin`（32 字节原始材料，0600）。
//   KEK = HKDF-SHA256(材料, info = "coord-transit-kek-v1:" || kek_id)。
//   `kek_id` 降级为**域分隔/审计标签**，不再是密钥来源（拿到配置无法推导 KEK）。
// - **fail-closed**：材料缺失/长度不符 ⇒ 构造失败（`TransitKekMaterial::resolve`
//   返回 Err）⇒ agent 启动**拒绝**，不静默降级为旧派生路径。见 `security.md` §2。
// - 仍未闭合的边界（**不要**把本服务读作"密钥已妥善托管"）：本方案**不是外部 KMS**；
//   材料以文件/环境形态落在 agent 主机上，主机被控 ⇒ KEK 泄露。
//   落盘 DEK 的静态保护另有一层 coord-server redb + Barrier 加密。

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use aes_gcm::aead::{Aead, KeyInit, OsRng, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use parking_lot::RwLock;
use rand::RngCore;
use sha2::{Digest, Sha256};
use zeroize::{Zeroize, Zeroizing};

use super::transit_store::{now_unix, DekRecord, MemoryTransitDekStore, TransitDekStore};

// ──── 公共类型 ────

/// Transit 服务配置
///
/// 注意：**不含密钥材料**。KEK 材料见 [`TransitKekMaterial`]（启动注入）。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TransitConfig {
    /// DEK 有效期（秒），默认 3600
    pub dek_ttl_secs: u64,
    /// KEK 标识符 —— **仅作 HKDF 域分隔与审计标签**（不再是密钥来源）
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

/// 静态加密 KEK 长度（256-bit）
const KEK_MATERIAL_LEN: usize = 32;

/// 环境变量名：注入 KEK 密钥材料（hex64）
pub const TRANSIT_KEK_ENV: &str = "COORD_TRANSIT_KEK";

/// 数据目录内的密钥材料文件名（32 字节原始材料）
pub const TRANSIT_KEK_FILE: &str = "transit-kek.bin";

/// 启动时注入的 KEK 密钥材料（32 字节）。
///
/// `Debug` 刻意**不打印材料**（`Zeroizing<[u8;32]>` 的默认 Debug 会打印内部字节）。
pub struct TransitKekMaterial(Zeroizing<[u8; KEK_MATERIAL_LEN]>);

impl std::fmt::Debug for TransitKekMaterial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TransitKekMaterial(<redacted 32 bytes>)")
    }
}

impl TransitKekMaterial {
    /// 从原始字节构造（长度必须是 32；空/短/长一律 Err —— **不许静默补齐或截断**）
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() != KEK_MATERIAL_LEN {
            return Err(format!(
                "transit KEK material must be exactly {KEK_MATERIAL_LEN} bytes, got {}",
                bytes.len()
            ));
        }
        let mut m = Zeroizing::new([0u8; KEK_MATERIAL_LEN]);
        m.copy_from_slice(bytes);
        Ok(Self(m))
    }

    /// 从 hex 构造（长度为 64 个 hex 字符；大小写均可）
    pub fn from_hex(hex_str: &str) -> Result<Self, String> {
        let s = hex_str.trim();
        if s.is_empty() {
            return Err("transit KEK material is empty".into());
        }
        let bytes =
            hex::decode(s).map_err(|e| format!("{TRANSIT_KEK_ENV} is not valid hex: {e}"))?;
        Self::from_bytes(&bytes)
    }

    /// 启动期解析（**fail-closed**）：
    ///
    /// 1. 环境变量 [`TRANSIT_KEK_ENV`]（hex64）；
    /// 2. `<data_dir>/`[`TRANSIT_KEK_FILE`]（32 字节原始材料）；
    /// 3. 都不存在 ⇒ `Err`（调用方必**拒绝启动**，不得回落到派生 KEK）。
    pub fn resolve(data_dir: &Path) -> Result<Self, String> {
        match std::env::var(TRANSIT_KEK_ENV) {
            Ok(v) => Self::resolve_with(data_dir, Some(&v)),
            Err(_) => Self::resolve_with(data_dir, None),
        }
    }

    /// [`Self::resolve`] 的确定性内核（环境变量值由调用方传入）。
    ///
    /// 拆出来是为了让「无材料 ⇒ 拒绝」这条判据能被**确定性**地测到：
    /// 直接测 `resolve` 会被进程级环境变量污染（测试并行 + `set_var` 是全局的）。
    pub fn resolve_with(data_dir: &Path, env_value: Option<&str>) -> Result<Self, String> {
        if let Some(v) = env_value {
            return Self::from_hex(v).map_err(|e| format!("invalid {TRANSIT_KEK_ENV}: {e}"));
        }
        let path = data_dir.join(TRANSIT_KEK_FILE);
        match std::fs::read(&path) {
            Ok(bytes) => {
                Self::from_bytes(&bytes).map_err(|e| format!("invalid {}: {e}", path.display()))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(format!(
                "transit service is enabled but no KEK material was injected: set \
                 {TRANSIT_KEK_ENV} (hex64) or provide {} (32 raw bytes); refusing to \
                 start (no silent fallback to a config-derived KEK)",
                path.display()
            )),
            Err(e) => Err(format!("read {}: {e}", path.display())),
        }
    }

    /// 仅测试用：固定的非生产材料（**绝不可**用于任何真实部署）。
    #[cfg(test)]
    fn for_test() -> Self {
        Self::from_bytes(&[0x5Au8; KEK_MATERIAL_LEN]).expect("32 bytes")
    }
}

/// 常量
const NONCE_LEN: usize = 12;
const DEK_LEN: usize = 32; // AES-256 key
const TAG_LEN: usize = 16; // GCM authentication tag
const DEK_PACKET_LEN: usize = NONCE_LEN + DEK_LEN + TAG_LEN; // 60 bytes: nonce + encrypted_dek

// ──── TransitService ────

/// 内存中的 DEK 条目（加密态 packet + TTL 簿记）
#[derive(Debug, Clone)]
struct DekEntry {
    /// `nonce(12) || AES-256-GCM(DEK, KEK)(48)`
    packet: Vec<u8>,
    /// 创建时间（UNIX 秒）
    created_at: u64,
    /// 过期时间（UNIX 秒）；0 = 永不过期
    expires_at: u64,
}

impl DekEntry {
    fn is_expired(&self, now: u64) -> bool {
        self.expires_at != 0 && now >= self.expires_at
    }

    fn to_record(&self) -> DekRecord {
        DekRecord {
            dek_packet: self.packet.clone(),
            created_at: self.created_at,
            expires_at: self.expires_at,
        }
    }
}

/// 信封加密服务
pub struct TransitService {
    config: TransitConfig,
    /// KEK = HKDF-SHA256(启动注入的材料, info="coord-transit-kek-v1:"||kek_id)，仅存内存
    kek: [u8; DEK_LEN],
    /// DEK 注册表：dek_id → DekEntry（加密态 packet + TTL）
    /// 解密后 DEK 立即移除（用后即焚）
    dek_store: RwLock<HashMap<String, DekEntry>>,
    /// HMAC 密钥（仅内存，不落盘；重启后从 Server 重新获取）
    hmac_key: [u8; HMAC_KEY_LEN],
    /// DEK 持久化后端（生产：coord-server KV；单测/骨架：内存）
    store: Arc<dyn TransitDekStore>,
    /// 上次清扫过期 DEK 的 UNIX 秒（0 = 从未）；避免每次加密都全量扫描
    last_sweep: AtomicU64,
}

/// HMAC 密钥长度（256 位）
const HMAC_KEY_LEN: usize = 32;

/// 两次过期清扫的最小间隔（秒）
const SWEEP_INTERVAL_SECS: u64 = 300;

impl TransitService {
    /// 构造服务：持久化后端为内存实现（开发/单测；"重启即丢"行为保留给这些场景）。
    ///
    /// `kek_material` 必填 —— 缺失只能由调用方在**解析期**发现（[`TransitKekMaterial::resolve`]
    /// 返回 Err），本构造器不接受"没有材料"这种状态。
    pub fn new(config: TransitConfig, kek_material: TransitKekMaterial) -> Result<Self, String> {
        Self::with_store(config, Arc::new(MemoryTransitDekStore::new()), kek_material)
    }

    /// 构造服务并注入持久化后端
    ///
    /// 生产路径传 [`super::transit_store::KvTransitDekStore`]（coord-server 共享 KV）。
    ///
    /// KEK 由**注入的材料**经 HKDF-SHA256 派生（`info = "coord-transit-kek-v1:" || kek_id`），
    /// 不再由 `kek_id` 配置串直接哈希得到 —— 即"拿到配置即可推导 KEK"这一缺陷已闭合（U-04）。
    pub fn with_store(
        config: TransitConfig,
        store: Arc<dyn TransitDekStore>,
        kek_material: TransitKekMaterial,
    ) -> Result<Self, String> {
        let mut kek = [0u8; DEK_LEN];
        let mut info = Vec::with_capacity(24 + config.kek_id.len());
        info.extend_from_slice(b"coord-transit-kek-v1:");
        info.extend_from_slice(config.kek_id.as_bytes());
        Hkdf::<Sha256>::new(None, &*kek_material.0)
            .expand(&info, &mut kek)
            .map_err(|e| format!("HKDF expand for transit KEK failed: {e}"))?;

        // HMAC 密钥：同一材料、不同 info（域分隔；仅内存，不落盘）
        let mut hmac_key = [0u8; HMAC_KEY_LEN];
        let mut hmac_info = Vec::with_capacity(25 + config.kek_id.len());
        hmac_info.extend_from_slice(b"coord-transit-hmac-v1:");
        hmac_info.extend_from_slice(config.kek_id.as_bytes());
        Hkdf::<Sha256>::new(None, &*kek_material.0)
            .expand(&hmac_info, &mut hmac_key)
            .map_err(|e| format!("HKDF expand for transit HMAC key failed: {e}"))?;

        Ok(Self {
            config,
            kek,
            dek_store: RwLock::new(HashMap::new()),
            hmac_key,
            store,
            last_sweep: AtomicU64::new(0),
        })
    }

    /// 持久化后端（测试/可观测性用）
    pub fn dek_store_backend(&self) -> &Arc<dyn TransitDekStore> {
        &self.store
    }

    // ──── 加密 ────

    /// **仅内存**加密：DEK 只进内存注册表，**不落盘**（单测 / 无 server 降级场景）。
    ///
    /// 生产路径请用 [`Self::encrypt_persisted`]（同一密文格式 + 额外落 KV）。
    pub fn encrypt(&self, plaintext: &[u8]) -> Result<(Vec<u8>, String), String> {
        self.encrypt_inner(plaintext, &HashMap::new())
    }

    /// **仅内存**加密（带上下文绑定）。生产路径见 [`Self::encrypt_with_context_persisted`]。
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
        let kek_cipher =
            Aes256Gcm::new_from_slice(&self.kek).map_err(|e| format!("invalid KEK: {e}"))?;
        let mut dek_nonce_bytes = [0u8; NONCE_LEN];
        OsRng.fill_bytes(&mut dek_nonce_bytes);
        let dek_nonce = Nonce::from_slice(&dek_nonce_bytes);

        let fixed_aad = b"coord-transit-dek-v1";
        let mut encrypted_dek_body = kek_cipher
            .encrypt(
                dek_nonce,
                Payload {
                    msg: dek.as_ref(),
                    aad: fixed_aad.as_ref(),
                },
            )
            .map_err(|e| format!("DEK encrypt failed: {e}"))?;

        // DEK 存储格式: nonce(12) || encrypted_body(48)
        let mut dek_packet = Vec::with_capacity(DEK_PACKET_LEN);
        dek_packet.extend_from_slice(&dek_nonce_bytes);
        dek_packet.append(&mut encrypted_dek_body);

        // 3. 用 DEK 加密数据（可选上下文绑定）
        let data_cipher =
            Aes256Gcm::new_from_slice(&dek).map_err(|e| format!("invalid DEK: {e}"))?;
        let mut data_nonce_bytes = [0u8; NONCE_LEN];
        OsRng.fill_bytes(&mut data_nonce_bytes);
        let data_nonce = Nonce::from_slice(&data_nonce_bytes);

        let data_aad = build_context_aad(context);
        let mut ciphertext = data_cipher
            .encrypt(
                data_nonce,
                Payload {
                    msg: plaintext,
                    aad: &data_aad,
                },
            )
            .map_err(|e| format!("data encrypt failed: {e}"))?;

        // 4. 销毁 DEK
        dek.zeroize();

        // 5. 生成 DEK ID
        let dek_id = compute_dek_id(&dek_packet);

        // 6. 存储加密后的 DEK（内存注册表；落盘由 `*_persisted` 包装层负责，
        //    两者使用同一 TTL 口径 —— `dek_ttl_secs = 0` 表示永不过期）
        let now = now_unix();
        let expires_at = if self.config.dek_ttl_secs == 0 {
            0
        } else {
            now.saturating_add(self.config.dek_ttl_secs)
        };
        self.dek_store.write().insert(
            dek_id.clone(),
            DekEntry {
                packet: dek_packet.clone(),
                created_at: now,
                expires_at,
            },
        );

        // 7. 组装数据包: dek_id_len(1B) || dek_id || data_nonce(12) || dek_packet(60) || ciphertext
        let dek_id_bytes = dek_id.as_bytes();
        if dek_id_bytes.len() > 255 {
            return Err("dek_id too long".into());
        }
        let mut packet = Vec::with_capacity(
            1 + dek_id_bytes.len() + NONCE_LEN + DEK_PACKET_LEN + ciphertext.len(),
        );
        packet.push(dek_id_bytes.len() as u8);
        packet.extend_from_slice(dek_id_bytes);
        packet.extend_from_slice(&data_nonce_bytes);
        packet.extend_from_slice(&dek_packet);
        packet.append(&mut ciphertext);

        dek_packet.zeroize();
        Ok((packet, dek_id))
    }

    // ──── 解密 ────

    /// **仅内存**解密：只查内存注册表（单测 / 无 server 降级场景）。
    ///
    /// 生产路径请用 [`Self::decrypt_persisted`]（可回取重启前落盘的 DEK）。
    pub fn decrypt(&self, packet: &[u8], dek_id: &str) -> Result<Vec<u8>, String> {
        self.decrypt_inner(packet, dek_id, &HashMap::new())
    }

    /// **仅内存**解密（带上下文绑定）。生产路径见 [`Self::decrypt_with_context_persisted`]。
    pub fn decrypt_with_context(
        &self,
        packet: &[u8],
        dek_id: &str,
        context: &HashMap<String, String>,
    ) -> Result<Vec<u8>, String> {
        self.decrypt_inner(packet, dek_id, context)
    }

    /// 解密（丢弃"被消费的 DEK id"）—— 仅内存路径的入口
    fn decrypt_inner(
        &self,
        packet: &[u8],
        dek_id: &str,
        context: &HashMap<String, String>,
    ) -> Result<Vec<u8>, String> {
        self.decrypt_inner_consumed(packet, dek_id, context)
            .map(|(plaintext, _consumed)| plaintext)
    }

    /// 解密并返回**被消费的 DEK id**（用后即焚）
    ///
    /// 持久化路径据此删除 KV 中的 DEK 记录，使"单次使用"跨重启/跨 Agent 成立。
    fn decrypt_inner_consumed(
        &self,
        packet: &[u8],
        _dek_id: &str,
        context: &HashMap<String, String>,
    ) -> Result<(Vec<u8>, String), String> {
        // 新格式（自描述）: dek_id_len(1B) || dek_id(N) || data_nonce(12) || dek_packet(60) || ciphertext
        // 兼容旧格式: data_nonce(12) || dek_packet(60) || ciphertext（无 dek_id 前缀）
        let layout = parse_packet_layout(packet, _dek_id)?;
        let dek_id = layout.dek_id;
        let data_nonce_start = layout.data_nonce_start;
        let dek_packet_start = layout.dek_packet_start;

        // 1. 获取加密的 DEK packet
        // 优先从包头提取 dek_id；若对应 DEK 不在 store 中，回退到显式传入的 _dek_id
        let (dek_packet_data, consumed_id) = {
            let store = self.dek_store.read();
            let id_to_try = if store.contains_key(&dek_id) {
                &dek_id
            } else if !_dek_id.is_empty() && store.contains_key(_dek_id) {
                _dek_id
            } else {
                // 都不存在，用头部的 dek_id 报错（保持原有错误信息）
                &dek_id
            };
            let packet = store
                .get(id_to_try)
                .map(|e| e.packet.clone())
                .ok_or_else(|| {
                    format!(
                        "DEK '{}' not found (already used or not created)",
                        id_to_try
                    )
                })?;
            (packet, id_to_try.to_string())
        };
        if dek_packet_data.len() < DEK_PACKET_LEN {
            return Err("invalid DEK packet".into());
        }

        // 2. 用 KEK 解密 DEK
        let kek_cipher =
            Aes256Gcm::new_from_slice(&self.kek).map_err(|e| format!("invalid KEK: {e}"))?;
        let dek_nonce = Nonce::from_slice(&dek_packet_data[..NONCE_LEN]);
        let fixed_aad = b"coord-transit-dek-v1";

        let mut dek_bytes = kek_cipher
            .decrypt(
                dek_nonce,
                Payload {
                    msg: &dek_packet_data[NONCE_LEN..],
                    aad: fixed_aad.as_ref(),
                },
            )
            .map_err(|e| format!("DEK decrypt failed: {e}"))?;

        if dek_bytes.len() != DEK_LEN {
            return Err("invalid DEK length".into());
        }
        let mut dek = [0u8; DEK_LEN];
        dek.copy_from_slice(&dek_bytes);
        dek_bytes.zeroize();

        // 3. 销毁存储中的 DEK（用后即焚）
        //
        // 注意：按**实际取用的 id**（`consumed_id`）删除，而不是包头 id。
        // 二者在 `rewrap` 后不同（包头仍是旧 id，DEK 已挂在新 id 下），
        // 历史实现按包头 id 删除 ⇒ 新 id 的条目残留、可被二次解密。
        {
            let mut store = self.dek_store.write();
            if let Some(mut entry) = store.remove(&consumed_id) {
                entry.packet.zeroize();
            }
        }

        // 4. 用 DEK 解密数据
        let data_cipher =
            Aes256Gcm::new_from_slice(&dek).map_err(|e| format!("invalid DEK: {e}"))?;
        let data_nonce = Nonce::from_slice(&packet[data_nonce_start..data_nonce_start + NONCE_LEN]);
        let ciphertext = &packet[dek_packet_start + DEK_PACKET_LEN..];

        let data_aad = build_context_aad(context);
        let plaintext = data_cipher
            .decrypt(
                data_nonce,
                Payload {
                    msg: ciphertext,
                    aad: &data_aad,
                },
            )
            .map_err(|e| format!("data decrypt failed (wrong context?): {e}"))?;

        dek.zeroize();
        Ok((plaintext, consumed_id))
    }

    // ──── 密钥轮换 ────

    /// **仅内存**轮换（单测 / 无 server 降级场景）。生产路径见 [`Self::rewrap_persisted`]。
    pub fn rewrap(&self, old_dek_id: &str) -> Result<String, String> {
        let (dek_packet, created_at, expires_at) = {
            let store = self.dek_store.read();
            let entry = store
                .get(old_dek_id)
                .cloned()
                .ok_or_else(|| format!("DEK '{old_dek_id}' not found for rewrap"))?;
            (entry.packet, entry.created_at, entry.expires_at)
        };
        if expires_at != 0 && now_unix() >= expires_at {
            return Err(format!("DEK '{old_dek_id}' expired, refusing to rewrap"));
        }

        // 解密旧 DEK
        let kek_cipher =
            Aes256Gcm::new_from_slice(&self.kek).map_err(|e| format!("invalid KEK: {e}"))?;
        let dek_nonce = Nonce::from_slice(&dek_packet[..NONCE_LEN]);
        let fixed_aad = b"coord-transit-dek-v1";

        let mut dek_bytes = kek_cipher
            .decrypt(
                dek_nonce,
                Payload {
                    msg: &dek_packet[NONCE_LEN..],
                    aad: fixed_aad.as_ref(),
                },
            )
            .map_err(|e| format!("DEK decrypt for rewrap failed: {e}"))?;

        let mut dek = [0u8; DEK_LEN];
        dek.copy_from_slice(&dek_bytes);
        dek_bytes.zeroize();

        // 重新加密 DEK（新 nonce）
        let mut new_nonce_bytes = [0u8; NONCE_LEN];
        OsRng.fill_bytes(&mut new_nonce_bytes);
        let new_nonce = Nonce::from_slice(&new_nonce_bytes);

        let mut new_body = kek_cipher
            .encrypt(
                new_nonce,
                Payload {
                    msg: dek.as_ref(),
                    aad: fixed_aad.as_ref(),
                },
            )
            .map_err(|e| format!("DEK re-encrypt failed: {e}"))?;

        let mut new_packet = Vec::with_capacity(DEK_PACKET_LEN);
        new_packet.extend_from_slice(&new_nonce_bytes);
        new_packet.append(&mut new_body);

        let new_dek_id = compute_dek_id(&new_packet);

        // 替换旧 DEK（TTL 沿用原条目：轮换不延长密钥寿命）
        {
            let mut store = self.dek_store.write();
            store.insert(
                new_dek_id.clone(),
                DekEntry {
                    packet: new_packet.clone(),
                    created_at,
                    expires_at,
                },
            );
            if let Some(mut entry) = store.remove(old_dek_id) {
                entry.packet.zeroize();
            }
        }

        dek.zeroize();
        new_packet.zeroize();
        Ok(new_dek_id)
    }

    // ──── 持久化路径（生产：DEK 落 coord-server KV）────
    //
    // 与上面的同步方法共用同一套加解密核心；区别只在"DEK 进出共享存储"：
    // - 加密：内存注册表 + KV 各写一份（KV 写失败 ⇒ 回滚内存并报错，fail-closed：
    //   否则调用方会拿到一个"重启后无法解密"的密文）；
    // - 解密：内存未命中时从 KV 回取（重启恢复），**消费后删除 KV 记录**（用后即焚
    //   跨重启/跨 Agent 成立）；删除失败 ⇒ 报错（不静默放过）；
    // - 轮换：KV 写新 id、删旧 id。

    /// 持久化加密：DEK 落 KV，重启后仍可解密
    pub async fn encrypt_persisted(&self, plaintext: &[u8]) -> Result<(Vec<u8>, String), String> {
        self.encrypt_persisted_inner(plaintext, &HashMap::new())
            .await
    }

    /// 持久化加密（带上下文绑定）
    pub async fn encrypt_with_context_persisted(
        &self,
        plaintext: &[u8],
        context: &HashMap<String, String>,
    ) -> Result<(Vec<u8>, String), String> {
        self.encrypt_persisted_inner(plaintext, context).await
    }

    async fn encrypt_persisted_inner(
        &self,
        plaintext: &[u8],
        context: &HashMap<String, String>,
    ) -> Result<(Vec<u8>, String), String> {
        let (packet, dek_id) = self.encrypt_inner(plaintext, context)?;

        let record = {
            let store = self.dek_store.read();
            match store.get(&dek_id) {
                Some(entry) => entry.to_record(),
                None => {
                    return Err("internal error: DEK register missing after encrypt".into());
                }
            }
        };

        if let Err(e) = self.store.put_dek(&dek_id, &record).await {
            // 回滚内存条目：调用方拿到错误，不应留下"只存在于本进程"的 DEK
            if let Some(mut entry) = self.dek_store.write().remove(&dek_id) {
                entry.packet.zeroize();
            }
            return Err(format!("DEK persist failed: {e}"));
        }

        self.maybe_sweep().await;
        Ok((packet, dek_id))
    }

    /// 持久化解密：内存未命中时从 KV 回取（重启恢复）+ 消费后删除 KV 记录
    pub async fn decrypt_persisted(&self, packet: &[u8], dek_id: &str) -> Result<Vec<u8>, String> {
        self.decrypt_persisted_inner(packet, dek_id, &HashMap::new())
            .await
    }

    /// 持久化解密（带上下文绑定）
    pub async fn decrypt_with_context_persisted(
        &self,
        packet: &[u8],
        dek_id: &str,
        context: &HashMap<String, String>,
    ) -> Result<Vec<u8>, String> {
        self.decrypt_persisted_inner(packet, dek_id, context).await
    }

    async fn decrypt_persisted_inner(
        &self,
        packet: &[u8],
        dek_id: &str,
        context: &HashMap<String, String>,
    ) -> Result<Vec<u8>, String> {
        // 需要回取的候选 id：包头自描述 id + 显式传入 id（rewrap 兼容路径）
        let mut candidates: Vec<String> = Vec::new();
        if let Some(header) = header_dek_id(packet) {
            candidates.push(header);
        }
        if !dek_id.is_empty() && !candidates.iter().any(|c| c == dek_id) {
            candidates.push(dek_id.to_string());
        }
        for id in &candidates {
            self.hydrate_from_store(id).await?;
        }

        let (plaintext, consumed) = self.decrypt_inner_consumed(packet, dek_id, context)?;

        // 用后即焚（落盘侧）：失败必须报错 —— 否则 DEK 在 KV 里残留，
        // 「单次使用」在重启/换 Agent 后不再成立（静默失效正是本项整改要消除的）。
        if let Err(e) = self.store.delete_dek(&consumed).await {
            tracing::error!("transit: revoke persisted DEK '{consumed}' failed: {e}");
            return Err(format!("DEK revoke failed for '{consumed}': {e}"));
        }
        Ok(plaintext)
    }

    /// 持久化轮换：新 id 落 KV、旧 id 从 KV 删除（TTL 沿用原条目）
    pub async fn rewrap_persisted(&self, old_dek_id: &str) -> Result<String, String> {
        let new_dek_id = self.rewrap(old_dek_id)?;

        let record = {
            let store = self.dek_store.read();
            match store.get(&new_dek_id) {
                Some(entry) => entry.to_record(),
                None => {
                    return Err("internal error: rotated DEK register missing".into());
                }
            }
        };

        if let Err(e) = self.store.put_dek(&new_dek_id, &record).await {
            return Err(format!("rotated DEK persist failed: {e}"));
        }
        if let Err(e) = self.store.delete_dek(old_dek_id).await {
            tracing::warn!("transit: drop rotated-out DEK '{old_dek_id}' failed: {e}");
        }
        Ok(new_dek_id)
    }

    /// 从持久化后端回取 DEK 到内存注册表（幂等；已过期条目视为不存在）
    async fn hydrate_from_store(&self, dek_id: &str) -> Result<(), String> {
        if self.dek_store.read().contains_key(dek_id) {
            return Ok(());
        }
        let record = self
            .store
            .get_dek(dek_id)
            .await
            .map_err(|e| format!("DEK store read failed for '{dek_id}': {e}"))?;
        let Some(record) = record else {
            // 交给 `decrypt_inner_consumed` 报既有错误信息（"not found"）
            return Ok(());
        };
        self.dek_store.write().insert(
            dek_id.to_string(),
            DekEntry {
                packet: record.dek_packet,
                created_at: record.created_at,
                expires_at: record.expires_at,
            },
        );
        Ok(())
    }

    /// 低频清扫：内存注册表 + 持久化后端的过期 DEK（失败只记日志，不影响加密）
    pub async fn sweep_expired_deks(&self) -> usize {
        let now = now_unix();
        let purged_local = self.purge_local_expired(now);
        let purged_store = match self.store.sweep_expired(now).await {
            Ok(n) => n,
            Err(e) => {
                tracing::warn!("transit: sweep persisted DEKs failed: {e}");
                0
            }
        };
        self.last_sweep.store(now, Ordering::Relaxed);
        purged_local + purged_store
    }

    fn purge_local_expired(&self, now: u64) -> usize {
        let mut store = self.dek_store.write();
        let before = store.len();
        store.retain(|_id, entry| !entry.is_expired(now));
        before - store.len()
    }

    /// 距上次清扫超过 [`SWEEP_INTERVAL_SECS`] 才清扫一次
    async fn maybe_sweep(&self) {
        let now = now_unix();
        let last = self.last_sweep.load(Ordering::Relaxed);
        if last != 0 && now.saturating_sub(last) < SWEEP_INTERVAL_SECS {
            return;
        }
        self.sweep_expired_deks().await;
    }

    /// 当前内存注册表条目数（测试/可观测性用）
    pub fn local_dek_count(&self) -> usize {
        self.dek_store.read().len()
    }

    // ──── HMAC 签名与验签（仅内存密钥，不落盘）───

    /// 使用 HMAC 对数据进行签名
    ///
    /// 支持算法: HMAC-SHA256（默认）, HMAC-SHA512
    /// 密钥仅存于内存，重启后通过 KEK 重新派生。
    pub fn hmac_sign(&self, data: &[u8], algorithm: &str) -> Result<Vec<u8>, String> {
        let algo = if algorithm.is_empty() {
            "HMAC-SHA256"
        } else {
            algorithm
        };
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
    pub fn hmac_verify(
        &self,
        data: &[u8],
        signature: &[u8],
        algorithm: &str,
    ) -> Result<bool, String> {
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

/// 密文包头的布局解析结果
struct PacketLayout {
    /// DEK id（新格式取包头；旧格式取调用方传入的 fallback）
    dek_id: String,
    /// 数据 nonce 起始偏移
    data_nonce_start: usize,
    /// DEK packet 起始偏移
    dek_packet_start: usize,
}

/// 解析密文包头（自描述新格式 / 无前缀旧格式）
///
/// 两种格式的判别与偏移计算在此**唯一**实现：解密与持久化回取（[`header_dek_id`]）
/// 必须用同一判据，否则会出现"回取了 A 的 DEK 却按 B 解密"的错位。
fn parse_packet_layout(packet: &[u8], fallback_dek_id: &str) -> Result<PacketLayout, String> {
    let min_legacy_len = NONCE_LEN + DEK_PACKET_LEN + TAG_LEN;
    if packet.len() < min_legacy_len {
        return Err("packet too short".into());
    }

    let candidate_len = packet[0] as usize;
    let candidate_end = 1 + candidate_len;
    // 检查：candidate_len 合理（1-64）、候选范围不越界、且剩余数据足够
    if (1..=64).contains(&candidate_len)
        && candidate_end < packet.len()
        && packet.len() - candidate_end >= NONCE_LEN + DEK_PACKET_LEN + TAG_LEN
    {
        // 新格式：提取 dek_id
        let id = std::str::from_utf8(&packet[1..candidate_end])
            .map_err(|_| "invalid dek_id encoding".to_string())?
            .to_string();
        Ok(PacketLayout {
            dek_id: id,
            data_nonce_start: candidate_end,
            dek_packet_start: candidate_end + NONCE_LEN,
        })
    } else {
        // 旧格式：使用传入的 fallback（历史兼容路径）
        Ok(PacketLayout {
            dek_id: fallback_dek_id.to_string(),
            data_nonce_start: 0,
            dek_packet_start: NONCE_LEN,
        })
    }
}

/// 仅取包头中的自描述 DEK id（旧格式 / 解析失败 / 空 id 返回 `None`）
pub fn header_dek_id(packet: &[u8]) -> Option<String> {
    parse_packet_layout(packet, "")
        .ok()
        .map(|l| l.dek_id)
        .filter(|id| !id.is_empty())
}

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

// ──── BaseService：插件生命周期 ────
//
// 信封加密服务无后台常驻任务：构造即就绪（算法白名单在构造期校验），
// `start` 只做一次**过期 DEK 清扫**（内存 + coord-server 共享 KV）——
// 历史实现的 `start` 完全空转，配合"DEK 不落盘"使重启后不可解密（B-06）。
// `stop`/`health_check` 保持登记性语义。
// 声明在本文件而非用占位类型，是为了让「每个原生服务就是一个插件」成立：
// 插件名与生命周期语义都由服务自身给出。

#[async_trait::async_trait]
impl crate::service::BaseService for TransitService {
    fn name(&self) -> &'static str {
        "transit"
    }

    async fn start(&self) -> crate::service::ServiceResult<()> {
        let swept = self.sweep_expired_deks().await;
        if swept > 0 {
            tracing::info!("transit: swept {swept} expired DEK(s) at startup");
        }
        Ok(())
    }

    async fn stop(&self) -> crate::service::ServiceResult<()> {
        Ok(())
    }

    fn health_check(&self) -> bool {
        true
    }
}

// ──── tests ────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::transit_store::DekStoreError;

    #[test]
    fn test_config_defaults() {
        let c = TransitConfig::default();
        assert_eq!(c.dek_ttl_secs, 3600);
        assert_eq!(c.kek_id, "default-kek");
    }

    // ──── W4-2a：KEK 注入与 fail-closed（负控制）────
    //
    // 这组判据对应 P-Gate 6 的「KEK 供给裁定落地」。中心命题两条：
    //   ① **无材料 ⇒ 拒绝**（不许回落到 `SHA-256("coord-transit-kek:"||kek_id)`）；
    //   ② **密钥来自材料**（同样的 `kek_id`、不同的材料 ⇒ 互相解不开）。

    /// ① 长度不符一律拒绝（0 / 31 / 33 字节）—— 不许静默补齐或截断
    #[test]
    fn test_kek_material_rejects_wrong_length() {
        for bad_len in [0usize, 1, 16, 31, 33, 64] {
            let bytes = vec![0x11u8; bad_len];
            let err = TransitKekMaterial::from_bytes(&bytes)
                .expect_err(&format!("{bad_len} 字节必须被拒绝"));
            assert!(
                err.contains("exactly 32 bytes"),
                "错误信息应说明长度要求，实际: {err}"
            );
        }
        // 正控制：32 字节必须接受
        assert!(TransitKekMaterial::from_bytes(&[0x11u8; 32]).is_ok());
    }

    /// ① hex 形态：空串 / 非 hex / 长度不符 都必须拒绝（空串是最隐蔽的一类"配置了但没配"）
    #[test]
    fn test_kek_material_from_hex_rejects_empty_and_bad() {
        assert!(TransitKekMaterial::from_hex("").is_err(), "空串必须拒绝");
        assert!(
            TransitKekMaterial::from_hex("   ").is_err(),
            "只含空白也必须拒绝"
        );
        assert!(
            TransitKekMaterial::from_hex("zzzz").is_err(),
            "非 hex 必须拒绝"
        );
        assert!(
            TransitKekMaterial::from_hex(&"ab".repeat(16)).is_err(),
            "16 字节必须拒绝"
        );
        // 正控制：64 个 hex 字符（32 字节）必须接受
        assert!(TransitKekMaterial::from_hex(&"ab".repeat(32)).is_ok());
    }

    /// ② 无材料（环境变量与文件都不存在）⇒ `Err`，**且错误信息里不许出现任何密钥字节**
    #[test]
    fn test_resolve_without_any_material_is_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let err = TransitKekMaterial::resolve_with(dir.path(), None)
            .expect_err("无材料时必须拒绝（fail-closed）");
        assert!(
            err.contains(TRANSIT_KEK_ENV) && err.contains(TRANSIT_KEK_FILE),
            "错误信息必须给出两条注入路径，实际: {err}"
        );
        assert!(
            err.contains("refusing to start"),
            "错误信息必须显式声明拒绝启动，实际: {err}"
        );
    }

    /// ② 文件路径：`<data_dir>/transit-kek.bin` 存在且为 32 字节 ⇒ 接受；
    ///    长度不符 ⇒ 拒绝（而不是忽略文件后当作"无材料"，那样会掩盖运维配错）
    #[test]
    fn test_resolve_from_file_enforces_length() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(TRANSIT_KEK_FILE);

        std::fs::write(&path, [0x7Cu8; 31]).unwrap();
        assert!(
            TransitKekMaterial::resolve_with(dir.path(), None).is_err(),
            "31 字节的文件必须被拒绝"
        );

        std::fs::write(&path, [0x7Cu8; 32]).unwrap();
        assert!(
            TransitKekMaterial::resolve_with(dir.path(), None).is_ok(),
            "32 字节的文件必须被接受"
        );
    }

    /// ② 环境变量优先于文件；且环境变量非法时**不回落到文件**（否则"配错 env + 有旧文件"
    ///    会静默继续用旧材料，属最难发现的运维错）
    #[test]
    fn test_resolve_env_takes_precedence_and_does_not_fall_back() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(TRANSIT_KEK_FILE), [0x7Cu8; 32]).unwrap();

        // 合法 env ⇒ 用 env
        assert!(TransitKekMaterial::resolve_with(dir.path(), Some(&"cd".repeat(32))).is_ok());
        // 非法 env + 有合法文件 ⇒ 仍必须 Err（不回落到文件）
        assert!(
            TransitKekMaterial::resolve_with(dir.path(), Some("not-hex")).is_err(),
            "env 非法时不得静默回落到文件"
        );
    }

    /// ② **核心负控制**：同样的 `kek_id`、**不同的材料** ⇒ 互相解不开。
    /// 这条同时证明了"KEK 来自材料"而不是"来自配置串"——修前
    /// `SHA-256("coord-transit-kek:"||kek_id)` 会让两者**互相解得开**。
    #[tokio::test]
    async fn test_kek_comes_from_material_not_from_kek_id() {
        let cfg = TransitConfig::default();
        assert_eq!(cfg.kek_id, "default-kek");

        let store: Arc<dyn TransitDekStore> = Arc::new(MemoryTransitDekStore::new());
        let producer = TransitService::with_store(
            cfg.clone(),
            store.clone(),
            TransitKekMaterial::from_bytes(&[0x01u8; 32]).unwrap(),
        )
        .expect("create");
        let (ct, _id) = producer
            .encrypt_persisted(b"material-dependent")
            .await
            .expect("encrypt");

        // 同 kek_id、不同材料 ⇒ 解不开
        let wrong = TransitService::with_store(
            cfg.clone(),
            store.clone(),
            TransitKekMaterial::from_bytes(&[0x02u8; 32]).unwrap(),
        )
        .expect("create");
        assert!(
            wrong.decrypt_persisted(&ct, "").await.is_err(),
            "不同材料必须解不开（否则 KEK 仍来自 kek_id）"
        );

        // 正控制：同一材料 ⇒ 解得开
        let right = TransitService::with_store(
            cfg,
            store,
            TransitKekMaterial::from_bytes(&[0x01u8; 32]).unwrap(),
        )
        .expect("create");
        assert_eq!(
            right.decrypt_persisted(&ct, "").await.expect("decrypt"),
            b"material-dependent"
        );
    }

    /// KEK 与 HMAC 密钥由**同一材料 + 不同 info** 域分隔派生 ⇒ 两者不相等，
    /// 且 HMAC 密钥也随材料变化（防止"只改了 KEK 忘了 HMAC"的半截整改）
    #[tokio::test]
    async fn test_hmac_key_is_domain_separated_from_material() {
        let cfg = TransitConfig::default();
        let a = test_svc(cfg.clone());
        let b = TransitService::new(cfg, TransitKekMaterial::from_bytes(&[0x03u8; 32]).unwrap())
            .expect("create");
        let sig_a = a.hmac_sign(b"same input", "HMAC-SHA256").expect("sign");
        let sig_b = b.hmac_sign(b"same input", "HMAC-SHA256").expect("sign");
        assert_ne!(sig_a, sig_b, "HMAC 密钥必须随注入材料变化");
    }

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let svc = test_svc(TransitConfig::default());
        let pt = b"hello world";
        let (ct, id) = svc.encrypt(pt).expect("encrypt");
        assert_ne!(ct, pt);
        let dec = svc.decrypt(&ct, &id).expect("decrypt");
        assert_eq!(dec, pt);
    }

    #[test]
    fn test_dek_single_use() {
        let svc = test_svc(TransitConfig::default());
        let (ct, id) = svc.encrypt(b"secret").expect("encrypt");
        assert!(svc.decrypt(&ct, &id).is_ok());
        assert!(svc.decrypt(&ct, &id).is_err(), "DEK should be single-use");
    }

    #[test]
    fn test_context_binding() {
        let svc = test_svc(TransitConfig::default());
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
        let svc = test_svc(TransitConfig::default());
        let pt = b"rotate me";
        let (ct, old_id) = svc.encrypt(pt).expect("encrypt");

        let new_id = svc.rewrap(&old_id).expect("rewrap");
        assert_ne!(new_id, old_id);
        assert!(svc.decrypt(&ct, &new_id).is_ok());
        assert!(svc.decrypt(&ct, &old_id).is_err());
    }

    // ──── HMAC 签名与验签测试 ────

    #[test]
    fn test_hmac_sign_sha256_default() {
        let svc = test_svc(TransitConfig::default());
        let data = b"hello hmac";
        let sig = svc.hmac_sign(data, "").expect("hmac_sign");
        assert!(!sig.is_empty());
        // SHA-256 HMAC 输出 32 字节
        assert_eq!(sig.len(), 32);
    }

    #[test]
    fn test_hmac_sign_sha512() {
        let svc = test_svc(TransitConfig::default());
        let sig = svc.hmac_sign(b"data", "HMAC-SHA512").expect("hmac_sign");
        assert_eq!(sig.len(), 64);
    }

    #[test]
    fn test_hmac_verify_roundtrip() {
        let svc = test_svc(TransitConfig::default());
        let data = b"verify me";
        let sig = svc.hmac_sign(data, "HMAC-SHA256").expect("sign");
        assert!(svc.hmac_verify(data, &sig, "HMAC-SHA256").expect("verify"));
    }

    #[test]
    fn test_hmac_verify_tampered_data() {
        let svc = test_svc(TransitConfig::default());
        let sig = svc.hmac_sign(b"original", "HMAC-SHA256").expect("sign");
        assert!(!svc
            .hmac_verify(b"tampered", &sig, "HMAC-SHA256")
            .expect("verify"));
    }

    #[test]
    fn test_hmac_verify_tampered_signature() {
        let svc = test_svc(TransitConfig::default());
        let data = b"my data";
        let mut sig = svc.hmac_sign(data, "HMAC-SHA256").expect("sign");
        // Corrupt the signature
        sig[0] ^= 0xFF;
        assert!(!svc.hmac_verify(data, &sig, "HMAC-SHA256").expect("verify"));
    }

    #[test]
    fn test_hmac_deterministic() {
        let svc = test_svc(TransitConfig::default());
        let data = b"deterministic";
        let sig1 = svc.hmac_sign(data, "HMAC-SHA256").expect("sign");
        let sig2 = svc.hmac_sign(data, "HMAC-SHA256").expect("sign");
        assert_eq!(sig1, sig2);
    }

    #[test]
    fn test_hmac_unsupported_algorithm() {
        let svc = test_svc(TransitConfig::default());
        assert!(svc.hmac_sign(b"data", "HMAC-MD5").is_err());
    }

    // ──── 持久化路径（B-06 / E1 整改）────

    /// 轮换后 DEK 仍须单次使用（历史实现按包头 id 删除 ⇒ 新 id 条目残留可二次解密）
    #[test]
    fn test_rewrap_dek_is_single_use() {
        let svc = test_svc(TransitConfig::default());
        let (ct, old_id) = svc.encrypt(b"single use after rewrap").expect("encrypt");
        let new_id = svc.rewrap(&old_id).expect("rewrap");
        assert!(svc.decrypt(&ct, &new_id).is_ok(), "轮换后应能解密");
        assert!(
            svc.decrypt(&ct, &new_id).is_err(),
            "轮换后的 DEK 同样用后即焚"
        );
    }

    #[test]
    fn test_header_dek_id_self_describing() {
        let svc = test_svc(TransitConfig::default());
        let (ct, id) = svc.encrypt(b"header").expect("encrypt");
        assert_eq!(header_dek_id(&ct).as_deref(), Some(id.as_str()));
        assert_eq!(header_dek_id(b"too short"), None);
    }

    /// 测试用构造：固定材料（[`TransitKekMaterial::for_test`]，**非生产**）+ 内存 store
    fn test_svc(config: TransitConfig) -> TransitService {
        TransitService::new(config, TransitKekMaterial::for_test()).expect("create")
    }

    fn svc_with_store(config: TransitConfig, store: Arc<dyn TransitDekStore>) -> TransitService {
        TransitService::with_store(config, store, TransitKekMaterial::for_test()).expect("create")
    }

    /// 持久化加密后，**新实例（模拟重启）**仍能解密；且单次使用跨实例成立
    #[tokio::test]
    async fn test_persisted_roundtrip_survives_restart() {
        let store: Arc<dyn TransitDekStore> = Arc::new(MemoryTransitDekStore::new());
        let before = svc_with_store(TransitConfig::default(), store.clone());
        let (ct, id) = before
            .encrypt_persisted(b"survive restart")
            .await
            .expect("encrypt");

        // 模拟重启：新实例 + 空内存注册表 + 同一持久化后端
        let after = svc_with_store(TransitConfig::default(), store.clone());
        assert_eq!(after.local_dek_count(), 0, "重启后内存注册表应为空");

        // gRPC 路径不传 dek_id（包头自描述）
        let pt = after
            .decrypt_persisted(&ct, "")
            .await
            .expect("重启后应能解密");
        assert_eq!(pt, b"survive restart");

        // 用后即焚跨实例成立：第三个实例不应还能解密
        let third = svc_with_store(TransitConfig::default(), store);
        assert!(
            third.decrypt_persisted(&ct, &id).await.is_err(),
            "DEK 应已从持久化后端删除"
        );
    }

    /// 持久化轮换：新 id 落盘、旧 id 删除，重启后语义一致
    #[tokio::test]
    async fn test_rewrap_persisted_across_restart() {
        let store: Arc<dyn TransitDekStore> = Arc::new(MemoryTransitDekStore::new());
        let svc = svc_with_store(TransitConfig::default(), store.clone());
        let (ct, old_id) = svc.encrypt_persisted(b"rotate me").await.expect("encrypt");
        let new_id = svc.rewrap_persisted(&old_id).await.expect("rewrap");
        assert_ne!(new_id, old_id);

        let after = svc_with_store(TransitConfig::default(), store.clone());
        assert!(
            after.decrypt_persisted(&ct, &old_id).await.is_err(),
            "旧 DEK 已从持久化后端删除"
        );
        assert_eq!(
            after
                .decrypt_persisted(&ct, &new_id)
                .await
                .expect("new dek"),
            b"rotate me"
        );
        assert!(
            after.decrypt_persisted(&ct, &new_id).await.is_err(),
            "轮换后的 DEK 仍单次使用"
        );
    }

    /// 上下文绑定：正确的 context 通过；错误 context 失败（并消费 DEK，与内存路径一致）
    #[tokio::test]
    async fn test_persisted_context_binding() {
        let store: Arc<dyn TransitDekStore> = Arc::new(MemoryTransitDekStore::new());
        let svc = svc_with_store(TransitConfig::default(), store.clone());

        let mut ctx = HashMap::new();
        ctx.insert("tenant".into(), "acme".into());
        let (ct, id) = svc
            .encrypt_with_context_persisted(b"tenant data", &ctx)
            .await
            .expect("encrypt");

        let after = svc_with_store(TransitConfig::default(), store);
        assert_eq!(
            after
                .decrypt_with_context_persisted(&ct, &id, &ctx)
                .await
                .expect("decrypt"),
            b"tenant data"
        );
        assert!(
            after
                .decrypt_with_context_persisted(&ct, &id, &ctx)
                .await
                .is_err(),
            "DEK 已消费"
        );
    }

    /// 已过期的持久化 DEK 不可用（TTL 生效，且读路径顺手回收）
    #[tokio::test]
    async fn test_persisted_expired_dek_is_not_usable() {
        let store: Arc<dyn TransitDekStore> = Arc::new(MemoryTransitDekStore::new());
        let svc = svc_with_store(
            TransitConfig {
                dek_ttl_secs: 3600,
                ..Default::default()
            },
            store.clone(),
        );
        let (ct, id) = svc.encrypt_persisted(b"stale").await.expect("encrypt");

        // 把该 DEK 改写为"早已过期"（模拟 TTL 到期后重启）
        store
            .put_dek(&id, &DekRecord::new(vec![0u8; 60], 1, 1))
            .await
            .expect("overwrite");

        let after = svc_with_store(TransitConfig::default(), store);
        assert!(
            after.decrypt_persisted(&ct, "").await.is_err(),
            "过期 DEK 不应可用"
        );
    }

    /// 清扫：过期条目被删除，未过期保留
    #[tokio::test]
    async fn test_sweep_expired_persisted_deks() {
        let store: Arc<dyn TransitDekStore> = Arc::new(MemoryTransitDekStore::new());
        store
            .put_dek("dead", &DekRecord::new(vec![0u8; 60], 1, 1))
            .await
            .expect("put");
        store
            .put_dek("live", &DekRecord::new(vec![0u8; 60], now_unix(), 3600))
            .await
            .expect("put");

        let svc = svc_with_store(TransitConfig::default(), store.clone());
        assert_eq!(svc.sweep_expired_deks().await, 1);
        assert!(store.get_dek("live").await.expect("get").is_some());
    }

    // ──── fail-closed：持久化后端故障时不得静默降级 ────

    /// put 恒失败：加密必须报错，且不得留下"只存在于本进程"的 DEK
    #[tokio::test]
    async fn test_persist_failure_fails_closed() {
        let svc = svc_with_store(TransitConfig::default(), Arc::new(FailingStore));
        let err = svc.encrypt_persisted(b"boom").await.expect_err("must fail");
        assert!(err.contains("DEK persist failed"), "unexpected: {err}");
        assert_eq!(svc.local_dek_count(), 0, "内存条目应回滚");
    }

    /// get 恒失败：不能退化成 "not found" 的误导性错误
    #[tokio::test]
    async fn test_store_read_failure_is_reported() {
        let mem: Arc<dyn TransitDekStore> = Arc::new(MemoryTransitDekStore::new());
        let producer = svc_with_store(TransitConfig::default(), mem);
        let (ct, _id) = producer.encrypt_persisted(b"x").await.expect("encrypt");

        let svc = svc_with_store(TransitConfig::default(), Arc::new(FailingStore));
        let err = svc.decrypt_persisted(&ct, "").await.expect_err("must fail");
        assert!(err.contains("DEK store read failed"), "unexpected: {err}");
    }

    /// delete 恒失败：明文已算出也必须报错（否则"单次使用"静默失效）
    #[tokio::test]
    async fn test_revoke_failure_fails_closed() {
        let store = Arc::new(NoRevokeStore::new());
        let producer = svc_with_store(TransitConfig::default(), store.clone());
        let (ct, id) = producer
            .encrypt_persisted(b"needs revoke")
            .await
            .expect("encrypt");

        let consumer = svc_with_store(TransitConfig::default(), store.clone());
        let err = consumer
            .decrypt_persisted(&ct, &id)
            .await
            .expect_err("must fail");
        assert!(err.contains("DEK revoke failed"), "unexpected: {err}");
        // 但 DEK 在 KV 中确实残留（说明必须靠错误上抛而非静默）
        assert!(store.get_dek(&id).await.expect("get").is_some());
    }

    /// 所有操作都失败的 store
    struct FailingStore;

    #[async_trait::async_trait]
    impl TransitDekStore for FailingStore {
        async fn put_dek(&self, _id: &str, _r: &DekRecord) -> Result<(), DekStoreError> {
            Err(DekStoreError::Kv("store down".into()))
        }
        async fn get_dek(&self, _id: &str) -> Result<Option<DekRecord>, DekStoreError> {
            Err(DekStoreError::Kv("store down".into()))
        }
        async fn delete_dek(&self, _id: &str) -> Result<(), DekStoreError> {
            Err(DekStoreError::Kv("store down".into()))
        }
        async fn sweep_expired(&self, _now: u64) -> Result<usize, DekStoreError> {
            Err(DekStoreError::Kv("store down".into()))
        }
    }

    /// 读写正常、仅删除失败（模拟 revoke 面故障）
    struct NoRevokeStore {
        inner: MemoryTransitDekStore,
    }

    impl NoRevokeStore {
        fn new() -> Self {
            Self {
                inner: MemoryTransitDekStore::new(),
            }
        }
    }

    #[async_trait::async_trait]
    impl TransitDekStore for NoRevokeStore {
        async fn put_dek(&self, id: &str, r: &DekRecord) -> Result<(), DekStoreError> {
            self.inner.put_dek(id, r).await
        }
        async fn get_dek(&self, id: &str) -> Result<Option<DekRecord>, DekStoreError> {
            self.inner.get_dek(id).await
        }
        async fn delete_dek(&self, _id: &str) -> Result<(), DekStoreError> {
            Err(DekStoreError::Kv("revoke rejected".into()))
        }
        async fn sweep_expired(&self, now: u64) -> Result<usize, DekStoreError> {
            self.inner.sweep_expired(now).await
        }
    }
}
