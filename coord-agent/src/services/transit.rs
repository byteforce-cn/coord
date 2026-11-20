// coord-agent: Transit —— KEK 材料集 + 用途化 HMAC + 命名密钥注册表入口
//
// 本模块保留两类**与命名密钥无关**的基础设施：
// - KEK 材料注入与轮换窗口（`TransitKekKeyring`）：命名密钥档案的版本材料
//   与 CCT3 的 DEK 都由它包裹（ADR-0019 D3，软件 KEK 不变）；
// - 用途化 HMAC（ADR-0012）：`purpose` + `key_id`，与命名密钥是两套模型。
//
// 命名密钥的档案、版本生命周期、签名与按名加密在 `transit_key.rs`；
// 存储与 CAS 在 `transit_key_store.rs`。gRPC 面在 `grpc_handlers.rs`。
//
// KEK 供给（材料注入；**禁止**由 `kek_id` 确定性派生）：
// - 环境变量 `COORD_TRANSIT_KEK`（hex64），或 `<agent data_dir>/transit-kek.bin`
//   （32 字节原始材料，0600）
// - 历史材料经 `COORD_TRANSIT_KEK_OLD`（`kek_id:hex64[,...]`）或
//   `<data_dir>/transit-kek-old.txt` 注入；格式非法即拒绝启动
// - fail-closed：主材料缺失/长度不符 ⇒ 构造失败 ⇒ agent 拒绝启动
//
// 边界（不要读作"密钥已妥善托管"）：本方案不是外部 KMS；材料以文件/环境形态落在
// agent 主机上，主机被控 ⇒ KEK 泄露（boundaries.md B-SE-2）。

use std::path::Path;
use std::sync::Arc;

use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use sha2::{Sha256, Sha512};
use zeroize::Zeroizing;

use super::transit_key::TransitKeyRegistry;
use super::transit_key_store::{MemoryTransitKeyStore, TransitKeyStore};

// ──── 公共类型 ────

/// Transit 服务配置
///
/// 注意：**不含密钥材料**。KEK 材料见 [`TransitKekMaterial`]（启动注入）。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TransitConfig {
    /// KEK 标识符 —— **仅作 HKDF 域分隔与审计标签**（不再是密钥来源）
    pub kek_id: String,
}

impl Default for TransitConfig {
    fn default() -> Self {
        Self {
            kek_id: "default-kek".into(),
        }
    }
}

impl TransitConfig {
    /// 读取主材料标识（`COORD_TRANSIT_KEK_ID`；未设置 / 空白 ⇒ `default-kek`）。
    pub fn from_env() -> Self {
        Self {
            kek_id: kek_id_from_env_value(std::env::var(TRANSIT_KEK_ID_ENV).ok().as_deref()),
        }
    }
}

/// 环境值 → 材料标识（空白视为未设置：不把不可见字符当标识）
fn kek_id_from_env_value(raw: Option<&str>) -> String {
    raw.map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| TransitConfig::default().kek_id)
}

/// 静态加密 KEK 长度（256-bit）
const KEK_MATERIAL_LEN: usize = 32;

/// 环境变量名：注入 KEK 密钥材料（hex64）
pub const TRANSIT_KEK_ENV: &str = "COORD_TRANSIT_KEK";

/// 数据目录内的密钥材料文件名（32 字节原始材料）
pub const TRANSIT_KEK_FILE: &str = "transit-kek.bin";

/// 历史材料环境变量（`kek_id:hex64[,kek_id:hex64...]`；G-TR-1 多材料解密窗口）
pub const TRANSIT_KEK_OLD_ENV: &str = "COORD_TRANSIT_KEK_OLD";

/// 历史材料文件名（每行 `kek_id hex64`；`#` 注释）
pub const TRANSIT_KEK_OLD_FILE: &str = "transit-kek-old.txt";

/// 主材料标识环境变量（域分隔 / 审计标签；**轮换时随材料换代**）。
///
/// 同一标识只能对应一份材料（历史集合拒绝与主 id 重复的条目）；标识不换代，
/// 旧记录 / 旧签名会落到新材料上，窗口命中与 fail-loud 都失效。
pub const TRANSIT_KEK_ID_ENV: &str = "COORD_TRANSIT_KEK_ID";

/// dev 模式默认 KEK 材料（`coord dev` 专用；见 ADR-0009）。
///
/// 固定值 ⇒ 只能用于本地开发：任何拿到源码的人都可解密用它加密的数据。
/// 进程内**仅**经 `AgentServer::with_dev_default_transit_kek(true)` 的调用链
/// （`coord dev`）在「env / 文件材料均缺失」时使用；`agent` 子命令 /
/// `agent.toml` 不可达，生产路径缺材料仍 fail-closed（`resolve` 口径逐字不变）。
pub const DEV_DEFAULT_KEK: [u8; KEK_MATERIAL_LEN] = *b"coord-dev-mode-insecure-kek-0000";

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
    /// 原始材料字节（仅 crate 内用于包裹/派生；不实现 `Debug`/`Display`）
    pub(crate) fn as_bytes(&self) -> &[u8; KEK_MATERIAL_LEN] {
        &self.0
    }

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

// ──── 多材料（G-TR-1）────

/// KEK 材料集合：主材料 + 历史材料（多材料并存解密窗口，G-TR-1）。
///
/// - 加密/回包只使用**主材料**；
/// - 解密按 DEK 记录 / 密文自带的材料标识选材料；无标识（旧格式旧记录）按
///   「主 → 历史」逐材料试解；
/// - 历史材料承担**解密窗口**与按显式 `key_id` 的 HMAC 复算（ADR-0012；HMAC 迁移
///   由消费方按新主材料重散列，`Rewrap` 只迁移 DEK）：清空历史材料前必须先 rewrap
///   全部存量 DEK（演练与回退路径见 `docs/production/ops/runbook.md` §4.4）。
#[derive(Debug)]
pub struct TransitKekKeyring {
    primary_id: String,
    primary: TransitKekMaterial,
    historical: Vec<(String, TransitKekMaterial)>,
}

impl TransitKekKeyring {
    /// 主材料标识校验：非空、不含 NUL、≤255 字节。
    ///
    /// DEK 包头用 1 字节长度写入标识 ⇒ 超长必须提前到构造期 fail-closed
    /// （否则要到加密期才报错，实例已带着非法配置启动）。
    fn validate_primary_kek_id(id: &str) -> Result<(), String> {
        if id.trim().is_empty() {
            return Err("primary KEK id must not be empty".into());
        }
        if id.contains('\0') {
            return Err("primary KEK id must not contain NUL".into());
        }
        if id.len() > u8::MAX as usize {
            return Err(format!("primary KEK id must not exceed {} bytes", u8::MAX));
        }
        Ok(())
    }

    /// 单主材料集合（无历史；dev / 单测 / 兼容构造；标识非法 ⇒ Err）
    pub fn single(primary_id: &str, primary: TransitKekMaterial) -> Result<Self, String> {
        Self::validate_primary_kek_id(primary_id)?;
        Ok(Self {
            primary_id: primary_id.to_string(),
            primary,
            historical: Vec::new(),
        })
    }

    /// 解析材料集合（主材料 fail-closed；历史材料可选）：
    ///
    /// - 主：`COORD_TRANSIT_KEK` hex64 → `<data_dir>/transit-kek.bin`（口径不变）；
    /// - 历史：`COORD_TRANSIT_KEK_OLD` 或 `<data_dir>/transit-kek-old.txt`
    ///   （格式见常量文档；任一历史条目非法 ⇒ 拒绝启动，不静默跳过）。
    pub fn resolve(data_dir: &Path, primary_id: &str) -> Result<Self, String> {
        let primary = TransitKekMaterial::resolve(data_dir)?;
        Self::resolve_with(data_dir, primary_id, primary)
    }

    /// [`Self::resolve`] 的确定性内核：环境变量经 [`Self::resolve_with_env`] 注入。
    pub fn resolve_with(
        data_dir: &Path,
        primary_id: &str,
        primary: TransitKekMaterial,
    ) -> Result<Self, String> {
        Self::resolve_with_env(
            data_dir,
            primary_id,
            primary,
            std::env::var(TRANSIT_KEK_OLD_ENV).ok(),
        )
    }

    /// 同 [`Self::resolve_with`]，历史材料环境变量显式传入（测试用；
    /// 进程级 env 在测试并行下不确定）。
    pub fn resolve_with_env(
        data_dir: &Path,
        primary_id: &str,
        primary: TransitKekMaterial,
        old_env_value: Option<String>,
    ) -> Result<Self, String> {
        Self::validate_primary_kek_id(primary_id)?;
        let mut historical: Vec<(String, TransitKekMaterial)> = Vec::new();

        match old_env_value {
            Some(v) if !v.trim().is_empty() => {
                for entry in v.split([',', ';']) {
                    let entry = entry.trim();
                    if entry.is_empty() {
                        continue;
                    }
                    historical.push(Self::parse_old_entry(entry)?);
                }
            }
            _ => {
                let path = data_dir.join(TRANSIT_KEK_OLD_FILE);
                if path.exists() {
                    let content = std::fs::read_to_string(&path)
                        .map_err(|e| format!("read {}: {e}", path.display()))?;
                    for line in content.lines() {
                        let line = line.trim();
                        if line.is_empty() || line.starts_with('#') {
                            continue;
                        }
                        historical.push(Self::parse_old_entry(line)?);
                    }
                }
            }
        }

        // 同一标识只能对应一份材料：主材料 id 出现在历史 / 历史重复 id ⇒ 拒绝
        if historical.iter().any(|(id, _)| id == primary_id) {
            return Err(format!(
                "historical KEK id '{primary_id}' duplicates the primary id"
            ));
        }
        let mut seen = std::collections::HashSet::new();
        for (id, _) in &historical {
            if !seen.insert(id.clone()) {
                return Err(format!("duplicate historical KEK id '{id}'"));
            }
        }

        Ok(Self {
            primary_id: primary_id.to_string(),
            primary,
            historical,
        })
    }

    /// `kek_id:hex64` / `kek_id<空白>hex64` 单条历史材料
    fn parse_old_entry(entry: &str) -> Result<(String, TransitKekMaterial), String> {
        let (id, hex) = entry.split_once([':', ' ', '\t']).ok_or_else(|| {
            format!("invalid historical KEK entry '{entry}' (expect `kek_id:hex64`)")
        })?;
        let id = id.trim();
        let hex = hex.trim();
        if id.is_empty() {
            return Err("historical KEK id must not be empty".into());
        }
        let material = TransitKekMaterial::from_hex(hex)?;
        Ok((id.to_string(), material))
    }

    pub fn primary_id(&self) -> &str {
        &self.primary_id
    }

    pub fn primary(&self) -> &TransitKekMaterial {
        &self.primary
    }

    pub fn historical(&self) -> &[(String, TransitKekMaterial)] {
        &self.historical
    }

    /// 按标识取材料（主或历史；未注入返回 None）
    pub fn material_for(&self, kek_id: &str) -> Option<&TransitKekMaterial> {
        if kek_id == self.primary_id {
            Some(&self.primary)
        } else {
            self.historical
                .iter()
                .find(|(id, _)| id == kek_id)
                .map(|(_, m)| m)
        }
    }
}

/// `purpose` 字节上限（含多字节 UTF-8；不 trim）
pub const HMAC_PURPOSE_MAX_LEN: usize = 128;

/// 用途标签校验（ADR-0012）：非空、不含 NUL、字节长度 ≤128
fn validate_hmac_purpose(purpose: &str) -> Result<(), String> {
    if purpose.is_empty() {
        return Err("invalid purpose: must not be empty".into());
    }
    if purpose.len() > HMAC_PURPOSE_MAX_LEN {
        return Err(format!(
            "invalid purpose: must not exceed {HMAC_PURPOSE_MAX_LEN} bytes"
        ));
    }
    if purpose.contains('\0') {
        return Err("invalid purpose: must not contain NUL".into());
    }
    Ok(())
}

/// 用途化 HMAC 密钥（ADR-0012 规范形式；段长前缀编码消除歧义）
fn derive_hmac_key(
    material: &TransitKekMaterial,
    kek_id: &str,
    purpose: &str,
) -> Result<[u8; DEK_LEN], String> {
    let mut info = Vec::with_capacity(24 + 4 + kek_id.len() + 4 + purpose.len());
    info.extend_from_slice(b"coord-transit-hmac-v1:");
    info.extend_from_slice(&(kek_id.len() as u32).to_be_bytes());
    info.extend_from_slice(kek_id.as_bytes());
    info.extend_from_slice(&(purpose.len() as u32).to_be_bytes());
    info.extend_from_slice(purpose.as_bytes());
    let mut out = [0u8; DEK_LEN];
    Hkdf::<Sha256>::new(None, &*material.0)
        .expand(&info, &mut out)
        .map_err(|e| format!("HKDF expand for transit HMAC key failed: {e}"))?;
    Ok(out)
}

/// 由已派生密钥计算 HMAC（空 `algorithm` = 默认 HMAC-SHA256）
fn hmac_bytes(key: &[u8; DEK_LEN], data: &[u8], algorithm: &str) -> Result<Vec<u8>, String> {
    let algo = if algorithm.is_empty() {
        "HMAC-SHA256"
    } else {
        algorithm
    };
    match algo.to_uppercase().as_str() {
        "HMAC-SHA256" => {
            let mut mac: Hmac<Sha256> = hmac::digest::KeyInit::new_from_slice(key)
                .map_err(|e| format!("HMAC-SHA256 init: {e}"))?;
            Mac::update(&mut mac, data);
            Ok(Mac::finalize(mac).into_bytes().to_vec())
        }
        "HMAC-SHA512" => {
            let mut mac: Hmac<Sha512> = hmac::digest::KeyInit::new_from_slice(key)
                .map_err(|e| format!("HMAC-SHA512 init: {e}"))?;
            Mac::update(&mut mac, data);
            Ok(Mac::finalize(mac).into_bytes().to_vec())
        }
        other => Err(format!("unsupported HMAC algorithm: {other}")),
    }
}

/// `DEK_LEN` 在 KEK 派生与 HMAC 派生里只是「32 字节输出」的同名常量。
const DEK_LEN: usize = 32;

// ──── TransitService ────

/// gRPC 面的 Transit 服务：KEK 材料集 + 命名密钥注册表
pub struct TransitService {
    config: TransitConfig,
    /// 材料集合：KEK 包裹与 HMAC 派生都从它取材料（主 + 历史）
    keyring: Arc<TransitKekKeyring>,
    /// 命名密钥档案（与 PKI 共用同一实例，ADR-0019 D1/D2）
    registry: Arc<TransitKeyRegistry>,
}

impl TransitService {
    /// 构造服务：持久化后端为内存实现（开发/单测）
    pub fn new(config: TransitConfig, kek_material: TransitKekMaterial) -> Result<Self, String> {
        let keyring = Arc::new(TransitKekKeyring::single(&config.kek_id, kek_material)?);
        Ok(Self::with_keyring(config, keyring))
    }

    /// 内存 store + 显式材料集（主 + 历史）
    pub fn with_keyring(config: TransitConfig, keyring: Arc<TransitKekKeyring>) -> Self {
        let registry = Arc::new(TransitKeyRegistry::new(
            Arc::new(MemoryTransitKeyStore::new()),
            keyring.clone(),
        ));
        Self {
            config,
            keyring,
            registry,
        }
    }

    /// 生产路径：注入共享存储（coord-server KV）与材料集
    pub fn with_store_keyring(
        config: TransitConfig,
        store: Arc<dyn TransitKeyStore>,
        keyring: TransitKekKeyring,
    ) -> Result<Self, String> {
        let keyring = Arc::new(keyring);
        let registry = Arc::new(TransitKeyRegistry::new(store, keyring.clone()));
        Ok(Self {
            config,
            keyring,
            registry,
        })
    }

    /// 复用外部注册表（PKI 与 transit 共用同一档案，ADR-0019 D1/D2）
    pub fn with_registry(config: TransitConfig, registry: Arc<TransitKeyRegistry>) -> Self {
        let keyring = registry.keyring().clone();
        Self {
            config,
            keyring,
            registry,
        }
    }

    pub fn registry(&self) -> &Arc<TransitKeyRegistry> {
        &self.registry
    }

    pub fn keyring(&self) -> &Arc<TransitKekKeyring> {
        &self.keyring
    }

    /// 主材料标识（HMAC 新写入口径 / 审计标签）
    pub fn primary_kek_id(&self) -> &str {
        self.keyring.primary_id()
    }

    // ──── HMAC（ADR-0012：用途化派生；密钥仅内存）───

    /// 按材料标识 + 用途派生 HMAC 密钥（未注入 ⇒ fail-loud）
    fn hmac_key_for(&self, key_id: &str, purpose: &str) -> Result<[u8; DEK_LEN], String> {
        let material = self.keyring.material_for(key_id).ok_or_else(|| {
            format!(
                "KEK material '{key_id}' is not injected; inject it (COORD_TRANSIT_KEK_OLD) \
                 to use it for HMAC"
            )
        })?;
        derive_hmac_key(material, key_id, purpose)
    }

    /// 使用 HMAC 对数据进行签名（返回 `(签名, 实际材料标识)`）
    ///
    /// 支持算法: HMAC-SHA256（默认）, HMAC-SHA512。
    /// `purpose` 必填（不同用途派生的密钥互不可验）；`key_id` 缺省/空 = 主材料。
    pub fn hmac_sign(
        &self,
        data: &[u8],
        purpose: &str,
        algorithm: &str,
        key_id: Option<&str>,
    ) -> Result<(Vec<u8>, String), String> {
        validate_hmac_purpose(purpose)?;
        let key_id = key_id
            .filter(|s| !s.is_empty())
            .unwrap_or(self.primary_kek_id());
        let key = self.hmac_key_for(key_id, purpose)?;
        Ok((hmac_bytes(&key, data, algorithm)?, key_id.to_string()))
    }

    /// 验证 HMAC 签名（常量时间比较；材料未注入 ⇒ 错误，不是 valid=false）
    pub fn hmac_verify(
        &self,
        data: &[u8],
        signature: &[u8],
        purpose: &str,
        algorithm: &str,
        key_id: &str,
    ) -> Result<bool, String> {
        validate_hmac_purpose(purpose)?;
        if key_id.is_empty() {
            return Err(
                "invalid key_id: must not be empty (persist the HmacSign key_id with the \
                 signature)"
                    .into(),
            );
        }
        let key = self.hmac_key_for(key_id, purpose)?;
        let expected = hmac_bytes(&key, data, algorithm)?;
        // 常量时间比较
        Ok(expected.len() == signature.len()
            && expected
                .iter()
                .zip(signature.iter())
                .fold(0u8, |acc, (a, b)| acc | (a ^ b))
                == 0)
    }

    pub fn config(&self) -> &TransitConfig {
        &self.config
    }
}

#[async_trait::async_trait]
impl crate::service::BaseService for TransitService {
    fn name(&self) -> &'static str {
        "transit"
    }

    async fn start(&self) -> crate::service::ServiceResult<()> {
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

    fn test_svc(config: TransitConfig) -> TransitService {
        TransitService::new(config, TransitKekMaterial::for_test()).expect("create")
    }

    #[test]
    fn test_config_defaults() {
        let c = TransitConfig::default();
        assert_eq!(c.kek_id, "default-kek");
    }

    // ──── KEK 注入与 fail-closed（负控制）────
    //
    // 中心命题两条：
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

    /// dev 默认 KEK（ADR-0009）：必须是可入构造的合法材料——长度由类型
    /// `[u8; KEK_MATERIAL_LEN]` 在编译期钉住，这里再钉「构造器接受」。
    #[test]
    fn test_dev_default_kek_is_valid_material() {
        TransitKekMaterial::from_bytes(&DEV_DEFAULT_KEK)
            .expect("dev default KEK must be valid 32-byte material");
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
        use crate::services::transit_key::{CreateKeySpec, KeyType, TransitKeyRegistry};
        use crate::services::transit_key_store::MemoryTransitKeyStore;

        let cfg = TransitConfig::default();
        assert_eq!(cfg.kek_id, "default-kek");

        let store: Arc<dyn TransitKeyStore> = Arc::new(MemoryTransitKeyStore::new());
        let producer = TransitKeyRegistry::new(
            store.clone(),
            Arc::new(
                TransitKekKeyring::single(
                    &cfg.kek_id,
                    TransitKekMaterial::from_bytes(&[0x01u8; 32]).unwrap(),
                )
                .unwrap(),
            ),
        );
        producer
            .create_key(CreateKeySpec {
                name: "k".into(),
                key_type: KeyType::Aes256Gcm96,
                exportable: false,
            })
            .await
            .expect("create key");
        let (ct, _version) = producer
            .encrypt("k", 0, b"material-dependent", b"")
            .await
            .unwrap();

        // 同 kek_id、不同材料 ⇒ 材料解不开
        let wrong = TransitKeyRegistry::new(
            store.clone(),
            Arc::new(
                TransitKekKeyring::single(
                    &cfg.kek_id,
                    TransitKekMaterial::from_bytes(&[0x02u8; 32]).unwrap(),
                )
                .unwrap(),
            ),
        );
        assert!(
            wrong.decrypt(None, &ct, b"").await.is_err(),
            "不同材料必须解不开（否则 KEK 仍来自 kek_id）"
        );

        // 正控制：同一材料 ⇒ 解得开
        let right = TransitKeyRegistry::new(
            store,
            Arc::new(
                TransitKekKeyring::single(
                    &cfg.kek_id,
                    TransitKekMaterial::from_bytes(&[0x01u8; 32]).unwrap(),
                )
                .unwrap(),
            ),
        );
        assert_eq!(
            right.decrypt(None, &ct, b"").await.expect("decrypt").0,
            b"material-dependent"
        );
    }

    /// KEK 与 HMAC 密钥由**同一材料 + 不同 info** 域分隔派生 ⇒ 两者不相等，
    /// 且 HMAC 密钥也随材料变化（防止"只改了 KEK 忘了 HMAC"的半截变更）
    #[tokio::test]
    async fn test_hmac_key_is_domain_separated_from_material() {
        let cfg = TransitConfig::default();
        let a = test_svc(cfg.clone());
        let b = TransitService::new(cfg, TransitKekMaterial::from_bytes(&[0x03u8; 32]).unwrap())
            .expect("create");
        let (sig_a, _) = a
            .hmac_sign(b"same input", "p:test", "HMAC-SHA256", None)
            .expect("sign");
        let (sig_b, _) = b
            .hmac_sign(b"same input", "p:test", "HMAC-SHA256", None)
            .expect("sign");
        assert_ne!(sig_a, sig_b, "HMAC 密钥必须随注入材料变化");
    }

    #[test]
    fn test_hmac_derivation_golden_vector() {
        let svc = test_svc(TransitConfig::default());
        let (sig, key_id) = svc
            .hmac_sign(b"golden-vector", "p:golden", "HMAC-SHA256", None)
            .expect("sign");
        assert_eq!(key_id, "default-kek");
        assert_eq!(
            hex::encode(sig),
            "5060a19382984c49ca53ac217ea0df9e44c8f8d4d6baa97453918c80978c2ef3"
        );
    }

    #[test]
    fn test_hmac_sign_sha256_default() {
        let svc = test_svc(TransitConfig::default());
        let data = b"hello hmac";
        let (sig, _) = svc.hmac_sign(data, "p:test", "", None).expect("hmac_sign");
        assert!(!sig.is_empty());
        // SHA-256 HMAC 输出 32 字节
        assert_eq!(sig.len(), 32);
    }

    #[test]
    fn test_hmac_sign_sha512() {
        let svc = test_svc(TransitConfig::default());
        let (sig, _) = svc
            .hmac_sign(b"data", "p:test", "HMAC-SHA512", None)
            .expect("hmac_sign");
        assert_eq!(sig.len(), 64);
    }

    #[test]
    fn test_hmac_verify_roundtrip() {
        let svc = test_svc(TransitConfig::default());
        let data = b"verify me";
        let (sig, _) = svc
            .hmac_sign(data, "p:test", "HMAC-SHA256", None)
            .expect("sign");
        assert!(svc
            .hmac_verify(data, &sig, "p:test", "HMAC-SHA256", "default-kek")
            .expect("verify"));
    }

    #[test]
    fn test_hmac_verify_tampered_data() {
        let svc = test_svc(TransitConfig::default());
        let (sig, _) = svc
            .hmac_sign(b"original", "p:test", "HMAC-SHA256", None)
            .expect("sign");
        assert!(!svc
            .hmac_verify(b"tampered", &sig, "p:test", "HMAC-SHA256", "default-kek")
            .expect("verify"));
    }

    #[test]
    fn test_hmac_verify_tampered_signature() {
        let svc = test_svc(TransitConfig::default());
        let data = b"my data";
        let (mut sig, _) = svc
            .hmac_sign(data, "p:test", "HMAC-SHA256", None)
            .expect("sign");
        // Corrupt the signature
        sig[0] ^= 0xFF;
        assert!(!svc
            .hmac_verify(data, &sig, "p:test", "HMAC-SHA256", "default-kek")
            .expect("verify"));
    }

    #[test]
    fn test_hmac_deterministic() {
        let svc = test_svc(TransitConfig::default());
        let data = b"deterministic";
        let (sig1, _) = svc
            .hmac_sign(data, "p:test", "HMAC-SHA256", None)
            .expect("sign");
        let (sig2, _) = svc
            .hmac_sign(data, "p:test", "HMAC-SHA256", None)
            .expect("sign");
        assert_eq!(sig1, sig2);
    }

    #[test]
    fn test_hmac_unsupported_algorithm() {
        let svc = test_svc(TransitConfig::default());
        assert!(svc.hmac_sign(b"data", "p:test", "HMAC-MD5", None).is_err());
    }

    #[test]
    fn test_hmac_purpose_must_not_be_empty() {
        let svc = test_svc(TransitConfig::default());
        let err = svc
            .hmac_sign(b"data", "", "HMAC-SHA256", None)
            .expect_err("empty purpose must fail");
        assert!(err.starts_with("invalid purpose"), "unexpected: {err}");
        let err = svc
            .hmac_verify(b"data", b"sig", "", "HMAC-SHA256", "default-kek")
            .expect_err("empty purpose must fail");
        assert!(err.starts_with("invalid purpose"), "unexpected: {err}");
    }

    #[test]
    fn test_hmac_purpose_validation_is_byte_level_and_never_trims() {
        let svc = test_svc(TransitConfig::default());
        assert!(svc
            .hmac_sign(b"d", "a\0b", "", None)
            .unwrap_err()
            .starts_with("invalid purpose"));
        let max = "x".repeat(128);
        assert!(
            svc.hmac_sign(b"d", &max, "", None).is_ok(),
            "128 bytes must pass"
        );
        let too_long = "x".repeat(129);
        assert!(svc
            .hmac_sign(b"d", &too_long, "", None)
            .unwrap_err()
            .starts_with("invalid purpose"));
        // 多字节按字节计：64 个 4 字节码位 = 256 字节 ⇒ 拒绝
        let multibyte = "🦀".repeat(64);
        assert!(svc
            .hmac_sign(b"d", &multibyte, "", None)
            .unwrap_err()
            .starts_with("invalid purpose"));
        // 空白串是合法不透明标签（不得 trim）
        assert!(svc.hmac_sign(b"d", " ", "", None).is_ok());
    }

    #[test]
    fn test_hmac_sign_default_and_explicit_primary_return_key_id() {
        let svc = test_svc(TransitConfig::default());
        let (sig_default, id_default) = svc
            .hmac_sign(b"data", "p:id", "HMAC-SHA256", None)
            .expect("sign");
        let (sig_empty, id_empty) = svc
            .hmac_sign(b"data", "p:id", "HMAC-SHA256", Some(""))
            .expect("sign with empty key_id = primary");
        assert_eq!(id_default, "default-kek");
        assert_eq!(id_empty, "default-kek");
        assert_eq!(sig_default, sig_empty);
    }

    #[test]
    fn test_hmac_purposes_are_mutually_unverifiable() {
        let svc = test_svc(TransitConfig::default());
        let (sig, _) = svc
            .hmac_sign(b"same data", "p:a", "HMAC-SHA256", None)
            .expect("sign");
        assert!(svc
            .hmac_verify(b"same data", &sig, "p:a", "HMAC-SHA256", "default-kek")
            .expect("same purpose"));
        assert!(!svc
            .hmac_verify(b"same data", &sig, "p:b", "HMAC-SHA256", "default-kek")
            .expect("other purpose"));
    }

    #[test]
    fn test_hmac_verify_requires_non_empty_key_id() {
        let svc = test_svc(TransitConfig::default());
        let err = svc
            .hmac_verify(b"data", b"sig", "p:a", "HMAC-SHA256", "")
            .expect_err("empty key_id must fail");
        assert!(err.starts_with("invalid key_id"), "unexpected: {err}");
    }

    #[test]
    fn test_hmac_unknown_key_id_fails_loud() {
        let svc = test_svc(TransitConfig::default());
        let err = svc
            .hmac_verify(b"data", b"sig", "p:a", "HMAC-SHA256", "ghost")
            .expect_err("not injected must fail loud");
        assert!(err.contains("is not injected"), "unexpected: {err}");
        assert!(
            svc.hmac_sign(b"data", "p:a", "HMAC-SHA256", Some("ghost"))
                .is_err(),
            "sign with unknown material must fail"
        );
    }

    /// 主材料 + 历史材料实例（HMAC 轮换窗口测试用）

    fn svc_with_history(primary_id: &str, primary_byte: u8, old: &[(&str, u8)]) -> TransitService {
        let store: Arc<dyn TransitKeyStore> = Arc::new(MemoryTransitKeyStore::new());
        TransitService::with_store_keyring(
            TransitConfig {
                kek_id: primary_id.into(),
                ..Default::default()
            },
            store,
            TransitKekKeyring::resolve_with_env(
                std::path::Path::new("/nonexistent"),
                primary_id,
                material(primary_byte),
                if old.is_empty() {
                    None
                } else {
                    Some(old_env_for(old))
                },
            )
            .expect("keyring"),
        )
        .expect("svc")
    }

    #[test]
    fn test_hmac_rotation_window_and_offline_fail_loud() {
        let purpose = "eis:hr:employee:id_number:v1";
        // 旧部署（材料 1 / id "old"）产生存量签名
        let old_svc = svc_with_history("old", 1, &[]);
        let (sig, key_id) = old_svc
            .hmac_sign(b"stored hash", purpose, "HMAC-SHA256", None)
            .expect("sign");
        assert_eq!(key_id, "old");

        // 轮换：主材料 2 / id "new" + 历史材料 1 / id "old" ⇒ 窗口内可验
        let rotated = svc_with_history("new", 2, &[("old", 1)]);
        assert!(rotated
            .hmac_verify(b"stored hash", &sig, purpose, "HMAC-SHA256", "old")
            .expect("window verify"));
        // 新签名用主材料
        let (sig_new, id_new) = rotated
            .hmac_sign(b"stored hash", purpose, "HMAC-SHA256", None)
            .expect("sign new");
        assert_eq!(id_new, "new");
        assert_ne!(sig, sig_new);
        // 旧材料复算散列变体（DRV-005 防重）
        let (variant, variant_id) = rotated
            .hmac_sign(b"stored hash", purpose, "HMAC-SHA256", Some("old"))
            .expect("variant");
        assert_eq!(variant_id, "old");
        assert_eq!(variant, sig, "同 (purpose,data,key_id) 必须可复算");

        // 历史材料下线（未注入）⇒ fail-loud（不是 valid=false）
        let offline = svc_with_history("new", 2, &[]);
        let err = offline
            .hmac_verify(b"stored hash", &sig, purpose, "HMAC-SHA256", "old")
            .expect_err("offline must fail loud");
        assert!(err.contains("is not injected"), "unexpected: {err}");
    }

    #[test]
    fn test_hmac_cross_agent_consistency() {
        let cfg = || TransitConfig {
            kek_id: "k1".into(),
            ..Default::default()
        };
        let a = test_svc(cfg());
        let b = test_svc(cfg());
        let (sig_a, id_a) = a
            .hmac_sign(b"same data", "p:same", "HMAC-SHA256", None)
            .expect("a");
        let (sig_b, id_b) = b
            .hmac_sign(b"same data", "p:same", "HMAC-SHA256", None)
            .expect("b");
        assert_eq!(id_a, id_b);
        assert_eq!(sig_a, sig_b, "同材料+id+purpose ⇒ 同签名");
        assert!(b
            .hmac_verify(b"same data", &sig_a, "p:same", "HMAC-SHA256", &id_a)
            .expect("cross-agent verify"));
    }

    fn material(byte: u8) -> TransitKekMaterial {
        TransitKekMaterial::from_bytes(&[byte; 32]).expect("32 bytes")
    }

    fn old_env_for(entries: &[(&str, u8)]) -> String {
        entries
            .iter()
            .map(|(id, b)| format!("{id}:{}", hex::encode([*b; 32])))
            .collect::<Vec<_>>()
            .join(",")
    }
}
