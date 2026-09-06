// 对象存储 —— 数据面（chunk 文件）+ manifest 编排
//
// 设计（docs/volume-object-storage.md 决策记录）：
//   - 对象 = (bucket, object_id)。对象 manifest 以**用户 KV** 形式存于保留前缀
//     `/obj/m/{bucket}/{object_id}`（`/kv/` 语义：加密/快照/压缩/强一致自动继承，
//     manifest 小、随快照携带——快照不含 chunk 文件）；
//   - chunk 数据**随 raft 日志复制**（日志==状态机），apply 时由
//     `apply_object_store_op` 落 append-only chunk 文件（不进 MVCC、不入快照）；
//     文件目录 = `<data_dir>/objects/<sha256hex>/chunk-{seq:08}`（对象目录名取
//     manifest key 的 sha256，路径可确定性推导、组件短、孤儿可回收）；
//   - 删除 = KV tombstone（manifest 消失）+ apply 同步删除 chunk 文件；
//     Creating 过期/孤儿文件由后台 GC（见 server 层 `object_gc_loop`）兜底；
//   - chunk 静态加密为独立开关（默认关闭）：DEK 化信封——配置根密钥（hex64）经
//     HKDF-SHA256 派生 KEK（仅内存），KEK 包裹随机 DEK（key_id 版本化；密文落盘
//     `<root>/objects/keys/`），新 chunk 文件头 `magic("COBJ2") || key_id(4 BE) ||
//     nonce(12)`；v1（"COBJ1"，Phase A 根密钥直作 DEK）文件兼容读取；DEK 按
//     `[object_storage].encryption_rotation_days` 自动轮换（仅影响新写入，旧 DEK
//     保留解密历史——对齐 /kv/ key_management 治理，见 STATUS.md 整改要点）；
//   - 硬前提：chunk 与 MVCC/快照物理隔离、配额 + 磁盘水位（服务层执行）、
//     流式协议绕 4MiB RPC 上限（coord.storage proto）、容量按副本折算披露。

use std::collections::HashSet;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use aes_gcm::{
    aead::{Aead, AeadCore, KeyInit, OsRng},
    Aes256Gcm, Nonce,
};
use coord_core::error::{Error, Result};
use hkdf::Hkdf;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::mvcc::{AppliedLogId, MvccStorage};
use super::redb_backend::RedbBackend;
use crate::raft::type_config::ObjectStoreOp;

/// 对象保留用户前缀（KV/Txn/Watch 对其拒绝；对象 manifest 存于其下）
pub const OBJECT_PREFIX: &[u8] = b"/obj/";
/// manifest 用户前缀
pub const MANIFEST_PREFIX: &[u8] = b"/obj/m/";
/// 对象根子目录名（位于 raft 数据目录下，如 `<data_dir>/objects/`）
const OBJECTS_DIR: &str = "objects";
/// chunk 加密文件头 magic（v2 信封：key_id 版本化，见 ChunkCrypto）
const CHUNK_MAGIC: &[u8; 5] = b"COBJ2";
/// v1 遗留加密格式（Phase A：根密钥直作 DEK）——仅读兼容
const CHUNK_MAGIC_V1: &[u8; 5] = b"COBJ1";
/// chunk 文件头 key_id 长度（4 字节 BE）
const KEY_ID_LEN: usize = 4;
/// 加密根密钥长度（256-bit）
const ROOT_KEY_LEN: usize = 32;
/// DEK 长度（256-bit）
const DEK_LEN: usize = 32;
/// KEK 长度（256-bit）
const KEK_LEN: usize = 32;
/// GCM nonce 长度（96-bit）
const NONCE_LEN: usize = 12;
/// GCM tag 长度（128-bit）
const TAG_LEN: usize = 16;
/// 密钥子目录名（位于 `<root>/objects/keys/`）
const KEYS_DIR: &str = "keys";
/// 包裹 DEK 落盘文件 magic（4 字节）
const WRAPPED_DEK_MAGIC: &[u8; 4] = b"ODK1";
/// 包裹 DEK 密文长度：nonce(12) + DEK(32) + tag(16) = 60
const WRAPPED_DEK_LEN: usize = NONCE_LEN + DEK_LEN + TAG_LEN;
/// DEK 落盘文件长度：magic(4) + wrapped(60) = 64
const KEY_FILE_LEN: usize = WRAPPED_DEK_MAGIC.len() + WRAPPED_DEK_LEN;
/// 退役 DEK 内存缓存上限（FIFO；防止长运行节点缓存无限增长）
const RETIRED_DEK_MAX: usize = 16;

// ──── 上限/限额（服务层 + apply 双保险） ────

/// 对象存储运行时限制（配置层构造；全集群必须一致）
#[derive(Debug, Clone)]
pub struct ObjectLimits {
    /// 单 chunk 字节上限（≤ 4MiB RPC 解码上限）
    pub chunk_size: usize,
    /// 单对象字节上限
    pub max_object_size: u64,
    /// 配额上限（全量合计；0 = 不限，admission 侧尽力而为）
    pub quota_bytes: u64,
    /// Creating 对象视为过期（无新 chunk）的秒数，之后由 GC 删除
    pub upload_timeout_secs: u64,
    /// chunk DEK 自动轮换间隔（秒；0 = 关闭）。仅加密启用时有意义。
    pub dek_rotation_secs: u64,
}

impl Default for ObjectLimits {
    fn default() -> Self {
        Self {
            chunk_size: 4 * 1024 * 1024,
            max_object_size: 256 * 1024 * 1024,
            quota_bytes: 0,
            upload_timeout_secs: 300,
            dek_rotation_secs: 0,
        }
    }
}

/// 对象存储启用配置（main.rs 按 `[object_storage]` 构造；Region 装配/root 共享）。
/// `Some` = 启用；各 raft 实例（root/Region）据此在**自己数据目录**下创建
/// `ChunkStore`。全集群配置必须一致（对齐 multi_raft 配置一致性约定）。
#[derive(Clone)]
pub struct ObjectStoreCtx {
    pub limits: Arc<ObjectLimits>,
    pub encryption_root_key_hex: Option<String>,
}

// ──── 引用与 key 编码 ────

/// 校验 (bucket, object_id) 合法性。
/// bucket：utf8、非空、≤255 字节、不含 `/`（保证 manifest key 可分节解析）。
/// object_id：非空、≤1024 字节（任意字节，可含 `/`）。
pub fn validate_ref(bucket: &[u8], object_id: &[u8]) -> Result<()> {
    let b = std::str::from_utf8(bucket)
        .map_err(|_| Error::InvalidArgument("bucket must be utf8".into()))?;
    if b.is_empty() {
        return Err(Error::InvalidArgument("bucket must not be empty".into()));
    }
    if bucket.len() > 255 {
        return Err(Error::InvalidArgument(
            "bucket must be <= 255 bytes".into(),
        ));
    }
    if bucket.contains(&b'/') {
        return Err(Error::InvalidArgument(
            "bucket must not contain '/'".into(),
        ));
    }
    if object_id.is_empty() {
        return Err(Error::InvalidArgument("object_id must not be empty".into()));
    }
    if object_id.len() > 1024 {
        return Err(Error::InvalidArgument(
            "object_id must be <= 1024 bytes".into(),
        ));
    }
    Ok(())
}

/// manifest 用户 key：`/obj/m/{bucket}/{object_id}`（bucket 不含 `/`，可分节解析）
pub fn manifest_key(bucket: &[u8], object_id: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(MANIFEST_PREFIX.len() + bucket.len() + 1 + object_id.len());
    k.extend_from_slice(MANIFEST_PREFIX);
    k.extend_from_slice(bucket);
    k.push(b'/');
    k.extend_from_slice(object_id);
    k
}

/// 从 manifest 用户 key 解析 (bucket, object_id)；非对象 key 返回 None
pub fn parse_manifest_key(key: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
    let rest = key.strip_prefix(MANIFEST_PREFIX)?;
    let sep = rest.iter().position(|b| *b == b'/')?;
    let (bucket, id) = rest.split_at(sep);
    let id = &id[1..];
    if bucket.is_empty() || id.is_empty() {
        return None;
    }
    Some((bucket.to_vec(), id.to_vec()))
}

/// 对象目录名 = sha256(manifest key) 的 hex（64 字符，确定性、组件短）
fn object_dir_hash(bucket: &[u8], object_id: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(manifest_key(bucket, object_id));
    hex(&hasher.finalize())
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0xf) as usize] as char);
    }
    s
}

/// 单 key 是否位于保留对象空间（`/obj/` 前缀）
pub fn key_in_object_space(key: &[u8]) -> bool {
    key.starts_with(OBJECT_PREFIX)
}

/// 半开区间 [start, end)（end 空 = 无上界）是否与保留对象空间相交
pub fn range_touches_object_space(start: &[u8], end: &[u8]) -> bool {
    if end.is_empty() || end == start {
        return key_in_object_space(start);
    }
    // 对象空间 = [/obj/, "/obj0")（前缀的字典序上界）
    let upper = b"/obj0".as_slice();
    let below_start = end <= OBJECT_PREFIX; // 区间整体结束于空间起点之前
    let above_end = start >= upper; // 区间整体起始于空间终点之后
    !below_start && !above_end
}

// ──── manifest（raft 强一致的 /kv/ 用户行，bincode） ────

/// 单个 chunk 记录（写入 apply 时由数据计算 sha256，确定性）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkRec {
    pub seq: u32,
    pub len: u64,
    pub sha256: [u8; 32],
}

/// 对象 manifest。删除态不在本结构中——删除 = KV tombstone（get 返回 None）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectManifest {
    /// Begin 声明的期望总字节数
    pub total_size: u64,
    /// 已收 chunk 字节合计
    pub size: u64,
    pub chunks: Vec<ChunkRec>,
    /// 是否已 Commit（false = 上传进行中）
    pub committed: bool,
    /// Begin 所在 raft revision
    pub create_revision: u64,
    /// 最近一次 manifest 写入所在 raft revision（GC 判断 stale Creating 用）
    pub last_revision: u64,
    /// Begin 提议墙钟（propose 侧填；apply 不读墙钟）
    pub started_at_unix: i64,
    /// 最近一次 chunk 提议墙钟（GC 判 stale Creating：now - last > timeout）
    pub last_write_at_unix: i64,
}

impl ObjectManifest {
    pub fn creating(total_size: u64, revision: u64, started_at_unix: i64) -> Self {
        Self {
            total_size,
            size: 0,
            chunks: Vec::new(),
            committed: false,
            create_revision: revision,
            last_revision: revision,
            started_at_unix,
            last_write_at_unix: started_at_unix,
        }
    }
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::serialize(self)
            .map_err(|e| Error::Internal(format!("encode object manifest: {e}")))
    }
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        bincode::deserialize(bytes).ok()
    }
}

// ──── chunk 文件存储 ────

/// chunk 密钥环内部状态（Mutex 保护：apply 写路径串行 + GC/读多线程并发）。
struct ChunkKeyringInner {
    active_dek: [u8; DEK_LEN],
    active_key_id: u32,
    next_key_id: u32,
    /// 已退役 DEK（key_id → 明文），供读取历史 chunk；FIFO 上限见 RETIRED_DEK_MAX
    retired: Vec<(u32, [u8; DEK_LEN])>,
    /// 最近一次 DEK 轮换墙钟（unix 秒）
    last_rotation_unix: i64,
}

impl ChunkKeyringInner {
    fn dek(&self, key_id: u32) -> Option<[u8; DEK_LEN]> {
        if key_id == self.active_key_id {
            return Some(self.active_dek);
        }
        self.retired
            .iter()
            .rev()
            .find(|(id, _)| *id == key_id)
            .map(|(_, d)| *d)
    }
}

/// chunk 静态加密上下文（None = 明文）。
///
/// DEK 化信封（对齐 /kv/ `key_management` 三层密钥）：
///   配置根密钥(hex64) → HKDF-SHA256(info="coord-obj-kek-v1") → KEK（仅内存）
///   → AES-256-GCM wrap/unwrap 随机 DEK（key_id 单调递增；密文落盘
///     `<root>/objects/keys/dek-{key_id:08x}.bin`）。
/// 新 chunk 以 active DEK 加密（文件头 `COBJ2 || key_id(4BE) || nonce(12)`）；
/// DEK 轮换后旧 DEK 保留在内存缓存供读历史 chunk。`legacy_root_dek` 仅用于
/// 读取 v1（"COBJ1"，根密钥直作 DEK）文件——Phase A 已落盘数据的兼容路径。
struct ChunkCrypto {
    /// v1 兼容读的根密钥明文（仅内存；v2 数据不再直用作 DEK）
    legacy_root_dek: [u8; DEK_LEN],
    kek: Aes256Gcm,
    keys_dir: PathBuf,
    inner: std::sync::Mutex<ChunkKeyringInner>,
}

impl ChunkCrypto {
    /// 从配置根密钥引导：派生 KEK；扫描 `keys_dir` 加载全部已包裹 DEK
    /// （active = 最大 key_id）；无密钥文件（首启/快照安装清空后）bootstrap
    /// 首个 DEK（key_id=1）并原子落盘。`root` = raft 数据目录。
    fn load(root: &Path, root_key_hex: &str) -> Result<Self> {
        let root_bytes = decode_hex(root_key_hex).map_err(|_| {
            Error::InvalidArgument("object storage root key must be hex64".into())
        })?;
        if root_bytes.len() != ROOT_KEY_LEN {
            return Err(Error::InvalidArgument(
                "object storage root key must be 32 bytes (hex64)".into(),
            ));
        }
        let mut root_key = [0u8; DEK_LEN];
        root_key.copy_from_slice(&root_bytes);

        // KEK = HKDF-SHA256(root_key, salt=None, info="coord-obj-kek-v1")
        let hkdf = Hkdf::<Sha256>::new(None, &root_key);
        let mut kek_bytes = [0u8; KEK_LEN];
        hkdf.expand(b"coord-obj-kek-v1", &mut kek_bytes)
            .map_err(|e| Error::Internal(format!("object chunk KEK derive: {e}")))?;
        let kek = Aes256Gcm::new_from_slice(&kek_bytes)
            .map_err(|e| Error::Internal(format!("init chunk KEK: {e}")))?;

        let keys_dir = root.join(OBJECTS_DIR).join(KEYS_DIR);
        let mut retired: Vec<(u32, [u8; DEK_LEN])> = Vec::new();
        let mut active_key_id = 0u32;
        let mut active_dek = [0u8; DEK_LEN];
        let mut last_rotation_unix = 0i64;

        if keys_dir.is_dir() {
            let mut entries: Vec<_> = std::fs::read_dir(&keys_dir)
                .map_err(|e| Error::Storage(format!("read {}: {e}", keys_dir.display())))?
                .filter_map(|e| e.ok())
                .collect();
            entries.sort_by_key(|e| e.file_name());
            for entry in entries {
                let fname = entry.file_name().to_string_lossy().to_string();
                if let Some(rest) =
                    fname.strip_prefix("dek-").and_then(|r| r.strip_suffix(".bin"))
                {
                    let Ok(key_id) = u32::from_str_radix(rest, 16) else {
                        continue;
                    };
                    let raw = std::fs::read(entry.path()).map_err(|e| {
                        Error::Storage(format!(
                            "read wrapped DEK {}: {e}",
                            entry.path().display()
                        ))
                    })?;
                    let dek = unwrap_dek_file(&kek, &raw)?;
                    if key_id > active_key_id {
                        // 更高 key_id 成为 active；原 active（若已加载）转入退役缓存
                        if active_key_id != 0 {
                            retired.push((active_key_id, active_dek));
                        }
                        active_key_id = key_id;
                        active_dek = dek;
                    } else {
                        retired.push((key_id, dek));
                    }
                } else if fname == "last_rotation" {
                    if let Ok(raw) = std::fs::read(entry.path()) {
                        if raw.len() == 8 {
                            last_rotation_unix = i64::from_be_bytes(raw.try_into().unwrap());
                        }
                    }
                }
            }
            if active_key_id == 0 {
                // keys 目录存在但无 DEK（快照安装清空后残留空目录）：bootstrap 兜底
                let (dek, id) = gen_dek(1);
                persist_wrapped_dek(&kek, &keys_dir, id, &dek)?;
                active_dek = dek;
                active_key_id = id;
            }
        } else {
            let (dek, id) = gen_dek(1);
            persist_wrapped_dek(&kek, &keys_dir, id, &dek)?;
            active_dek = dek;
            active_key_id = id;
        }
        if last_rotation_unix == 0 {
            last_rotation_unix = now_unix();
            persist_last_rotation(&keys_dir, last_rotation_unix)?;
        }

        Ok(Self {
            legacy_root_dek: root_key,
            kek,
            keys_dir,
            inner: std::sync::Mutex::new(ChunkKeyringInner {
                active_dek,
                active_key_id,
                next_key_id: active_key_id + 1,
                retired,
                last_rotation_unix,
            }),
        })
    }
    fn active_key_id(&self) -> u32 {
        self.inner.lock().unwrap().active_key_id
    }

    /// 加密单个 chunk → v2 文件载荷：magic(5) || key_id(4 BE) || nonce(12) || ct
    fn encrypt_chunk(&self, data: &[u8]) -> Result<Vec<u8>> {
        let inner = self.inner.lock().unwrap();
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let cipher = Aes256Gcm::new_from_slice(&inner.active_dek)
            .map_err(|e| Error::Internal(format!("init chunk DEK: {e}")))?;
        let ct = cipher
            .encrypt(&nonce, data)
            .map_err(|e| Error::Internal(format!("chunk encrypt: {e}")))?;
        let mut buf = Vec::with_capacity(CHUNK_MAGIC.len() + KEY_ID_LEN + NONCE_LEN + ct.len());
        buf.extend_from_slice(CHUNK_MAGIC);
        buf.extend_from_slice(&inner.active_key_id.to_be_bytes());
        buf.extend_from_slice(&nonce);
        buf.extend_from_slice(&ct);
        Ok(buf)
    }

    /// 解密 chunk 文件载荷：自动识别 v2（"COBJ2" 信封）与 v1（"COBJ1" 根密钥
    /// 直作 DEK）格式；不识别 → 错误。
    fn decrypt_chunk(&self, bytes: &[u8], path: &Path) -> Result<Vec<u8>> {
        // v2：COBJ2 || key_id(4 BE) || nonce(12) || ct
        if bytes.len() >= CHUNK_MAGIC.len() + KEY_ID_LEN + NONCE_LEN
            && &bytes[..CHUNK_MAGIC.len()] == CHUNK_MAGIC
        {
            let key_id = u32::from_be_bytes(
                bytes[CHUNK_MAGIC.len()..CHUNK_MAGIC.len() + KEY_ID_LEN]
                    .try_into()
                    .unwrap(),
            );
            let nonce: [u8; NONCE_LEN] = bytes
                [CHUNK_MAGIC.len() + KEY_ID_LEN..CHUNK_MAGIC.len() + KEY_ID_LEN + NONCE_LEN]
                .try_into()
                .unwrap();
            let ct = &bytes[CHUNK_MAGIC.len() + KEY_ID_LEN + NONCE_LEN..];
            let dek = self.inner.lock().unwrap().dek(key_id).ok_or_else(|| {
                Error::Internal(format!(
                    "chunk DEK key_id={key_id} unavailable ({})",
                    path.display()
                ))
            })?;
            let cipher = Aes256Gcm::new_from_slice(&dek)
                .map_err(|e| Error::Internal(format!("init chunk DEK: {e}")))?;
            return cipher.decrypt(Nonce::from_slice(&nonce), ct).map_err(|_| {
                Error::Internal(format!("chunk decrypt failed ({})", path.display()))
            });
        }
        // v1：COBJ1 || nonce(12) || ct（根密钥直作 DEK）
        if bytes.len() >= CHUNK_MAGIC_V1.len() + NONCE_LEN
            && &bytes[..CHUNK_MAGIC_V1.len()] == CHUNK_MAGIC_V1
        {
            let nonce: [u8; NONCE_LEN] =
                bytes[CHUNK_MAGIC_V1.len()..CHUNK_MAGIC_V1.len() + NONCE_LEN]
                    .try_into()
                    .unwrap();
            let ct = &bytes[CHUNK_MAGIC_V1.len() + NONCE_LEN..];
            let cipher = Aes256Gcm::new_from_slice(&self.legacy_root_dek)
                .map_err(|e| Error::Internal(format!("init legacy chunk cipher: {e}")))?;
            return cipher.decrypt(Nonce::from_slice(&nonce), ct).map_err(|_| {
                Error::Internal(format!(
                    "legacy chunk decrypt failed ({})",
                    path.display()
                ))
            });
        }
        Err(Error::Internal(format!(
            "corrupt encrypted chunk {} (bad magic/length)",
            path.display()
        )))
    }

    /// DEK 轮换：生成新 DEK（next key_id）、包裹落盘、原子切换 active；
    /// 旧 DEK 移入退役缓存。返回新 key_id。
    fn rotate(&self) -> Result<u32> {
        let mut inner = self.inner.lock().unwrap();
        let key_id = inner.next_key_id;
        let (dek, _) = gen_dek(key_id);
        persist_wrapped_dek(&self.kek, &self.keys_dir, key_id, &dek)?;
        let old_id = inner.active_key_id;
        let old_dek = inner.active_dek;
        inner.retired.push((old_id, old_dek));
        if inner.retired.len() > RETIRED_DEK_MAX {
            let excess = inner.retired.len() - RETIRED_DEK_MAX;
            inner.retired.drain(..excess);
        }
        inner.active_dek = dek;
        inner.active_key_id = key_id;
        inner.next_key_id = key_id + 1;
        inner.last_rotation_unix = now_unix();
        persist_last_rotation(&self.keys_dir, inner.last_rotation_unix)?;
        Ok(key_id)
    }

    /// 到期自动轮换（写路径调用；`rotation_secs` = 0 关闭）。返回新 key_id
    /// （未触发轮换返回 0）。
    fn maybe_rotate(&self, rotation_secs: u64) -> Result<u32> {
        if rotation_secs == 0 {
            return Ok(0);
        }
        let now = now_unix();
        let last = self.inner.lock().unwrap().last_rotation_unix;
        if now.saturating_sub(last) < rotation_secs as i64 {
            return Ok(0);
        }
        self.rotate()
    }
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// 生成随机 DEK（key_id 由调用方分配）
fn gen_dek(key_id: u32) -> ([u8; DEK_LEN], u32) {
    let mut dek = [0u8; DEK_LEN];
    rand::thread_rng().fill_bytes(&mut dek);
    (dek, key_id)
}

/// KEK 包裹 DEK → nonce(12) || ct(32+16) = 60 字节
fn wrap_dek(kek: &Aes256Gcm, dek: &[u8; DEK_LEN]) -> Result<Vec<u8>> {
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    let ct = kek
        .encrypt(&nonce, dek.as_ref())
        .map_err(|e| Error::Internal(format!("wrap chunk DEK: {e}")))?;
    let mut out = Vec::with_capacity(WRAPPED_DEK_LEN);
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ct);
    Ok(out)
}

fn unwrap_dek(kek: &Aes256Gcm, wrapped: &[u8]) -> Result<[u8; DEK_LEN]> {
    if wrapped.len() != WRAPPED_DEK_LEN {
        return Err(Error::Crypto(format!(
            "wrapped DEK must be {WRAPPED_DEK_LEN} bytes, got {}",
            wrapped.len()
        )));
    }
    let nonce = Nonce::from_slice(&wrapped[..NONCE_LEN]);
    let ct = &wrapped[NONCE_LEN..];
    let pt = kek
        .decrypt(nonce, ct)
        .map_err(|_| Error::Crypto("unwrap chunk DEK failed".into()))?;
    if pt.len() != DEK_LEN {
        return Err(Error::Crypto("unwrapped chunk DEK length".into()));
    }
    let mut dek = [0u8; DEK_LEN];
    dek.copy_from_slice(&pt);
    Ok(dek)
}

/// DEK 落盘文件：magic(4) || wrapped(60)；原子写（tmp + rename）
fn persist_wrapped_dek(
    kek: &Aes256Gcm,
    keys_dir: &Path,
    key_id: u32,
    dek: &[u8; DEK_LEN],
) -> Result<()> {
    let wrapped = wrap_dek(kek, dek)?;
    let mut buf = Vec::with_capacity(KEY_FILE_LEN);
    buf.extend_from_slice(WRAPPED_DEK_MAGIC);
    buf.extend_from_slice(&wrapped);
    let path = keys_dir.join(format!("dek-{key_id:08x}.bin"));
    atomic_write_file(&path, &buf)
}

fn unwrap_dek_file(kek: &Aes256Gcm, raw: &[u8]) -> Result<[u8; DEK_LEN]> {
    if raw.len() != KEY_FILE_LEN || &raw[..WRAPPED_DEK_MAGIC.len()] != WRAPPED_DEK_MAGIC {
        return Err(Error::Crypto("corrupt wrapped DEK file".into()));
    }
    unwrap_dek(kek, &raw[WRAPPED_DEK_MAGIC.len()..])
}

/// 记录最近 DEK 轮换墙钟（8 字节 BE unix 秒）
fn persist_last_rotation(keys_dir: &Path, unix: i64) -> Result<()> {
    atomic_write_file(&keys_dir.join("last_rotation"), &unix.to_be_bytes())
}

fn atomic_write_file(path: &Path, data: &[u8]) -> Result<()> {
    let dir = path
        .parent()
        .ok_or_else(|| Error::Internal("key path has no parent".into()))?;
    std::fs::create_dir_all(dir).map_err(|e| {
        Error::Storage(format!("create key dir {}: {e}", dir.display()))
    })?;
    let fname = path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_default();
    let tmp = dir.join(format!(".{fname}.tmp"));
    let result = (|| -> std::io::Result<()> {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
        drop(f);
        std::fs::rename(&tmp, path)?;
        Ok(())
    })();
    if let Err(e) = result {
        let _ = std::fs::remove_file(&tmp);
        return Err(Error::Storage(format!(
            "atomic write {}: {e}",
            path.display()
        )));
    }
    Ok(())
}

/// 单个 raft 实例（一个 Region / 根）的 chunk 文件存储。
/// 明文（加密关闭）时构造不创建任何目录（磁盘布局字节级不变），首个对象写入
/// 惰性建目录；加密开启时引导期创建 `<root>/objects/keys/`（包裹 DEK 落盘）。
pub struct ChunkStore {
    root: PathBuf,
    crypto: Option<Arc<ChunkCrypto>>,
    pub limits: Arc<ObjectLimits>,
    /// DEK 自动轮换间隔（秒；0 = 关闭），来自 `limits.dek_rotation_secs`
    rotation_secs: u64,
}

impl std::fmt::Debug for ChunkStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkStore")
            .field("root", &self.root)
            .field("encrypted", &self.crypto.is_some())
            .finish()
    }
}

impl ChunkStore {
    /// `root`：raft 数据目录（region 0 = 节点根目录；region ≥1 = region 数据目录）。
    /// chunk 文件位于 `<root>/objects/`。
    pub fn new(
        root: impl Into<PathBuf>,
        limits: Arc<ObjectLimits>,
        encryption_root_key_hex: Option<&str>,
    ) -> Result<Arc<Self>> {
        let root: PathBuf = root.into();
        let rotation_secs = limits.dek_rotation_secs;
        let crypto = match encryption_root_key_hex {
            Some(k) => Some(Arc::new(ChunkCrypto::load(&root, k)?)),
            None => None,
        };
        Ok(Arc::new(Self {
            root,
            crypto,
            limits,
            rotation_secs,
        }))
    }

    fn objects_dir(&self) -> PathBuf {
        self.root.join(OBJECTS_DIR)
    }

    fn object_dir(&self, bucket: &[u8], object_id: &[u8]) -> PathBuf {
        let h = object_dir_hash(bucket, object_id);
        self.objects_dir()
            .join(&h[..2])
            .join(&h)
    }

    pub fn chunk_path(&self, bucket: &[u8], object_id: &[u8], seq: u32) -> PathBuf {
        self.object_dir(bucket, object_id)
            .join(format!("chunk-{seq:08}"))
    }

    pub fn chunk_file_exists(&self, bucket: &[u8], object_id: &[u8], seq: u32) -> bool {
        self.chunk_path(bucket, object_id, seq).is_file()
    }

    /// apply 路径写入（阻塞）：加密（若启用）+ 原子 rename。幂等（同 seq 同内容）。
    pub fn write_chunk(&self, bucket: &[u8], object_id: &[u8], seq: u32, data: &[u8]) -> Result<()> {
        let path = self.chunk_path(bucket, object_id, seq);
        let dir = path
            .parent()
            .ok_or_else(|| Error::Internal("chunk path has no parent".into()))?;
        std::fs::create_dir_all(dir).map_err(|e| {
            Error::Storage(format!("create chunk dir {}: {e}", dir.display()))
        })?;
        let tmp = dir.join(format!(".chunk-{seq:08}.tmp"));

        let payload = match &self.crypto {
            Some(crypto) => {
                // DEK 到期自动轮换（多数写路径不触发；轮换仅影响新写入 chunk）
                crypto.maybe_rotate(self.rotation_secs)?;
                crypto.encrypt_chunk(data)?
            }
            None => data.to_vec(),
        };

        let write_result = (|| -> std::io::Result<()> {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&payload)?;
            f.sync_all()?;
            drop(f);
            std::fs::rename(&tmp, &path)?;
            Ok(())
        })();
        if let Err(e) = write_result {
            let _ = std::fs::remove_file(&tmp);
            return Err(Error::Storage(format!(
                "write chunk {}: {e}",
                path.display()
            )));
        }
        Ok(())
    }

    /// 读取 chunk（解密若启用）。文件缺失 → Error::NotFound。
    pub fn read_chunk(&self, bucket: &[u8], object_id: &[u8], seq: u32) -> Result<Vec<u8>> {
        let path = self.chunk_path(bucket, object_id, seq);
        let bytes = std::fs::read(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                Error::NotFound {
                    resource: "chunk file",
                    key: format!(
                        "{seq} ({}): node may be rebuilding from snapshot",
                        path.display()
                    ),
                }
            } else {
                Error::Storage(format!("read chunk {}: {e}", path.display()))
            }
        })?;
        match &self.crypto {
            None => Ok(bytes),
            Some(crypto) => crypto.decrypt_chunk(&bytes, &path),
        }
    }

    /// 删除对象全部 chunk 文件（幂等；Delete apply 后调用）
    pub fn delete_object_files(&self, bucket: &[u8], object_id: &[u8]) -> Result<()> {
        let dir = self.object_dir(bucket, object_id);
        let fan = dir.parent().unwrap_or(&dir);
        if dir.is_dir() {
            std::fs::remove_dir_all(&dir).map_err(|e| {
                Error::Storage(format!("remove object dir {}: {e}", dir.display()))
            })?;
        }
        // 顺带清理空 fan-out 目录（尽力而为）
        if fan != self.objects_dir() && fan.is_dir() {
            let _ = std::fs::remove_dir(fan);
        }
        Ok(())
    }

    /// 快照安装后清空全部 chunk 文件（本节点本地数据面重建，manifest 从快照恢复）
    pub fn clear_all(&self) -> Result<()> {
        let dir = self.objects_dir();
        if dir.is_dir() {
            std::fs::remove_dir_all(&dir).map_err(|e| {
                Error::Storage(format!("clear object dir {}: {e}", dir.display()))
            })?;
        }
        Ok(())
    }

    /// 强制轮换 chunk DEK：生成新 key_id，旧 DEK 移入缓存仍可读历史 chunk
    /// （管理/测试入口；生产自动轮换见 `write_chunk` 内 `maybe_rotate`）。
    /// 未启用加密返回 0。
    pub fn rotate_dek(&self) -> Result<u32> {
        match &self.crypto {
            Some(crypto) => crypto.rotate(),
            None => Ok(0),
        }
    }

    /// 当前活跃 chunk DEK 的 key_id（未启用加密返回 0）。
    pub fn active_key_id(&self) -> u32 {
        match &self.crypto {
            Some(crypto) => crypto.active_key_id(),
            None => 0,
        }
    }

    /// 已部署 chunk DEK 版本数（active + 退役缓存；未加密返回 0）。
    pub fn dek_version_count(&self) -> usize {
        match &self.crypto {
            Some(crypto) => {
                let inner = crypto.inner.lock().unwrap();
                1 + inner.retired.len()
            }
            None => 0,
        }
    }

    /// 已用磁盘字节（估算，遍历 chunk 文件求和；配额 admission 用）。
    /// 密钥目录（DEK 密文，见 KEYS_DIR）不计入对象配额。
    pub fn usage_bytes(&self) -> Result<u64> {
        let mut total = 0u64;
        let dir = self.objects_dir();
        if !dir.is_dir() {
            return Ok(0);
        }
        for entry in std::fs::read_dir(&dir).map_err(|e| {
            Error::Storage(format!("read object dir {}: {e}", dir.display()))
        })? {
            let entry = entry.map_err(|e| Error::Storage(format!("object dir entry: {e}")))?;
            if !entry.path().is_dir() {
                continue;
            }
            // 密钥目录（DEK 密文）不计入对象配额
            if entry.file_name() == KEYS_DIR {
                continue;
            }
            let fan = entry.path();
            for obj in std::fs::read_dir(&fan).map_err(|e| {
                Error::Storage(format!("read fan dir {}: {e}", fan.display()))
            })? {
                let obj = obj.map_err(|e| Error::Storage(format!("fan dir entry: {e}")))?;
                if !obj.path().is_dir() {
                    continue;
                }
                for f in std::fs::read_dir(obj.path()).map_err(|e| {
                    Error::Storage(format!("read object dir {}: {e}", obj.path().display()))
                })? {
                    let f = f.map_err(|e| Error::Storage(format!("object dir entry: {e}")))?;
                    if f.path().is_file() {
                        total += file_size(&f.path()).unwrap_or(0);
                    }
                }
            }
        }
        Ok(total)
    }

    /// 孤儿回收：删除不在 `live_hashes` 中的对象目录（含 chunk 文件）。
    /// 返回删除的对象目录数。live 集合 = 全部 manifest 的对象目录哈希。
    pub fn sweep_orphans(&self, live_hashes: &HashSet<String>) -> Result<u64> {
        let dir = self.objects_dir();
        if !dir.is_dir() {
            return Ok(0);
        }
        let mut removed = 0u64;
        for entry in std::fs::read_dir(&dir).map_err(|e| {
            Error::Storage(format!("read object dir {}: {e}", dir.display()))
        })? {
            let entry = entry.map_err(|e| Error::Storage(format!("object dir entry: {e}")))?;
            let fan = entry.path();
            if !fan.is_dir() {
                continue;
            }
            for sub in std::fs::read_dir(&fan).map_err(|e| {
                Error::Storage(format!("read fan dir {}: {e}", fan.display()))
            })? {
                let sub = sub.map_err(|e| Error::Storage(format!("fan dir entry: {e}")))?;
                let name = sub.file_name();
                let name = name.to_string_lossy().to_string();
                if sub.path().is_dir() && !live_hashes.contains(&name) {
                    match std::fs::remove_dir_all(sub.path()) {
                        Ok(()) => removed += 1,
                        Err(e) => {
                            tracing::warn!(
                                "orphan sweep: remove {} failed: {e}",
                                sub.path().display()
                            );
                        }
                    }
                }
            }
        }
        Ok(removed)
    }
}

fn file_size(path: &Path) -> Option<u64> {
    std::fs::metadata(path).ok().map(|m| m.len())
}

fn decode_hex(s: &str) -> std::result::Result<Vec<u8>, ()> {
    let s = s.trim();
    if s.len() % 2 != 0 {
        return Err(());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| ()))
        .collect()
}

// ──── raft apply：manifest 状态迁移 + chunk 文件副作用 ────

/// 执行单个 ObjectStoreOp（raft apply 路径，SM 单写者串行）。
///
/// 约定：
/// - 任何分支都保证 `META_LAST_APPLIED == revision`（有状态变更时由
///   put/delete_at_revision 同事务写入；无变更的 no-op 分支单独持久化 applied，
///   避免重启后整段重放）；
/// - 返回 `ok`：op 是否达成了语义期望（服务层据此映射 gRPC 错误；
///   no-op/冲突/重放 → false，**不产生 raft 错误**——用户级竞态不许 wedge raft）；
/// - chunk 文件写失败（磁盘）→ Err（数据面异常，raft 挂起由水位/配额前置避免）。
pub fn apply_object_store_op(
    sm: &MvccStorage<RedbBackend>,
    op: &ObjectStoreOp,
    revision: u64,
    applied: AppliedLogId,
    chunks: Option<&ChunkStore>,
) -> Result<bool> {
    // 重放守卫：该 revision 已应用 → no-op
    if sm.changelog_contains_revision(revision)? {
        return Ok(false);
    }

    let (bucket, object_id) = match op {
        ObjectStoreOp::Begin { bucket, object_id, .. }
        | ObjectStoreOp::Chunk {
            bucket, object_id, ..
        }
        | ObjectStoreOp::Commit { bucket, object_id }
        | ObjectStoreOp::Delete { bucket, object_id } => (bucket.clone(), object_id.clone()),
    };
    let key = manifest_key(&bucket, &object_id);

    match op {
        ObjectStoreOp::Begin {
            total_size, started_at_unix, ..
        } => {
            // 已存在（Committed 或 Creating）→ 冲突；已删除（tombstone，get=None）
            // 允许重建。
            if sm.get(&key)?.is_some() {
                // no-op：持久化 applied 水位即可
                sm.persist_applied(revision, applied)?;
                return Ok(false);
            }
            if *total_size == 0 {
                sm.persist_applied(revision, applied)?;
                return Ok(false);
            }
            let m = ObjectManifest::creating(*total_size, revision, *started_at_unix);
            let bytes = m.to_bytes()?;
            let _ = sm.put_at_revision(&key, &bytes, None, revision, applied)?;
            Ok(true)
        }
        ObjectStoreOp::Chunk {
            seq,
            data,
            now_unix,
            ..
        } => {
            let store = chunks.ok_or_else(|| {
                Error::Internal("object chunk op on node without chunk store".into())
            })?;
            let Some(raw) = sm.get(&key)? else {
                // manifest 缺失（无 Begin / 已删除）：no-op 落水位
                sm.persist_applied(revision, applied)?;
                return Ok(false);
            };
            let Some(mut m) = ObjectManifest::from_bytes(&raw) else {
                return Err(Error::Internal("corrupt object manifest row".into()));
            };
            if m.committed {
                sm.persist_applied(revision, applied)?;
                return Ok(false);
            }
            if *seq as usize != m.chunks.len() {
                // 顺序错乱（并发上传/重试重叠）：no-op
                sm.persist_applied(revision, applied)?;
                return Ok(false);
            }
            if data.is_empty() || data.len() > store.limits.chunk_size {
                sm.persist_applied(revision, applied)?;
                return Ok(false);
            }
            if m.size + data.len() as u64 > m.total_size
                || m.total_size > store.limits.max_object_size
            {
                sm.persist_applied(revision, applied)?;
                return Ok(false);
            }
            // 1) chunk 文件落盘（幂等；失败 → Err，raft 挂起由前置防护避免）
            store.write_chunk(&bucket, &object_id, *seq, data)?;
            // 2) manifest 追加（put_at_revision：replay 守卫 + changelog + applied）
            let mut hasher = Sha256::new();
            hasher.update(data);
            let sha: [u8; 32] = hasher.finalize().into();
            m.size += data.len() as u64;
            m.chunks.push(ChunkRec {
                seq: *seq,
                len: data.len() as u64,
                sha256: sha,
            });
            m.last_revision = revision;
            m.last_write_at_unix = *now_unix;
            let bytes = m.to_bytes()?;
            let _ = sm.put_at_revision(&key, &bytes, None, revision, applied)?;
            Ok(true)
        }
        ObjectStoreOp::Commit { .. } => {
            let Some(raw) = sm.get(&key)? else {
                sm.persist_applied(revision, applied)?;
                return Ok(false);
            };
            let Some(mut m) = ObjectManifest::from_bytes(&raw) else {
                return Err(Error::Internal("corrupt object manifest row".into()));
            };
            if m.committed {
                // 幂等（重试 Commit）：视为成功
                sm.persist_applied(revision, applied)?;
                return Ok(true);
            }
            if m.size != m.total_size || m.chunks.is_empty() {
                // 字节数与 Begin 声明不符（客户端中断/撒谎）：拒绝，留待 GC
                sm.persist_applied(revision, applied)?;
                return Ok(false);
            }
            m.committed = true;
            m.last_revision = revision;
            let bytes = m.to_bytes()?;
            let _ = sm.put_at_revision(&key, &bytes, None, revision, applied)?;
            Ok(true)
        }
        ObjectStoreOp::Delete { .. } => {
            // 删除语义：对「存在（任意状态：Creating/Committed）」的对象
            // tombstone + 删除 chunk 文件（幂等；no-op 也消耗 revision）。
            let exists = sm.get(&key)?.is_some();
            // KV tombstone（无论是否存在都消耗 revision；持久化 applied）
            let _ = sm.delete_at_revision(&key, revision, applied)?;
            if exists {
                if let Some(store) = chunks {
                    let _ = store.delete_object_files(&bucket, &object_id);
                }
                Ok(true)
            } else {
                Ok(false)
            }
        }
    }
}

// ──── 读侧工具（Stat/Get/GC 共用；调用方先做 ReadIndex 屏障） ────

/// 读取对象 manifest（已删除/不存在 → None）
pub fn read_manifest(
    sm: &MvccStorage<RedbBackend>,
    bucket: &[u8],
    object_id: &[u8],
) -> Result<Option<ObjectManifest>> {
    let key = manifest_key(bucket, object_id);
    match sm.get(&key)? {
        Some(bytes) => ObjectManifest::from_bytes(&bytes)
            .map(Some)
            .ok_or_else(|| Error::Internal("corrupt object manifest row".into())),
        None => Ok(None),
    }
}

/// 列出全部对象 manifest（`/obj/m/` 前缀，跳过已删除），返回 (bucket, object_id, manifest)。
pub fn list_manifests(
    sm: &MvccStorage<RedbBackend>,
) -> Result<Vec<(Vec<u8>, Vec<u8>, ObjectManifest)>> {
    let rows = sm.range(MANIFEST_PREFIX, 0)?;
    let mut out = Vec::with_capacity(rows.len());
    for (key, value) in rows {
        let Some((bucket, object_id)) = parse_manifest_key(&key) else {
            continue;
        };
        if let Some(m) = ObjectManifest::from_bytes(&value) {
            out.push((bucket, object_id, m));
        }
    }
    Ok(out)
}

/// live 对象目录哈希集（孤儿回收用）
pub fn live_object_hashes(
    manifests: &[(Vec<u8>, Vec<u8>, ObjectManifest)],
) -> HashSet<String> {
    manifests
        .iter()
        .map(|(b, id, _)| object_dir_hash(b, id))
        .collect()
}

/// 从 manifest 读取单 chunk（校验长度与序号对应；sha 校验为可选项）
pub fn read_manifest_chunk(
    store: &ChunkStore,
    m: &ObjectManifest,
    bucket: &[u8],
    object_id: &[u8],
    seq: u32,
) -> Result<Vec<u8>> {
    let rec = m
        .chunks
        .get(seq as usize)
        .ok_or_else(|| Error::NotFound {
            resource: "object chunk",
            key: seq.to_string(),
        })?;
    let data = store.read_chunk(bucket, object_id, seq)?;
    if data.len() as u64 != rec.len {
        return Err(Error::Internal(format!(
            "chunk {seq} size mismatch: manifest {}, file {}",
            rec.len,
            data.len()
        )));
    }
    Ok(data)
}

#[allow(dead_code)]
fn _read_all<R: Read>(mut r: R) -> std::io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    r.read_to_end(&mut buf)?;
    Ok(buf)
}

// ──── 测试 ────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_ref_ok() {
        validate_ref(b"bucket", b"obj/1").unwrap();
    }

    #[test]
    fn test_validate_ref_rejects() {
        assert!(validate_ref(b"", b"x").is_err());
        assert!(validate_ref(b"a/b", b"x").is_err());
        assert!(validate_ref(b"\xff\xfe", b"x").is_err());
        assert!(validate_ref(b"b", b"").is_err());
        assert!(validate_ref(b"b", &[0u8; 1025]).is_err());
        assert!(validate_ref(&[0u8; 256], b"x").is_err());
    }

    #[test]
    fn test_manifest_key_roundtrip() {
        for (b, id) in [
            (b"bucket".to_vec(), b"o".to_vec()),
            (b"b".to_vec(), b"a/b/c".to_vec()),
            (b"x".to_vec(), vec![0u8, 1, 2]),
        ] {
            let k = manifest_key(&b, &id);
            let (pb, pid) = parse_manifest_key(&k).unwrap();
            assert_eq!(pb, b);
            assert_eq!(pid, id);
        }
        assert!(parse_manifest_key(b"/kv/x").is_none());
        assert!(parse_manifest_key(b"/obj/m/").is_none());
    }

    #[test]
    fn test_manifest_serde() {
        let m = ObjectManifest {
            total_size: 10,
            size: 10,
            chunks: vec![ChunkRec {
                seq: 0,
                len: 10,
                sha256: [7u8; 32],
            }],
            committed: true,
            create_revision: 1,
            last_revision: 2,
            started_at_unix: 100,
            last_write_at_unix: 200,
        };
        let bytes = m.to_bytes().unwrap();
        let m2 = ObjectManifest::from_bytes(&bytes).unwrap();
        assert_eq!(m2.size, 10);
        assert!(m2.committed);
        assert_eq!(m2.chunks[0].sha256, [7u8; 32]);
    }

    #[test]
    fn test_chunk_store_plain_roundtrip() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = ChunkStore::new(dir.path(), Arc::new(ObjectLimits::default()), None).unwrap();
        assert!(!dir.path().join("objects").exists());
        store.write_chunk(b"b", b"o", 0, b"hello").unwrap();
        store.write_chunk(b"b", b"o", 1, b"world").unwrap();
        assert!(store.chunk_file_exists(b"b", b"o", 0));
        assert_eq!(store.read_chunk(b"b", b"o", 0).unwrap(), b"hello");
        assert_eq!(store.read_chunk(b"b", b"o", 1).unwrap(), b"world");
        assert!(store.read_chunk(b"b", b"o", 2).is_err());
        assert!(store.usage_bytes().unwrap() >= 10);
        store.delete_object_files(b"b", b"o").unwrap();
        assert!(!store.chunk_file_exists(b"b", b"o", 0));
    }

    #[test]
    fn test_chunk_store_encrypted_roundtrip() {
        let dir = tempfile::TempDir::new().unwrap();
        let key = "ab".repeat(32); // 64 hex chars
        let store =
            ChunkStore::new(dir.path(), Arc::new(ObjectLimits::default()), Some(&key)).unwrap();
        store.write_chunk(b"b", b"o", 0, b"secret-data").unwrap();
        // 落盘不是明文
        let raw = std::fs::read(store.chunk_path(b"b", b"o", 0)).unwrap();
        assert!(!raw.windows(11).any(|w| w == b"secret-data"));
        assert_eq!(store.read_chunk(b"b", b"o", 0).unwrap(), b"secret-data");
    }

    #[test]
    fn test_chunk_store_bad_key() {
        let dir = tempfile::TempDir::new().unwrap();
        assert!(ChunkStore::new(dir.path(), Arc::new(ObjectLimits::default()), Some("zz")).is_err());
    }

    /// 测试辅助：递归收集 `<root>/objects/` 下 chunk-* 文件（不含 keys/）
    fn chunk_files_for_test(root: &Path) -> Vec<PathBuf> {
        let mut out = Vec::new();
        let mut stack = vec![root.join(OBJECTS_DIR)];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for e in entries.flatten() {
                let p = e.path();
                if p.file_name() == Some(std::ffi::OsStr::new(KEYS_DIR)) {
                    continue;
                }
                if p.is_dir() {
                    stack.push(p);
                } else if p
                    .file_name()
                    .map(|n| n.to_string_lossy().starts_with("chunk-"))
                    .unwrap_or(false)
                {
                    out.push(p);
                }
            }
        }
        out
    }

    #[test]
    fn test_usage_bytes_sums_chunk_files_excludes_keys() {
        let dir = tempfile::TempDir::new().unwrap();
        // 明文：usage = chunk 文件字节精确和
        let plain = ChunkStore::new(dir.path(), Arc::new(ObjectLimits::default()), None).unwrap();
        plain.write_chunk(b"b", b"o", 0, b"hello").unwrap(); // 5B
        plain.write_chunk(b"b", b"o", 1, b"world!").unwrap(); // 6B
        plain.write_chunk(b"b", b"o2", 0, b"abc").unwrap(); // 3B
        assert_eq!(plain.usage_bytes().unwrap(), 14);

        // 加密：usage = 落盘 chunk 文件字节合计（keys/ 目录 DEK 密文不计入）
        let key = "ab".repeat(32);
        let enc = ChunkStore::new(dir.path(), Arc::new(ObjectLimits::default()), Some(&key)).unwrap();
        enc.write_chunk(b"b", b"o", 2, b"zz").unwrap();
        let mut on_disk = 0u64;
        for f in chunk_files_for_test(dir.path()) {
            on_disk += std::fs::metadata(f).unwrap().len();
        }
        assert_eq!(enc.usage_bytes().unwrap(), on_disk);
    }

    #[test]
    fn test_orphan_sweep() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = ChunkStore::new(dir.path(), Arc::new(ObjectLimits::default()), None).unwrap();
        store.write_chunk(b"live", b"o1", 0, b"a").unwrap();
        store.write_chunk(b"dead", b"o2", 0, b"b").unwrap();
        let live: HashSet<String> = vec![object_dir_hash(b"live", b"o1")].into_iter().collect();
        assert_eq!(store.sweep_orphans(&live).unwrap(), 1);
        assert!(store.chunk_file_exists(b"live", b"o1", 0));
        assert!(!store.chunk_file_exists(b"dead", b"o2", 0));
        // keys 目录不被孤儿回收误删
        let key = "ab".repeat(32);
        let enc = ChunkStore::new(dir.path(), Arc::new(ObjectLimits::default()), Some(&key)).unwrap();
        enc.write_chunk(b"live", b"o1", 1, b"c").unwrap();
        let live2: HashSet<String> = vec![object_dir_hash(b"live", b"o1")].into_iter().collect();
        assert_eq!(enc.sweep_orphans(&live2).unwrap(), 0);
        assert!(dir.path().join("objects/keys/dek-00000001.bin").is_file());
    }

    /// v2 文件头：COBJ2 || key_id(4 BE) || nonce(12)；DEK 密文落盘 keys/
    #[test]
    fn test_chunk_store_encrypted_v2_header_and_key_files() {
        let dir = tempfile::TempDir::new().unwrap();
        let key = "ab".repeat(32); // 64 hex chars
        let store =
            ChunkStore::new(dir.path(), Arc::new(ObjectLimits::default()), Some(&key)).unwrap();
        assert_eq!(store.active_key_id(), 1);
        store.write_chunk(b"b", b"o", 0, b"payload-0").unwrap();
        let raw = std::fs::read(store.chunk_path(b"b", b"o", 0)).unwrap();
        assert_eq!(&raw[..5], CHUNK_MAGIC, "v2 magic expected");
        assert_eq!(u32::from_be_bytes(raw[5..9].try_into().unwrap()), 1);
        assert_eq!(store.read_chunk(b"b", b"o", 0).unwrap(), b"payload-0");
        let keys = dir.path().join("objects").join("keys");
        assert!(keys.join("dek-00000001.bin").is_file());
        assert!(keys.join("last_rotation").is_file());
        // 明文不可见
        assert!(!raw.windows(9).any(|w| w == b"payload-0"));
    }

    /// DEK 轮换：新 chunk 用新 key_id，旧 chunk 仍可读；重启（同根密钥）后
    /// 从磁盘加载全部 DEK，历史 chunk 可读。
    #[test]
    fn test_chunk_dek_rotation_old_readable_across_restart() {
        let dir = tempfile::TempDir::new().unwrap();
        let key = "ab".repeat(32);
        let store =
            ChunkStore::new(dir.path(), Arc::new(ObjectLimits::default()), Some(&key)).unwrap();
        store.write_chunk(b"b", b"o", 0, b"old-chunk").unwrap();
        assert_eq!(store.active_key_id(), 1);
        assert_eq!(store.rotate_dek().unwrap(), 2);
        assert_eq!(store.active_key_id(), 2);
        assert_eq!(store.dek_version_count(), 2);
        store.write_chunk(b"b", b"o", 1, b"new-chunk").unwrap();
        let raw1 = std::fs::read(store.chunk_path(b"b", b"o", 1)).unwrap();
        assert_eq!(&raw1[..5], CHUNK_MAGIC);
        assert_eq!(u32::from_be_bytes(raw1[5..9].try_into().unwrap()), 2);
        assert_eq!(store.read_chunk(b"b", b"o", 0).unwrap(), b"old-chunk");
        assert_eq!(store.read_chunk(b"b", b"o", 1).unwrap(), b"new-chunk");
        // 重启恢复
        let store2 =
            ChunkStore::new(dir.path(), Arc::new(ObjectLimits::default()), Some(&key)).unwrap();
        assert_eq!(store2.active_key_id(), 2);
        assert_eq!(store2.dek_version_count(), 2);
        assert_eq!(store2.read_chunk(b"b", b"o", 0).unwrap(), b"old-chunk");
        assert_eq!(store2.read_chunk(b"b", b"o", 1).unwrap(), b"new-chunk");
        assert!(dir.path().join("objects/keys/dek-00000002.bin").is_file());
    }

    /// v1（Phase A）格式兼容读：COBJ1 || nonce(12) || ct，根密钥直作 DEK
    #[test]
    fn test_chunk_v1_legacy_format_read_compat() {
        let dir = tempfile::TempDir::new().unwrap();
        let key_hex = "ab".repeat(32);
        let key_bytes = decode_hex(&key_hex).unwrap();
        let store =
            ChunkStore::new(dir.path(), Arc::new(ObjectLimits::default()), Some(&key_hex)).unwrap();
        let path = store.chunk_path(b"b", b"legacy", 0);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let cipher = Aes256Gcm::new_from_slice(&key_bytes).unwrap();
        let ct = cipher.encrypt(&nonce, b"legacy-payload".as_ref()).unwrap();
        let mut buf = Vec::new();
        buf.extend_from_slice(CHUNK_MAGIC_V1);
        buf.extend_from_slice(&nonce);
        buf.extend_from_slice(&ct);
        std::fs::write(&path, &buf).unwrap();
        assert_eq!(
            store.read_chunk(b"b", b"legacy", 0).unwrap(),
            b"legacy-payload"
        );
    }

    /// 明文模式：轮换 API 均为 no-op
    #[test]
    fn test_chunk_rotation_plain_noop() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = ChunkStore::new(dir.path(), Arc::new(ObjectLimits::default()), None).unwrap();
        assert_eq!(store.active_key_id(), 0);
        assert_eq!(store.rotate_dek().unwrap(), 0);
        assert_eq!(store.dek_version_count(), 0);
    }

    /// 到期自动轮换：写路径在间隔到期后推进 key_id
    #[test]
    fn test_chunk_auto_rotate_by_age() {
        let dir = tempfile::TempDir::new().unwrap();
        let key = "ab".repeat(32);
        let mut limits = ObjectLimits::default();
        limits.dek_rotation_secs = 1;
        let store = ChunkStore::new(dir.path(), Arc::new(limits), Some(&key)).unwrap();
        store.write_chunk(b"b", b"o", 0, b"a").unwrap();
        assert_eq!(store.active_key_id(), 1);
        std::thread::sleep(std::time::Duration::from_millis(1100));
        store.write_chunk(b"b", b"o", 1, b"b").unwrap();
        assert_eq!(store.active_key_id(), 2);
        assert_eq!(store.read_chunk(b"b", b"o", 0).unwrap(), b"a");
        assert_eq!(store.read_chunk(b"b", b"o", 1).unwrap(), b"b");
    }
}
