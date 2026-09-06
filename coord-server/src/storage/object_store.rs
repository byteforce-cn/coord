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
//   - chunk 静态加密为独立开关（默认关闭）：随机 nonce 写文件头，根密钥直接作
//     AES-256-GCM 数据密钥（v1；轮换/独立 DEK 治理见 STATUS.md 整改要点）；
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
/// chunk 文件头 magic（加密文件用）
const CHUNK_MAGIC: &[u8; 5] = b"COBJ1";
/// 加密根密钥长度（256-bit）
const ROOT_KEY_LEN: usize = 32;
/// GCM nonce 长度（96-bit）
const NONCE_LEN: usize = 12;

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
}

impl Default for ObjectLimits {
    fn default() -> Self {
        Self {
            chunk_size: 4 * 1024 * 1024,
            max_object_size: 256 * 1024 * 1024,
            quota_bytes: 0,
            upload_timeout_secs: 300,
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

/// chunk 静态加密上下文（None = 明文）
struct ChunkCrypto {
    cipher: Aes256Gcm,
}

impl ChunkCrypto {
    /// 根密钥（32 字节 hex64 解析）
    fn from_root_key_hex(hex_key: &str) -> Result<Self> {
        let key = decode_hex(hex_key)
            .map_err(|_| Error::InvalidArgument("object storage root key must be hex64".into()))?;
        if key.len() != ROOT_KEY_LEN {
            return Err(Error::InvalidArgument(
                "object storage root key must be 32 bytes (hex64)".into(),
            ));
        }
        let key_bytes: [u8; ROOT_KEY_LEN] = key
            .try_into()
            .map_err(|_| Error::InvalidArgument("root key length".into()))?;
        Ok(Self {
            cipher: Aes256Gcm::new_from_slice(&key_bytes)
                .map_err(|e| Error::Internal(format!("init chunk cipher: {e}")))?,
        })
    }
}

/// 单个 raft 实例（一个 Region / 根）的 chunk 文件存储。
/// 构造不创建任何目录（关闭时磁盘布局字节级不变）；首个对象写入时惰性建目录。
pub struct ChunkStore {
    root: PathBuf,
    crypto: Option<Arc<ChunkCrypto>>,
    pub limits: Arc<ObjectLimits>,
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
        let crypto = match encryption_root_key_hex {
            Some(k) => Some(Arc::new(ChunkCrypto::from_root_key_hex(k)?)),
            None => None,
        };
        Ok(Arc::new(Self {
            root: root.into(),
            crypto,
            limits,
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
                // 随机 nonce + 文件头：magic(5) || nonce(12) || ciphertext
                let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
                let ct = crypto
                    .cipher
                    .encrypt(&nonce, data)
                    .map_err(|e| Error::Internal(format!("chunk encrypt: {e}")))?;
                let mut buf = Vec::with_capacity(CHUNK_MAGIC.len() + NONCE_LEN + ct.len());
                buf.extend_from_slice(CHUNK_MAGIC);
                buf.extend_from_slice(&nonce);
                buf.extend_from_slice(&ct);
                buf
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
            Some(crypto) => {
                if bytes.len() < CHUNK_MAGIC.len() + NONCE_LEN {
                    return Err(Error::Internal(format!(
                        "corrupt encrypted chunk {} (too short)",
                        path.display()
                    )));
                }
                if &bytes[..CHUNK_MAGIC.len()] != CHUNK_MAGIC {
                    return Err(Error::Internal(format!(
                        "corrupt encrypted chunk {} (bad magic)",
                        path.display()
                    )));
                }
                let nonce: [u8; NONCE_LEN] =
                    bytes[CHUNK_MAGIC.len()..CHUNK_MAGIC.len() + NONCE_LEN]
                        .try_into()
                        .unwrap();
                let ct = &bytes[CHUNK_MAGIC.len() + NONCE_LEN..];
                crypto
                    .cipher
                    .decrypt(Nonce::from_slice(&nonce), ct)
                    .map_err(|_| {
                        Error::Internal(format!("chunk decrypt failed ({})", path.display()))
                    })
            }
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

    /// 已用磁盘字节（估算，遍历 chunk 文件；配额 admission 用）
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
            for sub in std::fs::read_dir(entry.path()).map_err(|e| {
                Error::Storage(format!("read object subdir: {e}"))
            })? {
                let sub = sub.map_err(|e| Error::Storage(format!("object subdir entry: {e}")))?;
                total += file_size(&sub.path()).unwrap_or(0);
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
    }
}
