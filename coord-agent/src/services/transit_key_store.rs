// coord-agent: TransitKeyStore —— 命名密钥档案/版本材料的共享存储抽象（Memory / Kv）
//
// ADR-0019 D2：一把密钥 = 一份档案 + 每版本一条**被 KEK 包裹**的材料。
// - 档案：`/_transit/keys/{name}`（JSON，含三个版本边界与版本元数据）
// - 材料：`/_transit/keymat/{name}/{version}`（被 KEK 包裹后的字节，明文材料不落盘）
//
// 并发：所有写路径都是 **Txn 版本化 CAS**（对照 `pki_store.rs` 的
// `replace_active_cert_cas`）。档案 key 的 version 是唯一串行化点：
// - 创建：Version==0 比较（并发创建只有一个胜者）；
// - 轮换/配置/裁剪：比较读取到的 version，冲突 ⇒ VersionConflict，调用方重读后重试；
// - 删除：同样比较 version，避免删掉并发轮换出来的新版本。
// 材料与档案在**同一个 Txn** 里写入，因此不存在「档案指向一条没写成功的材料」。
//
// 值经 coord-server redb 持久化 + Barrier 加密落库；本层不持有 KEK，也看不到明文材料。

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use coord_proto::kv::PutRequest;
use coord_proto::txn::compare::{CompareResult, Target, TargetValue};
use coord_proto::txn::request_op::Op;
use coord_proto::txn::{Compare, RequestOp};

use crate::proxy::AgentInner;
use crate::services::workflow_store::prefix_end;

/// 档案键前缀
pub const KEY_PREFIX: &[u8] = b"/_transit/keys/";

/// 版本材料键前缀（与档案前缀分开：名称可含 `/`，不能靠后缀解析区分两者）
pub const MATERIAL_PREFIX: &[u8] = b"/_transit/keymat/";

/// 当前 UNIX 秒
pub fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

// ──── 数据模型 ────

/// 密钥档案（持久化；不含任何材料）
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyArchive {
    /// 密钥名（调用方命名空间；PKI leaf/CA 也在同一档案里）
    pub name: String,
    /// 规范化 key type：aes256-gcm96 / ed25519 / ecdsa-p256 / ... / rsa-4096
    pub key_type: String,
    /// 最新版本（从 1 开始，单调递增）
    pub latest_version: u32,
    /// 低于该版本的密文/签名不可用（解密/验签拒绝）
    pub min_decryption_version: u32,
    /// 低于该版本的版本不可用于新加密/签名
    pub min_encryption_version: u32,
    /// 删除闸门：默认 false（ADR-0019 D6/D7 的防误删唯一闸门）
    pub deletion_allowed: bool,
    /// 是否允许 ExportKey
    pub exportable: bool,
    /// CA 密钥等「任何权限都不可导出」的档案（ADR-0019 D6）
    #[serde(default)]
    pub never_exportable: bool,
    /// 创建时间（UNIX 秒）
    pub created_at: i64,
    /// 现存版本元数据（升序；trim 后移除 < min_decryption_version 的条目）
    pub versions: Vec<KeyVersionMeta>,
}

/// 版本元数据（不含材料）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyVersionMeta {
    pub version: u32,
    pub created_at: i64,
}

/// 版本材料（只含 KEK 包裹后的字节）
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyVersionRecord {
    /// `nonce(12) || AES-256-GCM(material, KEK)`
    pub wrapped: Vec<u8>,
    pub created_at: i64,
    /// 包裹该材料的 KEK 标识（轮换窗口内按标识选材料；材料下线 ⇒ fail-loud）
    pub kek_id: String,
}

// ──── 错误 ────

#[derive(Debug)]
pub enum TransitKeyStoreError {
    /// 创建时键已存在（CAS Version==0 失败）
    AlreadyExists(String),
    /// 版本化替换的 key version 与预期不符（并发写），调用方应重读再决策
    VersionConflict(String),
    /// 底层 KV/Txn 错误
    Kv(String),
    /// 序列化/反序列化错误
    Serialization(String),
}

impl std::fmt::Display for TransitKeyStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyExists(key) => write!(f, "key already exists: {key}"),
            Self::VersionConflict(key) => write!(f, "version conflict on {key}"),
            Self::Kv(msg) => write!(f, "kv error: {msg}"),
            Self::Serialization(msg) => write!(f, "serialization error: {msg}"),
        }
    }
}

impl std::error::Error for TransitKeyStoreError {}

// ──── 键编码 ────

pub fn archive_key(name: &str) -> Vec<u8> {
    let mut key = KEY_PREFIX.to_vec();
    key.extend_from_slice(name.as_bytes());
    key
}

pub fn material_key(name: &str, version: u32) -> Vec<u8> {
    let mut key = MATERIAL_PREFIX.to_vec();
    key.extend_from_slice(name.as_bytes());
    key.push(b'/');
    key.extend_from_slice(version.to_string().as_bytes());
    key
}

fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>, TransitKeyStoreError> {
    serde_json::to_vec(value).map_err(|e| TransitKeyStoreError::Serialization(e.to_string()))
}

fn deserialize<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, TransitKeyStoreError> {
    serde_json::from_slice(bytes).map_err(|e| TransitKeyStoreError::Serialization(e.to_string()))
}

fn put_op(key: Vec<u8>, value: Vec<u8>) -> RequestOp {
    RequestOp {
        op: Some(Op::RequestPut(PutRequest {
            key,
            value,
            lease_id: 0,
            prev_kv: false,
            request_id: vec![],
        })),
    }
}

fn delete_op(key: Vec<u8>) -> RequestOp {
    RequestOp {
        op: Some(Op::RequestDelete(coord_proto::kv::DeleteRequest {
            key,
            range_end: vec![],
            prev_kv: false,
            request_id: vec![],
        })),
    }
}

fn version_compare(key: Vec<u8>, expected: i64) -> Compare {
    Compare {
        result: CompareResult::Equal as i32,
        target: Target::Version as i32,
        key,
        target_value: Some(TargetValue::Version(expected)),
    }
}

// ──── trait ────

/// 命名密钥档案存储
///
/// 生产实现 [`KvTransitKeyStore`]（coord-server 共享 KV + Txn CAS），
/// 开发/单测实现 [`MemoryTransitKeyStore`]（同样的 CAS 语义）。
#[async_trait]
pub trait TransitKeyStore: Send + Sync {
    /// 读取档案及 key version（版本化 CAS 用；无返回 Ok(None)）
    async fn get_key(&self, name: &str) -> Result<Option<(KeyArchive, i64)>, TransitKeyStoreError>;

    /// 全量列出档案（服务端按前缀过滤）
    async fn list_keys(&self) -> Result<Vec<KeyArchive>, TransitKeyStoreError>;

    /// 原子创建：档案 + 第一个版本材料（CAS Version==0）
    async fn create_key(
        &self,
        archive: &KeyArchive,
        material: &KeyVersionRecord,
    ) -> Result<(), TransitKeyStoreError>;

    /// 原子替换：档案 + 可选的新版本材料 + 待删除的旧版本材料（CAS Version==expected）
    async fn replace_key(
        &self,
        expected_version: i64,
        archive: &KeyArchive,
        new_material: Option<&KeyVersionRecord>,
        delete_versions: &[u32],
    ) -> Result<(), TransitKeyStoreError>;

    /// 原子删除：档案 + 全部版本材料（CAS Version==expected）
    async fn delete_key(
        &self,
        expected_version: i64,
        archive: &KeyArchive,
    ) -> Result<(), TransitKeyStoreError>;

    /// 读取某版本材料（不存在返回 Ok(None)）
    async fn get_material(
        &self,
        name: &str,
        version: u32,
    ) -> Result<Option<KeyVersionRecord>, TransitKeyStoreError>;
}

// ──── MemoryTransitKeyStore ────

/// 内存实现（开发/单测）：与 Kv 实现同口径的 CAS 语义。
#[derive(Default)]
pub struct MemoryTransitKeyStore {
    keys: Mutex<HashMap<String, (KeyArchive, i64)>>,
    materials: Mutex<HashMap<(String, u32), KeyVersionRecord>>,
}

impl MemoryTransitKeyStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl TransitKeyStore for MemoryTransitKeyStore {
    async fn get_key(&self, name: &str) -> Result<Option<(KeyArchive, i64)>, TransitKeyStoreError> {
        Ok(self.keys.lock().get(name).cloned())
    }

    async fn list_keys(&self) -> Result<Vec<KeyArchive>, TransitKeyStoreError> {
        let mut out: Vec<KeyArchive> = self
            .keys
            .lock()
            .values()
            .map(|(archive, _)| archive.clone())
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    async fn create_key(
        &self,
        archive: &KeyArchive,
        material: &KeyVersionRecord,
    ) -> Result<(), TransitKeyStoreError> {
        let mut keys = self.keys.lock();
        if keys.contains_key(&archive.name) {
            return Err(TransitKeyStoreError::AlreadyExists(archive.name.clone()));
        }
        self.materials.lock().insert(
            (archive.name.clone(), archive.latest_version),
            material.clone(),
        );
        keys.insert(archive.name.clone(), (archive.clone(), 1));
        Ok(())
    }

    async fn replace_key(
        &self,
        expected_version: i64,
        archive: &KeyArchive,
        new_material: Option<&KeyVersionRecord>,
        delete_versions: &[u32],
    ) -> Result<(), TransitKeyStoreError> {
        let mut keys = self.keys.lock();
        let current = keys
            .get(&archive.name)
            .map(|(_, version)| *version)
            .unwrap_or(0);
        if current != expected_version {
            return Err(TransitKeyStoreError::VersionConflict(archive.name.clone()));
        }
        let mut materials = self.materials.lock();
        if let Some(material) = new_material {
            materials.insert(
                (archive.name.clone(), archive.latest_version),
                material.clone(),
            );
        }
        for version in delete_versions {
            materials.remove(&(archive.name.clone(), *version));
        }
        keys.insert(
            archive.name.clone(),
            (archive.clone(), expected_version + 1),
        );
        Ok(())
    }

    async fn delete_key(
        &self,
        expected_version: i64,
        archive: &KeyArchive,
    ) -> Result<(), TransitKeyStoreError> {
        let mut keys = self.keys.lock();
        let current = keys
            .get(&archive.name)
            .map(|(_, version)| *version)
            .unwrap_or(0);
        if current != expected_version {
            return Err(TransitKeyStoreError::VersionConflict(archive.name.clone()));
        }
        keys.remove(&archive.name);
        self.materials
            .lock()
            .retain(|(name, _), _| name != &archive.name);
        Ok(())
    }

    async fn get_material(
        &self,
        name: &str,
        version: u32,
    ) -> Result<Option<KeyVersionRecord>, TransitKeyStoreError> {
        Ok(self
            .materials
            .lock()
            .get(&(name.to_string(), version))
            .cloned())
    }
}

// ──── KvTransitKeyStore ────

/// 生产实现：通过 AgentInner.client 的 KV/Txn 能力访问 coord-server 共享存储。
pub struct KvTransitKeyStore {
    inner: Arc<AgentInner>,
}

impl KvTransitKeyStore {
    pub fn new(inner: Arc<AgentInner>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl TransitKeyStore for KvTransitKeyStore {
    async fn get_key(&self, name: &str) -> Result<Option<(KeyArchive, i64)>, TransitKeyStoreError> {
        let key = archive_key(name);
        let (kvs, _count, _rev) = self
            .inner
            .client
            .kv()
            .range_with_lease_full(&key, &[], 1, 0, false, false)
            .await
            .map_err(|e| TransitKeyStoreError::Kv(e.to_string()))?;
        match kvs.first() {
            Some((_k, value, _lease, version)) => Ok(Some((deserialize(value)?, *version))),
            None => Ok(None),
        }
    }

    async fn list_keys(&self) -> Result<Vec<KeyArchive>, TransitKeyStoreError> {
        let range_end = prefix_end(KEY_PREFIX);
        let pairs = self
            .inner
            .client
            .kv()
            .range(KEY_PREFIX, &range_end, 0, 0)
            .await
            .map_err(|e| TransitKeyStoreError::Kv(e.to_string()))?;
        let mut out: Vec<KeyArchive> = Vec::with_capacity(pairs.len());
        for (_k, value) in pairs {
            out.push(deserialize(&value)?);
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    async fn create_key(
        &self,
        archive: &KeyArchive,
        material: &KeyVersionRecord,
    ) -> Result<(), TransitKeyStoreError> {
        let key = archive_key(&archive.name);
        let archive_value = serialize(archive)?;
        let material_value = serialize(material)?;
        let compares = vec![version_compare(key.clone(), 0)];
        let ops = vec![
            put_op(key.clone(), archive_value),
            put_op(
                material_key(&archive.name, archive.latest_version),
                material_value,
            ),
        ];
        let resp = self
            .inner
            .client
            .txn()
            .txn(compares, ops, vec![])
            .await
            .map_err(|e| TransitKeyStoreError::Kv(e.to_string()))?;
        if resp.succeeded {
            Ok(())
        } else {
            Err(TransitKeyStoreError::AlreadyExists(
                String::from_utf8_lossy(&key).into_owned(),
            ))
        }
    }

    async fn replace_key(
        &self,
        expected_version: i64,
        archive: &KeyArchive,
        new_material: Option<&KeyVersionRecord>,
        delete_versions: &[u32],
    ) -> Result<(), TransitKeyStoreError> {
        let key = archive_key(&archive.name);
        let archive_value = serialize(archive)?;
        let compares = vec![version_compare(key.clone(), expected_version)];

        let mut ops = Vec::with_capacity(2 + delete_versions.len());
        if let Some(material) = new_material {
            ops.push(put_op(
                material_key(&archive.name, archive.latest_version),
                serialize(material)?,
            ));
        }
        for version in delete_versions {
            ops.push(delete_op(material_key(&archive.name, *version)));
        }
        ops.push(put_op(key.clone(), archive_value));

        let resp = self
            .inner
            .client
            .txn()
            .txn(compares, ops, vec![])
            .await
            .map_err(|e| TransitKeyStoreError::Kv(e.to_string()))?;
        if resp.succeeded {
            Ok(())
        } else {
            Err(TransitKeyStoreError::VersionConflict(
                String::from_utf8_lossy(&key).into_owned(),
            ))
        }
    }

    async fn delete_key(
        &self,
        expected_version: i64,
        archive: &KeyArchive,
    ) -> Result<(), TransitKeyStoreError> {
        let key = archive_key(&archive.name);
        let compares = vec![version_compare(key.clone(), expected_version)];
        let mut ops = Vec::with_capacity(1 + archive.versions.len());
        ops.push(delete_op(key.clone()));
        for meta in &archive.versions {
            ops.push(delete_op(material_key(&archive.name, meta.version)));
        }
        let resp = self
            .inner
            .client
            .txn()
            .txn(compares, ops, vec![])
            .await
            .map_err(|e| TransitKeyStoreError::Kv(e.to_string()))?;
        if resp.succeeded {
            Ok(())
        } else {
            Err(TransitKeyStoreError::VersionConflict(
                String::from_utf8_lossy(&key).into_owned(),
            ))
        }
    }

    async fn get_material(
        &self,
        name: &str,
        version: u32,
    ) -> Result<Option<KeyVersionRecord>, TransitKeyStoreError> {
        let key = material_key(name, version);
        let pairs = self
            .inner
            .client
            .kv()
            .range(&key, &[], 1, 0)
            .await
            .map_err(|e| TransitKeyStoreError::Kv(e.to_string()))?;
        match pairs.first() {
            Some((_k, value)) => Ok(Some(deserialize(value)?)),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn archive(name: &str, latest: u32) -> KeyArchive {
        KeyArchive {
            name: name.into(),
            key_type: "aes256-gcm96".into(),
            latest_version: latest,
            min_decryption_version: 1,
            min_encryption_version: 1,
            deletion_allowed: false,
            exportable: false,
            never_exportable: false,
            created_at: 1,
            versions: (1..=latest)
                .map(|version| KeyVersionMeta {
                    version,
                    created_at: 1,
                })
                .collect(),
        }
    }

    fn material(byte: u8) -> KeyVersionRecord {
        KeyVersionRecord {
            wrapped: vec![byte; 60],
            created_at: 1,
            kek_id: "kek-1".into(),
        }
    }

    #[tokio::test]
    async fn create_conflict_is_already_exists() {
        let store = MemoryTransitKeyStore::new();
        store
            .create_key(&archive("k", 1), &material(1))
            .await
            .unwrap();
        let err = store
            .create_key(&archive("k", 1), &material(2))
            .await
            .unwrap_err();
        assert!(matches!(err, TransitKeyStoreError::AlreadyExists(_)));
    }

    #[tokio::test]
    async fn replace_uses_expected_version_and_is_monotonic() {
        let store = MemoryTransitKeyStore::new();
        store
            .create_key(&archive("k", 1), &material(1))
            .await
            .unwrap();
        let (mut found, version) = store.get_key("k").await.unwrap().unwrap();
        assert_eq!(version, 1);

        found.latest_version = 2;
        found.versions.push(KeyVersionMeta {
            version: 2,
            created_at: 2,
        });
        store
            .replace_key(version, &found, Some(&material(2)), &[])
            .await
            .unwrap();
        let (_found, version) = store.get_key("k").await.unwrap().unwrap();
        assert_eq!(version, 2);
        assert_eq!(
            store.get_material("k", 2).await.unwrap().unwrap(),
            material(2)
        );

        // 旧 version 再替换 ⇒ 冲突（无丢更新）
        let err = store
            .replace_key(1, &found, Some(&material(3)), &[])
            .await
            .unwrap_err();
        assert!(matches!(err, TransitKeyStoreError::VersionConflict(_)));
    }

    #[tokio::test]
    async fn delete_removes_archive_and_all_materials() {
        let store = MemoryTransitKeyStore::new();
        store
            .create_key(&archive("k", 1), &material(1))
            .await
            .unwrap();
        let (found, version) = store.get_key("k").await.unwrap().unwrap();
        store.delete_key(version, &found).await.unwrap();
        assert!(store.get_key("k").await.unwrap().is_none());
        assert!(store.get_material("k", 1).await.unwrap().is_none());
    }

    #[test]
    fn key_encoding_is_prefix_separated() {
        assert_eq!(archive_key("a/b"), b"/_transit/keys/a/b".to_vec());
        assert_eq!(material_key("a/b", 3), b"/_transit/keymat/a/b/3".to_vec());
        assert!(!material_key("a/b", 3).starts_with(KEY_PREFIX));
    }
}
