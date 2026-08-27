// DEK 持久化存储（R-SEC-01 接线：`/_meta/dek/` 布局）
//
// - 密文 DEK 存于 `/_meta/dek/encrypted/{key_id_be4}`（TABLE_META）；
// - 上次轮换时间存于 `/_meta/dek/last_rotation`（8B BE unix 秒）。
// - 为节点本地数据（每个节点独立 Keyring 加密本地 store），不参与 raft 复制。
//
// 实现 `DekRotationStore` trait（P2-05），供 `spawn_dek_rotation_loop` 使用。

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use coord_core::error::{Error, Result};
use coord_core::storage::StorageBackend;

use crate::storage::mvcc::MvccStorage;
use crate::storage::mvcc::TABLE_META;
use crate::storage::redb_backend::RedbBackend;

use super::dek_rotation::DekRotationStore;
use super::key_management::EncryptedDek;

/// 密文 DEK 前缀（TABLE_META 内）：`/_meta/dek/encrypted/{key_id_be4}`
const DEK_ENCRYPTED_PREFIX: &[u8] = b"/_meta/dek/encrypted/";

/// 上次轮换时间键（TABLE_META 内）：`/_meta/dek/last_rotation`（8B BE unix 秒）
const DEK_LAST_ROTATION: &[u8] = b"/_meta/dek/last_rotation";

/// 编码密文 DEK 键
fn encode_dek_key(key_id: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(DEK_ENCRYPTED_PREFIX.len() + 4);
    key.extend_from_slice(DEK_ENCRYPTED_PREFIX);
    key.extend_from_slice(&key_id.to_be_bytes());
    key
}

/// 基于 MvccStorage 的 DEK 持久化存储。
///
/// 所有读写走 backend（redb 单实例共享），与业务数据同库但前缀隔离。
pub struct MvccDekStore {
    mvcc: Arc<MvccStorage<RedbBackend>>,
}

impl MvccDekStore {
    pub fn new(mvcc: Arc<MvccStorage<RedbBackend>>) -> Self {
        Self { mvcc }
    }

    /// 读取全部已持久化密文 DEK（按 key_id 升序）
    pub fn load_all_encrypted_deks(&self) -> Result<Vec<EncryptedDek>> {
        self.mvcc.backend().read(|tx| {
            let entries = tx.iter_prefix(TABLE_META, DEK_ENCRYPTED_PREFIX)?;
            let mut deks = Vec::new();
            for (key, value) in entries {
                let id_bytes: [u8; 4] = key[key.len() - 4..]
                    .try_into()
                    .map_err(|_| Error::DataCorruption("dek key id too short".into()))?;
                deks.push(EncryptedDek {
                    key_id: u32::from_be_bytes(id_bytes),
                    encrypted_bytes: value,
                });
            }
            Ok(deks)
        })
    }

    /// 持久化密文 DEK + 轮换时间（单写事务原子完成）
    fn persist_inner(&self, encrypted_dek: &EncryptedDek, rotated_at: SystemTime) -> Result<()> {
        encrypted_dek.validate()?;
        let key = encode_dek_key(encrypted_dek.key_id);
        let unix = rotated_at
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs();
        self.mvcc.backend().write(|tx| {
            tx.insert(TABLE_META, &key, &encrypted_dek.encrypted_bytes)?;
            tx.insert(TABLE_META, DEK_LAST_ROTATION, &unix.to_be_bytes())?;
            Ok(())
        })
    }
}

impl DekRotationStore for MvccDekStore {
    fn last_rotation(&self) -> Option<SystemTime> {
        self.mvcc
            .backend()
            .read(|tx| tx.get(TABLE_META, DEK_LAST_ROTATION))
            .ok()
            .flatten()
            .and_then(|bytes| {
                let arr: [u8; 8] = bytes.try_into().ok()?;
                Some(UNIX_EPOCH + Duration::from_secs(u64::from_be_bytes(arr)))
            })
    }

    fn persist(
        &self,
        encrypted_dek: &EncryptedDek,
        rotated_at: SystemTime,
    ) -> coord_core::error::Result<()> {
        self.persist_inner(encrypted_dek, rotated_at)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::redb_backend::RedbBackend;
    use coord_core::types::StorageConfig;
    use tempfile::TempDir;

    fn create_store() -> (TempDir, MvccDekStore) {
        let dir = TempDir::new().unwrap();
        let config = StorageConfig::default();
        let backend = RedbBackend::open(dir.path(), &config).unwrap();
        let mvcc = Arc::new(MvccStorage::new(backend).unwrap());
        let store = MvccDekStore::new(mvcc);
        (dir, store)
    }

    #[test]
    fn test_persist_and_load_roundtrip() {
        let (_dir, store) = create_store();
        assert!(store.load_all_encrypted_deks().unwrap().is_empty());
        assert!(store.last_rotation().is_none());

        let dek = EncryptedDek {
            key_id: 7,
            encrypted_bytes: vec![0x42u8; 60],
        };
        let now = SystemTime::now();
        store.persist(&dek, now).unwrap();

        let loaded = store.load_all_encrypted_deks().unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].key_id, 7);
        assert_eq!(loaded[0].encrypted_bytes, vec![0x42u8; 60]);
        assert!(store.last_rotation().is_some());
    }

    #[test]
    fn test_persist_multiple_key_ids() {
        let (_dir, store) = create_store();
        let now = SystemTime::now();
        store
            .persist(
                &EncryptedDek {
                    key_id: 1,
                    encrypted_bytes: vec![0x11u8; 60],
                },
                now,
            )
            .unwrap();
        store
            .persist(
                &EncryptedDek {
                    key_id: 3,
                    encrypted_bytes: vec![0x33u8; 60],
                },
                now,
            )
            .unwrap();
        let loaded = store.load_all_encrypted_deks().unwrap();
        assert_eq!(loaded.len(), 2);
        let ids: Vec<u32> = loaded.iter().map(|d| d.key_id).collect();
        assert_eq!(ids, vec![1, 3]);
    }
}
