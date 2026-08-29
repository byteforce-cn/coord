// Raft LogStore — Openraft RaftLogStorage + RaftLogReader 实现
//
// 使用 Redb 独立实例持久化 Raft Log（ADP §12.5）。
// 与业务数据 store.db 隔离，避免 Raft Log 频繁写入影响业务读写性能。
//
// 物理布局：
//   <data_dir>/raft-log/log.db  — Raft Log 条目、Vote、Committed、Last Purged

use std::fmt::Debug;
use std::io;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use openraft::storage::{IOFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::type_config::alias::{EntryOf, LogIdOf, VoteOf};
use openraft::OptionalSend;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use super::type_config::TypeConfig;
use crate::storage::snapshot::SnapshotTracker;

// ──── Redb 表定义 ────

/// Raft Log 条目表：Key = index (u64 BE), Value = bincode 序列化的 Entry
const TABLE_LOG: TableDefinition<&[u8], &[u8]> = TableDefinition::new("raft_log");

/// Vote 表：单条记录 Key = b"vote"
const TABLE_VOTE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("raft_vote");

/// Committed 表：单条记录 Key = b"committed"
const TABLE_COMMITTED: TableDefinition<&[u8], &[u8]> = TableDefinition::new("raft_committed");

/// Last Purged 表：单条记录 Key = b"last_purged"
const TABLE_LAST_PURGED: TableDefinition<&[u8], &[u8]> = TableDefinition::new("raft_last_purged");

// ──── 内部 Key 常量 ────

const KEY_VOTE: &[u8] = b"vote";
const KEY_COMMITTED: &[u8] = b"committed";
const KEY_LAST_PURGED: &[u8] = b"last_purged";

// ──── 序列化工具 ────

fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>, io::Error> {
    bincode::serialize(value).map_err(io::Error::other)
}

fn deserialize<'a, T: Deserialize<'a>>(data: &'a [u8]) -> Result<T, io::Error> {
    bincode::deserialize(data).map_err(io::Error::other)
}

/// 将 index 编码为 Redb Key（u64 大端）
fn index_key(index: u64) -> [u8; 8] {
    index.to_be_bytes()
}

// ──── LogStore ────

/// 基于 Redb 持久化的 Raft LogStore
///
/// 线程安全（内部 `Arc<Database>`），支持 Clone。
/// 所有写入操作通过 Redb 写事务原子提交。
///
/// purge 前置条件（P0-A/M0-5）：若设置了 `SnapshotTracker`，删除日志前必须
/// 存在覆盖 purge 点的已落盘快照，否则拒绝（防止"无快照 + 日志已删"不可恢复态）。
#[derive(Debug, Clone)]
pub struct LogStore {
    db: Arc<Database>,
    #[allow(dead_code)]
    path: PathBuf,
    /// 快照持久化守卫（由 main.rs 在创建 Raft 实例前注入）
    snapshot_tracker: Option<Arc<SnapshotTracker>>,
}

impl LogStore {
    /// 创建/打开 Raft Log 数据库
    ///
    /// `data_dir` 为 Coord 数据根目录，Raft Log 存储在 `<data_dir>/raft-log/log.db`。
    pub async fn new(data_dir: &Path) -> Result<Self, io::Error> {
        let raft_log_dir = data_dir.join("raft-log");
        std::fs::create_dir_all(&raft_log_dir).map_err(|e| {
            io::Error::other(format!(
                "create raft-log dir {}: {e}",
                raft_log_dir.display()
            ))
        })?;

        let log_path = raft_log_dir.join("log.db");
        let db = if log_path.exists() {
            Database::open(&log_path).map_err(|e| {
                io::Error::other(format!("open raft log db {}: {e}", log_path.display()))
            })?
        } else {
            Database::create(&log_path).map_err(|e| {
                io::Error::other(format!("create raft log db {}: {e}", log_path.display()))
            })?
        };

        // 确保所有表已创建
        {
            let write_tx = db
                .begin_write()
                .map_err(|e| io::Error::other(format!("begin init write tx: {e}")))?;
            {
                let _ = write_tx.open_table(TABLE_LOG);
                let _ = write_tx.open_table(TABLE_VOTE);
                let _ = write_tx.open_table(TABLE_COMMITTED);
                let _ = write_tx.open_table(TABLE_LAST_PURGED);
            }
            write_tx
                .commit()
                .map_err(|e| io::Error::other(format!("commit init tx: {e}")))?;
        }

        Ok(Self {
            db: Arc::new(db),
            path: raft_log_dir,
            snapshot_tracker: None,
        })
    }

    /// 注入快照持久化守卫（M0-5：创建 Raft 实例前调用）
    pub fn with_snapshot_tracker(mut self, tracker: Arc<SnapshotTracker>) -> Self {
        self.snapshot_tracker = Some(tracker);
        self
    }

    /// 读取已 purge 的日志位置（启动一致性检查用）
    pub fn last_purged(&self) -> Result<Option<LogIdOf<TypeConfig>>, io::Error> {
        self.read_meta(TABLE_LAST_PURGED, KEY_LAST_PURGED)
    }

    /// 同步点读指定 index 的日志条目。
    ///
    /// 读路径一致性校验用（防陈旧读）：对比状态机 `last_applied` 与本地日志中
    /// 同 index 的实际条目，检测"幻影态"（状态机应用过后来被新 leader 截断的条目）。
    /// 若该 index 已被 purge（快照覆盖），返回 `Ok(None)` 由调用方结合
    /// [`LogStore::last_purged`] 判断。
    pub fn get_entry_at(&self, index: u64) -> Result<Option<EntryOf<TypeConfig>>, io::Error> {
        let read_tx = self
            .db
            .begin_read()
            .map_err(|e| io::Error::other(format!("begin read tx: {e}")))?;
        let table = read_tx
            .open_table(TABLE_LOG)
            .map_err(|e| io::Error::other(format!("open log table: {e}")))?;
        let key_bytes = index_key(index);
        match table
            .get(key_bytes.as_slice())
            .map_err(|e| io::Error::other(format!("get log[{index}]: {e}")))?
        {
            Some(guard) => {
                let entry: EntryOf<TypeConfig> = deserialize(guard.value())?;
                Ok(Some(entry))
            }
            None => Ok(None),
        }
    }

    /// 检查 Raft 集群是否已初始化（存在已提交的日志即为已初始化）
    ///
    /// 通过检查 committed 元数据判断，比依赖 `raft.metrics()` 更可靠，
    /// 因为后者在 `Raft::new()` 返回后可能尚未被异步 core task 填充。
    pub fn is_initialized(&self) -> Result<bool, io::Error> {
        let committed: Option<LogIdOf<TypeConfig>> =
            self.read_meta(TABLE_COMMITTED, KEY_COMMITTED)?;
        Ok(committed.is_some())
    }

    // ──── 内部辅助方法 ────

    /// 读取单条元数据（vote/committed/last_purged）
    fn read_meta<T: for<'a> Deserialize<'a>>(
        &self,
        table_def: TableDefinition<&[u8], &[u8]>,
        key: &[u8],
    ) -> Result<Option<T>, io::Error> {
        let read_tx = self
            .db
            .begin_read()
            .map_err(|e| io::Error::other(format!("begin read tx: {e}")))?;
        let table = read_tx
            .open_table(table_def)
            .map_err(|e| io::Error::other(format!("open table: {e}")))?;
        match table
            .get(key)
            .map_err(|e| io::Error::other(format!("get key: {e}")))?
        {
            Some(guard) => {
                let data = guard.value();
                let value = deserialize(data)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// 写入单条元数据
    fn write_meta<T: Serialize>(
        &self,
        table_def: TableDefinition<&[u8], &[u8]>,
        key: &[u8],
        value: &T,
    ) -> Result<(), io::Error> {
        let write_tx = self
            .db
            .begin_write()
            .map_err(|e| io::Error::other(format!("begin write tx: {e}")))?;
        {
            let mut table = write_tx
                .open_table(table_def)
                .map_err(|e| io::Error::other(format!("open table: {e}")))?;
            let data = serialize(value)?;
            table
                .insert(key, data.as_slice())
                .map_err(|e| io::Error::other(format!("insert: {e}")))?;
        }
        write_tx
            .commit()
            .map_err(|e| io::Error::other(format!("commit write tx: {e}")))?;
        Ok(())
    }

    /// 删除单条元数据
    fn remove_meta(
        &self,
        table_def: TableDefinition<&[u8], &[u8]>,
        key: &[u8],
    ) -> Result<(), io::Error> {
        let write_tx = self
            .db
            .begin_write()
            .map_err(|e| io::Error::other(format!("begin write tx: {e}")))?;
        {
            let mut table = write_tx
                .open_table(table_def)
                .map_err(|e| io::Error::other(format!("open table: {e}")))?;
            table
                .remove(key)
                .map_err(|e| io::Error::other(format!("remove: {e}")))?;
        }
        write_tx
            .commit()
            .map_err(|e| io::Error::other(format!("commit write tx: {e}")))?;
        Ok(())
    }
}

// ──── RaftLogReader ────

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<EntryOf<TypeConfig>>, io::Error> {
        let read_tx = self
            .db
            .begin_read()
            .map_err(|e| io::Error::other(format!("begin read tx: {e}")))?;
        let table = read_tx
            .open_table(TABLE_LOG)
            .map_err(|e| io::Error::other(format!("open log table: {e}")))?;

        let start = match range.start_bound() {
            std::ops::Bound::Included(i) => *i,
            std::ops::Bound::Excluded(i) => *i + 1,
            std::ops::Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(i) => Some(*i),
            std::ops::Bound::Excluded(i) => Some(i.saturating_sub(1)),
            std::ops::Bound::Unbounded => None,
        };

        let mut entries = Vec::new();
        // 使用大端编码 Key 扫描，利用 Redb 的 B-Tree 有序性
        for idx in start.. {
            if let Some(end_idx) = end {
                if idx > end_idx {
                    break;
                }
            }
            let key_bytes = index_key(idx);
            match table
                .get(key_bytes.as_slice())
                .map_err(|e| io::Error::other(format!("get log[{}]: {e}", idx)))?
            {
                Some(guard) => {
                    let data = guard.value();
                    let entry: EntryOf<TypeConfig> = deserialize(data)?;
                    entries.push(entry);
                }
                None => break, // 到达日志末尾
            }
        }
        Ok(entries)
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<TypeConfig>>, io::Error> {
        self.read_meta(TABLE_VOTE, KEY_VOTE)
    }
}

// ──── RaftLogStorage ────

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, io::Error> {
        let last_purged: Option<LogIdOf<TypeConfig>> =
            self.read_meta(TABLE_LAST_PURGED, KEY_LAST_PURGED)?;

        // 找到最后一条日志条目
        let read_tx = self
            .db
            .begin_read()
            .map_err(|e| io::Error::other(format!("begin read tx: {e}")))?;
        let table = read_tx
            .open_table(TABLE_LOG)
            .map_err(|e| io::Error::other(format!("open log table: {e}")))?;

        // 直接从 B-Tree 取最大 index（O(1)，无扫描上限；P0-A.2）。
        // 旧实现从 committed 线索向后最多扫描 1000 条，committed 落后时漏报尾部。
        let last = {
            let guard = table
                .last()
                .map_err(|e| io::Error::other(format!("get last log: {e}")))?;
            guard
                .map(|(_, v)| {
                    let entry: EntryOf<TypeConfig> = deserialize(v.value())?;
                    Ok::<_, io::Error>(entry.log_id)
                })
                .transpose()?
        };

        Ok(LogState {
            last_log_id: last,
            last_purged_log_id: last_purged,
        })
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<TypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let write_tx = self
            .db
            .begin_write()
            .map_err(|e| io::Error::other(format!("begin write tx: {e}")))?;
        {
            let mut table = write_tx
                .open_table(TABLE_LOG)
                .map_err(|e| io::Error::other(format!("open log table: {e}")))?;
            for entry in entries {
                let idx = entry.log_id.index;
                let data = serialize(&entry)?;
                table
                    .insert(index_key(idx).as_slice(), data.as_slice())
                    .map_err(|e| io::Error::other(format!("insert log[{}]: {e}", idx)))?;
            }
        }
        write_tx
            .commit()
            .map_err(|e| io::Error::other(format!("commit append tx: {e}")))?;

        // Notify Raft that the log entries have been durably written to disk.
        // Without this callback, the Raft will never commit the entries and
        // client_write will hang forever (ADP §3.3, Openraft IOFlushed contract).
        callback.io_completed(Ok(()));

        Ok(())
    }

    async fn truncate_after(
        &mut self,
        last_log_id: Option<LogIdOf<TypeConfig>>,
    ) -> Result<(), io::Error> {
        let start_idx = last_log_id
            .as_ref()
            .map(|lid| lid.index.saturating_add(1))
            .unwrap_or(0);

        let write_tx = self
            .db
            .begin_write()
            .map_err(|e| io::Error::other(format!("begin write tx: {e}")))?;
        {
            let mut table = write_tx
                .open_table(TABLE_LOG)
                .map_err(|e| io::Error::other(format!("open log table: {e}")))?;
            // 从 start_idx 开始删除，直到找不到 key
            for idx in start_idx.. {
                let key_bytes = index_key(idx);
                match table.remove(key_bytes.as_slice()) {
                    Ok(Some(_)) => {}  // 已删除，继续
                    Ok(None) => break, // key 不存在，到达末尾
                    Err(e) => {
                        return Err(io::Error::other(format!("remove log[{}]: {e}", idx)));
                    }
                }
            }
        }
        write_tx
            .commit()
            .map_err(|e| io::Error::other(format!("commit truncate tx: {e}")))?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogIdOf<TypeConfig>) -> Result<(), io::Error> {
        // P0-A/M0-5 前置条件：存在覆盖 purge 点的已落盘快照才允许删除日志。
        // openraft 仅在快照构建成功后触发 purge，此守卫防止任何顺序颠倒/回退路径。
        if let Some(ref tracker) = self.snapshot_tracker {
            if !tracker.durable_covers(log_id.index) {
                return Err(io::Error::other(
                    format!(
                        "refusing to purge logs up to index {}: no durable snapshot covering this index",
                        log_id.index
                    ),
                ));
            }
        }

        let write_tx = self
            .db
            .begin_write()
            .map_err(|e| io::Error::other(format!("begin write tx: {e}")))?;
        {
            let mut table = write_tx
                .open_table(TABLE_LOG)
                .map_err(|e| io::Error::other(format!("open log table: {e}")))?;
            // 删除 [0, log_id.index] 范围内的所有日志条目
            for idx in 0..=log_id.index {
                let key_bytes = index_key(idx);
                let _ = table.remove(key_bytes.as_slice()); // 忽略 KeyNotFound
            }
            // 更新 last_purged
            let data = serialize(&log_id)?;
            let mut purged_table = write_tx
                .open_table(TABLE_LAST_PURGED)
                .map_err(|e| io::Error::other(format!("open purged table: {e}")))?;
            purged_table
                .insert(KEY_LAST_PURGED, data.as_slice())
                .map_err(|e| io::Error::other(format!("insert last_purged: {e}")))?;
        }
        write_tx
            .commit()
            .map_err(|e| io::Error::other(format!("commit purge tx: {e}")))?;
        Ok(())
    }

    async fn save_vote(&mut self, vote: &VoteOf<TypeConfig>) -> Result<(), io::Error> {
        self.write_meta(TABLE_VOTE, KEY_VOTE, vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogIdOf<TypeConfig>>,
    ) -> Result<(), io::Error> {
        match committed {
            Some(ref log_id) => self.write_meta(TABLE_COMMITTED, KEY_COMMITTED, log_id),
            None => self.remove_meta(TABLE_COMMITTED, KEY_COMMITTED),
        }
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<TypeConfig>>, io::Error> {
        self.read_meta(TABLE_COMMITTED, KEY_COMMITTED)
    }
}

// ──── 测试 ────

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::entry::RaftEntry;

    // ──── 序列化工具 ────

    #[test]
    fn test_index_key_zero() {
        assert_eq!(index_key(0), [0, 0, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn test_index_key_max() {
        assert_eq!(
            index_key(u64::MAX),
            [255, 255, 255, 255, 255, 255, 255, 255]
        );
    }

    #[test]
    fn test_index_key_ordering() {
        let k1 = index_key(1);
        let k2 = index_key(2);
        let k256 = index_key(256);
        assert!(k1 < k2);
        assert!(k2 < k256);
    }

    #[test]
    fn test_serialize_deserialize_u64() {
        let val: u64 = 42;
        let bytes = serialize(&val).unwrap();
        let decoded: u64 = deserialize(&bytes).unwrap();
        assert_eq!(decoded, 42);
    }

    #[test]
    fn test_serialize_deserialize_tuple() {
        let val: (u64, String) = (7, "test".to_string());
        let bytes = serialize(&val).unwrap();
        let decoded: (u64, String) = deserialize(&bytes).unwrap();
        assert_eq!(decoded, (7, "test".to_string()));
    }

    // ──── LogStore 创建与元数据操作 ────

    fn create_test_log_store() -> LogStore {
        let dir = std::env::temp_dir().join(format!(
            "coord-test-logstore-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async { LogStore::new(&dir).await.unwrap() })
    }

    #[test]
    fn test_log_store_create_empty() {
        let mut store = create_test_log_store();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let vote = store.read_vote().await.unwrap();
            assert!(vote.is_none());

            let committed = store.read_committed().await.unwrap();
            assert!(committed.is_none());

            let state = store.get_log_state().await.unwrap();
            assert!(state.last_log_id.is_none());
            assert!(state.last_purged_log_id.is_none());
        });
    }

    #[test]
    fn test_log_store_vote_crud() {
        let mut store = create_test_log_store();
        let rt = tokio::runtime::Runtime::new().unwrap();

        let vote = VoteOf::<TypeConfig>::new(5, 1);
        rt.block_on(async {
            store.save_vote(&vote).await.unwrap();
            let read = store.read_vote().await.unwrap();
            assert_eq!(read, Some(vote));
        });
    }

    #[test]
    fn test_log_store_committed_crud() {
        let mut store = create_test_log_store();
        let rt = tokio::runtime::Runtime::new().unwrap();

        let log_id = LogIdOf::<TypeConfig>::new(
            openraft::impls::leader_id_adv::LeaderId {
                term: 1u64,
                node_id: 0u64,
            },
            10,
        );
        rt.block_on(async {
            store.save_committed(Some(log_id.clone())).await.unwrap();
            let read = store.read_committed().await.unwrap();
            assert_eq!(read, Some(log_id));
        });
    }

    #[test]
    fn test_log_store_clear_committed() {
        let mut store = create_test_log_store();
        let rt = tokio::runtime::Runtime::new().unwrap();

        let log_id = LogIdOf::<TypeConfig>::new(
            openraft::impls::leader_id_adv::LeaderId {
                term: 1u64,
                node_id: 0u64,
            },
            10,
        );
        rt.block_on(async {
            store.save_committed(Some(log_id)).await.unwrap();
            store.save_committed(None).await.unwrap();
            let read = store.read_committed().await.unwrap();
            assert!(read.is_none());
        });
    }

    /// P0-A.2 回归：`get_log_state` 不得依赖 committed 线索的有限扫描。
    ///
    /// 旧实现从 committed 索引向后最多扫描 1000 条；当 committed 落后
    /// （如重启后 committed 尚未持久化、或日志尾部远超 committed）时
    /// 会漏报尾部日志，导致 openraft 认为日志为空/截断。
    #[test]
    fn test_get_log_state_no_scan_cap_regression() {
        let mut store = create_test_log_store();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            // committed 线索落后于日志尾部：仅有 index=5 的线索，日志写到 1500
            let hint = LogIdOf::<TypeConfig>::new(
                openraft::impls::leader_id_adv::LeaderId {
                    term: 1u64,
                    node_id: 1u64,
                },
                5,
            );
            store.save_committed(Some(hint)).await.unwrap();

            let entries: Vec<EntryOf<TypeConfig>> = (1u64..=1500)
                .map(|i| {
                    EntryOf::<TypeConfig>::new_blank(LogIdOf::<TypeConfig>::new(
                        openraft::impls::leader_id_adv::LeaderId {
                            term: 1u64,
                            node_id: 1u64,
                        },
                        i,
                    ))
                })
                .collect();
            store.append(entries, IOFlushed::noop()).await.unwrap();

            let state = store.get_log_state().await.unwrap();
            assert_eq!(state.last_log_id.map(|l| l.index), Some(1500));
        });
    }

    #[test]
    fn test_get_log_state_empty_without_committed() {
        let mut store = create_test_log_store();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let state = store.get_log_state().await.unwrap();
            assert!(state.last_log_id.is_none());
        });
    }
}
