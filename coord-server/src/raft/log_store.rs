// Raft LogStore — Openraft RaftLogStorage + RaftLogReader 实现
//
// 使用 Redb 独立实例持久化 Raft Log。
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

/// Raft Log 条目表：Key = index (u64 BE)，Value = 统一信封包裹的 Entry
/// （V1/V2/无前缀读兼容见 `crate::storage::envelope`）
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

// ──── 序列化工具（仅用于本模块持久化行） ────
//
// 行值统一为格式信封（`crate::storage::envelope`）：写路径一律带魔数/版本
// 前缀（V1 bincode）；读路径兼容无前缀旧行与 V1 / V2（postcard）。四项表
// （日志条目/Vote/Committed/LastPurged）共用这两个函数，保证前缀口径一致。

fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>, io::Error> {
    crate::storage::envelope::encode(value)
        .map_err(|e| io::Error::other(format!("encode raft row: {e}")))
}

fn deserialize<'a, T: Deserialize<'a>>(data: &'a [u8]) -> Result<T, io::Error> {
    crate::storage::envelope::decode(data)
        .map_err(|e| io::Error::other(format!("decode raft row: {e}")))
}

/// 将 index 编码为 Redb Key（u64 大端）
fn index_key(index: u64) -> [u8; 8] {
    index.to_be_bytes()
}

/// 校验解码条目的 index 与行 key 一致。
///
/// 行 key 由 `entry.log_id.index` 生成（写入路径唯一），不一致的行不可能来自
/// 正常写入——是格式信封/字节损坏后“宽松解码”出垃圾值的兜底拦截（篡改魔数时
/// 只能走旧格式路径，例如 bincode 对 Option 的非 0/1 字节按 `Some` 宽松接受）。
fn check_entry_key(key_bytes: &[u8], entry: &EntryOf<TypeConfig>) -> Result<(), io::Error> {
    if key_bytes != index_key(entry.log_id.index) {
        return Err(io::Error::other(format!(
            "log row index mismatch: key={:?} entry.log_id.index={} (corrupted row?)",
            key_bytes, entry.log_id.index
        )));
    }
    Ok(())
}

// ──── LogStore ────

/// 基于 Redb 持久化的 Raft LogStore
///
/// 线程安全（内部 `Arc<Database>`），支持 Clone。
/// 所有写入操作通过 Redb 写事务原子提交。
///
/// purge 前置条件：若设置了 `SnapshotTracker`，删除日志前必须
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

    /// 注入快照持久化守卫（创建 Raft 实例前调用）
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
                check_entry_key(key_bytes.as_slice(), &entry)?;
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
                    check_entry_key(key_bytes.as_slice(), &entry)?;
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

        // 直接从 B-Tree 取最大 index（O(1)，无扫描上限）。
        // 旧实现从 committed 线索向后最多扫描 1000 条，committed 落后时漏报尾部。
        let last = {
            let guard = table
                .last()
                .map_err(|e| io::Error::other(format!("get last log: {e}")))?;
            guard
                .map(|(k, v)| {
                    let entry: EntryOf<TypeConfig> = deserialize(v.value())?;
                    check_entry_key(k.value(), &entry)?;
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
        // client_write will hang forever .
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
        // 前置条件：存在覆盖 purge 点的已落盘快照才允许删除日志。
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

    // ──── 格式信封（P0：格式可辨识） ────

    fn test_entry(index: u64) -> EntryOf<TypeConfig> {
        EntryOf::<TypeConfig>::new_blank(LogIdOf::<TypeConfig>::new(
            openraft::impls::leader_id_adv::LeaderId {
                term: 1u64,
                node_id: 1u64,
            },
            index,
        ))
    }

    /// 新写入的行必须带统一信封前缀（白盒校验原始字节）。
    /// 负控制：写路径去掉 `envelope::encode` ⇒ 本用例必红。
    #[test]
    fn test_raft_rows_use_format_envelope() {
        use crate::storage::envelope::{MAGIC, VERSION};

        let mut store = create_test_log_store();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            store
                .save_vote(&VoteOf::<TypeConfig>::new(5, 1))
                .await
                .unwrap();
            store
                .append(vec![test_entry(1)], IOFlushed::noop())
                .await
                .unwrap();
        });

        let read_tx = store.db.begin_read().unwrap();
        let vote_table = read_tx.open_table(TABLE_VOTE).unwrap();
        let vote_row = vote_table.get(KEY_VOTE).unwrap().unwrap();
        assert!(
            vote_row.value().starts_with(&MAGIC),
            "vote row must carry envelope magic"
        );
        assert_eq!(vote_row.value()[MAGIC.len()], VERSION);

        let log_table = read_tx.open_table(TABLE_LOG).unwrap();
        let entry_row = log_table.get(index_key(1).as_slice()).unwrap().unwrap();
        assert!(
            entry_row.value().starts_with(&MAGIC),
            "log entry row must carry envelope magic"
        );
        assert_eq!(entry_row.value()[MAGIC.len()], VERSION);
    }

    /// 旧数据（无前缀 bincode）必须仍能解码（白盒注入旧格式行）。
    /// 负控制：读路径删掉旧格式回退 ⇒ 本用例必红。
    #[test]
    fn test_raft_legacy_rows_without_prefix_still_decode() {
        let mut store = create_test_log_store();
        let vote = VoteOf::<TypeConfig>::new(9, 3);
        let entry = test_entry(1);

        {
            let write_tx = store.db.begin_write().unwrap();
            {
                let mut vote_table = write_tx.open_table(TABLE_VOTE).unwrap();
                vote_table
                    .insert(KEY_VOTE, bincode::serialize(&vote).unwrap().as_slice())
                    .unwrap();
                let mut log_table = write_tx.open_table(TABLE_LOG).unwrap();
                log_table
                    .insert(
                        index_key(1).as_slice(),
                        bincode::serialize(&entry).unwrap().as_slice(),
                    )
                    .unwrap();
            }
            write_tx.commit().unwrap();
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            assert_eq!(store.read_vote().await.unwrap(), Some(vote));
        });
        assert_eq!(store.get_entry_at(1).unwrap().unwrap().log_id.index, 1);
    }

    /// 篡改信封 ⇒ 读行必须显式报错，不得静默解成垃圾。
    /// 负控制：放宽版本校验 / 把损坏行当旧格式强行解码 ⇒ 本用例必红。
    #[test]
    fn test_raft_tampered_envelope_fails_loudly() {
        use crate::storage::envelope::{MAGIC, VERSION, VERSION_V2};

        let mut store = create_test_log_store();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            store
                .save_vote(&VoteOf::<TypeConfig>::new(2, 1))
                .await
                .unwrap();
            store
                .append(vec![test_entry(1)], IOFlushed::noop())
                .await
                .unwrap();
        });

        let raw_row =
            |store: &LogStore, table: TableDefinition<&[u8], &[u8]>, key: &[u8]| -> Vec<u8> {
                let read_tx = store.db.begin_read().unwrap();
                let t = read_tx.open_table(table).unwrap();
                t.get(key).unwrap().unwrap().value().to_vec()
            };
        let overwrite_row =
            |store: &LogStore, table: TableDefinition<&[u8], &[u8]>, key: &[u8], bytes: &[u8]| {
                let write_tx = store.db.begin_write().unwrap();
                {
                    let mut t = write_tx.open_table(table).unwrap();
                    t.insert(key, bytes).unwrap();
                }
                write_tx.commit().unwrap();
            };

        // 1) 篡改 Vote 行的版本字节（认识魔数、不在受支持版本内 ⇒ 显式错误）
        let mut vote_row = raw_row(&store, TABLE_VOTE, KEY_VOTE);
        assert_eq!(vote_row[MAGIC.len()], VERSION);
        vote_row[MAGIC.len()] = VERSION_V2 + 1;
        overwrite_row(&store, TABLE_VOTE, KEY_VOTE, &vote_row);
        let err = rt.block_on(async { store.read_vote().await }).unwrap_err();
        assert!(
            err.to_string().contains("unsupported envelope version"),
            "tampered version must fail explicitly, got: {err}"
        );

        // 2) 篡改 Entry 行的魔数首字节（魔数损坏 ⇒ 只能按旧格式整行解码；
        //    bincode 结构不成立或尾随剩余字节 ⇒ 显式失败，不会静默解出垃圾）
        let mut entry_row = raw_row(&store, TABLE_LOG, index_key(1).as_slice());
        assert_eq!(entry_row[0], MAGIC[0]);
        entry_row[0] = 0x03;
        overwrite_row(&store, TABLE_LOG, index_key(1).as_slice(), &entry_row);
        let err = store.get_entry_at(1).unwrap_err();
        assert!(
            err.to_string().contains("decode raft row"),
            "corrupted magic must fail explicitly, got: {err}"
        );

        // 3) 结构合法但内容被改（index 与行 key 不一致）⇒ 由 key/内容
        //    不变量兜底拦截（魔数完好时结构校验拦不住数值篡改）
        let mut entry = test_entry(1);
        entry.log_id.index = 2;
        overwrite_row(
            &store,
            TABLE_LOG,
            index_key(1).as_slice(),
            &serialize(&entry).unwrap(),
        );
        let err = store.get_entry_at(1).unwrap_err();
        assert!(
            err.to_string().contains("index mismatch"),
            "row key invariant must catch index tamper, got: {err}"
        );
    }

    /// V2（postcard）行：四个表全部可读（新行读）。
    /// 负控制：删除 V2 分支（视为不支持版本）⇒ 本用例必红。
    #[test]
    fn test_raft_v2_rows_decode_all_tables() {
        use crate::storage::envelope::encode_v2;

        let mut store = create_test_log_store();
        let vote = VoteOf::<TypeConfig>::new(7, 2);
        let entry = test_entry(1);
        let committed = LogIdOf::<TypeConfig>::new(
            openraft::impls::leader_id_adv::LeaderId {
                term: 3u64,
                node_id: 1u64,
            },
            5,
        );
        let last_purged = LogIdOf::<TypeConfig>::new(
            openraft::impls::leader_id_adv::LeaderId {
                term: 2u64,
                node_id: 1u64,
            },
            3,
        );

        {
            let write_tx = store.db.begin_write().unwrap();
            {
                let mut vote_table = write_tx.open_table(TABLE_VOTE).unwrap();
                vote_table
                    .insert(KEY_VOTE, encode_v2(&vote).as_slice())
                    .unwrap();
                let mut committed_table = write_tx.open_table(TABLE_COMMITTED).unwrap();
                committed_table
                    .insert(KEY_COMMITTED, encode_v2(&committed).as_slice())
                    .unwrap();
                let mut purged_table = write_tx.open_table(TABLE_LAST_PURGED).unwrap();
                purged_table
                    .insert(KEY_LAST_PURGED, encode_v2(&last_purged).as_slice())
                    .unwrap();
                let mut log_table = write_tx.open_table(TABLE_LOG).unwrap();
                log_table
                    .insert(index_key(1).as_slice(), encode_v2(&entry).as_slice())
                    .unwrap();
            }
            write_tx.commit().unwrap();
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            assert_eq!(store.read_vote().await.unwrap(), Some(vote));
            assert_eq!(store.read_committed().await.unwrap(), Some(committed));
            let entries = store.try_get_log_entries(1u64..=1).await.unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].log_id, entry.log_id);
        });
        assert_eq!(store.last_purged().unwrap(), Some(last_purged));
    }

    /// 混读：同一存储内旧行（无前缀）/ V1 / V2 三路共存，全部可读。
    /// 负控制：任一读路径移除 ⇒ 对应用例必红。
    #[test]
    fn test_raft_mixed_format_rows_decode() {
        use crate::storage::envelope::encode_v2;

        let mut store = create_test_log_store();
        let vote_v2 = VoteOf::<TypeConfig>::new(7, 2);
        let committed_legacy = LogIdOf::<TypeConfig>::new(
            openraft::impls::leader_id_adv::LeaderId {
                term: 3u64,
                node_id: 1u64,
            },
            5,
        );
        let purged_v1 = LogIdOf::<TypeConfig>::new(
            openraft::impls::leader_id_adv::LeaderId {
                term: 2u64,
                node_id: 1u64,
            },
            3,
        );
        let entry_legacy = test_entry(1);
        let entry_v1 = test_entry(2);
        let entry_v2 = test_entry(3);

        {
            let write_tx = store.db.begin_write().unwrap();
            {
                let mut vote_table = write_tx.open_table(TABLE_VOTE).unwrap();
                vote_table
                    .insert(KEY_VOTE, encode_v2(&vote_v2).as_slice())
                    .unwrap();
                let mut committed_table = write_tx.open_table(TABLE_COMMITTED).unwrap();
                committed_table
                    .insert(
                        KEY_COMMITTED,
                        bincode::serialize(&committed_legacy).unwrap().as_slice(),
                    )
                    .unwrap();
                let mut purged_table = write_tx.open_table(TABLE_LAST_PURGED).unwrap();
                purged_table
                    .insert(KEY_LAST_PURGED, serialize(&purged_v1).unwrap().as_slice())
                    .unwrap();
                let mut log_table = write_tx.open_table(TABLE_LOG).unwrap();
                log_table
                    .insert(
                        index_key(1).as_slice(),
                        bincode::serialize(&entry_legacy).unwrap().as_slice(),
                    )
                    .unwrap();
                log_table
                    .insert(
                        index_key(2).as_slice(),
                        serialize(&entry_v1).unwrap().as_slice(),
                    )
                    .unwrap();
                log_table
                    .insert(index_key(3).as_slice(), encode_v2(&entry_v2).as_slice())
                    .unwrap();
            }
            write_tx.commit().unwrap();
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            assert_eq!(store.read_vote().await.unwrap(), Some(vote_v2));
            assert_eq!(
                store.read_committed().await.unwrap(),
                Some(committed_legacy)
            );
            let entries = store.try_get_log_entries(1u64..=3).await.unwrap();
            let indices: Vec<u64> = entries.iter().map(|e| e.log_id.index).collect();
            assert_eq!(indices, vec![1, 2, 3]);
        });
        assert_eq!(store.last_purged().unwrap(), Some(purged_v1));
    }

    /// 尾随字节篡改（V1 / 无前缀 / V2）⇒ 读路径显式失败（精确消费）。
    /// 负控制：bincode 侧改回 allow_trailing / 去掉 V2 remainder 断言 ⇒ 对应用例必红。
    #[test]
    fn test_raft_trailing_bytes_rejected() {
        use crate::storage::envelope::encode_v2;

        let mut store = create_test_log_store();
        let mut vote_v1 = serialize(&VoteOf::<TypeConfig>::new(5, 1)).unwrap();
        vote_v1.extend_from_slice(&[0xDE, 0xAD]);
        let mut committed_legacy = bincode::serialize(&LogIdOf::<TypeConfig>::new(
            openraft::impls::leader_id_adv::LeaderId {
                term: 1u64,
                node_id: 0u64,
            },
            10,
        ))
        .unwrap();
        committed_legacy.extend_from_slice(&[0xDE, 0xAD]);
        let mut purged_v2 = encode_v2(&LogIdOf::<TypeConfig>::new(
            openraft::impls::leader_id_adv::LeaderId {
                term: 1u64,
                node_id: 0u64,
            },
            9,
        ));
        purged_v2.extend_from_slice(&[0xDE, 0xAD]);

        {
            let write_tx = store.db.begin_write().unwrap();
            {
                let mut vote_table = write_tx.open_table(TABLE_VOTE).unwrap();
                vote_table.insert(KEY_VOTE, vote_v1.as_slice()).unwrap();
                let mut committed_table = write_tx.open_table(TABLE_COMMITTED).unwrap();
                committed_table
                    .insert(KEY_COMMITTED, committed_legacy.as_slice())
                    .unwrap();
                let mut purged_table = write_tx.open_table(TABLE_LAST_PURGED).unwrap();
                purged_table
                    .insert(KEY_LAST_PURGED, purged_v2.as_slice())
                    .unwrap();
            }
            write_tx.commit().unwrap();
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        let err = rt.block_on(async { store.read_vote().await }).unwrap_err();
        assert!(
            err.to_string().contains("decode raft row"),
            "V1 trailing bytes must fail explicitly, got: {err}"
        );
        let err = rt
            .block_on(async { store.read_committed().await })
            .unwrap_err();
        assert!(
            err.to_string().contains("decode raft row"),
            "legacy trailing bytes must fail explicitly, got: {err}"
        );
        let err = store.last_purged().unwrap_err();
        assert!(
            err.to_string().contains("trailing bytes"),
            "V2 trailing bytes must fail explicitly, got: {err}"
        );
    }

    /// V2 行魔数逐字节破坏 ⇒ 四个表类型全部显式失败（ADR-0005 锚点：
    /// postcard 载荷不可能按 bincode 结构成立）。
    /// 负控制：去掉魔数比较 / 给旧格式回退加试错解码 ⇒ 本用例必红。
    #[test]
    fn test_v2_magic_corruption_fails_explicitly_all_tables() {
        use crate::storage::envelope::{encode_v2, MAGIC};

        let vote = VoteOf::<TypeConfig>::new(5, 1);
        let entry = test_entry(1);
        let log_id = LogIdOf::<TypeConfig>::new(
            openraft::impls::leader_id_adv::LeaderId {
                term: 1u64,
                node_id: 1u64,
            },
            1,
        );

        for i in 0..MAGIC.len() {
            let mut vote_row = encode_v2(&vote);
            vote_row[i] = vote_row[i].wrapping_add(1);
            assert!(
                deserialize::<VoteOf<TypeConfig>>(&vote_row).is_err(),
                "V2 vote row magic byte {i} corruption must fail explicitly"
            );

            let mut entry_row = encode_v2(&entry);
            entry_row[i] = entry_row[i].wrapping_add(1);
            assert!(
                deserialize::<EntryOf<TypeConfig>>(&entry_row).is_err(),
                "V2 entry row magic byte {i} corruption must fail explicitly"
            );

            let mut log_id_row = encode_v2(&log_id);
            log_id_row[i] = log_id_row[i].wrapping_add(1);
            assert!(
                deserialize::<LogIdOf<TypeConfig>>(&log_id_row).is_err(),
                "V2 committed/last_purged row magic byte {i} corruption must fail explicitly"
            );
        }
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

    /// 回归：`get_log_state` 不得依赖 committed 线索的有限扫描。
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
