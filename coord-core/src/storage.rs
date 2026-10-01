// StorageBackend trait — 存储后端抽象
//
// 定义底层 KV 存储引擎的读写、Compaction 和状态查询的最小契约。
// Redb 作为首个实现。上层（StateMachine、Barrier、Changelog）仅依赖此 trait，
// 不直接引用 redb::Database。

use std::path::Path;

use crate::error::Result;
use crate::types::StorageConfig;

/// 存储后端抽象 trait
///
/// 封装底层 KV 存储引擎的读写、Compaction 和状态查询能力。
/// 实现方负责管理连接生命周期、事务隔离级别和存储文件。
pub trait StorageBackend: Send + Sync {
    /// 打开/创建数据库实例
    fn open(path: &Path, config: &StorageConfig) -> Result<Self>
    where
        Self: Sized;

    /// 执行只读事务。闭包内可进行多次读取，事务在闭包结束后自动释放。
    fn read<T>(&self, f: impl FnOnce(&dyn ReadTx) -> Result<T>) -> Result<T>;

    /// 执行读写事务。闭包返回 Ok 时原子提交，返回 Err 时自动回滚。
    fn write<T>(&self, f: impl FnOnce(&mut dyn WriteTx) -> Result<T>) -> Result<T>;

    /// 触发 Compaction，清理不再被活跃事务引用的旧版本数据。
    fn compact(&self) -> Result<()>;

    /// R-RFT-19：等待 `idle` 时长的无写入静默期后执行 compact。
    ///
    /// 默认实现直接 `compact()`；能跟踪写入活动的后端（RedbBackend）覆盖此
    /// 方法，将维护压缩与在线读写窗口错开（规避 redb 独占写锁的长事务窗口）。
    fn compact_with_idle_window(
        &self,
        _idle: std::time::Duration,
        _max_wait: std::time::Duration,
    ) -> Result<()> {
        self.compact()
    }

    /// 返回数据库文件在磁盘上的大小（字节）。
    fn disk_size_bytes(&self) -> Result<u64>;

    /// 返回数据库中存活的 Key 总数（不含 tombstone 和内部元数据 Key）。
    fn key_count(&self) -> Result<u64>;
}

/// 只读事务句柄
///
/// 提供表级读写能力。实现方负责管理事务的生命周期和 MVCC 快照隔离。
pub trait ReadTx {
    /// 从指定表读取单个 Key 的值
    fn get(&self, table: &str, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// 前缀扫描指定表的 Key 范围
    fn iter_prefix(&self, table: &str, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// 区间扫描指定表的 Key 范围 `[start, end)`（半开区间；end 为空表示扫描到表尾）
    ///
    /// R-SVC-07：支撑 `[key, range_end)` 半开区间语义。返回的 Key 满足
    /// `start <= key` 且（end 非空时）`key < end`，按 Key 字典序排列。
    fn iter_range(&self, table: &str, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
}

/// 读写事务句柄
///
/// 扩展 ReadTx，提供写入和删除能力。
pub trait WriteTx: ReadTx {
    /// 插入/更新 Key-Value
    fn insert(&mut self, table: &str, key: &[u8], value: &[u8]) -> Result<()>;

    /// 删除 Key
    fn remove(&mut self, table: &str, key: &[u8]) -> Result<()>;
}
