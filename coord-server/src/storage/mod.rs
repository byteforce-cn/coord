// coord-server 存储模块
//
// 包含：
// - redb_backend:          Redb 对 StorageBackend trait 的实现
// - mvcc:                  MVCC 版本化存储层
// - snapshot:              快照导出/导入（生产特性）
// - snapshot_scheduler:    自动定时快照调度（生产特性）
// - compaction:            Compaction 调度与管理（生产特性）
// - write_batcher:         Multi-Raft 共享写入批处理器（v6.0）

pub mod compaction;
pub mod disk_watermark;
pub mod mvcc;
pub mod object_store;
pub mod redb_backend;
pub mod snapshot;
pub mod snapshot_limiter;
pub mod snapshot_scheduler;
pub mod write_batcher;
