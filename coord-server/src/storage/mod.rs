// coord-server 存储模块
//
// 包含：
// - redb_backend:          Redb 对 StorageBackend trait 的实现（P0）
// - mvcc:                  MVCC 版本化存储层（P0）
// - snapshot:              快照导出/导入（P5 — 生产特性）
// - snapshot_scheduler:    自动定时快照调度（P5 — 生产特性，ADP §19.2）
// - compaction:            Compaction 调度与管理（P5 — 生产特性）
// - write_batcher:         Multi-Raft 共享写入批处理器（v6.0）

pub mod compaction;
pub mod mvcc;
pub mod redb_backend;
pub mod snapshot;
pub mod snapshot_limiter;
pub mod snapshot_scheduler;
pub mod write_batcher;
