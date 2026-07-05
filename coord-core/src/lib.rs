// coord-core: 公共 Trait 与类型
//
// 本 Crate 定义所有其他 Crate 共享的公共抽象：
// - StorageBackend trait（存储引擎抽象）
// - Error 类型（公共错误枚举）
// - 核心类型（Revision, LeaseID, Key 等）
// - Region 类型与 Key 编码（Multi-Raft）
// - Result 类型别名

pub mod auth;
pub mod discovery;
pub mod error;
pub mod region;
pub mod storage;
pub mod types;
