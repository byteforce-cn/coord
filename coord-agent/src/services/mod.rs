// coord-agent: 可插拔服务 — 模块声明
//
// 每个高级基础服务为一个独立模块，实现 BaseService trait。
// 通过 ServiceManager 按需加载和生命周期管理。
//
// 参见 docs/client-agent-architecture-v3.md §5。

pub mod cache;
pub mod circuit_breaker;
pub mod config_center;
pub mod event_notification;
pub mod idgen;
pub mod leader_election;
pub mod lock;
pub mod mq;
pub mod opa;
pub mod policy;
pub mod rate_limiter;
pub mod registry;
pub mod replication;
pub mod scheduler;
pub mod transit;
pub mod workflow;
