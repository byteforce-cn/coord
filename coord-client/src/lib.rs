// coord-client: 客户端 SDK
//
// 封装 gRPC 通信、Leader 发现、连接管理、重试逻辑。
// 提供高级 API：Lock。
//
// 模块结构（ADP §10.2-10.3）：
// - config:      客户端配置（端点、超时、重试参数）
// - leader:      Leader 发现与缓存
// - retry:       重试策略（指数退避、错误分类）
// - client:      主客户端 + KV/Lease/Watch/Txn/Maintenance 子客户端 + 高级 Lock API
// - route_cache: Multi-Raft 客户端路由缓存（v6.0）

pub mod client;
pub mod config;
pub mod leader;
pub mod pool;
pub mod retry;
pub mod route_cache;

// 重新导出主要类型
pub use client::{
    Client, KvClient, LeaseClient, LeaseKeeper, Lock, MaintenanceClient, TxnClient, WatchClient,
};
pub use config::Config;
pub use leader::LeaderDiscovery;
pub use retry::{RetryDecision, RetryState, classify_error};
