// coord-client: 客户端 SDK
//
// 封装 gRPC 通信、Leader 发现、连接管理、重试逻辑。
// 提供高级 API：Lock。
//
// 模块结构（ADP §10.2-10.3）：
// - config:      客户端配置（端点、超时、重试参数、TLS 配置）
// - leader:      Leader 发现与缓存
// - retry:       重试策略（指数退避、错误分类）
// - client:      主客户端 + KV/Lease/Watch/Txn/Maintenance 子客户端 + 高级 Lock API
// - tls:         TLS/mTLS Channel 构建（Config.tls，供连接池与 Leader 发现共用）
// （R-AGT-20：route_cache 死代码已移除——单连接直连模式无需 Leader 路由缓存）

pub mod client;
pub mod config;
pub mod leader;
pub mod pool;
pub mod retry;
mod tls;

// 重新导出主要类型
pub use client::{
    Client, KvClient, LeaseClient, LeaseKeeper, Lock, MaintenanceClient, TxnClient, WatchClient,
};
pub use config::{Config, TlsConfig};
pub use leader::LeaderDiscovery;
pub use retry::{classify_error, RetryDecision, RetryState};
