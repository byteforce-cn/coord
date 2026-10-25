// coord-client: 客户端 SDK
//
// 封装 gRPC 通信、Leader 发现、连接管理、重试逻辑。
// 提供高级 API：Lock。
//
// 模块结构：
// - config:      客户端配置（端点、超时、重试参数、TLS 配置）
// - leader:      Leader 发现与缓存
// - retry:       重试策略（指数退避、错误分类）
// - client:      主客户端 + KV/Lease/Watch/Txn/Maintenance/Storage 子客户端
//                + 高级 Lock API
// - txn_v2:      coord.txn.v2 事务会话（Begin/Read/Commit/Abort，绑定协调节点）
// - watch_v2:    coord.watch.v2 订阅（cursor 续订 + 断线重连，至少一次投递）
// - lease_v2:    coord.lease.v2 租约（全局 lease_id、跨 Region revoke）
// - storage_v2:  coord.storage.v2 对象面（manifest 用全局 commit_ts/read_ts）
// - tls:         TLS/mTLS Channel 构建（Config.tls，供连接池与 Leader 发现共用）
// （R-AGT-20：route_cache 死代码已移除——单连接直连模式无需 Leader 路由缓存）

pub mod auth;
pub mod client;
pub mod config;
pub mod credential;
pub mod leader;
pub mod lease_v2;
pub mod pool;
pub mod refresh;
pub mod retry;
pub mod storage_v2;
mod tls;
pub mod txn_v2;
mod v2_compat;
pub mod watch_v2;

// 重新导出主要类型
pub use auth::AuthClient;
pub use client::{
    ApprovalClient, Client, KvClient, KvV2Client, LeaseClient, LeaseKeeper, Lock,
    MaintenanceClient, ObjectData, ObjectReader, ObjectWriter, PutObjectResult, StorageClient,
    TxnClient, WatchClient, DEFAULT_OBJECT_CHUNK_SIZE,
};
pub use config::{Config, DataPlane, TlsConfig};
pub use credential::{
    current_request_token, in_proxy_context, scoped_proxied_request, scoped_request_token,
    AuthedChannel, CachedTokenProvider, CredentialInterceptor, FallbackTokenProvider,
    NoopTokenProvider, RequestScopedTokenProvider, TokenProvider,
};
pub use leader::LeaderDiscovery;
pub use lease_v2::{LeaseV2Client, LeaseV2Grant, LeaseV2State};
pub use refresh::{spawn_session_refresher, SessionGateway, SessionTokens, REFRESH_LEAD_SECS};
pub use retry::{classify_error, RetryDecision, RetryState};
pub use storage_v2::{ObjectDataV2, PutObjectV2Result, StorageV2Client};
pub use txn_v2::{delete_mutation, put_mutation, TxnCommit, TxnV2Client, TxnV2Session};
pub use watch_v2::{
    WatchV2Batch, WatchV2Change, WatchV2Client, WatchV2Cursor, WatchV2Event, WatchV2Filter,
    WatchV2Options, WatchV2Subscription,
};
