// coord-server: 服务端实现
//
// 本 Crate 包含：
// - storage/  : Redb 存储后端 + MVCC 存储层
// - raft/     : Openraft Raft 共识适配（P1）
// - pd/       : Placement Driver 全局调度器（v6.0 Multi-Raft）
// - txn/      : Txn 原子事务执行器（P1）
// - watch/    : Watch 变更监听（P1）
// - lease/    : Lease 租约管理（P2）
// - security/ : Barrier 加密层 + Key Management + Seal/Unseal（P2-P3）
// - timer/    : Timer Wheel 时间轮（P1）
// - auth/     : Auth 认证鉴权 + RBAC（P3）
// - metrics/  : Prometheus 指标收集
// - health/   : HTTP Health Check 端点
// - tls/      : TLS/mTLS 传输安全

pub mod auth;
pub mod bff;
pub mod health;
pub mod lease;
pub mod metrics;
pub mod pd;
pub mod raft;
pub mod security;
pub mod server;
pub mod storage;
pub mod timer;
pub mod tls;
pub mod txn;
pub mod watch;
