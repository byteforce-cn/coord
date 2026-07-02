// 公共错误类型定义
//
// 所有 Crate 共享此错误类型。coord-server 在 gRPC 响应中将 Error 映射为
// 对应的 tonic::Status code（参见 ADP.md §23.2）。

/// coord-core 公共 Result 类型
pub type Result<T> = std::result::Result<T, Error>;

/// coord-core 公共错误类型
#[derive(Debug, thiserror::Error)]
pub enum Error {
    // ──── 通用错误 ────
    /// 内部错误（不可恢复）
    #[error("internal error: {0}")]
    Internal(String),

    /// 无效参数
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// 资源不存在
    #[error("{resource} not found: {key}")]
    NotFound {
        resource: &'static str,
        key: String,
    },

    /// 资源已存在
    #[error("{resource} already exists: {key}")]
    AlreadyExists {
        resource: &'static str,
        key: String,
    },

    /// 权限不足
    #[error("permission denied: {0}")]
    PermissionDenied(String),

    /// 未认证
    #[error("unauthenticated: {0}")]
    Unauthenticated(String),

    // ──── 共识层错误 ────
    /// 当前节点不是 Leader
    #[error("not leader; leader is {leader_addr:?}")]
    NotLeader {
        leader_addr: Option<String>,
    },

    /// 不是 Leader 且不知道 Leader 地址
    #[error("not leader, leader hint unavailable")]
    NotLeaderNoHint,

    /// Raft 集群不可用（无 Leader 或未完成选举）
    #[error("cluster unavailable: {0}")]
    ClusterUnavailable(String),

    /// Raft 日志复制超时
    #[error("request timeout")]
    RequestTimeout,

    // ──── 存储层错误 ────
    /// 存储 I/O 错误
    #[error("storage error: {0}")]
    Storage(String),

    /// 指定的 Revision 不可用（已被 Compaction 清理）
    #[error("revision {revision} compacted; oldest available: {oldest}")]
    RevisionCompacted {
        revision: u64,
        oldest: u64,
    },

    /// 数据损坏
    #[error("data corruption: {0}")]
    DataCorruption(String),

    // ──── Lease 错误 ────
    /// Lease 不存在或已过期
    #[error("lease {lease_id} not found or expired")]
    LeaseNotFound {
        lease_id: i64,
    },

    /// TTL 超出允许范围
    #[error("lease TTL {ttl}s out of range [{min}, {max}]")]
    LeaseTTLOutOfRange {
        ttl: i64,
        min: i64,
        max: i64,
    },

    // ──── Txn 错误 ────
    /// 事务操作过多
    #[error("txn too large: {ops} operations, max {max}")]
    TxnTooLarge {
        ops: usize,
        max: usize,
    },

    /// CAS 条件不满足（非错误，业务判断用）
    #[error("txn compare failed")]
    TxnCompareFailed,

    // ──── Watch 错误 ────
    /// Watch 连接数达到上限
    #[error("too many watch connections: {current}/{max}")]
    WatchTooManyConnections {
        current: usize,
        max: usize,
    },

    // ──── 安全层错误 ────
    /// 集群处于 Sealed 状态
    #[error("cluster is sealed")]
    ClusterSealed,

    /// 集群处于 Unsealing 状态
    #[error("cluster is unsealing")]
    ClusterUnsealing,

    /// 加密/解密失败
    #[error("crypto error: {0}")]
    Crypto(String),

    /// Shamir 分片不足
    #[error("insufficient shares: have {have}, need {need}")]
    InsufficientShares {
        have: usize,
        need: usize,
    },

    // ──── Auth 错误 ────
    /// Auth 未启用
    #[error("auth not enabled")]
    AuthNotEnabled,

    /// Token 过期
    #[error("token expired")]
    TokenExpired,

    /// 无效 Token
    #[error("invalid token: {0}")]
    InvalidToken(String),

    /// 用户已存在
    #[error("user {name} already exists")]
    UserAlreadyExists {
        name: String,
    },

    /// 角色已存在
    #[error("role {name} already exists")]
    RoleAlreadyExists {
        name: String,
    },

    // ──── Multi-Raft / Region 错误 ────

    /// Region 不存在
    #[error("region {region_id} not found")]
    RegionNotFound {
        region_id: u64,
    },

    /// 当前节点不是目标 Region 的 Leader
    #[error("not leader for region {region_id}; leader is {leader_addr:?}")]
    RegionNotLeader {
        region_id: u64,
        leader_addr: Option<String>,
    },

    /// 客户端 Epoch 过期（Region 已分裂或成员变更）
    #[error("stale epoch for region {region_id}: client={client_conf_ver}/{client_version}, server={server_conf_ver}/{server_version}")]
    EpochStale {
        region_id: u64,
        client_conf_ver: u64,
        client_version: u64,
        server_conf_ver: u64,
        server_version: u64,
    },

    /// Key 不属于当前 Region 的 Key Range
    #[error("key not in region {region_id} range")]
    KeyNotInRegion {
        region_id: u64,
    },

    /// Region 分裂进行中
    #[error("region {region_id} split in progress")]
    RegionSplitInProgress {
        region_id: u64,
    },

    /// Placement Driver 不可用
    #[error("PD unavailable: {0}")]
    PdUnavailable(String),

    /// 路由表未就绪（初始化中）
    #[error("route table not ready")]
    RouteNotReady,
}
