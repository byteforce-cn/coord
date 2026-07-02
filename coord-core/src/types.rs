// 核心类型定义
//
// 包含 Revision、LeaseID 等公共类型，所有 Crate 共享。

use serde::{Deserialize, Serialize};

/// 全局单调递增的 Revision 编号（64-bit）
pub type Revision = u64;

/// Lease 唯一标识符
pub type LeaseID = i64;

/// 节点唯一标识符
pub type NodeID = u64;

/// Raft Term 编号
pub type Term = u64;

/// Raft Log Index
pub type LogIndex = u64;

/// 存储表名（用于 StorageBackend 的表级操作）
pub type TableName = &'static str;

// ─── Multi-Raft Region 类型 ───

/// Region 全局唯一标识符（单调递增分配）
pub type RegionId = u64;

/// Region 版本号（monotonic，每次 Split 递增）
pub type RegionVersion = u64;

/// Region 配置版本号（monotonic，每次成员变更递增）
pub type ConfVersion = u64;

/// Region Epoch：防止过期请求
///
/// 客户端每次请求携带已知的 Epoch。服务端校验：
/// - conf_ver 不匹配 → 返回 RegionNotLeader
/// - version 不匹配 → 返回 RegionSplit（Region 已分裂）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegionEpoch {
    /// 成员变更版本：每次 add_peer / remove_peer 递增
    pub conf_ver: ConfVersion,
    /// Region 分裂版本：每次 Split 递增
    pub version: RegionVersion,
}

impl RegionEpoch {
    /// 创建初始 Epoch（conf_ver=1, version=1）
    pub fn initial() -> Self {
        Self {
            conf_ver: 1,
            version: 1,
        }
    }

    /// 判断客户端 Epoch 是否过期
    pub fn is_client_stale(&self, client: &RegionEpoch) -> bool {
        client.conf_ver < self.conf_ver || client.version < self.version
    }
}

/// Region 副本角色
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeerRole {
    /// 投票成员（参与 Raft 共识）
    Voter,
    /// 学习者（不参与投票，异步追赶日志）
    Learner,
}

/// Region 副本信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Peer {
    /// 节点唯一标识
    pub node_id: NodeID,
    /// Raft 通信地址
    pub raft_addr: String,
    /// 副本角色
    pub role: PeerRole,
}

/// Region 元数据
///
/// 描述一个 Region 的 Key Range、Epoch、副本分布和近似统计信息。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionMeta {
    /// Region 全局唯一标识
    pub region_id: RegionId,
    /// Key Range 起始（包含）
    pub start_key: Vec<u8>,
    /// Key Range 结束（不包含）
    pub end_key: Vec<u8>,
    /// 当前 Epoch
    pub epoch: RegionEpoch,
    /// 副本所在节点列表
    pub peers: Vec<Peer>,
    /// 近似数据量（字节），用于 Split 决策
    pub approximate_size: u64,
    /// 近似 Key 数量，用于 Split 决策
    pub approximate_keys: u64,
}

impl RegionMeta {
    /// 判断 key 是否属于此 Region 的 Key Range
    ///
    /// Key Range 语义：左闭右开 [start_key, end_key)
    pub fn contains_key(&self, key: &[u8]) -> bool {
        key >= self.start_key.as_slice()
            && (self.end_key.is_empty() || key < self.end_key.as_slice())
    }

    /// 获取当前 Leader 的地址（若有）
    ///
    /// 注意：RegionMeta 不直接存储 Leader 信息，
    /// 此方法仅用于从 peers 推测（实际 Leader 由 PD/Raft 状态维护）。
    pub fn voter_peers(&self) -> impl Iterator<Item = &Peer> {
        self.peers.iter().filter(|p| p.role == PeerRole::Voter)
    }
}

/// 已知的内部表名常量
pub mod tables {
    use super::TableName;

    /// 用户 KV 数据表
    pub const KV: TableName = "kv";

    /// 内部元数据表
    pub const META: TableName = "meta";

    /// Lease 绑定表
    pub const LEASE: TableName = "lease";

    /// 认证数据表
    pub const AUTH: TableName = "auth";

    /// 变更日志表（Changelog）
    pub const CHANGELOG: TableName = "changelog";
}

/// 存储配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// 数据库文件路径
    pub data_dir: String,
    /// 最大活跃读事务数（Redb 限制）
    pub max_readers: u32,
    /// 每次 Compaction 的间隔（秒）
    pub compaction_interval_secs: u64,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: "/var/lib/coord".to_string(),
            max_readers: 256,
            compaction_interval_secs: 3600,
        }
    }
}
