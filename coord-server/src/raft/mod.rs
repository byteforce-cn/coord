// Raft 共识层模块
//
// 包含：
// - type_config:     Coord 的 Openraft RaftTypeConfig 定义
// - log_store:       RaftLogStorage + RaftLogReader 实现（Redb 持久化）
// - state_machine:   RaftStateMachine + RaftSnapshotBuilder 实现
// - network:         RaftNetworkFactory + RaftNetwork 实现（Tonic gRPC）
// - region:          Multi-Raft Region 管理器（RegionHandle + RegionManager）
//
// P1-06 openraft 类型隔离边界：openraft 仍为 alpha（0.10.0-alpha.25，版本
// 精确锁定见 `docs/production/16-openraft-governance.md`）。`openraft::` 路径
// 只允许出现在本 crate 的 `raft/` 模块内部；其余模块与 `coord` CLI、测试
// 一律经本文件提供的别名与构造函数使用。升级 openraft 版本时，编译缺口
// 应只出现在本目录（详见 ADR §升级演练）。

pub mod log_store;
pub mod network;
pub mod region;
pub mod state_machine;
pub mod type_config;

use std::collections::BTreeSet;
use std::sync::Arc;

/// Coord 的完整 Raft 类型别名
pub type CoordRaft = openraft::Raft<type_config::TypeConfig, state_machine::StateMachineStore>;

/// Raft 运行配置（默认值；调用方可用结构体更新语法覆盖心跳/选举超时）
pub type RaftConfig = openraft::Config;

/// 集群节点描述（BasicNode：含 raft 通信地址）
pub type RaftNode = openraft::impls::BasicNode;

/// 成员变更指令（AddVoterIds / RemoveVoters）
pub type ChangeMembers = openraft::ChangeMembers<u64, RaftNode>;

// P1-06：openraft 类型面（仅 re-export 本目录/测试实际需要的少数名字，
// 名单有意识维护，随升级演练更新）
pub use openraft::impls::leader_id_adv::LeaderId;
pub use openraft::rt::WatchReceiver;
pub use openraft::storage::{RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine};
pub use openraft::type_config::alias::{LogIdOf, StoredMembershipOf};
pub use openraft::Membership;
pub use openraft::ReadPolicy;

/// 构造集群节点描述（P1-06 门面）
pub fn new_basic_node(addr: &str) -> RaftNode {
    RaftNode::new(addr)
}

/// 构造"添加 Voter"成员变更（P1-06 门面）
pub fn add_voter_ids(ids: BTreeSet<u64>) -> ChangeMembers {
    openraft::ChangeMembers::AddVoterIds(ids)
}

/// 构造"移除 Voter"成员变更（P1-06 门面）
pub fn remove_voter_ids(ids: BTreeSet<u64>) -> ChangeMembers {
    openraft::ChangeMembers::RemoveVoters(ids)
}

/// 构造 Raft 实例（P1-06 门面：`openraft::Raft::new` 的泛型参数收敛于此）
pub async fn new_raft<N, LS, SM>(
    node_id: u64,
    config: Arc<RaftConfig>,
    network: N,
    log_store: LS,
    state_machine: SM,
) -> Result<openraft::Raft<type_config::TypeConfig, SM>, Box<dyn std::error::Error + Send + Sync>>
where
    N: openraft::RaftNetworkFactory<type_config::TypeConfig> + 'static,
    LS: openraft::storage::RaftLogStorage<type_config::TypeConfig> + 'static,
    SM: openraft::storage::RaftStateMachine<type_config::TypeConfig> + 'static,
{
    openraft::Raft::new(node_id, config, network, log_store, state_machine)
        .await
        .map_err(|e| format!("create raft instance: {e}").into())
}
