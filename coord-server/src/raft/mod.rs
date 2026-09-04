// Raft 共识层模块
//
// 包含：
// - type_config:     Coord 的 Openraft RaftTypeConfig 定义
// - log_store:       RaftLogStorage + RaftLogReader 实现（Redb 持久化）
// - state_machine:   RaftStateMachine + RaftSnapshotBuilder 实现
// - network:         RaftNetworkFactory + RaftNetwork 实现（Tonic gRPC）
// - region:          Multi-Raft Region 管理器（RegionHandle + RegionManager）
//
// P1-06 openraft 类型隔离边界：openraft 仍为 alpha（0.10.0-alpha.34，版本
// 精确锁定见 `docs/production/16-openraft-governance.md`）。`openraft::` 与
// `openraft_multi::` 路径只允许出现在本 crate 的 `raft/` 模块内部；其余模块与
// `coord` CLI、测试一律经本文件提供的别名与构造函数使用。升级 openraft 版本时，
// 编译缺口应只出现在本目录（详见 ADR §升级演练）。

pub mod log_store;
pub mod network;
pub mod region;
pub mod region_runtime;
pub mod state_machine;
pub mod type_config;

pub use network::{RegionRaftNetworkFactory, RaftNetworkFactoryImpl, RaftRpcServer, RaftRpcService};
pub use region_runtime::{region_data_dir, RegionRuntime, RegionRuntimeSpec};

use std::collections::BTreeSet;
use std::sync::Arc;

/// Coord 的完整 Raft 类型别名
pub type CoordRaft = openraft::Raft<type_config::TypeConfig, state_machine::StateMachineStore>;

/// Raft 运行配置（默认值；调用方可用结构体更新语法覆盖心跳/选举超时）
pub type RaftConfig = openraft::Config;

/// R-RFT-19：Raft 运行时调优参数（来自 `[raft]` 配置段）。
///
/// 全部字段为 `Option`：`None` = 保持 openraft 默认值（0.10.0-alpha.25：
/// 心跳 50ms、选举 150–300ms、安装快照 200ms、快照策略 since_last:5000）。
#[derive(Debug, Clone, Copy, Default)]
pub struct RaftTuning {
    /// 心跳间隔（毫秒）
    pub heartbeat_interval_ms: Option<u64>,
    /// 选举超时下限（毫秒）
    pub election_timeout_min_ms: Option<u64>,
    /// 选举超时上限（毫秒）
    pub election_timeout_max_ms: Option<u64>,
    /// 安装快照超时（毫秒）
    pub install_snapshot_timeout_ms: Option<u64>,
    /// 快照策略：距上次快照累积的日志条数（0 = Never，禁用自动快照）
    pub snapshot_logs_since_last: Option<u64>,
}

/// R-RFT-19：将调优参数应用到 RaftConfig（None 字段保持 openraft 默认值）。
///
/// 放在本模块内以维持 P1-06 的 openraft 类型隔离边界（CLI 层不直接引用
/// openraft 路径）。
pub fn apply_tuning(config: &mut RaftConfig, tuning: &RaftTuning) {
    if let Some(v) = tuning.heartbeat_interval_ms {
        config.heartbeat_interval = v;
    }
    if let Some(v) = tuning.election_timeout_min_ms {
        config.election_timeout_min = v;
    }
    if let Some(v) = tuning.election_timeout_max_ms {
        config.election_timeout_max = v;
    }
    if let Some(v) = tuning.install_snapshot_timeout_ms {
        config.install_snapshot_timeout = v;
    }
    if let Some(v) = tuning.snapshot_logs_since_last {
        config.snapshot_policy = if v == 0 {
            openraft::SnapshotPolicy::Never
        } else {
            openraft::SnapshotPolicy::LogsSinceLast(v)
        };
    }
}

/// 集群节点描述（BasicNode：含 raft 通信地址）
pub type RaftNode = openraft::impls::BasicNode;

/// 成员变更指令（AddVoterIds / RemoveVoters）
pub type ChangeMembers = openraft::ChangeMembers<u64, RaftNode>;

/// 快照数据（字节缓冲，Cursor 可读可写）。
///
/// 0.10.0-alpha.34 起 `RaftStateMachine::SnapshotData` 与 `RaftNetworkV2::SnapshotData`
/// 必须为同一类型（`Raft::new` 要求 `NetSnapshot::SnapshotData ==
/// RaftStateMachine::SnapshotData`），此处统一收敛（P1-06 门面）。
pub type RaftSnapshotData = std::io::Cursor<Vec<u8>>;

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
    // alpha.34：`Raft::new` 要求网络与状态机的 SnapshotData 为同一类型
    N::Network: openraft::network::NetSnapshot<
        type_config::TypeConfig,
        SnapshotData = SM::SnapshotData,
    >,
{
    openraft::Raft::new(node_id, config, network, log_store, state_machine)
        .await
        .map_err(|e| format!("create raft instance: {e}").into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apply_tuning_partial_and_full() {
        let mut config = RaftConfig::default();
        let defaults = RaftConfig::default();

        // 全部 None：保持默认
        apply_tuning(&mut config, &RaftTuning::default());
        assert_eq!(config.heartbeat_interval, defaults.heartbeat_interval);
        assert_eq!(config.election_timeout_min, defaults.election_timeout_min);
        assert_eq!(config.election_timeout_max, defaults.election_timeout_max);

        // 部分覆盖：仅改心跳与选举
        apply_tuning(
            &mut config,
            &RaftTuning {
                heartbeat_interval_ms: Some(100),
                election_timeout_min_ms: Some(400),
                election_timeout_max_ms: Some(800),
                install_snapshot_timeout_ms: None,
                snapshot_logs_since_last: None,
            },
        );
        assert_eq!(config.heartbeat_interval, 100);
        assert_eq!(config.election_timeout_min, 400);
        assert_eq!(config.election_timeout_max, 800);
        assert_eq!(
            config.install_snapshot_timeout, defaults.install_snapshot_timeout,
            "未配置字段保持默认"
        );

        // 快照策略：0 = Never，其余 = LogsSinceLast
        apply_tuning(
            &mut config,
            &RaftTuning {
                snapshot_logs_since_last: Some(0),
                ..RaftTuning::default()
            },
        );
        assert!(matches!(
            config.snapshot_policy,
            openraft::SnapshotPolicy::Never
        ));
        apply_tuning(
            &mut config,
            &RaftTuning {
                snapshot_logs_since_last: Some(2500),
                ..RaftTuning::default()
            },
        );
        assert!(matches!(
            config.snapshot_policy,
            openraft::SnapshotPolicy::LogsSinceLast(2500)
        ));
    }
}
