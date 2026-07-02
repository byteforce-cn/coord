// Raft 共识层模块
//
// 包含：
// - type_config:     Coord 的 Openraft RaftTypeConfig 定义
// - log_store:       RaftLogStorage + RaftLogReader 实现（Redb 持久化）
// - state_machine:   RaftStateMachine + RaftSnapshotBuilder 实现
// - network:         RaftNetworkFactory + RaftNetwork 实现（Tonic gRPC）
// - region:          Multi-Raft Region 管理器（RegionHandle + RegionManager）

pub mod log_store;
pub mod network;
pub mod region;
pub mod state_machine;
pub mod type_config;

/// Coord 的完整 Raft 类型别名
pub type CoordRaft = openraft::Raft<type_config::TypeConfig, state_machine::StateMachineStore>;
