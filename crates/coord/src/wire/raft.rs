//! Wire-format adapters for Raft log entries and membership commands.
//!
//! This module is the canonical proto ↔ domain seam for the Raft
//! replication surface. All `coord_proto::coord::v1::Raft*` types enter
//! and leave the server through the pure functions in this file; the
//! rest of the code base deals exclusively with the domain enums
//! defined in [`coord_core::raft_runtime`].
//!
use anyhow::anyhow;
use coord_core::raft_runtime::{MembershipNode, PersistedLogEntry, StateMachineCommand};

pub use coord_proto::coord::v1::raft_command::Op as RaftOp;
pub use coord_proto::coord::v1::{
    RaftBeginJointConsensusCommand, RaftBusinessCommand, RaftCommand,
    RaftFinalizeMembershipCommand, RaftLogEntry, RaftMemberAddCommand, RaftMemberNode,
    RaftMemberRemoveCommand, RaftRestoreRuntimeSnapshotCommand,
};

/// Build a [`PersistedLogEntry`] from the on-wire proto representation.
///
/// Returns an error if `entry.command` is missing its inner `op`.
pub fn log_entry_from_proto(entry: RaftLogEntry) -> anyhow::Result<PersistedLogEntry> {
    let op = entry
        .command
        .and_then(|command| command.op)
        .ok_or_else(|| anyhow!("raft log entry command is required"))?;

    let command = match op {
        RaftOp::MemberAdd(add) => StateMachineCommand::MemberAdd {
            node_id: add.node_id,
            address: add.address,
        },
        RaftOp::MemberRemove(remove) => StateMachineCommand::MemberRemove {
            node_id: remove.node_id,
        },
        RaftOp::BeginJointConsensus(joint) => StateMachineCommand::BeginJointConsensus {
            old_members: joint
                .old_members
                .into_iter()
                .map(membership_from_proto)
                .collect(),
            new_members: joint
                .new_members
                .into_iter()
                .map(membership_from_proto)
                .collect(),
        },
        RaftOp::FinalizeMembership(finalize) => StateMachineCommand::FinalizeMembership {
            members: finalize
                .members
                .into_iter()
                .map(membership_from_proto)
                .collect(),
        },
        RaftOp::RestoreRuntimeSnapshot(restore) => StateMachineCommand::RestoreRuntimeSnapshot {
            payload_json: restore.payload_json,
            payload_version: restore.payload_version,
        },
        RaftOp::BusinessCommand(business) => StateMachineCommand::BusinessCommand {
            namespace: business.namespace,
            payload: business.payload,
        },
    };

    Ok(PersistedLogEntry {
        index: entry.index,
        term: entry.term,
        command,
    })
}

/// Serialise a [`PersistedLogEntry`] to its proto representation.
pub fn log_entry_to_proto(entry: &PersistedLogEntry) -> RaftLogEntry {
    let op = match &entry.command {
        StateMachineCommand::MemberAdd { node_id, address } => {
            RaftOp::MemberAdd(RaftMemberAddCommand {
                node_id: node_id.clone(),
                address: address.clone(),
            })
        }
        StateMachineCommand::MemberRemove { node_id } => {
            RaftOp::MemberRemove(RaftMemberRemoveCommand {
                node_id: node_id.clone(),
            })
        }
        StateMachineCommand::BeginJointConsensus {
            old_members,
            new_members,
        } => RaftOp::BeginJointConsensus(RaftBeginJointConsensusCommand {
            old_members: old_members.iter().map(membership_to_proto).collect(),
            new_members: new_members.iter().map(membership_to_proto).collect(),
        }),
        StateMachineCommand::FinalizeMembership { members } => {
            RaftOp::FinalizeMembership(RaftFinalizeMembershipCommand {
                members: members.iter().map(membership_to_proto).collect(),
            })
        }
        StateMachineCommand::RestoreRuntimeSnapshot {
            payload_json,
            payload_version,
        } => RaftOp::RestoreRuntimeSnapshot(RaftRestoreRuntimeSnapshotCommand {
            payload_json: payload_json.clone(),
            payload_version: *payload_version,
        }),
        StateMachineCommand::BusinessCommand { namespace, payload } => {
            RaftOp::BusinessCommand(RaftBusinessCommand {
                namespace: namespace.clone(),
                payload: payload.clone(),
            })
        }
    };

    RaftLogEntry {
        index: entry.index,
        term: entry.term,
        command: Some(RaftCommand { op: Some(op) }),
    }
}

fn membership_from_proto(node: RaftMemberNode) -> MembershipNode {
    MembershipNode {
        node_id: node.node_id,
        address: node.address,
    }
}

fn membership_to_proto(node: &MembershipNode) -> RaftMemberNode {
    RaftMemberNode {
        node_id: node.node_id.clone(),
        address: node.address.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn joint_consensus_roundtrip_preserves_members() {
        let entry = PersistedLogEntry {
            index: 10,
            term: 3,
            command: StateMachineCommand::BeginJointConsensus {
                old_members: vec![MembershipNode {
                    node_id: "a".into(),
                    address: "127.0.0.1:1".into(),
                }],
                new_members: vec![
                    MembershipNode {
                        node_id: "a".into(),
                        address: "127.0.0.1:1".into(),
                    },
                    MembershipNode {
                        node_id: "b".into(),
                        address: "127.0.0.1:2".into(),
                    },
                ],
            },
        };
        let roundtrip = log_entry_from_proto(log_entry_to_proto(&entry)).unwrap();
        match roundtrip.command {
            StateMachineCommand::BeginJointConsensus {
                old_members,
                new_members,
            } => {
                assert_eq!(old_members.len(), 1);
                assert_eq!(new_members.len(), 2);
                assert_eq!(new_members[1].address, "127.0.0.1:2");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn missing_command_reports_error() {
        let entry = RaftLogEntry {
            index: 1,
            term: 1,
            command: None,
        };
        assert!(log_entry_from_proto(entry).is_err());
    }
}
