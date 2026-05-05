//! Joint-consensus membership changes for `RaftRuntime`.
//!
//! Extracted from `raft_runtime.rs` as part of Batch 4c stage 3.

use super::*;

impl RaftRuntime {
    pub async fn propose_member_add(
        &self,
        node_id: String,
        address: String,
    ) -> Result<(bool, Vec<String>), String> {
        if node_id.trim().is_empty() || address.trim().is_empty() {
            return Err("node_id and address are required".to_string());
        }

        let _guard = self.op_lock.lock().await;
        self.refresh_role_metric().await;

        self.ensure_leader().await?;

        if self.joint_consensus.read().await.is_some() {
            return Err(
                "membership change already in progress (joint consensus active)".to_string(),
            );
        }

        let members_before = self.state.members().read().await.clone();
        if members_before
            .get(&node_id)
            .map(|existing| existing == &address)
            .unwrap_or(false)
        {
            let mut names: Vec<String> = members_before.keys().cloned().collect();
            names.sort();
            return Ok((false, names));
        }

        let mut members_after = members_before.clone();
        members_after.insert(node_id.clone(), address);

        self.propose_membership_change(&members_before, &members_after)
            .await?;

        let _ = self.broadcast_committed_logs_to(&members_before).await;
        let _ = self.broadcast_committed_logs_to(&members_after).await;
        let _ = self.broadcast_committed_logs().await;
        self.refresh_role_metric().await;

        let mut names: Vec<String> = self.state.members().read().await.keys().cloned().collect();
        names.sort();

        Ok((true, names))
    }

    pub async fn propose_member_remove(
        &self,
        node_id: String,
        force_unreachable: bool,
    ) -> Result<(bool, Vec<String>), String> {
        if node_id.trim().is_empty() {
            return Err("node_id is required".to_string());
        }
        if node_id == self.state.runtime().node_id {
            return Err("cannot remove local node in current mode".to_string());
        }

        let _guard = self.op_lock.lock().await;
        self.refresh_role_metric().await;

        self.ensure_leader().await?;

        if self.joint_consensus.read().await.is_some() {
            return Err(
                "membership change already in progress (joint consensus active)".to_string(),
            );
        }

        let members_before = self.state.members().read().await.clone();
        if !members_before.contains_key(&node_id) {
            let mut names: Vec<String> = members_before.keys().cloned().collect();
            names.sort();
            return Ok((false, names));
        }

        let mut members_after = members_before.clone();
        members_after.remove(&node_id);

        let change_result = self
            .propose_membership_change(&members_before, &members_after)
            .await;

        if let Err(err) = change_result {
            if force_unreachable
                && self
                    .force_remove_unreachable(&node_id, &members_before, &members_after)
                    .await?
            {
                warn!(
                    removed_node = %node_id,
                    "applied force unreachable member removal policy"
                );
            } else {
                return Err(format!(
                    "failed to commit joint consensus member removal: {err}. use --force-unreachable only for emergency unreachable-node removal"
                ));
            }
        }

        let _ = self.broadcast_committed_logs_to(&members_before).await;
        let _ = self.broadcast_committed_logs_to(&members_after).await;
        let _ = self.broadcast_committed_logs().await;
        self.refresh_role_metric().await;

        let mut names: Vec<String> = self.state.members().read().await.keys().cloned().collect();
        names.sort();

        Ok((true, names))
    }

    pub(super) async fn propose_membership_change(
        &self,
        old_members: &Members,
        new_members: &Members,
    ) -> Result<(), String> {
        let term = self
            .store
            .load_metadata()
            .map_err(|err| format!("failed to load raft metadata: {err}"))?
            .current_term
            .max(1);

        let begin_entry = self
            .store
            .append_new_entry(
                term,
                StateMachineCommand::BeginJointConsensus {
                    old_members: members_to_nodes(old_members),
                    new_members: members_to_nodes(new_members),
                },
            )
            .map_err(|err| format!("failed to append joint consensus begin entry: {err}"))?;

        let begin_committed = self
            .replicate_for_quorum(begin_entry.index, term, old_members)
            .await?;

        if !begin_committed {
            return Err("failed to commit joint consensus begin entry with old quorum".to_string());
        }

        self.apply_committed_entries().await?;

        let finalize_entry = self
            .store
            .append_new_entry(
                term,
                StateMachineCommand::FinalizeMembership {
                    members: members_to_nodes(new_members),
                },
            )
            .map_err(|err| format!("failed to append joint consensus finalize entry: {err}"))?;

        let finalize_committed = if old_members.len() <= 1 {
            // Single-node bootstrap policy: allow first scale-out without strict joint quorum.
            self.replicate_for_quorum(finalize_entry.index, term, old_members)
                .await?
        } else {
            self.replicate_for_joint_quorum(finalize_entry.index, term, old_members, new_members)
                .await?
        };

        if !finalize_committed {
            return Err("failed to commit joint consensus finalize entry".to_string());
        }

        self.apply_committed_entries().await?;

        Ok(())
    }

    pub(super) async fn force_remove_unreachable(
        &self,
        target_node_id: &str,
        old_members: &Members,
        new_members: &Members,
    ) -> Result<bool, String> {
        if old_members.len() > 2 {
            return Ok(false);
        }

        let Some(target_addr) = old_members.get(target_node_id) else {
            return Ok(false);
        };

        if !self.leader_quorum_lost_timed_out().await {
            return Err(
                "force removal requires sustained quorum loss timeout before execution".to_string(),
            );
        }

        let metadata = self
            .store
            .load_metadata()
            .map_err(|err| format!("failed to load raft metadata for force removal: {err}"))?;
        let term = metadata.current_term.max(1);

        let reachable = self
            .replicate_to_peer(target_node_id, target_addr, metadata.last_log_index, term)
            .await
            .unwrap_or(false);
        if reachable {
            return Err(
                "target node appears reachable; refusing force-unreachable remove".to_string(),
            );
        }

        let entry = self
            .store
            .append_new_entry(
                term,
                StateMachineCommand::FinalizeMembership {
                    members: members_to_nodes(new_members),
                },
            )
            .map_err(|err| format!("failed to append force finalize membership entry: {err}"))?;

        let metadata = self
            .store
            .commit_to(entry.index)
            .map_err(|err| format!("failed to force commit membership finalize entry: {err}"))?;
        self.state
            .metrics()
            .raft_log_commit_index
            .set(metadata.commit_index as i64);
        self.apply_committed_entries().await?;

        warn!(
            removed_node = %target_node_id,
            commit_index = metadata.commit_index,
            "force-unreachable member removal committed locally"
        );

        Ok(true)
    }
}
