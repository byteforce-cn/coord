//! Log replication, append-entries handling, commit broadcast and
//! state-machine application for `RaftRuntime`.
//!
//! Extracted from `raft_runtime.rs` as part of Batch 4c stage 3.

use super::*;
use coord_core::raft_runtime::{
    RuntimeSnapshotRestoreContext, RuntimeSnapshotRestorer, apply_state_machine_entry,
};

struct ServerRuntimeSnapshotRestorer {
    state: CoordinatorState,
    store: RaftStore,
    modules: ReplicatedModuleRegistry,
}

#[async_trait::async_trait]
impl RuntimeSnapshotRestorer for ServerRuntimeSnapshotRestorer {
    async fn restore_runtime_snapshot(
        &self,
        payload_json: &str,
        _payload_version: u32,
        context: RuntimeSnapshotRestoreContext,
    ) -> Result<(), String> {
        let mut payload = persistence::payload_v5_from_json(payload_json)
            .map_err(|err| format!("failed to parse restore payload json from raft log: {err}"))?;

        if payload.consistency.replay_strategy != "raft_log_replay" {
            return Err(format!(
                "unsupported restore replay strategy in raft log command: {}",
                payload.consistency.replay_strategy
            ));
        }

        persistence::annotate_payload_v5_with_raft_metadata(
            &mut payload,
            context.commit_index,
            context.last_applied_index,
        );

        persistence::restore_payload_v5_for_raft_replay(&self.state, payload.clone())
            .await
            .map_err(|err| format!("failed to apply restore payload during raft replay: {err}"))?;

        restore_extra_registered_module_snapshots(&self.modules, &payload.modules).await?;

        self.store.save_runtime_snapshot(&payload).map_err(|err| {
            format!("failed to persist replayed runtime snapshot into redb: {err}")
        })?;

        Ok(())
    }
}

impl RaftRuntime {
    pub async fn handle_append_entries(
        &self,
        request: RaftAppendEntriesRequest,
    ) -> Result<RaftAppendEntriesResponse, String> {
        let _guard = self.op_lock.lock().await;

        debug!(
            leader_id = %request.leader_id,
            term = request.term,
            prev_log_index = request.prev_log_index,
            entry_count = request.entries.len(),
            leader_commit = request.leader_commit,
            "received raft append_entries request"
        );

        if !request.leader_id.trim().is_empty() && !request.leader_addr.trim().is_empty() {
            let mut members = self.state.members().write().await;
            members.insert(request.leader_id.clone(), request.leader_addr.clone());
        }

        let metadata = self
            .store
            .load_metadata()
            .map_err(|err| format!("failed to load raft metadata: {err}"))?;

        if request.term < metadata.current_term {
            return Ok(RaftAppendEntriesResponse {
                success: false,
                term: metadata.current_term,
                last_log_index: metadata.last_log_index,
                message: "stale term".to_string(),
            });
        }

        self.store
            .update_term(request.term)
            .map_err(|err| format!("failed to update raft term: {err}"))?;

        let leader_hint = if request.leader_id.trim().is_empty() {
            None
        } else {
            Some(request.leader_id.clone())
        };
        self.become_follower(leader_hint).await;
        self.mark_leader_contact_now().await;

        if request.prev_log_index > 0 {
            let Some(prev) = self
                .store
                .read_log_entry(request.prev_log_index)
                .map_err(|err| format!("failed to read prev log entry: {err}"))?
            else {
                let metadata = self
                    .store
                    .load_metadata()
                    .map_err(|err| format!("failed to load raft metadata: {err}"))?;
                return Ok(RaftAppendEntriesResponse {
                    success: false,
                    term: metadata.current_term,
                    last_log_index: metadata.last_log_index,
                    message: "prev_log_index missing".to_string(),
                });
            };

            if prev.term != request.prev_log_term {
                self.store
                    .truncate_logs_from(request.prev_log_index)
                    .map_err(|err| format!("failed to truncate conflicting logs: {err}"))?;
                let metadata = self
                    .store
                    .load_metadata()
                    .map_err(|err| format!("failed to load raft metadata: {err}"))?;
                return Ok(RaftAppendEntriesResponse {
                    success: false,
                    term: metadata.current_term,
                    last_log_index: metadata.last_log_index,
                    message: "prev_log_term mismatch".to_string(),
                });
            }
        }

        let mut entries = Vec::with_capacity(request.entries.len());
        for entry in request.entries {
            entries.push(
                crate::wire::raft::log_entry_from_proto(entry)
                    .map_err(|err| format!("failed to parse raft log entry: {err}"))?,
            );
        }

        self.store
            .append_entries_from_leader(&entries)
            .map_err(|err| format!("failed to append leader entries: {err}"))?;
        let metadata = self
            .store
            .commit_to(request.leader_commit)
            .map_err(|err| format!("failed to advance commit index: {err}"))?;

        self.state
            .metrics()
            .raft_log_commit_index
            .set(metadata.commit_index as i64);
        self.state.metrics().raft_node_state.set(0);

        self.apply_committed_entries().await?;

        let final_metadata = self
            .store
            .load_metadata()
            .map_err(|err| format!("failed to reload raft metadata: {err}"))?;

        Ok(RaftAppendEntriesResponse {
            success: true,
            term: final_metadata.current_term,
            last_log_index: final_metadata.last_log_index,
            message: "ok".to_string(),
        })
    }

    pub(super) async fn replicate_for_quorum(
        &self,
        target_index: u64,
        term: u64,
        members: &HashMap<String, String>,
    ) -> Result<bool, String> {
        let quorum = majority(members.len());
        let mut acks = 1_usize;

        for (peer_id, address) in members {
            if peer_id == &self.state.runtime().node_id {
                continue;
            }
            if address.trim().is_empty() || address == "self" {
                continue;
            }

            let replicated = self
                .replicate_to_peer(peer_id, address, target_index, term)
                .await;
            match replicated {
                Ok(true) => {
                    acks = acks.saturating_add(1);
                }
                Ok(false) => {
                    debug!(peer_id = %peer_id, "peer did not ack raft entry");
                }
                Err(err) => {
                    warn!(peer_id = %peer_id, error = %err, "raft replication to peer failed");
                }
            }
        }

        if acks >= quorum {
            let metadata = self
                .store
                .commit_to(target_index)
                .map_err(|err| format!("failed to commit raft log entry: {err}"))?;
            self.state
                .metrics()
                .raft_log_commit_index
                .set(metadata.commit_index as i64);
            return Ok(true);
        }

        Ok(false)
    }

    pub(super) async fn replicate_for_active_configuration(
        &self,
        target_index: u64,
        term: u64,
    ) -> Result<bool, String> {
        if let Some(joint) = self.joint_consensus.read().await.clone() {
            return self
                .replicate_for_joint_quorum(
                    target_index,
                    term,
                    &joint.old_members,
                    &joint.new_members,
                )
                .await;
        }

        let members = self.state.members().read().await.clone();
        self.replicate_for_quorum(target_index, term, &members)
            .await
    }

    pub(super) async fn replicate_for_joint_quorum(
        &self,
        target_index: u64,
        term: u64,
        old_members: &Members,
        new_members: &Members,
    ) -> Result<bool, String> {
        let old_quorum = majority(old_members.len());
        let new_quorum = majority(new_members.len());

        let mut acknowledged = HashSet::new();
        acknowledged.insert(self.state.runtime().node_id.clone());

        let mut replication_targets = old_members.clone();
        for (node_id, addr) in new_members {
            replication_targets.insert(node_id.clone(), addr.clone());
        }

        for (peer_id, address) in replication_targets {
            if peer_id == self.state.runtime().node_id {
                continue;
            }
            if address.trim().is_empty() || address == "self" {
                continue;
            }

            match self
                .replicate_to_peer(&peer_id, &address, target_index, term)
                .await
            {
                Ok(true) => {
                    acknowledged.insert(peer_id);
                }
                Ok(false) => {
                    debug!("peer did not ack joint-consensus raft entry");
                }
                Err(err) => {
                    warn!(error = %err, "joint-consensus raft replication to peer failed");
                }
            }
        }

        let old_acks = old_members
            .keys()
            .filter(|node_id| acknowledged.contains(*node_id))
            .count();
        let new_acks = new_members
            .keys()
            .filter(|node_id| acknowledged.contains(*node_id))
            .count();

        if old_acks >= old_quorum && new_acks >= new_quorum {
            let metadata = self
                .store
                .commit_to(target_index)
                .map_err(|err| format!("failed to commit joint-consensus raft entry: {err}"))?;
            self.state
                .metrics()
                .raft_log_commit_index
                .set(metadata.commit_index as i64);
            return Ok(true);
        }

        Ok(false)
    }

    pub(super) async fn broadcast_committed_logs(&self) -> bool {
        let members = self.state.members().read().await.clone();
        self.broadcast_committed_logs_to(&members).await
    }

    pub(super) async fn broadcast_committed_logs_to(
        &self,
        members: &HashMap<String, String>,
    ) -> bool {
        let metadata = match self.store.load_metadata() {
            Ok(metadata) => metadata,
            Err(err) => {
                warn!(error = %err, "failed to load metadata for commit broadcast");
                return false;
            }
        };

        let mut acks = 1_usize;
        let quorum = majority(members.len());

        for (peer_id, address) in members {
            if peer_id == &self.state.runtime().node_id
                || address.trim().is_empty()
                || address == "self"
            {
                continue;
            }

            match self
                .replicate_to_peer(
                    peer_id,
                    address,
                    metadata.last_log_index,
                    metadata.current_term.max(1),
                )
                .await
            {
                Ok(true) => {
                    acks = acks.saturating_add(1);
                }
                Ok(false) => {
                    debug!(peer_id = %peer_id, "peer rejected heartbeat/replication broadcast");
                }
                Err(err) => {
                    warn!(peer_id = %peer_id, error = %err, "failed to broadcast committed raft logs");
                }
            }
        }

        acks >= quorum
    }

    pub(super) async fn replicate_to_peer(
        &self,
        peer_id: &str,
        address: &str,
        target_index: u64,
        term: u64,
    ) -> Result<bool, String> {
        let endpoint = normalize_endpoint(address);
        let mut client = timeout(
            Duration::from_secs(2),
            RaftInternalServiceClient::connect(endpoint.clone()),
        )
        .await
        .map_err(|_| format!("timeout connecting raft peer {peer_id} at {endpoint}"))?
        .map_err(|err| format!("failed to connect raft peer {peer_id} at {endpoint}: {err}"))?;

        let mut next_index = 1_u64;
        loop {
            let prev_log_index = next_index.saturating_sub(1);
            let prev_log_term = if prev_log_index == 0 {
                0
            } else {
                self.store
                    .read_log_entry(prev_log_index)
                    .map_err(|err| format!("failed reading prev log entry: {err}"))?
                    .map(|entry| entry.term)
                    .unwrap_or_default()
            };

            let entries = self
                .store
                .read_log_entries_from(next_index, 128)
                .map_err(|err| format!("failed reading raft logs for replication: {err}"))?
                .into_iter()
                .filter(|entry| entry.index <= target_index)
                .collect::<Vec<_>>();

            let leader_commit = self
                .store
                .load_metadata()
                .map_err(|err| format!("failed loading commit index: {err}"))?
                .commit_index;

            let request = RaftAppendEntriesRequest {
                leader_id: self.state.runtime().node_id.clone(),
                term,
                prev_log_index,
                prev_log_term,
                entries: entries
                    .iter()
                    .map(crate::wire::raft::log_entry_to_proto)
                    .collect(),
                leader_commit,
                leader_addr: self.grpc_addr.clone(),
            };

            debug!(
                peer_id = %peer_id,
                term,
                next_index,
                prev_log_index,
                entry_count = request.entries.len(),
                leader_commit,
                target_index,
                "sending raft append_entries request"
            );

            let response = timeout(
                Duration::from_secs(2),
                client.append_entries(Request::new(request)),
            )
            .await
            .map_err(|_| format!("append_entries timeout to peer {peer_id}"))?
            .map_err(|err| format!("append_entries rpc failed for peer {peer_id}: {err}"))?
            .into_inner();

            if response.term > term {
                self.store
                    .update_term(response.term)
                    .map_err(|err| format!("failed to update higher raft term: {err}"))?;
                self.become_follower(None).await;
                return Ok(false);
            }

            if response.success {
                let sent_last = entries
                    .last()
                    .map(|entry| entry.index)
                    .unwrap_or(prev_log_index);
                if sent_last >= target_index {
                    return Ok(true);
                }

                next_index = sent_last.saturating_add(1);
                continue;
            }

            if response.last_log_index == 0 && next_index == 1 {
                return Ok(false);
            }

            next_index = response
                .last_log_index
                .saturating_add(1)
                .min(next_index.saturating_sub(1))
                .max(1);
        }
    }

    pub(super) async fn apply_committed_entries(&self) -> Result<(), String> {
        loop {
            let metadata = self
                .store
                .load_metadata()
                .map_err(|err| format!("failed to load raft metadata: {err}"))?;
            if metadata.last_applied_index >= metadata.commit_index {
                break;
            }

            let next_index = metadata.last_applied_index.saturating_add(1);
            let Some(entry) = self
                .store
                .read_log_entry(next_index)
                .map_err(|err| format!("failed to read committed raft log entry: {err}"))?
            else {
                return Err(format!(
                    "raft log gap detected at committed index {next_index}"
                ));
            };

            self.apply_entry(&entry).await?;
            self.store
                .mark_applied(next_index)
                .map_err(|err| format!("failed to mark raft log entry applied: {err}"))?;
        }

        Ok(())
    }

    pub(super) async fn bootstrap_local_member_entry(&self) -> Result<(), String> {
        let _guard = self.op_lock.lock().await;

        let metadata = self
            .store
            .load_metadata()
            .map_err(|err| format!("failed to load raft metadata: {err}"))?;
        if metadata.last_log_index > 0 {
            return Ok(());
        }

        let entry = self
            .store
            .append_new_entry(
                metadata.current_term.max(1),
                StateMachineCommand::MemberAdd {
                    node_id: self.state.runtime().node_id.clone(),
                    address: self.grpc_addr.clone(),
                },
            )
            .map_err(|err| format!("failed to append bootstrap membership entry: {err}"))?;

        let metadata = self
            .store
            .commit_to(entry.index)
            .map_err(|err| format!("failed to commit bootstrap membership entry: {err}"))?;
        self.state
            .metrics()
            .raft_log_commit_index
            .set(metadata.commit_index as i64);
        self.apply_committed_entries().await?;

        info!(
            node_id = %self.state.runtime().node_id,
            commit_index = metadata.commit_index,
            "bootstrapped local raft membership entry"
        );

        Ok(())
    }

    pub(super) async fn apply_entry(&self, entry: &PersistedLogEntry) -> Result<(), String> {
        let metadata = self.store.load_metadata().map_err(|err| {
            format!("failed to load raft metadata while applying log entry: {err}")
        })?;
        let restorer = ServerRuntimeSnapshotRestorer {
            state: self.state.clone(),
            store: self.store.clone(),
            modules: self.modules.clone(),
        };

        apply_state_machine_entry(
            &self.state,
            &self.joint_consensus,
            &self.modules,
            &self.pending_results,
            entry,
            Some(&restorer),
            RuntimeSnapshotRestoreContext {
                commit_index: metadata.commit_index,
                last_applied_index: metadata.last_applied_index,
            },
        )
        .await
    }
}
