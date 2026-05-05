//! Role / leadership state transitions and leader-contact bookkeeping for
//! `RaftRuntime`.
//!
//! Extracted from `raft_runtime.rs` as part of Batch 4c stage 3.

use super::*;

impl RaftRuntime {
    pub(super) async fn ensure_leader(&self) -> Result<(), String> {
        if !self.is_leader().await {
            return Err(format!(
                "not leader, current leader is {}",
                self.leader_hint_label().await
            ));
        }

        Ok(())
    }

    pub(super) async fn is_leader(&self) -> bool {
        self.current_role().await == NodeRole::Leader
    }

    pub(super) async fn refresh_role_metric(&self) {
        self.state
            .metrics()
            .raft_node_state
            .set(self.current_role().await.metric_value());
    }

    pub(super) async fn current_role(&self) -> NodeRole {
        *self.role.read().await
    }

    pub(super) async fn set_role(&self, role: NodeRole, leader_hint: Option<String>) {
        {
            let mut current = self.role.write().await;
            *current = role;
        }

        {
            let mut hint = self.leader_hint.write().await;
            *hint = leader_hint;
        }

        self.refresh_role_metric().await;
    }

    pub(super) async fn become_leader(&self) {
        self.set_role(NodeRole::Leader, Some(self.state.runtime().node_id.clone()))
            .await;
        self.mark_leader_contact_now().await;
        self.mark_quorum_contact_now().await;
    }

    pub(super) async fn become_candidate(&self) {
        self.set_role(NodeRole::Candidate, None).await;
    }

    pub(super) async fn become_follower(&self, leader_hint: Option<String>) {
        self.set_role(NodeRole::Follower, leader_hint).await;
    }

    pub(super) async fn leader_hint_label(&self) -> String {
        self.leader_hint
            .read()
            .await
            .clone()
            .unwrap_or_else(|| "unknown".to_string())
    }

    pub(super) async fn sync_role_from_membership(&self) {
        let this_node = self.state.runtime().node_id.clone();
        let leader = {
            let members = self.state.members().read().await;
            members
                .keys()
                .min()
                .cloned()
                .unwrap_or_else(|| this_node.clone())
        };

        if leader == this_node {
            self.become_leader().await;
        } else {
            self.become_follower(Some(leader)).await;
            self.mark_leader_contact_now().await;
        }
    }

    pub(super) async fn current_consensus_members_for_election(&self) -> Members {
        if let Some(joint) = self.joint_consensus.read().await.clone() {
            let mut merged = joint.old_members;
            for (node_id, address) in joint.new_members {
                merged.insert(node_id, address);
            }
            return merged;
        }

        self.state.members().read().await.clone()
    }

    pub(super) async fn mark_leader_contact_now(&self) {
        {
            let mut last_seen = self.last_leader_contact.lock().await;
            *last_seen = Instant::now();
        }

        self.reset_election_deadline().await;
    }

    pub(super) async fn reset_election_deadline(&self) {
        let mut deadline = self.election_deadline.lock().await;
        *deadline = Instant::now() + random_election_timeout();
    }

    pub(super) async fn mark_quorum_contact_now(&self) {
        let mut last_quorum = self.last_quorum_contact.lock().await;
        *last_quorum = Instant::now();
    }

    pub(super) async fn leader_timed_out(&self) -> bool {
        let deadline = self.election_deadline.lock().await;
        Instant::now() >= *deadline
    }

    pub(super) async fn leader_quorum_lost_timed_out(&self) -> bool {
        let last_quorum = self.last_quorum_contact.lock().await;
        last_quorum.elapsed() >= LEADER_QUORUM_LOSS_TIMEOUT
    }
}
