//! Raft leader election (vote, pre-vote, election round) for
//! `RaftRuntime`.
//!
//! Extracted from `raft_runtime.rs` as part of Batch 4c stage 3.

use super::*;

impl RaftRuntime {
    pub async fn handle_request_vote(
        &self,
        request: RaftRequestVoteRequest,
    ) -> Result<RaftRequestVoteResponse, String> {
        let _guard = self.op_lock.lock().await;

        let mut metadata = self
            .store
            .load_metadata()
            .map_err(|err| format!("failed to load raft metadata: {err}"))?;

        if request.term < metadata.current_term {
            return Ok(RaftRequestVoteResponse {
                vote_granted: false,
                term: metadata.current_term,
                message: "stale term".to_string(),
            });
        }

        let term_advanced = request.term > metadata.current_term;
        metadata = self
            .store
            .update_term(request.term)
            .map_err(|err| format!("failed to update raft term: {err}"))?;

        if term_advanced {
            self.become_follower(None).await;
            self.mark_leader_contact_now().await;
        }

        let local_last_term = if metadata.last_log_index == 0 {
            0
        } else {
            self.store
                .read_log_entry(metadata.last_log_index)
                .map_err(|err| format!("failed to read last log entry: {err}"))?
                .map(|entry| entry.term)
                .unwrap_or_default()
        };

        let candidate_up_to_date = request.last_log_term > local_last_term
            || (request.last_log_term == local_last_term
                && request.last_log_index >= metadata.last_log_index);

        let already_voted_for = metadata.voted_for.clone();
        let vote_granted = if !candidate_up_to_date {
            false
        } else if let Some(voted_for) = already_voted_for {
            voted_for == request.candidate_id
        } else {
            true
        };

        if vote_granted {
            self.store
                .record_vote(request.term, Some(request.candidate_id.clone()))
                .map_err(|err| format!("failed to persist vote: {err}"))?;
            self.become_follower(None).await;
            self.mark_leader_contact_now().await;
        }

        let metadata = self
            .store
            .load_metadata()
            .map_err(|err| format!("failed to reload raft metadata: {err}"))?;

        Ok(RaftRequestVoteResponse {
            vote_granted,
            term: metadata.current_term,
            message: if vote_granted {
                "vote granted".to_string()
            } else {
                "vote rejected".to_string()
            },
        })
    }

    pub async fn handle_pre_vote(
        &self,
        request: RaftPreVoteRequest,
    ) -> Result<RaftPreVoteResponse, String> {
        let _guard = self.op_lock.lock().await;

        let metadata = self
            .store
            .load_metadata()
            .map_err(|err| format!("failed to load raft metadata: {err}"))?;

        if request.term < metadata.current_term {
            return Ok(RaftPreVoteResponse {
                vote_granted: false,
                term: metadata.current_term,
                message: "stale term".to_string(),
            });
        }

        let local_last_term = self.load_last_log_term(metadata.last_log_index)?;
        let candidate_up_to_date = request.last_log_term > local_last_term
            || (request.last_log_term == local_last_term
                && request.last_log_index >= metadata.last_log_index);

        if !candidate_up_to_date {
            return Ok(RaftPreVoteResponse {
                vote_granted: false,
                term: metadata.current_term,
                message: "candidate log is not up-to-date".to_string(),
            });
        }

        // If leader lease is still active, avoid disruptive elections.
        if !self.leader_timed_out().await {
            return Ok(RaftPreVoteResponse {
                vote_granted: false,
                term: metadata.current_term,
                message: "leader lease is active".to_string(),
            });
        }

        Ok(RaftPreVoteResponse {
            vote_granted: true,
            term: metadata.current_term,
            message: "pre-vote granted".to_string(),
        })
    }

    pub(super) async fn start_election(&self) -> Result<(), String> {
        let _guard = self.op_lock.lock().await;

        if self.is_leader().await {
            return Ok(());
        }

        let members = self.current_consensus_members_for_election().await;
        let quorum = majority(members.len());
        if quorum <= 1 {
            self.become_leader().await;
            return Ok(());
        }

        if !self.run_pre_vote(&members).await? {
            self.become_follower(None).await;
            self.reset_election_deadline().await;
            return Ok(());
        }

        let metadata = self
            .store
            .load_metadata()
            .map_err(|err| format!("failed to load raft metadata for election: {err}"))?;
        let election_term = metadata.current_term.saturating_add(1).max(1);
        let last_log_index = metadata.last_log_index;
        let last_log_term = self.load_last_log_term(last_log_index)?;

        self.store
            .record_vote(election_term, Some(self.state.runtime().node_id.clone()))
            .map_err(|err| format!("failed to persist self-vote for election: {err}"))?;
        self.become_candidate().await;

        let request = RaftRequestVoteRequest {
            candidate_id: self.state.runtime().node_id.clone(),
            term: election_term,
            last_log_index,
            last_log_term,
        };

        let mut votes = 1_usize;
        for (peer_id, address) in &members {
            if peer_id == &self.state.runtime().node_id {
                continue;
            }
            if address.trim().is_empty() || address == "self" {
                continue;
            }

            match self
                .request_vote_from_peer(peer_id, address, request.clone())
                .await
            {
                Ok(response) => {
                    if response.term > election_term {
                        self.store.update_term(response.term).map_err(|err| {
                            format!("failed to update higher raft term during election: {err}")
                        })?;
                        self.become_follower(None).await;
                        self.mark_leader_contact_now().await;
                        return Ok(());
                    }

                    if response.vote_granted {
                        votes = votes.saturating_add(1);
                    }
                }
                Err(err) => {
                    warn!(peer_id = %peer_id, error = %err, "request_vote rpc failed");
                }
            }
        }

        if votes >= quorum {
            self.become_leader().await;
            self.state.metrics().raft_leader_elections_total.inc();
            info!(
                term = election_term,
                votes, quorum, "raft election succeeded; this node became leader"
            );
            let _ = self.broadcast_committed_logs().await;
            return Ok(());
        }

        self.become_follower(None).await;
        self.mark_leader_contact_now().await;
        warn!(
            term = election_term,
            votes, quorum, "raft election failed to reach quorum"
        );

        Ok(())
    }

    pub(super) async fn run_pre_vote(
        &self,
        members: &HashMap<String, String>,
    ) -> Result<bool, String> {
        let metadata = self
            .store
            .load_metadata()
            .map_err(|err| format!("failed to load raft metadata for pre-vote: {err}"))?;

        let quorum = majority(members.len());
        let pre_vote_term = metadata.current_term.saturating_add(1).max(1);
        let last_log_index = metadata.last_log_index;
        let last_log_term = self.load_last_log_term(last_log_index)?;

        let request = RaftPreVoteRequest {
            candidate_id: self.state.runtime().node_id.clone(),
            term: pre_vote_term,
            last_log_index,
            last_log_term,
        };

        let mut votes = 1_usize;
        for (peer_id, address) in members {
            if peer_id == &self.state.runtime().node_id {
                continue;
            }
            if address.trim().is_empty() || address == "self" {
                continue;
            }

            match self
                .pre_vote_from_peer(peer_id, address, request.clone())
                .await
            {
                Ok(response) => {
                    if response.term > metadata.current_term {
                        self.store.update_term(response.term).map_err(|err| {
                            format!("failed to update higher term from pre-vote response: {err}")
                        })?;
                        self.become_follower(None).await;
                        self.mark_leader_contact_now().await;
                        return Ok(false);
                    }

                    if response.vote_granted {
                        votes = votes.saturating_add(1);
                    }
                }
                Err(err) => {
                    warn!(peer_id = %peer_id, error = %err, "pre_vote rpc failed");
                }
            }
        }

        if votes >= quorum {
            return Ok(true);
        }

        warn!(
            term = pre_vote_term,
            votes, quorum, "raft pre-vote did not reach quorum"
        );
        Ok(false)
    }

    pub(super) async fn request_vote_from_peer(
        &self,
        peer_id: &str,
        address: &str,
        request: RaftRequestVoteRequest,
    ) -> Result<RaftRequestVoteResponse, String> {
        let endpoint = normalize_endpoint(address);
        let mut client = timeout(
            Duration::from_secs(2),
            RaftInternalServiceClient::connect(endpoint.clone()),
        )
        .await
        .map_err(|_| format!("timeout connecting raft peer {peer_id} at {endpoint}"))?
        .map_err(|err| format!("failed to connect raft peer {peer_id} at {endpoint}: {err}"))?;

        timeout(
            Duration::from_secs(2),
            client.request_vote(Request::new(request)),
        )
        .await
        .map_err(|_| format!("request_vote timeout to peer {peer_id}"))?
        .map_err(|err| format!("request_vote rpc failed for peer {peer_id}: {err}"))
        .map(|response| response.into_inner())
    }

    pub(super) async fn pre_vote_from_peer(
        &self,
        peer_id: &str,
        address: &str,
        request: RaftPreVoteRequest,
    ) -> Result<RaftPreVoteResponse, String> {
        let endpoint = normalize_endpoint(address);
        let mut client = timeout(
            Duration::from_secs(2),
            RaftInternalServiceClient::connect(endpoint.clone()),
        )
        .await
        .map_err(|_| format!("timeout connecting raft peer {peer_id} at {endpoint}"))?
        .map_err(|err| format!("failed to connect raft peer {peer_id} at {endpoint}: {err}"))?;

        timeout(
            Duration::from_secs(2),
            client.pre_vote(Request::new(request)),
        )
        .await
        .map_err(|_| format!("pre_vote timeout to peer {peer_id}"))?
        .map_err(|err| format!("pre_vote rpc failed for peer {peer_id}: {err}"))
        .map(|response| response.into_inner())
    }
}
