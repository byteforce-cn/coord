//! Backup / restore / config-put proposals for `RaftRuntime`.
//!
//! Extracted from `raft_runtime.rs` as part of Batch 4c stage 3.

use super::*;

impl RaftRuntime {
    /// Collect a v5 backup snapshot annotated with the current Raft commit index.
    #[tracing::instrument(skip(self), name = "raft.backup_snapshot")]
    pub async fn snapshot_backup_payload(&self) -> Result<persistence::BackupPayloadV5, String> {
        self.ensure_leader().await?;

        let mut payload = persistence::snapshot_payload_v5(&self.state)
            .await
            .map_err(|err| format!("failed to collect v5 backup snapshot: {err}"))?;
        let metadata = self
            .store
            .load_metadata()
            .map_err(|err| format!("failed to load raft metadata for backup snapshot: {err}"))?;
        persistence::annotate_payload_v5_with_raft_metadata(
            &mut payload,
            metadata.commit_index,
            metadata.last_applied_index,
        );
        Ok(payload)
    }

    #[tracing::instrument(skip(self, payload_json), name = "raft.propose_restore")]
    pub async fn propose_backup_restore(&self, payload_json: String) -> Result<String, String> {
        if payload_json.trim().is_empty() {
            return Err("payload_json cannot be empty".to_string());
        }

        let _guard = self.op_lock.lock().await;
        self.refresh_role_metric().await;
        self.ensure_leader().await?;

        let payload = persistence::payload_v5_from_json(&payload_json)
            .map_err(|err| format!("invalid backup payload json: {err}"))?;

        if payload.consistency.replay_strategy != "raft_log_replay" {
            return Err(format!(
                "unsupported backup replay strategy: {}",
                payload.consistency.replay_strategy
            ));
        }

        let canonical_json = persistence::payload_to_json_v5(&payload)
            .map_err(|err| format!("failed to canonicalize backup payload json: {err}"))?;

        let term = self
            .store
            .load_metadata()
            .map_err(|err| format!("failed to load raft metadata: {err}"))?
            .current_term
            .max(1);

        let entry = self
            .store
            .append_new_entry(
                term,
                StateMachineCommand::RestoreRuntimeSnapshot {
                    payload_json: canonical_json,
                    payload_version: payload.version,
                },
            )
            .map_err(|err| format!("failed to append backup restore raft log entry: {err}"))?;

        let committed = self
            .replicate_for_active_configuration(entry.index, term)
            .await?;
        if !committed {
            return Err("failed to reach quorum for backup restore replay command".to_string());
        }

        self.apply_committed_entries().await?;
        let _ = self.broadcast_committed_logs().await;

        let upgrade_note = if let Some(v) = payload.consistency.upgraded_from_version {
            format!(" (upgraded from payload v{v})")
        } else {
            String::new()
        };

        Ok(format!(
            "backup restored via raft_log_replay at payload v{}{}",
            payload.version, upgrade_note
        ))
    }

    #[allow(dead_code)] // 待 CLI backup 命令接入后移除
    pub async fn propose_put_config(
        &self,
        key: String,
        value: String,
    ) -> Result<ConfigEntry, String> {
        if key.trim().is_empty() {
            return Err("config key cannot be empty".to_string());
        }

        let payload = coord_core::config::ConfigCenter::encode_put_command_bytes(&key, &value);
        self.propose_business_command("config", payload).await?;

        self.state
            .config()
            .get(&key)
            .await
            .ok_or_else(|| format!("config key '{key}' missing after raft apply"))
            .or({
                // Fallback: construct a synthetic entry (apply must have set it)
                Ok(ConfigEntry {
                    key,
                    value,
                    version: 1,
                    revision: 0,
                })
            })
    }

    /// Propose a generic business command through Raft consensus.
    ///
    /// # Parameters
    /// - `namespace`: the module namespace (must match a registered `ReplicatedModule`)
    /// - `payload`: serialized command bytes (module-defined format)
    #[tracing::instrument(
        skip(self, payload),
        name = "raft.propose_business_command",
        fields(namespace)
    )]
    pub async fn propose_business_command(
        &self,
        namespace: &str,
        payload: Vec<u8>,
    ) -> Result<(), String> {
        let _guard = self.op_lock.lock().await;
        self.refresh_role_metric().await;
        self.ensure_leader().await?;

        let term = self
            .store
            .load_metadata()
            .map_err(|err| format!("failed to load raft metadata: {err}"))?
            .current_term
            .max(1);

        let entry = self
            .store
            .append_new_entry(
                term,
                StateMachineCommand::BusinessCommand {
                    namespace: namespace.to_string(),
                    payload,
                },
            )
            .map_err(|err| format!("failed to append {namespace} raft log entry: {err}"))?;

        let committed = self
            .replicate_for_active_configuration(entry.index, term)
            .await?;
        if !committed {
            return Err(format!(
                "failed to reach quorum for {namespace} business command"
            ));
        }

        self.apply_committed_entries().await?;
        let _ = self.broadcast_committed_logs().await;

        Ok(())
    }

    /// Propose a generic business command and wait for the apply result.
    ///
    /// Unlike [`propose_business_command`] which returns `Ok(())` on success,
    /// this variant installs a result waiter keyed by the log index so that
    /// the module's [`ReplicatedModule::apply`] return value is threaded back
    /// to the caller. Use this when you need the committed apply outcome (e.g.
    /// to report `AcquireOutcome` for a lock acquire without re-applying).
    #[tracing::instrument(
        skip(self, payload),
        name = "raft.propose_business_command_for_result",
        fields(namespace)
    )]
    pub async fn propose_business_command_for_result(
        &self,
        namespace: &str,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, String> {
        let _guard = self.op_lock.lock().await;
        self.refresh_role_metric().await;
        self.ensure_leader().await?;

        let term = self
            .store
            .load_metadata()
            .map_err(|err| format!("failed to load raft metadata: {err}"))?
            .current_term
            .max(1);

        let entry = self
            .store
            .append_new_entry(
                term,
                StateMachineCommand::BusinessCommand {
                    namespace: namespace.to_string(),
                    payload,
                },
            )
            .map_err(|err| format!("failed to append {namespace} raft log entry: {err}"))?;

        // Register a result waiter keyed by the log index before replication.
        let (tx, mut rx) = tokio::sync::oneshot::channel();
        {
            let mut pending = self
                .pending_results
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            pending.insert(entry.index, tx);
        }

        let committed = self
            .replicate_for_active_configuration(entry.index, term)
            .await
            .inspect_err(|_err| {
                // Clean up the waiter on replication failure.
                let mut pending = self
                    .pending_results
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                pending.remove(&entry.index);
            })?;

        if !committed {
            let mut pending = self
                .pending_results
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            pending.remove(&entry.index);
            return Err(format!(
                "failed to reach quorum for {namespace} business command"
            ));
        }

        self.apply_committed_entries().await?;
        let _ = self.broadcast_committed_logs().await;

        // Retrieve the apply result sent by apply_entry() during apply_committed_entries().
        rx.try_recv().unwrap_or_else(|_| {
            Err(format!(
                "{namespace} apply result not received for entry at index {}",
                entry.index
            ))
        })
    }
}
