use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, anyhow};
use coord_core::raft_runtime::{PersistedLogEntry, StateMachineCommand};
use openraft::{BasicNode, Config as RaftConfig};
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use crate::persistence;

const RAFT_META_TABLE: TableDefinition<&str, &str> = TableDefinition::new("raft_meta");
const RAFT_LOG_TABLE: TableDefinition<u64, &str> = TableDefinition::new("raft_log");
const META_KEY: &str = "metadata";
const BOOTSTRAP_KEY: &str = "bootstrap";
const RUNTIME_SNAPSHOT_KEY: &str = "runtime_snapshot";

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RaftMetadata {
    pub current_term: u64,
    pub voted_for: Option<String>,
    pub last_log_index: u64,
    pub last_applied_index: u64,
    pub commit_index: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpenRaftBootstrap {
    pub node_id: String,
    pub node: BasicNode,
    pub cluster_name: String,
}

#[derive(Clone)]
pub struct RaftStore {
    db: Arc<Database>,
    db_path: PathBuf,
    pub raft_config: Arc<RaftConfig>,
}

impl RaftStore {
    pub fn open(data_dir: &Path, node_id: &str, grpc_addr: &str) -> anyhow::Result<Self> {
        let db_path = data_dir.join("coord.redb");
        let db = if db_path.exists() {
            Database::open(&db_path)
                .with_context(|| format!("failed to open redb database: {}", db_path.display()))?
        } else {
            Database::create(&db_path)
                .with_context(|| format!("failed to create redb database: {}", db_path.display()))?
        };

        let raft_config = RaftConfig {
            cluster_name: "coord-dev".to_string(),
            ..RaftConfig::default()
        }
        .validate()
        .map_err(anyhow::Error::msg)
        .context("invalid OpenRaft configuration")?;

        let store = Self {
            db: Arc::new(db),
            db_path,
            raft_config: Arc::new(raft_config),
        };

        store.ensure_tables()?;
        if store.read_json::<RaftMetadata>(META_KEY)?.is_none() {
            store.save_metadata(&RaftMetadata::default())?;
        }

        store.write_json(
            BOOTSTRAP_KEY,
            &OpenRaftBootstrap {
                node_id: node_id.to_string(),
                node: BasicNode::new(grpc_addr),
                cluster_name: store.raft_config.cluster_name.clone(),
            },
        )?;

        Ok(store)
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    pub fn load_bootstrap(&self) -> anyhow::Result<Option<OpenRaftBootstrap>> {
        self.read_json(BOOTSTRAP_KEY)
    }

    pub fn load_metadata(&self) -> anyhow::Result<RaftMetadata> {
        Ok(self.read_json(META_KEY)?.unwrap_or_default())
    }

    pub fn save_metadata(&self, metadata: &RaftMetadata) -> anyhow::Result<()> {
        self.write_json(META_KEY, metadata)
    }

    pub fn update_term(&self, term: u64) -> anyhow::Result<RaftMetadata> {
        let mut metadata = self.load_metadata()?;
        if term > metadata.current_term {
            metadata.current_term = term;
            metadata.voted_for = None;
            self.save_metadata(&metadata)?;
        }
        Ok(metadata)
    }

    pub fn record_vote(
        &self,
        term: u64,
        voted_for: Option<String>,
    ) -> anyhow::Result<RaftMetadata> {
        let mut metadata = self.load_metadata()?;
        if term > metadata.current_term {
            metadata.current_term = term;
        }
        metadata.voted_for = voted_for;
        self.save_metadata(&metadata)?;
        Ok(metadata)
    }

    pub fn append_new_entry(
        &self,
        term: u64,
        command: StateMachineCommand,
    ) -> anyhow::Result<PersistedLogEntry> {
        let mut metadata = self.load_metadata()?;
        if term > metadata.current_term {
            metadata.current_term = term;
            metadata.voted_for = None;
        }

        let entry = PersistedLogEntry {
            index: metadata.last_log_index.saturating_add(1),
            term: metadata.current_term.max(1),
            command,
        };

        self.write_log_entry(&entry)?;

        metadata.last_log_index = entry.index;
        self.save_metadata(&metadata)?;

        Ok(entry)
    }

    pub fn append_entries_from_leader(
        &self,
        entries: &[PersistedLogEntry],
    ) -> anyhow::Result<RaftMetadata> {
        if entries.is_empty() {
            return self.load_metadata();
        }

        let mut metadata = self.load_metadata()?;

        for entry in entries {
            if entry.index == 0 {
                return Err(anyhow!("raft log entry index must be greater than zero"));
            }

            if let Some(existing) = self.read_log_entry(entry.index)? {
                if existing.term != entry.term {
                    self.truncate_logs_from(entry.index)?;
                    metadata.last_log_index = entry.index.saturating_sub(1);
                } else {
                    if existing.command == entry.command {
                        continue;
                    }

                    self.truncate_logs_from(entry.index)?;
                    metadata.last_log_index = entry.index.saturating_sub(1);
                }
            }

            self.write_log_entry(entry)?;
            metadata.last_log_index = metadata.last_log_index.max(entry.index);
            metadata.current_term = metadata.current_term.max(entry.term);
        }

        self.save_metadata(&metadata)?;
        Ok(metadata)
    }

    pub fn read_log_entry(&self, index: u64) -> anyhow::Result<Option<PersistedLogEntry>> {
        let read_txn = self
            .db
            .begin_read()
            .context("failed to begin redb read transaction")?;
        let table = read_txn
            .open_table(RAFT_LOG_TABLE)
            .context("failed to open redb raft_log table for read")?;

        let Some(value) = table
            .get(index)
            .with_context(|| format!("failed to read redb raft log index: {index}"))?
        else {
            return Ok(None);
        };

        let decoded = serde_json::from_str(value.value())
            .with_context(|| format!("failed to decode raft log entry json at index: {index}"))?;
        Ok(Some(decoded))
    }

    pub fn read_log_entries_from(
        &self,
        start_index: u64,
        limit: usize,
    ) -> anyhow::Result<Vec<PersistedLogEntry>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let read_txn = self
            .db
            .begin_read()
            .context("failed to begin redb read transaction")?;
        let table = read_txn
            .open_table(RAFT_LOG_TABLE)
            .context("failed to open redb raft_log table for range read")?;

        let iter = table
            .range(start_index..)
            .with_context(|| format!("failed to iterate raft logs from index: {start_index}"))?;

        let mut out = Vec::new();
        for next in iter.take(limit) {
            let (_, value) = next.context("failed to read raft log range item")?;
            let entry: PersistedLogEntry = serde_json::from_str(value.value())
                .context("failed to decode raft log range entry json")?;
            out.push(entry);
        }

        Ok(out)
    }

    pub fn truncate_logs_from(&self, start_index: u64) -> anyhow::Result<()> {
        let write_txn = self
            .db
            .begin_write()
            .context("failed to begin redb write transaction")?;
        {
            let mut table = write_txn
                .open_table(RAFT_LOG_TABLE)
                .context("failed to open redb raft_log table for truncate")?;

            let keys_to_remove = table
                .range(start_index..)
                .with_context(|| format!("failed to iterate logs for truncate from {start_index}"))?
                .map(|row| row.map(|(key, _)| key.value()))
                .collect::<Result<Vec<u64>, _>>()
                .context("failed to collect raft log truncate keys")?;

            for key in keys_to_remove {
                table
                    .remove(key)
                    .with_context(|| format!("failed to remove raft log at index: {key}"))?;
            }
        }
        write_txn
            .commit()
            .context("failed to commit redb log truncate transaction")?;

        let mut metadata = self.load_metadata()?;
        metadata.last_log_index = metadata.last_log_index.min(start_index.saturating_sub(1));
        metadata.commit_index = metadata.commit_index.min(metadata.last_log_index);
        metadata.last_applied_index = metadata.last_applied_index.min(metadata.commit_index);
        self.save_metadata(&metadata)?;

        Ok(())
    }

    pub fn commit_to(&self, leader_commit: u64) -> anyhow::Result<RaftMetadata> {
        let mut metadata = self.load_metadata()?;
        let target = leader_commit.min(metadata.last_log_index);
        if target > metadata.commit_index {
            metadata.commit_index = target;
            self.save_metadata(&metadata)?;
        }
        Ok(metadata)
    }

    pub fn mark_applied(&self, last_applied_index: u64) -> anyhow::Result<RaftMetadata> {
        let mut metadata = self.load_metadata()?;
        metadata.last_applied_index = last_applied_index.min(metadata.commit_index);
        self.save_metadata(&metadata)?;
        Ok(metadata)
    }

    /// Save a v5 snapshot to the Redb store (canonical write path).
    pub fn save_runtime_snapshot(
        &self,
        payload: &persistence::BackupPayloadV5,
    ) -> anyhow::Result<()> {
        let payload_json = persistence::payload_to_json_v5(payload)?;
        self.write_json(RUNTIME_SNAPSHOT_KEY, &payload_json)
    }

    /// Load the stored runtime snapshot (v5 format).
    pub fn load_runtime_snapshot(&self) -> anyhow::Result<Option<persistence::BackupPayloadV5>> {
        let Some(payload_json) = self.read_json::<String>(RUNTIME_SNAPSHOT_KEY)? else {
            return Ok(None);
        };

        let payload = persistence::payload_v5_from_json(&payload_json)?;
        Ok(Some(payload))
    }

    fn ensure_tables(&self) -> anyhow::Result<()> {
        let write_txn = self
            .db
            .begin_write()
            .context("failed to begin write transaction for redb table init")?;
        {
            let _ = write_txn
                .open_table(RAFT_META_TABLE)
                .context("failed to open redb raft_meta table")?;
            let _ = write_txn
                .open_table(RAFT_LOG_TABLE)
                .context("failed to open redb raft_log table")?;
        }
        write_txn
            .commit()
            .context("failed to commit redb table initialization")?;
        Ok(())
    }

    fn write_json<T>(&self, key: &str, value: &T) -> anyhow::Result<()>
    where
        T: Serialize,
    {
        let payload =
            serde_json::to_string(value).context("failed to serialize redb json payload")?;

        let write_txn = self
            .db
            .begin_write()
            .context("failed to begin redb write transaction")?;
        {
            let mut table = write_txn
                .open_table(RAFT_META_TABLE)
                .context("failed to open redb raft_meta table for write")?;
            table
                .insert(key, payload.as_str())
                .with_context(|| format!("failed to write redb key: {key}"))?;
        }
        write_txn
            .commit()
            .context("failed to commit redb write transaction")?;

        Ok(())
    }

    fn read_json<T>(&self, key: &str) -> anyhow::Result<Option<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        let read_txn = self
            .db
            .begin_read()
            .context("failed to begin redb read transaction")?;
        let table = read_txn
            .open_table(RAFT_META_TABLE)
            .context("failed to open redb raft_meta table for read")?;

        let Some(value) = table
            .get(key)
            .with_context(|| format!("failed to read redb key: {key}"))?
        else {
            return Ok(None);
        };

        let decoded = serde_json::from_str(value.value())
            .with_context(|| format!("failed to decode json for redb key: {key}"))?;
        Ok(Some(decoded))
    }

    fn write_log_entry(&self, entry: &PersistedLogEntry) -> anyhow::Result<()> {
        let payload =
            serde_json::to_string(entry).context("failed to serialize raft log entry payload")?;

        let write_txn = self
            .db
            .begin_write()
            .context("failed to begin redb write transaction")?;
        {
            let mut table = write_txn
                .open_table(RAFT_LOG_TABLE)
                .context("failed to open redb raft_log table for write")?;
            table
                .insert(entry.index, payload.as_str())
                .with_context(|| format!("failed to write raft log entry: {}", entry.index))?;
        }
        write_txn
            .commit()
            .context("failed to commit redb raft log write transaction")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use uuid::Uuid;

    use coord_core::raft_runtime::{PersistedLogEntry, StateMachineCommand};

    use super::RaftStore;

    fn unique_temp_dir(tag: &str) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("coord-raft-{tag}-{}", Uuid::new_v4().simple()));
        std::fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    #[test]
    fn metadata_and_bootstrap_survive_restart() {
        let data_dir = unique_temp_dir("restart");

        {
            let store =
                RaftStore::open(&data_dir, "node-a", "127.0.0.1:9090").expect("open raft store");
            let before = store.load_metadata().expect("load raft metadata");
            assert_eq!(before.commit_index, 0);

            let entry = store
                .append_new_entry(
                    1,
                    StateMachineCommand::MemberAdd {
                        node_id: "node-b".to_string(),
                        address: "127.0.0.1:9092".to_string(),
                    },
                )
                .expect("append raft log entry");

            let next = store.commit_to(entry.index).expect("commit raft log entry");
            assert_eq!(next.commit_index, 1);

            let bootstrap = store
                .load_bootstrap()
                .expect("load bootstrap")
                .expect("bootstrap should exist");
            assert_eq!(bootstrap.node_id, "node-a");
            assert_eq!(bootstrap.node.addr, "127.0.0.1:9090");
        }

        let reopened =
            RaftStore::open(&data_dir, "node-a", "127.0.0.1:9090").expect("reopen raft store");
        let persisted = reopened
            .load_metadata()
            .expect("load metadata after restart");
        assert_eq!(persisted.commit_index, 1);
        assert_eq!(persisted.last_log_index, 1);
    }

    #[test]
    fn replicated_log_entries_survive_restart() {
        let data_dir = unique_temp_dir("logs");

        {
            let store =
                RaftStore::open(&data_dir, "node-a", "127.0.0.1:9090").expect("open raft store");
            store
                .append_entries_from_leader(&[
                    PersistedLogEntry {
                        index: 1,
                        term: 1,
                        command: StateMachineCommand::MemberAdd {
                            node_id: "node-a".to_string(),
                            address: "127.0.0.1:9090".to_string(),
                        },
                    },
                    PersistedLogEntry {
                        index: 2,
                        term: 1,
                        command: StateMachineCommand::MemberAdd {
                            node_id: "node-b".to_string(),
                            address: "127.0.0.1:9092".to_string(),
                        },
                    },
                ])
                .expect("append replicated entries");
            store.commit_to(2).expect("commit entries");
        }

        let reopened =
            RaftStore::open(&data_dir, "node-a", "127.0.0.1:9090").expect("reopen raft store");
        let entry = reopened
            .read_log_entry(2)
            .expect("read log entry")
            .expect("entry should exist");
        assert_eq!(entry.index, 2);
        assert_eq!(entry.term, 1);
    }

    #[test]
    fn runtime_snapshot_survives_restart() {
        use crate::persistence::{BackupConsistencyMeta, BackupPayloadV5, MemberItem};
        use std::collections::HashMap;

        let data_dir = unique_temp_dir("runtime-snapshot");

        let mut modules: HashMap<String, Vec<u8>> = HashMap::new();
        let members = vec![MemberItem {
            node_id: "node-a".to_string(),
            address: "127.0.0.1:9090".to_string(),
        }];
        modules.insert("members".to_string(), serde_json::to_vec(&members).unwrap());

        let payload = BackupPayloadV5 {
            version: 5,
            created_unix_ms: 123,
            modules,
            consistency: BackupConsistencyMeta::default(),
        };

        {
            let store =
                RaftStore::open(&data_dir, "node-a", "127.0.0.1:9090").expect("open raft store");
            store
                .save_runtime_snapshot(&payload)
                .expect("save runtime snapshot");
        }

        let reopened =
            RaftStore::open(&data_dir, "node-a", "127.0.0.1:9090").expect("reopen raft store");
        let restored = reopened
            .load_runtime_snapshot()
            .expect("load runtime snapshot")
            .expect("runtime snapshot should exist");

        // v5 JSON is loaded and parsed directly.
        // The important invariant is that the member list is preserved.
        assert_eq!(restored.version, 5);
        let members_bytes = restored.modules.get("members").expect("members module");
        let members: Vec<MemberItem> =
            serde_json::from_slice(members_bytes).expect("parse members");
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].node_id, "node-a");
    }
}
