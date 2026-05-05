use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use coord_core::config::ConfigEntry;
use coord_core::replication::ReplicatedModule;
use coord_core::state::CoordinatorState;
use coord_proto::coord::v1::raft_internal_service_client::RaftInternalServiceClient;
use coord_proto::coord::v1::{
    RaftAppendEntriesRequest, RaftAppendEntriesResponse, RaftPreVoteRequest, RaftPreVoteResponse,
    RaftRequestVoteRequest, RaftRequestVoteResponse,
};
use tokio::sync::{Mutex, RwLock};
use tokio::time::timeout;
use tonic::Request;
use tracing::{debug, info, warn};

use crate::persistence;
use crate::raft_store::{PersistedLogEntry, RaftStore, StateMachineCommand};

mod election;
mod helpers;
mod membership;
mod persistence_ops;
mod replication;
mod role;

#[cfg(test)]
use helpers::{ELECTION_TIMEOUT_BASE, ELECTION_TIMEOUT_JITTER_MAX};
use helpers::{
    majority, members_to_nodes, nodes_to_members, normalize_endpoint, random_election_timeout,
};

pub const RAFT_TICK_INTERVAL: Duration = Duration::from_millis(800);
const LEADER_QUORUM_LOSS_TIMEOUT: Duration = Duration::from_secs(6);

type Members = HashMap<String, String>;

#[derive(Clone, Debug)]
struct JointConsensusState {
    old_members: Members,
    new_members: Members,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NodeRole {
    Follower,
    Candidate,
    Leader,
}

impl NodeRole {
    fn label(self) -> &'static str {
        match self {
            Self::Follower => "Follower",
            Self::Candidate => "Candidate",
            Self::Leader => "Leader",
        }
    }

    fn metric_value(self) -> i64 {
        match self {
            Self::Follower => 0,
            Self::Candidate => 1,
            Self::Leader => 2,
        }
    }
}

#[derive(Clone)]
pub struct RaftRuntime {
    state: CoordinatorState,
    store: RaftStore,
    grpc_addr: String,
    op_lock: Arc<Mutex<()>>,
    role: Arc<RwLock<NodeRole>>,
    leader_hint: Arc<RwLock<Option<String>>>,
    last_leader_contact: Arc<Mutex<Instant>>,
    election_deadline: Arc<Mutex<Instant>>,
    last_quorum_contact: Arc<Mutex<Instant>>,
    joint_consensus: Arc<RwLock<Option<JointConsensusState>>>,
    /// 插件化模块注册表：namespace -> ReplicatedModule
    modules: Arc<RwLock<HashMap<String, Arc<dyn ReplicatedModule>>>>,
    /// 命令应用结果等待通道：log_index -> oneshot sender
    ///
    /// 由 `propose_business_command_for_result` 写入，由 `apply_entry` 读取并通知。
    /// 仅 leader propose 路径登记 waiter，follower apply 时此表为空。
    #[allow(clippy::type_complexity)] // one-shot waiter map — introduced in T-P0-03
    pending_results:
        Arc<std::sync::Mutex<HashMap<u64, tokio::sync::oneshot::Sender<Result<Vec<u8>, String>>>>>,
}

impl RaftRuntime {
    pub fn new(state: CoordinatorState, store: RaftStore, grpc_addr: String) -> Self {
        let now = Instant::now();
        let election_deadline = now + random_election_timeout();
        Self {
            state,
            store,
            grpc_addr,
            op_lock: Arc::new(Mutex::new(())),
            role: Arc::new(RwLock::new(NodeRole::Follower)),
            leader_hint: Arc::new(RwLock::new(None)),
            last_leader_contact: Arc::new(Mutex::new(now)),
            election_deadline: Arc::new(Mutex::new(election_deadline)),
            last_quorum_contact: Arc::new(Mutex::new(now)),
            joint_consensus: Arc::new(RwLock::new(None)),
            modules: Arc::new(RwLock::new(HashMap::new())),
            pending_results: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    /// 注册可复制模块
    ///
    /// # 参数
    ///
    /// - `module`: 实现了 ReplicatedModule trait 的模块实例
    ///
    /// # 注意
    ///
    /// 必须在 Raft 启动前完成所有模块的注册。
    /// 如果 namespace 重复，后注册的模块会覆盖先前的。
    pub async fn register_module(&self, module: Arc<dyn ReplicatedModule>) {
        let namespace = module.namespace().to_string();
        let mut modules = self.modules.write().await;
        modules.insert(namespace.clone(), module);
        info!(namespace = %namespace, "registered replicated module");
    }

    pub async fn initialize_local_member(&self) {
        {
            let mut members = self.state.members().write().await;
            members.insert(self.state.runtime().node_id.clone(), self.grpc_addr.clone());
        }

        if let Err(err) = self.bootstrap_local_member_entry().await {
            warn!(error = %err, "failed to bootstrap local raft membership entry");
        }

        self.sync_role_from_membership().await;
        self.refresh_role_metric().await;
    }

    pub async fn role_label(&self) -> String {
        self.current_role().await.label().to_string()
    }

    /// Returns the current Raft commit index from the persistent store.
    ///
    /// A value of 0 means the node has never committed any log entry (election
    /// not yet completed or cluster not bootstrapped). Used by `/readyz`.
    pub fn current_commit_index(&self) -> u64 {
        self.store
            .load_metadata()
            .map(|m| m.commit_index)
            .unwrap_or(0)
    }

    /// Snapshot of the current Raft membership map (`node_id -> grpc_addr`).
    /// Used by the cluster auto-join task to determine which peers still need to be added.
    pub async fn snapshot_members(&self) -> std::collections::HashMap<String, String> {
        self.state.members().read().await.clone()
    }

    pub async fn replay_committed_entries_on_startup(&self) -> Result<(), String> {
        self.apply_committed_entries().await
    }

    pub async fn tick(&self) {
        if self.is_leader().await {
            let has_quorum = self.broadcast_committed_logs().await;
            if has_quorum {
                self.mark_quorum_contact_now().await;
            } else if self.leader_quorum_lost_timed_out().await {
                warn!(
                    node_id = %self.state.runtime().node_id,
                    "leader lost quorum for too long; stepping down to follower"
                );
                self.become_follower(None).await;
                self.reset_election_deadline().await;
            }
            self.mark_leader_contact_now().await;
            self.refresh_role_metric().await;
            return;
        }

        if self.leader_timed_out().await {
            if let Err(err) = self.start_election().await {
                warn!(error = %err, "raft tick election attempt failed");
            }
        } else {
            self.refresh_role_metric().await;
        }
    }

    #[cfg(test)]
    async fn expire_leader_contact_for_test(&self) {
        {
            let mut last_seen = self.last_leader_contact.lock().await;
            *last_seen = Instant::now() - ELECTION_TIMEOUT_BASE - ELECTION_TIMEOUT_JITTER_MAX;
        }

        let mut deadline = self.election_deadline.lock().await;
        *deadline = Instant::now() - Duration::from_millis(1);
    }

    #[cfg(test)]
    async fn expire_quorum_contact_for_test(&self) {
        let mut last_quorum = self.last_quorum_contact.lock().await;
        *last_quorum = Instant::now() - LEADER_QUORUM_LOSS_TIMEOUT - Duration::from_millis(1);
    }

    fn load_last_log_term(&self, last_log_index: u64) -> Result<u64, String> {
        if last_log_index == 0 {
            return Ok(0);
        }

        self.store
            .read_log_entry(last_log_index)
            .map_err(|err| format!("failed to read last log entry: {err}"))
            .map(|entry| entry.map(|value| value.term).unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use coord_core::config::ConfigEntry;
    use coord_core::lock::{AcquireOutcome, LockManager};
    use coord_core::state::{CoordinatorState, RuntimeConfig};
    use coord_proto::coord::v1::{
        RaftAppendEntriesRequest, RaftPreVoteRequest, RaftRequestVoteRequest,
    };
    use uuid::Uuid;

    use super::RaftRuntime;
    use crate::persistence::{BackupConsistencyMeta, BackupPayloadV5, MemberItem};
    use crate::raft_store::RaftStore;

    fn unique_temp_dir(tag: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "coord-raft-runtime-{tag}-{}",
            Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    fn test_state(node_id: &str, data_dir: PathBuf) -> CoordinatorState {
        CoordinatorState::new(RuntimeConfig {
            node_id: node_id.to_string(),
            data_dir,
            dev_mode: true,
        })
        .expect("create coordinator state")
    }

    #[tokio::test]
    async fn member_add_commits_on_single_node() {
        let data_dir = unique_temp_dir("single-node-add");
        let state = test_state("node-a", data_dir.clone());
        let store =
            RaftStore::open(&data_dir, "node-a", "127.0.0.1:9800").expect("open raft store");
        let runtime = RaftRuntime::new(state.clone(), store, "127.0.0.1:9800".to_string());

        runtime.initialize_local_member().await;
        let (added, members) = runtime
            .propose_member_add("node-b".to_string(), "127.0.0.1:9801".to_string())
            .await
            .expect("member add should succeed");

        assert!(added);
        assert!(members.iter().any(|member| member == "node-a"));
        assert!(members.iter().any(|member| member == "node-b"));
    }

    #[tokio::test]
    async fn member_remove_commits_on_single_node() {
        let data_dir = unique_temp_dir("single-node-remove");
        let state = test_state("node-a", data_dir.clone());
        let store =
            RaftStore::open(&data_dir, "node-a", "127.0.0.1:9810").expect("open raft store");
        let runtime = RaftRuntime::new(state.clone(), store, "127.0.0.1:9810".to_string());

        runtime.initialize_local_member().await;
        runtime
            .propose_member_add("node-b".to_string(), "127.0.0.1:9811".to_string())
            .await
            .expect("member add should succeed");
        runtime.expire_quorum_contact_for_test().await;

        let (removed, members) = runtime
            .propose_member_remove("node-b".to_string(), true)
            .await
            .expect("member remove should succeed");

        assert!(removed);
        assert!(members.iter().any(|member| member == "node-a"));
        assert!(!members.iter().any(|member| member == "node-b"));
    }

    #[tokio::test]
    async fn member_remove_unreachable_without_force_fails() {
        let data_dir = unique_temp_dir("single-node-remove-no-force");
        let state = test_state("node-a", data_dir.clone());
        let store =
            RaftStore::open(&data_dir, "node-a", "127.0.0.1:9815").expect("open raft store");
        let runtime = RaftRuntime::new(state.clone(), store, "127.0.0.1:9815".to_string());

        runtime.initialize_local_member().await;
        runtime
            .propose_member_add("node-b".to_string(), "127.0.0.1:9816".to_string())
            .await
            .expect("member add should succeed");

        let err = runtime
            .propose_member_remove("node-b".to_string(), false)
            .await
            .expect_err("member remove should fail without force for unreachable peer");
        assert!(err.contains("force-unreachable"));
    }

    #[tokio::test]
    async fn append_entries_sets_follower_role_and_leader_hint() {
        let data_dir = unique_temp_dir("append-sets-follower");
        let state = test_state("node-b", data_dir.clone());
        let store =
            RaftStore::open(&data_dir, "node-b", "127.0.0.1:9820").expect("open raft store");
        let runtime = RaftRuntime::new(state.clone(), store, "127.0.0.1:9820".to_string());

        runtime.initialize_local_member().await;
        runtime
            .handle_append_entries(RaftAppendEntriesRequest {
                leader_id: "node-a".to_string(),
                term: 2,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Vec::new(),
                leader_commit: 0,
                leader_addr: "127.0.0.1:9821".to_string(),
            })
            .await
            .expect("append entries should succeed");

        assert_eq!(runtime.role_label().await, "Follower");
        let err = runtime
            .propose_member_add("node-c".to_string(), "127.0.0.1:9822".to_string())
            .await
            .expect_err("follower should reject member add proposal");
        assert!(err.contains("node-a") || err.contains("unknown"));
    }

    #[tokio::test]
    async fn election_without_quorum_falls_back_to_follower() {
        let data_dir = unique_temp_dir("election-no-quorum");
        let state = test_state("node-b", data_dir.clone());
        let store =
            RaftStore::open(&data_dir, "node-b", "127.0.0.1:9830").expect("open raft store");
        let runtime = RaftRuntime::new(state.clone(), store, "127.0.0.1:9830".to_string());

        runtime.initialize_local_member().await;
        runtime
            .handle_append_entries(RaftAppendEntriesRequest {
                leader_id: "node-a".to_string(),
                term: 3,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Vec::new(),
                leader_commit: 0,
                leader_addr: "127.0.0.1:1".to_string(),
            })
            .await
            .expect("append entries should succeed");

        runtime.expire_leader_contact_for_test().await;
        runtime.tick().await;

        assert_eq!(runtime.role_label().await, "Follower");
    }

    #[tokio::test]
    async fn granted_vote_resets_election_timeout() {
        let data_dir = unique_temp_dir("vote-resets-timeout");
        let state = test_state("node-b", data_dir.clone());
        let store =
            RaftStore::open(&data_dir, "node-b", "127.0.0.1:9840").expect("open raft store");
        let runtime = RaftRuntime::new(state, store, "127.0.0.1:9840".to_string());

        runtime.initialize_local_member().await;
        runtime.expire_leader_contact_for_test().await;
        assert!(runtime.leader_timed_out().await);

        let response = runtime
            .handle_request_vote(RaftRequestVoteRequest {
                candidate_id: "node-a".to_string(),
                term: 4,
                last_log_index: 1,
                last_log_term: 1,
            })
            .await
            .expect("request vote should succeed");

        assert!(response.vote_granted);
        assert!(!runtime.leader_timed_out().await);
    }

    #[tokio::test]
    async fn pre_vote_rejects_with_active_leader_lease() {
        let data_dir = unique_temp_dir("pre-vote-active-lease");
        let state = test_state("node-b", data_dir.clone());
        let store =
            RaftStore::open(&data_dir, "node-b", "127.0.0.1:9850").expect("open raft store");
        let runtime = RaftRuntime::new(state, store, "127.0.0.1:9850".to_string());

        runtime.initialize_local_member().await;
        runtime
            .handle_append_entries(RaftAppendEntriesRequest {
                leader_id: "node-a".to_string(),
                term: 2,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Vec::new(),
                leader_commit: 0,
                leader_addr: "127.0.0.1:9851".to_string(),
            })
            .await
            .expect("append entries should succeed");

        let response = runtime
            .handle_pre_vote(RaftPreVoteRequest {
                candidate_id: "node-c".to_string(),
                term: 3,
                last_log_index: 1,
                last_log_term: 1,
            })
            .await
            .expect("pre-vote should return response");

        assert!(!response.vote_granted);
        assert!(response.message.contains("leader lease"));
    }

    #[tokio::test]
    async fn pre_vote_grants_after_leader_timeout() {
        let data_dir = unique_temp_dir("pre-vote-timeout");
        let state = test_state("node-b", data_dir.clone());
        let store =
            RaftStore::open(&data_dir, "node-b", "127.0.0.1:9860").expect("open raft store");
        let runtime = RaftRuntime::new(state, store, "127.0.0.1:9860".to_string());

        runtime.initialize_local_member().await;
        runtime.expire_leader_contact_for_test().await;

        let response = runtime
            .handle_pre_vote(RaftPreVoteRequest {
                candidate_id: "node-c".to_string(),
                term: 3,
                last_log_index: 1,
                last_log_term: 1,
            })
            .await
            .expect("pre-vote should return response");

        assert!(response.vote_granted);
    }

    #[tokio::test]
    async fn leader_steps_down_after_quorum_loss_timeout() {
        let data_dir = unique_temp_dir("leader-step-down");
        let state = test_state("node-a", data_dir.clone());
        let store =
            RaftStore::open(&data_dir, "node-a", "127.0.0.1:9870").expect("open raft store");
        let runtime = RaftRuntime::new(state.clone(), store, "127.0.0.1:9870".to_string());

        runtime.initialize_local_member().await;
        {
            let mut members = state.members().write().await;
            members.insert("node-b".to_string(), "127.0.0.1:1".to_string());
        }

        runtime.expire_quorum_contact_for_test().await;
        runtime.tick().await;

        assert_eq!(runtime.role_label().await, "Follower");
    }

    #[tokio::test]
    async fn backup_restore_replays_through_raft_log_and_persists_v5_snapshot() {
        let data_dir = unique_temp_dir("backup-raft-replay");
        let state = test_state("node-a", data_dir.clone());
        let store =
            RaftStore::open(&data_dir, "node-a", "127.0.0.1:9880").expect("open raft store");
        let runtime = RaftRuntime::new(state.clone(), store.clone(), "127.0.0.1:9880".to_string());

        runtime.initialize_local_member().await;

        let mut modules = std::collections::HashMap::new();
        modules.insert(
            "members".to_string(),
            serde_json::to_vec(&vec![MemberItem {
                node_id: "node-a".to_string(),
                address: "127.0.0.1:9880".to_string(),
            }])
            .expect("serialize members"),
        );
        modules.insert(
            "registry".to_string(),
            serde_json::to_vec(&Vec::<coord_core::registry::RegistrationSnapshot>::new())
                .expect("serialize"),
        );
        modules.insert(
            "config".to_string(),
            serde_json::to_vec(&vec![ConfigEntry {
                key: "/restore/key".to_string(),
                value: "restored".to_string(),
                version: 1,
            }])
            .expect("serialize configs"),
        );
        modules.insert(
            "lock".to_string(),
            serde_json::to_vec(&Vec::<coord_core::lock::LockStateSnapshot>::new())
                .expect("serialize"),
        );

        let payload = BackupPayloadV5 {
            version: 5,
            created_unix_ms: 100,
            modules,
            consistency: BackupConsistencyMeta::default(),
        };

        let payload_json =
            crate::persistence::payload_to_json_v5(&payload).expect("serialize payload json");
        let message = runtime
            .propose_backup_restore(payload_json)
            .await
            .expect("raft backup restore should succeed");
        assert!(message.contains("raft_log_replay"));

        let restored = state
            .config()
            .get("/restore/key")
            .await
            .expect("restored config should exist");
        assert_eq!(restored.value, "restored");

        let snapshot = store
            .load_runtime_snapshot()
            .expect("load runtime snapshot")
            .expect("snapshot should exist");
        assert_eq!(snapshot.version, 5);
        assert_eq!(snapshot.consistency.replay_strategy, "raft_log_replay");
    }

    #[tokio::test]
    async fn propose_put_config_applies_to_state_on_single_node() {
        let data_dir = unique_temp_dir("put-config-single");
        let state = test_state("node-a", data_dir.clone());
        let store =
            RaftStore::open(&data_dir, "node-a", "127.0.0.1:9890").expect("open raft store");
        let runtime = RaftRuntime::new(state.clone(), store.clone(), "127.0.0.1:9890".to_string());
        runtime.initialize_local_member().await;
        runtime.register_module(state.config().clone()).await;

        let entry = runtime
            .propose_put_config("/app/env".to_string(), "production".to_string())
            .await
            .expect("put_config should succeed on single leader node");

        assert_eq!(entry.key, "/app/env");
        assert_eq!(entry.value, "production");

        // State reflects the applied config
        let stored = state
            .config()
            .get("/app/env")
            .await
            .expect("config key must exist");
        assert_eq!(stored.value, "production");
    }

    #[tokio::test]
    async fn propose_put_config_persists_to_raft_log() {
        let data_dir = unique_temp_dir("put-config-log");
        let state = test_state("node-a", data_dir.clone());
        let store =
            RaftStore::open(&data_dir, "node-a", "127.0.0.1:9891").expect("open raft store");
        let runtime = RaftRuntime::new(state.clone(), store.clone(), "127.0.0.1:9891".to_string());
        runtime.initialize_local_member().await;
        runtime.register_module(state.config().clone()).await;

        runtime
            .propose_put_config("/cfg/feature".to_string(), "enabled".to_string())
            .await
            .expect("put_config should succeed");

        let meta = store.load_metadata().expect("load metadata");
        // The put_config entry (plus bootstrap member add) should be in the log
        assert!(
            meta.last_log_index >= 2,
            "should have at least 2 log entries"
        );
        assert_eq!(
            meta.last_applied_index, meta.commit_index,
            "all entries applied"
        );
    }

    #[tokio::test]
    async fn propose_put_config_increments_version_on_overwrite() {
        let data_dir = unique_temp_dir("put-config-version");
        let state = test_state("node-a", data_dir.clone());
        let store =
            RaftStore::open(&data_dir, "node-a", "127.0.0.1:9892").expect("open raft store");
        let runtime = RaftRuntime::new(state.clone(), store.clone(), "127.0.0.1:9892".to_string());
        runtime.initialize_local_member().await;
        runtime.register_module(state.config().clone()).await;

        runtime
            .propose_put_config("/cfg/count".to_string(), "1".to_string())
            .await
            .expect("first put_config");
        let second = runtime
            .propose_put_config("/cfg/count".to_string(), "2".to_string())
            .await
            .expect("second put_config");

        assert_eq!(second.value, "2");
        assert_eq!(second.version, 2, "version must increment on overwrite");
    }

    #[tokio::test]
    async fn propose_put_config_rejects_empty_key() {
        let data_dir = unique_temp_dir("put-config-empty-key");
        let state = test_state("node-a", data_dir.clone());
        let store =
            RaftStore::open(&data_dir, "node-a", "127.0.0.1:9893").expect("open raft store");
        let runtime = RaftRuntime::new(state.clone(), store, "127.0.0.1:9893".to_string());
        runtime.initialize_local_member().await;

        let err = runtime
            .propose_put_config("".to_string(), "v".to_string())
            .await
            .expect_err("empty key should be rejected");
        assert!(
            err.contains("empty"),
            "error should mention empty key: {err}"
        );
    }

    #[tokio::test]
    async fn propose_put_config_rejected_when_not_leader() {
        let data_dir = unique_temp_dir("put-config-follower");
        let state = test_state("node-b", data_dir.clone());
        let store =
            RaftStore::open(&data_dir, "node-b", "127.0.0.1:9894").expect("open raft store");
        let runtime = RaftRuntime::new(state.clone(), store, "127.0.0.1:9894".to_string());
        runtime.initialize_local_member().await;

        // Simulate a heartbeat from an active leader — node-b becomes Follower
        runtime
            .handle_append_entries(RaftAppendEntriesRequest {
                leader_id: "node-a".to_string(),
                term: 5,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Vec::new(),
                leader_commit: 0,
                leader_addr: "127.0.0.1:9895".to_string(),
            })
            .await
            .expect("handle append entries");

        let err = runtime
            .propose_put_config("/k".to_string(), "v".to_string())
            .await
            .expect_err("follower should reject put_config proposal");
        assert!(
            err.contains("node-a") || err.contains("leader") || err.contains("unknown"),
            "error should mention leader redirect: {err}"
        );
    }

    #[tokio::test]
    async fn config_write_survives_failover_and_old_leader_rejects_new_write() {
        let data_dir = unique_temp_dir("failover-config");
        let state = test_state("node-a", data_dir.clone());
        let store =
            RaftStore::open(&data_dir, "node-a", "127.0.0.1:9896").expect("open raft store");
        let runtime = RaftRuntime::new(state.clone(), store, "127.0.0.1:9896".to_string());

        runtime.initialize_local_member().await;
        runtime.register_module(state.config().clone()).await;

        runtime
            .propose_put_config("/failover/config".to_string(), "v1".to_string())
            .await
            .expect("leader write should succeed before failover");

        runtime
            .handle_append_entries(RaftAppendEntriesRequest {
                leader_id: "node-b".to_string(),
                term: 10,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Vec::new(),
                leader_commit: runtime.current_commit_index(),
                leader_addr: "127.0.0.1:9991".to_string(),
            })
            .await
            .expect("heartbeat from new leader should be accepted");

        let err = runtime
            .propose_put_config("/failover/config".to_string(), "v2".to_string())
            .await
            .expect_err("old leader must reject writes after failover");
        assert!(err.contains("node-b") || err.contains("leader") || err.contains("unknown"));

        let stored = state
            .config()
            .get("/failover/config")
            .await
            .expect("committed pre-failover value must remain readable");
        assert_eq!(stored.value, "v1");
    }

    #[tokio::test]
    async fn lock_write_survives_failover_and_old_leader_rejects_new_write() {
        let data_dir = unique_temp_dir("failover-lock");
        let state = test_state("node-a", data_dir.clone());
        let store =
            RaftStore::open(&data_dir, "node-a", "127.0.0.1:9897").expect("open raft store");
        let runtime = RaftRuntime::new(state.clone(), store, "127.0.0.1:9897".to_string());

        runtime.initialize_local_member().await;
        runtime.register_module(state.locks().clone()).await;

        let acquire = runtime
            .propose_business_command_for_result(
                "lock",
                LockManager::encode_acquire_bytes(
                    "lock-failover",
                    "owner-a",
                    false,
                    "token-a",
                    4_102_444_800_000,
                ),
            )
            .await
            .expect("leader lock acquire should succeed before failover");
        let outcome: AcquireOutcome =
            serde_json::from_slice(&acquire).expect("decode lock acquire result");
        assert!(matches!(outcome, AcquireOutcome::Acquired { .. }));

        runtime
            .handle_append_entries(RaftAppendEntriesRequest {
                leader_id: "node-b".to_string(),
                term: 11,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Vec::new(),
                leader_commit: runtime.current_commit_index(),
                leader_addr: "127.0.0.1:9992".to_string(),
            })
            .await
            .expect("heartbeat from new leader should be accepted");

        let err = runtime
            .propose_business_command_for_result(
                "lock",
                LockManager::encode_acquire_bytes(
                    "lock-failover",
                    "owner-b",
                    false,
                    "token-b",
                    4_102_444_800_001,
                ),
            )
            .await
            .expect_err("old leader must reject lock writes after failover");
        assert!(err.contains("node-b") || err.contains("leader") || err.contains("unknown"));

        let holders = state.locks().list_holders().await;
        assert_eq!(
            holders.len(),
            1,
            "only the committed pre-failover holder should remain"
        );
        assert_eq!(holders[0].lock_name, "lock-failover");
        assert_eq!(holders[0].owner, "owner-a");
    }

    // ── T-P2-03: member change + business write concurrency ───────────────

    #[tokio::test]
    async fn member_add_interleaved_with_config_write() {
        let data_dir = unique_temp_dir("member-config-interleave");
        let state = test_state("node-a", data_dir.clone());
        let store =
            RaftStore::open(&data_dir, "node-a", "127.0.0.1:9900").expect("open raft store");
        let runtime = RaftRuntime::new(state.clone(), store, "127.0.0.1:9900".to_string());
        runtime.initialize_local_member().await;
        runtime.register_module(state.config().clone()).await;

        // Write config while still single-node (quorum=1), then add member
        let config_result = runtime
            .propose_put_config("/concurrent/key".to_string(), "value".to_string())
            .await;
        let entry = config_result.expect("config write should succeed");
        assert_eq!(entry.key, "/concurrent/key");
        assert_eq!(entry.value, "value");

        let (added, members) = runtime
            .propose_member_add("node-c".to_string(), "127.0.0.1:9901".to_string())
            .await
            .expect("member add should succeed");
        assert!(added);
        assert!(members.iter().any(|m| m == "node-c"));

        let stored = state
            .config()
            .get("/concurrent/key")
            .await
            .expect("config must exist");
        assert_eq!(stored.value, "value");
    }

    #[tokio::test]
    async fn member_add_interleaved_with_lock_acquire() {
        let data_dir = unique_temp_dir("member-lock-interleave");
        let state = test_state("node-a", data_dir.clone());
        let store =
            RaftStore::open(&data_dir, "node-a", "127.0.0.1:9902").expect("open raft store");
        let runtime = RaftRuntime::new(state.clone(), store, "127.0.0.1:9902".to_string());
        runtime.initialize_local_member().await;
        runtime.register_module(state.locks().clone()).await;

        // Acquire lock while single-node, then add member
        let acquire_bytes = runtime
            .propose_business_command_for_result(
                "lock",
                LockManager::encode_acquire_bytes(
                    "concurrent-lock",
                    "owner-x",
                    false,
                    "token-x",
                    4_102_444_800_000,
                ),
            )
            .await
            .expect("lock acquire should succeed");
        let outcome: AcquireOutcome =
            serde_json::from_slice(&acquire_bytes).expect("decode lock result");
        assert!(matches!(outcome, AcquireOutcome::Acquired { .. }));

        let (added, _) = runtime
            .propose_member_add("node-d".to_string(), "127.0.0.1:9903".to_string())
            .await
            .expect("member add should succeed");
        assert!(added);
    }

    #[tokio::test]
    async fn member_remove_does_not_break_subsequent_config_write() {
        let data_dir = unique_temp_dir("member-remove-then-config");
        let state = test_state("node-a", data_dir.clone());
        let store =
            RaftStore::open(&data_dir, "node-a", "127.0.0.1:9906").expect("open raft store");
        let runtime = RaftRuntime::new(state.clone(), store, "127.0.0.1:9906".to_string());
        runtime.initialize_local_member().await;
        runtime.register_module(state.config().clone()).await;

        runtime
            .propose_member_add("node-f".to_string(), "127.0.0.1:9907".to_string())
            .await
            .expect("add node-f");
        runtime.expire_quorum_contact_for_test().await;
        runtime
            .propose_member_remove("node-f".to_string(), true)
            .await
            .expect("remove node-f");

        let entry = runtime
            .propose_put_config("/after-remove".to_string(), "ok".to_string())
            .await
            .expect("config write after member remove should succeed");
        assert_eq!(entry.value, "ok");
    }

    #[tokio::test]
    async fn snapshot_during_member_change_captures_new_member() {
        let data_dir = unique_temp_dir("snapshot-member-change");
        let state = test_state("node-a", data_dir.clone());
        let store =
            RaftStore::open(&data_dir, "node-a", "127.0.0.1:9908").expect("open raft store");
        let runtime = RaftRuntime::new(state.clone(), store.clone(), "127.0.0.1:9908".to_string());
        runtime.initialize_local_member().await;
        runtime.register_module(state.config().clone()).await;

        // Write config while single-node, then add member, then snapshot
        runtime
            .propose_put_config("/snap/key".to_string(), "snap-val".to_string())
            .await
            .expect("config write");

        runtime
            .propose_member_add("node-g".to_string(), "127.0.0.1:9909".to_string())
            .await
            .expect("add node-g");

        let backup = runtime
            .snapshot_backup_payload()
            .await
            .expect("snapshot backup should succeed");
        let members_bytes = backup
            .modules
            .get("members")
            .expect("members module in snapshot");
        let members: Vec<crate::persistence::MemberItem> =
            serde_json::from_slice(members_bytes).expect("parse members");
        assert!(members.iter().any(|m| m.node_id == "node-g"));
    }
}
