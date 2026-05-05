//! Integration tests: single-node Raft membership bootstrap invariants.
//!
//! These tests exercise `CoordinatorState` initialisation and the membership
//! contract that the Raft runtime relies on at startup.  The full election
//! state-machine (vote/append-entries RPC handling, leader promotion, quorum
//! loss step-down) is covered by the unit tests inside
//! `src/raft_runtime.rs`.

use std::path::PathBuf;

use coord_core::state::{CoordinatorState, RuntimeConfig};

// ─── helpers ─────────────────────────────────────────────────────────────────

fn unique_temp_dir(tag: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "coord-it-election-{tag}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&path).expect("create temp dir");
    path
}

fn fresh_state(node_id: &str) -> CoordinatorState {
    CoordinatorState::new(RuntimeConfig {
        node_id: node_id.to_string(),
        data_dir: unique_temp_dir(node_id),
        dev_mode: true,
    })
    .expect("create coordinator state")
}

// ─── tests ───────────────────────────────────────────────────────────────────

/// A freshly-created state has exactly one member (the local node itself).
/// Raft bootstrap adds peers; the local node is always present from startup.
#[tokio::test]
async fn fresh_state_has_no_members() {
    let state = fresh_state("node-a");
    let members = state.members().read().await;
    assert_eq!(
        members.len(),
        1,
        "only the local node should be present before peer bootstrap"
    );
    assert!(
        members.contains_key("node-a"),
        "local node should be in members map"
    );
}

/// Node-id is preserved and accessible through the runtime config.
#[tokio::test]
async fn node_id_matches_runtime_config() {
    let state = fresh_state("test-node-election-1");
    assert_eq!(state.runtime().node_id, "test-node-election-1");
}

/// Config center starts empty; the Raft log will populate it after replay.
#[tokio::test]
async fn config_center_is_empty_on_startup() {
    let state = fresh_state("node-config-empty");
    assert!(state.config().snapshot().await.is_empty());
}

/// Lock manager starts with no active holders.
#[tokio::test]
async fn lock_manager_starts_empty() {
    let state = fresh_state("node-lock-empty");
    assert!(state.locks().list_holders().await.is_empty());
}

/// Service registry starts with no instances.
#[tokio::test]
async fn registry_starts_empty() {
    let state = fresh_state("node-registry-empty");
    assert_eq!(state.registry().service_count().await, 0);
}
