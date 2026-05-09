use anyhow::Context;
use coord_core::clock::{Clock, SystemClock};
use coord_core::config::ConfigEntry;
use coord_core::lock::LockStateSnapshot;
use coord_core::pki::PkiStateSnapshot;
use coord_core::registry::RegistrationSnapshot;
use coord_core::security::{EncryptedSecurityDomainBlob, SecurityPersistenceSnapshot};
use coord_core::state::CoordinatorState;
use coord_core::transit::TransitKeySnapshot;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Current canonical backup format version written by this binary.
pub const BACKUP_PAYLOAD_VERSION: u32 = 5;

fn default_replay_strategy() -> String {
    "raft_log_replay".to_string()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BackupConsistencyMeta {
    #[serde(default = "default_replay_strategy")]
    pub replay_strategy: String,
    #[serde(default)]
    pub upgraded_from_version: Option<u32>,
    #[serde(default)]
    pub raft_commit_index: Option<u64>,
    #[serde(default)]
    pub raft_last_applied_index: Option<u64>,
}

impl Default for BackupConsistencyMeta {
    fn default() -> Self {
        Self {
            replay_strategy: default_replay_strategy(),
            upgraded_from_version: None,
            raft_commit_index: None,
            raft_last_applied_index: None,
        }
    }
}

/// 命名空间化的备份格式（v5）
///
/// 使用 HashMap<namespace, bytes> 替代固定字段，
/// 允许模块独立管理自己的快照格式。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BackupPayloadV5 {
    pub version: u32,
    pub created_unix_ms: i64,
    /// 模块快照：namespace -> 序列化的快照数据
    ///
    /// 标准命名空间：
    /// - "members": 集群成员列表 (Vec<MemberItem>)
    /// - "registry": 服务注册数据 (Vec<RegistrationSnapshot>)
    /// - "config": 配置数据 (Vec<ConfigEntry>)
    /// - "lock": 锁状态 (Vec<LockStateSnapshot>)
    /// - "transit": Transit 密钥 (Vec<TransitKeySnapshot>)
    /// - "pki": PKI 状态 (Option<PkiStateSnapshot>)
    /// - "security": Security 状态 (SecurityPersistenceSnapshot + EncryptedSecurityDomainBlob)
    pub modules: HashMap<String, Vec<u8>>,
    #[serde(default)]
    pub consistency: BackupConsistencyMeta,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MemberItem {
    pub node_id: String,
    pub address: String,
}

/// Collects a consistent snapshot in v5 format.
pub async fn snapshot_payload_v5(state: &CoordinatorState) -> anyhow::Result<BackupPayloadV5> {
    let _barrier = state.snapshot_barrier().lock().await;

    let mut members: Vec<MemberItem> = state
        .members()
        .read()
        .await
        .iter()
        .map(|(node_id, address)| MemberItem {
            node_id: node_id.clone(),
            address: address.clone(),
        })
        .collect();
    members.sort_by(|a, b| a.node_id.cmp(&b.node_id));

    let mut modules: HashMap<String, Vec<u8>> = HashMap::new();
    modules.insert("members".to_string(), serde_json::to_vec(&members)?);
    modules.insert(
        "registry".to_string(),
        serde_json::to_vec(&state.registry().snapshot().await)?,
    );
    modules.insert(
        "config".to_string(),
        serde_json::to_vec(&state.config().snapshot().await)?,
    );
    modules.insert(
        "lock".to_string(),
        serde_json::to_vec(&state.locks().snapshot().await)?,
    );

    let security_status = state.security().seal_status().await;
    if security_status.initialized {
        if let Some(sec_state) = state.security().persistence_snapshot().await {
            modules.insert(
                "security_state".to_string(),
                serde_json::to_vec(&sec_state)?,
            );
        }
        let domain_blob = if security_status.sealed {
            state.security().cached_domain_blob().await
        } else {
            let domain = state
                .domain_lifecycle()
                .capture(state.security().export_auth_state_snapshot().await)
                .await;
            state
                .security()
                .encrypt_and_cache_domain_snapshot(domain)
                .await
                .ok()
        };
        if let Some(blob) = domain_blob {
            modules.insert("security_domain".to_string(), serde_json::to_vec(&blob)?);
        }
    } else {
        let transit_keys = state.transit().snapshot().await;
        if !transit_keys.is_empty() {
            modules.insert("transit".to_string(), serde_json::to_vec(&transit_keys)?);
        }
        let pki = state.pki().snapshot().await;
        modules.insert("pki".to_string(), serde_json::to_vec(&pki)?);
    }

    Ok(BackupPayloadV5 {
        version: BACKUP_PAYLOAD_VERSION,
        created_unix_ms: SystemClock.now_ms(),
        modules,
        consistency: BackupConsistencyMeta::default(),
    })
}

/// Annotate a v5 payload with Raft commit / applied index metadata.
pub fn annotate_payload_v5_with_raft_metadata(
    payload: &mut BackupPayloadV5,
    commit_index: u64,
    last_applied_index: u64,
) {
    payload.consistency.raft_commit_index = Some(commit_index);
    payload.consistency.raft_last_applied_index = Some(last_applied_index);
}

/// Restore coordinator state from a v5 backup payload.
pub async fn restore_payload_v5(
    state: &CoordinatorState,
    payload: BackupPayloadV5,
) -> anyhow::Result<()> {
    restore_payload_v5_with_policy(state, payload, true).await
}

/// Restore coordinator state from a v5 backup payload (Raft replay mode).
///
/// In Raft replay mode, missing local members are NOT auto-added.
pub async fn restore_payload_v5_for_raft_replay(
    state: &CoordinatorState,
    payload: BackupPayloadV5,
) -> anyhow::Result<()> {
    restore_payload_v5_with_policy(state, payload, false).await
}

async fn restore_payload_v5_with_policy(
    state: &CoordinatorState,
    payload: BackupPayloadV5,
    keep_local_member_when_absent: bool,
) -> anyhow::Result<()> {
    let has_security_domain = payload.modules.contains_key("security_state")
        || payload.modules.contains_key("security_domain");

    // Deserialize modules
    let members: Vec<MemberItem> = payload
        .modules
        .get("members")
        .map(|b| serde_json::from_slice(b))
        .transpose()?
        .unwrap_or_default();
    let registry: Vec<RegistrationSnapshot> = payload
        .modules
        .get("registry")
        .map(|b| serde_json::from_slice(b))
        .transpose()?
        .unwrap_or_default();
    let configs: Vec<ConfigEntry> = payload
        .modules
        .get("config")
        .map(|b| serde_json::from_slice(b))
        .transpose()?
        .unwrap_or_default();
    let locks: Vec<LockStateSnapshot> = payload
        .modules
        .get("lock")
        .map(|b| serde_json::from_slice(b))
        .transpose()?
        .unwrap_or_default();
    let transit_keys: Vec<TransitKeySnapshot> = payload
        .modules
        .get("transit")
        .map(|b| serde_json::from_slice(b))
        .transpose()?
        .unwrap_or_default();
    let pki: Option<PkiStateSnapshot> = payload
        .modules
        .get("pki")
        .map(|b| serde_json::from_slice(b))
        .transpose()?;
    let security_state: Option<SecurityPersistenceSnapshot> = payload
        .modules
        .get("security_state")
        .map(|b| serde_json::from_slice(b))
        .transpose()?;
    let security_domain_encrypted: Option<EncryptedSecurityDomainBlob> = payload
        .modules
        .get("security_domain")
        .map(|b| serde_json::from_slice(b))
        .transpose()?;

    // Capture expected counts for post-condition checks
    let expected_config_count = configs.len();
    let expected_member_count_min = if members.is_empty() { 0 } else { 1 };
    let snapshot_has_security = has_security_domain;

    state.registry().restore(registry).await;
    state.config().restore(configs).await;
    state.locks().restore(locks).await;

    if has_security_domain {
        state
            .security()
            .restore_persistence_state(security_state, security_domain_encrypted)
            .await
            .map_err(anyhow::Error::msg)?;

        let status = state.security().seal_status().await;
        if status.initialized {
            state
                .domain_lifecycle()
                .clear()
                .await
                .map_err(anyhow::Error::msg)?;
            state
                .metrics()
                .coord_security_sealed
                .set(if status.sealed { 1 } else { 0 });
        } else {
            state
                .transit()
                .restore(transit_keys)
                .await
                .map_err(anyhow::Error::msg)?;
            if let Some(pki_snapshot) = pki {
                state
                    .pki()
                    .restore(pki_snapshot)
                    .await
                    .map_err(anyhow::Error::msg)?;
            }
            state.metrics().coord_security_sealed.set(0);
        }
    } else {
        state
            .security()
            .restore_persistence_state(None, None)
            .await
            .map_err(anyhow::Error::msg)?;
        state
            .transit()
            .restore(transit_keys)
            .await
            .map_err(anyhow::Error::msg)?;
        if let Some(pki_snapshot) = pki {
            state
                .pki()
                .restore(pki_snapshot)
                .await
                .map_err(anyhow::Error::msg)?;
        }
        state.metrics().coord_security_sealed.set(0);
    }

    let mut member_map: HashMap<String, String> = members
        .into_iter()
        .map(|item| (item.node_id, item.address))
        .collect();
    if keep_local_member_when_absent {
        member_map
            .entry(state.runtime().node_id.clone())
            .or_insert_with(|| "self".to_string());
    }

    *state.members().write().await = member_map;

    state
        .metrics()
        .coord_services_registered_total
        .set(state.registry().service_count().await as i64);
    state
        .metrics()
        .coord_locks_held
        .set(state.locks().list_holders().await.len() as i64);

    verify_restore_invariants(
        state,
        expected_config_count,
        expected_member_count_min,
        snapshot_has_security,
    )
    .await?;

    Ok(())
}

/// Verify that the state after a restore satisfies minimum invariants.
async fn verify_restore_invariants(
    state: &CoordinatorState,
    expected_config_count: usize,
    expected_member_count_min: usize,
    snapshot_had_security: bool,
) -> anyhow::Result<()> {
    let member_count = state.members().read().await.len();
    if member_count < expected_member_count_min.max(1) {
        return Err(anyhow::anyhow!(
            "restore invariant violation: member list is empty after restore \
             (expected ≥{expected_member_count_min})"
        ));
    }

    let restored_config_count = state.config().snapshot().await.len();
    if restored_config_count != expected_config_count {
        return Err(anyhow::anyhow!(
            "restore invariant violation: config count mismatch after restore \
             (snapshot had {expected_config_count}, got {restored_config_count})"
        ));
    }

    if snapshot_had_security {
        let status = state.security().seal_status().await;
        if !status.initialized {
            return Err(anyhow::anyhow!(
                "restore invariant violation: security domain not initialized after restore \
                 (snapshot contained a security domain)"
            ));
        }
    }

    Ok(())
}

fn normalize_payload_v5(mut payload: BackupPayloadV5) -> BackupPayloadV5 {
    if payload.consistency.replay_strategy.trim().is_empty() {
        payload.consistency.replay_strategy = default_replay_strategy();
    }
    payload
}

/// Serialize a v5 backup payload to pretty JSON.
pub fn payload_to_json_v5(payload: &BackupPayloadV5) -> anyhow::Result<String> {
    serde_json::to_string_pretty(payload).context("failed to serialize v5 backup payload to json")
}

/// Parse a JSON string into a `BackupPayloadV5`.
///
/// Only version 5 payloads are accepted.
pub fn payload_v5_from_json(payload_json: &str) -> anyhow::Result<BackupPayloadV5> {
    #[derive(serde::Deserialize)]
    struct VersionProbe {
        version: u32,
    }
    let probe: VersionProbe = serde_json::from_str(payload_json)
        .context("failed to read version field from snapshot payload")?;

    if probe.version != 5 {
        return Err(anyhow::anyhow!(
            "unsupported snapshot payload version: {} (only version 5 is supported)",
            probe.version
        ));
    }

    let payload: BackupPayloadV5 =
        serde_json::from_str(payload_json).context("failed to parse v5 snapshot payload json")?;
    Ok(normalize_payload_v5(payload))
}

#[cfg(test)]
pub async fn restore_from_json(
    state: &CoordinatorState,
    payload_json: &str,
) -> anyhow::Result<bool> {
    if payload_json.trim().is_empty() {
        return Ok(false);
    }

    let payload = payload_v5_from_json(payload_json)?;
    restore_payload_v5(state, payload).await?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use coord_core::lock::AcquireOutcome;
    use coord_core::registry::ServiceInstance;
    use coord_core::state::RuntimeConfig;
    use uuid::Uuid;

    use super::*;

    fn unique_temp_dir(tag: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!("coord-{tag}-{}", Uuid::new_v4().simple()));
        std::fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    fn test_state(node_id: &str, data_dir: std::path::PathBuf) -> CoordinatorState {
        CoordinatorState::new(RuntimeConfig {
            node_id: node_id.to_string(),
            data_dir,
            dev_mode: true,
        })
        .expect("create coordinator state")
    }

    #[tokio::test]
    async fn persist_and_restore_roundtrip() {
        let data_dir = unique_temp_dir("roundtrip");

        let source = test_state("node-a", data_dir.clone());

        source
            .registry()
            .register(
                ServiceInstance {
                    service_name: "svc-a".to_string(),
                    instance_id: "inst-1".to_string(),
                    host: "127.0.0.1".to_string(),
                    port: 8080,
                    metadata: HashMap::new(),
                },
                60,
            )
            .await;
        source
            .config()
            .put("/app/name".to_string(), "coord".to_string())
            .await;

        let acquire = source
            .locks()
            .acquire("deploy", "worker-a", 60, false)
            .await;
        assert!(matches!(acquire, AcquireOutcome::Acquired { .. }));

        source
            .transit()
            .create_key("orders")
            .await
            .expect("create transit key");
        let (signature, _) = source
            .transit()
            .hmac_sign("orders", b"critical-data")
            .await
            .expect("sign transit payload");

        let issued = source
            .pki()
            .issue_certificate("svc-a.internal", vec!["svc-a.internal".to_string()], 3600)
            .await
            .expect("issue pki certificate");

        {
            let mut members = source.members().write().await;
            members.insert("node-b".to_string(), "10.0.0.2:9090".to_string());
        }

        let payload = snapshot_payload_v5(&source).await.expect("v5 snapshot");
        let json = payload_to_json_v5(&payload).expect("serialise v5");

        let restored = test_state("node-a", data_dir);
        let loaded = restore_from_json(&restored, &json).await.expect("restore");
        assert!(loaded);

        let services = restored.registry().discover("svc-a").await;
        assert_eq!(services.len(), 1);

        let config = restored.config().get("/app/name").await;
        assert_eq!(config.map(|entry| entry.value), Some("coord".to_string()));

        let holders = restored.locks().list_holders().await;
        assert_eq!(holders.len(), 1);

        let verified = restored
            .transit()
            .hmac_verify("orders", b"critical-data", &signature)
            .await
            .expect("verify restored transit key");
        assert!(verified);

        let renewed = restored
            .pki()
            .renew_certificate(&issued.serial_number, 3600)
            .await
            .expect("renew restored certificate record");
        assert_eq!(renewed.common_name, "svc-a.internal");

        let members = restored.members().read().await;
        assert!(members.contains_key("node-a"));
        assert!(members.contains_key("node-b"));
    }

    #[tokio::test]
    async fn restore_payload_keeps_local_member_when_absent() {
        let state = test_state("node-self", unique_temp_dir("members"));

        let mut modules = HashMap::new();
        modules.insert(
            "members".to_string(),
            serde_json::to_vec(&vec![MemberItem {
                node_id: "node-remote".to_string(),
                address: "10.0.0.9:9090".to_string(),
            }])
            .unwrap(),
        );
        modules.insert(
            "registry".to_string(),
            serde_json::to_vec(&Vec::<RegistrationSnapshot>::new()).unwrap(),
        );
        modules.insert(
            "config".to_string(),
            serde_json::to_vec(&Vec::<ConfigEntry>::new()).unwrap(),
        );
        modules.insert(
            "lock".to_string(),
            serde_json::to_vec(&Vec::<LockStateSnapshot>::new()).unwrap(),
        );

        let payload = BackupPayloadV5 {
            version: 5,
            created_unix_ms: SystemClock.now_ms(),
            modules,
            consistency: BackupConsistencyMeta::default(),
        };

        restore_payload_v5(&state, payload)
            .await
            .expect("restore payload should succeed");

        let members = state.members().read().await;
        assert!(members.contains_key("node-self"));
        assert!(members.contains_key("node-remote"));
    }

    #[tokio::test]
    async fn replay_restore_keeps_payload_members_exactly() {
        let state = test_state("node-local", unique_temp_dir("replay-members"));

        let mut modules = HashMap::new();
        modules.insert(
            "members".to_string(),
            serde_json::to_vec(&vec![MemberItem {
                node_id: "node-remote".to_string(),
                address: "10.0.0.9:9090".to_string(),
            }])
            .unwrap(),
        );
        modules.insert(
            "registry".to_string(),
            serde_json::to_vec(&Vec::<RegistrationSnapshot>::new()).unwrap(),
        );
        modules.insert(
            "config".to_string(),
            serde_json::to_vec(&Vec::<ConfigEntry>::new()).unwrap(),
        );
        modules.insert(
            "lock".to_string(),
            serde_json::to_vec(&Vec::<LockStateSnapshot>::new()).unwrap(),
        );

        let payload = BackupPayloadV5 {
            version: 5,
            created_unix_ms: SystemClock.now_ms(),
            modules,
            consistency: BackupConsistencyMeta::default(),
        };

        restore_payload_v5_for_raft_replay(&state, payload)
            .await
            .expect("raft replay restore should succeed");

        let members = state.members().read().await;
        assert!(!members.contains_key("node-local"));
        assert!(members.contains_key("node-remote"));
    }

    #[tokio::test]
    async fn snapshot_barrier_serialises_concurrent_snapshots() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let state = Arc::new(test_state("node-barrier", unique_temp_dir("barrier")));
        let concurrent_counter = Arc::new(AtomicUsize::new(0));
        let overlap_detected = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let state = Arc::clone(&state);
                let counter = Arc::clone(&concurrent_counter);
                let overlap = Arc::clone(&overlap_detected);
                tokio::spawn(async move {
                    let _guard = state.snapshot_barrier().lock().await;
                    let prev = counter.fetch_add(1, Ordering::SeqCst);
                    if prev > 0 {
                        overlap.store(true, Ordering::SeqCst);
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
                    counter.fetch_sub(1, Ordering::SeqCst);
                })
            })
            .collect();

        for h in handles {
            h.await.expect("task should not panic");
        }

        assert!(
            !overlap_detected.load(Ordering::SeqCst),
            "snapshot barrier should serialise concurrent calls"
        );
    }

    #[tokio::test]
    async fn restore_with_security_domain_clears_transit_engine() {
        let state = test_state("node-sec", unique_temp_dir("sec-domain"));

        state
            .transit()
            .create_key("should-be-wiped")
            .await
            .expect("create key");
        assert!(
            state
                .transit()
                .encrypt("should-be-wiped", b"x")
                .await
                .is_ok(),
            "key should be accessible before restore"
        );

        use coord_core::security::SecurityPersistenceSnapshot;
        let security_state = SecurityPersistenceSnapshot {
            initialized: true,
            sealed: true,
            shares_total: 1,
            threshold: 1,
            key_version: 1,
            wrapped_barrier_nonce_b64: "ZZZ=".to_string(),
            wrapped_barrier_ciphertext_b64: "ZZZ=".to_string(),
        };

        let mut modules = HashMap::new();
        modules.insert(
            "members".to_string(),
            serde_json::to_vec(&Vec::<MemberItem>::new()).unwrap(),
        );
        modules.insert(
            "registry".to_string(),
            serde_json::to_vec(&Vec::<RegistrationSnapshot>::new()).unwrap(),
        );
        modules.insert(
            "config".to_string(),
            serde_json::to_vec(&Vec::<ConfigEntry>::new()).unwrap(),
        );
        modules.insert(
            "lock".to_string(),
            serde_json::to_vec(&Vec::<LockStateSnapshot>::new()).unwrap(),
        );
        modules.insert(
            "security_state".to_string(),
            serde_json::to_vec(&security_state).unwrap(),
        );

        let payload = BackupPayloadV5 {
            version: BACKUP_PAYLOAD_VERSION,
            created_unix_ms: SystemClock.now_ms(),
            modules,
            consistency: BackupConsistencyMeta::default(),
        };

        restore_payload_v5(&state, payload).await.expect("restore");

        assert!(
            state
                .transit()
                .encrypt("should-be-wiped", b"x")
                .await
                .is_err(),
            "transit key must be gone after security domain restore"
        );
    }

    #[tokio::test]
    async fn snapshot_payload_v5_produces_version_5() {
        let state = test_state("node-v5", unique_temp_dir("v5-version"));
        let payload = snapshot_payload_v5(&state).await.expect("v5 snapshot");
        assert_eq!(payload.version, 5);
        assert!(payload.modules.contains_key("members"));
    }

    #[tokio::test]
    async fn v5_roundtrip_preserves_member_list() {
        let state = test_state("node-rt", unique_temp_dir("v5-rt"));
        {
            let mut m = state.members().write().await;
            m.insert("node-peer".to_string(), "10.0.0.1:9090".to_string());
        }

        let payload = snapshot_payload_v5(&state).await.expect("v5 snapshot");
        let json = payload_to_json_v5(&payload).expect("serialise v5");

        let restored = payload_v5_from_json(&json).expect("parse v5");
        assert_eq!(restored.version, 5);

        let members_bytes = restored.modules.get("members").expect("members module");
        let members: Vec<MemberItem> =
            serde_json::from_slice(members_bytes).expect("parse members");
        assert!(members.iter().any(|m| m.node_id == "node-peer"));
    }

    #[test]
    fn payload_v5_from_json_rejects_v4() {
        let json = r#"{"version": 4, "created_unix_ms": 0, "members": [], "registry": [], "configs": [], "locks": [], "transit_keys": []}"#;
        let err = payload_v5_from_json(json).expect_err("v4 should be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("unsupported") || msg.contains("version 5"),
            "error should mention version: {msg}"
        );
    }

    #[test]
    fn payload_v5_from_json_rejects_v99() {
        let json = r#"{"version": 99, "created_unix_ms": 0, "modules": {}}"#;
        let err = payload_v5_from_json(json).expect_err("v99 should be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("unsupported") || msg.contains("version"),
            "error should mention version: {msg}"
        );
    }

    #[tokio::test]
    async fn v5_snapshot_roundtrip_consistency_meta_preserved() {
        let state = test_state("node-v5m", unique_temp_dir("v5-meta"));
        state
            .config()
            .put("/meta/k".to_string(), "val".to_string())
            .await;

        let v5 = snapshot_payload_v5(&state).await.expect("v5 snapshot");
        assert_eq!(v5.version, 5);

        let json = payload_to_json_v5(&v5).expect("serialize v5");
        let restored = payload_v5_from_json(&json).expect("parse v5");
        assert_eq!(
            restored.consistency.replay_strategy,
            v5.consistency.replay_strategy
        );
    }

    #[tokio::test]
    async fn seal_backup_restore_unseal_transit_roundtrip() {
        let state = test_state("node-seal-rt", unique_temp_dir("seal-backup-rt"));
        let sec = state.security();
        let transit = state.transit();
        let pki = state.pki();
        let dlm = state.domain_lifecycle();

        let shares = sec.init_security(2, 2).await.expect("init");
        sec.unseal(&shares[0]).await.expect("share 1");
        sec.unseal(&shares[1]).await.expect("share 2");
        if let Some(domain) = sec.take_unsealed_domain_snapshot().await {
            dlm.restore_domain(domain)
                .await
                .expect("restore domain after init unseal");
        }

        transit
            .create_key("backup-key")
            .await
            .expect("create transit key");
        let (sig, _) = transit
            .hmac_sign("backup-key", b"data")
            .await
            .expect("sign");
        let issued = pki
            .issue_certificate("backup.internal", vec![], 3600)
            .await
            .expect("issue cert");

        let auth = sec.export_auth_state_snapshot().await;
        let domain = dlm.capture(auth).await;
        sec.seal_with_domain(domain).await.expect("seal");
        dlm.clear().await.expect("clear after seal");

        let payload = snapshot_payload_v5(&state).await.expect("v5 snapshot");
        assert!(
            payload.modules.contains_key("security_state"),
            "security state must be in backup"
        );
        assert!(
            payload.modules.contains_key("security_domain"),
            "domain blob must be in backup"
        );

        let restored_state = test_state("node-seal-rt", unique_temp_dir("seal-backup-rt-restored"));
        restore_payload_v5(&restored_state, payload)
            .await
            .expect("restore");

        let status = restored_state.security().seal_status().await;
        assert!(status.initialized);
        assert!(status.sealed);
        assert!(
            restored_state
                .transit()
                .hmac_sign("backup-key", b"x")
                .await
                .is_err(),
            "transit key must be inaccessible while sealed after restore"
        );

        restored_state
            .security()
            .unseal(&shares[0])
            .await
            .expect("restored share 1");
        restored_state
            .security()
            .unseal(&shares[1])
            .await
            .expect("restored share 2");
        let domain = restored_state
            .security()
            .take_unsealed_domain_snapshot()
            .await
            .expect("domain after restored unseal");
        restored_state
            .domain_lifecycle()
            .restore_domain(domain)
            .await
            .expect("restore domain after unseal");

        let verified = restored_state
            .transit()
            .hmac_verify("backup-key", b"data", &sig)
            .await
            .expect("verify with restored key");
        assert!(verified, "transit HMAC must verify after restore+unseal");

        let renewed = restored_state
            .pki()
            .renew_certificate(&issued.serial_number, 7200)
            .await
            .expect("renew restored cert");
        assert_eq!(renewed.common_name, "backup.internal");
    }

    #[tokio::test]
    async fn snapshot_payload_captures_transit_key_names() {
        let state = test_state("node-snap", unique_temp_dir("snap-transit"));
        state
            .transit()
            .create_key("my-key")
            .await
            .expect("create transit key");

        let payload = snapshot_payload_v5(&state).await.expect("v5 snapshot");

        assert!(
            !payload.modules.contains_key("security_state"),
            "security not init, no security_state in snapshot"
        );
        let transit_bytes = payload.modules.get("transit").expect("transit module");
        let transit_keys: Vec<TransitKeySnapshot> =
            serde_json::from_slice(transit_bytes).expect("parse transit");
        assert_eq!(transit_keys.len(), 1);
        assert_eq!(transit_keys[0].key_name, "my-key");
    }
}
