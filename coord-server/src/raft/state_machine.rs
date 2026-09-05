// Raft StateMachine — Openraft RaftStateMachine + RaftSnapshotBuilder 实现
//
// P0-A 重建（M0）：applied 状态同事务持久化（D-A4）、revision ≡ log index（D-A2）、
// apply 幂等守卫（D-A3）、快照落盘生命周期（A.6）、Lease 状态表（P0-B 骨架）。

use std::fmt;
use std::io;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;

use coord_core::storage::StorageBackend;
use futures::Stream;
use futures::TryStreamExt;
use openraft::storage::EntryResponder;
use openraft::storage::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use openraft::type_config::alias::{LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf};
use openraft::{EntryPayload, Membership, OptionalSend};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::type_config::{Command, Response, TypeConfig};
use crate::auth::manager::AuthManager;
use crate::auth::revocation::RevocationStore;
use crate::auth::token::TokenManager;
use crate::metrics::Metrics;
use crate::storage::mvcc::{
    AppliedLogId, ChangeEvent, EventType, KeyValueChange, MvccStorage, META_MEMBERSHIP,
    META_SNAPSHOT, TABLE_META,
};
use crate::storage::redb_backend::RedbBackend;
use crate::storage::snapshot::{
    export_snapshot_data, import_snapshot_data, SnapshotData, SnapshotTracker,
};
use crate::watch::WatchDispatcher;

fn io_err(e: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::other(e)
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct StoredSnapshot {
    pub meta: SnapshotMetaOf<TypeConfig>,
    pub data: Vec<u8>,
}

/// 持久化到 `META_SNAPSHOT` 的快照元数据（M0-4：启动时加载 current_snapshot）
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedSnapshotMeta {
    pub meta: SnapshotMetaOf<TypeConfig>,
    pub checksum: [u8; 32],
    pub path: String,
}

/// 计算快照数据字节的 SHA256 校验和（A.6.2）
fn sha256_hex(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// 解析 Raft 快照文件名 `snapshot-{idx}-{term}.snap`，返回 (idx, term)。
///
/// S-RCV-01：scheduler 的 `snapshot-{unix_ts}.snap`（单段数字）不匹配。
/// 此前清理逻辑把目录下所有 `.snap` 混排，时间戳文件名（约 1.7e9）字典序
/// 大于 Raft 的 index 段，被误判为“更新”，导致刚落盘的 Raft 快照被立即删除
/// —— META_SNAPSHOT/purge 守卫随即悬空，重启即不可恢复。
fn parse_raft_snapshot_name_free(name: &str) -> Option<(u64, u64)> {
    let rest = name.strip_prefix("snapshot-")?.strip_suffix(".snap")?;
    let (idx_s, term_s) = rest.split_once('-')?;
    Some((idx_s.parse().ok()?, term_s.parse().ok()?))
}

/// 清理快照目录，仅保留最新 3 份 Raft 快照（A.6.3；纯函数，可在 spawn_blocking 内执行）
fn cleanup_old_snapshots_free(
    snapshot_dir: &PathBuf,
    keep: &PathBuf,
) -> Result<(), io::Error> {
    let mut snaps: Vec<(u64, u64, PathBuf)> = match std::fs::read_dir(snapshot_dir) {
        Ok(entries) => entries
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name();
                let (idx, term) = parse_raft_snapshot_name_free(name.to_str()?)?;
                Some((idx, term, e.path()))
            })
            .collect(),
        Err(_) => return Ok(()),
    };
    // 按 index 降序（同 index 按 term 降序），保留最新 3 份
    snaps.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));
    for (_, _, path) in snaps.iter().skip(3) {
        if path == keep {
            continue; // 双保险：绝不删除刚写入的快照
        }
        if let Err(e) = std::fs::remove_file(path) {
            tracing::warn!("Failed to remove old snapshot {}: {e}", path.display());
        }
    }
    Ok(())
}

/// A.6.2 落盘纯函数：临时文件 → fsync → 原子 rename → 目录 fsync → SHA256 →
/// `META_SNAPSHOT` 写事务 → purge 守卫登记 → 旧快照清理。
///
/// Phase 1 T1.3：不触碰 `StateMachineStore` 内部锁（只经传入的 `Arc` 句柄访问
/// state_machine / snapshot_tracker），因此可放入 `spawn_blocking` 而无需持有
/// `&mut self`。`snapshot_tracker.record_durable` 在落盘成功后执行，登记时序与
/// 原同步实现完全一致。
#[allow(clippy::too_many_arguments)]
fn persist_snapshot_file_impl(
    snapshot_dir: &PathBuf,
    state_machine: &Arc<MvccStorage<RedbBackend>>,
    snapshot_tracker: &Arc<SnapshotTracker>,
    meta: &SnapshotMetaOf<TypeConfig>,
    data: &[u8],
) -> Result<(PathBuf, [u8; 32]), io::Error> {
    use std::io::Write;

    std::fs::create_dir_all(snapshot_dir).map_err(|e| {
        io::Error::other(format!(
            "create snapshot dir {}: {e}",
            snapshot_dir.display()
        ))
    })?;

    let last_idx = meta.last_log_id.as_ref().map(|l| l.index).unwrap_or(0);
    let last_term = meta
        .last_log_id
        .as_ref()
        .map(|l| l.leader_id.term)
        .unwrap_or(0);
    let final_path = snapshot_dir.join(format!("snapshot-{last_idx}-{last_term}.snap"));
    let tmp_path = snapshot_dir.join(format!(".snapshot-{last_idx}-{last_term}.snap.tmp"));

    // 1. 写临时文件并 fsync
    {
        let mut f = std::fs::File::create(&tmp_path)
            .map_err(|e| io::Error::other(format!("create {}: {e}", tmp_path.display())))?;
        f.write_all(data)
            .map_err(|e| io::Error::other(format!("write {}: {e}", tmp_path.display())))?;
        f.sync_all()
            .map_err(|e| io::Error::other(format!("fsync {}: {e}", tmp_path.display())))?;
    }

    // 2. 原子 rename + 目录 fsync（尽力而为）
    std::fs::rename(&tmp_path, &final_path).map_err(|e| {
        let _ = std::fs::remove_file(&tmp_path);
        io::Error::other(format!(
            "rename {} -> {}: {e}",
            tmp_path.display(),
            final_path.display()
        ))
    })?;
    if let Ok(dir) = std::fs::File::open(snapshot_dir) {
        let _ = dir.sync_all();
    }

    // 3. SHA256 校验和
    let checksum = sha256_hex(data);

    // 4. 持久化 META_SNAPSHOT + 登记 purge 守卫
    let persisted = PersistedSnapshotMeta {
        meta: meta.clone(),
        checksum,
        path: final_path.to_string_lossy().to_string(),
    };
    let persisted_bytes = bincode::serialize(&persisted)
        .map_err(|e| io::Error::other(format!("serialize snapshot meta: {e}")))?;
    state_machine
        .backend()
        .write(|tx| tx.insert(TABLE_META, META_SNAPSHOT, &persisted_bytes))
        .map_err(io_err)?;
    snapshot_tracker.record_durable(last_idx, last_term, final_path.clone());

    // 5. A.6.3：保留最近 3 份 Raft 快照，清理旧份
    //    （只识别 snapshot-{idx}-{term}.snap，绝不删除本次写入的文件）
    cleanup_old_snapshots_free(snapshot_dir, &final_path)?;

    Ok((final_path, checksum))
}

pub struct StateMachineStore {
    pub state_machine: Arc<MvccStorage<RedbBackend>>,
    pub last_applied: Mutex<Option<LogIdOf<TypeConfig>>>,
    pub last_membership: Mutex<StoredMembershipOf<TypeConfig>>,
    current_snapshot: Mutex<Option<StoredSnapshot>>,
    /// 快照落盘目录（A.6：临时文件 → fsync → rename → 校验和）
    snapshot_dir: PathBuf,
    /// purge 前置条件守卫（与 LogStore 共享，M0-5）
    snapshot_tracker: Arc<SnapshotTracker>,
    /// Watch 事件分发器（可选，Leader 节点持有，与 CoordNode 共享同一实例）
    pub watch_dispatcher: Option<Arc<WatchDispatcher>>,
    /// AuthManager 内存缓存视图（P0-C.2：apply 后同步，可选）
    pub auth_manager: Option<Arc<AuthManager>>,
    /// 吊销登记存储（P0-C.5：RevokeJti apply 后同步，可选）
    pub revocation_store: Option<Arc<RevocationStore>>,
    /// 会话表视图（P2-07：IssueSession/ConsumeSession apply 后同步，可选）
    pub session_manager: Option<Arc<TokenManager>>,
    /// 指标注册表（R-OBS-10：apply 延迟 / 快照耗时埋点，可选）
    pub metrics: Option<Arc<Metrics>>,
    /// T5.7（R-MR-04）：Lease Revoke 广播（可选，仅 region 0 状态机设置）。
    ///
    /// region 0 的 `LeaseOp::Revoke` apply（含过期清理）后向通道广播 lease_id，
    /// 各节点据此通知 Region raft leader 经 `Command::DeleteKeysByLease` 清理
    /// 各自 MVCC 中绑定该 Lease 的 Key（详见 `server/mod.rs` 的
    /// `start_region_lease_revoker`）。所有节点（含 follower）都会 apply region 0
    /// 日志并收到广播——因此无论哪个节点最终成为某 Region 的 leader，都能
    /// 看到广播并完成删除（幂等）。
    pub lease_revoke_tx: Option<tokio::sync::mpsc::UnboundedSender<i64>>,
}

// Manual Debug impl since MvccStorage may not be Debug
impl fmt::Debug for StateMachineStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StateMachineStore")
            .field("state_machine", &"MvccStorage<RedbBackend>")
            .field("last_applied", &self.last_applied)
            .field("last_membership", &self.last_membership)
            .finish()
    }
}

impl StateMachineStore {
    /// 创建状态机存储。
    ///
    /// 启动时（A.5）：从 `META_SNAPSHOT` 加载已落盘快照（校验 SHA256），
    /// 并向 `snapshot_tracker` 登记；`last_applied` 优先取快照 meta，否则取
    /// `META_LAST_APPLIED`（AppliedLogId → LogId）。
    pub fn new(
        state_machine: Arc<MvccStorage<RedbBackend>>,
        snapshot_dir: PathBuf,
        snapshot_tracker: Arc<SnapshotTracker>,
    ) -> Self {
        let empty_membership: Membership<u64, openraft::BasicNode> =
            Membership::new_with_defaults(vec![], vec![]);

        let mut last_applied: Option<LogIdOf<TypeConfig>> = None;
        let mut last_membership =
            StoredMembershipOf::<TypeConfig>::new(None, empty_membership.clone());
        let mut current_snapshot: Option<StoredSnapshot> = None;
        // 1. 从 META_SNAPSHOT 加载（A.5 步骤 1）
        let persisted = state_machine
            .backend()
            .read(|tx| tx.get(TABLE_META, META_SNAPSHOT))
            .ok()
            .flatten()
            .and_then(|bytes| bincode::deserialize::<PersistedSnapshotMeta>(&bytes).ok());

        if let Some(ref pmeta) = persisted {
            match std::fs::read(&pmeta.path) {
                Ok(data) => {
                    if sha256_hex(&data) == pmeta.checksum {
                        current_snapshot = Some(StoredSnapshot {
                            meta: pmeta.meta.clone(),
                            data,
                        });
                        last_applied = pmeta.meta.last_log_id;
                        last_membership = pmeta.meta.last_membership.clone();
                        let idx = last_applied.as_ref().map(|l| l.index).unwrap_or(0);
                        let term = last_applied.as_ref().map(|l| l.leader_id.term).unwrap_or(0);
                        snapshot_tracker.record_durable(idx, term, PathBuf::from(&pmeta.path));
                        tracing::info!(
                            "Loaded persisted snapshot: {} (last_log_id={:?})",
                            pmeta.path,
                            pmeta.meta.last_log_id
                        );
                    } else {
                        tracing::error!(
                            "Persisted snapshot {} checksum mismatch — starting without snapshot",
                            pmeta.path
                        );
                    }
                }
                Err(e) => {
                    tracing::error!(
                        "Persisted snapshot {} unreadable: {e} — starting without snapshot",
                        pmeta.path
                    );
                }
            }
        }

        // 2. 无快照时从 META_LAST_APPLIED 恢复（A.5 步骤 2）
        if current_snapshot.is_none() {
            if let Some(applied) = state_machine
                .get_applied_log_id()
                .ok()
                .flatten()
                .filter(|a| a.index > 0)
            {
                last_applied = Some(LogIdOf::<TypeConfig>::new(
                    openraft::impls::leader_id_adv::LeaderId {
                        term: applied.term,
                        node_id: applied.node_id,
                    },
                    applied.index,
                ));
            }
        }

        // 2.5 从 META_MEMBERSHIP 恢复（applied 持久化后，membership 不再依靠日志重放重建）
        if current_snapshot.is_none() {
            let persisted_membership = state_machine
                .backend()
                .read(|tx| tx.get(TABLE_META, META_MEMBERSHIP))
                .ok()
                .flatten()
                .and_then(|bytes| bincode::deserialize(&bytes).ok());
            if let Some(m) = persisted_membership {
                last_membership = m;
            }
        }

        Self {
            state_machine,
            last_applied: Mutex::new(last_applied),
            last_membership: Mutex::new(last_membership),
            current_snapshot: Mutex::new(current_snapshot),
            snapshot_dir,
            snapshot_tracker,
            watch_dispatcher: None,
            auth_manager: None,
            revocation_store: None,
            session_manager: None,
            metrics: None,
            lease_revoke_tx: None,
        }
    }

    /// 设置 Lease Revoke 广播通道（T5.7：仅 region 0 状态机调用）
    pub fn set_lease_revoke_tx(&mut self, tx: tokio::sync::mpsc::UnboundedSender<i64>) {
        self.lease_revoke_tx = Some(tx);
    }

    /// 设置 Watch 事件分发器（通常在 Leader 选举后调用）
    /// 与 CoordNode 共享同一 `Arc<WatchDispatcher>`，确保 apply 路径
    /// 分发的 Watch 事件与 gRPC Watch 订阅者使用同一个订阅表。
    pub fn set_watch_dispatcher(&mut self, dispatcher: Arc<WatchDispatcher>) {
        self.watch_dispatcher = Some(dispatcher);
    }

    /// 设置 AuthManager 内存缓存视图（P0-C.2：apply AuthOp 后同步）
    pub fn set_auth_manager(&mut self, manager: Arc<AuthManager>) {
        self.auth_manager = Some(manager);
    }

    /// 设置吊销登记存储（P0-C.5：apply RevokeJti 后同步）
    pub fn set_revocation_store(&mut self, store: Arc<RevocationStore>) {
        self.revocation_store = Some(store);
    }

    /// 设置会话表（P2-07：apply IssueSession/ConsumeSession 后同步 TokenManager 视图）
    pub fn set_session_manager(&mut self, manager: Arc<TokenManager>) {
        self.session_manager = Some(manager);
    }

    /// 推进 applied 状态：更新内存 + 持久化 `META_LAST_APPLIED`（D-A4）
    ///
    /// Normal 条目在命令事务内已持久化，此路径用于 Membership/Blank 等
    /// 不写 KV 事务的条目（单独小事务，幂等）。
    fn update_applied(&self, log_id: LogIdOf<TypeConfig>) -> Result<(), io::Error> {
        {
            *self.last_applied.lock() = Some(log_id);
        }
        let applied = AppliedLogId {
            term: log_id.leader_id.term,
            node_id: log_id.leader_id.node_id,
            index: log_id.index,
        };
        self.state_machine.set_last_applied(applied).map_err(io_err)
    }

    /// 持久化 membership（与 applied 持久化配套；重启后不再依靠日志重放重建）
    fn persist_membership(&self) -> Result<(), io::Error> {
        let bytes = bincode::serialize(&*self.last_membership.lock())
            .map_err(|e| io::Error::other(format!("serialize membership: {e}")))?;
        self.state_machine
            .backend()
            .write(|tx| tx.insert(TABLE_META, META_MEMBERSHIP, &bytes))
            .map_err(io_err)
    }

    /// 从磁盘恢复 applied LogId（`META_LAST_APPLIED`）
    fn load_applied(&self) -> Result<Option<LogIdOf<TypeConfig>>, io::Error> {
        let applied = self.state_machine.get_applied_log_id().map_err(io_err)?;
        Ok(applied.map(|a| {
            LogIdOf::<TypeConfig>::new(
                openraft::impls::leader_id_adv::LeaderId {
                    term: a.term,
                    node_id: a.node_id,
                },
                a.index,
            )
        }))
    }

    /// 执行单个 Normal 命令：revision ≡ entry index（D-A2），返回响应与 Watch 事件
    ///
    /// 幂等守卫（D-A3）：replayed 时返回 `(resp, None)`（不产生副作用、不分发事件）。
    fn execute_command(
        &self,
        sm: &MvccStorage<RedbBackend>,
        cmd: &Command,
        revision: u64,
        applied: AppliedLogId,
    ) -> Result<(Response, Option<ChangeEvent>), io::Error> {
        match cmd {
            Command::Put {
                key,
                value,
                lease_id,
            } => {
                let outcome = sm
                    .put_at_revision(key, value, *lease_id, revision, applied)
                    .map_err(io_err)?;
                let event = if outcome.replayed {
                    None
                } else {
                    Some(ChangeEvent {
                        revision,
                        changes: vec![KeyValueChange {
                            key: key.clone(),
                            value: Some(value.clone()),
                            prev_value: None,
                        }],
                        event_type: EventType::Put,
                    })
                };
                Ok((Response::Put { revision }, event))
            }
            Command::Delete { key } => {
                let outcome = sm
                    .delete_at_revision(key, revision, applied)
                    .map_err(io_err)?;
                let event = if outcome.replayed {
                    None
                } else {
                    Some(ChangeEvent {
                        revision,
                        changes: vec![KeyValueChange {
                            key: key.clone(),
                            value: None,
                            prev_value: None,
                        }],
                        event_type: EventType::Delete,
                    })
                };
                Ok((Response::Delete { revision }, event))
            }
            Command::DeleteRange { key, range_end } => {
                let outcome = sm
                    .delete_range_at_revision(key, range_end, revision, applied)
                    .map_err(io_err)?;
                let event = if outcome.replayed {
                    None
                } else {
                    Some(ChangeEvent {
                        revision,
                        changes: outcome
                            .deleted_keys
                            .iter()
                            .map(|k| KeyValueChange {
                                key: k.clone(),
                                value: None,
                                prev_value: None,
                            })
                            .collect(),
                        event_type: EventType::Delete,
                    })
                };
                Ok((
                    Response::DeleteRange {
                        revision,
                        deleted: outcome.deleted_keys.len() as u64,
                    },
                    event,
                ))
            }
            Command::Txn {
                compares,
                success_ops,
                failure_ops,
            } => {
                // 幂等守卫：apply 持有 sm 独占锁，先查后写无 TOCTOU 窗口
                let replayed = sm.changelog_contains_revision(revision).map_err(io_err)?;
                if replayed {
                    return Ok((
                        Response::Txn {
                            succeeded: false,
                            revision,
                            responses: Vec::new(),
                        },
                        None,
                    ));
                }

                let result = sm
                    .execute_txn_at_revision(compares, success_ops, failure_ops, revision, applied)
                    .map_err(io_err)?;

                // R-OBS-10：Txn 计数（条件不满足 = 冲突）
                if let Some(metrics) = &self.metrics {
                    metrics.record_txn(!result.succeeded);
                }

                // 从 Txn 操作中提取变更 Key
                // 将 success/failure 分支的操作转为 changes
                let ops = if result.succeeded {
                    success_ops
                } else {
                    failure_ops
                };
                let txn_changes: Vec<KeyValueChange> = ops
                    .iter()
                    .map(|op| match op {
                        crate::txn::TxnOp::Put { key, value, .. } => KeyValueChange {
                            key: key.clone(),
                            value: Some(value.clone()),
                            prev_value: None,
                        },
                        crate::txn::TxnOp::Delete { key } => KeyValueChange {
                            key: key.clone(),
                            value: None,
                            prev_value: None,
                        },
                        crate::txn::TxnOp::Range { .. } => KeyValueChange {
                            key: vec![],
                            value: None,
                            prev_value: None,
                        },
                    })
                    .filter(|c| !c.key.is_empty())
                    .collect();

                Ok((
                    Response::Txn {
                        succeeded: result.succeeded,
                        revision: result.revision,
                        responses: result.responses,
                    },
                    Some(ChangeEvent {
                        revision,
                        changes: txn_changes,
                        event_type: EventType::Txn,
                    }),
                ))
            }
            Command::Lease(op) => {
                let (outcome, changes) =
                    sm.apply_lease_op(op, revision, applied).map_err(io_err)?;
                let event = if outcome.replayed {
                    None
                } else {
                    Some(ChangeEvent {
                        revision,
                        changes,
                        event_type: EventType::Lease,
                    })
                };
                Ok((Response::Lease { revision }, event))
            }
            Command::Auth(op) => {
                // P0-C.2：AuthOp 入 raft 日志，apply 持久化 `/_sys/auth/`
                let outcome = sm.apply_auth_op(op, revision, applied).map_err(io_err)?;
                if !outcome.replayed {
                    // 同步内存缓存视图（AuthManager 与 RevocationStore）
                    if let Some(ref manager) = self.auth_manager {
                        manager.apply_auth_op_to_view(op);
                    }
                    if let (Some(ref store), crate::raft::type_config::AuthOp::RevokeJti { jti }) =
                        (&self.revocation_store, op)
                    {
                        store.revoke(jti);
                    }
                    // P2-07：同步会话表视图（TokenManager，各节点一致）
                    if let Some(ref tm) = self.session_manager {
                        match op {
                            crate::raft::type_config::AuthOp::IssueSession {
                                hash_hex,
                                username,
                                expires_at_unix,
                                is_refresh,
                            } => tm.register_session(
                                hash_hex,
                                username,
                                *expires_at_unix,
                                *is_refresh,
                            ),
                            crate::raft::type_config::AuthOp::ConsumeSession { hash_hex } => {
                                tm.remove_session(hash_hex)
                            }
                            _ => {}
                        }
                    }
                }
                Ok((Response::Auth { revision }, None))
            }
            Command::Compact { revision } => {
                // P1-01：raft 下发 compact revision，apply 分片删除（幂等、确定性）。
                // 非法 revision（> applied）在 apply 内钳制，拒绝由 RPC/提案层负责。
                let outcome = sm.apply_compact(*revision, applied).map_err(io_err)?;
                let _ = outcome; // 计数已由 apply 内日志记录
                let effective = (*revision).min(applied.index);
                Ok((
                    Response::Compact {
                        compacted_revision: effective,
                    },
                    None,
                ))
            }
            Command::DeleteKeysByLease { lease_id } => {
                // T5.7（R-MR-04）：per-Region lease 清理（apply 期按 lease_id 索引扫描
                // 删除，幂等；事件供 Watch 分发）。
                let (outcome, changes) = sm
                    .apply_delete_keys_by_lease(*lease_id, revision, applied)
                    .map_err(io_err)?;
                let (deleted, event) = if outcome.replayed {
                    (0, None)
                } else {
                    (
                        changes.len() as u64,
                        Some(ChangeEvent {
                            revision,
                            changes,
                            event_type: EventType::Lease,
                        }),
                    )
                };
                Ok((Response::DeleteRange { revision, deleted }, event))
            }
        }
    }
}

impl RaftStateMachine<TypeConfig> for StateMachineStore {
    type SnapshotData = super::RaftSnapshotData;
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogIdOf<TypeConfig>>, StoredMembershipOf<TypeConfig>), io::Error> {
        // D-A4：从盘读取（修复重启后全量重放问题）
        let last_applied = self.load_applied()?;
        let membership = self.last_membership.lock().clone();
        Ok((last_applied, membership))
    }

    async fn apply<Strm>(&mut self, entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>> + Unpin + OptionalSend,
    {
        let entries: Vec<EntryResponder<TypeConfig>> = entries.try_collect().await?;
        let sm: &MvccStorage<RedbBackend> = &self.state_machine;
        // R-OBS-10：apply 耗时埋点
        let apply_start = std::time::Instant::now();

        // M0-5 修复：Normal 条目 apply 时同步内存 last_applied。此前仅在
        // Membership 路径更新内存（持久化水位由写路径同事务写入
        // META_LAST_APPLIED），导致 build_snapshot 用陈旧/空白的 last_log_id
        // 生成快照 meta——openraft 按错误水位计算 purge 点（或根本跳过 purge），
        // 重启后快照 meta 也无法覆盖已 purge 的日志。
        let mut last_normal_log_id: Option<LogIdOf<TypeConfig>> = None;

        for (entry, maybe_responder) in entries {
            let response = match &entry.payload {
                EntryPayload::Normal(cmd) => {
                    // D-A2：revision ≡ log index
                    let revision = entry.log_id.index;
                    let applied = AppliedLogId {
                        term: entry.log_id.leader_id.term,
                        node_id: entry.log_id.leader_id.node_id,
                        index: revision,
                    };
                    let (resp, change_event) = self.execute_command(sm, cmd, revision, applied)?;
                    last_normal_log_id = Some(entry.log_id.clone());

                    // T5.7（R-MR-04）：region 0 状态机在 LeaseOp::Revoke apply（含
                    // 过期清理与显式 revoke）后广播 lease_id——所有节点 apply region 0
                    // 日志都会收到，最终由各 Region 的 raft leader 完成 per-Region
                    // Key 清理（幂等；重复广播仅产生 no-op）。
                    if let Some(tx) = &self.lease_revoke_tx {
                        if let crate::raft::type_config::Command::Lease(
                            crate::raft::type_config::LeaseOp::Revoke { id, .. },
                        ) = cmd
                        {
                            let _ = tx.send(*id);
                        }
                    }

                    // 分发 Watch 事件（非阻塞；replayed 时事件为 None）
                    if let (Some(dispatcher), Some(event)) = (&self.watch_dispatcher, change_event)
                    {
                        dispatcher.as_ref().dispatch(event);
                    }

                    resp
                }
                EntryPayload::Membership(mem) => {
                    *self.last_membership.lock() =
                        StoredMembershipOf::<TypeConfig>::new(Some(entry.log_id), mem.clone());
                    self.update_applied(entry.log_id)?;
                    self.persist_membership()?;
                    Response::Put { revision: 0 }
                }
                EntryPayload::Blank => Response::Put { revision: 0 },
            };

            if let Some(responder) = maybe_responder {
                responder.send(response);
            }
        }

        // M0-5 修复：以本批最后一个 Normal 条目同步内存 last_applied。
        if let Some(log_id) = last_normal_log_id {
            *self.last_applied.lock() = Some(log_id);
        }

        // R-OBS-10：记录 apply 耗时
        if let Some(metrics) = &self.metrics {
            metrics.record_apply(apply_start.elapsed().as_micros() as u64);
        }
        Ok(())
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<TypeConfig>,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), io::Error> {
        let data = snapshot.get_ref().clone();

        // 恢复快照数据到 MvccStorage
        if !data.is_empty() {
            let snapshot_data = SnapshotData::from_bytes_migrating(&data)
                .map_err(|e| io::Error::other(e.to_string()))?;
            // R-RFT-06：快照携带完整 applied LogId（term/node_id/index），
            // 导入时不再降级为 AppliedLogId::standalone（term/node_id 置零）
            let sm = Arc::clone(&self.state_machine);
            tokio::task::spawn_blocking(move || import_snapshot_data(sm.as_ref(), &snapshot_data))
                .await
                .map_err(|e| io::Error::other(format!("snapshot import task join: {e}")))?
                .map_err(|e| io::Error::other(e.to_string()))?;
        }

        // A.6：安装的快照同样落盘（tmp → fsync → rename → 校验和），保证重启可恢复。
        // Phase 1 T1.3：fsync 落盘段在阻塞线程池执行。
        let (path, checksum) = self.persist_snapshot_file_blocking(meta, data.clone()).await?;

        *self.current_snapshot.lock() = Some(StoredSnapshot {
            meta: meta.clone(),
            data,
        });
        *self.last_applied.lock() = meta.last_log_id;
        *self.last_membership.lock() = meta.last_membership.clone();
        self.persist_membership()?;

        let last_idx = meta.last_log_id.as_ref().map(|l| l.index).unwrap_or(0);
        let last_term = meta
            .last_log_id
            .as_ref()
            .map(|l| l.leader_id.term)
            .unwrap_or(0);
        self.snapshot_tracker
            .record_durable(last_idx, last_term, path.clone());
        tracing::info!("Installed snapshot persisted to {}", path.display());
        let _ = checksum;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<SnapshotOf<TypeConfig, super::RaftSnapshotData>>, io::Error> {
        let snap = self.current_snapshot.lock();
        match snap.as_ref() {
            Some(s) => Ok(Some(SnapshotOf::<TypeConfig, super::RaftSnapshotData> {
                meta: s.meta.clone(),
                snapshot: Cursor::new(s.data.clone()),
            })),
            None => Ok(None),
        }
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        StateMachineStore {
            state_machine: Arc::clone(&self.state_machine),
            last_applied: Mutex::new(*self.last_applied.lock()),
            last_membership: Mutex::new(self.last_membership.lock().clone()),
            current_snapshot: Mutex::new(self.current_snapshot.lock().clone()),
            snapshot_dir: self.snapshot_dir.clone(),
            snapshot_tracker: Arc::clone(&self.snapshot_tracker),
            watch_dispatcher: None,
            auth_manager: None,
            revocation_store: None,
            session_manager: None,
            metrics: self.metrics.clone(),
            lease_revoke_tx: None,
        }
    }
}

impl StateMachineStore {
    /// A.6.2：快照字节落盘 —— 临时文件 → fsync → 原子 rename → SHA256
    ///
    /// 返回（最终路径，SHA256 校验和）。同时持久化 `META_SNAPSHOT` 并登记 purge 守卫。
    ///
    /// Phase 1 T1.3：磁盘 IO（create_dir_all / File::create / write_all / fsync /
    /// rename / 目录 fsync / redb META_SNAPSHOT 写事务 / 旧快照清理）整体在阻塞线程池
    /// 执行（见 `persist_snapshot_file_blocking`）；本方法保留同步实现供启动自愈等
    /// 非热路径使用，内部委托同一纯函数，保证登记时序一致。
    fn persist_snapshot_file(
        &self,
        meta: &SnapshotMetaOf<TypeConfig>,
        data: &[u8],
    ) -> Result<(PathBuf, [u8; 32]), io::Error> {
        persist_snapshot_file_impl(
            &self.snapshot_dir,
            &self.state_machine,
            &self.snapshot_tracker,
            meta,
            data,
        )
    }

    /// 异步落盘：磁盘 IO 移入 `spawn_blocking`，避免阻塞 tokio worker
    /// （Phase 1 T1.3，Multi-Raft 前置）。保持 snapshot_tracker 登记时序
    /// （落盘成功后才 `record_durable`，与同步版完全一致）。
    async fn persist_snapshot_file_blocking(
        &self,
        meta: &SnapshotMetaOf<TypeConfig>,
        data: Vec<u8>,
    ) -> Result<(PathBuf, [u8; 32]), io::Error> {
        let snapshot_dir = self.snapshot_dir.clone();
        let state_machine = Arc::clone(&self.state_machine);
        let snapshot_tracker = Arc::clone(&self.snapshot_tracker);
        let meta = meta.clone();
        tokio::task::spawn_blocking(move || {
            persist_snapshot_file_impl(&snapshot_dir, &state_machine, &snapshot_tracker, &meta, &data)
        })
        .await
        .map_err(|e| io::Error::other(format!("snapshot persist task join: {e}")))?
    }

    /// S-RCV-01 启动自愈：日志已被 purge 但快照文件缺失时，从 MVCC 的
    /// `META_LAST_APPLIED` 重新导出并落盘快照（数据同源，无需网络安装）。
    ///
    /// 调用方须先确认 `META_LAST_APPLIED ≥ purge 点`（否则 MVCC 状态不足以
    /// 覆盖已删除的日志，应放行启动、依赖 leader 的 install-snapshot 补齐）。
    /// 成功返回快照文件路径，并同步恢复 `META_SNAPSHOT`、purge 守卫与内存视图。
    pub fn rebuild_snapshot_from_mvcc(&self) -> Result<PathBuf, io::Error> {
        let applied = self
            .state_machine
            .get_applied_log_id()
            .map_err(io_err)?
            .filter(|a| a.index > 0)
            .ok_or_else(|| {
                io::Error::other("MVCC has no applied state; cannot rebuild snapshot locally")
            })?;

        let last_log_id = LogIdOf::<TypeConfig>::new(
            openraft::impls::leader_id_adv::LeaderId {
                term: applied.term,
                node_id: applied.node_id,
            },
            applied.index,
        );

        let last_membership = self
            .state_machine
            .backend()
            .read(|tx| tx.get(TABLE_META, META_MEMBERSHIP))
            .ok()
            .flatten()
            .and_then(|bytes| bincode::deserialize(&bytes).ok())
            .unwrap_or_else(|| {
                StoredMembershipOf::<TypeConfig>::new(
                    None,
                    Membership::<u64, openraft::BasicNode>::new_with_defaults(vec![], vec![]),
                )
            });

        let meta = SnapshotMetaOf::<TypeConfig> {
            last_log_id: Some(last_log_id.clone()),
            last_membership: last_membership.clone(),
        };

        let snapshot_data = export_snapshot_data(&self.state_machine, applied.index, applied.term)
            .map_err(io_err)?;
        let data_bytes = snapshot_data.to_bytes().map_err(io_err)?;

        // A.6：落盘（临时文件 → fsync → rename → 校验和 → META_SNAPSHOT → purge 守卫）
        let (path, _checksum) = self.persist_snapshot_file(&meta, &data_bytes)?;

        *self.current_snapshot.lock() = Some(StoredSnapshot {
            meta: meta.clone(),
            data: data_bytes,
        });
        *self.last_applied.lock() = Some(last_log_id);
        *self.last_membership.lock() = last_membership;

        tracing::warn!(
            "Rebuilt missing raft snapshot from MVCC state: {} (last_log_id={:?})",
            path.display(),
            meta.last_log_id
        );
        Ok(path)
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachineStore {
    type SnapshotData = super::RaftSnapshotData;

    async fn build_snapshot(
        &mut self,
    ) -> Result<SnapshotOf<TypeConfig, super::RaftSnapshotData>, io::Error> {
        // R-OBS-10：快照构建耗时埋点
        let snapshot_start = std::time::Instant::now();

        let last_log_id = match self.load_applied() {
            // M0-5 修复：快照 meta 以存储层 META_LAST_APPLIED 为准（与快照数据
            // 导出同源），内存值仅作回退。此前直接读内存 last_applied，在
            // 长时间无 Membership 变更时写入陈旧/空白水位，导致快照 meta
            // 无法覆盖已 purge 日志或 openraft 跳过 purge。
            Ok(Some(applied)) => Some(applied),
            Ok(None) => *self.last_applied.lock(),
            Err(e) => return Err(e),
        };
        let last_membership = self.last_membership.lock().clone();

        let meta = SnapshotMetaOf::<TypeConfig> {
            last_log_id,
            last_membership: last_membership.clone(),
        };

        // 从 MvccStorage 导出真实快照数据（全库单读事务）——阻塞线程池执行（Phase 1 T1.3）
        let sm = Arc::clone(&self.state_machine);
        let export_last_idx = last_log_id.as_ref().map(|id| id.index).unwrap_or(0);
        let export_last_term = last_log_id
            .as_ref()
            .map(|id| id.leader_id.term)
            .unwrap_or(0);
        let data_bytes = tokio::task::spawn_blocking(move || {
            let snapshot_data = export_snapshot_data(sm.as_ref(), export_last_idx, export_last_term)
                .map_err(|e| io::Error::other(e.to_string()))?;
            snapshot_data
                .to_bytes()
                .map_err(|e| io::Error::other(e.to_string()))
        })
        .await
        .map_err(|e| io::Error::other(format!("snapshot export task join: {e}")))??;

        // A.6：落盘（临时文件 → fsync → rename → 校验和 → META_SNAPSHOT → purge 守卫）。
        // Phase 1 T1.3：fsync 落盘段在阻塞线程池执行。
        let (_path, _checksum) = self.persist_snapshot_file_blocking(&meta, data_bytes.clone()).await?;

        let snapshot = SnapshotOf::<TypeConfig, super::RaftSnapshotData> {
            meta: meta.clone(),
            snapshot: Cursor::new(data_bytes.clone()),
        };

        *self.current_snapshot.lock() = Some(StoredSnapshot {
            meta,
            data: data_bytes,
        });

        // R-OBS-10：记录快照构建耗时
        if let Some(metrics) = &self.metrics {
            metrics.record_snapshot(snapshot_start.elapsed().as_micros() as u64);
        }

        Ok(snapshot)
    }
}
