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

pub struct StateMachineStore {
    pub state_machine: Arc<MvccStorage<RedbBackend>>,
    pub last_applied: Mutex<Option<LogIdOf<TypeConfig>>>,
    pub last_membership: Mutex<StoredMembershipOf<TypeConfig>>,
    snapshot_idx: Mutex<u64>,
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
            snapshot_idx: Mutex::new(0),
            current_snapshot: Mutex::new(current_snapshot),
            snapshot_dir,
            snapshot_tracker,
            watch_dispatcher: None,
            auth_manager: None,
            revocation_store: None,
            session_manager: None,
        }
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
        }
    }
}

impl RaftStateMachine<TypeConfig> for StateMachineStore {
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
        Ok(())
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Cursor<Vec<u8>>, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<TypeConfig>,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), io::Error> {
        let data = snapshot.get_ref().clone();

        // 恢复快照数据到 MvccStorage
        if !data.is_empty() {
            let snapshot_data =
                SnapshotData::from_bytes(&data).map_err(|e| io::Error::other(e.to_string()))?;
            import_snapshot_data(&self.state_machine, &snapshot_data)
                .map_err(|e| io::Error::other(e.to_string()))?;
        }

        // A.6：安装的快照同样落盘（tmp → fsync → rename → 校验和），保证重启可恢复
        let (path, checksum) = self.persist_snapshot_file(meta, &data)?;

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

    async fn get_current_snapshot(&mut self) -> Result<Option<SnapshotOf<TypeConfig>>, io::Error> {
        let snap = self.current_snapshot.lock();
        match snap.as_ref() {
            Some(s) => Ok(Some(SnapshotOf::<TypeConfig> {
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
            snapshot_idx: Mutex::new(*self.snapshot_idx.lock()),
            current_snapshot: Mutex::new(self.current_snapshot.lock().clone()),
            snapshot_dir: self.snapshot_dir.clone(),
            snapshot_tracker: Arc::clone(&self.snapshot_tracker),
            watch_dispatcher: None,
            auth_manager: None,
            revocation_store: None,
            session_manager: None,
        }
    }
}

impl StateMachineStore {
    /// A.6.2：快照字节落盘 —— 临时文件 → fsync → 原子 rename → SHA256
    ///
    /// 返回（最终路径，SHA256 校验和）。同时持久化 `META_SNAPSHOT` 并登记 purge 守卫。
    fn persist_snapshot_file(
        &self,
        meta: &SnapshotMetaOf<TypeConfig>,
        data: &[u8],
    ) -> Result<(PathBuf, [u8; 32]), io::Error> {
        use std::io::Write;

        std::fs::create_dir_all(&self.snapshot_dir).map_err(|e| {
            io::Error::other(format!(
                "create snapshot dir {}: {e}",
                self.snapshot_dir.display()
            ))
        })?;

        let last_idx = meta.last_log_id.as_ref().map(|l| l.index).unwrap_or(0);
        let last_term = meta
            .last_log_id
            .as_ref()
            .map(|l| l.leader_id.term)
            .unwrap_or(0);
        let final_path = self
            .snapshot_dir
            .join(format!("snapshot-{last_idx}-{last_term}.snap"));
        let tmp_path = self
            .snapshot_dir
            .join(format!(".snapshot-{last_idx}-{last_term}.snap.tmp"));

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
        if let Ok(dir) = std::fs::File::open(&self.snapshot_dir) {
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
        self.state_machine
            .backend()
            .write(|tx| tx.insert(TABLE_META, META_SNAPSHOT, &persisted_bytes))
            .map_err(io_err)?;
        self.snapshot_tracker
            .record_durable(last_idx, last_term, final_path.clone());

        // 5. A.6.3：保留最近 3 份快照，清理旧份
        self.cleanup_old_snapshots(&final_path)?;

        Ok((final_path, checksum))
    }

    /// 清理快照目录，仅保留最新 3 份 `.snap` 文件（A.6.3）
    fn cleanup_old_snapshots(&self, _keep: &PathBuf) -> Result<(), io::Error> {
        let mut snaps: Vec<PathBuf> = match std::fs::read_dir(&self.snapshot_dir) {
            Ok(entries) => entries
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| p.extension().map(|e| e == "snap").unwrap_or(false))
                .collect(),
            Err(_) => return Ok(()),
        };
        snaps.sort();
        snaps.reverse(); // 文件名以 index 开头，字典序即新到旧
        for path in snaps.iter().skip(3) {
            if let Err(e) = std::fs::remove_file(path) {
                tracing::warn!("Failed to remove old snapshot {}: {e}", path.display());
            }
        }
        Ok(())
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachineStore {
    async fn build_snapshot(&mut self) -> Result<SnapshotOf<TypeConfig>, io::Error> {
        let mut idx = self.snapshot_idx.lock();
        *idx += 1;

        let last_log_id = *self.last_applied.lock();
        let last_membership = self.last_membership.lock().clone();

        let meta = SnapshotMetaOf::<TypeConfig> {
            last_log_id,
            last_membership: last_membership.clone(),
            snapshot_id: format!("snapshot-{}", *idx),
        };

        // 从 MvccStorage 导出真实快照数据
        let sm: &MvccStorage<RedbBackend> = &self.state_machine;
        let last_idx = last_log_id.as_ref().map(|id| id.index).unwrap_or(0);
        let last_term = last_log_id
            .as_ref()
            .map(|id| id.leader_id.term)
            .unwrap_or(0);
        let snapshot_data = export_snapshot_data(sm, last_idx, last_term)
            .map_err(|e| io::Error::other(e.to_string()))?;
        let data_bytes = snapshot_data
            .to_bytes()
            .map_err(|e| io::Error::other(e.to_string()))?;

        // A.6：落盘（临时文件 → fsync → rename → 校验和 → META_SNAPSHOT → purge 守卫）
        let (_path, _checksum) = self.persist_snapshot_file(&meta, &data_bytes)?;

        let snapshot = SnapshotOf::<TypeConfig> {
            meta: meta.clone(),
            snapshot: Cursor::new(data_bytes.clone()),
        };

        *self.current_snapshot.lock() = Some(StoredSnapshot {
            meta,
            data: data_bytes,
        });

        Ok(snapshot)
    }
}
