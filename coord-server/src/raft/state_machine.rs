// Raft StateMachine — Openraft RaftStateMachine + RaftSnapshotBuilder 实现

use std::fmt;
use std::io;
use std::io::Cursor;
use std::sync::Arc;

use futures::Stream;
use futures::TryStreamExt;
use openraft::storage::EntryResponder;
use openraft::storage::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use openraft::type_config::alias::{LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf};
use openraft::{EntryPayload, Membership, OptionalSend};
use parking_lot::Mutex;

use super::type_config::{Command, Response, TypeConfig};
use crate::storage::mvcc::{ChangeEvent, EventType, KeyValueChange, MvccStorage};
use crate::storage::redb_backend::RedbBackend;
use crate::storage::snapshot::{export_snapshot_data, import_snapshot_data, SnapshotData};
use crate::watch::WatchDispatcher;

fn io_err(e: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct StoredSnapshot {
    pub meta: SnapshotMetaOf<TypeConfig>,
    pub data: Vec<u8>,
}

pub struct StateMachineStore {
    pub state_machine: Arc<Mutex<MvccStorage<RedbBackend>>>,
    pub last_applied: Mutex<Option<LogIdOf<TypeConfig>>>,
    pub last_membership: Mutex<StoredMembershipOf<TypeConfig>>,
    snapshot_idx: Mutex<u64>,
    current_snapshot: Mutex<Option<StoredSnapshot>>,
    /// Watch 事件分发器（可选，Leader 节点持有，与 CoordNode 共享同一实例）
    pub watch_dispatcher: Option<Arc<WatchDispatcher>>,
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
    pub fn new(state_machine: MvccStorage<RedbBackend>) -> Self {
        let empty_membership: Membership<u64, openraft::BasicNode> =
            Membership::new_with_defaults(vec![], vec![]);
        Self {
            state_machine: Arc::new(Mutex::new(state_machine)),
            last_applied: Mutex::new(None),
            last_membership: Mutex::new(StoredMembershipOf::<TypeConfig>::new(
                None,
                empty_membership,
            )),
            snapshot_idx: Mutex::new(0),
            current_snapshot: Mutex::new(None),
            watch_dispatcher: None,
        }
    }

    /// 设置 Watch 事件分发器（通常在 Leader 选举后调用）
    /// 与 CoordNode 共享同一 `Arc<WatchDispatcher>`，确保 apply 路径
    /// 分发的 Watch 事件与 gRPC Watch 订阅者使用同一个订阅表。
    pub fn set_watch_dispatcher(&mut self, dispatcher: Arc<WatchDispatcher>) {
        self.watch_dispatcher = Some(dispatcher);
    }

    fn update_applied(&self, log_id: LogIdOf<TypeConfig>) {
        *self.last_applied.lock() = Some(log_id);
    }
}

impl RaftStateMachine<TypeConfig> for StateMachineStore {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogIdOf<TypeConfig>>, StoredMembershipOf<TypeConfig>), io::Error> {
        let last_applied = self.last_applied.lock().clone();
        let membership = self.last_membership.lock().clone();
        Ok((last_applied, membership))
    }

    async fn apply<Strm>(&mut self, entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>> + Unpin + OptionalSend,
    {
        let entries: Vec<EntryResponder<TypeConfig>> = entries.try_collect().await?;
        let sm = self.state_machine.lock();

        for (entry, maybe_responder) in entries {
            let response = match &entry.payload {
                EntryPayload::Normal(cmd) => {
                    let change_event: Option<ChangeEvent>; // 延迟初始化，每个分支都会赋值

                    let resp = match cmd {
                        Command::Put { key, value, lease_id } => {
                            let rev = sm.put(key, value, *lease_id).map_err(|e| io_err(e))?;
                            change_event = Some(ChangeEvent {
                                revision: rev,
                                changes: vec![KeyValueChange {
                                    key: key.clone(),
                                    value: Some(value.clone()),
                                    prev_value: None,
                                }],
                                event_type: EventType::Put,
                            });
                            Response::Put { revision: rev }
                        }
                        Command::Delete { key } => {
                            let rev = sm.delete(key).map_err(|e| io_err(e))?;
                            change_event = Some(ChangeEvent {
                                revision: rev,
                                changes: vec![KeyValueChange {
                                    key: key.clone(),
                                    value: None,
                                    prev_value: None,
                                }],
                                event_type: EventType::Delete,
                            });
                            Response::Delete { revision: rev }
                        }
                        Command::Txn {
                            compares,
                            success_ops,
                            failure_ops,
                        } => {
                            let result = sm
                                .execute_txn(compares, success_ops, failure_ops)
                                .map_err(|e| io_err(e))?;

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
                                    crate::txn::TxnOp::Put { key, value, .. } => {
                                        KeyValueChange {
                                            key: key.clone(),
                                            value: Some(value.clone()),
                                            prev_value: None,
                                        }
                                    }
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

                            change_event = Some(ChangeEvent {
                                revision: result.revision,
                                changes: txn_changes,
                                event_type: EventType::Txn,
                            });

                            Response::Txn {
                                succeeded: result.succeeded,
                                revision: result.revision,
                                responses: result.responses,
                            }
                        }
                    };

                    // 分发 Watch 事件（非阻塞）
                    if let (Some(dispatcher), Some(event)) =
                        (&self.watch_dispatcher, change_event)
                    {
                        dispatcher.as_ref().dispatch(event);
                    }

                    resp
                }
            ,
                EntryPayload::Membership(mem) => {
                    *self.last_membership.lock() =
                        StoredMembershipOf::<TypeConfig>::new(Some(entry.log_id), mem.clone());
                    Response::Put { revision: 0 }
                }
                EntryPayload::Blank => Response::Put { revision: 0 },
            };

            if let Some(responder) = maybe_responder {
                responder.send(response);
            }
            self.update_applied(entry.log_id);
        }
        Ok(())
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Cursor<Vec<u8>>, io::Error> {
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
            let snapshot_data = SnapshotData::from_bytes(&data)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            let sm = self.state_machine.lock();
            import_snapshot_data(&sm, &snapshot_data)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        }

        *self.current_snapshot.lock() = Some(StoredSnapshot {
            meta: meta.clone(),
            data,
        });
        *self.last_applied.lock() = meta.last_log_id.clone();
        *self.last_membership.lock() = meta.last_membership.clone();
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<SnapshotOf<TypeConfig>>, io::Error> {
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
            last_applied: Mutex::new(self.last_applied.lock().clone()),
            last_membership: Mutex::new(self.last_membership.lock().clone()),
            snapshot_idx: Mutex::new(*self.snapshot_idx.lock()),
            current_snapshot: Mutex::new(self.current_snapshot.lock().clone()),
            watch_dispatcher: None,
        }
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachineStore {
    async fn build_snapshot(&mut self) -> Result<SnapshotOf<TypeConfig>, io::Error> {
        let mut idx = self.snapshot_idx.lock();
        *idx += 1;

        let last_log_id = self.last_applied.lock().clone();
        let last_membership = self.last_membership.lock().clone();

        let meta = SnapshotMetaOf::<TypeConfig> {
            last_log_id: last_log_id.clone(),
            last_membership: last_membership.clone(),
            snapshot_id: format!("snapshot-{}", *idx),
        };

        // 从 MvccStorage 导出真实快照数据
        let sm = self.state_machine.lock();
        let last_idx = last_log_id.as_ref().map(|id| id.index).unwrap_or(0);
        let last_term = last_log_id
            .as_ref()
            .map(|id| id.leader_id.term)
            .unwrap_or(0);
        let snapshot_data = export_snapshot_data(&sm, last_idx, last_term)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let data_bytes = snapshot_data
            .to_bytes()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

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
