// Coord Server — gRPC 服务实现
//
// 实现 5 个 gRPC 服务（KV/Lease/Watch/Txn/Maintenance），
// 对接底层 Raft 共识 + StateMachine + LeaseManager + WatchDispatcher + Barrier。
//
// CoordNode 是服务端核心结构体，持有所有组件的引用。
// 写请求（Put/Delete/Txn）通过 Raft 共识提交，读请求直接访问本地状态机。

use std::sync::Arc;
use std::collections::HashMap;

use parking_lot::RwLock;
use openraft::rt::WatchReceiver;
use openraft::ReadPolicy;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use coord_proto::kv::{
    kv_server::Kv, DeleteRequest, DeleteResponse, KeyValue, PutRequest, PutResponse,
    RangeRequest, RangeResponse,
};
use coord_proto::lease::{
    lease_server::Lease, LeaseGrantRequest, LeaseGrantResponse, LeaseKeepAliveRequest,
    LeaseKeepAliveResponse, LeaseRevokeRequest, LeaseRevokeResponse,
};
use coord_proto::txn::{
    txn_server::Txn, Compare, RequestOp, ResponseOp, TxnRequest, TxnResponse,
};
use coord_proto::watch::{
    watch_server::Watch, WatchEvent, WatchRequest, WatchResponse,
};
use coord_proto::maintenance::{
    maintenance_server::Maintenance, SealRequest, SealResponse, StatusRequest, StatusResponse,
    UnsealRequest, UnsealResponse, SnapshotRequest, SnapshotResponse,
    MemberAddRequest, MemberAddResponse, MemberRemoveRequest, MemberRemoveResponse,
    MemberPromoteRequest, MemberPromoteResponse, MemberListRequest, MemberListResponse,
    MemberNode,
};

use crate::lease::LeaseManager;
use crate::raft::CoordRaft;
use crate::raft::type_config::{Command, Response};
use crate::storage::mvcc::MvccStorage;
use crate::storage::redb_backend::RedbBackend;
use crate::txn::{TxnCompare, TxnOp, TxnOpResponse};
use crate::watch::WatchDispatcher;

// ──── CoordNode ────

/// 幂等请求去重缓存条目
#[derive(Debug, Clone)]
struct IdempotentEntry {
    /// 上次响应返回的 revision
    revision: i64,
    /// 上次响应是否 succeeded（仅 Txn 使用）
    succeeded: bool,
}

/// 服务端核心节点，持有所有组件并实现 gRPC 服务 trait
pub struct CoordNode {
    /// MVCC 存储层（共享引用，读写均通过此实例）
    pub storage: Arc<MvccStorage<RedbBackend>>,
    /// Raft 共识实例（可选，集群模式下设置；单节点模式为 None）
    pub raft: Option<Arc<CoordRaft>>,
    /// Lease 管理器（可选，Leader 节点持有）
    pub lease_manager: Option<Arc<LeaseManager>>,
    /// Watch 分发器
    pub watch_dispatcher: Option<Arc<WatchDispatcher>>,
    /// 幂等请求去重缓存（request_id → 上次响应）
    idempotent_cache: RwLock<HashMap<Vec<u8>, IdempotentEntry>>,
}

impl CoordNode {
    pub fn new(storage: Arc<MvccStorage<RedbBackend>>) -> Self {
        Self {
            storage,
            raft: None,
            lease_manager: None,
            watch_dispatcher: None,
            idempotent_cache: RwLock::new(HashMap::new()),
        }
    }

    /// 启动 Lease 过期轮询循环（后台任务）。
    ///
    /// 每 200ms 调用 `LeaseManager::check_expired()`，
    /// 对已过期的 Lease 清理其绑定的 KV key。
    ///
    /// 应在 server 启动后调用（Leader 独占；Follower 无 LeaseManager 则跳过）。
    pub fn start_lease_expiry_worker(self: &Arc<Self>) {
        let node = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(200));
            loop {
                interval.tick().await;
                let Some(ref lm) = node.lease_manager else {
                    continue;
                };
                let actions = lm.check_expired();
                for action in actions {
                    match action {
                        crate::lease::LeaseAction::Expired { lease_id, attached_keys } => {
                            for key in &attached_keys {
                                // 通过 Raft（集群模式）或直接删除（单节点模式）
                                if let Some(ref raft) = node.raft {
                                    let cmd = Command::Delete { key: key.clone() };
                                    if let Err(e) = raft.client_write(cmd).await {
                                        tracing::warn!(
                                            "Lease {} expiry: failed to delete key via raft: {}",
                                            lease_id, e
                                        );
                                    }
                                } else {
                                    if let Err(e) = node.storage.delete(key) {
                                        tracing::warn!(
                                            "Lease {} expiry: failed to delete key: {}",
                                            lease_id, e
                                        );
                                    }
                                }
                            }
                            tracing::debug!(
                                "Lease {} expired, cleaned up {} attached keys",
                                lease_id,
                                attached_keys.len()
                            );
                        }
                    }
                }
            }
        });
    }

    /// 检查幂等 request_id：若已存在则返回缓存的 revision，否则执行操作并缓存
    fn check_idempotent(&self, request_id: &[u8]) -> Option<i64> {
        if request_id.is_empty() {
            return None;
        }
        self.idempotent_cache.read().get(request_id).map(|e| e.revision)
    }

    /// 缓存幂等请求结果
    fn cache_idempotent(&self, request_id: Vec<u8>, revision: i64) {
        if request_id.is_empty() {
            return;
        }
        self.idempotent_cache.write().insert(request_id, IdempotentEntry {
            revision,
            succeeded: true,
        });
    }

    /// 检查并缓存 Txn 幂等请求
    fn check_idempotent_txn(&self, request_id: &[u8]) -> Option<(bool, i64)> {
        if request_id.is_empty() {
            return None;
        }
        self.idempotent_cache.read().get(request_id).map(|e| (e.succeeded, e.revision))
    }

    /// 缓存 Txn 幂等请求结果
    fn cache_idempotent_txn(&self, request_id: Vec<u8>, succeeded: bool, revision: i64) {
        if request_id.is_empty() {
            return;
        }
        self.idempotent_cache.write().insert(request_id, IdempotentEntry {
            revision,
            succeeded,
        });
    }

    /// 确保线性一致性读：通过 ReadIndex 确认 Leader 身份和日志进度（ADP §11.2）
    ///
    /// 仅在 Raft 模式下生效；单节点模式直接返回。
    async fn ensure_linearizable(&self) -> Result<(), tonic::Status> {
        if let Some(ref raft) = self.raft {
            raft.ensure_linearizable(ReadPolicy::ReadIndex)
                .await
                .map_err(|e| {
                    tonic::Status::internal(format!("linearizable read failed: {e}"))
                })?;
        }
        Ok(())
    }
}

// ──── 工具函数 ────

fn to_kv_proto(
    key: &[u8],
    value: &[u8],
    meta: Option<&crate::storage::mvcc::KvMetadata>,
) -> KeyValue {
    match meta {
        Some(m) => KeyValue {
            key: key.to_vec(),
            value: value.to_vec(),
            create_revision: m.create_revision,
            mod_revision: m.mod_revision,
            version: m.version,
            lease_id: m.lease_id,
        },
        None => KeyValue {
            key: key.to_vec(),
            value: value.to_vec(),
            create_revision: 0,
            mod_revision: 0,
            version: 1,
            lease_id: 0,
        },
    }
}

fn map_err<E: std::fmt::Display>(e: E) -> tonic::Status {
    tonic::Status::internal(e.to_string())
}

// ──── KV Service ────

#[tonic::async_trait]
impl Kv for CoordNode {
    async fn put(
        &self,
        request: tonic::Request<PutRequest>,
    ) -> Result<tonic::Response<PutResponse>, tonic::Status> {
        let req = request.into_inner();
        let request_id = req.request_id.clone();

        // 幂等检查：相同 request_id 返回缓存的 revision
        if let Some(cached_rev) = self.check_idempotent(&request_id) {
            return Ok(tonic::Response::new(PutResponse {
                prev_kv: None,
                revision: cached_rev,
            }));
        }

        let lease_id = if req.lease_id != 0 {
            Some(req.lease_id)
        } else {
            None
        };

        // 若请求 prev_kv，在写入前读取当前值
        let prev_kv = if req.prev_kv {
            self.storage
                .get(&req.key)
                .map_err(map_err)?
                .map(|prev_value| {
                    let meta = self
                        .storage
                        .get_kv_metadata(&req.key)
                        .map_err(map_err)?;
                    Ok::<_, tonic::Status>(to_kv_proto(&req.key, &prev_value, meta.as_ref()))
                })
                .transpose()?
        } else {
            None
        };

        // 通过 Raft 共识提交（集群模式），或直接写入存储（单节点模式）
        let revision: u64 = if let Some(ref raft) = self.raft {
            let cmd = Command::Put {
                key: req.key.clone(),
                value: req.value.clone(),
                lease_id,
            };
            let resp = raft.client_write(cmd).await.map_err(|e| {
                tonic::Status::internal(format!("raft write failed: {e}"))
            })?;
            match resp.response() {
                Response::Put { revision } => *revision,
                _ => return Err(tonic::Status::internal("unexpected raft response")),
            }
        } else {
            self.storage.put(&req.key, &req.value, lease_id).map_err(map_err)?
        };

        // 若关联了 Lease，将 Key 绑定到 Lease（用于 Revoke 时自动清理）
        if let Some(lid) = lease_id {
            if let Some(ref lm) = self.lease_manager {
                let _ = lm.attach_key(lid, &req.key);
            }
        }

        // 缓存幂等结果
        self.cache_idempotent(request_id, revision as i64);

        Ok(tonic::Response::new(PutResponse {
            prev_kv,
            revision: revision as i64,
        }))
    }

    async fn range(
        &self,
        request: tonic::Request<RangeRequest>,
    ) -> Result<tonic::Response<RangeResponse>, tonic::Status> {
        let req = request.into_inner();
        let limit = if req.limit > 0 { req.limit as usize } else { usize::MAX };
        let keys_only = req.keys_only;
        let count_only = req.count_only;
        let target_revision = if req.revision > 0 { req.revision as u64 } else { 0 };

        // 线性一致性读：确认 Leader 身份后再读取（ADP §11.2）
        self.ensure_linearizable().await?;

        let mut kvs = Vec::new();

        if target_revision > 0 && req.range_end.is_empty() {
            // 历史快照读：单键查询指定 Revision 时的值
            if let Some(value) = self.storage.get_at_revision(&req.key, target_revision).map_err(map_err)? {
                let meta = self.storage.get_kv_metadata(&req.key).map_err(map_err)?;
                // 历史读取：使用查询 revision 作为 mod_revision
                let kv = if let Some(m) = meta {
                    KeyValue {
                        key: req.key.clone(),
                        value,
                        create_revision: m.create_revision,
                        mod_revision: target_revision as i64,
                        version: m.version,
                        lease_id: m.lease_id,
                    }
                } else {
                    KeyValue {
                        key: req.key.clone(),
                        value,
                        create_revision: target_revision as i64,
                        mod_revision: target_revision as i64,
                        version: 1,
                        lease_id: 0,
                    }
                };
                kvs.push(kv);
            }
        } else if !req.range_end.is_empty() {
            // 范围查询
            let results = self.storage.range(&req.key, limit).map_err(map_err)?;
            for (k, v) in results {
                let meta = self.storage.get_kv_metadata(&k).map_err(map_err)?;
                let kv = to_kv_proto(&k, &v, meta.as_ref());
                kvs.push(kv);
            }
        } else {
            // 单键精确查询（最新值）
            if let Some(value) = self.storage.get(&req.key).map_err(map_err)? {
                let meta = self
                    .storage
                    .get_kv_metadata(&req.key)
                    .map_err(map_err)?;
                let kv = to_kv_proto(&req.key, &value, meta.as_ref());
                kvs.push(kv);
            }
        }

        let count = kvs.len() as i64;
        let revision = if target_revision > 0 {
            target_revision as i64
        } else {
            self.storage.current_revision() as i64
        };

        if count_only {
            // 仅返回计数，不返回 kvs
            Ok(tonic::Response::new(RangeResponse {
                kvs: vec![],
                count,
                revision,
            }))
        } else {
            // 若 keys_only 为 true，清除 value 字段
            if keys_only {
                for kv in &mut kvs {
                    kv.value = Vec::new();
                }
            }

            Ok(tonic::Response::new(RangeResponse {
                kvs,
                count,
                revision,
            }))
        }
    }

    async fn delete(
        &self,
        request: tonic::Request<DeleteRequest>,
    ) -> Result<tonic::Response<DeleteResponse>, tonic::Status> {
        let req = request.into_inner();
        let prev_kv_requested = req.prev_kv;

        // 线性一致性读：确保能看到最新数据后再扫描要删除的 Key
        self.ensure_linearizable().await?;

        // 收集需要删除的 Key 列表
        let keys_to_delete: Vec<Vec<u8>> = if !req.range_end.is_empty() {
            // 范围删除：扫描出所有匹配前缀的 Key
            // 注：range_end 用于标识范围操作，实际匹配由 MvccStorage::range() 的前缀扫描完成
            // （与 range() handler 保持一致的语义）
            let results = self.storage.range(&req.key, usize::MAX).map_err(map_err)?;
            results
                .into_iter()
                .map(|(k, _)| k)
                .collect()
        } else {
            vec![req.key.clone()]
        };

        // 获取 prev_kv（如果需要）
        let prev_kvs: Vec<KeyValue> = if prev_kv_requested {
            keys_to_delete
                .iter()
                .filter_map(|key| {
                    self.storage
                        .get(key)
                        .ok()
                        .flatten()
                        .map(|value| {
                            let meta = self.storage.get_kv_metadata(key).ok().flatten();
                            to_kv_proto(key, &value, meta.as_ref())
                        })
                })
                .collect()
        } else {
            vec![]
        };

        let mut deleted: i64 = 0;
        let mut revision: i64 = 0;

        // 通过 Raft 共识提交（集群模式），或直接写入存储（单节点模式）
        for key in &keys_to_delete {
            // 检查 Key 是否存在（在 Raft 写入前）
            let exists = self.storage.get(key).map_err(map_err)?.is_some();

            if let Some(ref raft) = self.raft {
                let cmd = Command::Delete { key: key.clone() };
                let resp = raft.client_write(cmd).await.map_err(|e| {
                    tonic::Status::internal(format!("raft write failed: {e}"))
                })?;
                match resp.response() {
                    Response::Delete { revision: rev } => {
                        revision = *rev as i64;
                        if exists {
                            deleted += 1;
                        }
                    }
                    _ => {}
                }
            } else {
                if exists {
                    let rev = self.storage.delete(key).map_err(map_err)?;
                    revision = rev as i64;
                    deleted += 1;
                }
            }
        }

        Ok(tonic::Response::new(DeleteResponse {
            deleted,
            prev_kvs,
            revision,
        }))
    }
}

// ──── Txn Service ────

fn convert_compare(c: &Compare) -> Result<TxnCompare, tonic::Status> {
    use coord_proto::txn::compare::{CompareResult, Target};
    use crate::txn::{CompareOp, CompareTarget, CompareValue};

    let op = match CompareResult::try_from(c.result) {
        Ok(CompareResult::Equal) => CompareOp::Equal,
        Ok(CompareResult::Greater) => CompareOp::Greater,
        Ok(CompareResult::Less) => CompareOp::Less,
        Ok(CompareResult::NotEqual) => CompareOp::NotEqual,
        Err(_) => return Err(tonic::Status::invalid_argument("unknown compare result")),
    };

    let target = match Target::try_from(c.target) {
        Ok(Target::Version) => CompareTarget::Version,
        Ok(Target::Value) => CompareTarget::Value,
        Ok(Target::ModRev) => CompareTarget::ModRevision,
        Err(_) => return Err(tonic::Status::invalid_argument("unknown compare target")),
    };

    let target_value = match c.target_value.as_ref() {
        Some(coord_proto::txn::compare::TargetValue::Version(v)) => CompareValue::Version(*v),
        Some(coord_proto::txn::compare::TargetValue::Value(v)) => CompareValue::Value(v.clone()),
        Some(coord_proto::txn::compare::TargetValue::ModRevision(v)) => {
            CompareValue::ModRevision(*v)
        }
        None => return Err(tonic::Status::invalid_argument("missing target value")),
    };

    Ok(TxnCompare {
        key: c.key.clone(),
        op,
        target,
        target_value,
    })
}

fn convert_request_op(op: &RequestOp) -> Result<TxnOp, tonic::Status> {
    match op.op.as_ref() {
        Some(coord_proto::txn::request_op::Op::RequestPut(p)) => Ok(TxnOp::Put {
            key: p.key.clone(),
            value: p.value.clone(),
            lease_id: if p.lease_id != 0 {
                Some(p.lease_id)
            } else {
                None
            },
        }),
        Some(coord_proto::txn::request_op::Op::RequestDelete(d)) => Ok(TxnOp::Delete {
            key: d.key.clone(),
        }),
        Some(coord_proto::txn::request_op::Op::RequestRange(r)) => Ok(TxnOp::Range {
            key: r.key.clone(),
            range_end: r.range_end.clone(),
            limit: r.limit,
        }),
        None => Err(tonic::Status::invalid_argument("empty request op")),
    }
}

fn convert_response_op(resp: &TxnOpResponse) -> ResponseOp {
    match resp {
        TxnOpResponse::Put { revision } => ResponseOp {
            op: Some(coord_proto::txn::response_op::Op::ResponsePut(
                PutResponse {
                    prev_kv: None,
                    revision: *revision as i64,
                },
            )),
        },
        TxnOpResponse::Delete { revision } => ResponseOp {
            op: Some(coord_proto::txn::response_op::Op::ResponseDelete(
                DeleteResponse {
                    deleted: 1,
                    prev_kvs: vec![],
                    revision: *revision as i64,
                },
            )),
        },
        TxnOpResponse::Range {
            kvs,
            count,
            revision,
        } => {
            let proto_kvs: Vec<KeyValue> = kvs
                .iter()
                .map(|(k, v)| to_kv_proto(k, v, None))
                .collect();
            ResponseOp {
                op: Some(coord_proto::txn::response_op::Op::ResponseRange(
                    RangeResponse {
                        kvs: proto_kvs,
                        count: *count,
                        revision: *revision as i64,
                    },
                )),
            }
        }
    }
}

#[tonic::async_trait]
impl Txn for CoordNode {
    async fn txn(
        &self,
        request: tonic::Request<TxnRequest>,
    ) -> Result<tonic::Response<TxnResponse>, tonic::Status> {
        let req = request.into_inner();
        let request_id = req.request_id.clone();

        // 幂等检查：相同 request_id 返回缓存的结果
        if let Some((cached_succeeded, cached_rev)) = self.check_idempotent_txn(&request_id) {
            return Ok(tonic::Response::new(TxnResponse {
                succeeded: cached_succeeded,
                responses: vec![],
                revision: cached_rev,
            }));
        }

        let compares: Vec<TxnCompare> = req
            .compare
            .iter()
            .map(convert_compare)
            .collect::<Result<Vec<_>, _>>()?;

        let success_ops: Vec<TxnOp> = req
            .success
            .iter()
            .map(convert_request_op)
            .collect::<Result<Vec<_>, _>>()?;

        let failure_ops: Vec<TxnOp> = req
            .failure
            .iter()
            .map(convert_request_op)
            .collect::<Result<Vec<_>, _>>()?;

        // 通过 Raft 共识提交（集群模式），或直接执行（单节点模式）
        let result = if let Some(ref raft) = self.raft {
            let cmd = Command::Txn {
                compares: compares.clone(),
                success_ops: success_ops.clone(),
                failure_ops: failure_ops.clone(),
            };
            let resp = raft.client_write(cmd).await.map_err(|e| {
                tonic::Status::internal(format!("raft txn failed: {e}"))
            })?;
            match resp.response() {
                Response::Txn { succeeded, revision, responses } => {
                    crate::txn::TxnResult { succeeded: *succeeded, revision: *revision, responses: responses.to_vec() }
                }
                _ => return Err(tonic::Status::internal("unexpected raft response")),
            }
        } else {
            self.storage
                .execute_txn(&compares, &success_ops, &failure_ops)
                .map_err(map_err)?
        };

        let responses: Vec<ResponseOp> = result.responses.iter().map(convert_response_op).collect();

        // 若 Txn 成功执行，将 success_ops 中所有带 lease_id 的 Put 操作的 key 绑定到对应 Lease
        // （与 Put handler 保持一致的 Lease-Key 绑定语义，确保 Revoke/Expiry 时能正确清理）
        if result.succeeded {
            if let Some(ref lm) = self.lease_manager {
                for op in &success_ops {
                    if let TxnOp::Put {
                        key,
                        lease_id: Some(lid),
                        ..
                    } = op
                    {
                        let _ = lm.attach_key(*lid, key);
                    }
                }
            }
        }

        // 缓存幂等结果
        self.cache_idempotent_txn(request_id, result.succeeded, result.revision as i64);

        Ok(tonic::Response::new(TxnResponse {
            succeeded: result.succeeded,
            responses,
            revision: result.revision as i64,
        }))
    }
}

// ──── Lease Service ────

#[tonic::async_trait]
impl Lease for CoordNode {
    type LeaseKeepAliveStream =
        tokio_stream::wrappers::ReceiverStream<Result<LeaseKeepAliveResponse, tonic::Status>>;

    async fn lease_grant(
        &self,
        request: tonic::Request<LeaseGrantRequest>,
    ) -> Result<tonic::Response<LeaseGrantResponse>, tonic::Status> {
        let req = request.into_inner();
        let lease_mgr = self
            .lease_manager
            .as_ref()
            .ok_or_else(|| tonic::Status::unavailable("lease manager not available"))?;

        let id = lease_mgr.grant_with_id(req.ttl, req.id).await.map_err(map_err)?;

        Ok(tonic::Response::new(LeaseGrantResponse {
            id,
            ttl: req.ttl,
            error: String::new(),
        }))
    }

    async fn lease_revoke(
        &self,
        request: tonic::Request<LeaseRevokeRequest>,
    ) -> Result<tonic::Response<LeaseRevokeResponse>, tonic::Status> {
        let req = request.into_inner();
        let lease_mgr = self
            .lease_manager
            .as_ref()
            .ok_or_else(|| tonic::Status::unavailable("lease manager not available"))?;

        // 删除所有绑定到该 Lease 的 Key（扫描 KV_META 表）
        let _deleted = self.storage.delete_keys_by_lease(req.id).map_err(map_err)?;

        // 也通过 LeaseManager 的 attach_key 机制清理（双保险）
        let attached_keys = lease_mgr.take_attached_keys(req.id);
        for key in &attached_keys {
            let _ = self.storage.delete(key);
        }

        // Revoke Lease
        lease_mgr.revoke(req.id).await.map_err(map_err)?;

        Ok(tonic::Response::new(LeaseRevokeResponse {}))
    }

    async fn lease_keep_alive(
        &self,
        request: tonic::Request<tonic::Streaming<LeaseKeepAliveRequest>>,
    ) -> Result<tonic::Response<Self::LeaseKeepAliveStream>, tonic::Status> {
        let lease_mgr = self
            .lease_manager
            .as_ref()
            .ok_or_else(|| tonic::Status::unavailable("lease manager not available"))?;
        let lease_mgr = Arc::clone(lease_mgr);

        let mut stream = request.into_inner();
        let (tx, rx) = mpsc::channel::<Result<LeaseKeepAliveResponse, tonic::Status>>(16);

        // 后台任务：持续接收客户端的 KeepAlive 请求并续约
        tokio::spawn(async move {
            while let Ok(Some(req)) = stream.message().await {
                match lease_mgr.keep_alive(req.id).await {
                    Ok((id, ttl)) => {
                        let resp = LeaseKeepAliveResponse { id, ttl };
                        if tx.send(Ok(resp)).await.is_err() {
                            // 客户端已断开连接，停止处理
                            break;
                        }
                    }
                    Err(e) => {
                        let status = tonic::Status::not_found(format!("keep-alive failed: {e}"));
                        let _ = tx.send(Err(status)).await;
                        break;
                    }
                }
            }
        });

        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }
}

// ──── Watch Service ────

#[tonic::async_trait]
impl Watch for CoordNode {
    type WatchStream =
        tokio_stream::wrappers::ReceiverStream<Result<WatchResponse, tonic::Status>>;

    async fn watch(
        &self,
        request: tonic::Request<tonic::Streaming<WatchRequest>>,
    ) -> Result<tonic::Response<Self::WatchStream>, tonic::Status> {
        let dispatcher = self
            .watch_dispatcher
            .as_ref()
            .ok_or_else(|| tonic::Status::unavailable("watch not available"))?;

        let mut stream = request.into_inner();
        let first_req = stream
            .message()
            .await
            .map_err(|e| tonic::Status::internal(format!("watch stream error: {e}")))?
            .ok_or_else(|| tonic::Status::invalid_argument("empty watch request"))?;

        let create_req = match first_req.request {
            Some(coord_proto::watch::watch_request::Request::Create(c)) => c,
            None => {
                return Err(tonic::Status::invalid_argument(
                    "first watch request must be create",
                ))
            }
        };

        let watch_req = crate::watch::WatchRequest {
            key: create_req.key.clone(),
            range_end: create_req.range_end.clone(),
            start_revision: create_req.start_revision as u64,
        };

        let (watch_id, mut event_rx) = dispatcher.subscribe(watch_req, 1024).await;

        let (tx, rx) = mpsc::channel::<Result<WatchResponse, tonic::Status>>(16);

        let dispatcher_ref = Arc::clone(dispatcher);
        let storage_ref = Arc::clone(&self.storage);
        let start_rev = create_req.start_revision as u64;
        let key_prefix = create_req.key;
        let range_end = create_req.range_end;

        tokio::spawn(async move {
            // 如果指定了 start_revision > 0，先回放历史事件
            if start_rev > 0 {
                let dispatcher_for_replay = Arc::clone(&dispatcher_ref);
                let (history_tx, mut history_rx) = mpsc::channel::<crate::watch::WatchEvent>(256);

                let key_p = key_prefix.clone();
                let range_e = range_end.clone();
                let reader: Arc<dyn crate::watch::ChangelogReader> = storage_ref;
                let history_tx_for_closure = history_tx.clone();

                let replay_result = tokio::task::spawn_blocking(move || {
                    dispatcher_for_replay.replay_history(
                        watch_id,
                        &history_tx_for_closure,
                        &key_p,
                        &range_e,
                        start_rev,
                        reader.as_ref(),
                    )
                })
                .await;

                match replay_result {
                    Ok(Ok(())) => {
                        // 回放成功：drain 历史事件并发送
                        drop(history_tx); // 关闭 sender 使 receiver 可以终止
                        while let Some(event) = history_rx.recv().await {
                            if let Some(resp) = convert_watch_event_to_response(watch_id, &event) {
                                if tx.send(Ok(resp)).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        tracing::warn!(watch_id, "watch history replay failed: {e}");
                        // 发送 HistoryUnavailable 事件通知客户端
                        let resp = WatchResponse {
                            watch_id: watch_id as i64,
                            events: vec![WatchEvent {
                                r#type: coord_proto::watch::watch_event::EventType::HistoryUnavailable as i32,
                                kvs: vec![],
                                prev_kv: None,
                                revision: 0,
                            }],
                        };
                        let _ = tx.send(Ok(resp)).await;
                    }
                    Err(e) => {
                        tracing::warn!(watch_id, "spawn_blocking for watch replay panicked: {e}");
                    }
                }
            }

            // 实时事件循环
            loop {
                match event_rx.recv().await {
                    Some(event) => {
                        if let Some(resp) = convert_watch_event_to_response(watch_id, &event) {
                            if tx.send(Ok(resp)).await.is_err() {
                                break;
                            }
                        }
                    }
                    None => break,
                }
            }
            dispatcher_ref.unsubscribe(watch_id);
        });

        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }
}

/// 将内部 WatchEvent 转换为 protobuf WatchResponse
fn convert_watch_event_to_response(
    watch_id: u64,
    event: &crate::watch::WatchEvent,
) -> Option<WatchResponse> {
    let proto_events: Vec<WatchEvent> = event
        .events
        .iter()
        .map(|item| {
            let kvs: Vec<KeyValue> = item
                .kvs
                .iter()
                .map(|kv| KeyValue {
                    key: kv.key.clone(),
                    value: kv.value.clone().unwrap_or_default(),
                    create_revision: 0,
                    mod_revision: item.revision as i64,
                    version: 1,
                    lease_id: 0,
                })
                .collect();

            let event_type = match item.event_type {
                crate::watch::WatchEventType::Put => {
                    coord_proto::watch::watch_event::EventType::Put
                }
                crate::watch::WatchEventType::Delete => {
                    coord_proto::watch::watch_event::EventType::Delete
                }
                crate::watch::WatchEventType::BufferOverflow => {
                    coord_proto::watch::watch_event::EventType::BufferOverflow
                }
                crate::watch::WatchEventType::HistoryUnavailable => {
                    coord_proto::watch::watch_event::EventType::HistoryUnavailable
                }
            };

            WatchEvent {
                r#type: event_type as i32,
                kvs,
                prev_kv: None,
                revision: item.revision as i64,
            }
        })
        .collect();

    Some(WatchResponse {
        watch_id: watch_id as i64,
        events: proto_events,
    })
}

// ──── Maintenance Service ────

#[tonic::async_trait]
impl Maintenance for CoordNode {
    type SnapshotStream =
        tokio_stream::wrappers::ReceiverStream<Result<SnapshotResponse, tonic::Status>>;

    async fn seal(
        &self,
        _request: tonic::Request<SealRequest>,
    ) -> Result<tonic::Response<SealResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("seal not yet implemented"))
    }

    async fn unseal(
        &self,
        _request: tonic::Request<UnsealRequest>,
    ) -> Result<tonic::Response<UnsealResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unseal not yet implemented"))
    }

    async fn status(
        &self,
        _request: tonic::Request<StatusRequest>,
    ) -> Result<tonic::Response<StatusResponse>, tonic::Status> {
        let revision = self.storage.current_revision();

        let (raft_index, raft_term, raft_leader) = if let Some(ref raft) = self.raft {
            let m = raft.metrics().borrow_watched().clone();
            let leader = raft.current_leader().await;
            (
                m.last_applied.as_ref().map(|id| id.index).unwrap_or(0) as i64,
                m.current_term,
                leader.map(|id| id.to_string()).unwrap_or_default(),
            )
        } else {
            (0i64, 0u64, String::new())
        };

        Ok(tonic::Response::new(StatusResponse {
            revision: revision as i64,
            raft_index,
            raft_term,
            raft_leader,
            seal_status: String::from("unsealed"),
        }))
    }

    async fn snapshot(
        &self,
        _request: tonic::Request<SnapshotRequest>,
    ) -> Result<tonic::Response<Self::SnapshotStream>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "snapshot streaming not yet implemented",
        ))
    }

    // ──── Member Management ────

    async fn member_add(
        &self,
        request: tonic::Request<MemberAddRequest>,
    ) -> Result<tonic::Response<MemberAddResponse>, tonic::Status> {
        let req = request.into_inner();
        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| tonic::Status::failed_precondition("not a raft node"))?;

        // Step 1: Add as learner
        let node = openraft::impls::BasicNode::new(&req.raft_addr);
        raft.add_learner(req.node_id, node, true)
            .await
            .map_err(|e| tonic::Status::internal(format!("add_learner failed: {e}")))?;

        // Step 2: Promote to voter
        let mut voter_ids = std::collections::BTreeSet::new();
        voter_ids.insert(req.node_id);
        raft.change_membership(openraft::ChangeMembers::AddVoterIds(voter_ids), true)
            .await
            .map_err(|e| tonic::Status::internal(format!("change_membership failed: {e}")))?;

        Ok(tonic::Response::new(MemberAddResponse {
            success: true,
            message: format!(
                "node {} added as voter (grpc={}, raft={})",
                req.node_id, req.grpc_addr, req.raft_addr
            ),
        }))
    }

    async fn member_remove(
        &self,
        request: tonic::Request<MemberRemoveRequest>,
    ) -> Result<tonic::Response<MemberRemoveResponse>, tonic::Status> {
        let req = request.into_inner();
        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| tonic::Status::failed_precondition("not a raft node"))?;

        let mut remove_ids = std::collections::BTreeSet::new();
        remove_ids.insert(req.node_id);
        raft.change_membership(openraft::ChangeMembers::RemoveVoters(remove_ids), true)
            .await
            .map_err(|e| tonic::Status::internal(format!("change_membership failed: {e}")))?;

        Ok(tonic::Response::new(MemberRemoveResponse {
            success: true,
            message: format!("node {} removed from cluster", req.node_id),
        }))
    }

    async fn member_promote(
        &self,
        request: tonic::Request<MemberPromoteRequest>,
    ) -> Result<tonic::Response<MemberPromoteResponse>, tonic::Status> {
        let req = request.into_inner();
        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| tonic::Status::failed_precondition("not a raft node"))?;

        let mut voter_ids = std::collections::BTreeSet::new();
        voter_ids.insert(req.node_id);
        raft.change_membership(openraft::ChangeMembers::AddVoterIds(voter_ids), true)
            .await
            .map_err(|e| tonic::Status::internal(format!("change_membership failed: {e}")))?;

        Ok(tonic::Response::new(MemberPromoteResponse {
            success: true,
            message: format!("node {} promoted to voter", req.node_id),
        }))
    }

    async fn member_list(
        &self,
        _request: tonic::Request<MemberListRequest>,
    ) -> Result<tonic::Response<MemberListResponse>, tonic::Status> {
        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| tonic::Status::failed_precondition("not a raft node"))?;

        let m = raft.metrics().borrow_watched().clone();
        let leader_id = raft.current_leader().await;

        // Build member list from membership config
        let mut nodes = Vec::new();
        let membership = &m.membership_config;

        // Collect voter IDs for role classification
        let voter_ids: std::collections::BTreeSet<u64> = membership.voter_ids().collect();

        // Iterate over all nodes (voters + learners)
        for (id, _node) in membership.nodes() {
            let role = if voter_ids.contains(id) {
                if leader_id == Some(*id) {
                    "Leader"
                } else {
                    "Voter"
                }
            } else {
                "Learner"
            };
            nodes.push(MemberNode {
                id: *id,
                role: role.to_string(),
            });
        }

        Ok(tonic::Response::new(MemberListResponse {
            nodes,
            leader_id: leader_id.unwrap_or(0),
        }))
    }
}

// ──── 测试 ────

#[cfg(test)]
mod tests {
    use super::*;

    // ──── to_kv_proto ────

    #[test]
    fn test_to_kv_proto_basic() {
        use crate::storage::mvcc::KvMetadata;
        let meta = KvMetadata {
            version: 3,
            create_revision: 10,
            mod_revision: 42,
            lease_id: 0,
            deleted: false,
        };
        let kv = to_kv_proto(b"hello", b"world", Some(&meta));
        assert_eq!(kv.key, b"hello");
        assert_eq!(kv.value, b"world");
        assert_eq!(kv.create_revision, 10);
        assert_eq!(kv.mod_revision, 42);
        assert_eq!(kv.version, 3);
        assert_eq!(kv.lease_id, 0);
    }

    #[test]
    fn test_to_kv_proto_empty_value() {
        let kv = to_kv_proto(b"empty", b"", None);
        assert_eq!(kv.key, b"empty");
        assert!(kv.value.is_empty());
        assert_eq!(kv.create_revision, 0);
    }

    #[test]
    fn test_to_kv_proto_no_metadata() {
        let kv = to_kv_proto(b"k", b"v", None);
        assert_eq!(kv.version, 1);
        assert_eq!(kv.create_revision, 0);
        assert_eq!(kv.mod_revision, 0);
    }

    // ──── map_err ────

    #[test]
    fn test_map_err_returns_internal_status() {
        let status = map_err("test error message");
        assert_eq!(status.code(), tonic::Code::Internal);
        assert!(status.message().contains("test error message"));
    }

    #[test]
    fn test_map_err_with_display_type() {
        let err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let status = map_err(err);
        assert_eq!(status.code(), tonic::Code::Internal);
        assert!(status.message().contains("file not found"));
    }

    // ──── convert_compare ────

    #[test]
    fn test_convert_compare_equal_version() {
        let c = coord_proto::txn::Compare {
            key: b"mykey".to_vec(),
            result: coord_proto::txn::compare::CompareResult::Equal as i32,
            target: coord_proto::txn::compare::Target::Version as i32,
            target_value: Some(coord_proto::txn::compare::TargetValue::Version(5)),
        };
        let result = convert_compare(&c).unwrap();
        assert_eq!(result.key, b"mykey");
        assert!(matches!(result.op, crate::txn::CompareOp::Equal));
        assert!(matches!(result.target, crate::txn::CompareTarget::Version));
    }

    #[test]
    fn test_convert_compare_greater_value() {
        let c = coord_proto::txn::Compare {
            key: b"k".to_vec(),
            result: coord_proto::txn::compare::CompareResult::Greater as i32,
            target: coord_proto::txn::compare::Target::Value as i32,
            target_value: Some(coord_proto::txn::compare::TargetValue::Value(b"val".to_vec())),
        };
        let result = convert_compare(&c).unwrap();
        assert!(matches!(result.op, crate::txn::CompareOp::Greater));
        assert!(matches!(result.target, crate::txn::CompareTarget::Value));
    }

    #[test]
    fn test_convert_compare_less_mod_revision() {
        let c = coord_proto::txn::Compare {
            key: b"k".to_vec(),
            result: coord_proto::txn::compare::CompareResult::Less as i32,
            target: coord_proto::txn::compare::Target::ModRev as i32,
            target_value: Some(coord_proto::txn::compare::TargetValue::ModRevision(10)),
        };
        let result = convert_compare(&c).unwrap();
        assert!(matches!(result.op, crate::txn::CompareOp::Less));
        assert!(matches!(result.target, crate::txn::CompareTarget::ModRevision));
    }

    #[test]
    fn test_convert_compare_not_equal() {
        let c = coord_proto::txn::Compare {
            key: b"k".to_vec(),
            result: coord_proto::txn::compare::CompareResult::NotEqual as i32,
            target: coord_proto::txn::compare::Target::Version as i32,
            target_value: Some(coord_proto::txn::compare::TargetValue::Version(3)),
        };
        let result = convert_compare(&c).unwrap();
        assert!(matches!(result.op, crate::txn::CompareOp::NotEqual));
    }

    #[test]
    fn test_convert_compare_missing_target_value() {
        let c = coord_proto::txn::Compare {
            key: b"k".to_vec(),
            result: coord_proto::txn::compare::CompareResult::Equal as i32,
            target: coord_proto::txn::compare::Target::Version as i32,
            target_value: None,
        };
        let result = convert_compare(&c);
        assert!(result.is_err());
    }

    // ──── convert_request_op ────

    #[test]
    fn test_convert_request_op_put() {
        let op = coord_proto::txn::RequestOp {
            op: Some(coord_proto::txn::request_op::Op::RequestPut(
                coord_proto::kv::PutRequest {
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                    lease_id: 0,
                    prev_kv: false,
                    request_id: vec![],
                },
            )),
        };
        let result = convert_request_op(&op).unwrap();
        match result {
            crate::txn::TxnOp::Put { key, value, lease_id } => {
                assert_eq!(key, b"k");
                assert_eq!(value, b"v");
                assert_eq!(lease_id, None);
            }
            _ => panic!("expected Put"),
        }
    }

    #[test]
    fn test_convert_request_op_delete() {
        let op = coord_proto::txn::RequestOp {
            op: Some(coord_proto::txn::request_op::Op::RequestDelete(
                coord_proto::kv::DeleteRequest {
                    key: b"del".to_vec(),
                    range_end: vec![],
                    prev_kv: false,
                    request_id: vec![],
                },
            )),
        };
        let result = convert_request_op(&op).unwrap();
        match result {
            crate::txn::TxnOp::Delete { key } => assert_eq!(key, b"del"),
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn test_convert_request_op_range() {
        let op = coord_proto::txn::RequestOp {
            op: Some(coord_proto::txn::request_op::Op::RequestRange(
                coord_proto::kv::RangeRequest {
                    key: b"prefix".to_vec(),
                    range_end: b"prefixz".to_vec(),
                    limit: 100,
                    revision: 0,
                    keys_only: false,
                    count_only: false,
                },
            )),
        };
        let result = convert_request_op(&op).unwrap();
        match result {
            crate::txn::TxnOp::Range { key, range_end, limit } => {
                assert_eq!(key, b"prefix");
                assert_eq!(range_end, b"prefixz");
                assert_eq!(limit, 100);
            }
            _ => panic!("expected Range"),
        }
    }

    #[test]
    fn test_convert_request_op_empty() {
        let op = coord_proto::txn::RequestOp { op: None };
        let result = convert_request_op(&op);
        assert!(result.is_err());
    }

    // ──── convert_response_op ────

    #[test]
    fn test_convert_response_op_put() {
        let resp = crate::txn::TxnOpResponse::Put { revision: 42 };
        let proto = convert_response_op(&resp);
        match proto.op {
            Some(coord_proto::txn::response_op::Op::ResponsePut(p)) => {
                assert_eq!(p.revision, 42);
            }
            _ => panic!("expected ResponsePut"),
        }
    }

    #[test]
    fn test_convert_response_op_delete() {
        let resp = crate::txn::TxnOpResponse::Delete { revision: 7 };
        let proto = convert_response_op(&resp);
        match proto.op {
            Some(coord_proto::txn::response_op::Op::ResponseDelete(d)) => {
                assert_eq!(d.revision, 7);
                assert_eq!(d.deleted, 1);
            }
            _ => panic!("expected ResponseDelete"),
        }
    }

    #[test]
    fn test_convert_response_op_range() {
        let resp = crate::txn::TxnOpResponse::Range {
            kvs: vec![(b"k".to_vec(), b"v".to_vec())],
            count: 1,
            revision: 10,
        };
        let proto = convert_response_op(&resp);
        match proto.op {
            Some(coord_proto::txn::response_op::Op::ResponseRange(r)) => {
                assert_eq!(r.kvs.len(), 1);
                assert_eq!(r.count, 1);
                assert_eq!(r.revision, 10);
            }
            _ => panic!("expected ResponseRange"),
        }
    }

    // ──── CoordNode type verification ────

    /// CoordNode fields are pub for external access. This test verifies
    /// the struct definition compiles (compile-time assertion).
    #[test]
    fn test_coord_node_type_accessible() {
        // Verify helper functions are callable (compile-time check)
        let kv = to_kv_proto(b"k", b"v", None);
        assert_eq!(kv.key, b"k");

        let err = map_err("ok");
        assert_eq!(err.code(), tonic::Code::Internal);
    }
}
