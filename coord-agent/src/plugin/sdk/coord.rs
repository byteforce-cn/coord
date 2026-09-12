// coord-agent: 宿主导入 SDK 的真实后端（coord-client → coord-server）
//
// 职责：
// - 把 SDK 的 typed 请求翻译为 coord-client 调用；
// - 把 `coord_core::Error` 归一化为 [`SdkError`]（§7.3 错误映射）；
// - 管理插件的后台 Lease 保活句柄（插件停止时按插件回收）。
//
// 幂等 / 重试 / Leader 发现全部下沉到 coord-client（插件不感知集群拓扑）。

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::mpsc;

use coord_client::client::{LeaseKeeper, ObjectReader, ObjectWriter};
use coord_client::Client;
use coord_core::error::Error as CoreError;

use crate::plugin::identity::{PluginClientSource, SharedPluginClients};
use crate::plugin::sdk::backend::{
    Compare, CompareOp, CompareTarget, KvDelete, KvDeleteOut, KvPut, KvPutOut, KvRange, KvRangeOut,
    KvRecord, ObjectGetOut, ObjectPutOut, ObjectStatDto, PluginSdkBackend, SdkError, SdkErrorCode,
    SdkResult, TxnOp, TxnOpOut, TxnOut, TxnReq, WatchEventDto, WatchEventKind, WatchSubscribe,
};

// ──── 错误映射 ────

/// `coord_core::Error` → [`SdkError`]（§7.3）。
pub fn map_core_error(e: CoreError) -> SdkError {
    let code = match &e {
        CoreError::NotFound { .. } | CoreError::LeaseNotFound { .. } => SdkErrorCode::NotFound,
        CoreError::NotLeader { .. } | CoreError::NotLeaderNoHint => SdkErrorCode::Unavailable,
        CoreError::ClusterUnavailable(_) | CoreError::RequestTimeout => SdkErrorCode::Unavailable,
        CoreError::PermissionDenied(_) | CoreError::Unauthenticated(_) => SdkErrorCode::Forbidden,
        CoreError::Backpressure(_) | CoreError::WatchTooManyConnections { .. } => {
            SdkErrorCode::ResourceExhausted
        }
        CoreError::TxnTooLarge { .. } => SdkErrorCode::ResourceExhausted,
        CoreError::InvalidArgument(_) | CoreError::LeaseTTLOutOfRange { .. } => {
            SdkErrorCode::InvalidArgument
        }
        _ => SdkErrorCode::Internal,
    };
    SdkError::new(code, e.to_string())
}

// ──── 后端 ────

/// Watch 事件接收器（订阅句柄持有的内部类型）。
type WatchReceiver = Arc<
    tokio::sync::Mutex<mpsc::Receiver<coord_core::error::Result<coord_proto::watch::WatchEvent>>>,
>;

/// 上传会话（`Option` = 已提交/已中止，占位以免句柄被复用）。
type UploadSlot = Arc<tokio::sync::Mutex<Option<ObjectWriter>>>;

/// 下载会话：底层 [`ObjectReader`] + 因 `max_len` 截断而暂存的尾巴。
struct ReadSession {
    reader: ObjectReader,
    /// 上一次读取超出 `max_len` 的部分（下次优先返回；保证单次返回 ≤ `max_len`）。
    pending: Vec<u8>,
}
/// 下载会话槽位（每会话独立锁：允许同一插件的多个下载会话并发读取）。
type ReadSlot = Arc<tokio::sync::Mutex<ReadSession>>;
/// 基于 coord-client 的 SDK 后端。
///
/// 出站客户端按**插件身份**解析（D5）：
/// - 开通了插件账户 → 使用带受限 CCT 的专属连接；
/// - 未开通（明文开发 / 开通失败）→ 回退 agent 共享连接。
pub struct CoordSdkBackend {
    clients: Arc<dyn PluginClientSource>,
    /// 后台保活句柄：`(plugin, lease_id) → LeaseKeeper`
    keepers: Mutex<HashMap<(String, i64), LeaseKeeper>>,
    /// 活跃 Watch 订阅：`(plugin, sub_id) → 事件接收器`
    watches: Mutex<HashMap<(String, u64), WatchReceiver>>,
    /// 订阅句柄分配器
    next_watch_id: AtomicU64,
    /// 活跃**上传会话**：`(plugin, session_id) → ObjectWriter`
    uploads: Mutex<HashMap<(String, u64), UploadSlot>>,
    /// 活跃**下载会话**：`(plugin, session_id) → ReadSession`
    reads: Mutex<HashMap<(String, u64), ReadSlot>>,
    /// 会话句柄分配器（上传/下载共用一个 id 空间，避免两条路径撞车）
    next_session_id: AtomicU64,
}

impl CoordSdkBackend {
    /// 由单一共享 coord-client 构建（未启用插件身份时的默认路径）。
    pub fn new(client: Client) -> Self {
        Self::with_source(Arc::new(SharedPluginClients::new(client)))
    }

    /// 由插件客户端来源构建（启用插件身份时用 `PluginIdentityManager::client_source`）。
    pub fn with_source(clients: Arc<dyn PluginClientSource>) -> Self {
        Self {
            clients,
            keepers: Mutex::new(HashMap::new()),
            watches: Mutex::new(HashMap::new()),
            next_watch_id: AtomicU64::new(1),
            uploads: Mutex::new(HashMap::new()),
            reads: Mutex::new(HashMap::new()),
            next_session_id: AtomicU64::new(1),
        }
    }

    /// 解析插件出站客户端。
    fn client(&self, plugin: &str) -> Client {
        self.clients.client_for(plugin)
    }

    /// 后台保活句柄数（测试 / 诊断）。
    pub fn keeper_count(&self) -> usize {
        self.keepers.lock().len()
    }

    /// 活跃 Watch 订阅数（测试 / 诊断）。
    pub fn watch_count(&self) -> usize {
        self.watches.lock().len()
    }

    /// 活跃对象存储会话数（上传 + 下载；测试 / 诊断）。
    pub fn storage_session_count(&self) -> usize {
        self.uploads.lock().len() + self.reads.lock().len()
    }

    /// 取上传会话（不存在 → `not-found`）。
    fn upload_session(&self, plugin: &str, id: u64) -> SdkResult<UploadSlot> {
        self.uploads
            .lock()
            .get(&(plugin.to_string(), id))
            .cloned()
            .ok_or_else(|| SdkError::not_found(format!("storage upload session {id} not found")))
    }

    /// 摘除上传会话（提交 / 中止用；不存在 → `not-found`）。
    fn take_upload_session(&self, plugin: &str, id: u64) -> SdkResult<UploadSlot> {
        self.uploads
            .lock()
            .remove(&(plugin.to_string(), id))
            .ok_or_else(|| SdkError::not_found(format!("storage upload session {id} not found")))
    }

    /// 取下载会话（不存在 → `not-found`）。
    fn read_session(&self, plugin: &str, id: u64) -> SdkResult<ReadSlot> {
        self.reads
            .lock()
            .get(&(plugin.to_string(), id))
            .cloned()
            .ok_or_else(|| SdkError::not_found(format!("storage download session {id} not found")))
    }

    /// 单键预读（`prev_kv` 语义；与 agent KV 代理一致，非原子）。
    async fn read_one(&self, plugin: &str, key: &[u8]) -> SdkResult<Option<KvRecord>> {
        let (kvs, _count, _rev) = self
            .client(plugin)
            .kv()
            .range_with_lease_full(key, &[], 1, 0, false, false)
            .await
            .map_err(map_core_error)?;
        Ok(kvs.into_iter().next().map(record_from))
    }
}

/// Watch 事件 → DTO。
fn watch_event_to_dto(event: coord_proto::watch::WatchEvent) -> WatchEventDto {
    use coord_proto::watch::watch_event::EventType;
    let kind = match EventType::try_from(event.r#type).unwrap_or(EventType::Put) {
        EventType::Put => WatchEventKind::Put,
        EventType::Delete => WatchEventKind::Delete,
        EventType::BufferOverflow => WatchEventKind::BufferOverflow,
        EventType::HistoryUnavailable => WatchEventKind::HistoryUnavailable,
    };
    WatchEventDto {
        kind,
        kvs: event.kvs.into_iter().map(proto_kv_to_record).collect(),
        prev_kv: event.prev_kv.map(proto_kv_to_record),
        revision: event.revision,
    }
}

/// `(key, value, lease_id, version)` → [`KvRecord`]
fn record_from((key, value, lease_id, version): (Vec<u8>, Vec<u8>, i64, i64)) -> KvRecord {
    KvRecord {
        key,
        value,
        lease_id,
        version,
    }
}

/// DTO `Compare` → proto `Compare`
fn to_proto_compare(c: &Compare) -> coord_proto::txn::Compare {
    use coord_proto::txn::compare::{CompareResult, Target, TargetValue};
    let result = match c.op {
        CompareOp::Equal => CompareResult::Equal,
        CompareOp::Greater => CompareResult::Greater,
        CompareOp::Less => CompareResult::Less,
        CompareOp::NotEqual => CompareResult::NotEqual,
    };
    let target = match c.target {
        CompareTarget::Version => Target::Version,
        CompareTarget::Value => Target::Value,
        CompareTarget::ModRevision => Target::ModRev,
    };
    let target_value = match c.target {
        CompareTarget::Value => Some(TargetValue::Value(c.bytes_value.clone())),
        CompareTarget::Version => Some(TargetValue::Version(c.int_value)),
        CompareTarget::ModRevision => Some(TargetValue::ModRevision(c.int_value)),
    };
    coord_proto::txn::Compare {
        result: result as i32,
        target: target as i32,
        key: c.key.clone(),
        target_value,
    }
}

/// DTO `TxnOp` → proto `RequestOp`
fn to_proto_op(op: &TxnOp) -> coord_proto::txn::RequestOp {
    use coord_proto::txn::request_op::Op;
    let inner = match op {
        TxnOp::Put(p) => Op::RequestPut(coord_proto::kv::PutRequest {
            key: p.key.clone(),
            value: p.value.clone(),
            lease_id: p.lease_id,
            prev_kv: p.prev_kv,
            request_id: p.request_id.clone(),
        }),
        TxnOp::Range(r) => Op::RequestRange(coord_proto::kv::RangeRequest {
            key: r.key.clone(),
            range_end: r.range_end.clone(),
            limit: r.limit,
            revision: r.revision,
            keys_only: r.keys_only,
            count_only: r.count_only,
        }),
        TxnOp::Delete(d) => Op::RequestDelete(coord_proto::kv::DeleteRequest {
            key: d.key.clone(),
            range_end: d.range_end.clone(),
            prev_kv: d.prev_kv,
            request_id: d.request_id.clone(),
        }),
    };
    coord_proto::txn::RequestOp { op: Some(inner) }
}

/// proto `ResponseOp` → DTO `TxnOpOut`
fn from_proto_response(op: coord_proto::txn::ResponseOp) -> Option<TxnOpOut> {
    use coord_proto::txn::response_op::Op;
    match op.op? {
        Op::ResponsePut(p) => Some(TxnOpOut::Put(KvPutOut {
            prev_kv: p.prev_kv.map(proto_kv_to_record),
            revision: p.revision,
        })),
        Op::ResponseRange(r) => Some(TxnOpOut::Range(KvRangeOut {
            kvs: r.kvs.into_iter().map(proto_kv_to_record).collect(),
            count: r.count,
            revision: r.revision,
        })),
        Op::ResponseDelete(d) => Some(TxnOpOut::Delete(KvDeleteOut {
            deleted: d.deleted,
            prev_kvs: d.prev_kvs.into_iter().map(proto_kv_to_record).collect(),
            revision: d.revision,
        })),
    }
}

/// proto `KeyValue` → [`KvRecord`]
fn proto_kv_to_record(kv: coord_proto::kv::KeyValue) -> KvRecord {
    KvRecord {
        key: kv.key,
        value: kv.value,
        lease_id: kv.lease_id,
        version: kv.version,
    }
}

/// proto `ObjectStat` → [`ObjectStatDto`]
fn stat_from_proto(stat: coord_proto::storage::ObjectStat) -> ObjectStatDto {
    ObjectStatDto {
        bucket: stat.bucket,
        object_id: stat.object_id,
        size: stat.size,
        chunks: stat.chunks,
        revision: stat.revision,
        exists: stat.exists,
        committed: stat.committed,
    }
}

#[async_trait]
impl PluginSdkBackend for CoordSdkBackend {
    async fn kv_put(&self, plugin: &str, req: KvPut) -> SdkResult<KvPutOut> {
        // prev_kv：先读后写（与 agent KV 代理同语义的 best-effort 实现）
        let prev = if req.prev_kv {
            self.read_one(plugin, &req.key).await?
        } else {
            None
        };
        let revision = self
            .client(plugin)
            .kv()
            .put_full(&req.key, &req.value, req.lease_id, &req.request_id)
            .await
            .map_err(map_core_error)?;
        Ok(KvPutOut {
            prev_kv: prev,
            revision: revision as i64,
        })
    }

    async fn kv_range(&self, plugin: &str, req: KvRange) -> SdkResult<KvRangeOut> {
        let (kvs, count, revision) = self
            .client(plugin)
            .kv()
            .range_with_lease_full(
                &req.key,
                &req.range_end,
                req.limit,
                req.revision,
                req.keys_only,
                req.count_only,
            )
            .await
            .map_err(map_core_error)?;
        Ok(KvRangeOut {
            kvs: kvs.into_iter().map(record_from).collect(),
            count,
            revision,
        })
    }

    async fn kv_delete(&self, plugin: &str, req: KvDelete) -> SdkResult<KvDeleteOut> {
        let prev_kvs = if req.prev_kv {
            let (kvs, _count, _rev) = self
                .client(plugin)
                .kv()
                .range_with_lease_full(&req.key, &req.range_end, 0, 0, false, false)
                .await
                .map_err(map_core_error)?;
            kvs.into_iter().map(record_from).collect()
        } else {
            Vec::new()
        };
        let (deleted, revision) = self
            .client(plugin)
            .kv()
            .delete_full(&req.key, &req.range_end, req.prev_kv, &req.request_id)
            .await
            .map_err(map_core_error)?;
        Ok(KvDeleteOut {
            deleted,
            prev_kvs,
            revision,
        })
    }

    async fn txn(&self, plugin: &str, req: TxnReq) -> SdkResult<TxnOut> {
        let compares = req.compares.iter().map(to_proto_compare).collect();
        let success = req.success.iter().map(to_proto_op).collect();
        let failure = req.failure.iter().map(to_proto_op).collect();
        let resp = self
            .client(plugin)
            .txn()
            .txn_full(compares, success, failure, req.request_id)
            .await
            .map_err(map_core_error)?;
        Ok(TxnOut {
            succeeded: resp.succeeded,
            revision: resp.revision,
            responses: resp
                .responses
                .into_iter()
                .filter_map(from_proto_response)
                .collect(),
        })
    }

    async fn lease_grant(&self, plugin: &str, ttl: i64, id: i64) -> SdkResult<i64> {
        self.client(plugin)
            .lease()
            .grant_with_id(ttl, id)
            .await
            .map_err(map_core_error)
    }

    async fn lease_revoke(&self, plugin: &str, id: i64) -> SdkResult<()> {
        // 先停保活，避免撤销后仍被续约
        self.lease_stop_keep_alive(plugin, id).await?;
        self.client(plugin)
            .lease()
            .revoke(id)
            .await
            .map_err(map_core_error)
    }

    async fn lease_keep_alive(&self, plugin: &str, id: i64) -> SdkResult<()> {
        let key = (plugin.to_string(), id);
        if self.keepers.lock().contains_key(&key) {
            // 幂等：同一 lease 重复调用只保留一个句柄
            return Ok(());
        }
        let keeper = self
            .client(plugin)
            .lease()
            .keep_alive_background(id)
            .await
            .map_err(map_core_error)?;
        // 竞态兜底：并发首次调用时以先插入者为准，后者丢弃自身句柄
        let mut keepers = self.keepers.lock();
        if keepers.contains_key(&key) {
            drop(keepers);
            drop(keeper);
            return Ok(());
        }
        keepers.insert(key, keeper);
        Ok(())
    }

    async fn lease_stop_keep_alive(&self, plugin: &str, id: i64) -> SdkResult<()> {
        // Drop LeaseKeeper 只停止续约（不撤销租约），符合 `stop()` 语义
        self.keepers.lock().remove(&(plugin.to_string(), id));
        Ok(())
    }

    async fn watch_subscribe(&self, plugin: &str, req: WatchSubscribe) -> SdkResult<u64> {
        let rx = self
            .client(plugin)
            .watch()
            .watch_full(&req.key, &req.range_end, req.start_revision, req.prev_kv)
            .await
            .map_err(map_core_error)?;
        let id = self.next_watch_id.fetch_add(1, Ordering::Relaxed);
        self.watches.lock().insert(
            (plugin.to_string(), id),
            Arc::new(tokio::sync::Mutex::new(rx)),
        );
        Ok(id)
    }

    async fn watch_next(&self, plugin: &str, id: u64) -> SdkResult<Option<WatchEventDto>> {
        let rx = { self.watches.lock().get(&(plugin.to_string(), id)).cloned() };
        let Some(rx) = rx else {
            return Err(SdkError::not_found(format!(
                "watch subscription {id} not found for plugin '{plugin}'"
            )));
        };
        // 每个订阅一个锁：允许同一插件的多个订阅并发等待
        let mut guard = rx.lock().await;
        match guard.recv().await {
            Some(Ok(event)) => Ok(Some(watch_event_to_dto(event))),
            // 客户端背压合成信号：映射为 RESOURCE_EXHAUSTED（插件需重订阅）
            Some(Err(CoreError::Backpressure(msg))) => Err(SdkError::resource_exhausted(format!(
                "watch buffer overflow: {msg}"
            ))),
            Some(Err(e)) => Err(map_core_error(e)),
            None => Ok(None),
        }
    }

    async fn watch_close(&self, plugin: &str, id: u64) -> SdkResult<()> {
        self.watches.lock().remove(&(plugin.to_string(), id));
        Ok(())
    }

    async fn storage_put(
        &self,
        plugin: &str,
        bucket: &str,
        object_id: &[u8],
        data: &[u8],
    ) -> SdkResult<ObjectPutOut> {
        // 分块上传（默认 4MiB chunk，对齐服务端 chunk_size_bytes）
        let out = self
            .client(plugin)
            .storage()
            .put_chunked(
                bucket,
                object_id,
                data,
                coord_client::DEFAULT_OBJECT_CHUNK_SIZE,
            )
            .await
            .map_err(map_core_error)?;
        Ok(ObjectPutOut {
            revision: out.revision as i64,
            size: out.size as i64,
            chunks: out.chunks as i64,
        })
    }

    async fn storage_get(
        &self,
        plugin: &str,
        bucket: &str,
        object_id: &[u8],
    ) -> SdkResult<ObjectGetOut> {
        let obj = self
            .client(plugin)
            .storage()
            .get(bucket, object_id)
            .await
            .map_err(map_core_error)?;
        Ok(ObjectGetOut {
            stat: stat_from_proto(obj.stat),
            data: obj.data,
        })
    }

    async fn storage_stat(
        &self,
        plugin: &str,
        bucket: &str,
        object_id: &[u8],
    ) -> SdkResult<Option<ObjectStatDto>> {
        let stat = self
            .client(plugin)
            .storage()
            .stat(bucket, object_id)
            .await
            .map_err(map_core_error)?;
        Ok(stat.map(stat_from_proto))
    }

    async fn storage_delete(
        &self,
        plugin: &str,
        bucket: &str,
        object_id: &[u8],
    ) -> SdkResult<bool> {
        self.client(plugin)
            .storage()
            .delete(bucket, object_id)
            .await
            .map_err(map_core_error)
    }

    // ──── 对象存储流式会话（批次 10）────

    async fn storage_open_write(
        &self,
        plugin: &str,
        bucket: &str,
        object_id: &[u8],
        total_size: u64,
    ) -> SdkResult<u64> {
        let storage = self.client(plugin).storage();
        // `total_size == 0` = 未知长度（finish 时按实际字节定长）；`> 0` = 声明长度。
        let writer = if total_size == 0 {
            storage.open_put_unknown(bucket, object_id).await
        } else {
            storage.open_put(bucket, object_id, total_size).await
        }
        .map_err(map_core_error)?;
        let id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
        self.uploads.lock().insert(
            (plugin.to_string(), id),
            Arc::new(tokio::sync::Mutex::new(Some(writer))),
        );
        Ok(id)
    }

    async fn storage_write_chunk(&self, plugin: &str, id: u64, data: &[u8]) -> SdkResult<u64> {
        let session = self.upload_session(plugin, id)?;
        let mut guard = session.lock().await;
        match guard.as_mut() {
            Some(writer) => writer.write_chunk(data).await.map_err(map_core_error),
            None => Err(SdkError::not_found(format!(
                "storage upload session {id} is already finished"
            ))),
        }
    }

    async fn storage_commit_write(&self, plugin: &str, id: u64) -> SdkResult<ObjectPutOut> {
        let session = self.take_upload_session(plugin, id)?;
        let mut guard = session.lock().await;
        let writer = guard.take().ok_or_else(|| {
            SdkError::not_found(format!("storage upload session {id} is already finished"))
        })?;
        let out = writer.finish().await.map_err(map_core_error)?;
        Ok(ObjectPutOut {
            revision: out.revision as i64,
            size: out.size as i64,
            chunks: out.chunks as i64,
        })
    }

    async fn storage_abort_write(&self, plugin: &str, id: u64) -> SdkResult<()> {
        // 幂等：会话不存在 = 已提交或已中止
        let Some(session) = self.uploads.lock().remove(&(plugin.to_string(), id)) else {
            return Ok(());
        };
        let mut guard = session.lock().await;
        if let Some(writer) = guard.take() {
            writer.abort();
        }
        Ok(())
    }

    async fn storage_open_read(
        &self,
        plugin: &str,
        bucket: &str,
        object_id: &[u8],
    ) -> SdkResult<u64> {
        let reader = self
            .client(plugin)
            .storage()
            .open_get(bucket, object_id)
            .await
            .map_err(map_core_error)?;
        let id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
        self.reads.lock().insert(
            (plugin.to_string(), id),
            Arc::new(tokio::sync::Mutex::new(ReadSession {
                reader,
                pending: Vec::new(),
            })),
        );
        Ok(id)
    }

    async fn storage_read_chunk(
        &self,
        plugin: &str,
        id: u64,
        max_len: u64,
    ) -> SdkResult<Option<Vec<u8>>> {
        if max_len == 0 {
            return Err(SdkError::invalid_argument("max_len must be > 0"));
        }
        let session = self.read_session(plugin, id)?;
        let mut sessions = session.lock().await;
        let max_len = max_len as usize;

        // 上次截断的尾巴优先返回（保证单次返回 ≤ max_len）
        if !sessions.pending.is_empty() {
            let take = max_len.min(sessions.pending.len());
            return Ok(Some(sessions.pending.drain(..take).collect()));
        }
        match sessions.reader.read_chunk().await.map_err(map_core_error)? {
            None => Ok(None),
            Some(chunk) if chunk.len() <= max_len => Ok(Some(chunk)),
            Some(mut chunk) => {
                sessions.pending = chunk.split_off(max_len);
                Ok(Some(chunk))
            }
        }
    }

    async fn storage_reader_stat(&self, plugin: &str, id: u64) -> SdkResult<ObjectStatDto> {
        let session = self.read_session(plugin, id)?;
        let guard = session.lock().await;
        Ok(stat_from_proto(guard.reader.stat().clone()))
    }

    async fn storage_close_read(&self, plugin: &str, id: u64) -> SdkResult<()> {
        // 幂等：会话不存在 = 已关闭
        let Some(session) = self.reads.lock().remove(&(plugin.to_string(), id)) else {
            return Ok(());
        };
        session.lock().await.reader.close();
        Ok(())
    }

    async fn release_plugin(&self, plugin: &str) {
        let removed: Vec<(String, i64)> = {
            let mut keepers = self.keepers.lock();
            let keys: Vec<(String, i64)> = keepers
                .keys()
                .filter(|(p, _)| p == plugin)
                .cloned()
                .collect();
            for k in &keys {
                keepers.remove(k);
            }
            keys
        };
        let watches = {
            let mut w = self.watches.lock();
            let keys: Vec<(String, u64)> = w.keys().filter(|(p, _)| p == plugin).cloned().collect();
            for k in &keys {
                w.remove(k);
            }
            keys.len()
        };
        // 对象存储流式会话：摘除后由 `Drop` 兜底（上传中止任务 / 下载关闭流并归还连接）。
        let (uploads, reads) = {
            let mut u = self.uploads.lock();
            let n = u.keys().filter(|(p, _)| p == plugin).count();
            u.retain(|(p, _), _| p != plugin);
            let mut r = self.reads.lock();
            let m = r.keys().filter(|(p, _)| p == plugin).count();
            r.retain(|(p, _), _| p != plugin);
            (n, m)
        };
        if !removed.is_empty() || watches > 0 || uploads > 0 || reads > 0 {
            tracing::debug!(
                "plugin '{plugin}': released {} lease keep-alive handle(s), {watches} watch \
                 subscription(s), {uploads} upload session(s), {reads} download session(s)",
                removed.len()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_core_errors_to_sdk_codes() {
        assert_eq!(
            map_core_error(CoreError::NotFound {
                resource: "key",
                key: "/k".into()
            })
            .code,
            SdkErrorCode::NotFound
        );
        assert_eq!(
            map_core_error(CoreError::NotLeaderNoHint).code,
            SdkErrorCode::Unavailable
        );
        assert_eq!(
            map_core_error(CoreError::PermissionDenied("nope".into())).code,
            SdkErrorCode::Forbidden
        );
        assert_eq!(
            map_core_error(CoreError::Backpressure("full".into())).code,
            SdkErrorCode::ResourceExhausted
        );
        assert_eq!(
            map_core_error(CoreError::InvalidArgument("bad".into())).code,
            SdkErrorCode::InvalidArgument
        );
        assert_eq!(
            map_core_error(CoreError::Storage("io".into())).code,
            SdkErrorCode::Internal
        );
        assert_eq!(
            map_core_error(CoreError::LeaseNotFound { lease_id: 1 }).code,
            SdkErrorCode::NotFound
        );
    }

    #[test]
    fn maps_compare_targets_and_ops() {
        let c = Compare {
            op: CompareOp::NotEqual,
            target: CompareTarget::ModRevision,
            key: b"/k".to_vec(),
            int_value: 3,
            bytes_value: Vec::new(),
        };
        let p = to_proto_compare(&c);
        assert_eq!(
            p.result,
            coord_proto::txn::compare::CompareResult::NotEqual as i32
        );
        assert_eq!(p.target, coord_proto::txn::compare::Target::ModRev as i32);
        assert!(matches!(
            p.target_value,
            Some(coord_proto::txn::compare::TargetValue::ModRevision(3))
        ));

        let op = to_proto_op(&TxnOp::Put(KvPut {
            key: b"/k".to_vec(),
            value: b"v".to_vec(),
            lease_id: 5,
            prev_kv: true,
            request_id: b"rid".to_vec(),
        }));
        match op.op {
            Some(coord_proto::txn::request_op::Op::RequestPut(put)) => {
                assert_eq!(put.lease_id, 5);
                assert!(put.prev_kv);
                assert_eq!(put.request_id, b"rid");
            }
            other => panic!("unexpected op: {other:?}"),
        }
    }

    #[test]
    fn maps_txn_response_ops() {
        let resp = coord_proto::txn::ResponseOp {
            op: Some(coord_proto::txn::response_op::Op::ResponsePut(
                coord_proto::kv::PutResponse {
                    prev_kv: Some(coord_proto::kv::KeyValue {
                        key: b"/k".to_vec(),
                        value: b"old".to_vec(),
                        create_revision: 1,
                        mod_revision: 2,
                        version: 3,
                        lease_id: 0,
                    }),
                    revision: 9,
                },
            )),
        };
        match from_proto_response(resp) {
            Some(TxnOpOut::Put(p)) => {
                assert_eq!(p.revision, 9);
                let prev = p.prev_kv.expect("prev kv");
                assert_eq!(prev.value, b"old");
                assert_eq!(prev.version, 3);
            }
            other => panic!("unexpected response: {other:?}"),
        }
    }
}
