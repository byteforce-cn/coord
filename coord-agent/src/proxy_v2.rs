// coord-agent: KV-T v2 数据面代理（`coord.tso.v2` / `coord.kv.v2` / `coord.lease.v2` /
// `coord.txn.v2`）
//
// 与 v1 代理的分工：v1 走 `coord_client::Client` 的 v1 方法 + 本地 KV 读缓存；
// v2 直接调用 `coord-client` 的 v2 包装，**不经过**读缓存（v2 revision/read_ts 是
// 全局快照语义，缓存无法证明一致）。路由 epoch 由 agent 内部的 `Client` 学习并在
// 每次尝试时附带，Java 客户端不需要（也不应该）自己维护 epoch。
//
// 错误码：`coord-client` 的 v2 上层会把 `TXN_CONFLICT`/`COMMIT_REJECTED`/
// `SAFE_TS_LAG` 等码保留成 `Error::V2`；代理原样重建 `Status` + trailer，避免
// Java SDK 只看到 gRPC 状态码而把「重跑事务」误判成内部错误。
//
// Txn 代理必须把同一 `txn_id` 的后续调用送回 **Begin 选中的协调节点**：服务端
// 会话只活在协调节点（ADR-0015 D12.2），按「当前 leader」重新解析会让 leader
// 切换后的每个事务变成 `TXN_NOT_FOUND`。
//
// 本切片覆盖 TSO/KV/Lease/Txn；Watch/Storage 的 v2 代理在后续切片交付。

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use coord_client::txn_v2::{TxnCommit, TxnV2Session};
use coord_core::error::Error as CoreError;
use coord_core::error_code::{attach, error_code_of, CoordErrorCode};
use coord_proto::kv::v2 as kv_v2;
use coord_proto::kv::v2::kv_server::Kv as KvV2;
use coord_proto::lease::v2 as lease_v2;
use coord_proto::lease::v2::lease_server::Lease as LeaseV2;
use coord_proto::tso::v2 as tso_v2;
use coord_proto::tso::v2::tso_server::Tso as TsoV2;
use coord_proto::txn::v2 as txn_v2;
use coord_proto::txn::v2::txn_server::Txn as TxnV2;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::proxy::{map_core_error, AgentInner};

/// 无 Server 连接时的 fail-closed 响应。
///
/// 不能用 v1 骨架的「占位成功」：v2 写请求带 `commit_ts`，假成功会让调用方以为
/// 写已落盘。`UNAVAILABLE` 是可重试的准确语义（agent 还在，但后端集群不可达）。
fn no_backend() -> tonic::Status {
    attach(
        tonic::Status::unavailable("agent has no server connection"),
        CoordErrorCode::Unavailable,
    )
}

/// v2 错误映射：`Error::V2` 保留 wire 码；其余沿用 v1 的 `map_core_error`。
fn map_v2_error(e: CoreError) -> tonic::Status {
    if let CoreError::V2 { code, message } = &e {
        return code.into_status(message.clone());
    }
    map_core_error(e)
}

/// proto 的 `int64` 时间戳 → u64（负数/越界不能回绕）。
fn ts_from_i64(name: &str, ts: i64) -> Result<u64, tonic::Status> {
    u64::try_from(ts).map_err(|_| {
        attach(
            tonic::Status::invalid_argument(format!("{name} must not be negative: {ts}")),
            CoordErrorCode::InvalidArgument,
        )
    })
}

/// 响应里的 u64 时间戳 → proto `int64`。
fn ts_to_i64(name: &str, ts: u64) -> Result<i64, tonic::Status> {
    i64::try_from(ts).map_err(|_| {
        attach(
            tonic::Status::internal(format!("{name} exceeds the int64 wire range: {ts}")),
            CoordErrorCode::Internal,
        )
    })
}

// ──── TSO v2 ────

/// `coord.tso.v2.TSO` 代理。
#[derive(Debug, Clone)]
pub struct TsoV2Proxy {
    inner: Option<Arc<AgentInner>>,
}

impl TsoV2Proxy {
    pub fn new(inner: Option<Arc<AgentInner>>) -> Self {
        Self { inner }
    }
}

#[tonic::async_trait]
impl TsoV2 for TsoV2Proxy {
    async fn allocate(
        &self,
        request: tonic::Request<tso_v2::AllocateRequest>,
    ) -> Result<tonic::Response<tso_v2::AllocateResponse>, tonic::Status> {
        let inner = self.inner.as_ref().ok_or_else(no_backend)?;
        let (start_ts, end_ts) = inner
            .client
            .kv_v2()
            .allocate(request.into_inner().batch_size)
            .await
            .map_err(map_v2_error)?;
        Ok(tonic::Response::new(tso_v2::AllocateResponse {
            start_ts: ts_to_i64("start_ts", start_ts)?,
            end_ts: ts_to_i64("end_ts", end_ts)?,
        }))
    }

    async fn get_ts(
        &self,
        _request: tonic::Request<tso_v2::GetTsRequest>,
    ) -> Result<tonic::Response<tso_v2::GetTsResponse>, tonic::Status> {
        let inner = self.inner.as_ref().ok_or_else(no_backend)?;
        let ts = inner
            .client
            .kv_v2()
            .get_commit_ts()
            .await
            .map_err(map_v2_error)?;
        Ok(tonic::Response::new(tso_v2::GetTsResponse {
            ts: ts_to_i64("ts", ts)?,
        }))
    }
}

// ──── KV v2 ────

/// `coord.kv.v2.KV` 代理。
#[derive(Debug, Clone)]
pub struct KvV2Proxy {
    inner: Option<Arc<AgentInner>>,
}

impl KvV2Proxy {
    pub fn new(inner: Option<Arc<AgentInner>>) -> Self {
        Self { inner }
    }
}

#[tonic::async_trait]
impl KvV2 for KvV2Proxy {
    async fn put(
        &self,
        request: tonic::Request<kv_v2::PutRequest>,
    ) -> Result<tonic::Response<kv_v2::PutResponse>, tonic::Status> {
        let req = request.into_inner();
        let commit_ts = ts_from_i64("commit_ts", req.commit_ts)?;
        let inner = self.inner.as_ref().ok_or_else(no_backend)?;
        let (prev_kv, commit_ts) = inner
            .client
            .kv_v2()
            .put_with_lease(
                &req.key,
                &req.value,
                req.lease_id,
                commit_ts,
                req.prev_kv,
                &req.request_id,
            )
            .await
            .map_err(map_v2_error)?;
        Ok(tonic::Response::new(kv_v2::PutResponse {
            prev_kv,
            commit_ts: ts_to_i64("commit_ts", commit_ts)?,
        }))
    }

    async fn range(
        &self,
        request: tonic::Request<kv_v2::RangeRequest>,
    ) -> Result<tonic::Response<kv_v2::RangeResponse>, tonic::Status> {
        let req = request.into_inner();
        let read_ts = ts_from_i64("read_ts", req.read_ts)?;
        let inner = self.inner.as_ref().ok_or_else(no_backend)?;
        let (kvs, count, read_ts) = inner
            .client
            .kv_v2()
            .range(
                &req.key,
                &req.range_end,
                req.limit,
                read_ts,
                req.keys_only,
                req.count_only,
            )
            .await
            .map_err(map_v2_error)?;
        Ok(tonic::Response::new(kv_v2::RangeResponse {
            kvs,
            count,
            read_ts: ts_to_i64("read_ts", read_ts)?,
        }))
    }

    async fn delete(
        &self,
        request: tonic::Request<kv_v2::DeleteRequest>,
    ) -> Result<tonic::Response<kv_v2::DeleteResponse>, tonic::Status> {
        let req = request.into_inner();
        let commit_ts = ts_from_i64("commit_ts", req.commit_ts)?;
        let inner = self.inner.as_ref().ok_or_else(no_backend)?;
        let (deleted, prev_kvs, commit_ts) = inner
            .client
            .kv_v2()
            .delete(
                &req.key,
                &req.range_end,
                commit_ts,
                req.prev_kv,
                &req.request_id,
            )
            .await
            .map_err(map_v2_error)?;
        Ok(tonic::Response::new(kv_v2::DeleteResponse {
            deleted,
            prev_kvs,
            commit_ts: ts_to_i64("commit_ts", commit_ts)?,
        }))
    }
}

// ──── Lease v2 ────

/// `coord.lease.v2.Lease` 代理。
#[derive(Debug, Clone)]
pub struct LeaseV2Proxy {
    inner: Option<Arc<AgentInner>>,
}

impl LeaseV2Proxy {
    pub fn new(inner: Option<Arc<AgentInner>>) -> Self {
        Self { inner }
    }
}

#[tonic::async_trait]
impl LeaseV2 for LeaseV2Proxy {
    type LeaseKeepAliveStream =
        ReceiverStream<Result<lease_v2::LeaseKeepAliveResponse, tonic::Status>>;

    async fn lease_grant(
        &self,
        request: tonic::Request<lease_v2::LeaseGrantRequest>,
    ) -> Result<tonic::Response<lease_v2::LeaseGrantResponse>, tonic::Status> {
        let req = request.into_inner();
        let inner = self.inner.as_ref().ok_or_else(no_backend)?;
        let grant = inner
            .client
            .lease_v2()
            .grant(req.ttl_seconds, req.lease_id, &req.request_id)
            .await
            .map_err(map_v2_error)?;
        Ok(tonic::Response::new(lease_v2::LeaseGrantResponse {
            lease_id: grant.lease_id,
            ttl_seconds: grant.ttl_seconds,
            granted_ts: ts_to_i64("granted_ts", grant.granted_ts)?,
        }))
    }

    async fn lease_revoke(
        &self,
        request: tonic::Request<lease_v2::LeaseRevokeRequest>,
    ) -> Result<tonic::Response<lease_v2::LeaseRevokeResponse>, tonic::Status> {
        let req = request.into_inner();
        let inner = self.inner.as_ref().ok_or_else(no_backend)?;
        let commit_ts = inner
            .client
            .lease_v2()
            .revoke(req.lease_id, &req.request_id)
            .await
            .map_err(map_v2_error)?;
        Ok(tonic::Response::new(lease_v2::LeaseRevokeResponse {
            commit_ts: ts_to_i64("commit_ts", commit_ts)?,
        }))
    }

    async fn lease_keep_alive(
        &self,
        request: tonic::Request<tonic::Streaming<lease_v2::LeaseKeepAliveRequest>>,
    ) -> Result<tonic::Response<Self::LeaseKeepAliveStream>, tonic::Status> {
        let mut stream_in = request.into_inner();
        let inner = self.inner.as_ref().ok_or_else(no_backend)?.clone();

        let (tx, rx) = mpsc::channel::<Result<lease_v2::LeaseKeepAliveResponse, tonic::Status>>(16);
        tokio::spawn(async move {
            while let Ok(Some(req)) = stream_in.message().await {
                match inner.client.lease_v2().keep_alive(req.lease_id).await {
                    Ok(ttl_seconds) => {
                        let resp = lease_v2::LeaseKeepAliveResponse {
                            lease_id: req.lease_id,
                            ttl_seconds,
                        };
                        if tx.send(Ok(resp)).await.is_err() {
                            break; // 客户端已断开
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(map_v2_error(e))).await;
                        break;
                    }
                }
            }
        });
        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }

    async fn lease_time_to_live(
        &self,
        request: tonic::Request<lease_v2::LeaseTimeToLiveRequest>,
    ) -> Result<tonic::Response<lease_v2::LeaseTimeToLiveResponse>, tonic::Status> {
        let req = request.into_inner();
        let inner = self.inner.as_ref().ok_or_else(no_backend)?;
        let state = inner
            .client
            .lease_v2()
            .time_to_live(req.lease_id, req.keys)
            .await
            .map_err(map_v2_error)?;
        Ok(tonic::Response::new(lease_v2::LeaseTimeToLiveResponse {
            lease_id: state.lease_id,
            ttl_seconds: state.ttl_seconds,
            granted_ttl_seconds: state.granted_ttl_seconds,
            keys: state.keys,
        }))
    }
}

// ──── Txn v2 ────

/// 代理侧会话登记的空闲上界。
///
/// 服务端 `[kv_t].txn_ttl_secs` 被校验为 `1..=3600`（秒），所以超过 1 小时 +
/// 余量的登记一定已被服务端回收：先过期代理侧登记永远不会短于服务端 TTL，
/// 不会把「服务端仍然活着的会话」提前变成代理本地的 `TXN_NOT_FOUND`。
const TXN_PROXY_MAX_IDLE: Duration = Duration::from_secs(3_600 + 60);

/// 一个被代理钉住的会话；`session` 为 `None` 只发生在取走/回收的瞬间。
struct TxnSlot {
    session: tokio::sync::Mutex<Option<TxnV2Session>>,
    last_used_ms: AtomicU64,
}

impl TxnSlot {
    fn new(session: TxnV2Session, now_ms: u64) -> Self {
        Self {
            session: tokio::sync::Mutex::new(Some(session)),
            last_used_ms: AtomicU64::new(now_ms),
        }
    }

    fn touch(&self, now_ms: u64) {
        self.last_used_ms.store(now_ms, Ordering::Relaxed);
    }
}

/// `txn_id` → 会话登记。代理是这些会话的**唯一持有者**（服务端按节点本地会话
/// 处理）：丢了登记 = 协调节点重启，调用方必须重跑整个事务（D12.2）。
struct TxnSessions {
    start: Instant,
    by_id: parking_lot::Mutex<HashMap<Vec<u8>, Arc<TxnSlot>>>,
}

impl TxnSessions {
    fn new() -> Self {
        Self {
            start: Instant::now(),
            by_id: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    fn now_ms(&self) -> u64 {
        // 进程内单调时钟，只用于比较空闲时长；不回绕（u64 毫秒 ≈ 5.8 亿年）。
        self.start.elapsed().as_millis() as u64
    }

    fn insert(&self, txn_id: Vec<u8>, slot: Arc<TxnSlot>) {
        self.by_id.lock().insert(txn_id, slot);
    }

    fn get(&self, txn_id: &[u8]) -> Option<Arc<TxnSlot>> {
        self.by_id.lock().get(txn_id).cloned()
    }

    fn remove(&self, txn_id: &[u8]) -> Option<Arc<TxnSlot>> {
        self.by_id.lock().remove(txn_id)
    }

    fn forget(&self, txn_id: &[u8]) {
        self.by_id.lock().remove(txn_id);
    }

    /// 回收被放弃的会话登记（客户端既不 Commit 也不 Abort）。惰性执行：只在
    /// `Begin` 入口扫一遍，避免后台任务的生命周期与 Agent 停机耦合。
    fn sweep(&self) {
        self.sweep_at(self.now_ms());
    }

    fn sweep_at(&self, now_ms: u64) {
        let max_idle_ms = TXN_PROXY_MAX_IDLE.as_millis() as u64;
        self.by_id.lock().retain(|_, slot| {
            now_ms.saturating_sub(slot.last_used_ms.load(Ordering::Relaxed)) < max_idle_ms
        });
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.by_id.lock().len()
    }

    #[cfg(test)]
    fn insert_idle_at(&self, txn_id: Vec<u8>, last_used_ms: u64) {
        self.insert(
            txn_id,
            Arc::new(TxnSlot {
                session: tokio::sync::Mutex::new(None),
                last_used_ms: AtomicU64::new(last_used_ms),
            }),
        );
    }
}

/// 未知/已结束会话的响应（与服务端 `txn_session_status` 同一口径）。
fn txn_not_found() -> tonic::Status {
    attach(
        tonic::Status::aborted("transaction session not found or expired; restart the transaction"),
        CoordErrorCode::TxnNotFound,
    )
}

/// `coord.txn.v2.Txn` 代理。
#[derive(Clone)]
pub struct TxnV2Proxy {
    inner: Option<Arc<AgentInner>>,
    sessions: Arc<TxnSessions>,
}

impl TxnV2Proxy {
    pub fn new(inner: Option<Arc<AgentInner>>) -> Self {
        Self {
            inner,
            sessions: Arc::new(TxnSessions::new()),
        }
    }
}

#[tonic::async_trait]
impl TxnV2 for TxnV2Proxy {
    async fn begin(
        &self,
        request: tonic::Request<txn_v2::BeginRequest>,
    ) -> Result<tonic::Response<txn_v2::BeginResponse>, tonic::Status> {
        let req = request.into_inner();
        let start_ts = ts_from_i64("start_ts", req.start_ts)?;
        let inner = self.inner.as_ref().ok_or_else(no_backend)?;
        self.sessions.sweep();
        let session = inner
            .client
            .txn_v2()
            .begin(start_ts, req.ttl_ms)
            .await
            .map_err(map_v2_error)?;
        let txn_id = session.txn_id().to_vec();
        let start_ts = session.start_ts();
        let now_ms = self.sessions.now_ms();
        self.sessions
            .insert(txn_id.clone(), Arc::new(TxnSlot::new(session, now_ms)));
        Ok(tonic::Response::new(txn_v2::BeginResponse {
            txn_id,
            start_ts: ts_to_i64("start_ts", start_ts)?,
        }))
    }

    async fn read(
        &self,
        request: tonic::Request<txn_v2::ReadRequest>,
    ) -> Result<tonic::Response<txn_v2::ReadResponse>, tonic::Status> {
        let req = request.into_inner();
        // 没有后端时先失败在连接层：Read 不能回「会话不存在」诱导调用方重跑事务。
        self.inner.as_ref().ok_or_else(no_backend)?;
        let slot = self.sessions.get(&req.txn_id).ok_or_else(txn_not_found)?;
        let result = {
            let guard = slot.session.lock().await;
            let session = guard.as_ref().ok_or_else(txn_not_found)?;
            slot.touch(self.sessions.now_ms());
            session
                .read(&req.key, &req.range_end, req.limit, req.keys_only)
                .await
                .map_err(map_v2_error)
        };
        match result {
            Ok((kvs, read_ts)) => Ok(tonic::Response::new(txn_v2::ReadResponse {
                kvs,
                read_ts: ts_to_i64("read_ts", read_ts)?,
            })),
            Err(status) => {
                // 服务端已回收（TTL/节点重启）：登记是死条目，删掉避免每次再打一趟。
                if error_code_of(&status).as_deref() == Some(CoordErrorCode::TxnNotFound.as_str()) {
                    self.sessions.forget(&req.txn_id);
                }
                Err(status)
            }
        }
    }

    async fn commit(
        &self,
        request: tonic::Request<txn_v2::CommitRequest>,
    ) -> Result<tonic::Response<txn_v2::CommitResponse>, tonic::Status> {
        let req = request.into_inner();
        if req.txn_id.is_empty() {
            return Err(attach(
                tonic::Status::invalid_argument("txn_id must not be empty"),
                CoordErrorCode::InvalidArgument,
            ));
        }
        self.inner.as_ref().ok_or_else(no_backend)?;
        // 先摘登记：并发的第二次 Commit 不能重复决策。
        let slot = self
            .sessions
            .remove(&req.txn_id)
            .ok_or_else(txn_not_found)?;
        let mut guard = slot.session.lock().await;
        let session = guard.take().ok_or_else(txn_not_found)?;
        match session
            .commit_in_place(req.mutations, &req.request_id)
            .await
        {
            Ok(TxnCommit::Committed { commit_ts }) => {
                Ok(tonic::Response::new(txn_v2::CommitResponse {
                    commit_ts: ts_to_i64("commit_ts", commit_ts)?,
                }))
            }
            Ok(TxnCommit::Conflict) => Err(attach(
                tonic::Status::aborted("transaction conflict; restart with a newer start_ts"),
                CoordErrorCode::TxnConflict,
            )),
            Err(e) => {
                // 传输失败时把会话放回登记：同一 txn_id + request_id 的重试由
                // 服务端幂等回放首次 commit_ts（会话已确认成功/冲突时不回填）。
                guard.replace(session);
                drop(guard);
                self.sessions.insert(req.txn_id, slot);
                Err(map_v2_error(e))
            }
        }
    }

    async fn abort(
        &self,
        request: tonic::Request<txn_v2::AbortRequest>,
    ) -> Result<tonic::Response<txn_v2::AbortResponse>, tonic::Status> {
        let req = request.into_inner();
        self.inner.as_ref().ok_or_else(no_backend)?;
        let Some(slot) = self.sessions.remove(&req.txn_id) else {
            // 与服务端同一口径：未知/已过期会话的 Abort 幂等成功。
            return Ok(tonic::Response::new(txn_v2::AbortResponse {}));
        };
        let mut guard = slot.session.lock().await;
        let Some(session) = guard.take() else {
            return Ok(tonic::Response::new(txn_v2::AbortResponse {}));
        };
        session.abort().await.map_err(map_v2_error)?;
        Ok(tonic::Response::new(txn_v2::AbortResponse {}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use coord_core::error_code::error_code_of;

    #[tokio::test]
    async fn skeleton_txn_fails_closed_and_does_not_fake_a_missing_session() {
        let proxy = TxnV2Proxy::new(None);
        let err = proxy
            .begin(tonic::Request::new(txn_v2::BeginRequest {
                start_ts: 0,
                ttl_ms: 0,
            }))
            .await
            .expect_err("no server connection must fail");
        assert_eq!(err.code(), tonic::Code::Unavailable, "{err:?}");
        assert_eq!(
            error_code_of(&err).as_deref(),
            Some(CoordErrorCode::Unavailable.as_str()),
            "{err:?}"
        );

        // 没有后端 ≠ 会话不存在：Read 不能回 TXN_NOT_FOUND 让调用方重跑事务。
        let err = proxy
            .read(tonic::Request::new(txn_v2::ReadRequest {
                txn_id: b"t".to_vec(),
                ..Default::default()
            }))
            .await
            .expect_err("skeleton read must fail");
        assert_eq!(err.code(), tonic::Code::Unavailable, "{err:?}");
        assert_eq!(
            error_code_of(&err).as_deref(),
            Some(CoordErrorCode::Unavailable.as_str()),
            "{err:?}"
        );
    }

    #[test]
    fn txn_sessions_sweep_only_drops_entries_past_the_server_ttl_bound() {
        let sessions = TxnSessions::new();
        let max_idle_ms = TXN_PROXY_MAX_IDLE.as_millis() as u64;
        sessions.insert_idle_at(b"old".to_vec(), 0);
        sessions.insert_idle_at(b"fresh".to_vec(), max_idle_ms);
        sessions.sweep_at(max_idle_ms);
        assert_eq!(sessions.len(), 1, "only the expired entry is dropped");
        assert!(sessions.get(b"old").is_none());
        assert!(sessions.get(b"fresh").is_some());
    }

    #[tokio::test]
    async fn skeleton_tso_fails_closed_with_a_retryable_code() {
        let proxy = TsoV2Proxy::new(None);
        let err = proxy
            .get_ts(tonic::Request::new(tso_v2::GetTsRequest {}))
            .await
            .expect_err("no server connection must fail");
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert_eq!(
            error_code_of(&err).as_deref(),
            Some(CoordErrorCode::Unavailable.as_str())
        );
    }

    #[tokio::test]
    async fn skeleton_kv_does_not_fake_a_write() {
        let proxy = KvV2Proxy::new(None);
        let err = proxy
            .put(tonic::Request::new(kv_v2::PutRequest {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                commit_ts: 1,
                ..Default::default()
            }))
            .await
            .expect_err("no server connection must fail");
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert_eq!(
            error_code_of(&err).as_deref(),
            Some(CoordErrorCode::Unavailable.as_str())
        );
    }

    #[tokio::test]
    async fn skeleton_lease_fails_closed() {
        let proxy = LeaseV2Proxy::new(None);
        let err = proxy
            .lease_grant(tonic::Request::new(lease_v2::LeaseGrantRequest {
                ttl_seconds: 30,
                ..Default::default()
            }))
            .await
            .expect_err("no server connection must fail");
        assert_eq!(err.code(), tonic::Code::Unavailable);
    }

    #[test]
    fn v2_errors_keep_their_wire_code_and_status() {
        let err = map_v2_error(CoreError::V2 {
            code: CoordErrorCode::TxnConflict,
            message: "conflict".into(),
        });
        assert_eq!(err.code(), tonic::Code::Aborted);
        assert_eq!(
            error_code_of(&err).as_deref(),
            Some(CoordErrorCode::TxnConflict.as_str())
        );

        let lag = map_v2_error(CoreError::V2 {
            code: CoordErrorCode::SafeTsLag,
            message: "lagging".into(),
        });
        assert_eq!(lag.code(), tonic::Code::Unavailable);
        assert_eq!(
            error_code_of(&lag).as_deref(),
            Some(CoordErrorCode::SafeTsLag.as_str())
        );
    }

    #[test]
    fn negative_timestamps_are_rejected_before_the_wire() {
        let err = ts_from_i64("commit_ts", -1).expect_err("negative ts must be rejected");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            error_code_of(&err).as_deref(),
            Some(CoordErrorCode::InvalidArgument.as_str())
        );
    }
}
