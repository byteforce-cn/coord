//! `coord.watch.v2` 的首帧校验、守卫与流服务（ADR-0015 D5/D9）。
//!
//! 本模块只负责 RPC 面：把 `WatchCreateRequest` 翻译成 `WatchStart`/`WatchFilter`，
//! 用 [`WatchCoordinator`] 拉取批次并写入有界响应通道。事件顺序、原子批与历史窗口
//! 的语义单点定义在 [`super::watch`] —— 这里不得重新分批、重排或跳读。
//!
//! 授权分两段：能力门禁由 `KvTState::gate`/鉴权层完成；首帧的 key/prefix 判定
//! 只能在这里做（流 body 不可预读），因此 `DeferredScopeGrants` 存在时必须用
//! [`coord_core::grpc_auth::check_deferred_scope`] 判定，缺省语义见该类型文档。

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use coord_core::error_code::{attach, CoordErrorCode};
use coord_core::grpc_auth::{check_deferred_scope, DeferredScopeGrants, ScopeAccess};
use coord_proto::watch::v2 as watch_v2;
use tokio::sync::mpsc;
use tonic::{Code, Request, Status, Streaming};

use crate::metrics::Metrics;

use super::watch::{
    EventKindFilter, WatchCoordinator, WatchCursor, WatchError, WatchEvent, WatchFilter, WatchStart,
};
use super::KvTCapability;

/// 空批（无事件可读）时的轮询间隔。
///
/// 拉模型没有通知通道：间隔太短会空转 raft 读，太长会把订阅延迟直接加到
/// 写入到可见的路径上。100ms 与 v1 dispatcher 的节拍同量级。
const WATCH_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// 每条流的响应通道容量。
///
/// 有界是必须的：慢消费者只能阻塞自己的拉取任务（背压），不能把服务端内存
/// 变成无界队列；也不得为了“不阻塞”而丢事件。
const WATCH_STREAM_BUFFER: usize = 32;

/// 一帧 create 解析出的订阅参数。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WatchSpec {
    pub start: WatchStart,
    pub filter: WatchFilter,
}

/// 校验并翻译首帧 create。
///
/// `start_revision` 与 `start_cursor` 同时给出是请求歧义：不采用“后者优先”，
/// 否则同一个请求在不同实现里会订阅到不同位置。
pub(crate) fn spec_from_create(create: &watch_v2::WatchCreateRequest) -> Result<WatchSpec, Status> {
    let kind = match create.filter {
        0 | 1 => EventKindFilter::All,
        2 => EventKindFilter::PutsOnly,
        3 => EventKindFilter::DeletesOnly,
        other => {
            return Err(invalid_argument(format!(
                "unknown watch event filter {other} (expected ALL/PUTS_ONLY/DELETES_ONLY)"
            )))
        }
    };
    if create.start_revision < 0 {
        return Err(invalid_argument("start_revision must be >= 0"));
    }
    let start_cursor = match &create.start_cursor {
        Some(cursor) if cursor.commit_ts < 0 => {
            return Err(invalid_argument("start_cursor.commit_ts must be >= 0"))
        }
        Some(cursor) => Some(WatchCursor {
            shard_id: cursor.shard_id,
            commit_ts: cursor.commit_ts as u64,
        }),
        None => None,
    };
    if start_cursor.is_some() && create.start_revision != 0 {
        return Err(invalid_argument(
            "start_revision and start_cursor are mutually exclusive",
        ));
    }

    let filter = WatchFilter {
        key: create.key.clone(),
        range_end: create.range_end.clone(),
        kind,
        prev_kv: create.prev_kv,
    };
    filter.validate().map_err(watch_status)?;

    Ok(WatchSpec {
        start: WatchStart {
            start_revision: create.start_revision as u64,
            start_cursor,
        },
        filter,
    })
}

/// 订阅范围的守卫：保留命名空间、对象空间与延后 scope。
///
/// 全 keyspace 订阅不做保留前缀的“触碰”判定——它必然覆盖所有命名空间，
/// 能走到这里的只有无 scope 约束（鉴权关闭/root）的调用方；有约束的调用方
/// 会被 [`check_deferred_scope`] 拒绝（无界区间无法被有限 scope 覆盖）。
pub(crate) fn guard_create(
    create: &watch_v2::WatchCreateRequest,
    grants: Option<&DeferredScopeGrants>,
) -> Result<(), Status> {
    let all_keyspace = create.key.is_empty() && create.range_end.is_empty();
    if !all_keyspace {
        let (key, range_end) = normalized_bounds(&create.key, &create.range_end);
        if crate::storage::object_store::range_touches_object_space(&key, &range_end) {
            return Err(invalid_argument(
                "watch on reserved object-storage namespace /obj/ is not supported",
            ));
        }
        if let Some(ns) = crate::auth::interceptor::reserved_internal_kv_namespace(&key, &range_end)
        {
            return Err(attach(
                Status::permission_denied(format!(
                    "watch on reserved internal namespace {ns} is not supported"
                )),
                CoordErrorCode::PermissionDenied,
            ));
        }
    }

    if let Some(grants) = grants {
        let access = watch_scope_access(&create.key, &create.range_end);
        check_deferred_scope(Some(grants), &[access]).map_err(|reason| {
            attach(
                Status::permission_denied(reason),
                CoordErrorCode::PermissionDenied,
            )
        })?;
    }
    Ok(())
}

/// 订阅实际会触碰的范围（`(lo, hi)`，`hi` 空 = 单 key）。
///
/// v2 的 `range_end` 空 = 单 key；`range_end == key` 同样按单 key（与
/// `RangeSemantics::of` 一致），避免把“看起来像区间”的请求当成有界区间。
fn normalized_bounds(key: &[u8], range_end: &[u8]) -> (Vec<u8>, Vec<u8>) {
    if range_end.is_empty() || range_end == key {
        (key.to_vec(), Vec::new())
    } else {
        (key.to_vec(), range_end.to_vec())
    }
}

/// scope 判定用的访问区间：全 keyspace 用无上界符号表达，点订阅用点语义。
pub(crate) fn watch_scope_access(key: &[u8], range_end: &[u8]) -> ScopeAccess {
    if key.is_empty() && range_end.is_empty() {
        return ScopeAccess::range(
            Vec::new(),
            coord_core::kv_range::UNBOUNDED_RANGE_END.to_vec(),
        );
    }
    ScopeAccess::from_range(key.to_vec(), range_end.to_vec())
}

/// Watch 错误 → gRPC 状态（带稳定结构化错误码）。
pub(crate) fn watch_status(err: WatchError) -> Status {
    match err {
        WatchError::InvalidRequest(msg) => invalid_argument(msg),
        WatchError::CursorUnavailable { shard_id } => invalid_argument(format!(
            "watch cursor refers to unknown shard {shard_id}; resync from a fresh revision"
        )),
        WatchError::HistoryUnavailable {
            requested,
            oldest_available,
        } => attach(
            Status::out_of_range(format!(
                "watch history unavailable: commit_ts {requested} is before the oldest retained \
                 position (shard {}, commit_ts {}); resync required",
                oldest_available.shard_id, oldest_available.commit_ts
            )),
            CoordErrorCode::HistoryUnavailable,
        ),
        WatchError::Unavailable(msg) => {
            attach(Status::unavailable(msg), CoordErrorCode::Unavailable)
        }
        WatchError::Storage(err) => crate::server::map_err(err),
    }
}

fn invalid_argument(message: impl Into<String>) -> Status {
    attach(
        Status::new(Code::InvalidArgument, message),
        CoordErrorCode::InvalidArgument,
    )
}

// ──── 流服务 ────

type WatchStream = mpsc::Receiver<Result<watch_v2::WatchResponse, Status>>;

/// 一条 bidi 流的共享状态：订阅任务全部退出后递减 stream gauge。
struct StreamGuard {
    metrics: Option<Arc<Metrics>>,
}

impl StreamGuard {
    fn new(metrics: Option<Arc<Metrics>>) -> Self {
        if let Some(metrics) = &metrics {
            metrics.inc_kv_t_watch_streams();
        }
        Self { metrics }
    }
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        if let Some(metrics) = &self.metrics {
            metrics.dec_kv_t_watch_streams();
        }
    }
}

/// 订阅任务的所有权：`abort` 用来响应 cancel。
struct Subscription {
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// 启动一条 `coord.watch.v2` 流。
///
/// 首帧在这里同步解析：参数错误与 `HISTORY_UNAVAILABLE` 必须以 RPC 错误返回，
/// 而不是“先建立流再在流里报错”——后者会让客户端误以为订阅已生效。
pub(crate) async fn serve(
    node: &crate::server::CoordNode,
    request: Request<Streaming<watch_v2::WatchRequest>>,
) -> Result<tonic::Response<WatchStream>, Status> {
    node.kvt().gate(KvTCapability::Watch)?;
    super::services::guard_routing_epoch(node, request.metadata())?;
    let Some(shards) = node.commit_log_shards().cloned() else {
        return Err(attach(
            Status::failed_precondition(
                "coord.watch.v2 requires Commit Log shard assembly (multi-raft missing)",
            ),
            CoordErrorCode::FailedPrecondition,
        ));
    };

    let grants = request.extensions().get::<DeferredScopeGrants>().cloned();
    let mut stream = request.into_inner();
    let first = stream
        .message()
        .await?
        .ok_or_else(|| invalid_argument("empty watch request stream"))?;
    let create = match first.request {
        Some(watch_v2::watch_request::Request::Create(create)) => create,
        Some(watch_v2::watch_request::Request::Cancel(_)) | None => {
            return Err(invalid_argument("first watch request must be create"))
        }
    };
    let spec = spec_from_create(&create)?;
    guard_create(&create, grants.as_ref())?;

    let metrics = node.metrics().cloned();
    let coordinator = WatchCoordinator::new(Arc::clone(&shards));
    let first_cursor = coordinator
        .start(spec.start.clone())
        .await
        .map_err(watch_status)?;

    let (tx, rx) = mpsc::channel(WATCH_STREAM_BUFFER);
    let guard = Arc::new(StreamGuard::new(metrics.clone()));

    let next_id = Arc::new(AtomicI64::new(2));
    let (first_id, first_sub) = spawn_subscription(
        first_cursor,
        spec.filter,
        1,
        tx.clone(),
        Arc::clone(&shards),
        metrics.clone(),
        Arc::clone(&guard),
    );
    let mut subscriptions = std::collections::HashMap::new();
    subscriptions.insert(first_id, first_sub);

    tokio::spawn(read_requests(
        stream,
        shards,
        grants,
        tx,
        next_id,
        subscriptions,
        metrics,
        guard,
    ));

    Ok(tonic::Response::new(rx))
}

/// 读取同一流后续的 create/cancel；请求流结束后订阅继续（客户端仍可读响应）。
#[allow(clippy::too_many_arguments)]
async fn read_requests(
    mut stream: Streaming<watch_v2::WatchRequest>,
    shards: Arc<super::commit_log_shards::CommitLogShardManager>,
    grants: Option<DeferredScopeGrants>,
    tx: mpsc::Sender<Result<watch_v2::WatchResponse, Status>>,
    next_id: Arc<AtomicI64>,
    mut subscriptions: std::collections::HashMap<i64, Subscription>,
    metrics: Option<Arc<Metrics>>,
    guard: Arc<StreamGuard>,
) {
    while let Ok(Some(frame)) = stream.message().await {
        match frame.request {
            Some(watch_v2::watch_request::Request::Create(create)) => {
                let id = next_id.fetch_add(1, Ordering::Relaxed);
                let spec = match spec_from_create(&create) {
                    Ok(spec) => spec,
                    Err(status) => {
                        let _ = tx.send(Err(status)).await;
                        return;
                    }
                };
                if let Err(status) = guard_create(&create, grants.as_ref()) {
                    let _ = tx.send(Err(status)).await;
                    return;
                }
                let coordinator = WatchCoordinator::new(Arc::clone(&shards));
                let cursor = match coordinator.start(spec.start).await {
                    Ok(cursor) => cursor,
                    Err(err) => {
                        let _ = tx.send(Err(watch_status(err))).await;
                        return;
                    }
                };
                let (id, sub) = spawn_subscription(
                    cursor,
                    spec.filter,
                    id,
                    tx.clone(),
                    Arc::clone(&shards),
                    metrics.clone(),
                    Arc::clone(&guard),
                );
                subscriptions.insert(id, sub);
            }
            Some(watch_v2::watch_request::Request::Cancel(cancel)) => {
                // 未知 id 是幂等取消：订阅可能已因历史不足或流终止而结束。
                subscriptions.remove(&cancel.watch_id);
            }
            None => {}
        }
    }
    // 请求流结束不等于取消：客户端可以 half-close 后继续读响应。订阅句柄随本任务
    // 释放；drop 会 abort 它们，但只有响应通道关闭（客户端断开）才是真正的结束
    // 信号——因此这里显式 forget 掉句柄，让拉取任务继续运行到发送失败为止。
    for (_, sub) in subscriptions.drain() {
        std::mem::forget(sub);
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_subscription(
    cursor: WatchCursor,
    filter: WatchFilter,
    watch_id: i64,
    tx: mpsc::Sender<Result<watch_v2::WatchResponse, Status>>,
    shards: Arc<super::commit_log_shards::CommitLogShardManager>,
    metrics: Option<Arc<Metrics>>,
    guard: Arc<StreamGuard>,
) -> (i64, Subscription) {
    let handle = tokio::spawn(async move {
        // 任务持有 StreamGuard：最后一条订阅结束时才把 stream gauge 归零。
        let _guard = guard;
        run_subscription(cursor, filter, watch_id, tx, shards, metrics).await;
    });
    (watch_id, Subscription { handle })
}

async fn run_subscription(
    mut cursor: WatchCursor,
    filter: WatchFilter,
    watch_id: i64,
    tx: mpsc::Sender<Result<watch_v2::WatchResponse, Status>>,
    shards: Arc<super::commit_log_shards::CommitLogShardManager>,
    metrics: Option<Arc<Metrics>>,
) {
    let coordinator = WatchCoordinator::new(shards);
    loop {
        let batch = match coordinator.poll(cursor, &filter).await {
            Ok(batch) => batch,
            Err(WatchError::Unavailable(_)) => {
                tokio::time::sleep(WATCH_POLL_INTERVAL).await;
                continue;
            }
            Err(err) => {
                if matches!(err, WatchError::HistoryUnavailable { .. }) {
                    if let Some(metrics) = &metrics {
                        metrics.inc_kv_t_watch_history_unavailable();
                    }
                }
                let _ = deliver(&tx, Err(watch_status(err)), metrics.as_ref()).await;
                return;
            }
        };
        cursor = batch.cursor;
        let lag = batch.watermark.saturating_sub(
            batch
                .events
                .last()
                .map_or(cursor.commit_ts, |event| event.commit_ts),
        );
        if let Some(metrics) = &metrics {
            metrics.record_kv_t_watch_batch(batch.events.len() as u64, lag);
        }
        let response = watch_v2::WatchResponse {
            watch_id,
            events: batch.events.into_iter().map(event_to_proto).collect(),
            cursor: Some(watch_v2::WatchCursor {
                shard_id: cursor.shard_id,
                commit_ts: cursor.commit_ts as i64,
            }),
            watermark: batch.watermark as i64,
            sealed: batch.sealed,
        };
        if !deliver(&tx, Ok(response), metrics.as_ref()).await {
            return;
        }
        if !batch.has_more {
            tokio::time::sleep(WATCH_POLL_INTERVAL).await;
        }
    }
}

/// 有界发送：满时等待（背压），并计数；通道关闭 = 客户端断开。
async fn deliver(
    tx: &mpsc::Sender<Result<watch_v2::WatchResponse, Status>>,
    message: Result<watch_v2::WatchResponse, Status>,
    metrics: Option<&Arc<Metrics>>,
) -> bool {
    match tx.try_send(message) {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Full(message)) => {
            if let Some(metrics) = metrics {
                metrics.inc_kv_t_watch_backpressure();
            }
            tx.send(message).await.is_ok()
        }
        Err(mpsc::error::TrySendError::Closed(_)) => false,
    }
}

fn event_to_proto(event: WatchEvent) -> watch_v2::WatchEvent {
    watch_v2::WatchEvent {
        commit_ts: event.commit_ts as i64,
        changes: event
            .changes
            .into_iter()
            .map(|change| {
                let key = change.key;
                let r#type = if change.value.is_some() {
                    watch_v2::change::Type::Put as i32
                } else {
                    watch_v2::change::Type::Delete as i32
                };
                watch_v2::Change {
                    r#type,
                    prev_kv: change.prev.map(|prev| prev_to_proto(&key, prev)),
                    key,
                    value: change.value.unwrap_or_default(),
                }
            })
            .collect(),
    }
}

fn prev_to_proto(
    key: &[u8],
    prev: crate::storage::commit_log::CommitPrevValue,
) -> coord_proto::kv::v2::KeyValue {
    coord_proto::kv::v2::KeyValue {
        key: key.to_vec(),
        value: prev.value,
        create_revision: prev.create_revision,
        mod_revision: prev.mod_revision,
        version: prev.version,
        lease_id: prev.lease_id,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use coord_core::error_code::error_code_of;

    fn base_create() -> watch_v2::WatchCreateRequest {
        watch_v2::WatchCreateRequest {
            key: b"/app/a".to_vec(),
            range_end: Vec::new(),
            start_revision: 0,
            start_cursor: None,
            prev_kv: false,
            filter: 1,
        }
    }

    fn create_for(key: &[u8], range_end: &[u8]) -> watch_v2::WatchCreateRequest {
        watch_v2::WatchCreateRequest {
            key: key.to_vec(),
            range_end: range_end.to_vec(),
            ..base_create()
        }
    }

    fn proto_cursor(shard_id: u64, commit_ts: i64) -> watch_v2::WatchCursor {
        watch_v2::WatchCursor {
            shard_id,
            commit_ts,
        }
    }

    #[test]
    fn spec_requires_exactly_one_start_form() {
        let mut create = base_create();
        create.start_revision = 10;
        create.start_cursor = Some(proto_cursor(1, 5));
        let err = spec_from_create(&create).unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
        assert_eq!(error_code_of(&err).as_deref(), Some("INVALID_ARGUMENT"));
    }

    #[test]
    fn spec_validates_filter_revision_and_range() {
        for filter in [4, -1, 99] {
            let mut create = base_create();
            create.filter = filter;
            assert_eq!(
                spec_from_create(&create).unwrap_err().code(),
                Code::InvalidArgument
            );
        }
        let mut negative = base_create();
        negative.start_revision = -1;
        assert_eq!(
            spec_from_create(&negative).unwrap_err().code(),
            Code::InvalidArgument
        );
        let mut negative_cursor = base_create();
        negative_cursor.start_cursor = Some(proto_cursor(1, -1));
        assert_eq!(
            spec_from_create(&negative_cursor).unwrap_err().code(),
            Code::InvalidArgument
        );
        let mut reversed = base_create();
        reversed.key = b"z".to_vec();
        reversed.range_end = b"a".to_vec();
        assert_eq!(
            spec_from_create(&reversed).unwrap_err().code(),
            Code::InvalidArgument
        );
    }

    #[test]
    fn guard_denies_reserved_namespaces_and_object_space() {
        assert_eq!(
            guard_create(&create_for(b"/_pki/x", b""), None)
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        assert_eq!(
            guard_create(&create_for(b"/_lease/x", b""), None)
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        assert_eq!(
            guard_create(&create_for(b"/obj/a", b""), None)
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
        assert!(guard_create(&create_for(b"/app/a", b""), None).is_ok());
        assert!(guard_create(&create_for(b"", b""), None).is_ok());
    }

    #[test]
    fn guard_enforces_deferred_scope_prefixes() {
        let grants = DeferredScopeGrants {
            capability_id: "data:watch:subscribe".into(),
            grant_scopes: vec!["/app/a/".into()],
        };
        assert!(guard_create(&create_for(b"/app/a/x", b""), Some(&grants)).is_ok());
        assert_eq!(
            guard_create(&create_for(b"/app/b/x", b""), Some(&grants))
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        assert_eq!(
            guard_create(&create_for(b"", b""), Some(&grants))
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        let unrestricted = DeferredScopeGrants {
            capability_id: "data:watch:subscribe".into(),
            grant_scopes: vec![String::new()],
        };
        assert!(guard_create(&create_for(b"", b""), Some(&unrestricted)).is_ok());
    }

    #[test]
    fn history_unavailable_keeps_the_diagnostic_anchor() {
        let status = watch_status(WatchError::HistoryUnavailable {
            requested: 1,
            oldest_available: WatchCursor {
                shard_id: 2,
                commit_ts: 0,
            },
        });
        assert_eq!(status.code(), Code::OutOfRange);
        assert_eq!(
            error_code_of(&status).as_deref(),
            Some("HISTORY_UNAVAILABLE")
        );
        assert!(status.message().contains("shard 2"), "{}", status.message());
        assert!(
            status.message().contains("commit_ts 0"),
            "{}",
            status.message()
        );
    }

    #[test]
    fn scope_access_uses_v2_watch_semantics() {
        assert_eq!(
            watch_scope_access(b"", b"").range_end,
            coord_core::kv_range::UNBOUNDED_RANGE_END.to_vec()
        );
        assert_eq!(
            watch_scope_access(b"/app/a", b"").range_end,
            Vec::<u8>::new()
        );
        assert_eq!(
            watch_scope_access(b"/app/a", b"/app/c").range_end,
            b"/app/c".to_vec()
        );
    }
}
