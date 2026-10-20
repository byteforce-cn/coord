//! `coord.watch.v2` 客户端订阅（KV-T G4，#158）。
//!
//! 服务端是一条 bidi 流：create 之后持续下发批，批里带 `cursor`/`watermark`/
//! `sealed`。本包装把 `watch_id` 隐藏掉（一条订阅一条流），并在断线时用**最后一
//! 批的 cursor** 重开流；因此投递语义是**至少一次**：断线前已发出但客户端没收到
//! 的批会在重连后重放。调用方需要严格一次时按 `commit_ts` 幂等。
//!
//! `HISTORY_UNAVAILABLE` 是终点：保留窗口已回收请求位点，重连只会永远失败，
//! 所以直接映射成 `Error::HistoryUnavailable` 并结束订阅（调用方全量重同步）。

use coord_core::error::{Error, Result};
use coord_core::error_code::error_code_of;
use coord_proto::watch::v2 as proto;
use tokio::sync::{mpsc, oneshot, watch};

use crate::client::{from_status, Client};
use crate::retry::RetryState;

/// 服务端游标：同一 shard 内的精确续订位置。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatchV2Cursor {
    pub shard_id: u64,
    pub commit_ts: u64,
}

impl WatchV2Cursor {
    pub(crate) fn to_proto(self) -> proto::WatchCursor {
        proto::WatchCursor {
            shard_id: self.shard_id,
            commit_ts: self.commit_ts as i64,
        }
    }
}

impl From<proto::WatchCursor> for WatchV2Cursor {
    fn from(value: proto::WatchCursor) -> Self {
        Self {
            shard_id: value.shard_id,
            // 服务端不会发负数；防御性截断避免把“畸形响应”变成一次越界重连。
            commit_ts: u64::try_from(value.commit_ts).unwrap_or(0),
        }
    }
}

/// 事件类型过滤（与服务端 `coord.watch.v2.EventFilter` 对应）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WatchV2Filter {
    #[default]
    All,
    PutsOnly,
    DeletesOnly,
}

impl WatchV2Filter {
    pub(crate) fn to_proto(self) -> i32 {
        match self {
            Self::All => proto::EventFilter::All as i32,
            Self::PutsOnly => proto::EventFilter::PutsOnly as i32,
            Self::DeletesOnly => proto::EventFilter::DeletesOnly as i32,
        }
    }
}

/// 订阅参数：起点二选一（`from_revision` 或 `from_cursor`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchV2Options {
    pub key: Vec<u8>,
    pub range_end: Vec<u8>,
    pub prev_kv: bool,
    pub filter: WatchV2Filter,
    pub start_revision: u64,
    pub start_cursor: Option<WatchV2Cursor>,
}

impl WatchV2Options {
    /// 单 key 订阅，从当前 watermark 之后开始。
    pub fn new(key: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            range_end: Vec::new(),
            prev_kv: false,
            filter: WatchV2Filter::All,
            start_revision: 0,
            start_cursor: None,
        }
    }

    /// `[key, range_end)` 区间；空 `range_end` 表示单 key。
    pub fn with_range(mut self, range_end: impl Into<Vec<u8>>) -> Self {
        self.range_end = range_end.into();
        self
    }

    pub fn with_prev_kv(mut self, prev_kv: bool) -> Self {
        self.prev_kv = prev_kv;
        self
    }

    pub fn with_filter(mut self, filter: WatchV2Filter) -> Self {
        self.filter = filter;
        self
    }

    /// 从 `revision`（含）开始；清除显式 cursor，避免两个起点歧义。
    pub fn from_revision(mut self, revision: u64) -> Self {
        self.start_revision = revision;
        self.start_cursor = None;
        self
    }

    /// 从精确 cursor 续订；清除 `start_revision`。
    pub fn from_cursor(mut self, cursor: WatchV2Cursor) -> Self {
        self.start_revision = 0;
        self.start_cursor = Some(cursor);
        self
    }
}

/// 单条 change；`prev` 只在 `prev_kv=true` 且此前存在时填充。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchV2Change {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub prev: Option<coord_proto::kv::v2::KeyValue>,
}

/// 一个事务 = 一个原子事件。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchV2Event {
    pub commit_ts: u64,
    pub changes: Vec<WatchV2Change>,
}

/// 一批下发（空批 ack 不向上投递，只更新 cursor）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchV2Batch {
    pub events: Vec<WatchV2Event>,
    pub cursor: Option<WatchV2Cursor>,
    pub watermark: u64,
    pub sealed: bool,
}

/// 订阅句柄：`recv()` 拿批，`cursor()` 拿最近一次持久化位置，`cancel()` 主动停。
pub struct WatchV2Subscription {
    rx: mpsc::Receiver<Result<WatchV2Batch>>,
    cursor: watch::Receiver<Option<WatchV2Cursor>>,
    stop: Option<oneshot::Sender<()>>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl WatchV2Subscription {
    /// 下一条批；`None` = 订阅结束（取消或不可恢复错误已下发）。
    pub async fn recv(&mut self) -> Option<Result<WatchV2Batch>> {
        self.rx.recv().await
    }

    /// 最近一次收到的批之后的位置；重连/重启后从这里续订。
    pub fn cursor(&self) -> Option<WatchV2Cursor> {
        *self.cursor.borrow()
    }

    /// 主动取消并等待后台任务退出。
    pub async fn cancel(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
    }
}

impl Drop for WatchV2Subscription {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

/// v2 Watch 客户端：`Client::watch_v2()` 构造。
#[derive(Clone)]
pub struct WatchV2Client {
    client: Client,
}

impl WatchV2Client {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// 建立订阅。连接/首帧错误通过 [`WatchV2Subscription::recv`] 返回。
    pub async fn watch(&self, options: WatchV2Options) -> Result<WatchV2Subscription> {
        if options.start_cursor.is_some() && options.start_revision != 0 {
            return Err(Error::InvalidArgument(
                "start_revision and start_cursor are mutually exclusive".into(),
            ));
        }
        let (tx, rx) = mpsc::channel(64);
        let (cursor_tx, cursor_rx) = watch::channel(options.start_cursor);
        let (stop_tx, stop_rx) = oneshot::channel();
        let handle = tokio::spawn(run(self.client.clone(), options, tx, cursor_tx, stop_rx));
        Ok(WatchV2Subscription {
            rx,
            cursor: cursor_rx,
            stop: Some(stop_tx),
            handle: Some(handle),
        })
    }
}

async fn run(
    client: Client,
    options: WatchV2Options,
    tx: mpsc::Sender<Result<WatchV2Batch>>,
    cursor_tx: watch::Sender<Option<WatchV2Cursor>>,
    mut stop: oneshot::Receiver<()>,
) {
    let mut retry = RetryState::new(client.retry_config());
    // 已确认消费到的位置；断线后用它重开流。`None` 表示还没收到过任何响应。
    let mut resume = options.start_cursor;
    loop {
        let create = proto::WatchCreateRequest {
            key: options.key.clone(),
            range_end: options.range_end.clone(),
            start_revision: if resume.is_some() {
                0
            } else {
                i64::try_from(options.start_revision).unwrap_or(i64::MAX)
            },
            start_cursor: resume.map(WatchV2Cursor::to_proto),
            prev_kv: options.prev_kv,
            filter: options.filter.to_proto(),
        };
        let opened = tokio::select! {
            _ = &mut stop => return,
            res = open_stream(&client, create) => res,
        };
        match opened {
            Ok(mut stream) => loop {
                let next = tokio::select! {
                    _ = &mut stop => return,
                    msg = stream.message() => msg,
                };
                match next {
                    Ok(Some(response)) => {
                        if let Some(cursor) = response.cursor {
                            resume = Some(WatchV2Cursor::from(cursor));
                            let _ = cursor_tx.send(resume);
                        }
                        let events = match response
                            .events
                            .into_iter()
                            .map(event_from_proto)
                            .collect::<Result<Vec<_>>>()
                        {
                            Ok(events) => events,
                            Err(err) => {
                                let _ = tx.send(Err(err)).await;
                                return;
                            }
                        };
                        let batch = WatchV2Batch {
                            events,
                            cursor: resume,
                            watermark: u64::try_from(response.watermark).unwrap_or(0),
                            sealed: response.sealed,
                        };
                        // ack（空批）只用来对齐 cursor：调用方等的是事件。
                        if batch.events.is_empty() {
                            continue;
                        }
                        retry.reset();
                        let sent = tokio::select! {
                            _ = &mut stop => return,
                            sent = tx.send(Ok(batch)) => sent,
                        };
                        if sent.is_err() {
                            return;
                        }
                    }
                    Ok(None) => break,
                    Err(status) if retryable(&status) => break,
                    Err(status) => {
                        let _ = tokio::select! {
                            _ = &mut stop => return,
                            sent = tx.send(Err(watch_error(status))) => sent,
                        };
                        return;
                    }
                }
            },
            Err(status) if retryable(&status) => {}
            Err(status) => {
                let _ = tokio::select! {
                    _ = &mut stop => return,
                    sent = tx.send(Err(watch_error(status))) => sent,
                };
                return;
            }
        }

        match retry.next_attempt() {
            Some(wait) => {
                tokio::select! {
                    _ = &mut stop => return,
                    _ = tokio::time::sleep(wait) => {}
                }
            }
            None => {
                let _ = tokio::select! {
                    _ = &mut stop => return,
                    sent = tx.send(Err(Error::ClusterUnavailable(
                        "watch reconnect attempts exhausted".into(),
                    ))) => sent,
                };
                return;
            }
        }
    }
}

async fn open_stream(
    client: &Client,
    create: proto::WatchCreateRequest,
) -> std::result::Result<tonic::Streaming<proto::WatchResponse>, tonic::Status> {
    let (_endpoint, channel) = client
        .get_leader_watch_channel()
        .await
        .map_err(|err| tonic::Status::unavailable(err.to_string()))?;
    let mut stub = proto::watch_client::WatchClient::new(channel);
    let (req_tx, req_rx) = mpsc::channel(2);
    req_tx
        .send(proto::WatchRequest {
            request: Some(proto::watch_request::Request::Create(create)),
        })
        .await
        .map_err(|_| tonic::Status::internal("watch request channel closed"))?;
    // 收端由服务端 first-frame 驱动；create 已入队后即可关闭请求侧（服务端在
    // half-close 后继续推事件）。保持 req_tx 存活只会多占一条空流。
    drop(req_tx);
    stub.watch(tonic::Request::new(
        tokio_stream::wrappers::ReceiverStream::new(req_rx),
    ))
    .await
    .map(|response| response.into_inner())
}

fn event_from_proto(event: proto::WatchEvent) -> Result<WatchV2Event> {
    let commit_ts = u64::try_from(event.commit_ts)
        .map_err(|_| Error::Internal("watch event carries a negative commit_ts".into()))?;
    let mut changes = Vec::with_capacity(event.changes.len());
    for change in event.changes {
        let value = match proto::change::Type::try_from(change.r#type) {
            Ok(proto::change::Type::Put) => Some(change.value),
            Ok(proto::change::Type::Delete) => None,
            Ok(proto::change::Type::Unspecified) | Err(_) => {
                return Err(Error::Internal(
                    "watch event carries an unspecified change type".into(),
                ))
            }
        };
        changes.push(WatchV2Change {
            key: change.key,
            value,
            prev: change.prev_kv,
        });
    }
    Ok(WatchV2Event { commit_ts, changes })
}

fn watch_error(status: tonic::Status) -> Error {
    match error_code_of(&status).as_deref() {
        Some("HISTORY_UNAVAILABLE") => Error::HistoryUnavailable {
            revision: 0,
            detail: status.message().to_string(),
        },
        _ => from_status(status),
    }
}

/// 可重连的失败：传输/流级别。`HISTORY_UNAVAILABLE` 与请求错误不在其中。
fn retryable(status: &tonic::Status) -> bool {
    if error_code_of(status).as_deref() == Some("HISTORY_UNAVAILABLE") {
        return false;
    }
    matches!(
        status.code(),
        tonic::Code::Unavailable
            | tonic::Code::DeadlineExceeded
            | tonic::Code::ResourceExhausted
            | tonic::Code::Cancelled
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_round_trips_and_filters_map_to_contract_values() {
        let cursor = WatchV2Cursor {
            shard_id: 3,
            commit_ts: 42,
        };
        assert_eq!(WatchV2Cursor::from(cursor.to_proto()), cursor);
        assert_eq!(WatchV2Filter::All.to_proto(), 1);
        assert_eq!(WatchV2Filter::PutsOnly.to_proto(), 2);
        assert_eq!(WatchV2Filter::DeletesOnly.to_proto(), 3);
    }

    #[test]
    fn options_keep_start_forms_exclusive() {
        let cursor = WatchV2Cursor {
            shard_id: 1,
            commit_ts: 9,
        };
        let options = WatchV2Options::new(b"/app/a").from_cursor(cursor);
        assert_eq!(options.start_cursor, Some(cursor));
        assert_eq!(options.start_revision, 0);
        let options = options.from_revision(11);
        assert_eq!(options.start_cursor, None);
        assert_eq!(options.start_revision, 11);
    }

    #[test]
    fn retryable_statuses_exclude_history_unavailable() {
        use coord_core::error_code::{attach, CoordErrorCode};

        assert!(retryable(&tonic::Status::unavailable("x")));
        assert!(retryable(&tonic::Status::deadline_exceeded("x")));
        assert!(retryable(&tonic::Status::resource_exhausted("x")));
        assert!(!retryable(&attach(
            tonic::Status::invalid_argument("x"),
            CoordErrorCode::InvalidArgument
        )));
        assert!(!retryable(&attach(
            tonic::Status::out_of_range("x"),
            CoordErrorCode::HistoryUnavailable
        )));
    }

    #[test]
    fn history_unavailable_maps_to_a_terminal_error() {
        use coord_core::error_code::{attach, CoordErrorCode};

        let err = watch_error(attach(
            tonic::Status::out_of_range("history gone"),
            CoordErrorCode::HistoryUnavailable,
        ));
        assert!(matches!(err, Error::HistoryUnavailable { .. }));
    }

    #[test]
    fn change_type_round_trips() {
        let event = event_from_proto(proto::WatchEvent {
            commit_ts: 7,
            changes: vec![
                proto::Change {
                    r#type: proto::change::Type::Put as i32,
                    key: b"a".to_vec(),
                    value: b"v".to_vec(),
                    prev_kv: None,
                },
                proto::Change {
                    r#type: proto::change::Type::Delete as i32,
                    key: b"b".to_vec(),
                    value: Vec::new(),
                    prev_kv: None,
                },
            ],
        })
        .expect("decode");
        assert_eq!(event.commit_ts, 7);
        assert_eq!(event.changes[0].value.as_deref(), Some(b"v".as_slice()));
        assert_eq!(event.changes[1].value, None);
    }
}
