//! `coord.txn.v2` 客户端会话（KV-T G3，#151）。
//!
//! 服务端会话只活在**创建它的协调节点**上（ADR-0015 D12.2），因此本包装在
//! `begin` 时固定一条 `Channel`，后续 `Read`/`Commit`/`Abort` 全部复用它；
//! 不做换 leader 重试（换节点必然 `TXN_NOT_FOUND`，重试只会白跑整个事务）。
//!
//! 冲突不自动重试：`commit` 返回 [`TxnCommit::Conflict`]，调用方必须换新的
//! `start_ts` 重跑读阶段。隐藏重试会把「读到旧快照」变成不可观测的重复读。

use coord_core::error::{Error, Result};
use coord_core::error_code::{error_code_of, CoordErrorCode};
use coord_proto::kv::v2 as kv_v2;
use coord_proto::txn::v2 as proto;

use crate::client::{from_status, Client};
use crate::credential::AuthedChannel;
use crate::retry::RetryState;

/// `Commit` 的结果：冲突是业务结果，不是传输失败。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnCommit {
    /// 决策已落盘；各 Region 的 apply 是异步的（用 `read_ts >= commit_ts` 读）。
    Committed { commit_ts: u64 },
    /// 读集/谓词在 `(start_ts, commit_ts]` 内被写过：换新 `start_ts` 重跑。
    Conflict,
}

/// v2 事务客户端：只负责 `Begin` 选会话，读写都走 [`TxnV2Session`]。
#[derive(Clone)]
pub struct TxnV2Client {
    client: Client,
}

impl TxnV2Client {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// 开启服务端会话并固定协调节点。
    ///
    /// `start_ts = 0` 由服务端向 TSO 申请；显式 `start_ts` 必须来自 TSO 批次，
    /// 且 `<=` 目标节点当前 `safe_ts`（否则服务端在 `read_ts_wait_ms` 内等待）。
    pub async fn begin(&self, start_ts: u64, ttl_ms: u32) -> Result<TxnV2Session> {
        let start_ts = ts_to_i64("start_ts", start_ts)?;
        let (endpoint, channel) = self.client.get_leader_channel().await?;
        let call_channel = channel.clone();
        let resp = retry_epoch_stale(&self.client, move || {
            let mut stub = proto::txn_client::TxnClient::new(call_channel.clone());
            let mut request = tonic::Request::new(proto::BeginRequest { start_ts, ttl_ms });
            self.client.attach_routing_epoch(&mut request);
            async move { stub.begin(request).await }
        })
        .await;
        match resp {
            Ok(resp) => {
                let inner = resp.into_inner();
                let start_ts = u64::try_from(inner.start_ts)
                    .map_err(|_| Error::Internal("negative start_ts in response".into()))?;
                Ok(TxnV2Session {
                    client: self.client.clone(),
                    endpoint,
                    channel: Some(channel),
                    txn_id: inner.txn_id,
                    start_ts,
                })
            }
            Err(status) => {
                self.client.return_channel(&endpoint, channel);
                Err(txn_error(status))
            }
        }
    }
}

/// 绑定到单个协调节点的事务会话；`drop` 时把连接归还连接池。
pub struct TxnV2Session {
    client: Client,
    endpoint: String,
    channel: Option<AuthedChannel>,
    txn_id: Vec<u8>,
    start_ts: u64,
}

impl TxnV2Session {
    /// 会话标识（同节点重试时回传）。
    pub fn txn_id(&self) -> &[u8] {
        &self.txn_id
    }

    /// 本事务的快照时间戳（所有读共用）。
    pub fn start_ts(&self) -> u64 {
        self.start_ts
    }

    /// 读快照并记录读集；`range_end` 为空 = point read。
    ///
    /// 返回 `(kvs, read_ts)`，`read_ts` 恒等于 `start_ts`。
    pub async fn read(
        &self,
        key: &[u8],
        range_end: &[u8],
        limit: i64,
        keys_only: bool,
    ) -> Result<(Vec<kv_v2::KeyValue>, u64)> {
        let channel = self.channel()?.clone();
        let txn_id = self.txn_id.clone();
        let key = key.to_vec();
        let range_end = range_end.to_vec();
        let resp = retry_epoch_stale(&self.client, move || {
            let mut stub = proto::txn_client::TxnClient::new(channel.clone());
            let mut request = tonic::Request::new(proto::ReadRequest {
                txn_id: txn_id.clone(),
                key: key.clone(),
                range_end: range_end.clone(),
                limit,
                keys_only,
            });
            self.client.attach_routing_epoch(&mut request);
            async move { stub.read(request).await }
        })
        .await
        .map_err(txn_error)?
        .into_inner();
        let read_ts = u64::try_from(resp.read_ts)
            .map_err(|_| Error::Internal("negative read_ts in response".into()))?;
        Ok((resp.kvs, read_ts))
    }

    /// 提交写集；`request_id` 非空时在会话 TTL 内幂等（响应丢失可直接重试）。
    ///
    /// 成功返回即决策已落盘，不等待各 Region apply。
    pub async fn commit(
        self,
        mutations: Vec<proto::Mutation>,
        request_id: &[u8],
    ) -> Result<TxnCommit> {
        let channel = self.channel()?.clone();
        let txn_id = self.txn_id.clone();
        let request_id = request_id.to_vec();
        let client = self.client.clone();
        let call_client = client.clone();
        let resp = retry_epoch_stale(&client, move || {
            let mut stub = proto::txn_client::TxnClient::new(channel.clone());
            let mut request = tonic::Request::new(proto::CommitRequest {
                txn_id: txn_id.clone(),
                mutations: mutations.clone(),
                request_id: request_id.clone(),
            });
            call_client.attach_routing_epoch(&mut request);
            async move { stub.commit(request).await }
        })
        .await;
        match resp {
            Ok(resp) => {
                let commit_ts = u64::try_from(resp.into_inner().commit_ts)
                    .map_err(|_| Error::Internal("negative commit_ts in response".into()))?;
                Ok(TxnCommit::Committed { commit_ts })
            }
            Err(status) if is_txn_conflict(&status) => Ok(TxnCommit::Conflict),
            Err(status) => Err(txn_error(status)),
        }
    }

    /// 幂等放弃会话；未知/已过期会话返回 `Ok`。
    pub async fn abort(self) -> Result<()> {
        let channel = self.channel()?.clone();
        let txn_id = self.txn_id.clone();
        let client = self.client.clone();
        let call_client = client.clone();
        retry_epoch_stale(&client, move || {
            let mut stub = proto::txn_client::TxnClient::new(channel.clone());
            let mut request = tonic::Request::new(proto::AbortRequest {
                txn_id: txn_id.clone(),
            });
            call_client.attach_routing_epoch(&mut request);
            async move { stub.abort(request).await }
        })
        .await
        .map_err(txn_error)?;
        Ok(())
    }

    fn channel(&self) -> Result<&AuthedChannel> {
        self.channel
            .as_ref()
            .ok_or_else(|| Error::Internal("txn session channel already released".into()))
    }
}

impl Drop for TxnV2Session {
    fn drop(&mut self) {
        if let Some(channel) = self.channel.take() {
            self.client.return_channel(&self.endpoint, channel);
        }
    }
}

/// 只在 `EPOCH_STALE` 上重试：入口守卫拒绝的请求没有副作用，学到服务端回传的
/// 新版本后重放同一请求；其它错误原样交给调用方映射（`TXN_CONFLICT` 等业务结果
/// 不能被吞进重试）。
async fn retry_epoch_stale<T, F, Fut>(
    client: &Client,
    mut attempt: F,
) -> std::result::Result<T, tonic::Status>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<T, tonic::Status>>,
{
    let mut retry = RetryState::new(client.retry_config());
    loop {
        match attempt().await {
            Ok(value) => return Ok(value),
            Err(status) => {
                client.learn_routing_epoch(&status);
                if error_code_of(&status).as_deref() != Some(CoordErrorCode::EpochStale.as_str()) {
                    return Err(status);
                }
                match retry.next_attempt() {
                    Some(wait) => tokio::time::sleep(wait).await,
                    None => return Err(status),
                }
            }
        }
    }
}

/// `PUT` mutation：`range_end` 必须为空（服务端拒绝其它形状）。
pub fn put_mutation(key: &[u8], value: &[u8], lease_id: i64) -> proto::Mutation {
    proto::Mutation {
        op: proto::mutation::Op::Put as i32,
        key: key.to_vec(),
        range_end: Vec::new(),
        value: value.to_vec(),
        lease_id,
    }
}

/// 单 key `DELETE` mutation。区间删除尚未交付（服务端 `UNIMPLEMENTED`）。
pub fn delete_mutation(key: &[u8]) -> proto::Mutation {
    proto::Mutation {
        op: proto::mutation::Op::Delete as i32,
        key: key.to_vec(),
        range_end: Vec::new(),
        value: Vec::new(),
        lease_id: 0,
    }
}

fn ts_to_i64(name: &str, ts: u64) -> Result<i64> {
    i64::try_from(ts)
        .map_err(|_| Error::InvalidArgument(format!("{name} {ts} exceeds i64 wire range")))
}

fn is_txn_conflict(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::Aborted
        && error_code_of(status).as_deref() == Some(CoordErrorCode::TxnConflict.as_str())
}

/// v2 事务错误：先按稳定错误码分支，再回落到通用 gRPC 映射。
pub(crate) fn txn_error(status: tonic::Status) -> Error {
    match error_code_of(&status).as_deref() {
        Some(code) if code == CoordErrorCode::TxnConflict.as_str() => {
            // 只有 commit 会把冲突当成功结果；其它 RPC 看到冲突说明服务端状态异常。
            Error::Internal(format!("unexpected TXN_CONFLICT: {}", status.message()))
        }
        Some(code) if code == CoordErrorCode::TxnNotFound.as_str() => Error::V2 {
            code: CoordErrorCode::TxnNotFound,
            message: status.message().to_string(),
        },
        Some(code)
            if code == CoordErrorCode::TxnTooLarge.as_str()
                || code == CoordErrorCode::ReadSetTooLarge.as_str() =>
        {
            Error::V2 {
                code: CoordErrorCode::from_wire(code).unwrap_or(CoordErrorCode::Internal),
                message: status.message().to_string(),
            }
        }
        _ => from_status(status),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use coord_core::error_code::attach;

    #[test]
    fn commit_conflict_is_a_result_not_an_error() {
        let status = attach(
            tonic::Status::aborted("conflict"),
            CoordErrorCode::TxnConflict,
        );
        assert!(is_txn_conflict(&status));
        assert!(!is_txn_conflict(&attach(
            tonic::Status::aborted("missing"),
            CoordErrorCode::TxnNotFound
        )));
    }

    #[test]
    fn txn_error_maps_protocol_codes() {
        let missing = attach(tonic::Status::aborted("gone"), CoordErrorCode::TxnNotFound);
        assert!(matches!(
            txn_error(missing),
            Error::V2 {
                code: CoordErrorCode::TxnNotFound,
                ..
            }
        ));

        let too_large = attach(
            tonic::Status::invalid_argument("big"),
            CoordErrorCode::TxnTooLarge,
        );
        assert!(matches!(
            txn_error(too_large),
            Error::V2 {
                code: CoordErrorCode::TxnTooLarge,
                ..
            }
        ));

        let read_set = attach(
            tonic::Status::invalid_argument("read set"),
            CoordErrorCode::ReadSetTooLarge,
        );
        assert!(matches!(
            txn_error(read_set),
            Error::V2 {
                code: CoordErrorCode::ReadSetTooLarge,
                ..
            }
        ));

        // 未附码的 UNAVAILABLE 仍按传输错误处理。
        assert!(matches!(
            txn_error(tonic::Status::unavailable("down")),
            Error::ClusterUnavailable(_)
        ));
    }

    #[test]
    fn mutation_helpers_build_single_key_ops() {
        let put = put_mutation(b"k", b"v", 7);
        assert_eq!(put.op(), proto::mutation::Op::Put);
        assert!(put.range_end.is_empty());
        assert_eq!(put.lease_id, 7);

        let delete = delete_mutation(b"k");
        assert_eq!(delete.op(), proto::mutation::Op::Delete);
        assert!(delete.range_end.is_empty());
    }
}
