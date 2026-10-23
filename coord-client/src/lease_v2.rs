//! `coord.lease.v2` 客户端（#172）。
//!
//! 与服务端 `coord-server/src/kvt/lease.rs` 对齐：租约元数据只存在 region 0，
//! `Revoke` 的跨区 key 删除由服务端用同一个 `commit_ts` 原子应用；客户端只透传
//! `lease_id`/`request_id`，不在本地保存租约状态。
//!
//! epoch 口径与 `KvV2Client` 相同：每次尝试都附上已学到的 routing epoch，
//! `EPOCH_STALE` 有界重试；其他错误按码映射返回。

use coord_core::error::{Error, Result};
use coord_proto::lease::v2 as lease_v2;

use crate::client::Client;

/// `LeaseGrant` 的结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseV2Grant {
    pub lease_id: i64,
    pub ttl_seconds: i64,
    /// 发放时的 TSO；只读参考，不参与 revision 语义。
    pub granted_ts: u64,
}

/// `LeaseTimeToLive` 的结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseV2State {
    pub lease_id: i64,
    /// 剩余 TTL；`<= 0` = 已过期。
    pub ttl_seconds: i64,
    pub granted_ttl_seconds: i64,
    /// `keys=true` 时返回；服务端可能按上限截断。
    pub keys: Vec<Vec<u8>>,
}

/// `coord.lease.v2` 客户端。
#[derive(Clone)]
pub struct LeaseV2Client {
    client: Client,
}

impl LeaseV2Client {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// 授予租约；`lease_id = 0` 表示由服务端分配，`request_id` 为空表示不去重。
    pub async fn grant(
        &self,
        ttl_seconds: i64,
        lease_id: i64,
        request_id: &[u8],
    ) -> Result<LeaseV2Grant> {
        let client = self.client.clone();
        let request_id = request_id.to_vec();
        let grant = self
            .client
            .execute_write_with_retry(move |channel| {
                let mut stub = lease_v2::lease_client::LeaseClient::new(channel);
                let mut request = tonic::Request::new(lease_v2::LeaseGrantRequest {
                    ttl_seconds,
                    lease_id,
                    request_id: request_id.clone(),
                });
                client.attach_routing_epoch(&mut request);
                async move { stub.lease_grant(request).await.map(|r| r.into_inner()) }
            })
            .await?;
        Ok(LeaseV2Grant {
            lease_id: grant.lease_id,
            ttl_seconds: grant.ttl_seconds,
            granted_ts: u64::try_from(grant.granted_ts)
                .map_err(|_| Error::Internal("negative granted_ts in response".into()))?,
        })
    }

    /// 撤销租约；返回绑定 key 被删除的 `commit_ts`。
    ///
    /// `0` = 本次没有新决策（幂等重放/本就过期），不是错误。
    pub async fn revoke(&self, lease_id: i64, request_id: &[u8]) -> Result<u64> {
        let client = self.client.clone();
        let request_id = request_id.to_vec();
        let revoke = self
            .client
            .execute_write_with_retry(move |channel| {
                let mut stub = lease_v2::lease_client::LeaseClient::new(channel);
                let mut request = tonic::Request::new(lease_v2::LeaseRevokeRequest {
                    lease_id,
                    request_id: request_id.clone(),
                });
                client.attach_routing_epoch(&mut request);
                async move {
                    stub.lease_revoke(request)
                        .await
                        .map(|r| r.into_inner().commit_ts)
                }
            })
            .await?;
        u64::try_from(revoke).map_err(|_| Error::Internal("negative commit_ts in response".into()))
    }

    /// 单次续约：打开双向流发一帧，读回剩余 TTL。
    ///
    /// 后台周期性续约需要生命周期/停止语义，属于 Agent 迁移的范围，不在这里
    /// 用一个无主的 `tokio::spawn` 代替。
    pub async fn keep_alive(&self, lease_id: i64) -> Result<i64> {
        let client = self.client.clone();
        let response = self
            .client
            .execute_v2_call_with_epoch_retry(move |channel| {
                let mut stub = lease_v2::lease_client::LeaseClient::new(channel);
                let mut request =
                    tonic::Request::new(tokio_stream::once(lease_v2::LeaseKeepAliveRequest {
                        lease_id,
                    }));
                client.attach_routing_epoch(&mut request);
                async move {
                    let mut stream = stub.lease_keep_alive(request).await?.into_inner();
                    match stream.message().await? {
                        Some(response) => Ok(response),
                        None => Err(tonic::Status::internal(
                            "keep-alive stream closed before a response",
                        )),
                    }
                }
            })
            .await?;
        Ok(response.ttl_seconds)
    }

    /// 查询剩余 TTL（`keys = true` 时附带绑定的 key 列表）。
    pub async fn time_to_live(&self, lease_id: i64, keys: bool) -> Result<LeaseV2State> {
        let client = self.client.clone();
        let state = self
            .client
            .execute_v2_call_with_epoch_retry(move |channel| {
                let mut stub = lease_v2::lease_client::LeaseClient::new(channel);
                let mut request =
                    tonic::Request::new(lease_v2::LeaseTimeToLiveRequest { lease_id, keys });
                client.attach_routing_epoch(&mut request);
                async move {
                    stub.lease_time_to_live(request)
                        .await
                        .map(|r| r.into_inner())
                }
            })
            .await?;
        Ok(LeaseV2State {
            lease_id: state.lease_id,
            ttl_seconds: state.ttl_seconds,
            granted_ttl_seconds: state.granted_ttl_seconds,
            keys: state.keys,
        })
    }
}
