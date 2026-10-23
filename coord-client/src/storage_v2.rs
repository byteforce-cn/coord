//! `coord.storage.v2` 客户端（#172）。
//!
//! 与 v1 `StorageClient` 的差别只在 revision 口径：manifest 的 `commit_ts` 由
//! 服务端在 Commit 时分配并返回，`Get`/`Stat` 可带 `read_ts` 做快照读（0 =
//! 当前 safe_ts）。流式分片、配额、`/obj/` 固定区间等语义由服务端契约固定，
//! 客户端只做分组与字段透传。

use coord_core::error::{Error, Result};
use coord_proto::storage::v2 as storage_v2;
use coord_proto::storage::v2::storage_client::StorageClient as StorageV2Stub;
use storage_v2::{
    get_response, put_request, DeleteRequest, GetRequest, ObjectStat, PutMeta, PutRequest,
    StatRequest,
};

use crate::client::{ts_to_i64, Client, DEFAULT_OBJECT_CHUNK_SIZE, STORAGE_DECODE_LIMIT};

/// 对象上传结果（v2：`commit_ts` 是 manifest 的全局时间戳）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutObjectV2Result {
    pub commit_ts: u64,
    pub size: u64,
    pub chunks: u64,
}

/// 对象下载结果（Get 首条 stat + 全量数据）。
#[derive(Debug, Clone)]
pub struct ObjectDataV2 {
    pub stat: ObjectStat,
    pub data: Vec<u8>,
}

/// `coord.storage.v2` 客户端。
///
/// - `put` 以 `(bucket, object_id)` 为幂等键：已 Committed 的对象再次 Put 返回
///   `ALREADY_EXISTS`（v2 仍不支持覆盖写）；
/// - `read_ts` 早于 manifest 的 compact 水位 ⇒ `OUT_OF_RANGE`；
/// - 上传中断的残留由服务端 GC 回收，客户端可 `delete` 后整体重试。
#[derive(Clone)]
pub struct StorageV2Client {
    client: Client,
}

impl StorageV2Client {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// 上传整对象（默认 chunk 4MiB）。
    pub async fn put(
        &self,
        bucket: &str,
        object_id: &[u8],
        data: &[u8],
    ) -> Result<PutObjectV2Result> {
        self.put_chunked(bucket, object_id, data, DEFAULT_OBJECT_CHUNK_SIZE)
            .await
    }

    /// 上传整对象（自定义 chunk 分片；≤ 4MiB 且 ≤ 服务端 chunk_size_bytes）。
    pub async fn put_chunked(
        &self,
        bucket: &str,
        object_id: &[u8],
        data: &[u8],
        chunk_size: usize,
    ) -> Result<PutObjectV2Result> {
        if chunk_size == 0 || chunk_size > 4 * 1024 * 1024 {
            return Err(Error::InvalidArgument(
                "object chunk_size must be in (0, 4MiB]".into(),
            ));
        }
        if data.is_empty() {
            return Err(Error::InvalidArgument(
                "object data must not be empty".into(),
            ));
        }
        let client = self.client.clone();
        let bucket = bucket.to_string();
        let object_id = object_id.to_vec();
        let data = data.to_vec();
        let response = self
            .client
            .execute_write_with_retry(move |channel| {
                let mut stub =
                    StorageV2Stub::new(channel).max_decoding_message_size(STORAGE_DECODE_LIMIT);
                // 消息流每次尝试都重建：重试不能复用已被消费的 stream。
                let mut msgs = Vec::with_capacity(data.len() / chunk_size + 2);
                msgs.push(PutRequest {
                    part: Some(put_request::Part::Meta(PutMeta {
                        bucket: bucket.clone(),
                        object_id: object_id.clone(),
                        total_size: data.len() as i64,
                    })),
                });
                for chunk in data.chunks(chunk_size) {
                    msgs.push(PutRequest {
                        part: Some(put_request::Part::Chunk(chunk.to_vec())),
                    });
                }
                let mut request = tonic::Request::new(tokio_stream::iter(msgs));
                client.attach_routing_epoch(&mut request);
                async move { stub.put(request).await.map(|r| r.into_inner()) }
            })
            .await?;
        Ok(PutObjectV2Result {
            commit_ts: u64::try_from(response.commit_ts)
                .map_err(|_| Error::Internal("negative commit_ts in response".into()))?,
            size: u64::try_from(response.size)
                .map_err(|_| Error::Internal("negative size in response".into()))?,
            chunks: u64::try_from(response.chunks)
                .map_err(|_| Error::Internal("negative chunks in response".into()))?,
        })
    }

    /// 下载整对象（`read_ts = 0` = 服务端 safe_ts）。
    ///
    /// # Errors
    /// - `Error::NotFound`：对象不存在/已删除；
    /// - `Error::RevisionCompacted` 等：`read_ts` 早于 compact 水位。
    pub async fn get(&self, bucket: &str, object_id: &[u8], read_ts: u64) -> Result<ObjectDataV2> {
        let read_ts = ts_to_i64("read_ts", read_ts)?;
        let client = self.client.clone();
        let bucket = bucket.to_string();
        let object_id = object_id.to_vec();
        let (stat, data) = self
            .client
            .execute_v2_call_with_epoch_retry(move |channel| {
                let mut stub =
                    StorageV2Stub::new(channel).max_decoding_message_size(STORAGE_DECODE_LIMIT);
                let mut request = tonic::Request::new(GetRequest {
                    bucket: bucket.clone(),
                    object_id: object_id.clone(),
                    read_ts,
                });
                client.attach_routing_epoch(&mut request);
                async move {
                    let mut stream = stub.get(request).await?.into_inner();
                    let mut stat = None;
                    let mut data = Vec::new();
                    while let Some(msg) = stream.message().await? {
                        match msg.part {
                            Some(get_response::Part::Stat(s)) => stat = Some(s),
                            Some(get_response::Part::Chunk(chunk)) => {
                                data.extend_from_slice(&chunk);
                            }
                            None => {}
                        }
                    }
                    Ok((stat, data))
                }
            })
            .await?;
        let stat = stat.ok_or_else(|| Error::Internal("Get stream missing stat".into()))?;
        Ok(ObjectDataV2 { stat, data })
    }

    /// 查询对象元数据。不存在/已删除 → `Ok(None)`（与 v1 同口径）。
    pub async fn stat(
        &self,
        bucket: &str,
        object_id: &[u8],
        read_ts: u64,
    ) -> Result<Option<ObjectStat>> {
        let read_ts = ts_to_i64("read_ts", read_ts)?;
        let client = self.client.clone();
        let bucket = bucket.to_string();
        let object_id = object_id.to_vec();
        self.client
            .execute_v2_call_with_epoch_retry(move |channel| {
                let mut stub =
                    StorageV2Stub::new(channel).max_decoding_message_size(STORAGE_DECODE_LIMIT);
                let mut request = tonic::Request::new(StatRequest {
                    bucket: bucket.clone(),
                    object_id: object_id.clone(),
                    read_ts,
                });
                client.attach_routing_epoch(&mut request);
                async move {
                    match stub.stat(request).await {
                        Ok(response) => Ok(response.into_inner().stat),
                        Err(status) if status.code() == tonic::Code::NotFound => Ok(None),
                        Err(status) => Err(status),
                    }
                }
            })
            .await
    }

    /// 删除对象（tombstone + 服务端同步删 chunk 文件），返回
    /// `(是否实际删除, tombstone commit_ts)`。
    ///
    /// 重试 Delete 时结果未知，调用方应把 `Error::NotFound` 视为「已删除」。
    pub async fn delete_full(
        &self,
        bucket: &str,
        object_id: &[u8],
        request_id: &[u8],
    ) -> Result<(bool, u64)> {
        let client = self.client.clone();
        let bucket = bucket.to_string();
        let object_id = object_id.to_vec();
        let request_id = request_id.to_vec();
        let response = self
            .client
            .execute_write_with_retry(move |channel| {
                let mut stub =
                    StorageV2Stub::new(channel).max_decoding_message_size(STORAGE_DECODE_LIMIT);
                let mut request = tonic::Request::new(DeleteRequest {
                    bucket: bucket.clone(),
                    object_id: object_id.clone(),
                    request_id: request_id.clone(),
                });
                client.attach_routing_epoch(&mut request);
                async move { stub.delete(request).await.map(|r| r.into_inner()) }
            })
            .await?;
        Ok((
            response.deleted,
            u64::try_from(response.commit_ts)
                .map_err(|_| Error::Internal("negative commit_ts in response".into()))?,
        ))
    }

    /// 删除对象。`true` = 本次实际删除。
    pub async fn delete(&self, bucket: &str, object_id: &[u8], request_id: &[u8]) -> Result<bool> {
        Ok(self.delete_full(bucket, object_id, request_id).await?.0)
    }
}
