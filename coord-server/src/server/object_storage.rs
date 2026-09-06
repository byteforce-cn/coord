// 对象存储 gRPC 服务（coord.storage.Storage）——流式 Put/Get + Stat/Delete
//
// 语义与数据面设计见 `super::mod`（CoordNode）、`crate::storage::object_store`、
// docs/volume-object-storage.md 决策记录。要点：
//   - manifest 走 raft（强一致），chunk 数据随 raft 日志复制后落 chunk 文件；
//   - Put = client-streaming：首条 meta（Begin，经 raft 创建 Creating manifest），
//     其后每消息一个 chunk（各自经 raft 提交，单 chunk 单日志条目）；
//   - 上传中途任何失败（leader 变更/超时/字节不符）→ 中止并返回错误，残留
//     Creating 由 GC（`object_gc_loop`）在 upload_timeout 后回收；
//   - Get = server-streaming：ReadIndex 屏障 → 首条 stat → 逐 chunk 读文件；
//   - Delete = tombstone（apply 同步删 chunk 文件）。

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use coord_proto::storage::storage_server::Storage;
use coord_proto::storage::{
    get_response, put_request, DeleteRequest, DeleteResponse, GetRequest, GetResponse,
    ObjectStat, PutMeta, PutRequest, PutResponse, StatRequest, StatResponse,
};

use super::{map_err, CoordNode, ObjectTarget};
use crate::raft::type_config::{Command, ObjectStoreOp, Response};
use crate::raft::CoordRaft;
use crate::storage::mvcc::MvccStorage;
use crate::storage::object_store::{
    self, list_manifests, live_object_hashes, read_manifest, validate_ref, ChunkStore,
    ObjectLimits,
};
use crate::storage::redb_backend::RedbBackend;

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// 读 manifest（须已过 ReadIndex 屏障）
fn read_manifest_or_status(
    target: &ObjectTarget,
    bucket: &[u8],
    object_id: &[u8],
) -> Result<Option<object_store::ObjectManifest>, tonic::Status> {
    read_manifest(&target.mvcc, bucket, object_id).map_err(map_err)
}

fn not_found(bucket: &str, object_id: &[u8]) -> tonic::Status {
    tonic::Status::not_found(format!(
        "object {bucket}/{} not found",
        String::from_utf8_lossy(object_id)
    ))
}

fn obj_stat(bucket: &str, object_id: &[u8], m: &object_store::ObjectManifest) -> ObjectStat {
    ObjectStat {
        bucket: bucket.to_string(),
        object_id: object_id.to_vec(),
        size: m.size as i64,
        chunks: m.chunks.len() as i64,
        revision: m.last_revision as i64,
        exists: true,
        committed: m.committed,
    }
}

#[tonic::async_trait]
impl Storage for CoordNode {
    async fn put(
        &self,
        request: tonic::Request<tonic::Streaming<PutRequest>>,
    ) -> Result<tonic::Response<PutResponse>, tonic::Status> {
        // 磁盘水位只读闸（可用 <5% 时 RESOURCE_EXHAUSTED）
        self.ensure_writable()?;
        // 对象存储未启用 → 服务未注册，防御性拒绝
        let limits: Arc<ObjectLimits> = self
            .object_limits
            .clone()
            .ok_or_else(|| tonic::Status::failed_precondition("object storage disabled"))?;

        let mut stream = request.into_inner();

        // ── 首条必须为 meta ──
        let meta: PutMeta = match stream.message().await? {
            Some(m) => match m.part {
                Some(put_request::Part::Meta(meta)) => meta,
                _ => {
                    return Err(tonic::Status::invalid_argument(
                        "first PutRequest message must carry meta (bucket/object_id/total_size)",
                    ))
                }
            },
            None => return Err(tonic::Status::invalid_argument("empty Put stream")),
        };
        let bucket = meta.bucket;
        let object_id = meta.object_id;
        validate_ref(bucket.as_bytes(), &object_id).map_err(|e| map_err(e))?;
        let total_size = meta.total_size as u64;
        if total_size == 0 || total_size > limits.max_object_size {
            return Err(tonic::Status::invalid_argument(format!(
                "total_size must be in (0, {}]",
                limits.max_object_size
            )));
        }

        // 路由目标（manifest key → Region / legacy 根）；chunk 存储必须就绪
        let target = self.object_target_for(bucket.as_bytes(), &object_id)?;
        let store: Arc<ChunkStore> = target
            .chunk_store
            .clone()
            .ok_or_else(|| tonic::Status::failed_precondition("object storage disabled"))?;

        // 配额 admission（尽力而为）：估算 = 已用 + 本次声明
        if limits.quota_bytes > 0 {
            let used = store.usage_bytes().map_err(map_err)?;
            if used.saturating_add(total_size) > limits.quota_bytes {
                return Err(tonic::Status::resource_exhausted(format!(
                    "object storage quota exceeded: used {used} + {total_size} > {}",
                    limits.quota_bytes
                )));
            }
        }

        // Begin（raft）：已存在 → ALREADY_EXISTS
        let (_, ok) = self
            .propose_object_op(
                &target,
                ObjectStoreOp::Begin {
                    bucket: bucket.as_bytes().to_vec(),
                    object_id: object_id.clone(),
                    total_size,
                    started_at_unix: now_unix(),
                },
            )
            .await?;
        if !ok {
            return Err(tonic::Status::already_exists(format!(
                "object {bucket}/{} already exists (committed or upload in progress)",
                String::from_utf8_lossy(&object_id)
            )));
        }

        // ── 逐 chunk 经 raft 提交（单 chunk 单日志条目；字节总量与总 size
        //    的一致性由 Commit 在状态机侧校验）──
        let mut seq: u32 = 0;
        let mut sent_bytes: u64 = 0;
        loop {
            let msg = match stream.message().await {
                Ok(Some(m)) => m,
                Ok(None) => break,
                Err(e) => {
                    // 客户端中止：残留 Creating 由 GC 回收
                    return Err(tonic::Status::aborted(format!(
                        "put stream error (partial object left for GC): {e}"
                    )));
                }
            };
            let data = match msg.part {
                Some(put_request::Part::Chunk(data)) => data,
                Some(put_request::Part::Meta(_)) => {
                    return Err(tonic::Status::invalid_argument(
                        "meta must only appear as the first message",
                    ))
                }
                None => {
                    return Err(tonic::Status::invalid_argument("empty PutRequest message"))
                }
            };
            if data.is_empty() || data.len() > limits.chunk_size {
                return Err(tonic::Status::invalid_argument(format!(
                    "chunk size {} must be in (0, {}]",
                    data.len(),
                    limits.chunk_size
                )));
            }
            if sent_bytes.saturating_add(data.len() as u64) > total_size {
                return Err(tonic::Status::invalid_argument(
                    "uploaded bytes exceed declared total_size",
                ));
            }
            let (_, ok) = self
                .propose_object_op(
                    &target,
                    ObjectStoreOp::Chunk {
                        bucket: bucket.as_bytes().to_vec(),
                        object_id: object_id.clone(),
                        seq,
                        data,
                        now_unix: now_unix(),
                    },
                )
                .await?;
            if !ok {
                return Err(tonic::Status::aborted(
                    "chunk rejected by state machine (ordering/limits); abort upload \
                     (partial object left for GC)",
                ));
            }
            sent_bytes += 0; // data 已移入 raft 命令，仅以 seq 计数
            seq += 1;
        }

        // ── 收尾：至少一个 chunk + Commit（状态机校验 size == total_size）──
        if seq == 0 {
            return Err(tonic::Status::invalid_argument(
                "object must contain at least one chunk",
            ));
        }
        let (rev, ok) = self
            .propose_object_op(
                &target,
                ObjectStoreOp::Commit {
                    bucket: bucket.as_bytes().to_vec(),
                    object_id: object_id.clone(),
                },
            )
            .await?;
        if !ok {
            return Err(tonic::Status::invalid_argument(
                "commit rejected: received bytes != declared total_size (partial object left \
                 for GC)",
            ));
        }
        tracing::info!(
            bucket = %bucket,
            chunks = seq,
            size = total_size,
            "object committed"
        );
        Ok(tonic::Response::new(PutResponse {
            revision: rev as i64,
            size: total_size as i64,
            chunks: seq as i64,
        }))
    }

    type GetStream = ReceiverStream<Result<GetResponse, tonic::Status>>;

    async fn get(
        &self,
        request: tonic::Request<GetRequest>,
    ) -> Result<tonic::Response<Self::GetStream>, tonic::Status> {
        let req = request.into_inner();
        let bucket = req.bucket;
        let object_id = req.object_id;
        validate_ref(bucket.as_bytes(), &object_id).map_err(|e| map_err(e))?;

        let target = self.object_target_for(bucket.as_bytes(), &object_id)?;
        let store: Arc<ChunkStore> = target
            .chunk_store
            .clone()
            .ok_or_else(|| tonic::Status::failed_precondition("object storage disabled"))?;

        // ReadIndex 线性一致屏障（防陈旧读，与 KV 同口径）
        self.object_linearizable(&target).await?;

        let manifest = read_manifest_or_status(&target, bucket.as_bytes(), &object_id)?
            .ok_or_else(|| not_found(&bucket, &object_id))?;
        if !manifest.committed {
            return Err(tonic::Status::failed_precondition(format!(
                "object {bucket}/{} upload in progress or aborted (not committed)",
                String::from_utf8_lossy(&object_id)
            )));
        }

        // 流式发送：首条 stat，然后逐 chunk（文件读移入阻塞线程池）
        let (tx, rx) = mpsc::channel::<Result<GetResponse, tonic::Status>>(4);
        let mvcc = Arc::clone(&target.mvcc);
        let bucket_c = bucket.clone();
        let object_id_c = object_id.clone();
        tokio::spawn(async move {
            let first = GetResponse {
                part: Some(get_response::Part::Stat(obj_stat(
                    &bucket_c,
                    &object_id_c,
                    &manifest,
                ))),
            };
            if tx.send(Ok(first)).await.is_err() {
                return;
            }
            let n = manifest.chunks.len() as u32;
            for seq in 0..n {
                let mvcc = Arc::clone(&mvcc);
                let store = Arc::clone(&store);
                let bucket = bucket_c.clone();
                let object_id = object_id_c.clone();
                let res = tokio::task::spawn_blocking(move || {
                    // 每次读前用最新 manifest（对象可能并发删除/重建）
                    let m = read_manifest(&mvcc, bucket.as_bytes(), &object_id)
                        .map_err(map_err)?;
                    let m = match m {
                        Some(m) if m.committed => m,
                        _ => {
                            return Err(tonic::Status::not_found(format!(
                                "object {}/{} disappeared during Get",
                                String::from_utf8_lossy(bucket.as_bytes()),
                                String::from_utf8_lossy(&object_id)
                            )))
                        }
                    };
                    let data = object_store::read_manifest_chunk(
                        &store,
                        &m,
                        bucket.as_bytes(),
                        &object_id,
                        seq,
                    )
                    .map_err(map_err)?;
                    Ok::<_, tonic::Status>(data)
                })
                .await;
                let data = match res {
                    Ok(Ok(d)) => d,
                    Ok(Err(e)) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                    Err(e) => {
                        let _ = tx
                            .send(Err(tonic::Status::internal(format!(
                                "chunk read task join: {e}"
                            ))))
                            .await;
                        return;
                    }
                };
                if tx
                    .send(Ok(GetResponse {
                        part: Some(get_response::Part::Chunk(data)),
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }

    async fn stat(
        &self,
        request: tonic::Request<StatRequest>,
    ) -> Result<tonic::Response<StatResponse>, tonic::Status> {
        let req = request.into_inner();
        let bucket = req.bucket;
        let object_id = req.object_id;
        validate_ref(bucket.as_bytes(), &object_id).map_err(|e| map_err(e))?;

        let target = self.object_target_for(bucket.as_bytes(), &object_id)?;
        self.object_linearizable(&target).await?;
        let manifest = read_manifest_or_status(&target, bucket.as_bytes(), &object_id)?
            .ok_or_else(|| not_found(&bucket, &object_id))?;
        Ok(tonic::Response::new(StatResponse {
            stat: Some(obj_stat(&bucket, &object_id, &manifest)),
        }))
    }

    async fn delete(
        &self,
        request: tonic::Request<DeleteRequest>,
    ) -> Result<tonic::Response<DeleteResponse>, tonic::Status> {
        self.ensure_writable()?;
        let req = request.into_inner();
        let bucket = req.bucket;
        let object_id = req.object_id;
        validate_ref(bucket.as_bytes(), &object_id).map_err(|e| map_err(e))?;

        let target = self.object_target_for(bucket.as_bytes(), &object_id)?;
        self.object_linearizable(&target).await?;
        if read_manifest_or_status(&target, bucket.as_bytes(), &object_id)?.is_none() {
            return Err(not_found(&bucket, &object_id));
        }
        let (rev, ok) = self
            .propose_object_op(
                &target,
                ObjectStoreOp::Delete {
                    bucket: bucket.as_bytes().to_vec(),
                    object_id: object_id.clone(),
                },
            )
            .await?;
        Ok(tonic::Response::new(DeleteResponse {
            deleted: ok,
            revision: rev as i64,
        }))
    }
}

/// 对象存储 GC 循环（每 raft 实例一个：legacy/root + 各数据 Region）。
///
/// 仅在该 raft 的 **leader** 上运行：
/// 1. stale Creating 回收——manifest `last_write_at_unix` 距今 > upload_timeout
///    （中断上传）→ 经 raft 提 Delete（tombstone + 同步删 chunk 文件）；
/// 2. 孤儿 chunk 文件回收——以 ReadIndex 后的 live manifest 集为基准，删除磁盘
///    上无 manifest 对应的对象目录（删除提交后文件删除前崩溃的残留）。
///
/// 先读屏障（ReadIndex）再扫描，杜绝基于陈旧 live 集误删新建对象文件。
pub async fn object_gc_loop(
    node_id: u64,
    raft: Arc<CoordRaft>,
    mvcc: Arc<MvccStorage<RedbBackend>>,
    chunk_store: Arc<ChunkStore>,
    limits: Arc<ObjectLimits>,
    gc_interval_secs: u64,
) {
    let interval = std::time::Duration::from_secs(gc_interval_secs.max(1));
    let mut ticker = tokio::time::interval(interval);
    tracing::info!("object gc loop started (leader-only, interval {interval:?})");
    loop {
        ticker.tick().await;
        // 仅 leader（避免 follower 落后误删）
        if raft.current_leader().await != Some(node_id) {
            continue;
        }
        // ReadIndex 屏障（leader 身份复核在 openraft ReadPolicy::ReadIndex 内）
        if let Err(e) = raft
            .ensure_linearizable(crate::raft::ReadPolicy::ReadIndex)
            .await
        {
            tracing::debug!("object gc read barrier failed: {e}");
            continue;
        }
        let manifests = match list_manifests(&mvcc) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!("object gc scan failed: {e}");
                continue;
            }
        };
        let now = now_unix();
        // 1) stale Creating → Delete
        for (bucket, object_id, m) in &manifests {
            if !m.committed && now.saturating_sub(m.last_write_at_unix) > limits.upload_timeout_secs as i64 {
                let cmd = Command::ObjectStore(ObjectStoreOp::Delete {
                    bucket: bucket.clone(),
                    object_id: object_id.clone(),
                });
                match raft.client_write(cmd).await {
                    Ok(resp) => match resp.response() {
                        Response::ObjectStore { ok, .. } => tracing::info!(
                            "object gc: deleted stale creating object {}/{} (ok={ok})",
                            String::from_utf8_lossy(bucket),
                            String::from_utf8_lossy(object_id)
                        ),
                        _ => tracing::warn!("object gc: unexpected response"),
                    },
                    Err(e) => tracing::warn!("object gc delete propose failed: {e}"),
                }
            }
        }
        // 2) 孤儿 chunk 文件回收
        let live = live_object_hashes(&manifests);
        match chunk_store.sweep_orphans(&live) {
            Ok(n) if n > 0 => tracing::info!("object gc: swept {n} orphan object dirs"),
            Ok(_) => {}
            Err(e) => tracing::warn!("object gc orphan sweep failed: {e}"),
        }
    }
}
