// coord-agent: 宿主导入 SDK（计划 §7）
//
// - [`PluginSdk`]：唯一门面 —— 先做**作用域守卫**（agent 侧第一道防御），
//   再委托给 [`PluginSdkBackend`]；插件看到的是 JS（`bind`），
//   wasm 宿主（Phase 4）复用同一门面。
// - [`backend`]：DTO / 错误映射 / 后后端抽象；
// - [`coord`]：基于 coord-client 的真实后端；
// - `bind`：rquickjs 绑定（feature `plugin-js`）。

pub mod backend;
pub mod coord;

#[cfg(feature = "plugin-js")]
pub mod bind;

pub use backend::{
    key_resource, Compare, CompareOp, CompareTarget, KvDelete, KvDeleteOut, KvPut, KvPutOut,
    KvRange, KvRangeOut, KvRecord, ObjectGetOut, ObjectPutOut, ObjectStatDto, PluginScope,
    PluginSdkBackend, SdkError, SdkErrorCode, SdkResult, TxnOp, TxnOpOut, TxnOut, TxnReq,
    WatchEventDto, WatchEventKind, WatchSubscribe, CAP_KV_DELETE, CAP_KV_READ, CAP_KV_WRITE,
    CAP_LEASE_GRANT, CAP_LEASE_KEEPALIVE, CAP_LEASE_REVOKE, CAP_STORAGE_READ, CAP_STORAGE_WRITE,
    CAP_TXN_EXECUTE, CAP_WATCH_SUBSCRIBE,
};
pub use coord::CoordSdkBackend;

use std::sync::Arc;

use crate::plugin::hooks::{CallCtx, CallDecision, CallOp, CallOutcome, HookRegistry};
use crate::plugin::manifest::PluginCapability;

/// 宿主导入 SDK 门面。
///
/// **作用域守卫 + 调用面钩子在此强制**：任何后端都无法绕过（fail-closed）。
pub struct PluginSdk {
    plugin: String,
    scope: PluginScope,
    backend: Arc<dyn PluginSdkBackend>,
    /// 调用面 typed 钩子（Phase 2.2；None / 空注册表 = 零开销）
    hooks: Option<Arc<HookRegistry>>,
}

impl PluginSdk {
    /// 由插件名 + 能力声明 + 后端构建。
    pub fn new(
        plugin: impl Into<String>,
        capabilities: &[PluginCapability],
        backend: Arc<dyn PluginSdkBackend>,
    ) -> Self {
        let plugin = plugin.into();
        let scope = PluginScope::new(plugin.clone(), capabilities);
        Self {
            plugin,
            scope,
            backend,
            hooks: None,
        }
    }

    /// 挂载调用面钩子注册表。
    pub fn with_hooks(mut self, hooks: Arc<HookRegistry>) -> Self {
        self.hooks = Some(hooks);
        self
    }

    /// 插件名。
    pub fn plugin(&self) -> &str {
        &self.plugin
    }

    /// 作用域（供诊断 / 测试）。
    pub fn scope(&self) -> &PluginScope {
        &self.scope
    }

    /// 调用面钩子包装：`before` → 宿主调用 → `after`（无钩子时零开销）。
    async fn run_op<T>(
        &self,
        op: CallOp,
        resource: String,
        payload_bytes: usize,
        call: impl std::future::Future<Output = SdkResult<T>>,
    ) -> SdkResult<T> {
        let Some(hooks) = &self.hooks else {
            return call.await;
        };
        if hooks.is_empty() {
            return call.await;
        }
        let ctx = CallCtx {
            plugin: self.plugin.clone(),
            op,
            resource,
            payload_bytes,
        };
        if let CallDecision::Deny(reason) = hooks.before(&ctx) {
            // 钩子拒绝：宿主调用不会发生（future 未 poll）
            return Err(SdkError::forbidden(format!(
                "plugin call-face hook denied {} on '{}': {reason}",
                op.as_str(),
                ctx.resource
            )));
        }
        let result = call.await;
        hooks.after(&ctx, &CallOutcome::from_result(&result));
        result
    }

    /// `kv.put`。
    ///
    /// `prev_kv = true` 走**预读**实现（与 agent KV 代理 `prev_kv` 同语义，非原子），
    /// 因此额外要求插件声明 `data:kv:read`。
    pub async fn kv_put(&self, req: KvPut) -> SdkResult<KvPutOut> {
        let resource = key_resource(&req.key)?;
        self.scope.check(CAP_KV_WRITE, &resource)?;
        if req.prev_kv {
            self.scope.check(CAP_KV_READ, &resource)?;
        }
        let bytes = req.key.len() + req.value.len();
        self.run_op(
            CallOp::KvPut,
            resource,
            bytes,
            self.backend.kv_put(&self.plugin, req),
        )
        .await
    }

    /// `kv.range`。
    pub async fn kv_range(&self, req: KvRange) -> SdkResult<KvRangeOut> {
        let resource = key_resource(&req.key)?;
        self.scope.check(CAP_KV_READ, &resource)?;
        let bytes = req.key.len() + req.range_end.len();
        self.run_op(
            CallOp::KvRange,
            resource,
            bytes,
            self.backend.kv_range(&self.plugin, req),
        )
        .await
    }

    /// `kv.delete`。
    ///
    /// `prev_kv = true` 走预读实现，额外要求 `data:kv:read`。
    pub async fn kv_delete(&self, req: KvDelete) -> SdkResult<KvDeleteOut> {
        let resource = key_resource(&req.key)?;
        self.scope.check(CAP_KV_DELETE, &resource)?;
        if req.prev_kv {
            self.scope.check(CAP_KV_READ, &resource)?;
        }
        let bytes = req.key.len() + req.range_end.len();
        self.run_op(
            CallOp::KvDelete,
            resource,
            bytes,
            self.backend.kv_delete(&self.plugin, req),
        )
        .await
    }

    /// `kv.get`：单键读取（**不存在 → `ErrNotFound`**，与 core ABI 的
    /// `kv_get` 返回 `ERR_NOT_FOUND` 逐字一致）。
    ///
    /// 三条 ABI 路径（组件 / core / JS）共用本实现，guest 侧永远看到同一语义。
    pub async fn kv_get(&self, key: Vec<u8>) -> SdkResult<Vec<u8>> {
        let resource = key_resource(&key)?;
        self.scope.check(CAP_KV_READ, &resource)?;
        let bytes = key.len();
        self.run_op(CallOp::KvGet, resource, bytes, async {
            let out = self
                .backend
                .kv_range(
                    &self.plugin,
                    KvRange {
                        key,
                        range_end: Vec::new(),
                        limit: 1,
                        revision: 0,
                        keys_only: false,
                        count_only: false,
                    },
                )
                .await?;
            out.kvs
                .into_iter()
                .next()
                .map(|rec| rec.value)
                .ok_or_else(|| SdkError::not_found("key not found"))
        })
        .await
    }

    /// `kv.create`：create-if-absent CAS（`version == 0` 才写入）。
    ///
    /// 已存在 → `ErrConflict`（三条 ABI 路径同语义：WIT `sdk-error.conflict`
    /// / core `ERR_CONFLICT` / JS `ErrConflict`）。返回写入所在 revision。
    ///
    /// 这是锁 / 选举 / idgen 组合原语的原子基础；实现走 `txn` 的
    /// compare-version-0（server 无独立 Create RPC），因此除 `data:kv:write`
    /// 外还需 `data:kv:read`（compare 目标）与 `data:txn:execute`。
    pub async fn kv_create(&self, key: Vec<u8>, value: Vec<u8>, lease_id: i64) -> SdkResult<i64> {
        let resource = key_resource(&key)?;
        self.scope.check(CAP_TXN_EXECUTE, "")?;
        self.scope.check(CAP_KV_READ, &resource)?;
        self.scope.check(CAP_KV_WRITE, &resource)?;
        let bytes = key.len() + value.len();
        let conflict_key = resource.clone();
        self.run_op(CallOp::KvCreate, resource, bytes, async move {
            let out = self
                .backend
                .txn(
                    &self.plugin,
                    TxnReq {
                        compares: vec![Compare {
                            op: CompareOp::Equal,
                            target: CompareTarget::Version,
                            key: key.clone(),
                            int_value: 0,
                            bytes_value: Vec::new(),
                        }],
                        success: vec![TxnOp::Put(KvPut {
                            key,
                            value,
                            lease_id,
                            prev_kv: false,
                            request_id: Vec::new(),
                        })],
                        failure: Vec::new(),
                        request_id: Vec::new(),
                    },
                )
                .await?;
            if out.succeeded {
                Ok(out
                    .responses
                    .iter()
                    .rev()
                    .find_map(|r| match r {
                        TxnOpOut::Put(p) => Some(p.revision),
                        _ => None,
                    })
                    .unwrap_or(out.revision))
            } else {
                Err(SdkError::conflict(format!(
                    "key '{conflict_key}' already exists"
                )))
            }
        })
        .await
    }

    /// `txn`：逐条件 / 逐操作做能力 + 作用域校验（任一越界即整体拒绝）。
    ///
    /// 除数据面能力外还要求 `data:txn:execute`（server 侧 `/coord.txn.Txn/Txn`
    /// 的能力映射，缺声明会在 server 被拒）。
    pub async fn txn(&self, req: TxnReq) -> SdkResult<TxnOut> {
        self.scope.check(CAP_TXN_EXECUTE, "")?;
        for c in &req.compares {
            self.scope.check(CAP_KV_READ, &key_resource(&c.key)?)?;
        }
        for op in req.success.iter().chain(req.failure.iter()) {
            match op {
                TxnOp::Put(p) => {
                    self.scope.check(CAP_KV_WRITE, &key_resource(&p.key)?)?;
                }
                TxnOp::Delete(d) => {
                    self.scope.check(CAP_KV_DELETE, &key_resource(&d.key)?)?;
                }
                TxnOp::Range(r) => {
                    self.scope.check(CAP_KV_READ, &key_resource(&r.key)?)?;
                }
            }
        }
        let resource = req
            .compares
            .first()
            .map(|c| String::from_utf8_lossy(&c.key).into_owned())
            .unwrap_or_default();
        let bytes = req.compares.len() + req.success.len() + req.failure.len();
        self.run_op(
            CallOp::Txn,
            resource,
            bytes,
            self.backend.txn(&self.plugin, req),
        )
        .await
    }

    /// `lease.grant`。
    ///
    /// 租约不带资源键（server 侧不对 Lease RPC 提取 scope key），
    /// 因此该能力**必须以空 scope 声明**；声明非空 scope 会在 agent 侧直接拒绝
    /// （与 server 的 fail-closed 行为一致，提前失败）。
    pub async fn lease_grant(&self, ttl: i64, id: i64) -> SdkResult<i64> {
        self.scope.check(CAP_LEASE_GRANT, "")?;
        if ttl <= 0 {
            return Err(SdkError::invalid_argument("lease ttl must be > 0"));
        }
        self.run_op(
            CallOp::LeaseGrant,
            format!("lease:{id}"),
            0,
            self.backend.lease_grant(&self.plugin, ttl, id),
        )
        .await
    }

    /// `lease.revoke`。
    pub async fn lease_revoke(&self, id: i64) -> SdkResult<()> {
        self.scope.check(CAP_LEASE_REVOKE, "")?;
        self.run_op(
            CallOp::LeaseRevoke,
            format!("lease:{id}"),
            0,
            self.backend.lease_revoke(&self.plugin, id),
        )
        .await
    }

    /// `lease.keepAlive`（启动后台保活）。
    pub async fn lease_keep_alive(&self, id: i64) -> SdkResult<()> {
        self.scope.check(CAP_LEASE_KEEPALIVE, "")?;
        self.run_op(
            CallOp::LeaseKeepAlive,
            format!("lease:{id}"),
            0,
            self.backend.lease_keep_alive(&self.plugin, id),
        )
        .await
    }

    /// 停止后台保活（不撤销租约）。
    pub async fn lease_stop_keep_alive(&self, id: i64) -> SdkResult<()> {
        self.scope.check(CAP_LEASE_KEEPALIVE, "")?;
        self.backend.lease_stop_keep_alive(&self.plugin, id).await
    }

    /// `watch.subscribe`：建立订阅，返回订阅句柄。
    pub async fn watch_subscribe(&self, req: WatchSubscribe) -> SdkResult<u64> {
        self.scope.check(CAP_WATCH_SUBSCRIBE, "")?;
        self.backend.watch_subscribe(&self.plugin, req).await
    }

    /// `watch.next`：取下一条事件（`None` = 流结束）。
    ///
    /// 溢出走 `ErrResourceExhausted`（插件应按返回的 revision 重订阅并全量同步）。
    pub async fn watch_next(&self, id: u64) -> SdkResult<Option<WatchEventDto>> {
        self.backend.watch_next(&self.plugin, id).await
    }

    /// `watch.close`（幂等）。
    pub async fn watch_close(&self, id: u64) -> SdkResult<()> {
        self.backend.watch_close(&self.plugin, id).await
    }

    /// `storage.put`（分块上传）。
    ///
    /// 对象存储 capability 与租约一样**不带资源键**（server 侧不对 Storage RPC
    /// 提取 scope key），因此须以空 scope 声明。
    pub async fn storage_put(
        &self,
        bucket: &str,
        object_id: &[u8],
        data: &[u8],
    ) -> SdkResult<ObjectPutOut> {
        self.scope.check(CAP_STORAGE_WRITE, "")?;
        if bucket.is_empty() {
            return Err(SdkError::invalid_argument("bucket must not be empty"));
        }
        self.run_op(
            CallOp::StoragePut,
            format!("bucket:{bucket}"),
            data.len(),
            self.backend
                .storage_put(&self.plugin, bucket, object_id, data),
        )
        .await
    }

    /// `storage.get`。
    pub async fn storage_get(&self, bucket: &str, object_id: &[u8]) -> SdkResult<ObjectGetOut> {
        self.scope.check(CAP_STORAGE_READ, "")?;
        self.run_op(
            CallOp::StorageGet,
            format!("bucket:{bucket}"),
            0,
            self.backend.storage_get(&self.plugin, bucket, object_id),
        )
        .await
    }

    /// `storage.stat`（不存在 → `Ok(None)`）。
    pub async fn storage_stat(
        &self,
        bucket: &str,
        object_id: &[u8],
    ) -> SdkResult<Option<ObjectStatDto>> {
        self.scope.check(CAP_STORAGE_READ, "")?;
        self.backend
            .storage_stat(&self.plugin, bucket, object_id)
            .await
    }

    /// `storage.delete`。
    pub async fn storage_delete(&self, bucket: &str, object_id: &[u8]) -> SdkResult<bool> {
        self.scope.check(CAP_STORAGE_WRITE, "")?;
        self.run_op(
            CallOp::StorageDelete,
            format!("bucket:{bucket}"),
            0,
            self.backend.storage_delete(&self.plugin, bucket, object_id),
        )
        .await
    }

    // ──── 对象存储流式会话（批次 10：不整块驻留内存）────

    /// `storage.openWrite`：打开分块上传会话（返回会话句柄）。
    ///
    /// `total_size > 0` = 声明长度（须与写入总量一致）；`total_size == 0` =
    /// **未知长度**（`commit` 时按实际字节定长，上限 = server `max_object_size`）。
    pub async fn storage_open_write(
        &self,
        bucket: &str,
        object_id: &[u8],
        total_size: u64,
    ) -> SdkResult<u64> {
        self.scope.check(CAP_STORAGE_WRITE, "")?;
        if bucket.is_empty() {
            return Err(SdkError::invalid_argument("bucket must not be empty"));
        }
        self.run_op(
            CallOp::StoragePut,
            format!("bucket:{bucket}"),
            0,
            self.backend
                .storage_open_write(&self.plugin, bucket, object_id, total_size),
        )
        .await
    }

    /// `storage.writeChunk`：追加一个 chunk；返回**累计**已写字节数。
    pub async fn storage_write_chunk(&self, id: u64, data: &[u8]) -> SdkResult<u64> {
        self.scope.check(CAP_STORAGE_WRITE, "")?;
        if data.is_empty() {
            return Err(SdkError::invalid_argument("chunk must not be empty"));
        }
        let len = data.len();
        self.run_op(
            CallOp::StoragePut,
            format!("session:{id}"),
            len,
            self.backend.storage_write_chunk(&self.plugin, id, data),
        )
        .await
    }

    /// `storage.commitWrite`：提交上传（返回 Commit 结果）。
    pub async fn storage_commit_write(&self, id: u64) -> SdkResult<ObjectPutOut> {
        self.scope.check(CAP_STORAGE_WRITE, "")?;
        self.run_op(
            CallOp::StoragePut,
            format!("session:{id}"),
            0,
            self.backend.storage_commit_write(&self.plugin, id),
        )
        .await
    }

    /// `storage.abortWrite`：放弃上传（幂等）。
    pub async fn storage_abort_write(&self, id: u64) -> SdkResult<()> {
        self.scope.check(CAP_STORAGE_WRITE, "")?;
        self.backend.storage_abort_write(&self.plugin, id).await
    }

    /// `storage.openRead`：打开分块下载会话（首条 stat 已就绪）。
    pub async fn storage_open_read(&self, bucket: &str, object_id: &[u8]) -> SdkResult<u64> {
        self.scope.check(CAP_STORAGE_READ, "")?;
        self.run_op(
            CallOp::StorageGet,
            format!("bucket:{bucket}"),
            0,
            self.backend
                .storage_open_read(&self.plugin, bucket, object_id),
        )
        .await
    }

    /// `storage.readChunk`：读取下一个 chunk（≤ `max_len`；`Ok(None)` = 读完）。
    pub async fn storage_read_chunk(&self, id: u64, max_len: u64) -> SdkResult<Option<Vec<u8>>> {
        self.scope.check(CAP_STORAGE_READ, "")?;
        self.run_op(
            CallOp::StorageGet,
            format!("session:{id}"),
            0,
            self.backend.storage_read_chunk(&self.plugin, id, max_len),
        )
        .await
    }

    /// `storage.readerStat`：下载会话的对象元数据（打开时已取得）。
    pub async fn storage_reader_stat(&self, id: u64) -> SdkResult<ObjectStatDto> {
        self.scope.check(CAP_STORAGE_READ, "")?;
        self.backend.storage_reader_stat(&self.plugin, id).await
    }

    /// `storage.closeRead`：关闭下载会话（幂等）。
    pub async fn storage_close_read(&self, id: u64) -> SdkResult<()> {
        self.backend.storage_close_read(&self.plugin, id).await
    }

    /// 插件停止：释放该插件持有的全部后台句柄。
    pub async fn release(&self) {
        self.backend.release_plugin(&self.plugin).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    /// 记录调用的 stub 后端（验证守卫在门面层生效）。
    #[derive(Default)]
    struct StubBackend {
        puts: parking_lot::Mutex<Vec<Vec<u8>>>,
    }

    #[async_trait]
    impl PluginSdkBackend for StubBackend {
        async fn kv_put(&self, _plugin: &str, req: KvPut) -> SdkResult<KvPutOut> {
            self.puts.lock().push(req.key);
            Ok(KvPutOut {
                prev_kv: None,
                revision: 7,
            })
        }
        async fn kv_range(&self, _plugin: &str, _req: KvRange) -> SdkResult<KvRangeOut> {
            Ok(KvRangeOut {
                kvs: vec![],
                count: 0,
                revision: 7,
            })
        }
        async fn kv_delete(&self, _plugin: &str, _req: KvDelete) -> SdkResult<KvDeleteOut> {
            Ok(KvDeleteOut {
                deleted: 0,
                prev_kvs: vec![],
                revision: 7,
            })
        }
        async fn txn(&self, _plugin: &str, _req: TxnReq) -> SdkResult<TxnOut> {
            Ok(TxnOut::default())
        }
        async fn lease_grant(&self, _plugin: &str, _ttl: i64, _id: i64) -> SdkResult<i64> {
            Ok(42)
        }
        async fn lease_revoke(&self, _plugin: &str, _id: i64) -> SdkResult<()> {
            Ok(())
        }
        async fn lease_keep_alive(&self, _plugin: &str, _id: i64) -> SdkResult<()> {
            Ok(())
        }
        async fn lease_stop_keep_alive(&self, _plugin: &str, _id: i64) -> SdkResult<()> {
            Ok(())
        }
        async fn watch_subscribe(&self, _plugin: &str, _req: WatchSubscribe) -> SdkResult<u64> {
            Ok(1)
        }
        async fn watch_next(&self, _plugin: &str, _id: u64) -> SdkResult<Option<WatchEventDto>> {
            Ok(None)
        }
        async fn watch_close(&self, _plugin: &str, _id: u64) -> SdkResult<()> {
            Ok(())
        }
        async fn storage_put(
            &self,
            _plugin: &str,
            _bucket: &str,
            _object_id: &[u8],
            data: &[u8],
        ) -> SdkResult<ObjectPutOut> {
            Ok(ObjectPutOut {
                revision: 9,
                size: data.len() as i64,
                chunks: 1,
            })
        }
        async fn storage_get(
            &self,
            _plugin: &str,
            bucket: &str,
            object_id: &[u8],
        ) -> SdkResult<ObjectGetOut> {
            Ok(ObjectGetOut {
                stat: ObjectStatDto {
                    bucket: bucket.to_string(),
                    object_id: object_id.to_vec(),
                    size: 2,
                    chunks: 1,
                    revision: 9,
                    exists: true,
                    committed: true,
                },
                data: b"hi".to_vec(),
            })
        }
        async fn storage_stat(
            &self,
            _plugin: &str,
            _bucket: &str,
            _object_id: &[u8],
        ) -> SdkResult<Option<ObjectStatDto>> {
            Ok(None)
        }
        async fn storage_delete(
            &self,
            _plugin: &str,
            _bucket: &str,
            _object_id: &[u8],
        ) -> SdkResult<bool> {
            Ok(true)
        }
    }

    fn cap(id: &str, scope: &str) -> PluginCapability {
        PluginCapability {
            id: id.into(),
            scope: scope.into(),
        }
    }

    fn put(key: &str) -> KvPut {
        KvPut {
            key: key.as_bytes().to_vec(),
            value: b"v".to_vec(),
            lease_id: 0,
            prev_kv: false,
            request_id: Vec::new(),
        }
    }

    #[tokio::test]
    async fn facade_enforces_scope_before_backend() {
        let backend = Arc::new(StubBackend::default());
        let sdk = PluginSdk::new(
            "p",
            &[cap(CAP_KV_WRITE, "/app/counter/")],
            Arc::clone(&backend) as Arc<dyn PluginSdkBackend>,
        );

        let out = sdk.kv_put(put("/app/counter/a")).await.unwrap();
        assert_eq!(out.revision, 7);
        assert_eq!(backend.puts.lock().len(), 1);

        // 越界：后端不应被调用
        let err = sdk.kv_put(put("/app/other/a")).await.unwrap_err();
        assert_eq!(err.code, SdkErrorCode::Forbidden);
        assert_eq!(backend.puts.lock().len(), 1);
    }

    #[tokio::test]
    async fn facade_rejects_txn_when_any_branch_out_of_scope() {
        let backend = Arc::new(StubBackend::default());
        let sdk = PluginSdk::new(
            "p",
            &[
                cap(CAP_KV_READ, "/app/counter/"),
                cap(CAP_KV_WRITE, "/app/counter/"),
            ],
            Arc::clone(&backend) as Arc<dyn PluginSdkBackend>,
        );
        let req = TxnReq {
            compares: vec![],
            success: vec![TxnOp::Put(put("/app/counter/a"))],
            failure: vec![TxnOp::Put(put("/app/other/b"))],
            request_id: Vec::new(),
        };
        let err = sdk.txn(req).await.unwrap_err();
        assert_eq!(err.code, SdkErrorCode::Forbidden);
    }

    #[tokio::test]
    async fn txn_requires_txn_execute_capability() {
        let backend = Arc::new(StubBackend::default());
        let sdk = PluginSdk::new(
            "p",
            &[
                cap(CAP_KV_READ, "/app/"),
                cap(CAP_KV_WRITE, "/app/"),
                cap(CAP_TXN_EXECUTE, ""),
            ],
            Arc::clone(&backend) as Arc<dyn PluginSdkBackend>,
        );
        let req = TxnReq {
            compares: vec![],
            success: vec![TxnOp::Put(put("/app/counter/a"))],
            failure: vec![],
            request_id: Vec::new(),
        };
        assert!(sdk.txn(req.clone()).await.is_ok());

        // 去掉 data:txn:execute → 拒绝
        let sdk_no_txn = PluginSdk::new(
            "p",
            &[cap(CAP_KV_READ, "/app/"), cap(CAP_KV_WRITE, "/app/")],
            Arc::clone(&backend) as Arc<dyn PluginSdkBackend>,
        );
        let err = sdk_no_txn.txn(req).await.unwrap_err();
        assert_eq!(err.code, SdkErrorCode::Forbidden);
    }

    #[tokio::test]
    async fn prev_kv_put_requires_read_capability() {
        let backend = Arc::new(StubBackend::default());
        let sdk = PluginSdk::new(
            "p",
            &[cap(CAP_KV_WRITE, "/app/counter/")],
            Arc::clone(&backend) as Arc<dyn PluginSdkBackend>,
        );
        let mut req = put("/app/counter/a");
        req.prev_kv = true;
        let err = sdk.kv_put(req).await.unwrap_err();
        assert_eq!(err.code, SdkErrorCode::Forbidden);
        assert!(err.message.contains("data:kv:read"));
    }

    #[tokio::test]
    async fn lease_capability_with_nonempty_scope_fails_closed() {
        let backend = Arc::new(StubBackend::default());
        let sdk = PluginSdk::new(
            "p",
            &[cap(CAP_LEASE_GRANT, "/lease/")],
            Arc::clone(&backend) as Arc<dyn PluginSdkBackend>,
        );
        // 租约无资源键 → 非空 scope 无法匹配 → 拒绝（提前于 server）
        assert!(sdk.lease_grant(30, 0).await.is_err());
    }

    #[tokio::test]
    async fn lease_grant_requires_capability_and_positive_ttl() {
        let backend = Arc::new(StubBackend::default());
        let sdk = PluginSdk::new(
            "p",
            &[cap(CAP_LEASE_GRANT, "")],
            Arc::clone(&backend) as Arc<dyn PluginSdkBackend>,
        );
        assert_eq!(sdk.lease_grant(30, 0).await.unwrap(), 42);
        let err = sdk.lease_grant(0, 0).await.unwrap_err();
        assert_eq!(err.code, SdkErrorCode::InvalidArgument);
        // 未声明的能力
        let err = sdk.lease_revoke(42).await.unwrap_err();
        assert_eq!(err.code, SdkErrorCode::Forbidden);
    }
}
