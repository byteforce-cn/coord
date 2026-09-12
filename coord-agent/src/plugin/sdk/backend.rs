// coord-agent: 宿主导入 SDK — 类型面、错误映射与后端抽象（计划 §7）
//
// 分层：
// - [`PluginSdk`](super::PluginSdk)：唯一对外门面（作用域守卫 + 后端调用）；
// - [`PluginSdkBackend`]：真正执行协调调用的后端（`CoordSdkBackend` 走 coord-client；
//   测试可注入 stub；Phase 4 的 wasm 宿主复用同一后端面）。
//
// 关键不变式：
// - 作用域守卫**不在后端**，而在门面（[`super::PluginSdk`]）里强制；
//   任何后端实现都无法绕过（fail-closed）。
// - 能力 ID 与 server 内置能力表（`coord-server/src/auth/capability.rs`）逐字对齐。

use std::fmt;

use async_trait::async_trait;
use coord_core::auth::trie::ScopeTrie;

use crate::plugin::manifest::PluginCapability;

// ──── 能力 ID（与 server `builtin_capabilities` 对齐）────

/// KV 读（kv.range / txn 的 compare 与 range 分支）
pub const CAP_KV_READ: &str = "data:kv:read";
/// KV 写（kv.put / txn 的 put 分支）
pub const CAP_KV_WRITE: &str = "data:kv:write";
/// KV 删（kv.delete / txn 的 delete 分支）
pub const CAP_KV_DELETE: &str = "data:kv:delete";
/// 事务执行（server 侧 `/coord.txn.Txn/Txn` 的能力映射）
pub const CAP_TXN_EXECUTE: &str = "data:txn:execute";
/// 租约授予
pub const CAP_LEASE_GRANT: &str = "data:lease:grant";
/// 租约撤销
pub const CAP_LEASE_REVOKE: &str = "data:lease:revoke";
/// 租约保活
pub const CAP_LEASE_KEEPALIVE: &str = "data:lease:keepalive";
/// Watch 订阅（Phase 3）
pub const CAP_WATCH_SUBSCRIBE: &str = "data:watch:subscribe";
/// 对象存储读（Phase 3）
pub const CAP_STORAGE_READ: &str = "data:storage:read";
/// 对象存储写（Phase 3）
pub const CAP_STORAGE_WRITE: &str = "data:storage:write";

// ──── 错误映射（§7.3）────

/// SDK 错误分类 → 插件侧异常（`ErrNotFound` / `ErrUnavailable` / ...）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SdkErrorCode {
    /// 资源不存在
    NotFound,
    /// 集群不可用 / 非 Leader（宿主已重试）
    Unavailable,
    /// 能力或作用域不足
    Forbidden,
    /// 配额 / 背压耗尽
    ResourceExhausted,
    /// 参数非法
    InvalidArgument,
    /// CAS 未命中（`kv.create`：键已存在）—— WIT `sdk-error.conflict`
    Conflict,
    /// 其余内部错误
    Internal,
}

impl SdkErrorCode {
    /// 全部变体（ABI 账本的一致性测试用它枚举，避免手写清单漂移）。
    pub const ALL: [SdkErrorCode; 7] = [
        SdkErrorCode::NotFound,
        SdkErrorCode::Unavailable,
        SdkErrorCode::Forbidden,
        SdkErrorCode::ResourceExhausted,
        SdkErrorCode::InvalidArgument,
        SdkErrorCode::Conflict,
        SdkErrorCode::Internal,
    ];

    /// 插件侧异常名（JS `err.name`）。
    pub const fn exception_name(self) -> &'static str {
        match self {
            SdkErrorCode::NotFound => "ErrNotFound",
            SdkErrorCode::Unavailable => "ErrUnavailable",
            SdkErrorCode::Forbidden => "ErrForbidden",
            SdkErrorCode::ResourceExhausted => "ErrResourceExhausted",
            SdkErrorCode::InvalidArgument => "ErrInvalidArgument",
            SdkErrorCode::Conflict => "ErrConflict",
            SdkErrorCode::Internal => "ErrInternal",
        }
    }

    /// 稳定短名（日志 / 指标）。
    pub const fn as_str(self) -> &'static str {
        match self {
            SdkErrorCode::NotFound => "not_found",
            SdkErrorCode::Unavailable => "unavailable",
            SdkErrorCode::Forbidden => "forbidden",
            SdkErrorCode::ResourceExhausted => "resource_exhausted",
            SdkErrorCode::InvalidArgument => "invalid_argument",
            SdkErrorCode::Conflict => "conflict",
            SdkErrorCode::Internal => "internal",
        }
    }
}

/// 宿主 SDK 错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SdkError {
    /// 分类（决定插件侧异常名）
    pub code: SdkErrorCode,
    /// 面向插件作者的消息
    pub message: String,
}

impl SdkError {
    pub fn new(code: SdkErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(SdkErrorCode::NotFound, message)
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(SdkErrorCode::Unavailable, message)
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::new(SdkErrorCode::Forbidden, message)
    }

    pub fn resource_exhausted(message: impl Into<String>) -> Self {
        Self::new(SdkErrorCode::ResourceExhausted, message)
    }

    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(SdkErrorCode::InvalidArgument, message)
    }

    /// create-if-absent CAS 未命中（键已存在）—— 三条 ABI 路径同语义。
    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(SdkErrorCode::Conflict, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(SdkErrorCode::Internal, message)
    }
}

impl fmt::Display for SdkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code.exception_name(), self.message)
    }
}

impl std::error::Error for SdkError {}

/// SDK 调用结果。
pub type SdkResult<T> = Result<T, SdkError>;

// ──── 作用域守卫 ────

/// 插件声明的能力 + scope 集合（agent 侧第一道防御，与 server ScopeTrie 同源）。
///
/// 语义与 `coord-server` 一致：
/// - 未声明所需能力 → 拒绝（fail-closed）；
/// - 声明的 scope 为空 → 该能力无限制；
/// - 否则按路径段前缀 trie 匹配资源键（`*` 末尾通配）。
#[derive(Debug, Clone, Default)]
pub struct PluginScope {
    plugin: String,
    grants: Vec<(String, Option<ScopeTrie>)>,
}

impl PluginScope {
    /// 由 manifest 的能力声明构建（scope 非法 → 该项记为永不放行）。
    pub fn new(plugin: impl Into<String>, capabilities: &[PluginCapability]) -> Self {
        let mut grants: Vec<(String, Option<ScopeTrie>)> = Vec::new();
        for cap in capabilities {
            let mut trie = ScopeTrie::new();
            let built = match trie.insert(&cap.scope) {
                Ok(()) => Some(trie),
                Err(e) => {
                    tracing::warn!(
                        "plugin capability '{}' has invalid scope '{}': {e}; failing closed",
                        cap.id,
                        cap.scope
                    );
                    None
                }
            };
            grants.push((cap.id.clone(), built));
        }
        Self {
            plugin: plugin.into(),
            grants,
        }
    }

    /// 无任何能力声明（纯观测插件 / 测试）。
    pub fn empty(plugin: impl Into<String>) -> Self {
        Self {
            plugin: plugin.into(),
            grants: Vec::new(),
        }
    }

    /// 插件名。
    pub fn plugin(&self) -> &str {
        &self.plugin
    }

    /// 声明的能力数。
    pub fn len(&self) -> usize {
        self.grants.len()
    }

    pub fn is_empty(&self) -> bool {
        self.grants.is_empty()
    }

    /// 校验 `capability` 能否作用于 `resource`。
    pub fn check(&self, capability: &str, resource: &str) -> SdkResult<()> {
        let mut declared = false;
        for (id, trie) in &self.grants {
            if id != capability && id != "*" {
                continue;
            }
            declared = true;
            if let Some(trie) = trie {
                if trie.matches(resource) {
                    return Ok(());
                }
            }
        }
        if declared {
            Err(SdkError::forbidden(format!(
                "plugin '{}' resource '{resource}' is outside the declared scope of capability \
                 '{capability}'",
                self.plugin
            )))
        } else {
            Err(SdkError::forbidden(format!(
                "plugin '{}' does not declare capability '{capability}'",
                self.plugin
            )))
        }
    }
}

/// 把字节键转成 scope 资源键（非 UTF-8 → fail-closed）。
pub fn key_resource(key: &[u8]) -> SdkResult<String> {
    std::str::from_utf8(key).map(str::to_owned).map_err(|_| {
        SdkError::forbidden("non-UTF-8 keys are not addressable under a declared scope")
    })
}

// ──── DTO：KV / Txn / Lease ────

/// KV 记录的宿主可见字段（§7.1 `kv.range` 的 kvs 元素）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvRecord {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub lease_id: i64,
    pub version: i64,
}

/// `kv.put` 请求。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvPut {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    /// 绑定的 lease（0 = 不绑定）
    pub lease_id: i64,
    /// 是否回传旧值
    pub prev_kv: bool,
    /// 幂等去重 ID（空 = 不去重）
    pub request_id: Vec<u8>,
}

/// `kv.put` 结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvPutOut {
    pub prev_kv: Option<KvRecord>,
    pub revision: i64,
}

/// `kv.range` 请求。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvRange {
    pub key: Vec<u8>,
    /// 左闭右开（空 = 单键精确查询）
    pub range_end: Vec<u8>,
    /// 最大条数（0 = 无限制）
    pub limit: i64,
    /// 历史 revision（0 = 最新）
    pub revision: i64,
    pub keys_only: bool,
    pub count_only: bool,
}

/// `kv.range` 结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvRangeOut {
    pub kvs: Vec<KvRecord>,
    pub count: i64,
    pub revision: i64,
}

/// `kv.delete` 请求。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvDelete {
    pub key: Vec<u8>,
    pub range_end: Vec<u8>,
    pub prev_kv: bool,
    pub request_id: Vec<u8>,
}

/// `kv.delete` 结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvDeleteOut {
    pub deleted: i64,
    pub prev_kvs: Vec<KvRecord>,
    pub revision: i64,
}

/// Compare 目标字段。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareTarget {
    Version,
    Value,
    ModRevision,
}

/// Compare 比较运算。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Equal,
    Greater,
    Less,
    NotEqual,
}

/// 单个 Compare 条件。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Compare {
    pub op: CompareOp,
    pub target: CompareTarget,
    pub key: Vec<u8>,
    /// `target = Version | ModRevision` 时的整型比较值
    pub int_value: i64,
    /// `target = Value` 时的字节比较值
    pub bytes_value: Vec<u8>,
}

/// Txn 分支操作。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxnOp {
    Put(KvPut),
    Range(KvRange),
    Delete(KvDelete),
}

/// Txn 请求。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TxnReq {
    pub compares: Vec<Compare>,
    pub success: Vec<TxnOp>,
    pub failure: Vec<TxnOp>,
    pub request_id: Vec<u8>,
}

/// Txn 分支操作的响应。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxnOpOut {
    Put(KvPutOut),
    Range(KvRangeOut),
    Delete(KvDeleteOut),
}

/// Txn 结果。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TxnOut {
    pub succeeded: bool,
    pub revision: i64,
    pub responses: Vec<TxnOpOut>,
}

// ──── DTO：Watch（§7.2）────

/// `watch.subscribe` 请求。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WatchSubscribe {
    /// 起始键 / 前缀
    pub key: Vec<u8>,
    /// 范围结束（空 = 单键精确监听）
    pub range_end: Vec<u8>,
    /// 起始 revision（0 = 从最新开始）
    pub start_revision: i64,
    /// 事件是否携带旧值
    pub prev_kv: bool,
}

/// Watch 事件类型（与服务端 `WatchEvent.EventType` 对齐）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchEventKind {
    Put,
    Delete,
    /// 服务端缓冲溢出：事件已丢弃，插件应按 revision 重订阅并全量同步
    BufferOverflow,
    /// 历史 changelog 已被清理：无法从指定 revision 回放
    HistoryUnavailable,
}

impl WatchEventKind {
    /// 插件侧字符串（JS `event.type`）。
    pub const fn as_str(self) -> &'static str {
        match self {
            WatchEventKind::Put => "PUT",
            WatchEventKind::Delete => "DELETE",
            WatchEventKind::BufferOverflow => "BUFFER_OVERFLOW",
            WatchEventKind::HistoryUnavailable => "HISTORY_UNAVAILABLE",
        }
    }
}

/// 一条 Watch 事件。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchEventDto {
    pub kind: WatchEventKind,
    /// 事件涉及的 KV（DELETE 时为 tombstone）
    pub kvs: Vec<KvRecord>,
    /// 旧值（`prev_kv = true` 时填充）
    pub prev_kv: Option<KvRecord>,
    /// 事件 revision
    pub revision: i64,
}

// ──── DTO：对象存储（§7.2）────

/// 对象元数据（`storage.stat`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectStatDto {
    pub bucket: String,
    pub object_id: Vec<u8>,
    /// Committed 对象字节数
    pub size: i64,
    /// chunk 数
    pub chunks: i64,
    /// 最近一次状态机写入所在 raft revision
    pub revision: i64,
    /// false = 不存在或已删除
    pub exists: bool,
    /// 是否存在且已完成上传
    pub committed: bool,
}

/// `storage.put` 结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectPutOut {
    pub revision: i64,
    pub size: i64,
    pub chunks: i64,
}

/// `storage.get` 结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectGetOut {
    pub stat: ObjectStatDto,
    pub data: Vec<u8>,
}

// ──── 后端抽象 ────

/// 协调原语后端（真正执行调用的地方）。
///
/// `plugin` 为调用方插件名：用于后台句柄归属（保活流按插件回收）与审计。
#[async_trait]
pub trait PluginSdkBackend: Send + Sync + 'static {
    async fn kv_put(&self, plugin: &str, req: KvPut) -> SdkResult<KvPutOut>;

    async fn kv_range(&self, plugin: &str, req: KvRange) -> SdkResult<KvRangeOut>;

    async fn kv_delete(&self, plugin: &str, req: KvDelete) -> SdkResult<KvDeleteOut>;

    async fn txn(&self, plugin: &str, req: TxnReq) -> SdkResult<TxnOut>;

    /// 授予租约（`id = 0` → 自动分配）；返回 lease id。
    async fn lease_grant(&self, plugin: &str, ttl: i64, id: i64) -> SdkResult<i64>;

    /// 撤销租约。
    async fn lease_revoke(&self, plugin: &str, id: i64) -> SdkResult<()>;

    /// 启动后台保活（幂等：同一 lease 重复调用只保留一个句柄）。
    async fn lease_keep_alive(&self, plugin: &str, id: i64) -> SdkResult<()>;

    /// 停止后台保活（不撤销租约）。
    async fn lease_stop_keep_alive(&self, plugin: &str, id: i64) -> SdkResult<()>;

    /// 建立 Watch 订阅，返回订阅句柄（后端内部分配，插件不感知）。
    async fn watch_subscribe(&self, plugin: &str, req: WatchSubscribe) -> SdkResult<u64>;

    /// 取下一条事件；`Ok(None)` = 流已结束（插件应 `close` 并决定是否重订阅）。
    async fn watch_next(&self, plugin: &str, id: u64) -> SdkResult<Option<WatchEventDto>>;

    /// 关闭订阅（幂等）。
    async fn watch_close(&self, plugin: &str, id: u64) -> SdkResult<()>;

    /// 上传对象（分块，见 `DEFAULT_OBJECT_CHUNK_SIZE`）。
    async fn storage_put(
        &self,
        plugin: &str,
        bucket: &str,
        object_id: &[u8],
        data: &[u8],
    ) -> SdkResult<ObjectPutOut>;

    /// 下载对象（ReadIndex 强一致读）。
    async fn storage_get(
        &self,
        plugin: &str,
        bucket: &str,
        object_id: &[u8],
    ) -> SdkResult<ObjectGetOut>;

    /// 查询对象元数据（不存在/已删 → `Ok(None)`）。
    async fn storage_stat(
        &self,
        plugin: &str,
        bucket: &str,
        object_id: &[u8],
    ) -> SdkResult<Option<ObjectStatDto>>;

    /// 删除对象（返回是否本次实际删除）。
    async fn storage_delete(&self, plugin: &str, bucket: &str, object_id: &[u8])
        -> SdkResult<bool>;

    // ──── 对象存储**流式会话**（批次 10：不整块驻留内存）────
    //
    // 这些方法给「大对象」留出有界内存路径：上传逐块送出、下载逐块取回，
    // 单次驻留内存 = 一个 chunk（≤ 4MiB）+ 调用方缓冲，而不是整个对象。
    //
    // 默认实现返回 `internal`：只有支持会话的后端（`CoordSdkBackend`）才需要
    // 覆盖；纯桩后端若不支持流式，会在被调用时**显式失败**而不是静默降级。

    /// 打开上传会话（`total_size`：`> 0` = 声明长度，`0` = 未知长度，提交时定长）；
    /// 返回会话句柄。
    async fn storage_open_write(
        &self,
        _plugin: &str,
        _bucket: &str,
        _object_id: &[u8],
        _total_size: u64,
    ) -> SdkResult<u64> {
        Err(SdkError::internal(
            "storage upload sessions are not implemented by this backend",
        ))
    }

    /// 追加一个 chunk；返回**累计**已写字节数。
    async fn storage_write_chunk(&self, _plugin: &str, _id: u64, _data: &[u8]) -> SdkResult<u64> {
        Err(SdkError::internal(
            "storage upload sessions are not implemented by this backend",
        ))
    }

    /// 提交上传（返回 Commit 结果）。
    async fn storage_commit_write(&self, _plugin: &str, _id: u64) -> SdkResult<ObjectPutOut> {
        Err(SdkError::internal(
            "storage upload sessions are not implemented by this backend",
        ))
    }

    /// 放弃上传（幂等）。
    async fn storage_abort_write(&self, _plugin: &str, _id: u64) -> SdkResult<()> {
        Err(SdkError::internal(
            "storage upload sessions are not implemented by this backend",
        ))
    }

    /// 打开下载会话；返回会话句柄。
    async fn storage_open_read(
        &self,
        _plugin: &str,
        _bucket: &str,
        _object_id: &[u8],
    ) -> SdkResult<u64> {
        Err(SdkError::internal(
            "storage download sessions are not implemented by this backend",
        ))
    }

    /// 读取下一个 chunk（≤ `max_len` 字节；`Ok(None)` = 读完）。
    async fn storage_read_chunk(
        &self,
        _plugin: &str,
        _id: u64,
        _max_len: u64,
    ) -> SdkResult<Option<Vec<u8>>> {
        Err(SdkError::internal(
            "storage download sessions are not implemented by this backend",
        ))
    }

    /// 下载会话的对象元数据（打开时已取得）。
    async fn storage_reader_stat(&self, _plugin: &str, _id: u64) -> SdkResult<ObjectStatDto> {
        Err(SdkError::internal(
            "storage download sessions are not implemented by this backend",
        ))
    }

    /// 关闭下载会话（幂等）。
    async fn storage_close_read(&self, _plugin: &str, _id: u64) -> SdkResult<()> {
        Err(SdkError::internal(
            "storage download sessions are not implemented by this backend",
        ))
    }

    /// 插件停止：释放该插件持有的全部后台句柄（默认无操作）。
    async fn release_plugin(&self, _plugin: &str) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cap(id: &str, scope: &str) -> PluginCapability {
        PluginCapability {
            id: id.into(),
            scope: scope.into(),
        }
    }

    #[test]
    fn scope_allows_declared_prefix() {
        let scope = PluginScope::new("p", &[cap(CAP_KV_WRITE, "/app/counter/")]);
        assert!(scope.check(CAP_KV_WRITE, "/app/counter/a").is_ok());
        assert!(scope.check(CAP_KV_WRITE, "/app/counter/").is_ok());
    }

    #[test]
    fn scope_rejects_out_of_range_and_missing_capability() {
        let scope = PluginScope::new("p", &[cap(CAP_KV_WRITE, "/app/counter/")]);
        // 越界
        let err = scope.check(CAP_KV_WRITE, "/app/other/a").unwrap_err();
        assert_eq!(err.code, SdkErrorCode::Forbidden);
        // 未声明能力
        let err = scope.check(CAP_KV_READ, "/app/counter/a").unwrap_err();
        assert_eq!(err.code, SdkErrorCode::Forbidden);
        assert!(err.message.contains("does not declare"));
    }

    #[test]
    fn empty_scope_is_unrestricted_for_that_capability() {
        let scope = PluginScope::new("p", &[cap(CAP_KV_READ, "")]);
        assert!(scope.check(CAP_KV_READ, "/anything/at/all").is_ok());
        // 但未声明的能力仍拒绝
        assert!(scope.check(CAP_KV_WRITE, "/anything/at/all").is_err());
    }

    #[test]
    fn wildcard_scope_matches_subtree_only() {
        let scope = PluginScope::new("p", &[cap(CAP_KV_READ, "/app/*")]);
        assert!(scope.check(CAP_KV_READ, "/app/x/y").is_ok());
        assert!(scope.check(CAP_KV_READ, "/other/x").is_err());
    }

    #[test]
    fn invalid_scope_fails_closed() {
        // "=" 不在 scope 允许字符集内 → trie 构建失败 → 永不放行
        let scope = PluginScope::new("p", &[cap(CAP_STORAGE_READ, "bucket=inbox")]);
        assert!(scope.check(CAP_STORAGE_READ, "/bucket/inbox/o1").is_err());
    }

    #[test]
    fn non_utf8_key_is_rejected() {
        let err = key_resource(&[0xff, 0xfe]).unwrap_err();
        assert_eq!(err.code, SdkErrorCode::Forbidden);
        assert_eq!(key_resource(b"/k").unwrap(), "/k");
    }

    #[test]
    fn error_display_carries_exception_name() {
        let e = SdkError::not_found("key gone");
        assert_eq!(e.to_string(), "ErrNotFound: key gone");
        assert_eq!(SdkErrorCode::Unavailable.exception_name(), "ErrUnavailable");
    }
}
