// coord-agent: 调用面 typed 钩子（Phase 2.2 / 计划 §9.2）
//
// 与网关层（2.1）互补：
// - 网关层：**所有**入站 gRPC 的观察/拒绝面（含代理透传流量），只看 path/headers；
// - 调用面（本模块）：**仅插件发起**的协调调用，且有完整 typed 语义
//   （操作枚举 + 资源键 + 载荷大小 + 结果），可 `before` 拒绝 / `after` 观察。
//
// 因此代理热路径不进调用面钩子（D6 热路径保护）。
//
// 执行顺序：`before` → 作用域守卫通过后的宿主调用 → `after`。
// 钩子是**同步**的（热路径不做 IO）；异步审计请自行投递到队列。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::plugin::sdk::SdkErrorCode;

/// 调用面操作枚举（§9.2）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallOp {
    KvPut,
    KvRange,
    KvDelete,
    /// 单键读取（`kv.get`；三条 ABI 路径共用）
    KvGet,
    /// create-if-absent CAS（`kv.create`；三条 ABI 路径共用）
    KvCreate,
    Txn,
    LeaseGrant,
    LeaseRevoke,
    LeaseKeepAlive,
    StoragePut,
    StorageGet,
    StorageDelete,
}

impl CallOp {
    /// 稳定短名（指标 / 审计）。
    pub const fn as_str(self) -> &'static str {
        match self {
            CallOp::KvPut => "kv_put",
            CallOp::KvRange => "kv_range",
            CallOp::KvDelete => "kv_delete",
            CallOp::KvGet => "kv_get",
            CallOp::KvCreate => "kv_create",
            CallOp::Txn => "txn",
            CallOp::LeaseGrant => "lease_grant",
            CallOp::LeaseRevoke => "lease_revoke",
            CallOp::LeaseKeepAlive => "lease_keepalive",
            CallOp::StoragePut => "storage_put",
            CallOp::StorageGet => "storage_get",
            CallOp::StorageDelete => "storage_delete",
        }
    }

    /// 是否写操作（审计便利判定）。
    pub const fn is_write(self) -> bool {
        matches!(
            self,
            CallOp::KvPut
                | CallOp::KvDelete
                | CallOp::KvCreate
                | CallOp::Txn
                | CallOp::LeaseGrant
                | CallOp::LeaseRevoke
                | CallOp::StoragePut
                | CallOp::StorageDelete
        )
    }
}

/// 一次插件调用的上下文。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallCtx {
    /// 发起插件名
    pub plugin: String,
    /// 操作
    pub op: CallOp,
    /// 资源键（KV 为 key；租约为 `lease:<id>` 或空）
    pub resource: String,
    /// 载荷字节数（key + value 等，便于配额审计）
    pub payload_bytes: usize,
}

/// 钩子判定。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallDecision {
    /// 放行
    Allow,
    /// 拒绝（插件侧收到 `ErrForbidden`）
    Deny(String),
}

/// 调用结果摘要（`after` 入参）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CallOutcome {
    /// 是否成功
    pub ok: bool,
    /// 失败分类
    pub error_code: Option<SdkErrorCode>,
}

impl CallOutcome {
    /// 由 SDK 结果构造。
    pub fn from_result<T>(result: &Result<T, crate::plugin::sdk::SdkError>) -> Self {
        match result {
            Ok(_) => Self {
                ok: true,
                error_code: None,
            },
            Err(e) => Self {
                ok: false,
                error_code: Some(e.code),
            },
        }
    }
}

/// 调用面钩子（同步；同名覆盖）。
pub trait CallHook: Send + Sync + 'static {
    /// 钩子名。
    fn name(&self) -> &str;

    /// 调用前判定。
    fn before(&self, _ctx: &CallCtx) -> CallDecision {
        CallDecision::Allow
    }

    /// 调用后观察。
    fn after(&self, _ctx: &CallCtx, _outcome: &CallOutcome) {}
}

/// 钩子注册表（按插件名匹配；`"*"` = 匹配全部插件）。
#[derive(Default)]
pub struct HookRegistry {
    hooks: RwLock<Vec<(String, Arc<dyn CallHook>)>>,
    before_total: AtomicU64,
    denied_total: AtomicU64,
    after_total: AtomicU64,
}

impl std::fmt::Debug for HookRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HookRegistry")
            .field("hooks", &self.hooks.read().len())
            .field("before_total", &self.before_total.load(Ordering::Relaxed))
            .field("denied_total", &self.denied_total.load(Ordering::Relaxed))
            .finish()
    }
}

/// 注册表统计数据。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HookStats {
    /// `before` 调用次数
    pub before_total: u64,
    /// 被拒绝次数
    pub denied_total: u64,
    /// `after` 调用次数
    pub after_total: u64,
}

impl HookRegistry {
    /// 空注册表（无钩子 → 零开销）。
    pub fn new() -> Self {
        Self::default()
    }

    /// 注册钩子：`plugin_pattern` 为精确插件名或 `"*"`。
    pub fn register(&self, plugin_pattern: impl Into<String>, hook: Arc<dyn CallHook>) {
        let pattern = plugin_pattern.into();
        let name = hook.name().to_string();
        let mut hooks = self.hooks.write();
        hooks.retain(|(p, h)| !(p == &pattern && h.name() == name));
        tracing::info!("plugin call-face hook registered: '{name}' for '{pattern}'");
        hooks.push((pattern, hook));
    }

    /// 是否无钩子（热路径可跳过）。
    pub fn is_empty(&self) -> bool {
        self.hooks.read().is_empty()
    }

    /// 钩子数。
    pub fn len(&self) -> usize {
        self.hooks.read().len()
    }

    /// 统计。
    pub fn stats(&self) -> HookStats {
        HookStats {
            before_total: self.before_total.load(Ordering::Relaxed),
            denied_total: self.denied_total.load(Ordering::Relaxed),
            after_total: self.after_total.load(Ordering::Relaxed),
        }
    }

    /// 依次调用匹配插件的 `before`；任一拒绝即短路（fail-closed）。
    pub fn before(&self, ctx: &CallCtx) -> CallDecision {
        self.before_total.fetch_add(1, Ordering::Relaxed);
        let hooks = self.hooks.read();
        for (pattern, hook) in hooks.iter() {
            if pattern != "*" && pattern != &ctx.plugin {
                continue;
            }
            if let CallDecision::Deny(reason) = hook.before(ctx) {
                self.denied_total.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    "plugin call-face hook '{}' denied {} on '{}' (plugin='{}')",
                    hook.name(),
                    ctx.op.as_str(),
                    ctx.resource,
                    ctx.plugin
                );
                return CallDecision::Deny(format!("{}: {reason}", hook.name()));
            }
        }
        CallDecision::Allow
    }

    /// 通知匹配插件的 `after`。
    pub fn after(&self, ctx: &CallCtx, outcome: &CallOutcome) {
        self.after_total.fetch_add(1, Ordering::Relaxed);
        let hooks = self.hooks.read();
        for (pattern, hook) in hooks.iter() {
            if pattern != "*" && pattern != &ctx.plugin {
                continue;
            }
            hook.after(ctx, outcome);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::sdk::SdkError;
    use parking_lot::Mutex;

    /// 拒绝越界写、记录全部调用的钩子。
    struct AuditHook {
        log: Arc<Mutex<Vec<String>>>,
    }

    impl CallHook for AuditHook {
        fn name(&self) -> &str {
            "audit"
        }

        fn before(&self, ctx: &CallCtx) -> CallDecision {
            self.log
                .lock()
                .push(format!("before:{}:{}", ctx.op.as_str(), ctx.resource));
            if ctx.op == CallOp::KvPut && !ctx.resource.starts_with("/app/") {
                CallDecision::Deny("writes must live under /app/".into())
            } else {
                CallDecision::Allow
            }
        }

        fn after(&self, ctx: &CallCtx, outcome: &CallOutcome) {
            self.log.lock().push(format!(
                "after:{}:{}:ok={}",
                ctx.op.as_str(),
                ctx.resource,
                outcome.ok
            ));
        }
    }

    fn ctx(plugin: &str, op: CallOp, resource: &str) -> CallCtx {
        CallCtx {
            plugin: plugin.into(),
            op,
            resource: resource.into(),
            payload_bytes: 3,
        }
    }

    #[test]
    fn before_denies_and_after_observes() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let registry = HookRegistry::new();
        registry.register(
            "*",
            Arc::new(AuditHook {
                log: Arc::clone(&log),
            }),
        );

        assert_eq!(
            registry.before(&ctx("counter", CallOp::KvRange, "/app/counter/a")),
            CallDecision::Allow
        );
        // 越界写被拒绝
        match registry.before(&ctx("counter", CallOp::KvPut, "/tmp/x")) {
            CallDecision::Deny(reason) => assert!(reason.contains("audit:"), "{reason}"),
            other => panic!("expected deny, got {other:?}"),
        }
        assert_eq!(
            registry.before(&ctx("counter", CallOp::KvPut, "/app/counter/a")),
            CallDecision::Allow
        );

        registry.after(
            &ctx("counter", CallOp::KvPut, "/app/counter/a"),
            &CallOutcome::from_result::<()>(&Ok(())),
        );
        registry.after(
            &ctx("counter", CallOp::KvRange, "/app/counter/a"),
            &CallOutcome::from_result::<()>(&Err(SdkError::not_found("nope"))),
        );

        let stats = registry.stats();
        assert_eq!(stats.before_total, 3);
        assert_eq!(stats.denied_total, 1);
        assert_eq!(stats.after_total, 2);

        let log = log.lock().clone();
        assert!(log.contains(&"before:kv_put:/tmp/x".to_string()), "{log:?}");
        assert!(
            log.contains(&"after:kv_range:/app/counter/a:ok=false".to_string()),
            "{log:?}"
        );
    }

    #[test]
    fn plugin_pattern_filters_hooks() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let registry = HookRegistry::new();
        registry.register(
            "counter",
            Arc::new(AuditHook {
                log: Arc::clone(&log),
            }),
        );

        assert_eq!(
            registry.before(&ctx("other", CallOp::KvPut, "/tmp/x")),
            CallDecision::Allow,
            "hook registered for 'counter' must not fire for 'other'"
        );
        assert!(log.lock().is_empty());
    }

    #[test]
    fn deny_short_circuits_remaining_hooks() {
        struct AlwaysDeny;
        impl CallHook for AlwaysDeny {
            fn name(&self) -> &str {
                "always-deny"
            }
            fn before(&self, _ctx: &CallCtx) -> CallDecision {
                CallDecision::Deny("no".into())
            }
        }

        let log = Arc::new(Mutex::new(Vec::new()));
        let registry = HookRegistry::new();
        registry.register("*", Arc::new(AlwaysDeny));
        registry.register(
            "*",
            Arc::new(AuditHook {
                log: Arc::clone(&log),
            }),
        );

        assert!(matches!(
            registry.before(&ctx("p", CallOp::KvPut, "/app/x")),
            CallDecision::Deny(_)
        ));
        assert!(
            log.lock().is_empty(),
            "later hooks must not run after a deny"
        );
    }

    #[test]
    fn empty_registry_is_free() {
        let registry = HookRegistry::new();
        assert!(registry.is_empty());
        // 空注册表下 before/after 仍是安全 no-op
        assert_eq!(
            registry.before(&ctx("p", CallOp::KvPut, "/x")),
            CallDecision::Allow
        );
        registry.after(
            &ctx("p", CallOp::KvPut, "/x"),
            &CallOutcome {
                ok: true,
                error_code: None,
            },
        );
    }

    #[test]
    fn op_classification() {
        assert!(CallOp::KvPut.is_write());
        assert!(CallOp::Txn.is_write());
        assert!(!CallOp::KvRange.is_write());
        assert_eq!(CallOp::LeaseKeepAlive.as_str(), "lease_keepalive");
    }
}
