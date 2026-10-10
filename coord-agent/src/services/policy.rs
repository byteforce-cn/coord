// coord-agent: 权限策略引擎 (Policy Service) — 安全层
//
// 实现 BaseService trait，提供基于规则的授权决策引擎（RBAC/ABAC）。
// 支持策略管理、条件匹配、优先级排序、通配符匹配。
// 设计为可扩展至 OPA Wasm 的策略决策点。
//
// Bundle 管理:
// - 策略包存储在 Server KV（`/_policy/bundles/` 前缀），多 Agent 共享
// - OpaEngine 负责本地 Rego 求值和 explain
// - PolicyService 负责 bundle CRUD（KV 读写）和 OpaEngine 策略同步
//
// Bundle 分发与加载（G-POL-1）：
// - **启动加载**：`start()` 从 Server KV 全量加载 enabled bundles 进本地引擎
//   （不带 KV 的骨架模式跳过）；
// - **变更传播**：Watch 订阅 `/_policy/bundles/` 前缀（水位续传），断连/溢出
//   退化为「指数退避 + 全量对账」重连；收敛口径见 proto 与契约页
//   （稳态亚秒级；断连窗口 + 对账 ≤10s 量级）；
// - **版本一致性**：Put / Rollback / SetEnabled 均为 per-key version CAS，
//   冲突有限重试（≤5）后报错；并发下版本单调、无丢更新。

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;
use tokio::sync::watch;

use crate::proxy::AgentInner;
use crate::service::{BaseService, ServiceResult};
use crate::services::opa::{OpaConfig, OpaEngine};

use coord_proto::kv::PutRequest;
use coord_proto::txn::compare::{CompareResult, Target, TargetValue};
use coord_proto::txn::request_op::Op;
use coord_proto::txn::{Compare, RequestOp};

// ──── 公共类型 ────

/// 策略效果
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyEffect {
    Allow,
    Deny,
}

/// 策略条件
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PolicyCondition {
    pub attribute: String,
    pub operator: String,
    pub value: String,
}

/// 策略定义
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Policy {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub effect: PolicyEffect,
    pub subjects: Vec<String>,
    pub actions: Vec<String>,
    pub resources: Vec<String>,
    #[serde(default)]
    pub conditions: Vec<PolicyCondition>,
    pub priority: i32,
}

/// 访问请求
#[derive(Debug, Clone)]
pub struct AccessRequest {
    pub subject: String,
    pub action: String,
    pub resource: String,
    pub context: HashMap<String, String>,
}

/// 策略决策结果
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyDecision {
    pub effect: PolicyEffect,
    pub matched_policy_id: Option<String>,
    pub reason: String,
}

// ──── Bundle 类型 ────

/// 策略包信息（对外暴露，存储在 Server KV）
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BundleInfo {
    pub bundle_id: String,
    pub name: String,
    pub namespace: String,
    pub tenant_id: String,
    pub enabled: bool,
    /// 当前版本号（每次成功上传/回滚 +1）
    #[serde(default)]
    pub version: i64,
    pub created_at: i64,
    pub updated_at: i64,
}

/// 策略包历史版本信息（用于回滚目标发现）
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BundleVersionInfo {
    pub version: i64,
    pub created_at: i64,
    pub is_current: bool,
}

/// 策略包完整内容（KV 存储格式）
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct BundleRecord {
    pub info: BundleInfo,
    pub rego_content: String,
}

// ──── Bundle 分发态（G-POL-1 观测）────

/// bundle 分发/加载统计（由启动加载与 Watch 后台任务维护；采样任务读入指标）。
#[derive(Debug, Default)]
pub struct PolicySyncStats {
    /// 本地引擎当前已加载的 enabled bundle 数（gauge）
    pub loaded_bundles: AtomicI64,
    /// 最近一次成功全量加载/对账的 unix 秒（0 = 从未成功；gauge）
    pub last_success_unix: AtomicI64,
    /// 成功加载/对账累计次数（单调）
    pub load_ok_total: AtomicU64,
    /// 失败（订阅失败/对账失败/事件应用失败）累计次数（单调）
    pub load_error_total: AtomicU64,
}

impl BundleRecord {
    /// 当前生效记录 key（唯一键 = tenant/namespace/name）
    fn storage_key(bundle_id: &str) -> Vec<u8> {
        format!("/_policy/bundles/{bundle_id}").into_bytes()
    }

    /// 历史版本快照 key（用于按版本回滚）
    fn snapshot_key(bundle_id: &str, version: i64) -> Vec<u8> {
        format!("/_policy/bundles/{bundle_id}@v{version}").into_bytes()
    }

    fn prefix_key() -> Vec<u8> {
        b"/_policy/bundles/".to_vec()
    }

    fn make_bundle_id(tenant_id: &str, namespace: &str, name: &str) -> String {
        format!("{tenant_id}/{namespace}/{name}")
    }

    fn new(
        tenant_id: &str,
        namespace: &str,
        name: &str,
        rego: &str,
        version: i64,
        now: i64,
    ) -> Self {
        let bundle_id = Self::make_bundle_id(tenant_id, namespace, name);
        Self {
            info: BundleInfo {
                bundle_id,
                name: name.to_string(),
                namespace: namespace.to_string(),
                tenant_id: tenant_id.to_string(),
                enabled: true,
                version,
                created_at: now,
                updated_at: now,
            },
            rego_content: rego.to_string(),
        }
    }
}

fn unix_ts_i64() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// 在阻塞线程池执行 OpaEngine 的同步 CPU 密集操作（Rego 编译 / 引擎重建 / explain）。
///
/// OpaEngine 内部 `engine.write()` + regorus 编译/求值
/// 均为同步 CPU 操作，直接内联在 async bundle 方法里会阻塞 agent 的 tokio worker。
/// 统一经 `spawn_blocking` 移出 worker；引擎本身受内部 RwLock 保护、线程安全，
/// 多调用方串行化语义不变（与 grpc_handlers `Policy::evaluate` 的既有先例一致）。
async fn opa_blocking<F, T>(f: F) -> ServiceResult<T>
where
    F: FnOnce() -> Result<T, String> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| format!("opa blocking task join error: {e}"))?
        .map_err(|e| e.into())
}

// ──── Bundle 加载与变更传播（G-POL-1，自由函数以便 Watch 任务共享）────

/// 从 bundle KV key 推导本地 OPA policy_id（`{namespace}/{name}`）。
///
/// 返回 `None` 表示不是当前记录（版本快照 `@v` 或格式不符）——不直接消费。
fn policy_id_from_bundle_key(key: &[u8]) -> Option<String> {
    let s = std::str::from_utf8(key).ok()?;
    let rest = s.strip_prefix("/_policy/bundles/")?;
    if rest.contains("@v") {
        return None;
    }
    let parts: Vec<&str> = rest.split('/').collect();
    if parts.len() != 3 {
        return None;
    }
    // tenant/namespace/name → policy_id = namespace/name（与 put_bundle 的同步口径一致）
    Some(format!("{}/{}", parts[1], parts[2]))
}

/// 汇总 KV pairs 中「可加载的 enabled bundle」（跳过快照/disabled）。
///
/// 解析失败的单条记录跳过并计数（不因单条坏记录阻断全量加载，
/// 与「断连对账覆盖式恢复」同一姿态）。
fn collect_enabled_bundles(pairs: &[(Vec<u8>, Vec<u8>)]) -> (Vec<(String, String)>, usize) {
    let mut loaded = Vec::new();
    let mut skipped = 0usize;
    for (k, v) in pairs {
        if policy_id_from_bundle_key(k).is_none() {
            continue; // 版本快照不参与引擎加载
        }
        match serde_json::from_slice::<BundleRecord>(v) {
            Ok(rec) if rec.info.enabled => {
                loaded.push((
                    format!("{}/{}", rec.info.namespace, rec.info.name),
                    rec.rego_content,
                ));
            }
            Ok(_) => {} // disabled：不加载
            Err(_) => skipped += 1,
        }
    }
    (loaded, skipped)
}

/// 全量加载/对账：KV 扫描 → 本地引擎整体替换（覆盖式，消除断连窗口差异）。
async fn load_all_bundles(
    inner: &Arc<AgentInner>,
    opa: &Arc<OpaEngine>,
    stats: &PolicySyncStats,
) -> Result<usize, String> {
    let prefix = BundleRecord::prefix_key();
    let range_end = prefix_end(&prefix);
    let pairs = inner
        .client
        .kv()
        .range(&prefix, &range_end, 0, 0)
        .await
        .map_err(|e| format!("kv range bundles: {e}"))?;
    let (loaded, skipped) = collect_enabled_bundles(&pairs);
    if skipped > 0 {
        tracing::warn!("Policy: skipped {skipped} unparseable bundle record(s) during load");
    }
    let count = loaded.len();
    let opa_owned = Arc::clone(opa);
    opa_blocking(move || opa_owned.load_policies(&loaded))
        .await
        .map_err(|e| format!("load bundles into engine: {e}"))?;
    stats.loaded_bundles.store(count as i64, Ordering::Relaxed);
    stats
        .last_success_unix
        .store(unix_ts_i64(), Ordering::Relaxed);
    stats.load_ok_total.fetch_add(1, Ordering::Relaxed);
    Ok(count)
}

/// 应用单条 bundle Watch 事件（新增/更新/删除/启停）。
async fn apply_bundle_event(
    opa: &Arc<OpaEngine>,
    stats: &PolicySyncStats,
    key: &[u8],
    value: Option<&[u8]>,
) -> Result<(), String> {
    let Some(policy_id) = policy_id_from_bundle_key(key) else {
        return Ok(()); // 快照 key 不直接消费
    };
    match value {
        None => {
            let opa_owned = Arc::clone(opa);
            let pid = policy_id.clone();
            opa_blocking(move || {
                opa_owned.remove_policy(&pid);
                Ok(())
            })
            .await
            .map_err(|e| format!("remove bundle policy: {e}"))?;
        }
        Some(v) => {
            let rec: BundleRecord =
                serde_json::from_slice(v).map_err(|e| format!("deserialize bundle event: {e}"))?;
            let pid = format!("{}/{}", rec.info.namespace, rec.info.name);
            if rec.info.enabled {
                let opa_owned = Arc::clone(opa);
                let rego = rec.rego_content;
                opa_blocking(move || opa_owned.add_policy(&pid, &rego))
                    .await
                    .map_err(|e| format!("apply bundle policy: {e}"))?;
            } else {
                let opa_owned = Arc::clone(opa);
                opa_blocking(move || {
                    opa_owned.remove_policy(&pid);
                    Ok(())
                })
                .await
                .map_err(|e| format!("remove disabled bundle policy: {e}"))?;
            }
        }
    }
    stats
        .loaded_bundles
        .store(opa.policy_count() as i64, Ordering::Relaxed);
    stats
        .last_success_unix
        .store(unix_ts_i64(), Ordering::Relaxed);
    Ok(())
}

/// Bundle 变更传播后台任务：Watch 订阅 + 断连/溢出对账重连。
///
/// 收敛口径（与 proto/契约页同口径）：
/// - 稳态：Watch 推送驱动，亚秒级生效；
/// - 断连/溢出：指数退避（封顶 8s）后全量对账重连 ⇒ ≤10s 量级收敛。
async fn run_bundle_watch(
    inner: Arc<AgentInner>,
    opa: Arc<OpaEngine>,
    stats: Arc<PolicySyncStats>,
    mut shutdown: watch::Receiver<()>,
) {
    use coord_proto::watch::watch_event::EventType;

    let prefix: &[u8] = b"/_policy/bundles/";
    let mut last_rev: i64 = 0;
    let mut attempt: u32 = 0;

    'outer: loop {
        match inner.client.watch().watch(prefix, last_rev).await {
            Ok(mut event_rx) => {
                attempt = 0;
                loop {
                    tokio::select! {
                        _ = shutdown.changed() => break 'outer,
                        event = event_rx.recv() => match event {
                            Some(Ok(we)) => {
                                if we.revision > last_rev {
                                    last_rev = we.revision;
                                }
                                let needs_reconcile = matches!(
                                    we.r#type,
                                    t if t == EventType::BufferOverflow as i32
                                        || t == EventType::HistoryUnavailable as i32
                                );
                                let mut apply_failed = false;
                                for kv in &we.kvs {
                                    let value = if we.r#type == EventType::Delete as i32 {
                                        None
                                    } else {
                                        Some(kv.value.as_slice())
                                    };
                                    if let Err(e) =
                                        apply_bundle_event(&opa, &stats, &kv.key, value).await
                                    {
                                        tracing::warn!(
                                            "Policy: apply bundle event failed: {e}; will reconcile"
                                        );
                                        stats.load_error_total.fetch_add(1, Ordering::Relaxed);
                                        apply_failed = true;
                                    }
                                }
                                if apply_failed {
                                    match load_all_bundles(&inner, &opa, &stats).await {
                                        Ok(n) => tracing::info!(
                                            "Policy: reconciled {n} bundles after apply error"
                                        ),
                                        Err(e) => {
                                            stats.load_error_total.fetch_add(1, Ordering::Relaxed);
                                            tracing::error!("Policy: reconcile after apply error failed: {e}");
                                        }
                                    }
                                }
                                if needs_reconcile {
                                    tracing::warn!(
                                        "Policy: watch overflow/history-unavailable; full reconcile"
                                    );
                                    break; // → 对账重连
                                }
                            }
                            Some(Err(e)) => {
                                tracing::warn!(
                                    "Policy: bundle watch stream error: {e}; reconcile + reconnect"
                                );
                                break;
                            }
                            None => {
                                tracing::warn!(
                                    "Policy: bundle watch stream ended; reconcile + reconnect"
                                );
                                break;
                            }
                        }
                    }
                }
            }
            Err(e) => {
                tracing::warn!("Policy: bundle watch subscribe failed: {e}; will retry");
                stats.load_error_total.fetch_add(1, Ordering::Relaxed);
            }
        }

        // 断连/溢出/订阅失败 → 指数退避（封顶 8s，收敛承诺 ≤10s 量级）+ 全量对账
        attempt += 1;
        let backoff = std::time::Duration::from_secs(1u64 << attempt.saturating_sub(1).min(3));
        tokio::select! {
            _ = shutdown.changed() => break 'outer,
            _ = tokio::time::sleep(backoff) => {}
        }
        match load_all_bundles(&inner, &opa, &stats).await {
            Ok(n) => {
                tracing::info!("Policy: reconciled {n} bundles after reconnect");
                last_rev = 0;
                attempt = 0;
            }
            Err(e) => {
                stats.load_error_total.fetch_add(1, Ordering::Relaxed);
                tracing::error!("Policy: reconcile failed: {e}; retrying");
            }
        }
    }
    tracing::info!("Policy: bundle watch task stopped");
}

// ──── PolicyService ────

/// 权限策略引擎
///
/// 基于规则的授权决策点，支持 RBAC/ABAC 策略评估。
/// 集成 OpaEngine 以支持 Rego bundle 管理和 explain。
/// Bundle 存储在 Server KV，多 Agent 共享。
pub struct PolicyService {
    /// RBAC 策略（本地内存）
    policies: RwLock<BTreeMap<String, Policy>>,
    started: RwLock<bool>,
    max_policies: usize,
    /// OPA 引擎（Regorus），用于 Rego 求值和 explain
    opa_engine: Arc<OpaEngine>,
    /// Agent 内部句柄（访问 Server KV）
    inner: Option<Arc<AgentInner>>,
    /// Bundle 分发态观测（G-POL-1）
    sync_stats: Arc<PolicySyncStats>,
    /// Watch 后台任务停机信号（仅带 KV 时存在）
    shutdown_tx: RwLock<Option<watch::Sender<()>>>,
}

impl std::fmt::Debug for PolicyService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let p = self.policies.read();
        f.debug_struct("PolicyService")
            .field("policy_count", &p.len())
            .field("started", &self.started)
            .field("has_kv", &self.inner.is_some())
            .finish()
    }
}

impl PolicyService {
    /// 创建不带 KV 的 PolicyService（仅 RBAC 引擎）
    pub fn new(max_policies: usize) -> Self {
        // OpaEngine::new 当前无失败路径（空引擎、无 Rego 依赖）
        let opa_engine = Arc::new(match OpaEngine::new(OpaConfig::default()) {
            Ok(engine) => engine,
            Err(e) => {
                // OpaEngine::new 当前无失败路径；若失败则退化为空引擎，绝不 panic。
                tracing::error!("create OpaEngine failed: {e}; falling back to empty engine");
                OpaEngine::default()
            }
        });
        Self {
            policies: RwLock::new(BTreeMap::new()),
            started: RwLock::new(false),
            max_policies,
            opa_engine,
            inner: None,
            sync_stats: Arc::new(PolicySyncStats::default()),
            shutdown_tx: RwLock::new(None),
        }
    }

    /// 创建带 Server KV 接入的 PolicyService（支持 bundle CRUD）
    pub fn with_kv(max_policies: usize, inner: Arc<AgentInner>) -> Self {
        // OpaEngine::new 当前无失败路径（空引擎、无 Rego 依赖）
        let opa_engine = Arc::new(match OpaEngine::new(OpaConfig::default()) {
            Ok(engine) => engine,
            Err(e) => {
                // OpaEngine::new 当前无失败路径；若失败则退化为空引擎，绝不 panic。
                tracing::error!("create OpaEngine failed: {e}; falling back to empty engine");
                OpaEngine::default()
            }
        });
        Self {
            policies: RwLock::new(BTreeMap::new()),
            started: RwLock::new(false),
            max_policies,
            opa_engine,
            inner: Some(inner),
            sync_stats: Arc::new(PolicySyncStats::default()),
            shutdown_tx: RwLock::new(None),
        }
    }

    /// Bundle 分发态观测统计（采样任务读入指标；G-POL-1）
    pub fn sync_stats(&self) -> Arc<PolicySyncStats> {
        Arc::clone(&self.sync_stats)
    }

    /// 获取 OPA 引擎引用
    pub fn opa_engine(&self) -> &Arc<OpaEngine> {
        &self.opa_engine
    }

    // ──── RBAC 策略管理 ────

    pub fn add_policy(&self, policy: Policy) -> ServiceResult<()> {
        let mut policies = self.policies.write();
        if policies.len() >= self.max_policies {
            return Err(format!("max policies ({}) reached", self.max_policies).into());
        }
        policies.insert(policy.id.clone(), policy);
        Ok(())
    }

    pub fn get_policy(&self, id: &str) -> ServiceResult<Option<Policy>> {
        let policies = self.policies.read();
        Ok(policies.get(id).cloned())
    }

    pub fn remove_policy(&self, id: &str) -> ServiceResult<bool> {
        let mut policies = self.policies.write();
        Ok(policies.remove(id).is_some())
    }

    pub fn list_policies(&self) -> ServiceResult<Vec<Policy>> {
        let policies = self.policies.read();
        Ok(policies.values().cloned().collect())
    }

    // ──── RBAC 策略评估 ────

    pub fn evaluate(&self, request: &AccessRequest) -> ServiceResult<PolicyDecision> {
        let policies = self.policies.read();
        let mut matches: Vec<&Policy> = policies
            .values()
            .filter(|p| self.policy_matches(p, request))
            .collect();
        matches.sort_by_key(|p| std::cmp::Reverse(p.priority));

        if let Some(deny) = matches.iter().find(|p| p.effect == PolicyEffect::Deny) {
            return Ok(PolicyDecision {
                effect: PolicyEffect::Deny,
                matched_policy_id: Some(deny.id.clone()),
                reason: format!("explicitly denied by policy '{}'", deny.name),
            });
        }

        if let Some(allow) = matches.iter().find(|p| p.effect == PolicyEffect::Allow) {
            return Ok(PolicyDecision {
                effect: PolicyEffect::Allow,
                matched_policy_id: Some(allow.id.clone()),
                reason: format!("allowed by policy '{}'", allow.name),
            });
        }

        Ok(PolicyDecision {
            effect: PolicyEffect::Deny,
            matched_policy_id: None,
            reason: "no matching policy (default deny)".into(),
        })
    }

    fn policy_matches(&self, policy: &Policy, request: &AccessRequest) -> bool {
        if !Self::match_any(&policy.subjects, &request.subject) {
            return false;
        }
        if !Self::match_any(&policy.actions, &request.action) {
            return false;
        }
        if !Self::match_any(&policy.resources, &request.resource) {
            return false;
        }
        for condition in &policy.conditions {
            if !Self::eval_condition(condition, &request.context) {
                return false;
            }
        }
        true
    }

    fn match_any(patterns: &[String], value: &str) -> bool {
        if patterns.is_empty() {
            return false;
        }
        patterns.iter().any(|p| Self::wildcard_match(p, value))
    }

    fn wildcard_match(pattern: &str, value: &str) -> bool {
        if pattern == "*" {
            return true;
        }
        if let Some(prefix) = pattern.strip_suffix('*') {
            return value.starts_with(prefix);
        }
        pattern == value
    }

    fn eval_condition(condition: &PolicyCondition, context: &HashMap<String, String>) -> bool {
        let attr_value = match context.get(&condition.attribute) {
            Some(v) => v,
            None => return false,
        };
        match condition.operator.as_str() {
            "eq" => attr_value == &condition.value,
            "neq" => attr_value != &condition.value,
            "contains" => attr_value.contains(&condition.value),
            "prefix" => attr_value.starts_with(&condition.value),
            "gte" | "lte" | "gt" | "lt" => {
                let a: f64 = match attr_value.parse() {
                    Ok(v) => v,
                    Err(_) => return false,
                };
                let b: f64 = match condition.value.parse() {
                    Ok(v) => v,
                    Err(_) => return false,
                };
                match condition.operator.as_str() {
                    "gte" => a >= b,
                    "lte" => a <= b,
                    "gt" => a > b,
                    "lt" => a < b,
                    _ => false,
                }
            }
            _ => false,
        }
    }

    // ──── Bundle 管理（Server KV 后端）───

    fn require_kv(&self) -> ServiceResult<&Arc<AgentInner>> {
        self.inner
            .as_ref()
            .ok_or_else(|| "Policy bundle API requires AgentInner (server KV connection)".into())
    }

    /// 原子更新当前 bundle 记录（Txn CAS 基于 per-key version，防止并发覆盖丢更新）。
    async fn cas_put_current(
        inner: &Arc<AgentInner>,
        key: &[u8],
        new_value: Vec<u8>,
        expected_version: i64,
    ) -> Result<bool, String> {
        let compare = Compare {
            result: CompareResult::Equal as i32,
            target: Target::Version as i32,
            key: key.to_vec(),
            target_value: Some(TargetValue::Version(expected_version)),
        };
        let put = RequestOp {
            op: Some(Op::RequestPut(PutRequest {
                key: key.to_vec(),
                value: new_value,
                lease_id: 0,
                prev_kv: false,
                request_id: vec![],
            })),
        };
        let resp = inner
            .client
            .txn()
            .txn(vec![compare], vec![put], vec![])
            .await
            .map_err(|e| format!("kv txn: {e}"))?;
        Ok(resp.succeeded)
    }

    /// 读取当前 bundle 记录及其 per-key version（CAS 基准）。
    /// 返回 (Option<record>, version)；记录不存在时 version 为 0。
    async fn read_current(
        inner: &Arc<AgentInner>,
        key: &[u8],
    ) -> Result<(Option<BundleRecord>, i64), String> {
        let (kvs, _count, _rev) = inner
            .client
            .kv()
            .range_with_lease_full(key, key, 1, 0, false, false)
            .await
            .map_err(|e| format!("kv range: {e}"))?;
        match kvs.into_iter().next() {
            Some((_k, v, _lease, ver)) if !v.is_empty() => {
                let rec: BundleRecord =
                    serde_json::from_slice(&v).map_err(|e| format!("deserialize bundle: {e}"))?;
                Ok((Some(rec), ver))
            }
            _ => Ok((None, 0)),
        }
    }

    /// 上传/更新 Rego 策略包到 Server KV。
    ///
    /// 语义（按 (tenant_id, namespace, name) 唯一键）：
    /// - 写入前先 OPA 编译校验，编译失败返回错误且不落库；
    /// - 每次成功上传版本号 +1，并写入历史版本快照（`@v{n}`）供回滚；
    /// - 当前记录通过 Txn CAS（per-key version）原子覆盖，并发冲突时有限重试；
    /// - 更新保留原有 enabled 状态。
    pub async fn put_bundle(
        &self,
        tenant_id: &str,
        namespace: &str,
        name: &str,
        rego: &str,
    ) -> ServiceResult<BundleInfo> {
        let inner = self.require_kv()?;

        // 1. 写入前 OPA 编译校验（失败不落库、不改引擎）——阻塞线程池执行
        {
            let opa = Arc::clone(&self.opa_engine);
            let rego_owned = rego.to_string();
            opa_blocking(move || opa.validate_rego(&rego_owned))
                .await
                .map_err(|e| format!("bundle rego compile error: {e}"))?;
        }

        let bundle_id = BundleRecord::make_bundle_id(tenant_id, namespace, name);
        let key = BundleRecord::storage_key(&bundle_id);
        let now = unix_ts_i64();

        let mut attempts = 0;
        loop {
            attempts += 1;
            if attempts > 5 {
                return Err("put_bundle: too many concurrent conflicts".into());
            }

            // 2. 读取当前记录 + per-key version（CAS 基准）
            let (current, cur_version) = Self::read_current(inner, &key).await?;
            let new_version = current.as_ref().map_or(1, |c| c.info.version + 1);

            // 3. 构建新记录（保留 enabled 状态）
            let record = match current {
                Some(mut rec) => {
                    rec.rego_content = rego.to_string();
                    rec.info.version = new_version;
                    rec.info.updated_at = now;
                    rec
                }
                None => BundleRecord::new(tenant_id, namespace, name, rego, new_version, now),
            };

            // 4. 写入历史版本快照（key 含版本号，幂等）
            let snapshot_key = BundleRecord::snapshot_key(&bundle_id, new_version);
            let snapshot_val =
                serde_json::to_vec(&record).map_err(|e| format!("serialize bundle: {e}"))?;
            inner
                .client
                .kv()
                .put(&snapshot_key, &snapshot_val)
                .await
                .map_err(|e| format!("kv put snapshot: {e}"))?;

            // 5. 原子覆盖当前记录（CAS on version）
            let value =
                serde_json::to_vec(&record).map_err(|e| format!("serialize bundle: {e}"))?;
            if Self::cas_put_current(inner, &key, value, cur_version).await? {
                // 6. 同步到本地 OpaEngine（仅 enabled bundle）——阻塞线程池执行
                if record.info.enabled {
                    let policy_id = format!("{}/{}", record.info.namespace, record.info.name);
                    let opa = Arc::clone(&self.opa_engine);
                    let rego_owned = record.rego_content.clone();
                    opa_blocking(move || opa.add_policy(&policy_id, &rego_owned))
                        .await
                        .map_err(|e| format!("opa add_policy: {e}"))?;
                }
                tracing::info!(
                    "Policy: put bundle '{}' v{} (tenant={}, ns={})",
                    name,
                    new_version,
                    tenant_id,
                    namespace
                );
                return Ok(record.info);
            }
            // CAS 冲突 → 重试
        }
    }

    /// 按历史版本回滚策略包。
    ///
    /// 语义：读取 `@v{version}` 快照 → 编译校验 → 作为新版本（当前版本+1）原子写入，
    /// 保留当前 enabled 状态；已是目标版本时返回错误。
    pub async fn rollback_bundle(
        &self,
        bundle_id: &str,
        version: i64,
    ) -> ServiceResult<BundleInfo> {
        let inner = self.require_kv()?;
        if version < 1 {
            return Err(format!("invalid rollback version: {version}").into());
        }

        // 1. 读取目标版本快照
        let snapshot_key = BundleRecord::snapshot_key(bundle_id, version);
        let snapshot = {
            let kvs = inner
                .client
                .kv()
                .range(&snapshot_key, &snapshot_key, 1, 0)
                .await
                .map_err(|e| format!("kv range: {e}"))?;
            match kvs.into_iter().next() {
                Some((_k, v)) if !v.is_empty() => serde_json::from_slice::<BundleRecord>(&v)
                    .map_err(|e| format!("deserialize snapshot: {e}"))?,
                _ => return Err(format!("bundle version {version} not found").into()),
            }
        };

        // 2. 编译校验快照 Rego——阻塞线程池执行
        {
            let opa = Arc::clone(&self.opa_engine);
            let rego_owned = snapshot.rego_content.clone();
            opa_blocking(move || opa.validate_rego(&rego_owned))
                .await
                .map_err(|e| format!("bundle rego compile error: {e}"))?;
        }

        let key = BundleRecord::storage_key(bundle_id);
        let now = unix_ts_i64();

        let mut attempts = 0;
        loop {
            attempts += 1;
            if attempts > 5 {
                return Err("rollback_bundle: too many concurrent conflicts".into());
            }

            // 3. 读取当前记录（存在性 + CAS 基准）
            let (current, cur_version) = Self::read_current(inner, &key).await?;
            let current = current.ok_or_else(|| format!("bundle '{bundle_id}' not found"))?;

            if current.info.version == version {
                return Err(format!("bundle already at version {version}").into());
            }

            // 4. 回滚 = 恢复快照内容为“新版本”（当前版本+1），保留 enabled
            let new_version = current.info.version + 1;
            let mut restored = snapshot.clone();
            restored.info.version = new_version;
            restored.info.enabled = current.info.enabled;
            restored.info.created_at = current.info.created_at;
            restored.info.updated_at = now;

            // 5. 写回滚后的新版本快照
            let new_snap_key = BundleRecord::snapshot_key(bundle_id, new_version);
            let new_snap_val =
                serde_json::to_vec(&restored).map_err(|e| format!("serialize bundle: {e}"))?;
            inner
                .client
                .kv()
                .put(&new_snap_key, &new_snap_val)
                .await
                .map_err(|e| format!("kv put snapshot: {e}"))?;

            // 6. 原子更新当前记录
            let value =
                serde_json::to_vec(&restored).map_err(|e| format!("serialize bundle: {e}"))?;
            if Self::cas_put_current(inner, &key, value, cur_version).await? {
                let policy_id = format!("{}/{}", restored.info.namespace, restored.info.name);
                let opa = Arc::clone(&self.opa_engine);
                let rego_owned = restored.rego_content.clone();
                opa_blocking(move || opa.add_policy(&policy_id, &rego_owned))
                    .await
                    .map_err(|e| format!("opa add_policy: {e}"))?;
                tracing::info!(
                    "Policy: rolled back bundle '{}' to v{} (now v{})",
                    bundle_id,
                    version,
                    new_version
                );
                return Ok(restored.info);
            }
            // CAS 冲突 → 重试
        }
    }

    /// 列出策略包的全部历史版本（用于回滚目标发现）。
    pub async fn list_bundle_versions(
        &self,
        bundle_id: &str,
    ) -> ServiceResult<Vec<BundleVersionInfo>> {
        let inner = self.require_kv()?;

        // 当前记录（判断 is_current + 当前版本号）
        let key = BundleRecord::storage_key(bundle_id);
        let (current, _v) = Self::read_current(inner, &key).await?;
        let current_version = current.as_ref().map(|c| c.info.version).unwrap_or(0);

        // 扫描该 bundle 下的全部版本快照
        let prefix = format!("/_policy/bundles/{bundle_id}@v").into_bytes();
        let range_end = prefix_end(&prefix);
        let pairs = inner
            .client
            .kv()
            .range(&prefix, &range_end, 0, 0)
            .await
            .map_err(|e| format!("kv range: {e}"))?;

        let mut versions: Vec<BundleVersionInfo> = Vec::new();
        for (_k, v) in pairs {
            if let Ok(rec) = serde_json::from_slice::<BundleRecord>(&v) {
                versions.push(BundleVersionInfo {
                    version: rec.info.version,
                    created_at: rec.info.updated_at,
                    is_current: rec.info.version == current_version,
                });
            }
        }
        versions.sort_by_key(|vi| vi.version);
        Ok(versions)
    }

    /// 从 Server KV 删除策略包
    pub async fn delete_bundle(&self, bundle_id: &str) -> ServiceResult<bool> {
        let inner = self.require_kv()?;
        let key = BundleRecord::storage_key(bundle_id);

        // 先读取 bundle 信息用于清理 OpaEngine
        let namespace_and_name = {
            let pairs = inner
                .client
                .kv()
                .range(&key, &key, 1, 0)
                .await
                .map_err(|e| format!("kv range: {e}"))?;
            if let Some((_k, v)) = pairs.into_iter().next() {
                let rec: BundleRecord =
                    serde_json::from_slice(&v).map_err(|e| format!("deserialize bundle: {e}"))?;
                Some((rec.info.namespace, rec.info.name))
            } else {
                None
            }
        };

        inner
            .client
            .kv()
            .delete(&key)
            .await
            .map_err(|e| format!("kv delete bundle: {e}"))?;

        // 从本地 OpaEngine 移除（触发引擎重建）——阻塞线程池执行
        if let Some((ns, name)) = namespace_and_name {
            let policy_id = format!("{}/{}", ns, name);
            let opa = Arc::clone(&self.opa_engine);
            let _ = opa_blocking(move || {
                opa.remove_policy(&policy_id);
                Ok::<(), String>(())
            })
            .await;
        }

        tracing::info!("Policy: deleted bundle '{}'", bundle_id);
        Ok(true)
    }

    /// 列出策略包（从 Server KV range scan；跳过历史版本快照）
    pub async fn list_bundles(&self, tenant_id: Option<&str>) -> ServiceResult<Vec<BundleInfo>> {
        let inner = self.require_kv()?;
        let prefix = BundleRecord::prefix_key();
        let range_end = prefix_end(&prefix);

        let pairs = inner
            .client
            .kv()
            .range(&prefix, &range_end, 0, 0)
            .await
            .map_err(|e| format!("kv range: {e}"))?;

        let mut bundles: Vec<BundleInfo> = Vec::new();
        for (k, v) in pairs {
            // 跳过版本快照（key 含 "@v"），仅返回当前记录
            if String::from_utf8_lossy(&k).contains("@v") {
                continue;
            }
            if let Ok(rec) = serde_json::from_slice::<BundleRecord>(&v) {
                if tenant_id.is_none_or(|tid| rec.info.tenant_id == tid) {
                    bundles.push(rec.info);
                }
            }
        }
        Ok(bundles)
    }

    /// 启用/禁用策略包（更新 Server KV）。
    ///
    /// 语义：幂等（状态未变化直接返回成功）+ 原子（Txn CAS 基于 per-key version）。
    pub async fn set_bundle_enabled(&self, bundle_id: &str, enabled: bool) -> ServiceResult<bool> {
        let inner = self.require_kv()?;
        let key = BundleRecord::storage_key(bundle_id);
        let now = unix_ts_i64();

        let mut attempts = 0;
        loop {
            attempts += 1;
            if attempts > 5 {
                return Err("set_bundle_enabled: too many concurrent conflicts".into());
            }

            let (rec, cur_version) = Self::read_current(inner, &key).await?;
            let rec = rec.ok_or_else(|| format!("bundle '{bundle_id}' not found"))?;

            // 幂等：状态未变化直接返回成功
            if rec.info.enabled == enabled {
                return Ok(true);
            }

            let mut rec = rec;
            rec.info.enabled = enabled;
            rec.info.updated_at = now;

            let new_val = serde_json::to_vec(&rec).map_err(|e| format!("serialize bundle: {e}"))?;
            if Self::cas_put_current(inner, &key, new_val, cur_version).await? {
                // 同步到本地 OpaEngine（add/remove 均触发引擎重建）——阻塞线程池执行
                let policy_id = format!("{}/{}", rec.info.namespace, rec.info.name);
                if enabled {
                    let opa = Arc::clone(&self.opa_engine);
                    let rego_owned = rec.rego_content.clone();
                    opa_blocking(move || opa.add_policy(&policy_id, &rego_owned))
                        .await
                        .map_err(|e| format!("opa add_policy: {e}"))?;
                } else {
                    let opa = Arc::clone(&self.opa_engine);
                    let _ = opa_blocking(move || {
                        opa.remove_policy(&policy_id);
                        Ok::<(), String>(())
                    })
                    .await;
                }
                tracing::info!("Policy: bundle '{}' enabled={}", bundle_id, enabled);
                return Ok(true);
            }
            // CAS 冲突 → 重试
        }
    }

    /// 解释策略决策（本地 OpaEngine）
    pub fn explain(&self, query: &str, input_json: &str) -> ServiceResult<String> {
        self.opa_engine
            .explain(query, input_json)
            .map_err(|e| e.into())
    }
}

// ──── BaseService trait ────

#[async_trait]
impl BaseService for PolicyService {
    fn name(&self) -> &'static str {
        "policy"
    }

    async fn start(&self) -> ServiceResult<()> {
        *self.started.write() = true;

        // G-POL-1：启动加载——全量 enabled bundles 进本地引擎（骨架模式跳过）
        if let Some(inner) = self.inner.clone() {
            match load_all_bundles(&inner, &self.opa_engine, &self.sync_stats).await {
                Ok(n) => tracing::info!("Policy: loaded {n} enabled bundle(s) at startup"),
                Err(e) => {
                    self.sync_stats
                        .load_error_total
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        "Policy: initial bundle load failed: {e}; watch + reconcile will retry"
                    );
                }
            }

            // G-POL-1：变更传播——Watch 订阅 + 断连对账（重启时先停旧任务）
            if let Some(tx) = self.shutdown_tx.write().take() {
                let _ = tx.send(());
            }
            let (tx, rx) = watch::channel::<()>(());
            *self.shutdown_tx.write() = Some(tx);
            let inner = inner.clone();
            let opa = Arc::clone(&self.opa_engine);
            let stats = Arc::clone(&self.sync_stats);
            tokio::spawn(run_bundle_watch(inner, opa, stats, rx));
        }

        tracing::info!("PolicyService started");
        Ok(())
    }

    async fn stop(&self) -> ServiceResult<()> {
        if let Some(tx) = self.shutdown_tx.write().take() {
            let _ = tx.send(());
        }
        *self.started.write() = false;
        self.policies.write().clear();
        tracing::info!("PolicyService stopped");
        Ok(())
    }

    fn health_check(&self) -> bool {
        *self.started.read()
    }
}

// ──── 工具函数 ────

/// 生成 range_end: 将 prefix 最后一个字节 +1
fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    for i in (0..end.len()).rev() {
        if end[i] < 0xff {
            end[i] += 1;
            end.truncate(i + 1);
            return end;
        }
    }
    // prefix 全为 0xff，返回空表示扫描到无穷
    vec![]
}

// ──── 单元测试 ────

#[cfg(test)]
mod tests {
    use super::*;

    fn new_svc() -> PolicyService {
        PolicyService::new(1024)
    }

    #[test]
    fn test_policy_add_and_evaluate() {
        let svc = new_svc();
        svc.add_policy(Policy {
            id: "p1".into(),
            name: "admin-access".into(),
            description: "".into(),
            effect: PolicyEffect::Allow,
            subjects: vec!["role:admin".into()],
            actions: vec!["*".into()],
            resources: vec!["*".into()],
            conditions: vec![],
            priority: 10,
        })
        .unwrap();

        let req = AccessRequest {
            subject: "role:admin".into(),
            action: "read".into(),
            resource: "/data".into(),
            context: HashMap::new(),
        };
        let decision = svc.evaluate(&req).unwrap();
        assert_eq!(decision.effect, PolicyEffect::Allow);
    }

    #[test]
    fn test_deny_overrides_allow() {
        let svc = new_svc();
        svc.add_policy(Policy {
            id: "allow-all".into(),
            name: "a".into(),
            description: "".into(),
            effect: PolicyEffect::Allow,
            subjects: vec!["*".into()],
            actions: vec!["*".into()],
            resources: vec!["*".into()],
            conditions: vec![],
            priority: 1,
        })
        .unwrap();
        svc.add_policy(Policy {
            id: "deny-bob".into(),
            name: "d".into(),
            description: "".into(),
            effect: PolicyEffect::Deny,
            subjects: vec!["user:bob".into()],
            actions: vec!["*".into()],
            resources: vec!["*".into()],
            conditions: vec![],
            priority: 100,
        })
        .unwrap();

        let req = AccessRequest {
            subject: "user:bob".into(),
            action: "read".into(),
            resource: "/data".into(),
            context: HashMap::new(),
        };
        let decision = svc.evaluate(&req).unwrap();
        assert_eq!(decision.effect, PolicyEffect::Deny);
    }

    #[test]
    fn test_default_deny() {
        let svc = new_svc();
        let req = AccessRequest {
            subject: "unknown".into(),
            action: "read".into(),
            resource: "/data".into(),
            context: HashMap::new(),
        };
        let decision = svc.evaluate(&req).unwrap();
        assert_eq!(decision.effect, PolicyEffect::Deny);
    }

    #[test]
    fn test_bundle_id_format() {
        let id = BundleRecord::make_bundle_id("tenant-1", "default", "my-policy");
        assert_eq!(id, "tenant-1/default/my-policy");
    }

    #[test]
    fn test_storage_key_format() {
        let key = BundleRecord::storage_key("tenant-1/default/my-policy");
        assert_eq!(
            String::from_utf8_lossy(&key),
            "/_policy/bundles/tenant-1/default/my-policy"
        );
    }

    #[test]
    fn test_snapshot_key_format() {
        let key = BundleRecord::snapshot_key("tenant-1/default/my-policy", 3);
        assert_eq!(
            String::from_utf8_lossy(&key),
            "/_policy/bundles/tenant-1/default/my-policy@v3"
        );
    }

    #[test]
    fn test_snapshot_key_distinct_from_current() {
        // 快照 key 必须与当前记录 key 不同，且不与 list_bundles 前缀扫描混淆（@v 过滤）
        let bundle_id = "t/d/p";
        let current = BundleRecord::storage_key(bundle_id);
        let snap = BundleRecord::snapshot_key(bundle_id, 1);
        assert_ne!(current, snap);
        assert!(String::from_utf8_lossy(&snap).contains("@v"));
        assert!(!String::from_utf8_lossy(&current).contains("@v"));
    }

    #[test]
    fn test_bundle_record_new_sets_version() {
        let rec = BundleRecord::new("tenant-1", "default", "my-policy", "package p", 7, 1234);
        assert_eq!(rec.info.version, 7);
        assert!(rec.info.enabled);
        assert_eq!(rec.info.created_at, 1234);
        assert_eq!(rec.rego_content, "package p");
    }

    #[test]
    fn test_bundle_info_version_roundtrip() {
        // 旧记录（无 version 字段）反序列化时 version 默认 0，保持向后兼容
        let old = br#"{"info":{"bundle_id":"t/d/p","name":"p","namespace":"d","tenant_id":"t","enabled":true,"created_at":1,"updated_at":2},"rego_content":"package p"}"#;
        let rec: BundleRecord = serde_json::from_slice(old).expect("deserialize legacy record");
        assert_eq!(rec.info.version, 0);
        assert_eq!(rec.info.name, "p");
        assert_eq!(rec.rego_content, "package p");
    }

    // ──── G-POL-1：bundle 加载/分发纯函数 ────

    #[test]
    fn test_policy_id_from_bundle_key() {
        assert_eq!(
            policy_id_from_bundle_key(b"/_policy/bundles/t1/default/p1"),
            Some("default/p1".to_string())
        );
        // 版本快照不直接消费（回滚/上传由当前记录事件驱动）
        assert_eq!(
            policy_id_from_bundle_key(b"/_policy/bundles/t1/default/p1@v3"),
            None
        );
        // 前缀不符
        assert_eq!(policy_id_from_bundle_key(b"/_policy/other/t1"), None);
        // 段数不符（非法 key 不得让任务 panic/误删）
        assert_eq!(policy_id_from_bundle_key(b"/_policy/bundles/t1/p1"), None);
    }

    #[test]
    fn test_collect_enabled_bundles_skips_snapshot_disabled_and_bad_json() {
        let enabled = BundleRecord::new("t1", "default", "p1", "package a", 1, 1);
        let mut disabled = BundleRecord::new("t1", "default", "p2", "package b", 1, 1);
        disabled.info.enabled = false;

        let pairs = vec![
            (
                BundleRecord::storage_key("t1/default/p1"),
                serde_json::to_vec(&enabled).expect("ser"),
            ),
            // 版本快照：即便内容 enabled 也不参与加载
            (
                BundleRecord::snapshot_key("t1/default/p1", 1),
                serde_json::to_vec(&enabled).expect("ser"),
            ),
            (
                BundleRecord::storage_key("t1/default/p2"),
                serde_json::to_vec(&disabled).expect("ser"),
            ),
            // 坏记录：跳过并计数，不阻断全量加载
            (
                BundleRecord::storage_key("t1/default/p3"),
                b"not-json".to_vec(),
            ),
        ];

        let (loaded, skipped) = collect_enabled_bundles(&pairs);
        assert_eq!(
            loaded,
            vec![("default/p1".to_string(), "package a".to_string())],
            "仅 enabled 当前记录进入引擎"
        );
        assert_eq!(skipped, 1, "坏记录计数但不阻断");
    }

    #[test]
    fn test_policy_sync_stats_default() {
        let stats = PolicySyncStats::default();
        assert_eq!(stats.loaded_bundles.load(Ordering::Relaxed), 0);
        assert_eq!(stats.last_success_unix.load(Ordering::Relaxed), 0);
        assert_eq!(stats.load_ok_total.load(Ordering::Relaxed), 0);
        assert_eq!(stats.load_error_total.load(Ordering::Relaxed), 0);
    }
}
