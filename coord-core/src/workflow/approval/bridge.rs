//! 审批桥：唯一写路径（开节点 / 授权 + 命令 / 效果→事件 / 终态上报）。
//!
//! 引擎（[`super::engine`]）是纯计算，存储（[`super::store`]）只有一条带 CAS 的写路径。
//! 本模块把两者接起来，并集中承载三件引擎不负责的事：
//!
//! - **开节点**：[`ApprovalBridge::ensure_node_open`] 把 `approve` 节点降级为席位，
//!   幂等地进入分支并开启首轮；
//! - **授权 + 命令**：[`ApprovalBridge::command`] 在调用引擎前做外部授权判定（自审 /
//!   加签管理 / 驳回 / 终止），引擎仍负责席位归属判定，二者都不放松；
//! - **效果→事件**：命令产生的 [`Effect::Event`] 经 [`super::events::event_record`]
//!   fail-closed 构造出站记录，事件 id 由 `(approval_id, rev_after, index)` 确定性生成，
//!   使重放/重试产出同一批 id。
//!
//! 所有写入都走 [`super::store::ApprovalStore`] 的 CAS：冲突返回 `Ok(false)` 时重读并
//! 重放命令（≤3 次），绝不盲写覆盖。授权判定与写路径在同一次 `load` 上完成，因此授权
//! 结论与提交的状态属于同一修订号。

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::{json, Value};

use crate::workflow::durable::OutboxRecord;
use crate::workflow::model::{ApproveTask, WorkflowInstance};
use crate::workflow::ports::{Clock, StoreError};

use super::engine::{Engine, StartSpec};
use super::events::event_record;
use super::model::{
    ApprovalError, ApprovalPolicy, BranchStatus, Effect, Id, InstanceStatus, Principal, Scope,
    SeatSpec, TaskKind,
};
use super::store::{ApprovalStore, ExpectedRev, IdemRecord};

/// CAS 冲突后的最大尝试次数（含首次）：达到即放弃并返回 [`BridgeError::Stale`]。
const MAX_COMMIT_ATTEMPTS: u32 = 3;

/// 内部生命周期权威 actor：终止等由工作流运行时驱动的内部动作以它执行，不受节点
/// `sign_admins` 用户授权面约束（用户路径仍只认 `sign_admins`）。
pub const SYSTEM_ACTOR: &str = "system";

/// 席位解析端口：把 `approve` 节点的角色名展开为具体审批人。
///
/// 解析在启动/命令路径注入（定义解析期只做形状校验）。返回 `None` 表示无映射，调用方
/// 必须 fail-closed，不得把未解析角色静默当作空席位。
pub trait ApproverResolver: Send + Sync {
    /// 解析角色为审批人列表；无映射返回 `None`。
    fn resolve(&self, role: &str) -> Option<Vec<Principal>>;
}

/// 静态映射的 [`ApproverResolver`]（角色名 → 审批人列表）。
pub struct MapApproverResolver {
    map: HashMap<String, Vec<Principal>>,
}

impl MapApproverResolver {
    /// 用角色 → 审批人列表的映射构造解析器。
    pub fn new(map: HashMap<String, Vec<Principal>>) -> Self {
        Self { map }
    }
}

impl ApproverResolver for MapApproverResolver {
    fn resolve(&self, role: &str) -> Option<Vec<Principal>> {
        self.map.get(role).cloned()
    }
}

/// 审批命令的结果。
#[derive(Debug, Clone, PartialEq)]
pub enum CommandOutcome {
    /// 命令已生效：`response` 为返回给调用方的响应；`node_outcome` 非空表示命令操作的
    /// 分支本轮已判为通过 / 否决；`terminal` 非空表示实例已终态。
    Applied {
        response: Value,
        node_outcome: Option<NodeOutcome>,
        terminal: Option<ApprovalOutcome>,
    },
    /// 幂等键命中：命令此前已成功提交，原样返回首次的响应，引擎未被触碰。
    Duplicate(Value),
}

/// [`ApprovalBridge::expire_due`] 的结果：过期席位数 + 过期导致的分支结论。
///
/// 过期本身不产生实例终态；`node_outcome` 只承载「过期把最后一批未决席位收敛成
/// 通过 / 否决」这一混合判定（全员过期 / 仍 open 为 `None`）。
#[derive(Debug, Clone, PartialEq)]
pub struct ExpireOutcome {
    pub expired: u32,
    pub node_outcome: Option<NodeOutcome>,
}

/// 节点（分支/轮次）本轮的外部上报口径：通过或否决。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeOutcome {
    Passed,
    Rejected,
}

/// 实例终态的外部上报口径。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalOutcome {
    Completed,
    Rejected,
    Cancelled,
    Terminated,
}

/// 桥的外部命令。`task_id` / `branch_id` 指向引擎内的席位与分支。
#[derive(Debug, Clone, PartialEq)]
pub enum ApprovalCommand<'a> {
    Approve {
        task_id: &'a str,
        comment: Option<&'a str>,
    },
    Reject {
        task_id: &'a str,
        comment: Option<&'a str>,
    },
    Acknowledge {
        task_id: &'a str,
    },
    AddSign {
        branch_id: &'a str,
        seats: Vec<SeatSpec>,
        comment: Option<&'a str>,
    },
    RemoveSign {
        task_ids: Vec<String>,
    },
    ReturnToNode {
        branch_id: &'a str,
        target_node: &'a str,
        comment: Option<&'a str>,
    },
    Transfer {
        task_id: &'a str,
        to: &'a str,
    },
    Delegate {
        task_id: &'a str,
        to: &'a str,
    },
    Withdraw,
    Terminate {
        reason: &'a str,
    },
}

/// 桥错误：`ApprovalError` 与 `StoreError` 的忠实映射 + 桥自身的授权/并发错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BridgeError {
    NotFound(String),
    NotAuthorized(String),
    InvalidTransition(String),
    InvalidArgument(String),
    /// CAS 冲突重试耗尽（未盲写）。
    Stale(String),
    Store(String),
    Serialization(String),
}

impl std::fmt::Display for BridgeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BridgeError::NotFound(x) => write!(f, "not found: {x}"),
            BridgeError::NotAuthorized(x) => write!(f, "not authorized: {x}"),
            BridgeError::InvalidTransition(x) => write!(f, "invalid transition: {x}"),
            BridgeError::InvalidArgument(x) => write!(f, "invalid argument: {x}"),
            BridgeError::Stale(x) => write!(f, "stale: {x}"),
            BridgeError::Store(x) => write!(f, "store error: {x}"),
            BridgeError::Serialization(x) => write!(f, "serialization error: {x}"),
        }
    }
}

impl std::error::Error for BridgeError {}

impl From<ApprovalError> for BridgeError {
    fn from(e: ApprovalError) -> Self {
        match e {
            ApprovalError::NotFound(x) => BridgeError::NotFound(x),
            ApprovalError::InvalidTransition(x) => BridgeError::InvalidTransition(x),
            ApprovalError::NotAuthorized(x) => BridgeError::NotAuthorized(x),
            ApprovalError::InvalidArgument(x) => BridgeError::InvalidArgument(x),
            ApprovalError::StaleRevision { expected, actual } => {
                BridgeError::Stale(format!("expected {expected}, actual {actual}"))
            }
            ApprovalError::SerializationError(x) => BridgeError::Serialization(x),
        }
    }
}

impl From<StoreError> for BridgeError {
    fn from(e: StoreError) -> Self {
        match e {
            StoreError::NotFound(x) => BridgeError::NotFound(x),
            StoreError::SerializationError(x) => BridgeError::Serialization(x),
            other => BridgeError::Store(other.to_string()),
        }
    }
}

/// 审批桥：store + clock + 可选的席位解析器。
pub struct ApprovalBridge {
    store: Arc<dyn ApprovalStore>,
    clock: Arc<dyn Clock>,
    resolver: Option<Arc<dyn ApproverResolver>>,
}

impl ApprovalBridge {
    /// 用存储、时钟与可选的角色解析器构造。
    pub fn new(
        store: Arc<dyn ApprovalStore>,
        clock: Arc<dyn Clock>,
        resolver: Option<Arc<dyn ApproverResolver>>,
    ) -> Self {
        Self {
            store,
            clock,
            resolver,
        }
    }

    /// 打开 `approve` 节点：幂等地进入分支并开启首轮。
    ///
    /// 实例不存在则先 `Engine::start`（`CreateOnly`），已存在且已有同 `node` 的 open
    /// 分支则直接复用不重开（分支 + 首轮在一次提交里生效，不存在「分支开了轮次没开」的
    /// 中间态）。`task.roles` 逐角色经 [`ApproverResolver`] 展开；任一角色无映射即
    /// fail-closed，且解析后若没有任何可计票的 [`TaskKind::Approval`] 席位，绝不静默
    /// 开出一个「无人可批」的节点（否则纯阅签 / 角色未解析会立即自动通过）。
    ///
    /// 多节点流程的分支父子关系（parent）由运行时接线决定，本方法只进根节点。
    pub async fn ensure_node_open(
        &self,
        wf: &WorkflowInstance,
        node: &str,
        task: &ApproveTask,
    ) -> Result<(), BridgeError> {
        let seats = self.resolve_seats(task, &wf.context)?;
        let policy = ApprovalPolicy {
            allow_self_approve: task.allow_self_approve,
            sign_admins: task.sign_admins.clone(),
        };
        let now = self.clock.now_ms();
        let deadline = task.deadline_ms(now);

        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let committed = match self.store.load(&wf.id).await.map_err(BridgeError::from)? {
                Some((mut engine, mod_rev)) => {
                    if engine
                        .branches
                        .values()
                        .any(|b| b.node == node && b.status == BranchStatus::Open)
                    {
                        return Ok(());
                    }
                    let events = self.open_node_events(
                        &mut engine,
                        node,
                        task,
                        &seats,
                        &policy,
                        deadline,
                        now,
                    )?;
                    self.store
                        .commit(&engine, &events, ExpectedRev::Exact(mod_rev))
                        .await
                        .map_err(BridgeError::from)?
                }
                None => {
                    let mut engine = Engine::start(
                        StartSpec {
                            id: wf.id.clone(),
                            definition_ns: wf.definition_ns.clone(),
                            definition_name: wf.definition_name.clone(),
                            definition_version: wf.definition_version.clone(),
                            definition_hash: wf.definition_hash.clone().unwrap_or_default(),
                            initiator: wf.initiator.clone().unwrap_or_else(|| "anonymous".into()),
                            context: workflow_scope(&wf.context),
                        },
                        now,
                    )?;
                    let events = self.open_node_events(
                        &mut engine,
                        node,
                        task,
                        &seats,
                        &policy,
                        deadline,
                        now,
                    )?;
                    self.store
                        .commit(&engine, &events, ExpectedRev::CreateOnly)
                        .await
                        .map_err(BridgeError::from)?
                }
            };
            if committed {
                return Ok(());
            }
            if attempt >= MAX_COMMIT_ATTEMPTS {
                return Err(BridgeError::Stale(format!("ensure_node_open {node}")));
            }
        }
    }

    /// 执行外部命令：先查幂等键，再授权 + 应用 + CAS 提交。
    ///
    /// `idem_key` 命中即返回 [`CommandOutcome::Duplicate`]（引擎不执行）；未命中时该键
    /// 与状态、事件在同一提交里写，因此「响应能读到」等价于「命令已生效」。
    pub async fn command(
        &self,
        approval_id: &str,
        actor: &str,
        cmd: ApprovalCommand<'_>,
        idem_key: Option<&str>,
    ) -> Result<CommandOutcome, BridgeError> {
        self.command_inner(approval_id, actor, None, cmd, idem_key)
            .await
    }

    /// 以「服务代终端用户」形式执行命令。
    ///
    /// `actor` 是被代表的终端用户（授权判定按它做席位归属），`acting_service` 是持有
    /// CCT 的服务主体（授权白名单由 gRPC handler 先判定）。投票/审计记录同时保留二者：
    /// `VoteRecord.actor` / `AuditEntry.by` = 被代表用户，`acting_service` = 执行服务。
    pub async fn command_on_behalf(
        &self,
        approval_id: &str,
        actor: &str,
        acting_service: &str,
        cmd: ApprovalCommand<'_>,
        idem_key: Option<&str>,
    ) -> Result<CommandOutcome, BridgeError> {
        self.command_inner(approval_id, actor, Some(acting_service), cmd, idem_key)
            .await
    }

    async fn command_inner(
        &self,
        approval_id: &str,
        actor: &str,
        acting_service: Option<&str>,
        cmd: ApprovalCommand<'_>,
        idem_key: Option<&str>,
    ) -> Result<CommandOutcome, BridgeError> {
        let now = self.clock.now_ms();
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            // 每轮重查幂等键：并发同 key 的命令里，先提交者写下了响应，慢者的 CAS
            // 冲突重试必须在此命中 `Duplicate` 而不是拿新修订号再 Apply 一次。
            if let Some(key) = idem_key {
                if let Some(cached) = self
                    .store
                    .load_idem(approval_id, key)
                    .await
                    .map_err(BridgeError::from)?
                {
                    return Ok(CommandOutcome::Duplicate(cached));
                }
            }
            let (mut engine, mod_rev) = self
                .store
                .load(approval_id)
                .await
                .map_err(BridgeError::from)?
                .ok_or_else(|| BridgeError::NotFound(approval_id.to_string()))?;
            let branch_id = self.command_branch(&engine, &cmd);
            // 归因只对本次命令有效：引擎是本次加载的局部聚合，提交前清空，
            // 避免随存储实现（如内存 store 直接持有对象）泄漏到下一条命令。
            engine.set_acting_service(acting_service.map(str::to_string));
            let applied = self.apply_command(&mut engine, actor, cmd.clone(), now);
            engine.set_acting_service(None);
            let effects = applied?;
            let response = response_for(&engine);
            let node_outcome = branch_id
                .as_deref()
                .and_then(|id| engine.branches.get(id))
                .and_then(|b| node_outcome_for(b.status));
            let terminal = terminal_outcome(engine.instance.status);
            let rev_after = engine.instance.revision;
            let events = self.effects_to_records(approval_id, &effects, rev_after, now)?;
            let idem = idem_key.map(|key| IdemRecord {
                key: key.to_string(),
                response: response.clone(),
            });
            let committed = self
                .store
                .commit_with_idem(&engine, &events, ExpectedRev::Exact(mod_rev), idem.as_ref())
                .await
                .map_err(BridgeError::from)?;
            if committed {
                return Ok(CommandOutcome::Applied {
                    response,
                    node_outcome,
                    terminal,
                });
            }
            if attempt >= MAX_COMMIT_ATTEMPTS {
                return Err(BridgeError::Stale(format!("command on {approval_id}")));
            }
        }
    }

    /// 底层存储的只读句柄（查询面：实例快照投影 + 待办分页扫描）。
    ///
    /// 只暴露读；写路径仍只有本模块的命令方法（服务层不得绕过桥直接写）。
    pub fn store(&self) -> &Arc<dyn ApprovalStore> {
        &self.store
    }

    /// 仍 open 的席位中最早的 `deadline`（无 open 席位或无 deadline 返回 `None`）。
    ///
    /// 纯只读：不改引擎状态、不提交。运行时据此在席位到期后重排下一条
    /// `ApprovalDeadline` 定时器（仍有更晚到期的 open 席位 ⇒ 继续驱动）。
    pub async fn next_open_deadline(&self, approval_id: &str) -> Result<Option<i64>, BridgeError> {
        let Some((engine, _)) = self
            .store
            .load(approval_id)
            .await
            .map_err(BridgeError::from)?
        else {
            return Ok(None);
        };
        Ok(engine
            .tasks
            .values()
            .filter(|t| t.status.is_open())
            .filter_map(|t| t.deadline)
            .min())
    }

    /// 过期所有 `deadline <= now_ms` 的未决席位；返回过期计数与该次过期导致的节点结论。
    ///
    /// 过期只把席位置 `Expired`（经引擎 `recount`，全员过期/减签耗尽保持轮次 `Open`），
    /// 不产生实例终态——超时的最终处置由外部策略决定，桥不代判。但过期可能把「已有票 +
    /// 过期席」的混合轮次收敛为通过 / 否决（`node_outcome`），这是截止路径的恢复触发器。
    pub async fn expire_due(
        &self,
        approval_id: &str,
        now_ms: i64,
    ) -> Result<ExpireOutcome, BridgeError> {
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let (mut engine, mod_rev) = self
                .store
                .load(approval_id)
                .await
                .map_err(BridgeError::from)?
                .ok_or_else(|| BridgeError::NotFound(approval_id.to_string()))?;
            let due: Vec<Id> = engine
                .tasks
                .values()
                .filter(|t| t.status.is_open() && t.deadline.is_some_and(|d| d <= now_ms))
                .map(|t| t.id.clone())
                .collect();
            if due.is_empty() {
                return Ok(ExpireOutcome {
                    expired: 0,
                    node_outcome: None,
                });
            }
            // 过期前记录 due 席位所属且仍 open 的分支：过期收敛后据此判定是否形成结论。
            let affected: Vec<Id> = due
                .iter()
                .filter_map(|id| engine.tasks.get(id))
                .filter(|t| {
                    engine
                        .branches
                        .get(&t.branch_id)
                        .is_some_and(|b| b.status == BranchStatus::Open)
                })
                .map(|t| t.branch_id.clone())
                .collect();
            let mut effects = Vec::new();
            for task_id in &due {
                let expected = engine.instance.revision;
                effects.extend(engine.expire(expected, task_id, now_ms)?);
            }
            let rev_after = engine.instance.revision;
            let events = self.effects_to_records(approval_id, &effects, rev_after, now_ms)?;
            let committed = self
                .store
                .commit(&engine, &events, ExpectedRev::Exact(mod_rev))
                .await
                .map_err(BridgeError::from)?;
            if committed {
                let node_outcome = affected
                    .iter()
                    .filter_map(|b| engine.branches.get(b))
                    .find_map(|b| node_outcome_for(b.status));
                return Ok(ExpireOutcome {
                    expired: due.len() as u32,
                    node_outcome,
                });
            }
            if attempt >= MAX_COMMIT_ATTEMPTS {
                return Err(BridgeError::Stale(format!("expire_due on {approval_id}")));
            }
        }
    }

    /// 定义走到末端时收口实例：仅当没有 open 分支、根分支已 `Completed`、且实例尚未
    /// `Completed` 时调用引擎 `finish`。返回 `Some(Completed)` 只在首次收口时出现。
    pub async fn finish_if_complete(
        &self,
        approval_id: &str,
    ) -> Result<Option<ApprovalOutcome>, BridgeError> {
        let now = self.clock.now_ms();
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let (mut engine, mod_rev) = self
                .store
                .load(approval_id)
                .await
                .map_err(BridgeError::from)?
                .ok_or_else(|| BridgeError::NotFound(approval_id.to_string()))?;
            if engine.instance.status == InstanceStatus::Completed
                || engine
                    .branches
                    .values()
                    .any(|b| b.status == BranchStatus::Open)
            {
                return Ok(None);
            }
            let root_completed = engine
                .branches
                .values()
                .filter(|b| b.parent_branch_id.is_none())
                .max_by_key(|b| b.created_at)
                .map(|b| b.status == BranchStatus::Completed)
                .unwrap_or(false);
            if !root_completed {
                return Ok(None);
            }
            let expected = engine.instance.revision;
            let effects = engine.finish(expected, now)?;
            let rev_after = engine.instance.revision;
            let events = self.effects_to_records(approval_id, &effects, rev_after, now)?;
            let committed = self
                .store
                .commit(&engine, &events, ExpectedRev::Exact(mod_rev))
                .await
                .map_err(BridgeError::from)?;
            if committed {
                return Ok(terminal_outcome(engine.instance.status));
            }
            if attempt >= MAX_COMMIT_ATTEMPTS {
                return Err(BridgeError::Stale(format!(
                    "finish_if_complete {approval_id}"
                )));
            }
        }
    }

    /// 管理路径强制终止实例；用户 actor 仅 `sign_admins` 可调用，内部生命周期权威
    /// [`SYSTEM_ACTOR`] 恒可调用。
    pub async fn terminate(
        &self,
        approval_id: &str,
        actor: &str,
        reason: &str,
    ) -> Result<(), BridgeError> {
        let now = self.clock.now_ms();
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let (mut engine, mod_rev) = self
                .store
                .load(approval_id)
                .await
                .map_err(BridgeError::from)?
                .ok_or_else(|| BridgeError::NotFound(approval_id.to_string()))?;
            self.guard_terminate(&engine, actor)?;
            let expected = engine.instance.revision;
            let effects = engine.terminate(expected, actor, reason, now)?;
            let rev_after = engine.instance.revision;
            let events = self.effects_to_records(approval_id, &effects, rev_after, now)?;
            let committed = self
                .store
                .commit(&engine, &events, ExpectedRev::Exact(mod_rev))
                .await
                .map_err(BridgeError::from)?;
            if committed {
                return Ok(());
            }
            if attempt >= MAX_COMMIT_ATTEMPTS {
                return Err(BridgeError::Stale(format!("terminate {approval_id}")));
            }
        }
    }

    // ------------------------------------------------------------ 内部实现

    /// 展开 `task` 的席位：运行时表达式 + 静态名单 + 角色解析结果；并做可计票席位守卫。
    ///
    /// 四个席位来源（`approvers` / `pool` / `readers` / `roles`）的每个条目都支持
    /// `${ ... }` 运行时表达式，按实例 `context` 求值（`$input` 与 `$context` 同值，
    /// 与 `with` 的求值口径一致）。表达式可返回单个主体字符串或主体数组；空结果、
    /// 非字符串元素与空主体一律 fail-closed——席位来源解析不出人时绝不能开出
    /// 「无人可批」的节点（空轮次会被引擎立即判通过）。
    fn resolve_seats(
        &self,
        task: &ApproveTask,
        context: &Value,
    ) -> Result<Vec<SeatSpec>, BridgeError> {
        let mut seats: Vec<SeatSpec> = self
            .expand_source("approvers", &task.approvers, context)?
            .iter()
            .map(|p| SeatSpec::approver(p))
            .collect();
        let pool = self.expand_source("pool", &task.pool, context)?;
        if !pool.is_empty() {
            seats.push(SeatSpec::pool(pool));
        }
        seats.extend(
            self.expand_source("readers", &task.readers, context)?
                .iter()
                .map(|p| SeatSpec::reader(p)),
        );
        for role in self.expand_source("roles", &task.roles, context)? {
            let principals = self
                .resolver
                .as_ref()
                .and_then(|r| r.resolve(&role))
                .ok_or_else(|| BridgeError::InvalidArgument(format!("approve.role.{role}")))?;
            seats.extend(principals.iter().map(|p| SeatSpec::approver(p)));
        }
        // 无可计票席位（纯阅签 / 角色全未解析）绝不能开节点：空轮次会被引擎判为通过。
        if !seats.iter().any(|s| s.kind == TaskKind::Approval) {
            return Err(BridgeError::InvalidArgument("approve seats".into()));
        }
        Ok(seats)
    }

    /// 展开单个席位来源：字面量原样保留，`${ ... }` 条目按实例上下文求值。
    ///
    /// 声明了来源却展开为空（`[]` / `null`）即拒绝，错误信息点名来源键。
    fn expand_source(
        &self,
        source: &str,
        entries: &[String],
        context: &Value,
    ) -> Result<Vec<Principal>, BridgeError> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let invalid = || BridgeError::InvalidArgument(format!("approve.{source}"));
        let evaluator = crate::workflow::expression::ExpressionEvaluator::new();
        // 与运行时 `with` 的口径一致：`$input` 与 `$context` 绑定同一实例上下文。
        let vars = HashMap::from([("input".to_string(), context.clone())]);
        let mut principals = Vec::new();
        for entry in entries {
            let trimmed = entry.trim();
            let is_expression = trimmed.starts_with("${") && trimmed.ends_with('}');
            if !is_expression {
                principals.push(entry.clone());
                continue;
            }
            let value = evaluator
                .evaluate_with_vars(trimmed, context, &vars)
                .map_err(|e| BridgeError::InvalidArgument(format!("approve.{source}: {e}")))?;
            match value {
                Value::String(principal) => principals.push(principal),
                Value::Array(items) => {
                    for item in items {
                        match item {
                            Value::String(principal) => principals.push(principal),
                            _ => return Err(invalid()),
                        }
                    }
                }
                _ => return Err(invalid()),
            }
        }
        if principals.is_empty() || principals.iter().any(|p| p.is_empty()) {
            return Err(invalid());
        }
        Ok(principals)
    }

    /// 在已加载的引擎上进入节点 + 开启首轮，返回两段命令的全部效果记录。
    #[allow(clippy::too_many_arguments)]
    fn open_node_events(
        &self,
        engine: &mut Engine,
        node: &str,
        task: &ApproveTask,
        seats: &[SeatSpec],
        policy: &ApprovalPolicy,
        deadline: Option<i64>,
        now: i64,
    ) -> Result<Vec<OutboxRecord>, BridgeError> {
        let (branch_id, enter_effects) = engine.enter_branch(
            engine.instance.revision,
            node,
            None,
            task.mode,
            task.to_reject(),
            policy.clone(),
            now,
        )?;
        let mut effects = enter_effects;
        let (_, _, open_effects) = engine.open_approval(
            engine.instance.revision,
            &branch_id,
            seats.to_vec(),
            deadline,
            now,
        )?;
        effects.extend(open_effects);
        let rev_after = engine.instance.revision;
        let approval_id = engine.instance.id.clone();
        self.effects_to_records(&approval_id, &effects, rev_after, now)
    }

    /// 把命令应用到引擎：先做外部授权判定，再调用对应的引擎命令。
    fn apply_command(
        &self,
        engine: &mut Engine,
        actor: &str,
        cmd: ApprovalCommand<'_>,
        now: i64,
    ) -> Result<Vec<Effect>, BridgeError> {
        let expected = engine.instance.revision;
        let effects = match cmd {
            ApprovalCommand::Approve { task_id, comment } => {
                self.guard_self_approve(engine, actor, task_id)?;
                engine.approve(expected, task_id, actor, comment, now)
            }
            ApprovalCommand::Reject { task_id, comment } => {
                self.guard_self_approve(engine, actor, task_id)?;
                engine.reject(expected, task_id, actor, comment, now)
            }
            ApprovalCommand::Acknowledge { task_id } => {
                self.guard_self_approve(engine, actor, task_id)?;
                engine.acknowledge(expected, task_id, actor, now)
            }
            ApprovalCommand::Transfer { task_id, to } => {
                self.guard_self_approve(engine, actor, task_id)?;
                engine.transfer(expected, task_id, to, actor, now)
            }
            ApprovalCommand::Delegate { task_id, to } => {
                self.guard_self_approve(engine, actor, task_id)?;
                engine.delegate(expected, task_id, to, actor, now)
            }
            ApprovalCommand::AddSign {
                branch_id,
                seats,
                comment,
            } => {
                self.guard_sign_admin(engine, actor, branch_id)?;
                engine
                    .add_sign(expected, branch_id, seats, actor, comment, now)
                    .map(|(_, effects)| effects)
            }
            ApprovalCommand::RemoveSign { task_ids } => {
                let branch_id = self.branch_of_tasks(engine, &task_ids)?;
                self.guard_sign_admin(engine, actor, &branch_id)?;
                engine.remove_sign(expected, &branch_id, &task_ids, actor, now)
            }
            ApprovalCommand::ReturnToNode {
                branch_id,
                target_node,
                comment,
            } => {
                self.guard_return_to_node(engine, actor, branch_id)?;
                // 驳回重开必须与开席**同批**：`return_to_node` 只建新 attempt 分支、不开
                // 轮次，若不同批 `open_approval`，节点会停在「Open 但零席位」——无待办、
                // 无 deadline、无人可投，工作流永久挂起。席位取目标节点最近一次 attempt
                // 的席位；重开不设 deadline（旧的绝对截止早已过期，沿用会立即过期），
                // 兜底出口是 `sign_admins` 的 Terminate / AddSign。
                let seats = prior_seats(engine, target_node)?;
                let (new_branch, mut effects) =
                    engine.return_to_node(expected, branch_id, target_node, actor, comment, now)?;
                let rev = engine.instance.revision;
                let (_, _, open_effects) =
                    engine.open_approval(rev, &new_branch, seats, None, now)?;
                effects.extend(open_effects);
                Ok(effects)
            }
            ApprovalCommand::Withdraw => engine.withdraw(expected, actor, now),
            ApprovalCommand::Terminate { reason } => {
                self.guard_terminate(engine, actor)?;
                engine.terminate(expected, actor, reason, now)
            }
        };
        effects.map_err(BridgeError::from)
    }

    /// 自审守卫：发起人批自己的节点时，除非节点策略允许，一律拒绝。
    /// 发起人未知（`"anonymous"`）时不适用该规则。
    fn guard_self_approve(
        &self,
        engine: &Engine,
        actor: &str,
        task_id: &str,
    ) -> Result<(), BridgeError> {
        if engine.instance.initiator == "anonymous" || engine.instance.initiator != actor {
            return Ok(());
        }
        let Some(branch) = engine
            .tasks
            .get(task_id)
            .and_then(|t| engine.branches.get(&t.branch_id))
        else {
            // 席位/分支不存在由引擎给出 NotFound，不在此抢先报授权错。
            return Ok(());
        };
        if branch.policy.allow_self_approve {
            return Ok(());
        }
        Err(BridgeError::NotAuthorized(format!(
            "{actor} cannot approve a node they initiated"
        )))
    }

    /// 加签 / 减签守卫：仅分支的 `sign_admins` 或实例发起人。
    fn guard_sign_admin(
        &self,
        engine: &Engine,
        actor: &str,
        branch_id: &str,
    ) -> Result<(), BridgeError> {
        let branch = engine
            .branches
            .get(branch_id)
            .ok_or_else(|| BridgeError::NotFound(branch_id.to_string()))?;
        if branch.policy.sign_admins.iter().any(|a| a == actor)
            || engine.instance.initiator == actor
        {
            return Ok(());
        }
        Err(BridgeError::NotAuthorized(format!(
            "{actor} is not a sign admin of {branch_id}"
        )))
    }

    /// 驳回至节点守卫：当前分支的未决席位持有人、`sign_admins` 或实例发起人。
    fn guard_return_to_node(
        &self,
        engine: &Engine,
        actor: &str,
        branch_id: &str,
    ) -> Result<(), BridgeError> {
        let branch = engine
            .branches
            .get(branch_id)
            .ok_or_else(|| BridgeError::NotFound(branch_id.to_string()))?;
        if branch.policy.sign_admins.iter().any(|a| a == actor)
            || engine.instance.initiator == actor
        {
            return Ok(());
        }
        let holds_seat = engine.tasks.values().any(|t| {
            t.branch_id == branch.id
                && t.status.is_open()
                && (t.assignee.as_deref() == Some(actor)
                    || t.candidates.iter().any(|c| c == actor)
                    || t.delegation.as_ref().map(|d| d.to.as_str()) == Some(actor))
        });
        if holds_seat {
            return Ok(());
        }
        Err(BridgeError::NotAuthorized(format!(
            "{actor} holds no open seat on {branch_id}"
        )))
    }

    /// 终止守卫：内部生命周期权威 [`SYSTEM_ACTOR`] 恒可调用；用户 actor 仅 `sign_admins`
    /// （实例任一分支声明者）。
    fn guard_terminate(&self, engine: &Engine, actor: &str) -> Result<(), BridgeError> {
        if actor == SYSTEM_ACTOR {
            return Ok(());
        }
        let allowed = engine
            .branches
            .values()
            .any(|b| b.policy.sign_admins.iter().any(|a| a == actor));
        if allowed {
            Ok(())
        } else {
            Err(BridgeError::NotAuthorized(format!(
                "{actor} is not a sign admin"
            )))
        }
    }

    /// 命令操作的分支：投票类取席位的 `branch_id`，加签/减签/驳回至节点取给定分支，
    /// 撤回/终止不指向任何分支。用于命令提交后判定该分支是否已收敛为结论。
    fn command_branch(&self, engine: &Engine, cmd: &ApprovalCommand<'_>) -> Option<Id> {
        match cmd {
            ApprovalCommand::Approve { task_id, .. }
            | ApprovalCommand::Reject { task_id, .. }
            | ApprovalCommand::Acknowledge { task_id }
            | ApprovalCommand::Transfer { task_id, .. }
            | ApprovalCommand::Delegate { task_id, .. } => {
                engine.tasks.get(*task_id).map(|t| t.branch_id.clone())
            }
            ApprovalCommand::AddSign { branch_id, .. }
            | ApprovalCommand::ReturnToNode { branch_id, .. } => Some((*branch_id).to_string()),
            ApprovalCommand::RemoveSign { task_ids } => self.branch_of_tasks(engine, task_ids).ok(),
            ApprovalCommand::Withdraw | ApprovalCommand::Terminate { .. } => None,
        }
    }

    /// 减签目标的所属分支：取首个目标的席位分支；空目标退化为当前 open 分支。
    fn branch_of_tasks(&self, engine: &Engine, task_ids: &[String]) -> Result<Id, BridgeError> {
        if let Some(first) = task_ids.first() {
            let task = engine
                .tasks
                .get(first)
                .ok_or_else(|| BridgeError::NotFound(first.clone()))?;
            return Ok(task.branch_id.clone());
        }
        engine
            .branches
            .values()
            .find(|b| b.status == BranchStatus::Open)
            .map(|b| b.id.clone())
            .ok_or_else(|| BridgeError::InvalidTransition("no open branch".into()))
    }

    /// 效果批次 → 出站记录：每个 `Event` 一条，id 由 `(approval_id, rev_after, index)`
    /// 确定性生成（重放/重试产出同一批 id）。
    fn effects_to_records(
        &self,
        approval_id: &str,
        effects: &[Effect],
        rev_after: u64,
        now: i64,
    ) -> Result<Vec<OutboxRecord>, BridgeError> {
        let mut records = Vec::new();
        for effect in effects {
            if let Effect::Event { name, data } = effect {
                let id = format!("{approval_id}:{rev_after}:{}", records.len());
                records.push(event_record(approval_id, name, data, now, id)?);
            }
        }
        Ok(records)
    }
}

/// 实例状态的响应标签（大写）。
fn status_label(status: InstanceStatus) -> &'static str {
    match status {
        InstanceStatus::Pending => "PENDING",
        InstanceStatus::Running => "RUNNING",
        InstanceStatus::Completed => "COMPLETED",
        InstanceStatus::Rejected => "REJECTED",
        InstanceStatus::Terminated => "TERMINATED",
        InstanceStatus::Cancelled => "CANCELLED",
        InstanceStatus::Faulted => "FAULTED",
    }
}

/// 命令响应：`{"instance_id": ..., "status": <UPPERCASE>}`（幂等缓存同值）。
fn response_for(engine: &Engine) -> Value {
    json!({
        "instance_id": engine.instance.id.clone(),
        "status": status_label(engine.instance.status),
    })
}

/// 终态映射：仅 `Completed/Rejected/Cancelled/Terminated` 产生外部终态。
fn terminal_outcome(status: InstanceStatus) -> Option<ApprovalOutcome> {
    match status {
        InstanceStatus::Completed => Some(ApprovalOutcome::Completed),
        InstanceStatus::Rejected => Some(ApprovalOutcome::Rejected),
        InstanceStatus::Cancelled => Some(ApprovalOutcome::Cancelled),
        InstanceStatus::Terminated => Some(ApprovalOutcome::Terminated),
        InstanceStatus::Pending | InstanceStatus::Running | InstanceStatus::Faulted => None,
    }
}

/// 分支状态 → 节点结论：只有通过 / 否决产生结论，其余（含仍 `Open`）为 `None`。
fn node_outcome_for(status: BranchStatus) -> Option<NodeOutcome> {
    match status {
        BranchStatus::Completed => Some(NodeOutcome::Passed),
        BranchStatus::Rejected => Some(NodeOutcome::Rejected),
        _ => None,
    }
}

/// 目标节点最近一次 attempt 的席位，供驳回重开复用。
///
/// 席位按 `Task` 逐字还原（`kind` / `assignee` / `candidates`）。目标节点无可复用席位时
/// fail-closed：绝不留下「Open 但零席位」的不可恢复分支。
fn prior_seats(engine: &Engine, target_node: &str) -> Result<Vec<SeatSpec>, ApprovalError> {
    let branch = match engine
        .branches
        .values()
        .filter(|b| b.node == target_node)
        .max_by_key(|b| b.attempt)
    {
        Some(b) => b,
        None => {
            return Err(ApprovalError::InvalidTransition(
                "return target has no prior seats".into(),
            ))
        }
    };
    let seats: Vec<SeatSpec> = engine
        .tasks
        .values()
        .filter(|t| t.branch_id == branch.id)
        .map(|t| SeatSpec {
            assignee: t.assignee.clone(),
            candidates: t.candidates.clone(),
            kind: t.kind,
        })
        .collect();
    if seats.is_empty() {
        return Err(ApprovalError::InvalidTransition(
            "return target has no prior seats".into(),
        ));
    }
    Ok(seats)
}

/// 工作流实例的 JSON 上下文 → 审批实例作用域；非对象（含 `Null`）取空作用域。
fn workflow_scope(context: &Value) -> Scope {
    match context {
        Value::Object(map) => map.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        _ => Scope::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::approval::model::{
        Branch, InstanceStatus as ApprovalStatus, JoinPolicy, RejectPolicy, RoundStatus, TaskStatus,
    };
    use crate::workflow::approval::store::{ApprovalInstanceEntry, MemoryApprovalStore};
    use crate::workflow::ports::test_utils::TestClock;
    use serde_json::json;

    const ID: &str = "inst-bridge";
    const T0: i64 = 10_000;

    fn wf() -> WorkflowInstance {
        WorkflowInstance {
            id: ID.into(),
            definition_ns: "test".into(),
            definition_name: "wf".into(),
            definition_version: "1.0.0".into(),
            status: crate::workflow::model::InstanceStatus::Running,
            context: Value::Null,
            task_stack: vec![],
            current_task_index: 0,
            created_at: T0,
            updated_at: T0,
            output: None,
            fault: None,
            suspension_meta: None,
            initiator: Some("zhangsan".into()),
            definition_hash: Some("sha256:bridge".into()),
            engine_version: None,
        }
    }

    fn wf_with_context(context: Value) -> WorkflowInstance {
        let mut instance = wf();
        instance.context = context;
        instance
    }

    fn approve_task(approvers: &[&str]) -> ApproveTask {
        ApproveTask {
            approvers: approvers.iter().map(|s| s.to_string()).collect(),
            pool: vec![],
            readers: vec![],
            roles: vec![],
            mode: JoinPolicy::All,
            veto: true,
            deadline: None,
            on_pass: None,
            on_reject: None,
            sign_admins: vec![],
            allow_self_approve: false,
        }
    }

    fn build(
        resolver: Option<Arc<dyn ApproverResolver>>,
    ) -> (ApprovalBridge, Arc<MemoryApprovalStore>) {
        let store = Arc::new(MemoryApprovalStore::new());
        let clock: Arc<dyn Clock> = Arc::new(TestClock::new(T0));
        let bridge = ApprovalBridge::new(store.clone(), clock, resolver);
        (bridge, store)
    }

    /// 加载实例并返回某审批人的未决席位 id。
    async fn task_id_for(store: &MemoryApprovalStore, assignee: &str) -> String {
        let (engine, _) = store.load(ID).await.unwrap().expect("instance");
        engine
            .tasks
            .values()
            .find(|t| t.assignee.as_deref() == Some(assignee))
            .map(|t| t.id.clone())
            .expect("task for assignee")
    }

    #[tokio::test]
    async fn ensure_node_open_starts_engine_and_persists() {
        let (bridge, store) = build(None);
        bridge
            .ensure_node_open(&wf(), "review", &approve_task(&["alice"]))
            .await
            .unwrap();

        let (engine, rev) = store.load(ID).await.unwrap().expect("persisted");
        assert_eq!(rev, 1);
        assert_eq!(engine.instance.id, ID);
        assert_eq!(engine.instance.initiator, "zhangsan");
        assert_eq!(engine.instance.definition_hash, "sha256:bridge");
        assert_eq!(engine.instance.status, ApprovalStatus::Running);
        assert_eq!(engine.branches.len(), 1);
        assert_eq!(engine.tasks.len(), 1);
        let branch = engine.branches.values().next().unwrap();
        assert_eq!(branch.node, "review");
        assert_eq!(branch.status, BranchStatus::Open);
        assert_eq!(
            engine
                .rounds
                .values()
                .filter(|r| r.status == RoundStatus::Open)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn ensure_node_open_reuses_existing_open_branch() {
        let (bridge, store) = build(None);
        let task = approve_task(&["alice"]);
        bridge
            .ensure_node_open(&wf(), "review", &task)
            .await
            .unwrap();
        let before = store.load(ID).await.unwrap().unwrap();
        let events_before = store.events(ID).await;

        bridge
            .ensure_node_open(&wf(), "review", &task)
            .await
            .unwrap();

        let after = store.load(ID).await.unwrap().unwrap();
        assert_eq!(before, after, "复用不得改状态或修订号");
        assert_eq!(after.0.branches.len(), 1, "不得重开分支");
        assert_eq!(after.0.tasks.len(), 1, "不得重复派席位");
        assert_eq!(after.1, 1, "不得再提交");
        assert_eq!(store.events(ID).await, events_before);
    }

    #[tokio::test]
    async fn ensure_node_open_expands_roles_via_resolver() {
        let resolver = MapApproverResolver::new(HashMap::from([(
            "manager".to_string(),
            vec!["alice".to_string(), "bob".to_string()],
        )]));
        let (bridge, store) = build(Some(Arc::new(resolver)));
        let mut task = approve_task(&[]);
        task.roles = vec!["manager".into()];

        bridge
            .ensure_node_open(&wf(), "review", &task)
            .await
            .unwrap();

        let (engine, _) = store.load(ID).await.unwrap().unwrap();
        let mut assignees: Vec<&str> = engine
            .tasks
            .values()
            .filter_map(|t| t.assignee.as_deref())
            .collect();
        assignees.sort_unstable();
        assert_eq!(assignees, vec!["alice", "bob"]);
    }

    #[tokio::test]
    async fn ensure_node_open_unresolved_role_fails_closed() {
        let resolver = MapApproverResolver::new(HashMap::new());
        let (bridge, store) = build(Some(Arc::new(resolver)));
        let mut task = approve_task(&[]);
        task.roles = vec!["manager".into()];

        let err = bridge
            .ensure_node_open(&wf(), "review", &task)
            .await
            .unwrap_err();
        assert_eq!(
            err,
            BridgeError::InvalidArgument("approve.role.manager".into())
        );
        assert!(store.load(ID).await.unwrap().is_none(), "不得开节点");
    }

    #[tokio::test]
    async fn ensure_node_open_expands_runtime_approvers_from_input() {
        let (bridge, store) = build(None);
        let task = approve_task(&["${ $input.approvers }"]);

        bridge
            .ensure_node_open(
                &wf_with_context(json!({"approvers": ["alice", "bob"]})),
                "review",
                &task,
            )
            .await
            .unwrap();

        let (engine, _) = store.load(ID).await.unwrap().unwrap();
        let mut assignees: Vec<&str> = engine
            .tasks
            .values()
            .filter_map(|t| t.assignee.as_deref())
            .collect();
        assignees.sort_unstable();
        assert_eq!(assignees, vec!["alice", "bob"]);
    }

    #[tokio::test]
    async fn ensure_node_open_expands_runtime_role_names() {
        let resolver = MapApproverResolver::new(HashMap::from([(
            "manager".to_string(),
            vec!["alice".to_string()],
        )]));
        let (bridge, store) = build(Some(Arc::new(resolver)));
        let mut task = approve_task(&[]);
        task.roles = vec!["${ $input.role }".into()];

        bridge
            .ensure_node_open(
                &wf_with_context(json!({"role": "manager"})),
                "review",
                &task,
            )
            .await
            .unwrap();

        let (engine, _) = store.load(ID).await.unwrap().unwrap();
        assert_eq!(
            engine
                .tasks
                .values()
                .filter_map(|t| t.assignee.as_deref())
                .collect::<Vec<_>>(),
            vec!["alice"]
        );
    }

    #[tokio::test]
    async fn ensure_node_open_empty_runtime_approvers_fails_closed() {
        let (bridge, store) = build(None);
        let task = approve_task(&["${ $input.approvers }"]);

        let err = bridge
            .ensure_node_open(&wf_with_context(json!({"approvers": []})), "review", &task)
            .await
            .unwrap_err();
        assert_eq!(
            err,
            BridgeError::InvalidArgument("approve.approvers".into())
        );
        assert!(store.load(ID).await.unwrap().is_none(), "不得开节点");
    }

    #[tokio::test]
    async fn ensure_node_open_non_string_runtime_approvers_fails_closed() {
        let (bridge, store) = build(None);
        let task = approve_task(&["${ $input.approvers }"]);

        let err = bridge
            .ensure_node_open(
                &wf_with_context(json!({"approvers": [41]})),
                "review",
                &task,
            )
            .await
            .unwrap_err();
        assert_eq!(
            err,
            BridgeError::InvalidArgument("approve.approvers".into())
        );
        assert!(store.load(ID).await.unwrap().is_none(), "不得开节点");
    }

    #[tokio::test]
    async fn ensure_node_open_readers_only_fails_closed() {
        let (bridge, store) = build(None);
        let mut task = approve_task(&[]);
        task.readers = vec!["alice".into()];

        let err = bridge
            .ensure_node_open(&wf(), "review", &task)
            .await
            .unwrap_err();
        assert_eq!(err, BridgeError::InvalidArgument("approve seats".into()));
        assert!(store.load(ID).await.unwrap().is_none(), "不得开节点");
    }

    #[tokio::test]
    async fn self_approve_blocked_by_default() {
        let (bridge, store) = build(None);
        // 发起人 zhangsan 自己是审批人；策略默认不允许自审。
        bridge
            .ensure_node_open(&wf(), "review", &approve_task(&["zhangsan"]))
            .await
            .unwrap();
        let task_id = task_id_for(&store, "zhangsan").await;

        let err = bridge
            .command(
                ID,
                "zhangsan",
                ApprovalCommand::Approve {
                    task_id: &task_id,
                    comment: None,
                },
                None,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, BridgeError::NotAuthorized(_)), "{err:?}");
    }

    #[tokio::test]
    async fn self_approve_allowed_when_policy_permits() {
        let (bridge, store) = build(None);
        let mut task = approve_task(&["zhangsan"]);
        task.allow_self_approve = true;
        bridge
            .ensure_node_open(&wf(), "review", &task)
            .await
            .unwrap();
        let task_id = task_id_for(&store, "zhangsan").await;

        let outcome = bridge
            .command(
                ID,
                "zhangsan",
                ApprovalCommand::Approve {
                    task_id: &task_id,
                    comment: None,
                },
                None,
            )
            .await
            .unwrap();
        assert!(matches!(outcome, CommandOutcome::Applied { .. }));
    }

    #[tokio::test]
    async fn non_assignee_vote_rejected() {
        let (bridge, store) = build(None);
        bridge
            .ensure_node_open(&wf(), "review", &approve_task(&["alice"]))
            .await
            .unwrap();
        let task_id = task_id_for(&store, "alice").await;

        // mallory 非发起人、非席位：桥放行，由引擎的席位归属判定拒绝。
        let err = bridge
            .command(
                ID,
                "mallory",
                ApprovalCommand::Approve {
                    task_id: &task_id,
                    comment: None,
                },
                None,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, BridgeError::NotAuthorized(_)), "{err:?}");
    }

    #[tokio::test]
    async fn add_sign_requires_sign_admin_or_initiator() {
        let (bridge, store) = build(None);
        let mut task = approve_task(&["alice"]);
        task.sign_admins = vec!["admin".into()];
        bridge
            .ensure_node_open(&wf(), "review", &task)
            .await
            .unwrap();
        let (engine, _) = store.load(ID).await.unwrap().unwrap();
        let branch_id = engine.branches.keys().next().unwrap().clone();

        let err = bridge
            .command(
                ID,
                "mallory",
                ApprovalCommand::AddSign {
                    branch_id: &branch_id,
                    seats: vec![SeatSpec::approver("dave")],
                    comment: None,
                },
                None,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, BridgeError::NotAuthorized(_)), "{err:?}");

        let outcome = bridge
            .command(
                ID,
                "admin",
                ApprovalCommand::AddSign {
                    branch_id: &branch_id,
                    seats: vec![SeatSpec::approver("dave")],
                    comment: None,
                },
                None,
            )
            .await
            .unwrap();
        assert!(matches!(outcome, CommandOutcome::Applied { .. }));
    }

    #[tokio::test]
    async fn terminate_requires_sign_admin() {
        let (bridge, store) = build(None);
        let mut task = approve_task(&["alice"]);
        task.sign_admins = vec!["admin".into()];
        bridge
            .ensure_node_open(&wf(), "review", &task)
            .await
            .unwrap();

        let err = bridge.terminate(ID, "mallory", "audit").await.unwrap_err();
        assert!(matches!(err, BridgeError::NotAuthorized(_)), "{err:?}");

        bridge.terminate(ID, "admin", "audit").await.unwrap();
        let (engine, _) = store.load(ID).await.unwrap().unwrap();
        assert_eq!(engine.instance.status, ApprovalStatus::Terminated);
    }

    /// 内部生命周期权威（[`SYSTEM_ACTOR`]）在**默认空** `sign_admins` 策略下也可终止：
    /// 取消工作流不得因节点未声明系统角色而留下仍可投票的 open 审批。
    #[tokio::test]
    async fn terminate_allows_system_actor_on_default_policy() {
        let (bridge, store) = build(None);
        bridge
            .ensure_node_open(&wf(), "review", &approve_task(&["alice"]))
            .await
            .unwrap();

        bridge
            .terminate(ID, SYSTEM_ACTOR, "workflow_cancelled")
            .await
            .unwrap();

        let (engine, _) = store.load(ID).await.unwrap().unwrap();
        assert_eq!(engine.instance.status, ApprovalStatus::Terminated);
    }

    #[tokio::test]
    async fn command_idem_replay_returns_cached_response_and_writes_once() {
        let (bridge, store) = build(None);
        bridge
            .ensure_node_open(&wf(), "review", &approve_task(&["alice"]))
            .await
            .unwrap();
        let task_id = task_id_for(&store, "alice").await;
        let events_after_open = store.events(ID).await.len();

        let first = bridge
            .command(
                ID,
                "alice",
                ApprovalCommand::Approve {
                    task_id: &task_id,
                    comment: None,
                },
                Some("idem-1"),
            )
            .await
            .unwrap();
        let response = match first {
            CommandOutcome::Applied { response, .. } => response,
            other => panic!("expected Applied, got {other:?}"),
        };
        let (_, rev_after_first) = store.load(ID).await.unwrap().unwrap();
        let events_after_first = store.events(ID).await;
        assert!(
            events_after_first.len() > events_after_open,
            "命令应写出事件"
        );

        // 重放：命中幂等键，返回缓存响应，不重复应用、不重复写事件。
        let second = bridge
            .command(
                ID,
                "alice",
                ApprovalCommand::Approve {
                    task_id: &task_id,
                    comment: None,
                },
                Some("idem-1"),
            )
            .await
            .unwrap();
        assert_eq!(second, CommandOutcome::Duplicate(response));
        let (_, rev_after_second) = store.load(ID).await.unwrap().unwrap();
        assert_eq!(rev_after_first, rev_after_second, "重放不得再提交");
        assert_eq!(
            store.events(ID).await,
            events_after_first,
            "重放不得重复写事件"
        );
    }

    /// 并发同 key：当本轮 CAS 冲突（他人已推进修订号）时，重试环必须先重查幂等键，
    /// 命中即返回 `Duplicate`，**不得**拿新修订号再 Apply 一次。
    #[tokio::test]
    async fn command_idem_retry_after_cas_conflict_returns_duplicate() {
        // 包装内存存储：open 提交（首轮）透传；命令提交一律 CAS 冲突；
        // 幂等键第二次读取才命中（模拟先提交者已写下响应）。
        struct ConflictOnceStore {
            inner: Arc<MemoryApprovalStore>,
            idem_reads: std::sync::atomic::AtomicUsize,
            commits: std::sync::atomic::AtomicUsize,
            cached: Value,
        }

        #[async_trait::async_trait]
        impl ApprovalStore for ConflictOnceStore {
            async fn load(&self, id: &str) -> Result<Option<(Engine, i64)>, StoreError> {
                self.inner.load(id).await
            }
            async fn load_idem(&self, id: &str, key: &str) -> Result<Option<Value>, StoreError> {
                if self
                    .idem_reads
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                    >= 1
                {
                    Ok(Some(self.cached.clone()))
                } else {
                    self.inner.load_idem(id, key).await
                }
            }
            async fn commit_with_idem(
                &self,
                engine: &Engine,
                events: &[OutboxRecord],
                expected: ExpectedRev,
                idem: Option<&IdemRecord>,
            ) -> Result<bool, StoreError> {
                let n = self
                    .commits
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if n == 0 {
                    self.inner
                        .commit_with_idem(engine, events, expected, idem)
                        .await
                } else {
                    Ok(false)
                }
            }
            async fn scan(
                &self,
                after: Option<&str>,
                limit: usize,
            ) -> Result<Vec<ApprovalInstanceEntry>, StoreError> {
                self.inner.scan(after, limit).await
            }
        }

        let inner = Arc::new(MemoryApprovalStore::new());
        let clock: Arc<dyn Clock> = Arc::new(TestClock::new(T0));
        let cached = json!({"instance_id": ID, "status": "RUNNING"});
        let bridge = ApprovalBridge::new(
            Arc::new(ConflictOnceStore {
                inner: Arc::clone(&inner),
                idem_reads: std::sync::atomic::AtomicUsize::new(0),
                commits: std::sync::atomic::AtomicUsize::new(0),
                cached: cached.clone(),
            }),
            clock,
            None,
        );
        bridge
            .ensure_node_open(&wf(), "review", &approve_task(&["alice"]))
            .await
            .unwrap();
        let task_id = task_id_for(&inner, "alice").await;

        let outcome = bridge
            .command(
                ID,
                "alice",
                ApprovalCommand::Approve {
                    task_id: &task_id,
                    comment: None,
                },
                Some("idem-race"),
            )
            .await
            .unwrap();
        assert_eq!(outcome, CommandOutcome::Duplicate(cached));
        // 冲突轮不落状态：状态仍是 open 单席，命令未被重复应用。
        let (engine, _) = inner.load(ID).await.unwrap().unwrap();
        assert!(engine.tasks.values().all(|t| t.status.is_open()));
    }

    #[tokio::test]
    async fn expire_due_marks_expired_without_pass() {
        let (bridge, store) = build(None);
        let mut task = approve_task(&["alice"]);
        task.deadline = Some("PT1H".into());
        bridge
            .ensure_node_open(&wf(), "review", &task)
            .await
            .unwrap();
        let deadline = task.deadline_ms(T0).unwrap();

        let outcome = bridge.expire_due(ID, deadline).await.unwrap();
        assert_eq!(outcome.expired, 1);
        assert_eq!(outcome.node_outcome, None);

        let (engine, _) = store.load(ID).await.unwrap().unwrap();
        assert!(engine
            .tasks
            .values()
            .all(|t| t.status == TaskStatus::Expired));
        // 单席位过期后轮次保持 Open：绝不自动通过（Q4 无自动判定）。
        assert!(engine
            .rounds
            .values()
            .all(|r| r.status == RoundStatus::Open));
        assert_eq!(engine.instance.status, ApprovalStatus::Running);
    }

    /// 命令使分支收敛为 `Completed` ⇒ 上报 `Some(Passed)`（实例仍非终态）。
    #[tokio::test]
    async fn command_reports_node_passed_when_branch_closes() {
        let (bridge, store) = build(None);
        bridge
            .ensure_node_open(&wf(), "review", &approve_task(&["alice"]))
            .await
            .unwrap();
        let task_id = task_id_for(&store, "alice").await;

        let outcome = bridge
            .command(
                ID,
                "alice",
                ApprovalCommand::Approve {
                    task_id: &task_id,
                    comment: None,
                },
                None,
            )
            .await
            .unwrap();

        match outcome {
            CommandOutcome::Applied {
                node_outcome,
                terminal,
                ..
            } => {
                assert_eq!(node_outcome, Some(NodeOutcome::Passed));
                assert_eq!(terminal, None, "分支通过不自动收口实例");
            }
            other => panic!("expected Applied, got {other:?}"),
        }
    }

    /// 过期把「1 票通过 + 1 席过期」的混合轮次收敛为通过 ⇒ 上报 `Some(Passed)`。
    #[tokio::test]
    async fn expire_due_reports_node_passed_when_last_seat_expires_into_pass() {
        let (bridge, store) = build(None);
        let mut task = approve_task(&["alice", "bob"]);
        task.deadline = Some("PT1H".into());
        bridge
            .ensure_node_open(&wf(), "review", &task)
            .await
            .unwrap();
        let deadline = task.deadline_ms(T0).unwrap();

        // alice 先通过：会签未满，轮次仍 Open。
        let alice = task_id_for(&store, "alice").await;
        let outcome = bridge
            .command(
                ID,
                "alice",
                ApprovalCommand::Approve {
                    task_id: &alice,
                    comment: None,
                },
                None,
            )
            .await
            .unwrap();
        assert!(
            matches!(
                outcome,
                CommandOutcome::Applied {
                    node_outcome: None,
                    ..
                }
            ),
            "一票未定不得上报结论：{outcome:?}"
        );

        // bob 到期：混合判定为通过。
        let expired = bridge.expire_due(ID, deadline).await.unwrap();
        assert_eq!(expired.expired, 1);
        assert_eq!(expired.node_outcome, Some(NodeOutcome::Passed));

        let (engine, _) = store.load(ID).await.unwrap().unwrap();
        assert_eq!(
            engine.branches.values().next().unwrap().status,
            BranchStatus::Completed
        );
    }

    #[tokio::test]
    async fn finish_if_complete_emits_completed_once() {
        let (bridge, store) = build(None);
        bridge
            .ensure_node_open(&wf(), "review", &approve_task(&["alice"]))
            .await
            .unwrap();
        let task_id = task_id_for(&store, "alice").await;
        bridge
            .command(
                ID,
                "alice",
                ApprovalCommand::Approve {
                    task_id: &task_id,
                    comment: None,
                },
                None,
            )
            .await
            .unwrap();
        let events_after_approve = store.events(ID).await.len();

        let first = bridge.finish_if_complete(ID).await.unwrap();
        assert_eq!(first, Some(ApprovalOutcome::Completed));
        let (engine, _) = store.load(ID).await.unwrap().unwrap();
        assert_eq!(engine.instance.status, ApprovalStatus::Completed);
        let events_after_finish = store.events(ID).await;
        assert!(events_after_finish.len() > events_after_approve);

        let second = bridge.finish_if_complete(ID).await.unwrap();
        assert_eq!(second, None, "已收口实例不得再次 finish");
        assert_eq!(
            store.events(ID).await,
            events_after_finish,
            "不得重复发完成事件"
        );
    }

    #[tokio::test]
    async fn policy_serde_default_compat() {
        // 旧文档无 `policy` 字段：反序列化为保守缺省，而不是报错。
        let mut engine = Engine::start(
            StartSpec {
                id: ID.into(),
                definition_ns: "test".into(),
                definition_name: "wf".into(),
                definition_version: "1.0.0".into(),
                definition_hash: "sha256:x".into(),
                initiator: "zhangsan".into(),
                context: Scope::new(),
            },
            T0,
        )
        .unwrap();
        let (branch_id, _) = engine
            .enter_branch(
                0,
                "review",
                None,
                JoinPolicy::All,
                RejectPolicy::AnyReject,
                ApprovalPolicy::default(),
                T0,
            )
            .unwrap();
        let branch = engine.branches.get(&branch_id).unwrap().clone();

        let mut value = serde_json::to_value(&branch).unwrap();
        assert!(value.get("policy").is_some(), "新文档须带 policy");
        value.as_object_mut().unwrap().remove("policy");

        let back: Branch = serde_json::from_value(value).unwrap();
        assert_eq!(back.policy, ApprovalPolicy::default());

        // 显式策略原样往返。
        let with_policy = Branch {
            policy: ApprovalPolicy {
                allow_self_approve: true,
                sign_admins: vec!["admin".into()],
            },
            ..branch
        };
        let round = serde_json::to_value(&with_policy).unwrap();
        assert_eq!(
            round["policy"],
            json!({"allow_self_approve": true, "sign_admins": ["admin"]})
        );
        let back: Branch = serde_json::from_value(round).unwrap();
        assert_eq!(back.policy, with_policy.policy);
    }
}
