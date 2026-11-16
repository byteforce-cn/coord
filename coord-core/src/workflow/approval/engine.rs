//! 命令层：状态机转换、不变量与效果流。
//!
//! 不变量（全部由命令方法内部强制）：
//! - 所有命令带 `expected_revision`（乐观并发守卫），陈旧写入返回
//!   [`ApprovalError::StaleRevision`] 且状态零变化；
//! - 终态实例拒绝一切变更命令；
//! - 同一实例同时最多一个 Open 分支（并发分支语义未定）；
//! - 一个分支同时最多一个 Open 轮次；
//! - 阅签不计票；已取消/过期席位不计入 `total_count`。
//!
//! 转换规则与判定顺序见 `spike/2026-10/spike-1-state-model/docs/TRANSITIONS.md`，实体字段见
//! 同目录的 `MODEL.md`（冻结证据）。本模块是纯计算：不读时钟、不做 I/O、不生成根标识——
//! 时间戳与实例 id 由调用方注入，因此同输入必然同输出。

use super::model::*;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;

/// 驳回至节点的累计上限（防驳回循环的模型层守卫；真实上限由定义侧配置）。
pub const MAX_JUMPS: u32 = 20;

/// 实例 id 的长度上限：实例 id 进入 KV 键，超长会越过后端键边界。
const MAX_INSTANCE_ID_LEN: usize = 128;

/// [`Engine::start`] 的入参：调用方注入的实例标识、定义身份与发起人。
#[derive(Debug, Clone)]
pub struct StartSpec {
    /// 实例 id，由调用方生成并注入（引擎不生成根标识）。
    pub id: String,
    pub definition_ns: String,
    pub definition_name: String,
    pub definition_version: String,
    /// 实例启动时钉死的定义内容哈希；漂移校验在加载定义的驱动层。
    pub definition_hash: String,
    pub initiator: Principal,
    /// 代 `initiator` 发起实例的服务主体（Start `on_behalf_of`）；`None` = 直接发起。
    pub initiator_acting_service: Option<Principal>,
    /// 实例级变量（作用域第三级）。
    pub context: Scope,
}

/// v2 审批实例的命令聚合根：实例 + 全部子实体 + 子实体 ID 生成所需的状态。
///
/// 不变量：聚合整体可序列化，`seq` 与 `node_attempts` 随之持久化——重载后继续执行既不
/// 产生重复的子实体 ID，节点进入次数也不倒退。两者没有时钟/随机数输入，同一输入序列
/// 必然产生同一 ID 序列（可重放）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Engine {
    pub instance: Instance,
    pub branches: BTreeMap<Id, Branch>,
    pub tasks: BTreeMap<Id, Task>,
    pub rounds: BTreeMap<Id, Round>,
    /// 节点名 → 已进入次数；同时充当驳回白名单。
    node_attempts: BTreeMap<String, u32>,
    /// 已分配的子实体序号（持久化，重载后继续递增）。
    seq: u64,
    /// 命令归因：代终端用户执行的服务主体（仅桥执行 on-behalf 命令期间设置）。
    ///
    /// 不持久化：它是单次命令的调用上下文，不是聚合状态；桥在提交前清空，
    /// 直接调用引擎命令（测试/内部路径）时恒为 `None`。
    #[serde(skip)]
    acting_service: Option<Principal>,
    /// 当前命令的客户端 action id（`idempotency_key`），由审批桥注入；见
    /// [`Self::set_action_id`]。
    #[serde(skip)]
    action_id: Option<String>,
    /// 单次命令内的临时抑制：驳回回退在同一命令里先把分支判为 `Rejected`，再由回退
    /// 逻辑重开目标节点；抑制期间不得把根分支的否决升级为实例终态。
    #[serde(skip)]
    defer_instance_rejection: bool,
}

impl Engine {
    // -------------------------------------------------------------- 构造

    /// 启动实例：校验注入字段后建 `Pending` 实例（`revision = 0`）。
    ///
    /// 必填字段（`id`/定义四元组/`initiator` 中除 `context` 外）为空即拒绝；`id` 另受
    /// 「不含 `/`（否则在 KV 键里造出额外层级）+ 不超过 128 字节」约束。错误参数里带
    /// 违规字段名，便于调用方定位。
    pub fn start(spec: StartSpec, now: i64) -> Result<Engine, ApprovalError> {
        for (field, value) in [
            ("id", spec.id.as_str()),
            ("definition_ns", spec.definition_ns.as_str()),
            ("definition_name", spec.definition_name.as_str()),
            ("definition_version", spec.definition_version.as_str()),
            ("definition_hash", spec.definition_hash.as_str()),
            ("initiator", spec.initiator.as_str()),
        ] {
            if value.is_empty() {
                return Err(ApprovalError::InvalidArgument(field.into()));
            }
        }
        if spec.id.contains('/') || spec.id.len() > MAX_INSTANCE_ID_LEN {
            return Err(ApprovalError::InvalidArgument("id".into()));
        }
        Ok(Engine {
            instance: Instance {
                id: spec.id,
                definition_ns: spec.definition_ns,
                definition_name: spec.definition_name,
                definition_version: spec.definition_version,
                definition_hash: spec.definition_hash,
                engine_version: APPROVAL_ENGINE_VERSION.into(),
                initiator: spec.initiator,
                initiator_acting_service: spec.initiator_acting_service,
                status: InstanceStatus::Pending,
                cancellation: None,
                context: spec.context,
                root_branch_id: None,
                revision: 0,
                jump_count: 0,
                audit: Vec::new(),
                returns: Vec::new(),
                created_at: now,
                updated_at: now,
            },
            branches: BTreeMap::new(),
            tasks: BTreeMap::new(),
            rounds: BTreeMap::new(),
            node_attempts: BTreeMap::new(),
            seq: 0,
            acting_service: None,
            action_id: None,
            defer_instance_rejection: false,
        })
    }

    /// 设置后续命令的服务归因；由审批桥在 on-behalf 命令期间注入。
    ///
    /// 调用方必须在命令结束（成功或失败）后清空，避免归因泄漏到下一条命令。
    pub(crate) fn set_acting_service(&mut self, service: Option<Principal>) {
        self.acting_service = service;
    }

    /// 设置后续记录的客户端 action id；由审批桥在命令期间注入（命令的
    /// `idempotency_key`），命令结束后必须清空（与 `acting_service` 同生命周期）。
    pub(crate) fn set_action_id(&mut self, action_id: Option<String>) {
        self.action_id = action_id;
    }

    // -------------------------------------------------------------- 流程

    /// 进入节点（分支）。线性原型：同一时刻只允许一个 Open 分支。
    ///
    /// `policy` 由驱动层从 `approve` 节点降级注入，随分支持久化供授权判定读取。
    ///
    /// 参数面由调用契约钉死（节点 / 父分支 / 判定策略 / 时钟逐项注入）。
    #[allow(clippy::too_many_arguments)]
    pub fn enter_branch(
        &mut self,
        expected: u64,
        node: &str,
        parent: Option<Id>,
        join: JoinPolicy,
        reject: RejectPolicy,
        policy: ApprovalPolicy,
        now: i64,
    ) -> Result<(Id, Vec<Effect>), ApprovalError> {
        self.guard(expected)?;
        if self
            .branches
            .values()
            .any(|b| b.status == BranchStatus::Open)
        {
            return Err(ApprovalError::InvalidTransition(
                "another branch is open".into(),
            ));
        }
        let mut out = Vec::new();
        if self.instance.status == InstanceStatus::Pending {
            self.instance.status = InstanceStatus::Running;
            out.push(Effect::InstanceStatus {
                from: InstanceStatus::Pending,
                to: InstanceStatus::Running,
            });
        }
        let id = self.create_branch(node, parent, join, reject, policy, now, &mut out);
        let attempt = self.branches[&id].attempt;
        self.instance.audit.push(AuditEntry {
            at: now,
            action: "enter_branch".into(),
            branch: Some(id.clone()),
            by: "engine".into(),
            detail: format!("node={node} attempt={attempt}"),
            task: None,
            acting_service: self.acting_service.clone(),
            action_id: self.action_id.clone(),
        });
        self.touch(now);
        Ok((id, out))
    }

    /// 开启一轮审批：`seats` 为席位集合（会签=多席、或签=多席、单审=一席、
    /// 阅签=ReadAck 席、候选池=无 assignee 席）。同一分支只允许一轮 Open。
    pub fn open_approval(
        &mut self,
        expected: u64,
        branch_id: &str,
        seats: Vec<SeatSpec>,
        deadline: Option<i64>,
        now: i64,
    ) -> Result<(Id, Vec<Id>, Vec<Effect>), ApprovalError> {
        self.guard(expected)?;
        let branch = self
            .branches
            .get(branch_id)
            .cloned()
            .ok_or_else(|| ApprovalError::NotFound(branch_id.into()))?;
        if branch.status != BranchStatus::Open {
            return Err(ApprovalError::InvalidTransition(format!(
                "branch {} is {:?}",
                branch.id, branch.status
            )));
        }
        if self
            .rounds
            .values()
            .any(|r| r.branch_id == branch.id && r.status == RoundStatus::Open)
        {
            return Err(ApprovalError::InvalidTransition(
                "round already open; use add_sign for more seats".into(),
            ));
        }
        let number = self
            .rounds
            .values()
            .filter(|r| r.branch_id == branch.id)
            .count() as u32
            + 1;
        let rid = self.gen("round");
        self.rounds.insert(
            rid.clone(),
            Round {
                id: rid.clone(),
                branch_id: branch.id.clone(),
                number,
                status: RoundStatus::Open,
                join: None,
                reject: None,
                total_count: 0,
                completed_count: 0,
                rejected_count: 0,
                reject_reason: None,
                opened_at: now,
                closed_at: None,
            },
        );
        let open_branch = self
            .branches
            .get_mut(branch_id)
            .ok_or_else(|| ApprovalError::NotFound(branch_id.into()))?;
        open_branch.round_ids.push(rid.clone());
        let mut out = vec![Effect::RoundOpened {
            id: rid.clone(),
            branch: branch.id.clone(),
            number,
        }];
        let ids = self.add_seats(&branch.id, &rid, seats, deadline, now, &mut out);
        self.recount(&rid, now, &mut out);
        self.touch(now);
        Ok((rid, ids, out))
    }

    /// 加签：向当前 Open 轮次追加席位（运行中动态增加审批人）。
    pub fn add_sign(
        &mut self,
        expected: u64,
        branch_id: &str,
        seats: Vec<SeatSpec>,
        by: &str,
        comment: Option<&str>,
        now: i64,
    ) -> Result<(Vec<Id>, Vec<Effect>), ApprovalError> {
        self.guard(expected)?;
        let branch = self
            .branches
            .get(branch_id)
            .cloned()
            .ok_or_else(|| ApprovalError::NotFound(branch_id.into()))?;
        if branch.status != BranchStatus::Open {
            return Err(ApprovalError::InvalidTransition(format!(
                "branch {} is {:?}",
                branch.id, branch.status
            )));
        }
        let rid = self
            .rounds
            .values()
            .find(|r| r.branch_id == branch.id && r.status == RoundStatus::Open)
            .map(|r| r.id.clone())
            .ok_or_else(|| ApprovalError::InvalidTransition("no open round to add seats".into()))?;
        let mut out = Vec::new();
        let ids = self.add_seats(&branch.id, &rid, seats, None, now, &mut out);
        self.recount(&rid, now, &mut out);
        self.instance.audit.push(AuditEntry {
            at: now,
            action: "add_sign".into(),
            branch: Some(branch.id.clone()),
            by: by.into(),
            detail: format!("seats={} comment={}", ids.len(), comment.unwrap_or("")),
            task: None,
            acting_service: self.acting_service.clone(),
            action_id: self.action_id.clone(),
        });
        self.touch(now);
        Ok((ids, out))
    }

    /// 加签位置：`BEFORE` 生成前加签轮次并挂起当前轮次；`AFTER` 生成待激活的后加签轮次；
    /// `CURRENT` 与 [`Engine::add_sign`] 同义。
    ///
    /// 派生席位记录 `origin` / `predecessor` / `spawned_by`，供 assignment 读面与历史投影
    /// 还原 seedj 的 `HumanTask.kind` / `predecessorTaskId` / `successorTaskId`。
    #[allow(clippy::too_many_arguments)]
    pub fn add_assignee(
        &mut self,
        expected: u64,
        task_id: &str,
        assignees: &[Principal],
        position: AddAssigneePosition,
        spawned_by: Option<&str>,
        by: &str,
        comment: Option<&str>,
        now: i64,
    ) -> Result<(Vec<Id>, Vec<Effect>), ApprovalError> {
        self.guard(expected)?;
        if assignees.is_empty() || assignees.iter().any(|p| p.is_empty()) {
            return Err(ApprovalError::InvalidArgument("assignees".into()));
        }
        let mut unique = assignees.to_vec();
        unique.sort_unstable();
        unique.dedup();
        if unique.len() != assignees.len() {
            return Err(ApprovalError::InvalidArgument("assignees".into()));
        }
        let anchor = self
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| ApprovalError::NotFound(task_id.into()))?;
        let branch = self
            .branches
            .get(&anchor.branch_id)
            .cloned()
            .ok_or_else(|| ApprovalError::NotFound(anchor.branch_id.clone()))?;
        if branch.status != BranchStatus::Open {
            return Err(ApprovalError::InvalidTransition(format!(
                "branch {} is {:?}",
                branch.id, branch.status
            )));
        }
        if !anchor.status.is_live() || anchor.status == TaskStatus::Scheduled {
            return Err(ApprovalError::InvalidTransition(format!(
                "task {} is {:?}",
                anchor.id, anchor.status
            )));
        }

        let mut out = Vec::new();
        let current_round = self
            .rounds
            .values()
            .find(|r| r.branch_id == branch.id && r.status == RoundStatus::Open)
            .map(|r| r.id.clone())
            .ok_or_else(|| ApprovalError::InvalidTransition("no open round to add seats".into()))?;
        let spawned_by = spawned_by.map(str::to_string);

        let ids = match position {
            AddAssigneePosition::Current => {
                if anchor.round_id != current_round {
                    return Err(ApprovalError::InvalidTransition(format!(
                        "task {} is not in the open round",
                        anchor.id
                    )));
                }
                let seats = assignees.iter().map(|p| SeatSpec::approver(p)).collect();
                let ids = self.add_seats(&branch.id, &current_round, seats, None, now, &mut out);
                self.recount(&current_round, now, &mut out);
                ids
            }
            AddAssigneePosition::Before => {
                self.suspend_round(&current_round, now, &mut out);
                let number = self.next_round_number(&branch.id);
                let rid = self.create_round(
                    &branch.id,
                    number,
                    RoundStatus::Open,
                    None,
                    None,
                    now,
                    &mut out,
                );
                let seats = assignees
                    .iter()
                    .map(|p| {
                        SeatSpec::derived(
                            p.clone(),
                            TaskOrigin::BeforeAdded,
                            anchor.id.clone(),
                            spawned_by.clone(),
                        )
                    })
                    .collect();
                let ids = self.add_seats(&branch.id, &rid, seats, None, now, &mut out);
                self.instance.audit.push(AuditEntry {
                    at: now,
                    action: "add_assignee_before".into(),
                    branch: Some(branch.id.clone()),
                    task: Some(anchor.id.clone()),
                    by: by.into(),
                    detail: json!({
                        "assignees": assignees,
                        "spawned_task_ids": ids,
                        "position": "BEFORE",
                        "comment": comment.unwrap_or(""),
                    })
                    .to_string(),
                    acting_service: self.acting_service.clone(),
                    action_id: self.action_id.clone(),
                });
                ids
            }
            AddAssigneePosition::After => {
                let number = self.next_round_number(&branch.id);
                let rid = self.create_round(
                    &branch.id,
                    number,
                    RoundStatus::Scheduled,
                    Some(JoinPolicy::Any),
                    None,
                    now,
                    &mut out,
                );
                let seats = assignees
                    .iter()
                    .map(|p| {
                        SeatSpec::derived(
                            p.clone(),
                            TaskOrigin::AfterAdded,
                            anchor.id.clone(),
                            spawned_by.clone(),
                        )
                    })
                    .collect();
                let ids = self.add_seats(&branch.id, &rid, seats, None, now, &mut out);
                if let Some(first) = ids.first() {
                    if let Some(t) = self.tasks.get_mut(&anchor.id) {
                        t.successor = Some(first.clone());
                        t.updated_at = now;
                    }
                }
                self.instance.audit.push(AuditEntry {
                    at: now,
                    action: "add_assignee_after".into(),
                    branch: Some(branch.id.clone()),
                    task: Some(anchor.id.clone()),
                    by: by.into(),
                    detail: json!({
                        "assignees": assignees,
                        "spawned_task_ids": ids,
                        "position": "AFTER",
                        "comment": comment.unwrap_or(""),
                    })
                    .to_string(),
                    acting_service: self.acting_service.clone(),
                    action_id: self.action_id.clone(),
                });
                ids
            }
        };
        self.touch(now);
        Ok((ids, out))
    }

    /// 减签（按 assignee）：取消当前轮次里指定责任人的未决席位，并返回重算后的轮次。
    ///
    /// 只移除 `PENDING` assignment（`Created` / `Claimed`）；挂起/待激活/已表态席位不动。
    pub fn remove_assignee(
        &mut self,
        expected: u64,
        task_id: &str,
        assignees: &[Principal],
        by: &str,
        now: i64,
    ) -> Result<(Vec<Id>, Round, Vec<Effect>), ApprovalError> {
        self.guard(expected)?;
        if assignees.is_empty() || assignees.iter().any(|p| p.is_empty()) {
            return Err(ApprovalError::InvalidArgument("assignees".into()));
        }
        let anchor = self
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| ApprovalError::NotFound(task_id.into()))?;
        let branch = self
            .branches
            .get(&anchor.branch_id)
            .cloned()
            .ok_or_else(|| ApprovalError::NotFound(anchor.branch_id.clone()))?;
        if branch.status != BranchStatus::Open {
            return Err(ApprovalError::InvalidTransition(format!(
                "branch {} is {:?}",
                branch.id, branch.status
            )));
        }
        let round = self
            .rounds
            .get(&anchor.round_id)
            .cloned()
            .ok_or_else(|| ApprovalError::NotFound(anchor.round_id.clone()))?;
        if round.status != RoundStatus::Open {
            return Err(ApprovalError::InvalidTransition(format!(
                "round {} is {:?}",
                round.id, round.status
            )));
        }
        let mut targets: Vec<Id> = Vec::new();
        let mut pruned: Vec<Id> = Vec::new();
        for t in self
            .tasks
            .values()
            .filter(|t| t.round_id == round.id && t.status.is_open())
        {
            if t.assignee
                .as_ref()
                .is_some_and(|a| assignees.iter().any(|p| p == a))
            {
                targets.push(t.id.clone());
            } else if t.assignee.is_none()
                && t.candidates
                    .iter()
                    .any(|c| assignees.iter().any(|p| p == c))
            {
                // 候选池席位：移除候选人不等于取消整个席位；池清空时才取消。
                let remaining: Vec<Principal> = t
                    .candidates
                    .iter()
                    .filter(|c| !assignees.iter().any(|p| p == *c))
                    .cloned()
                    .collect();
                if remaining.is_empty() {
                    targets.push(t.id.clone());
                } else {
                    pruned.push(t.id.clone());
                }
            }
        }
        if targets.is_empty() && pruned.is_empty() {
            return Err(ApprovalError::InvalidArgument("assignees".into()));
        }
        let mut out = Vec::new();
        for tid in &pruned {
            if let Some(t) = self.tasks.get_mut(tid) {
                t.candidates.retain(|c| !assignees.iter().any(|p| p == c));
                t.updated_at = now;
            }
        }
        self.cancel_tasks(&targets, now, &mut out);
        self.recount(&round.id, now, &mut out);
        self.instance.audit.push(AuditEntry {
            at: now,
            action: "remove_assignee".into(),
            branch: Some(branch.id.clone()),
            task: Some(anchor.id.clone()),
            by: by.into(),
            detail: json!({
                "assignees": assignees,
                "removed_task_ids": targets.iter().chain(pruned.iter()).collect::<Vec<_>>(),
            })
            .to_string(),
            acting_service: self.acting_service.clone(),
            action_id: self.action_id.clone(),
        });
        self.touch(now);
        let round = self
            .rounds
            .get(&round.id)
            .cloned()
            .ok_or_else(|| ApprovalError::NotFound(round.id.clone()))?;
        targets.extend(pruned);
        Ok((targets, round, out))
    }

    /// 减签：取消当前 Open 轮次中的未决席位（已表态的席位跳过；重复目标 id 整批拒绝）；
    /// 整批目标皆终态 ⇒ 零效果 `Ok(vec![])`（不写审计、不加 revision）。
    pub fn remove_sign(
        &mut self,
        expected: u64,
        branch_id: &str,
        task_ids: &[Id],
        by: &str,
        now: i64,
    ) -> Result<Vec<Effect>, ApprovalError> {
        self.guard(expected)?;
        // 入口去重：重复目标 id 是对调用方的输入错误，须在任何变更前整批拒绝（零写入）。
        let mut sorted: Vec<&str> = task_ids.iter().map(String::as_str).collect();
        sorted.sort_unstable();
        if sorted.windows(2).any(|w| w[0] == w[1]) {
            return Err(ApprovalError::InvalidArgument("task_ids".into()));
        }
        let branch = self
            .branches
            .get(branch_id)
            .cloned()
            .ok_or_else(|| ApprovalError::NotFound(branch_id.into()))?;
        if branch.status != BranchStatus::Open {
            return Err(ApprovalError::InvalidTransition(format!(
                "branch {} is {:?}",
                branch.id, branch.status
            )));
        }
        let rid = self
            .rounds
            .values()
            .find(|r| r.branch_id == branch.id && r.status == RoundStatus::Open)
            .map(|r| r.id.clone())
            .ok_or_else(|| {
                ApprovalError::InvalidTransition("no open round to remove seats".into())
            })?;
        let mut out = Vec::new();
        // 先校验全部目标：任一目标不存在或不属于本轮即整批零写入（命令错误不改状态）。
        for tid in task_ids {
            let t = self
                .tasks
                .get(tid)
                .ok_or_else(|| ApprovalError::NotFound(tid.clone()))?;
            if t.round_id != rid {
                return Err(ApprovalError::InvalidArgument(format!(
                    "task {} is not in the open round",
                    t.id
                )));
            }
        }
        // 已终态目标无席位可减：整批皆终态时零效果返回，状态不变。
        if !task_ids.is_empty()
            && !task_ids.iter().any(|tid| {
                self.tasks
                    .get(tid)
                    .map(|t| t.status.is_open())
                    .unwrap_or(false)
            })
        {
            return Ok(Vec::new());
        }
        // 校验全部通过后才开始变更，保证错误路径对先前目标也零副作用；已终态目标跳过，
        // 不发出 `Cancelled→Cancelled` 冗余效果。
        let mut cancelled = 0u32;
        for tid in task_ids {
            let Some(from) = self.tasks.get(tid).map(|t| t.status) else {
                continue;
            };
            if !from.is_open() {
                continue;
            }
            if let Some(t) = self.tasks.get_mut(tid) {
                t.status = TaskStatus::Cancelled;
                t.updated_at = now;
                cancelled += 1;
                out.push(Effect::TaskStatus {
                    id: tid.clone(),
                    from,
                    to: TaskStatus::Cancelled,
                });
            }
        }
        self.recount(&rid, now, &mut out);
        self.instance.audit.push(AuditEntry {
            at: now,
            action: "remove_sign".into(),
            branch: Some(branch.id.clone()),
            by: by.into(),
            detail: format!("seats={cancelled}"),
            task: None,
            acting_service: self.acting_service.clone(),
            action_id: self.action_id.clone(),
        });
        self.touch(now);
        Ok(out)
    }

    /// 认领（候选池席位）。
    pub fn claim(
        &mut self,
        expected: u64,
        task_id: &str,
        principal: &str,
        now: i64,
    ) -> Result<Vec<Effect>, ApprovalError> {
        self.guard(expected)?;
        let mut t = self
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| ApprovalError::NotFound(task_id.into()))?;
        if !t.status.is_open() {
            return Err(ApprovalError::InvalidTransition(format!(
                "task {} is {:?}",
                t.id, t.status
            )));
        }
        if t.assignee.is_some() {
            return Err(ApprovalError::InvalidTransition(format!(
                "task {} already assigned",
                t.id
            )));
        }
        if !t.candidates.iter().any(|c| c == principal) {
            return Err(ApprovalError::NotAuthorized(format!(
                "{principal} is not a candidate of {}",
                t.id
            )));
        }
        t.assignee = Some(principal.into());
        t.status = TaskStatus::Claimed;
        t.updated_at = now;
        let id = t.id.clone();
        self.tasks.insert(id.clone(), t);
        self.touch(now);
        Ok(vec![
            Effect::TaskAssigned {
                id: id.clone(),
                assignee: principal.into(),
                kind: AssignKind::Claim,
            },
            Effect::TaskStatus {
                id,
                from: TaskStatus::Created,
                to: TaskStatus::Claimed,
            },
        ])
    }

    /// 审批通过。
    pub fn approve(
        &mut self,
        expected: u64,
        task_id: &str,
        actor: &str,
        comment: Option<&str>,
        now: i64,
    ) -> Result<Vec<Effect>, ApprovalError> {
        self.vote(expected, task_id, actor, Decision::Approve, comment, now)
    }

    /// 审批否决。
    pub fn reject(
        &mut self,
        expected: u64,
        task_id: &str,
        actor: &str,
        comment: Option<&str>,
        now: i64,
    ) -> Result<Vec<Effect>, ApprovalError> {
        self.vote(expected, task_id, actor, Decision::Reject, comment, now)
    }

    /// 阅签：只读确认，不改变轮次计数。
    pub fn acknowledge(
        &mut self,
        expected: u64,
        task_id: &str,
        actor: &str,
        now: i64,
    ) -> Result<Vec<Effect>, ApprovalError> {
        self.guard(expected)?;
        let mut t = self
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| ApprovalError::NotFound(task_id.into()))?;
        if t.kind != TaskKind::ReadAck {
            return Err(ApprovalError::InvalidTransition(format!(
                "task {} is not a read-ack task",
                t.id
            )));
        }
        if !t.status.is_open() {
            return Err(ApprovalError::InvalidTransition(format!(
                "task {} is {:?}",
                t.id, t.status
            )));
        }
        self.actor_can_act(&t, actor, now)?;
        if t.assignee.is_none() {
            t.assignee = Some(actor.into());
        }
        let from = t.status;
        t.status = TaskStatus::Completed;
        t.updated_at = now;
        Self::revoke_delegation(&mut t, "RULE_SATISFIED", now);
        let id = t.id.clone();
        self.tasks.insert(id.clone(), t);
        self.touch(now);
        Ok(vec![Effect::TaskStatus {
            id,
            from,
            to: TaskStatus::Completed,
        }])
    }

    /// 转办：责任人变更，原审批人保留审计痕迹但不再审批（`transferred_from`）。
    ///
    /// 允许 `by` 是当前责任人（`assignee`）或未认领池席位的候选人；转办同时清空已有委派。
    pub fn transfer(
        &mut self,
        expected: u64,
        task_id: &str,
        to: &str,
        by: &str,
        now: i64,
    ) -> Result<Vec<Effect>, ApprovalError> {
        self.guard(expected)?;
        let mut t = self
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| ApprovalError::NotFound(task_id.into()))?;
        if !t.status.is_open() {
            return Err(ApprovalError::InvalidTransition(format!(
                "task {} is {:?}",
                t.id, t.status
            )));
        }
        if t.kind != TaskKind::Approval {
            return Err(ApprovalError::InvalidTransition(
                "only approval tasks can be transferred".into(),
            ));
        }
        let from_principal = match &t.assignee {
            Some(a) if a == by => a.clone(),
            Some(a) => {
                return Err(ApprovalError::NotAuthorized(format!(
                    "{by} is not the assignee ({a}) of {}",
                    t.id
                )))
            }
            None => {
                if !t.candidates.iter().any(|c| c == by) {
                    return Err(ApprovalError::NotAuthorized(format!(
                        "{by} is not a candidate of {}",
                        t.id
                    )));
                }
                by.into()
            }
        };
        t.transferred_from.push(from_principal);
        t.assignee = Some(to.into());
        // 转办是责任人的整体让位：原委派凭据随之失效（不保留代理 assignment）。
        t.delegation = None;
        let mut out = Vec::new();
        if t.status == TaskStatus::Created {
            t.status = TaskStatus::Claimed;
            out.push(Effect::TaskStatus {
                id: t.id.clone(),
                from: TaskStatus::Created,
                to: TaskStatus::Claimed,
            });
        }
        t.updated_at = now;
        let id = t.id.clone();
        self.tasks.insert(id.clone(), t);
        out.push(Effect::TaskAssigned {
            id,
            assignee: to.into(),
            kind: AssignKind::Transfer,
        });
        self.touch(now);
        Ok(out)
    }

    /// 委派：原审批人保留权限，被委派人可代批（投票记录带 `on_behalf_of`）。
    ///
    /// 只有当前责任人可委派；委派会覆盖同一席位已有的委派。
    pub fn delegate(
        &mut self,
        expected: u64,
        task_id: &str,
        to: &str,
        expires_at: Option<i64>,
        by: &str,
        now: i64,
    ) -> Result<Vec<Effect>, ApprovalError> {
        self.guard(expected)?;
        if to.is_empty() {
            return Err(ApprovalError::InvalidArgument("to".into()));
        }
        if expires_at.is_some_and(|at| at <= now) {
            return Err(ApprovalError::InvalidArgument("delegate_for".into()));
        }
        let mut t = self
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| ApprovalError::NotFound(task_id.into()))?;
        if !t.status.is_open() {
            return Err(ApprovalError::InvalidTransition(format!(
                "task {} is {:?}",
                t.id, t.status
            )));
        }
        match &t.assignee {
            Some(a) if a == by => {}
            _ => {
                return Err(ApprovalError::NotAuthorized(format!(
                    "only the assignee can delegate task {}",
                    t.id
                )))
            }
        }
        t.delegation = Some(Delegation {
            from: by.into(),
            to: to.into(),
            granted_at: now,
            expires_at,
            revoked_at: None,
            revoke_reason: None,
        });
        t.updated_at = now;
        let id = t.id.clone();
        self.tasks.insert(id.clone(), t);
        self.touch(now);
        Ok(vec![Effect::TaskAssigned {
            id,
            assignee: to.into(),
            kind: AssignKind::Delegate,
        }])
    }

    /// 催办：只写审计 / 通知，不改状态机；终态席位拒绝。
    ///
    /// 授权：发起人、分支 `sign_admins`、席位责任人/候选/被委派人。
    pub fn urge(
        &mut self,
        expected: u64,
        task_id: &str,
        by: &str,
        message: &str,
        now: i64,
    ) -> Result<Vec<Effect>, ApprovalError> {
        self.guard(expected)?;
        let task = self
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| ApprovalError::NotFound(task_id.into()))?;
        // 催办允许挂起中的前加签原席位（seedj 口径）；终态席位拒绝。
        if !task.status.is_live() {
            return Err(ApprovalError::InvalidTransition(format!(
                "task {} is {:?}",
                task.id, task.status
            )));
        }
        let branch = self
            .branches
            .get(&task.branch_id)
            .ok_or_else(|| ApprovalError::NotFound(task.branch_id.clone()))?;
        self.ensure_participant(branch, Some(&task), by, now)?;
        self.instance.audit.push(AuditEntry {
            at: now,
            action: "urge".into(),
            branch: Some(task.branch_id.clone()),
            task: Some(task.id.clone()),
            by: by.into(),
            detail: json!({
                "message": message,
                "targetStatus": match task.status {
                    TaskStatus::Suspended => "SUSPENDED",
                    TaskStatus::Scheduled => "SCHEDULED",
                    _ => "PENDING",
                },
            })
            .to_string(),
            acting_service: self.acting_service.clone(),
            action_id: self.action_id.clone(),
        });
        self.touch(now);
        Ok(Vec::new())
    }

    /// 抄送：只写审计 / 通知，不改变审批计数。
    ///
    /// 任务级抄送要求该节点定义 `allowCc: true`；实例级抄送仅限发起人或 `sign_admins`。
    /// 终态席位拒绝（`Rejected/Cancelled/Expired/Completed`）。
    pub fn cc(
        &mut self,
        expected: u64,
        task_id: Option<&str>,
        recipients: &[Principal],
        message: Option<&str>,
        by: &str,
        now: i64,
    ) -> Result<Vec<Effect>, ApprovalError> {
        self.guard(expected)?;
        if recipients.is_empty() || recipients.iter().any(|r| r.is_empty()) {
            return Err(ApprovalError::InvalidArgument("recipients".into()));
        }
        let (branch_id, task) = match task_id {
            Some(tid) => {
                let task = self
                    .tasks
                    .get(tid)
                    .cloned()
                    .ok_or_else(|| ApprovalError::NotFound(tid.into()))?;
                if !task.status.is_live() {
                    return Err(ApprovalError::InvalidTransition(format!(
                        "task {} is {:?}",
                        task.id, task.status
                    )));
                }
                let branch = self
                    .branches
                    .get(&task.branch_id)
                    .cloned()
                    .ok_or_else(|| ApprovalError::NotFound(task.branch_id.clone()))?;
                if !branch.policy.allow_cc {
                    return Err(ApprovalError::NotAuthorized(format!(
                        "node {} does not allow cc",
                        branch.node
                    )));
                }
                self.ensure_participant(&branch, Some(&task), by, now)?;
                (Some(branch.id), Some(task.id))
            }
            None => {
                let allowed = self.instance.initiator == by
                    || self
                        .branches
                        .values()
                        .any(|b| b.policy.sign_admins.iter().any(|a| a == by));
                if !allowed {
                    return Err(ApprovalError::NotAuthorized(format!(
                        "{by} cannot cc this instance"
                    )));
                }
                (None, None)
            }
        };
        let detail = json!({
            "recipients": recipients,
            "message": message,
        })
        .to_string();
        self.instance.audit.push(AuditEntry {
            at: now,
            action: "cc".into(),
            branch: branch_id,
            task,
            by: by.into(),
            detail,
            acting_service: self.acting_service.clone(),
            action_id: self.action_id.clone(),
        });
        self.touch(now);
        Ok(Vec::new())
    }

    /// 任务级撤回：保留已产生的通过记录，关闭当前分支并回退到 `target_node`。
    ///
    /// 仅该席位的原审批人（或其被委派人）可发起；**带否决票的席位不可撤回**（否决记录
    /// 不可撤销）。当前必须存在 Open 分支（下游未完成）。
    pub fn withdraw_task(
        &mut self,
        expected: u64,
        task_id: &str,
        by: &str,
        target_node: &str,
        comment: Option<&str>,
        now: i64,
    ) -> Result<(Id, Vec<Effect>), ApprovalError> {
        self.guard(expected)?;
        let task = self
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| ApprovalError::NotFound(task_id.into()))?;
        if task.kind != TaskKind::Approval {
            return Err(ApprovalError::InvalidTransition(format!(
                "task {} is not an approval task",
                task.id
            )));
        }
        let approved = task.votes.iter().any(|v| {
            v.decision == Decision::Approve
                && (v.actor == by || v.on_behalf_of.as_deref() == Some(by))
        });
        if !approved {
            return Err(ApprovalError::NotAuthorized(format!(
                "{by} has no approve record on {}",
                task.id
            )));
        }
        if task.votes.iter().any(|v| v.decision == Decision::Reject) {
            return Err(ApprovalError::InvalidTransition(format!(
                "task {} has a reject record and cannot be withdrawn",
                task.id
            )));
        }
        let current = self
            .branches
            .values()
            .find(|b| b.status == BranchStatus::Open)
            .cloned()
            .ok_or_else(|| {
                ApprovalError::InvalidTransition("no open branch to roll back from".into())
            })?;
        if !self.node_attempts.contains_key(target_node) {
            return Err(ApprovalError::InvalidArgument(format!(
                "target node {target_node} is not in the reached-node whitelist"
            )));
        }
        let mut out = Vec::new();
        let new_branch = self.rollback_to_node(
            &current.id,
            target_node,
            by,
            comment,
            "task_withdrawn",
            Some(&task.id),
            now,
            &mut out,
        )?;
        self.instance.audit.push(AuditEntry {
            at: now,
            action: "withdraw_task".into(),
            branch: Some(current.id),
            task: Some(task.id),
            by: by.into(),
            detail: comment.unwrap_or("").to_string(),
            acting_service: self.acting_service.clone(),
            action_id: self.action_id.clone(),
        });
        Ok((new_branch, out))
    }

    /// 催办/抄送的参与者判定：发起人、分支 `sign_admins`、席位责任人/候选/被委派人。
    fn ensure_participant(
        &self,
        branch: &Branch,
        task: Option<&Task>,
        by: &str,
        now: i64,
    ) -> Result<(), ApprovalError> {
        if self.instance.initiator == by || branch.policy.sign_admins.iter().any(|a| a == by) {
            return Ok(());
        }
        if task.is_some_and(|t| {
            t.assignee.as_deref() == Some(by)
                || t.candidates.iter().any(|c| c == by)
                || t.delegation
                    .as_ref()
                    .is_some_and(|d| d.to == by && d.is_active(now))
        }) {
            return Ok(());
        }
        Err(ApprovalError::NotAuthorized(format!(
            "{by} is not a participant of {}",
            branch.id
        )))
    }

    /// 驳回至节点：关闭当前分支（`Returned`），作废目标节点旧尝试，重开新尝试。
    ///
    /// 目标必须在已进入节点白名单（`node_attempts`）内；累计次数受 [`MAX_JUMPS`] 限制。
    /// 驳回记录写入 `instance.returns`，并递增 `jump_count`；新分支沿用目标旧尝试的
    /// join/reject 与父分支。
    pub fn return_to_node(
        &mut self,
        expected: u64,
        branch_id: &str,
        target_node: &str,
        by: &str,
        comment: Option<&str>,
        now: i64,
    ) -> Result<(Id, Vec<Effect>), ApprovalError> {
        self.guard(expected)?;
        let branch = self
            .branches
            .get(branch_id)
            .cloned()
            .ok_or_else(|| ApprovalError::NotFound(branch_id.into()))?;
        if branch.status != BranchStatus::Open {
            return Err(ApprovalError::InvalidTransition(format!(
                "branch {} is {:?}",
                branch.id, branch.status
            )));
        }
        if !self.node_attempts.contains_key(target_node) {
            return Err(ApprovalError::InvalidArgument(format!(
                "target node {target_node} is not in the reached-node whitelist"
            )));
        }
        if self.instance.jump_count >= MAX_JUMPS {
            return Err(ApprovalError::InvalidArgument(format!(
                "jump limit {MAX_JUMPS} reached"
            )));
        }
        let mut out = Vec::new();
        let new_id = self.rollback_to_node(
            &branch.id,
            target_node,
            by,
            comment,
            "return_to_node",
            None,
            now,
            &mut out,
        )?;
        Ok((new_id, out))
    }

    /// 关闭当前分支并重开目标节点（驳回至节点 / 驳回即回退共用）。
    ///
    /// 已是 `Rejected` 的分支保留判定状态（可在同一命令里先投票再回退）；仅 `Open`
    /// 分支改写为 `Returned`。目标节点旧尝试作废，新分支沿用其 join/reject/policy。
    #[allow(clippy::too_many_arguments)]
    fn rollback_to_node(
        &mut self,
        branch_id: &str,
        target_node: &str,
        by: &str,
        comment: Option<&str>,
        reason: &str,
        reject_task: Option<&str>,
        now: i64,
        out: &mut Vec<Effect>,
    ) -> Result<Id, ApprovalError> {
        let branch = self
            .branches
            .get(branch_id)
            .cloned()
            .ok_or_else(|| ApprovalError::NotFound(branch_id.into()))?;
        if !self.node_attempts.contains_key(target_node) {
            return Err(ApprovalError::InvalidArgument(format!(
                "target node {target_node} is not in the reached-node whitelist"
            )));
        }
        if self.instance.jump_count >= MAX_JUMPS {
            return Err(ApprovalError::InvalidArgument(format!(
                "jump limit {MAX_JUMPS} reached"
            )));
        }
        // 上游回退：丢弃全部待激活的后加签派生任务，并取消挂起/未决席位。
        self.discard_scheduled_after(&branch.id, "UPSTREAM_ROLLBACK", now, out);
        self.cancel_live_tasks_of_branch(&branch.id, now, out);
        self.close_rounds_of_branch(&branch.id, RoundStatus::Superseded, now, out);
        if branch.status == BranchStatus::Open {
            self.set_branch_status(&branch.id, BranchStatus::Returned, now, out);
        }

        let target_latest = self
            .branches
            .values()
            .filter(|b| b.node == target_node)
            .max_by_key(|b| b.attempt)
            .cloned();
        let (join, reject, policy) = target_latest
            .as_ref()
            .map(|b| (b.join, b.reject, b.policy.clone()))
            .unwrap_or((
                JoinPolicy::All,
                RejectPolicy::AnyReject,
                ApprovalPolicy::default(),
            ));
        if let Some(tb) = &target_latest {
            self.discard_scheduled_after(&tb.id, "UPSTREAM_ROLLBACK", now, out);
            self.cancel_live_tasks_of_branch(&tb.id, now, out);
            self.close_rounds_of_branch(&tb.id, RoundStatus::Superseded, now, out);
            self.set_branch_status(&tb.id, BranchStatus::Superseded, now, out);
        }
        let new_id = self.create_branch(
            target_node,
            target_latest
                .as_ref()
                .and_then(|b| b.parent_branch_id.clone()),
            join,
            reject,
            policy,
            now,
            out,
        );
        self.instance.jump_count += 1;
        self.instance.returns.push(ReturnRecord {
            from_node: branch.node.clone(),
            to_node: target_node.into(),
            by: by.into(),
            comment: comment.map(str::to_string),
            at: now,
        });
        self.instance.audit.push(AuditEntry {
            at: now,
            action: "return_to_node".into(),
            branch: Some(branch.id.clone()),
            by: by.into(),
            detail: format!("{} -> {} ({reason})", branch.node, target_node),
            task: None,
            acting_service: self.acting_service.clone(),
            action_id: self.action_id.clone(),
        });
        let jump_count = self.instance.jump_count;
        let mut data = json!({
            "instance_id": self.instance.id,
            "from_node": branch.node,
            "to_node": target_node,
            "jump_count": jump_count,
            "reason": reason,
        });
        if let Some(task_id) = reject_task {
            data["reject_task_ids"] = json!([task_id]);
        }
        self.emit(out, "com.china-workflow.instance.returned.v1", data);
        self.touch(now);
        Ok(new_id)
    }

    /// 驳回至节点：先记录否决票，再在**同一条命令**内关闭当前轮次/分支并重开
    /// `target_node`。
    ///
    /// 返回 `Some(new_branch)` 表示已回退（调用方须在同批提交里开席），`None` 表示
    /// 当前策略仍允许继续投票（`RecordAndContinue` 且通过条件仍可满足）。
    pub fn reject_to_node(
        &mut self,
        expected: u64,
        task_id: &str,
        actor: &str,
        comment: Option<&str>,
        target_node: &str,
        now: i64,
    ) -> Result<(Option<Id>, Vec<Effect>), ApprovalError> {
        self.guard(expected)?;
        let task = self
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| ApprovalError::NotFound(task_id.into()))?;
        // 目标与跳转上限在投票前校验：错误路径必须零副作用（不记录否决票）。
        if !self.node_attempts.contains_key(target_node) {
            return Err(ApprovalError::InvalidArgument(format!(
                "target node {target_node} is not in the reached-node whitelist"
            )));
        }
        if self.instance.jump_count >= MAX_JUMPS {
            return Err(ApprovalError::InvalidArgument(format!(
                "jump limit {MAX_JUMPS} reached"
            )));
        }
        let branch = self
            .branches
            .get(&task.branch_id)
            .cloned()
            .ok_or_else(|| ApprovalError::NotFound(task.branch_id.clone()))?;
        if branch.status != BranchStatus::Open {
            return Err(ApprovalError::InvalidTransition(format!(
                "branch {} is {:?}",
                branch.id, branch.status
            )));
        }
        // 否决先把分支判为 `Rejected`；若就地回退，「根分支否决 = 实例终态」必须延后，
        // 否则回退后实例会停在错误的终态。
        self.defer_instance_rejection = true;
        let voted = self.vote(expected, task_id, actor, Decision::Reject, comment, now);
        self.defer_instance_rejection = false;
        let mut effects = voted?;
        let branch = self
            .branches
            .get(&task.branch_id)
            .cloned()
            .ok_or_else(|| ApprovalError::NotFound(task.branch_id.clone()))?;
        if branch.status != BranchStatus::Rejected {
            // `RecordAndContinue`：否决已记录，但剩余席位仍可能满足通过条件。
            return Ok((None, effects));
        }
        let reason = self
            .rounds
            .values()
            .filter(|r| r.branch_id == branch.id)
            .max_by_key(|r| r.number)
            .and_then(|r| r.reject_reason.clone())
            .unwrap_or_else(|| "rejected".into());
        let new_branch = self.rollback_to_node(
            &branch.id,
            target_node,
            actor,
            comment,
            &reason,
            Some(task_id),
            now,
            &mut effects,
        )?;
        Ok((Some(new_branch), effects))
    }

    /// 撤回（仅发起人）：级联取消全部 open 分支 / 轮次 / 未决席位，实例置 `Cancelled`。
    pub fn withdraw(
        &mut self,
        expected: u64,
        by: &str,
        now: i64,
    ) -> Result<Vec<Effect>, ApprovalError> {
        self.guard(expected)?;
        if self.instance.initiator != by {
            return Err(ApprovalError::NotAuthorized(format!(
                "{by} is not the initiator"
            )));
        }
        let mut out = Vec::new();
        self.cancel_open_branches(BranchStatus::Cancelled, &mut out, now);
        let from = self.instance.status;
        self.instance.status = InstanceStatus::Cancelled;
        self.instance.cancellation = Some(Cancellation {
            kind: "withdraw".into(),
            reason: None,
            compensate: false,
            at: now,
        });
        out.push(Effect::InstanceStatus {
            from,
            to: InstanceStatus::Cancelled,
        });
        self.instance.audit.push(AuditEntry {
            at: now,
            action: "withdraw".into(),
            branch: None,
            by: by.into(),
            detail: String::new(),
            task: None,
            acting_service: self.acting_service.clone(),
            action_id: self.action_id.clone(),
        });
        self.emit(
            &mut out,
            "com.china-workflow.instance.cancelled.v1",
            json!({"instance_id": self.instance.id}),
        );
        self.touch(now);
        Ok(out)
    }

    /// 控制面取消（`Cancel`）：级联取消未决席位与轮次，实例置 `Cancelled`（**不是**
    /// `Terminated`），并记录补偿声明。
    ///
    /// `compensate = true` 只记入 `Cancellation` 供读面投影；coord 没有业务补偿动作模型，
    /// Saga 逆序由调用方执行（协调侧不猜测逆操作）。
    pub fn cancel(
        &mut self,
        expected: u64,
        by: &str,
        reason: Option<&str>,
        compensate: bool,
        now: i64,
    ) -> Result<Vec<Effect>, ApprovalError> {
        self.guard(expected)?;
        if self.instance.status.is_terminal() {
            return Err(ApprovalError::InvalidTransition(format!(
                "instance {} is {:?}",
                self.instance.id, self.instance.status
            )));
        }
        let mut out = Vec::new();
        self.cancel_open_branches(BranchStatus::Cancelled, &mut out, now);
        let from = self.instance.status;
        self.instance.status = InstanceStatus::Cancelled;
        self.instance.cancellation = Some(Cancellation {
            kind: "cancel".into(),
            reason: reason.map(str::to_string),
            compensate,
            at: now,
        });
        out.push(Effect::InstanceStatus {
            from,
            to: InstanceStatus::Cancelled,
        });
        self.instance.audit.push(AuditEntry {
            at: now,
            action: "cancel".into(),
            branch: None,
            by: by.into(),
            detail: reason.unwrap_or_default().to_string(),
            task: None,
            acting_service: self.acting_service.clone(),
            action_id: self.action_id.clone(),
        });
        self.emit(
            &mut out,
            "com.china-workflow.instance.cancelled.v1",
            json!({
                "instance_id": self.instance.id,
                "reason": reason.unwrap_or_default(),
                "compensate": compensate,
            }),
        );
        self.touch(now);
        Ok(out)
    }

    /// 强制终止（管理路径）：与 `withdraw` 相同的级联取消，实例置 `Terminated`。
    ///
    /// 本模型层不做权限判定：只记录 `by` 与审计；授权的契约面归 M2-C（见 ADR-0013 D7）。
    /// 缺口口径见 `spike/2026-10/spike-1-state-model/docs/GAPS.md`（Q6）。
    pub fn terminate(
        &mut self,
        expected: u64,
        by: &str,
        reason: &str,
        now: i64,
    ) -> Result<Vec<Effect>, ApprovalError> {
        self.guard(expected)?;
        let mut out = Vec::new();
        self.cancel_open_branches(BranchStatus::Terminated, &mut out, now);
        let from = self.instance.status;
        self.instance.status = InstanceStatus::Terminated;
        out.push(Effect::InstanceStatus {
            from,
            to: InstanceStatus::Terminated,
        });
        self.instance.audit.push(AuditEntry {
            at: now,
            action: "terminate".into(),
            branch: None,
            by: by.into(),
            detail: reason.into(),
            task: None,
            acting_service: self.acting_service.clone(),
            action_id: self.action_id.clone(),
        });
        self.emit(
            &mut out,
            "com.china-workflow.instance.terminated.v1",
            json!({"instance_id": self.instance.id, "reason": reason}),
        );
        self.touch(now);
        Ok(out)
    }

    /// 定义层已无后继节点时，由驱动方显式收口实例（分支通过与实例收口解耦：
    /// 状态机内部无法得知定义是否还有后继节点）。
    pub fn finish(&mut self, expected: u64, now: i64) -> Result<Vec<Effect>, ApprovalError> {
        self.guard(expected)?;
        if self
            .branches
            .values()
            .any(|b| b.status == BranchStatus::Open)
        {
            return Err(ApprovalError::InvalidTransition(
                "open branch remains".into(),
            ));
        }
        let last = self
            .branches
            .values()
            .filter(|b| b.parent_branch_id.is_none())
            .max_by_key(|b| b.created_at)
            .map(|b| b.status);
        let next = match last {
            Some(BranchStatus::Completed) => InstanceStatus::Completed,
            Some(BranchStatus::Rejected) => InstanceStatus::Rejected,
            _ => {
                return Err(ApprovalError::InvalidTransition(
                    "no completed branch to finish".into(),
                ))
            }
        };
        if self.instance.status == next {
            return Ok(Vec::new());
        }
        let before = self.instance.status;
        self.instance.status = next;
        let mut out = vec![Effect::InstanceStatus {
            from: before,
            to: next,
        }];
        let name = match next {
            InstanceStatus::Completed => "com.china-workflow.instance.completed.v1",
            InstanceStatus::Rejected => "com.china-workflow.instance.rejected.v1",
            _ => "com.china-workflow.instance.status_changed.v1",
        };
        let id = self.instance.id.clone();
        self.emit(&mut out, name, json!({ "instance_id": id }));
        self.touch(now);
        Ok(out)
    }

    /// 超时：由外部调度器驱动（无进程内定时器）。过期审批席位不计票；超时的最终处置
    /// 策略（自动通过/驳回/升级）未定，本命令只做「过期」这一状态变化。
    pub fn expire(
        &mut self,
        expected: u64,
        task_id: &str,
        now: i64,
    ) -> Result<Vec<Effect>, ApprovalError> {
        self.guard(expected)?;
        let t = self
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| ApprovalError::NotFound(task_id.into()))?;
        if !t.status.is_open() {
            return Err(ApprovalError::InvalidTransition(format!(
                "task {} is {:?}",
                t.id, t.status
            )));
        }
        let d = t.deadline.ok_or_else(|| {
            ApprovalError::InvalidArgument(format!("task {} has no deadline", t.id))
        })?;
        if now < d {
            return Err(ApprovalError::InvalidArgument(format!(
                "task {} deadline {d} not reached at {now}",
                t.id
            )));
        }
        let from = t.status;
        {
            let expired = self
                .tasks
                .get_mut(task_id)
                .ok_or_else(|| ApprovalError::NotFound(task_id.into()))?;
            expired.status = TaskStatus::Expired;
            expired.updated_at = now;
            Self::revoke_delegation(expired, "TASK_EXPIRED", now);
        }
        let mut out = vec![Effect::TaskStatus {
            id: task_id.to_string(),
            from,
            to: TaskStatus::Expired,
        }];
        if t.kind == TaskKind::Approval {
            self.recount(&t.round_id, now, &mut out);
        }
        self.touch(now);
        Ok(out)
    }

    /// 过期仍生效的委派（到期自动恢复原责任人）：只撤销代理权并写审计，不改席位状态。
    ///
    /// 由审批桥的截止路径驱动；同一批到期席位的 `DELEGATION_EXPIRED` 在 `History` 投影。
    pub fn expire_delegations(
        &mut self,
        expected: u64,
        now: i64,
    ) -> Result<Vec<Effect>, ApprovalError> {
        self.guard(expected)?;
        let expired: Vec<(Id, Id)> = self
            .tasks
            .values()
            .filter(|t| {
                t.delegation.as_ref().is_some_and(|d| {
                    d.revoked_at.is_none() && d.expires_at.is_some_and(|at| at <= now)
                })
            })
            .map(|t| (t.id.clone(), t.branch_id.clone()))
            .collect();
        if expired.is_empty() {
            return Ok(Vec::new());
        }
        for (tid, bid) in &expired {
            if let Some(t) = self.tasks.get_mut(tid) {
                if let Some(d) = t.delegation.as_mut() {
                    d.revoked_at = Some(now);
                    d.revoke_reason = Some("DELEGATION_EXPIRED".into());
                }
                t.updated_at = now;
            }
            self.instance.audit.push(AuditEntry {
                at: now,
                action: "delegation_expired".into(),
                branch: Some(bid.clone()),
                task: Some(tid.clone()),
                by: "engine".into(),
                detail: json!({"reason": "DELEGATION_EXPIRED"}).to_string(),
                acting_service: None,
                action_id: None,
            });
        }
        self.touch(now);
        Ok(Vec::new())
    }

    /// 超时策略 `fail`：取消全部活跃席位并把实例收口为 `Terminated`（超时失败）。
    ///
    /// 审批桥把该结果映射为工作流 `FAULTED` + `TASK_OVERDUE`；不在命令路径产生
    /// `Faulted`（B-WF-10 仅约束命令路径）。
    pub fn timeout_fail(
        &mut self,
        expected: u64,
        reason: &str,
        now: i64,
    ) -> Result<Vec<Effect>, ApprovalError> {
        self.guard(expected)?;
        let mut out = Vec::new();
        self.cancel_open_branches(BranchStatus::Terminated, &mut out, now);
        let from = self.instance.status;
        self.instance.status = InstanceStatus::Terminated;
        out.push(Effect::InstanceStatus {
            from,
            to: InstanceStatus::Terminated,
        });
        self.instance.audit.push(AuditEntry {
            at: now,
            action: "timeout_fail".into(),
            branch: None,
            task: None,
            by: "engine".into(),
            detail: reason.into(),
            acting_service: None,
            action_id: None,
        });
        self.emit(
            &mut out,
            "com.china-workflow.instance.terminated.v1",
            json!({"instance_id": self.instance.id, "reason": reason}),
        );
        self.touch(now);
        Ok(out)
    }

    /// 超时策略 `waiting`：取消当前轮次的未决席位，实例停在 `Waiting`，等待 Resume 重建。
    ///
    /// 不丢弃后加签待激活轮次（恢复后仍应在原轮次完成后执行）；挂起轮次也保留，恢复的
    /// 派生轮次通过后仍需恢复它们。
    pub fn timeout_wait(
        &mut self,
        expected: u64,
        reason: &str,
        now: i64,
    ) -> Result<Vec<Effect>, ApprovalError> {
        self.guard(expected)?;
        let mut out = Vec::new();
        let round_ids: Vec<Id> = self
            .rounds
            .values()
            .filter(|r| r.status == RoundStatus::Open)
            .map(|r| r.id.clone())
            .collect();
        for rid in round_ids {
            self.cancel_pending_tasks(&rid, now, &mut out);
            if let Some(r) = self.rounds.get_mut(&rid) {
                let from = r.status;
                r.status = RoundStatus::Cancelled;
                r.closed_at = Some(now);
                out.push(Effect::RoundStatus {
                    id: rid,
                    from,
                    to: RoundStatus::Cancelled,
                });
            }
        }
        let from = self.instance.status;
        self.instance.status = InstanceStatus::Waiting;
        out.push(Effect::InstanceStatus {
            from,
            to: InstanceStatus::Waiting,
        });
        self.instance.audit.push(AuditEntry {
            at: now,
            action: "timeout_wait".into(),
            branch: None,
            task: None,
            by: "engine".into(),
            detail: reason.into(),
            acting_service: None,
            action_id: None,
        });
        self.touch(now);
        Ok(out)
    }

    /// 等待态恢复：按上一轮席位重建任务并给新的相对截止，实例回到 `Running`。
    pub fn resume_waiting(
        &mut self,
        expected: u64,
        new_due_ms: i64,
        by: &str,
        reason: &str,
        now: i64,
    ) -> Result<(Vec<Id>, Vec<Effect>), ApprovalError> {
        self.guard_waiting(expected)?;
        if new_due_ms <= 0 {
            return Err(ApprovalError::InvalidArgument("new_due_in_ms".into()));
        }
        let branch = self
            .branches
            .values()
            .find(|b| b.status == BranchStatus::Open)
            .cloned()
            .ok_or_else(|| ApprovalError::InvalidTransition("no open branch to resume".into()))?;
        // 取最近一 **被取消** 的轮次（超时前的活跃轮次），不能取排队的后加签轮次：
        // 后加签要等恢复后的原轮次通过才激活。
        let last = self
            .rounds
            .values()
            .filter(|r| r.branch_id == branch.id && r.status == RoundStatus::Cancelled)
            .max_by_key(|r| r.number)
            .or_else(|| {
                self.rounds
                    .values()
                    .filter(|r| r.branch_id == branch.id && r.status != RoundStatus::Scheduled)
                    .max_by_key(|r| r.number)
            })
            .cloned()
            .ok_or_else(|| ApprovalError::InvalidTransition("no round to resume".into()))?;
        let seats: Vec<SeatSpec> = self
            .tasks
            .values()
            .filter(|t| t.round_id == last.id)
            .map(|t| SeatSpec {
                assignee: t.assignee.clone(),
                candidates: t.candidates.clone(),
                kind: t.kind,
                origin: t.origin,
                predecessor: t.predecessor.clone(),
                spawned_by: t.spawned_by.clone(),
            })
            .collect();
        if seats.is_empty() {
            return Err(ApprovalError::InvalidTransition(
                "previous round has no seats to resume".into(),
            ));
        }
        let number = self.next_round_number(&branch.id);
        let mut out = Vec::new();
        let rid = self.create_round(
            &branch.id,
            number,
            RoundStatus::Open,
            last.join,
            last.reject,
            now,
            &mut out,
        );
        let ids = self.add_seats(
            &branch.id,
            &rid,
            seats,
            Some(now + new_due_ms),
            now,
            &mut out,
        );
        let from = self.instance.status;
        self.instance.status = InstanceStatus::Running;
        out.push(Effect::InstanceStatus {
            from,
            to: InstanceStatus::Running,
        });
        self.instance.audit.push(AuditEntry {
            at: now,
            action: "resume_workflow".into(),
            branch: Some(branch.id),
            task: None,
            by: by.into(),
            detail: json!({
                "new_due_in_ms": new_due_ms,
                "reason": reason,
                "round_id": rid,
            })
            .to_string(),
            acting_service: self.acting_service.clone(),
            action_id: self.action_id.clone(),
        });
        self.touch(now);
        Ok((ids, out))
    }

    // -------------------------------------------------------------- 查询

    /// 三级作用域求值：task > branch > instance。
    pub fn resolve_var(&self, task_id: &str, key: &str) -> Option<&Value> {
        let t = self.tasks.get(task_id)?;
        if let Some(v) = t.scope.get(key) {
            return Some(v);
        }
        if let Some(b) = self.branches.get(&t.branch_id) {
            if let Some(v) = b.scope.get(key) {
                return Some(v);
            }
        }
        self.instance.context.get(key)
    }

    /// 待办查询：某人当前可处理的席位（待办列表的模型侧投影）。
    pub fn open_tasks_for(&self, principal: &str) -> Vec<&Task> {
        self.tasks
            .values()
            .filter(|t| {
                t.status.is_open()
                    && (t.assignee.as_deref() == Some(principal)
                        || (t.assignee.is_none() && t.candidates.iter().any(|c| c == principal))
                        || t.delegation.as_ref().is_some_and(|d| d.to == principal))
            })
            .collect()
    }

    /// 待办查询（带时钟）：已到期/已撤销的委派不再把席位暴露给代理人。
    pub fn open_tasks_for_at(&self, principal: &str, now: i64) -> Vec<&Task> {
        self.tasks
            .values()
            .filter(|t| {
                t.status.is_open()
                    && (t.assignee.as_deref() == Some(principal)
                        || (t.assignee.is_none() && t.candidates.iter().any(|c| c == principal))
                        || t.delegation
                            .as_ref()
                            .is_some_and(|d| d.to == principal && d.is_active(now)))
            })
            .collect()
    }

    /// 证据快照：实例与子实体的 JSON 投影（判定依据的原始数据）。
    ///
    /// 任一子实体序列化失败都返回错误而不是 panic：快照要么完整、要么明确失败。
    pub fn snapshot_json(&self) -> Result<Value, ApprovalError> {
        Ok(json!({
            "instance": serde_json::to_value(&self.instance)
                .map_err(|e| ApprovalError::SerializationError(e.to_string()))?,
            "branches": serde_json::to_value(&self.branches)
                .map_err(|e| ApprovalError::SerializationError(e.to_string()))?,
            "tasks": serde_json::to_value(&self.tasks)
                .map_err(|e| ApprovalError::SerializationError(e.to_string()))?,
            "rounds": serde_json::to_value(&self.rounds)
                .map_err(|e| ApprovalError::SerializationError(e.to_string()))?,
        }))
    }

    // ------------------------------------------------------------ 内部实现

    /// 子实体 ID：`{instance_id}:{prefix}-{seq:04}`。序号随聚合持久化 ⇒ 重载后不重复。
    fn gen(&mut self, prefix: &str) -> Id {
        self.seq += 1;
        format!("{}:{prefix}-{:04}", self.instance.id, self.seq)
    }

    fn touch(&mut self, now: i64) {
        self.instance.revision += 1;
        self.instance.updated_at = now;
    }

    fn guard(&self, expected: u64) -> Result<(), ApprovalError> {
        if self.instance.revision != expected {
            return Err(ApprovalError::StaleRevision {
                expected,
                actual: self.instance.revision,
            });
        }
        if self.instance.status.is_terminal() {
            return Err(ApprovalError::InvalidTransition(format!(
                "instance is {:?}",
                self.instance.status
            )));
        }
        Ok(())
    }

    /// 等待态命令的守卫：只校验修订号与非终态。
    fn guard_waiting(&self, expected: u64) -> Result<(), ApprovalError> {
        if self.instance.revision != expected {
            return Err(ApprovalError::StaleRevision {
                expected,
                actual: self.instance.revision,
            });
        }
        if self.instance.status != InstanceStatus::Waiting {
            return Err(ApprovalError::InvalidTransition(format!(
                "instance is {:?}, not Waiting",
                self.instance.status
            )));
        }
        Ok(())
    }

    fn emit(&self, out: &mut Vec<Effect>, name: &str, data: Value) {
        out.push(Effect::Event {
            name: name.into(),
            data,
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn create_branch(
        &mut self,
        node: &str,
        parent: Option<Id>,
        join: JoinPolicy,
        reject: RejectPolicy,
        policy: ApprovalPolicy,
        now: i64,
        out: &mut Vec<Effect>,
    ) -> Id {
        let attempt = self.node_attempts.get(node).copied().unwrap_or(0) + 1;
        self.node_attempts.insert(node.to_string(), attempt);
        let id = self.gen("branch");
        self.branches.insert(
            id.clone(),
            Branch {
                id: id.clone(),
                instance_id: self.instance.id.clone(),
                parent_branch_id: parent,
                node: node.into(),
                attempt,
                status: BranchStatus::Open,
                join,
                reject,
                policy,
                scope: Scope::new(),
                round_ids: Vec::new(),
                created_at: now,
                closed_at: None,
            },
        );
        if self.instance.root_branch_id.is_none() {
            self.instance.root_branch_id = Some(id.clone());
        }
        out.push(Effect::BranchCreated {
            id: id.clone(),
            node: node.into(),
            attempt,
        });
        id
    }

    fn add_seats(
        &mut self,
        branch_id: &str,
        round_id: &str,
        seats: Vec<SeatSpec>,
        deadline: Option<i64>,
        now: i64,
        out: &mut Vec<Effect>,
    ) -> Vec<Id> {
        let mut ids = Vec::new();
        for s in seats {
            if s.assignee.is_none() && s.candidates.is_empty() {
                continue;
            }
            let tid = self.gen("task");
            self.tasks.insert(
                tid.clone(),
                Task {
                    id: tid.clone(),
                    branch_id: branch_id.into(),
                    round_id: round_id.into(),
                    kind: s.kind,
                    status: if s.origin == TaskOrigin::AfterAdded {
                        TaskStatus::Scheduled
                    } else {
                        TaskStatus::Created
                    },
                    assignee: s.assignee.clone(),
                    candidates: s.candidates.clone(),
                    deadline,
                    origin: s.origin,
                    predecessor: s.predecessor.clone(),
                    successor: None,
                    spawned_by: s.spawned_by.clone(),
                    suspended_from: None,
                    delegation: None,
                    transferred_from: Vec::new(),
                    votes: Vec::new(),
                    scope: Scope::new(),
                    created_at: now,
                    updated_at: now,
                },
            );
            out.push(Effect::TaskCreated {
                id: tid.clone(),
                branch: branch_id.into(),
                round: round_id.into(),
                kind: s.kind,
                assignee: s.assignee,
                candidates: s.candidates,
            });
            ids.push(tid);
        }
        ids
    }

    /// 创建轮次（不动席位）：供前后加签派生轮次使用。
    #[allow(clippy::too_many_arguments)]
    fn create_round(
        &mut self,
        branch_id: &str,
        number: u32,
        status: RoundStatus,
        join: Option<JoinPolicy>,
        reject: Option<RejectPolicy>,
        now: i64,
        out: &mut Vec<Effect>,
    ) -> Id {
        let rid = self.gen("round");
        self.rounds.insert(
            rid.clone(),
            Round {
                id: rid.clone(),
                branch_id: branch_id.into(),
                number,
                status,
                join,
                reject,
                total_count: 0,
                completed_count: 0,
                rejected_count: 0,
                reject_reason: None,
                opened_at: now,
                closed_at: None,
            },
        );
        if let Some(branch) = self.branches.get_mut(branch_id) {
            branch.round_ids.push(rid.clone());
        }
        if status == RoundStatus::Open {
            out.push(Effect::RoundOpened {
                id: rid.clone(),
                branch: branch_id.into(),
                number,
            });
        }
        rid
    }

    fn next_round_number(&self, branch_id: &str) -> u32 {
        self.rounds
            .values()
            .filter(|r| r.branch_id == branch_id)
            .map(|r| r.number)
            .max()
            .unwrap_or(0)
            + 1
    }

    /// 前加签完成：恢复最近一个被挂起且序号更小的轮次。
    fn resume_suspended_round_below(
        &mut self,
        branch_id: &str,
        number: u32,
        now: i64,
        out: &mut Vec<Effect>,
    ) -> bool {
        let next = self
            .rounds
            .values()
            .filter(|r| {
                r.branch_id == branch_id && r.status == RoundStatus::Suspended && r.number < number
            })
            .max_by_key(|r| r.number)
            .map(|r| r.id.clone());
        match next {
            Some(rid) => {
                self.resume_round(&rid, now, out);
                true
            }
            None => false,
        }
    }

    /// 前驱轮次通过：激活本分支最早的后加签轮次（原轮次与后加签之间的业务节点不入栈）。
    ///
    /// 不按 `number > 前驱` 过滤：超时恢复会新建一轮，其序号大于已排队的后加签轮次，
    /// 但语义上后加签仍应在原轮次（恢复后的新轮次）通过后执行。
    fn activate_scheduled_after(
        &mut self,
        branch_id: &str,
        _number: u32,
        now: i64,
        out: &mut Vec<Effect>,
    ) -> bool {
        let next = self
            .rounds
            .values()
            .filter(|r| r.branch_id == branch_id && r.status == RoundStatus::Scheduled)
            .min_by_key(|r| r.number)
            .map(|r| r.id.clone());
        match next {
            Some(rid) => {
                self.activate_scheduled_round(&rid, now, out);
                true
            }
            None => false,
        }
    }

    /// 挂起轮次：未决席位置 `Suspended`（对外仍属原任务，但不可见/不可投票）。
    fn suspend_round(&mut self, round_id: &str, now: i64, out: &mut Vec<Effect>) {
        let task_ids: Vec<Id> = self
            .tasks
            .values()
            .filter(|t| t.round_id == round_id && t.status.is_open())
            .map(|t| t.id.clone())
            .collect();
        for tid in task_ids {
            if let Some(t) = self.tasks.get_mut(&tid) {
                let from = t.status;
                t.suspended_from = Some(from);
                t.status = TaskStatus::Suspended;
                t.updated_at = now;
                out.push(Effect::TaskStatus {
                    id: tid.clone(),
                    from,
                    to: TaskStatus::Suspended,
                });
                self.instance.audit.push(AuditEntry {
                    at: now,
                    action: "task_suspended".into(),
                    branch: Some(t.branch_id.clone()),
                    task: Some(tid.clone()),
                    by: "engine".into(),
                    detail: json!({"reason": "BEFORE_ADDED"}).to_string(),
                    acting_service: None,
                    action_id: None,
                });
            }
        }
        if let Some(r) = self.rounds.get_mut(round_id) {
            let from = r.status;
            r.status = RoundStatus::Suspended;
            out.push(Effect::RoundStatus {
                id: round_id.into(),
                from,
                to: RoundStatus::Suspended,
            });
        }
    }

    /// 恢复被前加签挂起的轮次：席位状态回填到挂起前，轮次重新 Open。
    fn resume_round(&mut self, round_id: &str, now: i64, out: &mut Vec<Effect>) {
        if let Some(r) = self.rounds.get_mut(round_id) {
            let from = r.status;
            if from != RoundStatus::Suspended {
                return;
            }
            r.status = RoundStatus::Open;
            out.push(Effect::RoundStatus {
                id: round_id.into(),
                from,
                to: RoundStatus::Open,
            });
        }
        let task_ids: Vec<Id> = self
            .tasks
            .values()
            .filter(|t| t.round_id == round_id && t.status == TaskStatus::Suspended)
            .map(|t| t.id.clone())
            .collect();
        for tid in task_ids {
            if let Some(t) = self.tasks.get_mut(&tid) {
                let to = t.suspended_from.take().unwrap_or(TaskStatus::Created);
                let from = t.status;
                t.status = to;
                t.updated_at = now;
                out.push(Effect::TaskStatus {
                    id: tid.clone(),
                    from,
                    to,
                });
                self.instance.audit.push(AuditEntry {
                    at: now,
                    action: "task_resumed".into(),
                    branch: Some(t.branch_id.clone()),
                    task: Some(tid.clone()),
                    by: "engine".into(),
                    detail: json!({"reason": "BEFORE_ADDED"}).to_string(),
                    acting_service: None,
                    action_id: None,
                });
            }
        }
    }

    /// 激活后加签轮次：待激活席位转为未决。
    fn activate_scheduled_round(&mut self, round_id: &str, now: i64, out: &mut Vec<Effect>) {
        let task_ids: Vec<Id> = self
            .tasks
            .values()
            .filter(|t| t.round_id == round_id && t.status == TaskStatus::Scheduled)
            .map(|t| t.id.clone())
            .collect();
        for tid in task_ids {
            if let Some(t) = self.tasks.get_mut(&tid) {
                t.status = TaskStatus::Created;
                t.updated_at = now;
                out.push(Effect::TaskStatus {
                    id: tid.clone(),
                    from: TaskStatus::Scheduled,
                    to: TaskStatus::Created,
                });
            }
        }
        if let Some(r) = self.rounds.get_mut(round_id) {
            let from = r.status;
            r.status = RoundStatus::Open;
            out.push(Effect::RoundStatus {
                id: round_id.into(),
                from,
                to: RoundStatus::Open,
            });
        }
    }

    /// 丢弃后加签派生任务（原任务否决 / 上游回退）：写审计并取消待激活席位。
    fn discard_scheduled_after(
        &mut self,
        branch_id: &str,
        reason: &str,
        now: i64,
        out: &mut Vec<Effect>,
    ) {
        let round_ids: Vec<Id> = self
            .rounds
            .values()
            .filter(|r| r.branch_id == branch_id && r.status == RoundStatus::Scheduled)
            .map(|r| r.id.clone())
            .collect();
        for rid in round_ids {
            let task_ids: Vec<Id> = self
                .tasks
                .values()
                .filter(|t| t.round_id == rid)
                .map(|t| t.id.clone())
                .collect();
            for tid in &task_ids {
                let mut discarded = false;
                let mut branch = branch_id.to_string();
                if let Some(t) = self.tasks.get_mut(tid) {
                    branch = t.branch_id.clone();
                    if t.status == TaskStatus::Scheduled {
                        t.status = TaskStatus::Cancelled;
                        t.updated_at = now;
                        discarded = true;
                        out.push(Effect::TaskStatus {
                            id: tid.clone(),
                            from: TaskStatus::Scheduled,
                            to: TaskStatus::Cancelled,
                        });
                    }
                }
                if discarded {
                    self.instance.audit.push(AuditEntry {
                        at: now,
                        action: "after_spawn_discarded".into(),
                        branch: Some(branch),
                        task: Some(tid.clone()),
                        by: "engine".into(),
                        detail: json!({"reason": reason}).to_string(),
                        acting_service: None,
                        action_id: None,
                    });
                }
            }
            if let Some(r) = self.rounds.get_mut(&rid) {
                let from = r.status;
                r.status = RoundStatus::Cancelled;
                r.closed_at = Some(now);
                out.push(Effect::RoundStatus {
                    id: rid,
                    from,
                    to: RoundStatus::Cancelled,
                });
            }
        }
    }

    /// 资格判定：直接责任人、候选池成员与被委派人都可操作；委派场景返回被代表人。
    /// 已到期/已撤销的委派不再授予代理权限。
    fn actor_can_act(
        &self,
        t: &Task,
        actor: &str,
        now: i64,
    ) -> Result<Option<Principal>, ApprovalError> {
        if let Some(d) = &t.delegation {
            if d.to == actor && d.is_active(now) {
                return Ok(Some(d.from.clone()));
            }
        }
        match &t.assignee {
            Some(a) if a == actor => Ok(None),
            Some(_) => Err(ApprovalError::NotAuthorized(format!(
                "task {} is assigned to another",
                t.id
            ))),
            None => {
                if t.candidates.iter().any(|c| c == actor) {
                    Ok(None)
                } else {
                    Err(ApprovalError::NotAuthorized(format!(
                        "task {} is not open to {actor}",
                        t.id
                    )))
                }
            }
        }
    }

    /// 终态/转办时撤销委派：保留记录并把原因写进 assignment 读面（不再授予代理权）。
    fn revoke_delegation(t: &mut Task, reason: &str, now: i64) {
        if let Some(d) = t.delegation.as_mut() {
            if d.revoked_at.is_none() {
                d.revoked_at = Some(now);
                d.revoke_reason = Some(reason.to_string());
            }
        }
    }

    fn vote(
        &mut self,
        expected: u64,
        task_id: &str,
        actor: &str,
        decision: Decision,
        comment: Option<&str>,
        now: i64,
    ) -> Result<Vec<Effect>, ApprovalError> {
        self.guard(expected)?;
        let mut t = self
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| ApprovalError::NotFound(task_id.into()))?;
        if t.kind != TaskKind::Approval {
            return Err(ApprovalError::InvalidTransition(format!(
                "task {} is not an approval task",
                t.id
            )));
        }
        if !t.status.is_open() {
            return Err(ApprovalError::InvalidTransition(format!(
                "task {} is {:?}",
                t.id, t.status
            )));
        }
        let on_behalf_of = self.actor_can_act(&t, actor, now)?;
        let implicit_claim = t.assignee.is_none();
        if implicit_claim {
            t.assignee = Some(actor.into());
        }
        let from = t.status;
        let to = match decision {
            Decision::Approve => TaskStatus::Completed,
            Decision::Reject => TaskStatus::Rejected,
        };
        t.votes.push(VoteRecord {
            round_id: t.round_id.clone(),
            decision,
            actor: actor.into(),
            on_behalf_of,
            comment: comment.map(str::to_string),
            at: now,
            acting_service: self.acting_service.clone(),
            action_id: self.action_id.clone(),
        });
        t.status = to;
        t.updated_at = now;
        Self::revoke_delegation(
            &mut t,
            match decision {
                Decision::Approve => "RULE_SATISFIED",
                Decision::Reject => "TASK_REJECTED",
            },
            now,
        );
        let rid = t.round_id.clone();
        let id = t.id.clone();
        self.tasks.insert(id.clone(), t);
        let mut out = Vec::new();
        if implicit_claim {
            out.push(Effect::TaskAssigned {
                id: id.clone(),
                assignee: actor.into(),
                kind: AssignKind::Claim,
            });
        }
        out.push(Effect::TaskStatus { id, from, to });
        self.recount(&rid, now, &mut out);
        self.touch(now);
        Ok(out)
    }

    /// 轮次统计与判定。会签/或签/定额/比例、一票否决、耗尽判定都在这里按
    /// `TRANSITIONS.md` §3 的 1→7 顺序收口。
    fn recount(&mut self, round_id: &str, now: i64, out: &mut Vec<Effect>) {
        let Some(round) = self.rounds.get(round_id).cloned() else {
            return;
        };
        if round.status != RoundStatus::Open {
            return;
        }
        let Some(branch) = self.branches.get(&round.branch_id).cloned() else {
            return;
        };
        if branch.status != BranchStatus::Open {
            return;
        }
        let approvals: Vec<Task> = self
            .tasks
            .values()
            .filter(|t| t.round_id == round.id && t.kind == TaskKind::Approval)
            .cloned()
            .collect();
        let total = approvals
            .iter()
            .filter(|t| {
                !matches!(
                    t.status,
                    TaskStatus::Cancelled
                        | TaskStatus::Expired
                        | TaskStatus::Suspended
                        | TaskStatus::Scheduled
                )
            })
            .count() as u32;
        let completed = approvals
            .iter()
            .filter(|t| t.status == TaskStatus::Completed)
            .count() as u32;
        let rejected = approvals
            .iter()
            .filter(|t| t.status == TaskStatus::Rejected)
            .count() as u32;

        let mut r = round.clone();
        r.total_count = total;
        r.completed_count = completed;
        r.rejected_count = rejected;
        out.push(Effect::RoundCounted {
            id: r.id.clone(),
            total,
            completed,
            rejected,
        });

        let mut verdict: Option<RoundStatus> = None;
        let mut reject_reason: Option<&str> = None;
        let open_count = approvals.iter().filter(|t| t.status.is_open()).count() as u32;
        let join = round.join.unwrap_or(branch.join);
        let reject = round.reject.unwrap_or(branch.reject);
        let required = required_approvals(join, total);
        let passed = match join {
            JoinPolicy::All => total > 0 && completed >= total,
            JoinPolicy::Any => completed >= 1,
            JoinPolicy::Quorum(n) => completed >= n,
            JoinPolicy::Ratio { .. } => completed >= required,
        };
        // 派生轮次（前/后加签）被整批移除：门禁已经不存在，按「空前加签通过」恢复下游，
        // 不能留下 `total == 0` 的 Open 轮次把原轮次永久挂起。原始轮次的 `total == 0`
        // 仍保持 Open（B-WF-5：全员过期/被减签无自动判定）。
        let derived_gate_empty = !approvals.is_empty()
            && approvals.iter().all(|t| {
                matches!(t.origin, TaskOrigin::BeforeAdded | TaskOrigin::AfterAdded)
                    && t.status == TaskStatus::Cancelled
            });
        if approvals.is_empty() || derived_gate_empty {
            // 从未创建可计票席位的轮次（如仅有阅签席）立即通过。反之 `total == 0`（全员过期
            // 或被减签）绝不自动通过——超时与减签耗尽保持 `Open`，处置由人决定（Q4 无自动判定）。
            verdict = Some(RoundStatus::Passed);
        } else if reject == RejectPolicy::AnyReject && rejected > 0 {
            verdict = Some(RoundStatus::Rejected);
            reject_reason = Some("any_reject");
        } else if matches!(reject, RejectPolicy::AtLeastRejects(n) if rejected >= n) && rejected > 0
        {
            verdict = Some(RoundStatus::Rejected);
            reject_reason = Some("reject_threshold_reached");
        } else if passed {
            verdict = Some(RoundStatus::Passed);
        } else if total > 0 {
            // `RecordAndContinue` 在每次否决后重算：剩余未投席位全部通过也无法满足
            // 通过条件时立即判不通过，不等所有人投完（死锁兜底）。`OnExhausted`
            // 保持「全部投完才判」的既有语义。
            let exhausted = completed + rejected >= total;
            let unsatisfiable = reject == RejectPolicy::RecordAndContinue
                && rejected > 0
                && completed + open_count < required;
            if exhausted || unsatisfiable {
                verdict = Some(RoundStatus::Rejected);
                reject_reason = Some(if reject == RejectPolicy::RecordAndContinue {
                    "sign_completion_unsatisfiable"
                } else {
                    "on_exhausted"
                });
            }
        }

        match verdict {
            Some(RoundStatus::Passed) => {
                r.status = RoundStatus::Passed;
                r.closed_at = Some(now);
                self.rounds.insert(r.id.clone(), r.clone());
                out.push(Effect::RoundStatus {
                    id: r.id.clone(),
                    from: RoundStatus::Open,
                    to: RoundStatus::Passed,
                });
                self.cancel_pending_tasks(round_id, now, out);
                // 前加签完成：先恢复被挂起的原轮次；否则前驱轮次通过后再激活后加签；
                // 两者都没有才关闭分支（走下一业务节点）。
                let resumed = self.resume_suspended_round_below(&r.branch_id, r.number, now, out);
                if !resumed {
                    let activated = self.activate_scheduled_after(&r.branch_id, r.number, now, out);
                    if !activated {
                        self.close_branch(&r.branch_id, BranchStatus::Completed, now, out);
                    }
                }
                let data = json!({
                    "instance_id": self.instance.id,
                    "branch_id": r.branch_id,
                    "round_id": r.id,
                    "total": total,
                    "completed": completed,
                    "rejected": rejected,
                });
                self.emit(out, "com.china-workflow.approval.round.passed.v1", data);
            }
            Some(RoundStatus::Rejected) => {
                r.status = RoundStatus::Rejected;
                r.reject_reason = reject_reason.map(str::to_string);
                r.closed_at = Some(now);
                self.rounds.insert(r.id.clone(), r.clone());
                out.push(Effect::RoundStatus {
                    id: r.id.clone(),
                    from: RoundStatus::Open,
                    to: RoundStatus::Rejected,
                });
                self.cancel_pending_tasks(round_id, now, out);
                // 前加签被否决 = 上游回退；原轮次仍在挂起，必须一并取消，不能留下
                // 「分支已 Rejected、原席位仍可恢复」的僵尸轮次。
                let discard_reason = if approvals
                    .iter()
                    .any(|t| t.origin == TaskOrigin::BeforeAdded)
                {
                    "UPSTREAM_ROLLBACK"
                } else {
                    "REJECTED"
                };
                self.discard_scheduled_after(&r.branch_id, discard_reason, now, out);
                self.cancel_live_tasks_of_branch(&r.branch_id, now, out);
                self.close_rounds_of_branch(&r.branch_id, RoundStatus::Cancelled, now, out);
                self.close_branch(&r.branch_id, BranchStatus::Rejected, now, out);
                let data = json!({
                    "instance_id": self.instance.id,
                    "branch_id": r.branch_id,
                    "round_id": r.id,
                    "total": total,
                    "completed": completed,
                    "rejected": rejected,
                    "reason": reject_reason,
                });
                self.emit(out, "com.china-workflow.approval.round.rejected.v1", data);
            }
            _ => {
                self.rounds.insert(r.id.clone(), r);
            }
        }
    }

    /// 通过/驳回的收口：关闭分支，并在根分支被否决时置实例 `Rejected`。
    fn close_branch(&mut self, branch_id: &str, to: BranchStatus, now: i64, out: &mut Vec<Effect>) {
        let Some(branch) = self.branches.get(branch_id).cloned() else {
            return;
        };
        let from = branch.status;
        let mut b = branch;
        b.status = to;
        b.closed_at = Some(now);
        self.branches.insert(b.id.clone(), b.clone());
        out.push(Effect::BranchStatus {
            id: b.id.clone(),
            from,
            to,
        });
        // 分支通过不自动收口实例：状态机内部无法得知定义是否还有后继节点，由驱动方在定义
        // 走到末端时调用 `finish`。分支否决是终局，直接收口实例。
        if !self.defer_instance_rejection
            && b.parent_branch_id.is_none()
            && to == BranchStatus::Rejected
        {
            let before = self.instance.status;
            if before != InstanceStatus::Rejected {
                self.instance.status = InstanceStatus::Rejected;
                out.push(Effect::InstanceStatus {
                    from: before,
                    to: InstanceStatus::Rejected,
                });
                let id = self.instance.id.clone();
                self.emit(
                    out,
                    "com.china-workflow.instance.rejected.v1",
                    json!({ "instance_id": id }),
                );
            }
        }
    }

    /// 非判定性关闭（Returned / Superseded / Cancelled / Terminated）：不触发实例收口，
    /// 与判定路径 [`Engine::close_branch`] 相对；关闭时间只写一次，重复设为同一状态无副作用。
    fn set_branch_status(
        &mut self,
        branch_id: &str,
        to: BranchStatus,
        now: i64,
        out: &mut Vec<Effect>,
    ) {
        if let Some(b) = self.branches.get_mut(branch_id) {
            let from = b.status;
            if from == to {
                return;
            }
            b.status = to;
            if b.closed_at.is_none() {
                b.closed_at = Some(now);
            }
            let id = b.id.clone();
            out.push(Effect::BranchStatus { id, from, to });
        }
    }

    /// 作废/取消一个分支下的全部 Open 轮次（非判定性，不经 [`Engine::recount`]）。
    fn close_rounds_of_branch(
        &mut self,
        branch_id: &str,
        to: RoundStatus,
        now: i64,
        out: &mut Vec<Effect>,
    ) {
        let ids: Vec<Id> = self
            .rounds
            .values()
            .filter(|r| {
                r.branch_id == branch_id
                    && matches!(
                        r.status,
                        RoundStatus::Open | RoundStatus::Suspended | RoundStatus::Scheduled
                    )
            })
            .map(|r| r.id.clone())
            .collect();
        for id in ids {
            if let Some(r) = self.rounds.get_mut(&id) {
                let from = r.status;
                r.status = to;
                r.closed_at = Some(now);
                out.push(Effect::RoundStatus { id, from, to });
            }
        }
    }

    /// 取消分支内全部仍可能恢复的席位（未决 / 挂起 / 待激活），用于回退/终止。
    fn cancel_live_tasks_of_branch(&mut self, branch_id: &str, now: i64, out: &mut Vec<Effect>) {
        let ids: Vec<Id> = self
            .tasks
            .values()
            .filter(|t| t.branch_id == branch_id && t.status.is_live())
            .map(|t| t.id.clone())
            .collect();
        self.cancel_tasks(&ids, now, out);
    }

    /// 取消一轮的全部未决席位：审批席与阅签席一并收口，轮次关闭后不残留 open 席位。
    fn cancel_pending_tasks(&mut self, round_id: &str, now: i64, out: &mut Vec<Effect>) {
        let ids: Vec<Id> = self
            .tasks
            .values()
            .filter(|t| t.round_id == round_id && t.status.is_open())
            .map(|t| t.id.clone())
            .collect();
        self.cancel_tasks(&ids, now, out);
    }

    fn cancel_tasks(&mut self, ids: &[Id], now: i64, out: &mut Vec<Effect>) {
        for id in ids {
            if let Some(t) = self.tasks.get_mut(id) {
                if t.status.is_live() {
                    let from = t.status;
                    t.status = TaskStatus::Cancelled;
                    t.updated_at = now;
                    Self::revoke_delegation(t, "TASK_CANCELLED", now);
                    out.push(Effect::TaskStatus {
                        id: id.clone(),
                        from,
                        to: TaskStatus::Cancelled,
                    });
                }
            }
        }
    }

    /// 级联取消全部 Open 分支：先取消其未决席位，再取消 Open 轮次，最后将分支置为
    /// `branch_status`（`withdraw` → `Cancelled`；`terminate` → `Terminated`）。
    fn cancel_open_branches(
        &mut self,
        branch_status: BranchStatus,
        out: &mut Vec<Effect>,
        now: i64,
    ) {
        let branch_ids: Vec<Id> = self
            .branches
            .values()
            .filter(|b| b.status == BranchStatus::Open)
            .map(|b| b.id.clone())
            .collect();
        for bid in &branch_ids {
            self.discard_scheduled_after(bid, "UPSTREAM_ROLLBACK", now, out);
            self.cancel_live_tasks_of_branch(bid, now, out);
            self.close_rounds_of_branch(bid, RoundStatus::Cancelled, now, out);
            self.set_branch_status(bid, branch_status, now, out);
        }
    }
}

/// 通过判定所需的通过票数：比例策略向上取整（4 席 × 2/3 ⇒ 需要 3 票）。
fn required_approvals(join: JoinPolicy, total: u32) -> u32 {
    match join {
        JoinPolicy::All => total,
        JoinPolicy::Any => 1,
        JoinPolicy::Quorum(n) => n,
        JoinPolicy::Ratio { num, den } => {
            if den == 0 {
                total
            } else {
                (total as u64 * num as u64).div_ceil(den as u64) as u32
            }
        }
    }
}
