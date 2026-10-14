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
                status: InstanceStatus::Pending,
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
        })
    }

    // -------------------------------------------------------------- 流程

    /// 进入节点（分支）。线性原型：同一时刻只允许一个 Open 分支。
    pub fn enter_branch(
        &mut self,
        expected: u64,
        node: &str,
        parent: Option<Id>,
        join: JoinPolicy,
        reject: RejectPolicy,
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
        let id = self.create_branch(node, parent, join, reject, now, &mut out);
        let attempt = self.branches[&id].attempt;
        self.instance.audit.push(AuditEntry {
            at: now,
            action: "enter_branch".into(),
            branch: Some(id.clone()),
            by: "engine".into(),
            detail: format!("node={node} attempt={attempt}"),
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
                total_count: 0,
                completed_count: 0,
                rejected_count: 0,
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
        });
        self.touch(now);
        Ok((ids, out))
    }

    /// 减签：取消当前 Open 轮次中的未决席位（已表态的席位不允许被减）。
    pub fn remove_sign(
        &mut self,
        expected: u64,
        branch_id: &str,
        task_ids: &[Id],
        by: &str,
        now: i64,
    ) -> Result<Vec<Effect>, ApprovalError> {
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
            .ok_or_else(|| {
                ApprovalError::InvalidTransition("no open round to remove seats".into())
            })?;
        let mut out = Vec::new();
        for tid in task_ids {
            let t = self
                .tasks
                .get(tid)
                .cloned()
                .ok_or_else(|| ApprovalError::NotFound(tid.clone()))?;
            if t.round_id != rid {
                return Err(ApprovalError::InvalidArgument(format!(
                    "task {} is not in the open round",
                    t.id
                )));
            }
            if !t.status.is_open() {
                return Err(ApprovalError::InvalidTransition(format!(
                    "task {} already decided",
                    t.id
                )));
            }
            let from = t.status;
            let t = self
                .tasks
                .get_mut(tid)
                .ok_or_else(|| ApprovalError::NotFound(tid.clone()))?;
            t.status = TaskStatus::Cancelled;
            t.updated_at = now;
            out.push(Effect::TaskStatus {
                id: tid.clone(),
                from,
                to: TaskStatus::Cancelled,
            });
        }
        self.recount(&rid, now, &mut out);
        self.instance.audit.push(AuditEntry {
            at: now,
            action: "remove_sign".into(),
            branch: Some(branch.id.clone()),
            by: by.into(),
            detail: format!("seats={}", task_ids.len()),
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
        self.actor_can_act(&t, actor)?;
        if t.assignee.is_none() {
            t.assignee = Some(actor.into());
        }
        let from = t.status;
        t.status = TaskStatus::Completed;
        t.updated_at = now;
        let id = t.id.clone();
        self.tasks.insert(id.clone(), t);
        self.touch(now);
        Ok(vec![Effect::TaskStatus {
            id,
            from,
            to: TaskStatus::Completed,
        }])
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
            InstanceStatus::Completed => "com.china-workflow.instance.completed",
            InstanceStatus::Rejected => "com.china-workflow.instance.rejected",
            _ => "com.china-workflow.instance.status_changed",
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
                        || t.delegation.as_ref().map(|d| d.to.as_str()) == Some(principal))
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

    fn emit(&self, out: &mut Vec<Effect>, name: &str, data: Value) {
        out.push(Effect::Event {
            name: name.into(),
            data,
        });
    }

    fn create_branch(
        &mut self,
        node: &str,
        parent: Option<Id>,
        join: JoinPolicy,
        reject: RejectPolicy,
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
                    status: TaskStatus::Created,
                    assignee: s.assignee.clone(),
                    candidates: s.candidates.clone(),
                    deadline,
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

    /// 资格判定：直接责任人、候选池成员与被委派人都可操作；委派场景返回被代表人。
    fn actor_can_act(&self, t: &Task, actor: &str) -> Result<Option<Principal>, ApprovalError> {
        if let Some(d) = &t.delegation {
            if d.to == actor {
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
        let on_behalf_of = self.actor_can_act(&t, actor)?;
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
        });
        t.status = to;
        t.updated_at = now;
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
            .filter(|t| !matches!(t.status, TaskStatus::Cancelled | TaskStatus::Expired))
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
        if branch.reject == RejectPolicy::AnyReject && rejected > 0 {
            verdict = Some(RoundStatus::Rejected);
        } else if total == 0 || completed >= total {
            verdict = Some(RoundStatus::Passed);
        } else {
            let pass = match branch.join {
                JoinPolicy::All => false,
                JoinPolicy::Any => completed >= 1,
                JoinPolicy::Quorum(n) => completed >= n,
                JoinPolicy::Ratio { num, den } => {
                    completed as u64 * den as u64 >= total as u64 * num as u64
                }
            };
            if pass {
                verdict = Some(RoundStatus::Passed);
            } else if completed + rejected >= total {
                verdict = Some(RoundStatus::Rejected);
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
                self.close_branch(&r.branch_id, BranchStatus::Completed, now, out);
                let data = json!({
                    "instance_id": self.instance.id,
                    "branch_id": r.branch_id,
                    "round_id": r.id,
                    "total": total,
                    "completed": completed,
                    "rejected": rejected,
                });
                self.emit(out, "com.china-workflow.approval.round.passed", data);
            }
            Some(RoundStatus::Rejected) => {
                r.status = RoundStatus::Rejected;
                r.closed_at = Some(now);
                self.rounds.insert(r.id.clone(), r.clone());
                out.push(Effect::RoundStatus {
                    id: r.id.clone(),
                    from: RoundStatus::Open,
                    to: RoundStatus::Rejected,
                });
                self.cancel_pending_tasks(round_id, now, out);
                self.close_branch(&r.branch_id, BranchStatus::Rejected, now, out);
                let data = json!({
                    "instance_id": self.instance.id,
                    "branch_id": r.branch_id,
                    "round_id": r.id,
                    "total": total,
                    "completed": completed,
                    "rejected": rejected,
                });
                self.emit(out, "com.china-workflow.approval.round.rejected", data);
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
        if b.parent_branch_id.is_none() && to == BranchStatus::Rejected {
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
                    "com.china-workflow.instance.rejected",
                    json!({ "instance_id": id }),
                );
            }
        }
    }

    /// 非判定性关闭（Returned / Superseded / Cancelled / Terminated）：不触发实例收口，
    /// 与判定路径 [`Engine::close_branch`] 相对；关闭时间只写一次。
    ///
    /// 核心生命周期命令不经过这条路径（判定一律走 `close_branch`），故当前没有调用方；
    /// 保留它是为了让管理命令沿用同一套关闭语义。
    #[allow(dead_code)]
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

    fn cancel_pending_tasks(&mut self, round_id: &str, now: i64, out: &mut Vec<Effect>) {
        let ids: Vec<Id> = self
            .tasks
            .values()
            .filter(|t| {
                t.round_id == round_id && t.kind == TaskKind::Approval && t.status.is_open()
            })
            .map(|t| t.id.clone())
            .collect();
        self.cancel_tasks(&ids, now, out);
    }

    fn cancel_tasks(&mut self, ids: &[Id], now: i64, out: &mut Vec<Effect>) {
        for id in ids {
            if let Some(t) = self.tasks.get_mut(id) {
                if t.status.is_open() {
                    let from = t.status;
                    t.status = TaskStatus::Cancelled;
                    t.updated_at = now;
                    out.push(Effect::TaskStatus {
                        id: id.clone(),
                        from,
                        to: TaskStatus::Cancelled,
                    });
                }
            }
        }
    }
}
