//! 命令层：状态机转换、不变量与效果流。
//!
//! 不变量（全部由方法内部强制）：
//! - 所有命令带 `expected_revision`（乐观并发守卫），陈旧写入返回 `StaleRevision` 且不改状态；
//! - 终态实例拒绝一切变更命令；
//! - 同一实例同时最多一个 Open 分支（线性原型；并发分支留待 fork 语义验证）；
//! - 一个分支同时最多一个 Open 轮次；
//! - 阅签不计票；已取消/过期席位不计入 total_count；
//! - 驳回至节点只允许回到白名单（已进入过的节点）内，且 jump_count 有上限。

use crate::model::*;
use serde_json::{json, Value};
use std::collections::BTreeMap;

/// 驳回至节点的累计上限（§8 jump_count 防循环的模型层守卫；真实上限由定义侧配置）。
pub const MAX_JUMPS: u32 = 20;

pub struct Engine {
    pub instance: Instance,
    pub branches: BTreeMap<Id, Branch>,
    pub tasks: BTreeMap<Id, Task>,
    pub rounds: BTreeMap<Id, Round>,
    /// 节点名 → 已进入次数；同时充当驳回白名单。
    node_attempts: BTreeMap<String, u32>,
    seq: u64,
}

impl Engine {
    // -------------------------------------------------------------- 构造

    pub fn start(
        ns: &str,
        name: &str,
        version: &str,
        initiator: &str,
        context: Scope,
        now: i64,
    ) -> Self {
        let mut e = Engine {
            instance: Instance {
                id: String::new(),
                definition_ns: ns.into(),
                definition_name: name.into(),
                definition_version: version.into(),
                initiator: initiator.into(),
                status: InstanceStatus::Pending,
                context,
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
        };
        e.instance.id = e.gen("inst");
        e
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
    ) -> Result<(Id, Vec<Effect>), Error> {
        self.guard(expected)?;
        if self
            .branches
            .values()
            .any(|b| b.status == BranchStatus::Open)
        {
            return Err(Error::InvalidTransition("another branch is open".into()));
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
    ) -> Result<(Id, Vec<Id>, Vec<Effect>), Error> {
        self.guard(expected)?;
        let branch = self
            .branches
            .get(branch_id)
            .cloned()
            .ok_or_else(|| Error::NotFound(branch_id.into()))?;
        if branch.status != BranchStatus::Open {
            return Err(Error::InvalidTransition(format!(
                "branch {} is {:?}",
                branch.id, branch.status
            )));
        }
        if self
            .rounds
            .values()
            .any(|r| r.branch_id == branch.id && r.status == RoundStatus::Open)
        {
            return Err(Error::InvalidTransition(
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
        self.branches
            .get_mut(branch_id)
            .unwrap()
            .round_ids
            .push(rid.clone());
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
    ) -> Result<(Vec<Id>, Vec<Effect>), Error> {
        self.guard(expected)?;
        let branch = self
            .branches
            .get(branch_id)
            .cloned()
            .ok_or_else(|| Error::NotFound(branch_id.into()))?;
        if branch.status != BranchStatus::Open {
            return Err(Error::InvalidTransition(format!(
                "branch {} is {:?}",
                branch.id, branch.status
            )));
        }
        let rid = self
            .rounds
            .values()
            .find(|r| r.branch_id == branch.id && r.status == RoundStatus::Open)
            .map(|r| r.id.clone())
            .ok_or_else(|| Error::InvalidTransition("no open round to add seats".into()))?;
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

    /// 减签：取消当前 Open 轮次中的未决席位（已表态的席位不允许被减，见 GAPS.md Q3）。
    pub fn remove_sign(
        &mut self,
        expected: u64,
        branch_id: &str,
        task_ids: &[Id],
        by: &str,
        now: i64,
    ) -> Result<Vec<Effect>, Error> {
        self.guard(expected)?;
        let branch = self
            .branches
            .get(branch_id)
            .cloned()
            .ok_or_else(|| Error::NotFound(branch_id.into()))?;
        if branch.status != BranchStatus::Open {
            return Err(Error::InvalidTransition(format!(
                "branch {} is {:?}",
                branch.id, branch.status
            )));
        }
        let rid = self
            .rounds
            .values()
            .find(|r| r.branch_id == branch.id && r.status == RoundStatus::Open)
            .map(|r| r.id.clone())
            .ok_or_else(|| Error::InvalidTransition("no open round to remove seats".into()))?;
        let mut out = Vec::new();
        for tid in task_ids {
            let t = self
                .tasks
                .get(tid)
                .cloned()
                .ok_or_else(|| Error::NotFound(tid.clone()))?;
            if t.round_id != rid {
                return Err(Error::InvalidArgument(format!(
                    "task {} is not in the open round",
                    t.id
                )));
            }
            if !t.status.is_open() {
                return Err(Error::InvalidTransition(format!(
                    "task {} already decided",
                    t.id
                )));
            }
            let from = t.status;
            let t = self.tasks.get_mut(tid).unwrap();
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
    ) -> Result<Vec<Effect>, Error> {
        self.guard(expected)?;
        let mut t = self
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| Error::NotFound(task_id.into()))?;
        if !t.status.is_open() {
            return Err(Error::InvalidTransition(format!(
                "task {} is {:?}",
                t.id, t.status
            )));
        }
        if t.assignee.is_some() {
            return Err(Error::InvalidTransition(format!(
                "task {} already assigned",
                t.id
            )));
        }
        if !t.candidates.iter().any(|c| c == principal) {
            return Err(Error::NotAuthorized(format!(
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

    pub fn approve(
        &mut self,
        expected: u64,
        task_id: &str,
        actor: &str,
        comment: Option<&str>,
        now: i64,
    ) -> Result<Vec<Effect>, Error> {
        self.vote(expected, task_id, actor, Decision::Approve, comment, now)
    }

    pub fn reject(
        &mut self,
        expected: u64,
        task_id: &str,
        actor: &str,
        comment: Option<&str>,
        now: i64,
    ) -> Result<Vec<Effect>, Error> {
        self.vote(expected, task_id, actor, Decision::Reject, comment, now)
    }

    /// 阅签：只读确认，不改变轮次计数（§10 阅签）。
    pub fn acknowledge(
        &mut self,
        expected: u64,
        task_id: &str,
        actor: &str,
        now: i64,
    ) -> Result<Vec<Effect>, Error> {
        self.guard(expected)?;
        let mut t = self
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| Error::NotFound(task_id.into()))?;
        if t.kind != TaskKind::ReadAck {
            return Err(Error::InvalidTransition(format!(
                "task {} is not a read-ack task",
                t.id
            )));
        }
        if !t.status.is_open() {
            return Err(Error::InvalidTransition(format!(
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

    /// 转办：责任人变更；原审批人保留审计标记但不再审批（临时口径，待 EIS 确认）。
    pub fn transfer(
        &mut self,
        expected: u64,
        task_id: &str,
        to: &str,
        by: &str,
        now: i64,
    ) -> Result<Vec<Effect>, Error> {
        self.guard(expected)?;
        let mut t = self
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| Error::NotFound(task_id.into()))?;
        if !t.status.is_open() {
            return Err(Error::InvalidTransition(format!(
                "task {} is {:?}",
                t.id, t.status
            )));
        }
        if t.kind != TaskKind::Approval {
            return Err(Error::InvalidTransition(
                "only approval tasks can be transferred".into(),
            ));
        }
        let from_principal = match &t.assignee {
            Some(a) if a == by => a.clone(),
            Some(a) => {
                return Err(Error::NotAuthorized(format!(
                    "{by} is not the assignee ({a}) of {}",
                    t.id
                )))
            }
            None => {
                if !t.candidates.iter().any(|c| c == by) {
                    return Err(Error::NotAuthorized(format!(
                        "{by} is not a candidate of {}",
                        t.id
                    )));
                }
                by.into()
            }
        };
        t.transferred_from.push(from_principal);
        t.assignee = Some(to.into());
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
    pub fn delegate(
        &mut self,
        expected: u64,
        task_id: &str,
        to: &str,
        by: &str,
        now: i64,
    ) -> Result<Vec<Effect>, Error> {
        self.guard(expected)?;
        let mut t = self
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| Error::NotFound(task_id.into()))?;
        if !t.status.is_open() {
            return Err(Error::InvalidTransition(format!(
                "task {} is {:?}",
                t.id, t.status
            )));
        }
        match &t.assignee {
            Some(a) if a == by => {}
            _ => {
                return Err(Error::NotAuthorized(format!(
                    "only the assignee can delegate task {}",
                    t.id
                )))
            }
        }
        t.delegation = Some(Delegation {
            from: by.into(),
            to: to.into(),
            granted_at: now,
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

    /// 驳回至节点：关闭当前节点（Returned），作废目标节点旧尝试，重开新尝试。
    /// 目标必须在已进入节点白名单内；累计次数受 `MAX_JUMPS` 限制。
    pub fn return_to_node(
        &mut self,
        expected: u64,
        branch_id: &str,
        target_node: &str,
        by: &str,
        comment: Option<&str>,
        now: i64,
    ) -> Result<(Id, Vec<Effect>), Error> {
        self.guard(expected)?;
        let branch = self
            .branches
            .get(branch_id)
            .cloned()
            .ok_or_else(|| Error::NotFound(branch_id.into()))?;
        if branch.status != BranchStatus::Open {
            return Err(Error::InvalidTransition(format!(
                "branch {} is {:?}",
                branch.id, branch.status
            )));
        }
        if !self.node_attempts.contains_key(target_node) {
            return Err(Error::InvalidArgument(format!(
                "target node {target_node} is not in the reached-node whitelist"
            )));
        }
        if self.instance.jump_count >= MAX_JUMPS {
            return Err(Error::InvalidArgument(format!(
                "jump limit {MAX_JUMPS} reached"
            )));
        }
        let mut out = Vec::new();
        let open_round = self
            .rounds
            .values()
            .find(|r| r.branch_id == branch.id && r.status == RoundStatus::Open)
            .map(|r| r.id.clone());
        if let Some(rid) = &open_round {
            self.cancel_pending_tasks(rid, now, &mut out);
        }
        self.close_rounds_of_branch(&branch.id, RoundStatus::Superseded, now, &mut out);
        self.set_branch_status(&branch.id, BranchStatus::Returned, now, &mut out);

        let target_latest = self
            .branches
            .values()
            .filter(|b| b.node == target_node)
            .max_by_key(|b| b.attempt)
            .cloned();
        let (join, reject) = target_latest
            .as_ref()
            .map(|b| (b.join, b.reject))
            .unwrap_or((JoinPolicy::All, RejectPolicy::AnyReject));
        if let Some(tb) = &target_latest {
            self.set_branch_status(&tb.id, BranchStatus::Superseded, now, &mut out);
        }
        let new_id = self.create_branch(
            target_node,
            target_latest
                .as_ref()
                .and_then(|b| b.parent_branch_id.clone()),
            join,
            reject,
            now,
            &mut out,
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
            detail: format!("{} -> {}", branch.node, target_node),
        });
        let jump_count = self.instance.jump_count;
        self.emit(
            &mut out,
            "com.china-workflow.instance.returned",
            json!({
                "instance_id": self.instance.id,
                "from_node": branch.node,
                "to_node": target_node,
                "jump_count": jump_count,
            }),
        );
        self.touch(now);
        Ok((new_id, out))
    }

    /// 撤回（发起人）。
    pub fn withdraw(&mut self, expected: u64, by: &str, now: i64) -> Result<Vec<Effect>, Error> {
        self.guard(expected)?;
        if self.instance.initiator != by {
            return Err(Error::NotAuthorized(format!("{by} is not the initiator")));
        }
        let mut out = Vec::new();
        self.cancel_open_branches(BranchStatus::Cancelled, &mut out, now);
        let from = self.instance.status;
        self.instance.status = InstanceStatus::Cancelled;
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
        });
        self.emit(
            &mut out,
            "com.china-workflow.instance.cancelled",
            json!({"instance_id": self.instance.id}),
        );
        self.touch(now);
        Ok(out)
    }

    /// 强制终止（管理路径；权限规则不在本原型范围，见 GAPS.md Q6）。
    pub fn terminate(
        &mut self,
        expected: u64,
        by: &str,
        reason: &str,
        now: i64,
    ) -> Result<Vec<Effect>, Error> {
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
        });
        self.emit(
            &mut out,
            "com.china-workflow.instance.terminated",
            json!({"instance_id": self.instance.id, "reason": reason}),
        );
        self.touch(now);
        Ok(out)
    }

    /// 定义层已无后继节点时，由驱动方显式收口实例（分支通过与实例收口解耦：
    /// 线性原型无法从状态机内部得知定义是否还有后继节点）。
    pub fn finish(&mut self, expected: u64, now: i64) -> Result<Vec<Effect>, Error> {
        self.guard(expected)?;
        if self
            .branches
            .values()
            .any(|b| b.status == BranchStatus::Open)
        {
            return Err(Error::InvalidTransition("open branch remains".into()));
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
                return Err(Error::InvalidTransition(
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

    /// 超时（由外部调度器驱动；本原型不做进程内定时器——见 GAPS.md G4）。
    /// 过期审批席位不计票；超时的最终处置策略（自动通过/驳回/升级）未定，见 GAPS.md Q5。
    pub fn expire(&mut self, expected: u64, task_id: &str, now: i64) -> Result<Vec<Effect>, Error> {
        self.guard(expected)?;
        let t = self
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| Error::NotFound(task_id.into()))?;
        if !t.status.is_open() {
            return Err(Error::InvalidTransition(format!(
                "task {} is {:?}",
                t.id, t.status
            )));
        }
        let d = t
            .deadline
            .ok_or_else(|| Error::InvalidArgument(format!("task {} has no deadline", t.id)))?;
        if now < d {
            return Err(Error::InvalidArgument(format!(
                "task {} deadline {d} not reached at {now}",
                t.id
            )));
        }
        let from = t.status;
        {
            let t = self.tasks.get_mut(task_id).unwrap();
            t.status = TaskStatus::Expired;
            t.updated_at = now;
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

    /// 三级作用域求值：task > branch > instance（§5 变量作用域）。
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

    /// 待办查询：某人当前可处理的席位（EIS/ICPS 待办列表的模型侧投影）。
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

    pub fn snapshot_json(&self) -> Value {
        json!({
            "instance": serde_json::to_value(&self.instance).unwrap(),
            "branches": serde_json::to_value(&self.branches).unwrap(),
            "tasks": serde_json::to_value(&self.tasks).unwrap(),
            "rounds": serde_json::to_value(&self.rounds).unwrap(),
        })
    }

    // ------------------------------------------------------------ 内部实现

    fn gen(&mut self, prefix: &str) -> Id {
        self.seq += 1;
        format!("{prefix}-{:04}", self.seq)
    }

    fn touch(&mut self, now: i64) {
        self.instance.revision += 1;
        self.instance.updated_at = now;
    }

    fn guard(&self, expected: u64) -> Result<(), Error> {
        if self.instance.revision != expected {
            return Err(Error::StaleRevision {
                expected,
                actual: self.instance.revision,
            });
        }
        if self.instance.status.is_terminal() {
            return Err(Error::InvalidTransition(format!(
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

    fn actor_can_act(&self, t: &Task, actor: &str) -> Result<Option<Principal>, Error> {
        if let Some(d) = &t.delegation {
            if d.to == actor {
                return Ok(Some(d.from.clone()));
            }
        }
        match &t.assignee {
            Some(a) if a == actor => Ok(None),
            Some(_) => Err(Error::NotAuthorized(format!(
                "task {} is assigned to another",
                t.id
            ))),
            None => {
                if t.candidates.iter().any(|c| c == actor) {
                    Ok(None)
                } else {
                    Err(Error::NotAuthorized(format!(
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
    ) -> Result<Vec<Effect>, Error> {
        self.guard(expected)?;
        let mut t = self
            .tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| Error::NotFound(task_id.into()))?;
        if t.kind != TaskKind::Approval {
            return Err(Error::InvalidTransition(format!(
                "task {} is not an approval task",
                t.id
            )));
        }
        if !t.status.is_open() {
            return Err(Error::InvalidTransition(format!(
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

    /// 轮次统计与判定。会签/或签/定额/比例、一票否决、耗尽判定都在这里收口。
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

    /// 通过/驳回的收口：关闭分支，并在根分支关闭且无其他 Open 分支时决定实例状态。
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
        // 分支通过不自动收口实例：线性原型无法从状态机内部得知定义是否还有后继节点，
        // 由驱动方在定义走到末端时调用 `finish`。分支否决是终局，直接收口实例。
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

    /// 非判定性关闭（Returned / Superseded / Cancelled / Terminated）：不触发实例收口。
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
            .filter(|r| r.branch_id == branch_id && r.status == RoundStatus::Open)
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
            let task_ids: Vec<Id> = self
                .tasks
                .values()
                .filter(|t| t.branch_id == *bid && t.status.is_open())
                .map(|t| t.id.clone())
                .collect();
            self.cancel_tasks(&task_ids, now, out);
            self.close_rounds_of_branch(bid, RoundStatus::Cancelled, now, out);
            self.set_branch_status(bid, branch_status, now, out);
        }
    }
}
