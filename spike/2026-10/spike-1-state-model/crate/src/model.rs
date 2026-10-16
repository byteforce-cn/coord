//! 实体与枚举。字段清单与不变量见 `../docs/MODEL.md`；转换规则见 `../docs/TRANSITIONS.md`。

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub type Id = String;
pub type Principal = String;

/// 三级变量作用域（task > branch > instance）的存储形态。
pub type Scope = BTreeMap<String, Value>;

// ---------------------------------------------------------------- Instance

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstanceStatus {
    Pending,
    Running,
    Completed,
    Rejected,
    Terminated,
    Cancelled,
    Faulted,
}

impl InstanceStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            InstanceStatus::Completed
                | InstanceStatus::Rejected
                | InstanceStatus::Terminated
                | InstanceStatus::Cancelled
                | InstanceStatus::Faulted
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Instance {
    pub id: Id,
    pub definition_ns: String,
    pub definition_name: String,
    pub definition_version: String,
    pub initiator: Principal,
    pub status: InstanceStatus,
    /// 实例级变量（作用域第三级）。
    pub context: Scope,
    pub root_branch_id: Option<Id>,
    /// 每次状态变更 +1；命令以它做乐观并发守卫（CAS）。
    pub revision: u64,
    /// 驳回至节点的累计次数（防循环，见 §8 jump_count）。
    pub jump_count: u32,
    pub audit: Vec<AuditEntry>,
    pub returns: Vec<ReturnRecord>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuditEntry {
    pub at: i64,
    pub action: String,
    pub branch: Option<Id>,
    pub by: Principal,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReturnRecord {
    pub from_node: String,
    pub to_node: String,
    pub by: Principal,
    pub comment: Option<String>,
    pub at: i64,
}

// ------------------------------------------------------------------ Branch

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BranchStatus {
    Open,
    Completed,
    /// 审批被一票否决 / 汇总不通过。
    Rejected,
    /// 驳回至节点主动关闭当前节点。
    Returned,
    /// 被驳回至节点重开而作废（非业务结果）。
    Superseded,
    Cancelled,
    Terminated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinPolicy {
    /// 会签：全部通过。
    All,
    /// 或签：任一通过。
    Any,
    /// 定额：通过数达到 n。
    Quorum(u32),
    /// 票签：通过比例达到 num/den（含等号）。
    Ratio { num: u32, den: u32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectPolicy {
    /// 一票否决。
    AnyReject,
    /// 全部投完仍未达通过条件才判不通过。
    OnExhausted,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Branch {
    pub id: Id,
    pub instance_id: Id,
    pub parent_branch_id: Option<Id>,
    /// 定义中的节点名（`do` 任务名）；驳回白名单以此为键。
    pub node: String,
    /// 第几次进入该节点（驳回重开递增）。
    pub attempt: u32,
    pub status: BranchStatus,
    pub join: JoinPolicy,
    pub reject: RejectPolicy,
    /// 分支级变量（作用域第二级）。
    pub scope: Scope,
    pub round_ids: Vec<Id>,
    pub created_at: i64,
    pub closed_at: Option<i64>,
}

// -------------------------------------------------------------------- Task

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskKind {
    /// 审批席位，计入通过/否决统计。
    Approval,
    /// 阅签席位，只读，不影响流程。
    ReadAck,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Created,
    Claimed,
    Completed,
    Rejected,
    Cancelled,
    Expired,
}

impl TaskStatus {
    pub fn is_open(self) -> bool {
        matches!(self, TaskStatus::Created | TaskStatus::Claimed)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Delegation {
    pub from: Principal,
    pub to: Principal,
    pub granted_at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Decision {
    Approve,
    Reject,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VoteRecord {
    pub round_id: Id,
    pub decision: Decision,
    pub actor: Principal,
    /// 委派场景下记录被代表人；直接投票为 None。
    pub on_behalf_of: Option<Principal>,
    pub comment: Option<String>,
    pub at: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Task {
    pub id: Id,
    pub branch_id: Id,
    /// 当前轮次；驳回重开后新轮次的席位是新 Task（历史 Task 保留审计）。
    pub round_id: Id,
    pub kind: TaskKind,
    pub status: TaskStatus,
    /// 直接指派或认领后的唯一责任人。
    pub assignee: Option<Principal>,
    /// 候选人池（未指派时可用）。
    pub candidates: Vec<Principal>,
    pub deadline: Option<i64>,
    /// 委派：原审批人保留权限，代办人可代批。
    pub delegation: Option<Delegation>,
    /// 转办来源链（审计；转办后原审批人不再具备审批权——临时口径，待 EIS 确认）。
    pub transferred_from: Vec<Principal>,
    pub votes: Vec<VoteRecord>,
    /// 任务级变量（作用域第一级）。
    pub scope: Scope,
    pub created_at: i64,
    pub updated_at: i64,
}

/// `open_approval` / `add_sign` 的席位描述。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SeatSpec {
    pub assignee: Option<Principal>,
    pub candidates: Vec<Principal>,
    pub kind: TaskKind,
}

impl SeatSpec {
    pub fn approver(p: &str) -> Self {
        Self {
            assignee: Some(p.into()),
            candidates: vec![],
            kind: TaskKind::Approval,
        }
    }
    pub fn reader(p: &str) -> Self {
        Self {
            assignee: Some(p.into()),
            candidates: vec![],
            kind: TaskKind::ReadAck,
        }
    }
    pub fn pool(candidates: Vec<Principal>) -> Self {
        Self {
            assignee: None,
            candidates,
            kind: TaskKind::Approval,
        }
    }
}

// ------------------------------------------------------------------- Round

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoundStatus {
    Open,
    Passed,
    Rejected,
    Superseded,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Round {
    pub id: Id,
    /// 轮次作用域为分支（一个节点实例）；不为单个 Task —— 见 GAPS.md R1。
    pub branch_id: Id,
    pub number: u32,
    pub status: RoundStatus,
    /// 可计票审批席位数（不含阅签；不含已取消/过期的席位）。
    pub total_count: u32,
    pub completed_count: u32,
    pub rejected_count: u32,
    pub opened_at: i64,
    pub closed_at: Option<i64>,
}

// ------------------------------------------------------------------ Effect

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssignKind {
    Claim,
    Transfer,
    Delegate,
}

/// 命令产生的状态变化与待投递事件（Outbox 语义在 Spike 3 验证，这里只产出数据）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Effect {
    InstanceStatus {
        from: InstanceStatus,
        to: InstanceStatus,
    },
    BranchCreated {
        id: Id,
        node: String,
        attempt: u32,
    },
    BranchStatus {
        id: Id,
        from: BranchStatus,
        to: BranchStatus,
    },
    TaskCreated {
        id: Id,
        branch: Id,
        round: Id,
        kind: TaskKind,
        assignee: Option<Principal>,
        candidates: Vec<Principal>,
    },
    TaskStatus {
        id: Id,
        from: TaskStatus,
        to: TaskStatus,
    },
    TaskAssigned {
        id: Id,
        assignee: Principal,
        kind: AssignKind,
    },
    RoundOpened {
        id: Id,
        branch: Id,
        number: u32,
    },
    RoundCounted {
        id: Id,
        total: u32,
        completed: u32,
        rejected: u32,
    },
    RoundStatus {
        id: Id,
        from: RoundStatus,
        to: RoundStatus,
    },
    Event {
        name: String,
        data: Value,
    },
}

// ------------------------------------------------------------------- Error

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Error {
    NotFound(String),
    InvalidTransition(String),
    NotAuthorized(String),
    InvalidArgument(String),
    StaleRevision { expected: u64, actual: u64 },
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NotFound(x) => write!(f, "not found: {x}"),
            Error::InvalidTransition(x) => write!(f, "invalid transition: {x}"),
            Error::NotAuthorized(x) => write!(f, "not authorized: {x}"),
            Error::InvalidArgument(x) => write!(f, "invalid argument: {x}"),
            Error::StaleRevision { expected, actual } => {
                write!(f, "stale revision: expected {expected}, actual {actual}")
            }
        }
    }
}

impl std::error::Error for Error {}
