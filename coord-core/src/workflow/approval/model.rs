//! v2 审批实体与枚举。
//!
//! 字段清单与不变量见 `spike/2026-10/spike-1-state-model/docs/MODEL.md`，状态转换规则见
//! 同目录的 `TRANSITIONS.md`；当前对外语义口径收敛在 `docs/production/workflow-semantics.md`。
//! 本模块只定义数据：不读时钟、不生成 ID、不做 I/O——时间戳与标识由调用方注入。

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// 实例 / 分支 / 任务 / 轮次的内部标识。
pub type Id = String;

/// 审批主体标识（发起人、审批人、委派人与被委派人）。
pub type Principal = String;

/// 三级变量作用域（task > branch > instance）的存储形态。
pub type Scope = BTreeMap<String, Value>;

/// v2 审批实例的引擎版本标识，实例启动时钉入 [`Instance::engine_version`]。
///
/// 同一实例不允许被其他版本驱动（见 ADR-0013 D1/D6）：驱动方按它区分 v1/v2 语义。
pub const APPROVAL_ENGINE_VERSION: &str = "v2";

// ---------------------------------------------------------------- Instance

/// 实例状态。终态 = `Completed` / `Rejected` / `Terminated` / `Cancelled` / `Faulted`，
/// 终态实例拒绝一切变更命令（见 [`InstanceStatus::is_terminal`]）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstanceStatus {
    /// 已创建、尚未进入任何节点。
    Pending,
    /// 已进入首个节点（存在 open 分支）。
    Running,
    /// 由 `finish` 显式收口；分支通过**不**自动收口实例。
    Completed,
    /// 审批否决即终局（一票否决 / 耗尽不通过）。
    Rejected,
    /// 管理路径终止。
    Terminated,
    /// 发起人撤回。
    Cancelled,
    /// 终态之一；命令路径不产生该状态。
    Faulted,
}

impl InstanceStatus {
    /// 终态判定：终态实例拒绝一切变更命令。
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

/// 流程实例（v2 聚合根）。
///
/// 不变量：`revision` 每次状态变更 +1，命令以它做乐观并发守卫（CAS）；
/// `root_branch_id` 指向首个分支；`jump_count` 为驳回至节点的累计次数。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Instance {
    pub id: Id,
    pub definition_ns: String,
    pub definition_name: String,
    pub definition_version: String,
    /// 实例启动时钉死的定义内容哈希。
    ///
    /// 漂移校验在加载定义的驱动层做（本模块只持有值）：同 `(ns, name, version)` 的
    /// 内容变化 = 新 hash，不覆盖（见 ADR-0013 D6）。
    pub definition_hash: String,
    /// 驱动本实例的引擎版本，取值 [`APPROVAL_ENGINE_VERSION`]。
    ///
    /// v2 实例不允许被其他版本驱动（见 ADR-0013 D1）。
    pub engine_version: String,
    pub initiator: Principal,
    pub status: InstanceStatus,
    /// 实例级变量（作用域第三级）。
    pub context: Scope,
    pub root_branch_id: Option<Id>,
    /// 每次状态变更 +1；命令以它做乐观并发守卫（CAS）。
    pub revision: u64,
    /// 驳回至节点的累计次数（防驳回循环；达到
    /// [`MAX_JUMPS`](crate::workflow::approval::engine::MAX_JUMPS) 即拒绝再次驳回）。
    pub jump_count: u32,
    pub audit: Vec<AuditEntry>,
    pub returns: Vec<ReturnRecord>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// 管理动作审计记录（进入节点 / 加签 / 减签 / 驳回 / 转办 / 委派 / 撤回 / 终止）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuditEntry {
    pub at: i64,
    pub action: String,
    pub branch: Option<Id>,
    pub by: Principal,
    pub detail: String,
}

/// 驳回至节点的记录：从 `from_node` 驳回到 `to_node`。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReturnRecord {
    pub from_node: String,
    pub to_node: String,
    pub by: Principal,
    pub comment: Option<String>,
    pub at: i64,
}

// ------------------------------------------------------------------ Branch

/// 分支状态（分支 = 节点的运行实例）。
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

/// 分支的通过判定策略。
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

/// 分支的不通过判定策略。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectPolicy {
    /// 一票否决。
    AnyReject,
    /// 全部投完仍未达通过条件才判不通过。
    OnExhausted,
}

/// 分支：一个节点的一次运行实例。
///
/// 不变量（v2.0 线性模型）：同一实例同时最多一个 `Open` 分支。
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
    /// 关闭时间只写一次（作废不回填）。
    pub closed_at: Option<i64>,
}

// -------------------------------------------------------------------- Task

/// 席位类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskKind {
    /// 审批席位，计入通过/否决统计。
    Approval,
    /// 阅签席位，只读，不影响流程。
    ReadAck,
}

/// 席位状态。
///
/// `Completed` / `Rejected` / `Cancelled` / `Expired` 为终态，投票只允许在
/// `Created` / `Claimed` 上发起（见 [`TaskStatus::is_open`]）。
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
    /// 未决判定：只有未决席位可被投票 / 指派 / 减签。
    pub fn is_open(self) -> bool {
        matches!(self, TaskStatus::Created | TaskStatus::Claimed)
    }
}

/// 委派凭据：被委派人与原审批人均可批（原审批人权限保留）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Delegation {
    pub from: Principal,
    pub to: Principal,
    pub granted_at: i64,
}

/// 票型（无弃权票）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Decision {
    Approve,
    Reject,
}

/// 投票记录（内嵌于 [`Task::votes`]；轮次只保留计数）。
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

/// 席位（审批任务）。
///
/// 不变量：投票仅限未决席位；转办 / 委派仅限未决席位；委派只允许当前
/// `assignee` 发起；`candidates` 在 `assignee = None` 时供池内人认领或直接投票
/// （隐式认领）。
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
    /// 超时阈值；过期由外部调用 `expire` 驱动（无进程内定时器）。
    pub deadline: Option<i64>,
    /// 委派：原审批人保留权限，代办人可代批。
    pub delegation: Option<Delegation>,
    /// 转办来源链（审计）；转办后原审批人不再具备审批权（见 ADR-0013 D2）。
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
    /// 指定审批人席位。
    pub fn approver(p: &str) -> Self {
        Self {
            assignee: Some(p.into()),
            candidates: vec![],
            kind: TaskKind::Approval,
        }
    }

    /// 阅签席位（不计票）。
    pub fn reader(p: &str) -> Self {
        Self {
            assignee: Some(p.into()),
            candidates: vec![],
            kind: TaskKind::ReadAck,
        }
    }

    /// 候选池席位：认领或直接投票时隐式认领。
    pub fn pool(candidates: Vec<Principal>) -> Self {
        Self {
            assignee: None,
            candidates,
            kind: TaskKind::Approval,
        }
    }
}

// ------------------------------------------------------------------- Round

/// 轮次状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoundStatus {
    Open,
    Passed,
    Rejected,
    /// 驳回重开而作废。
    Superseded,
    Cancelled,
}

/// 轮次：节点实例的一次投票周期。
///
/// 作用域为**分支**而非单个 Task —— 会签是「一个节点 N 席位」的投票，挂单 task
/// 无法同时统计多席位。
///
/// 不变量：一个分支同时最多一个 `Open` 轮次；`total_count = 0`（只有阅签）即视为通过；
/// 轮次不重开（驳回重开的是新分支新轮次）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Round {
    pub id: Id,
    pub branch_id: Id,
    /// 分支内轮次序号，从 1 起。
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

/// 责任人变更的成因（指派 / 转办 / 委派）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssignKind {
    Claim,
    Transfer,
    Delegate,
}

/// 命令产生的状态变化与待投递事件。
///
/// 本模块只产出数据：事件的落盘与投递由 outbox 事务负责（见 ADR-0013 D4）。
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
    /// 待投递事件草案；事件目录与正式契约由契约方固化。
    Event {
        name: String,
        data: Value,
    },
}

// ------------------------------------------------------------------- Error

/// v2 审批命令错误。
///
/// 变体与 `Display` 文案是调用方与测试定位失败对象的锚点（命令路径不 panic），
/// 逐字取自 Spike 1 状态模型（证据 `spike/2026-10/spike-1-state-model/`）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalError {
    NotFound(String),
    InvalidTransition(String),
    NotAuthorized(String),
    InvalidArgument(String),
    StaleRevision {
        expected: u64,
        actual: u64,
    },
    /// 快照等纯序列化失败（不改变状态，调用方拿不到半份结果）。
    ///
    /// 生产代码里序列化失败必须走返回错误（证据快照 `snapshot_json`），不是 panic。
    SerializationError(String),
}

impl std::fmt::Display for ApprovalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApprovalError::NotFound(x) => write!(f, "not found: {x}"),
            ApprovalError::InvalidTransition(x) => write!(f, "invalid transition: {x}"),
            ApprovalError::NotAuthorized(x) => write!(f, "not authorized: {x}"),
            ApprovalError::InvalidArgument(x) => write!(f, "invalid argument: {x}"),
            ApprovalError::StaleRevision { expected, actual } => {
                write!(f, "stale revision: expected {expected}, actual {actual}")
            }
            ApprovalError::SerializationError(msg) => {
                write!(f, "serialization error: {msg}")
            }
        }
    }
}

impl std::error::Error for ApprovalError {}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::de::DeserializeOwned;
    use serde_json::json;

    fn roundtrip<T>(value: T) -> T
    where
        T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        let encoded = serde_json::to_value(&value).expect("serialize");
        serde_json::from_value(encoded).expect("deserialize")
    }

    fn full_instance() -> Instance {
        Instance {
            id: "inst-1".into(),
            definition_ns: "default".into(),
            definition_name: "leave-approval".into(),
            definition_version: "1.0.0".into(),
            definition_hash: "sha256:abc".into(),
            engine_version: APPROVAL_ENGINE_VERSION.into(),
            initiator: "u-initiator".into(),
            status: InstanceStatus::Rejected,
            context: BTreeMap::from([("amount".to_string(), json!(3))]),
            root_branch_id: Some("inst-1:b0000".into()),
            revision: 7,
            jump_count: 1,
            audit: vec![AuditEntry {
                at: 100,
                action: "enter".into(),
                branch: Some("inst-1:b0000".into()),
                by: "u-initiator".into(),
                detail: "root".into(),
            }],
            returns: vec![ReturnRecord {
                from_node: "manager".into(),
                to_node: "apply".into(),
                by: "u-manager".into(),
                comment: Some("缺少附件".into()),
                at: 200,
            }],
            created_at: 100,
            updated_at: 300,
        }
    }

    fn full_branch() -> Branch {
        Branch {
            id: "inst-1:b0001".into(),
            instance_id: "inst-1".into(),
            parent_branch_id: Some("inst-1:b0000".into()),
            node: "manager".into(),
            attempt: 2,
            status: BranchStatus::Returned,
            join: JoinPolicy::Ratio { num: 2, den: 3 },
            reject: RejectPolicy::OnExhausted,
            scope: BTreeMap::from([("level".to_string(), json!("l2"))]),
            round_ids: vec!["inst-1:r0002".into()],
            created_at: 120,
            closed_at: Some(200),
        }
    }

    fn full_task() -> Task {
        Task {
            id: "inst-1:t0003".into(),
            branch_id: "inst-1:b0001".into(),
            round_id: "inst-1:r0002".into(),
            kind: TaskKind::ReadAck,
            status: TaskStatus::Expired,
            assignee: Some("u-approver".into()),
            candidates: vec!["u-a".into(), "u-b".into()],
            deadline: Some(300),
            delegation: Some(Delegation {
                from: "u-approver".into(),
                to: "u-proxy".into(),
                granted_at: 150,
            }),
            transferred_from: vec!["u-original".into()],
            votes: vec![VoteRecord {
                round_id: "inst-1:r0002".into(),
                decision: Decision::Reject,
                actor: "u-proxy".into(),
                on_behalf_of: Some("u-approver".into()),
                comment: Some("信息不足".into()),
                at: 180,
            }],
            scope: BTreeMap::from([("urgent".to_string(), json!(true))]),
            created_at: 130,
            updated_at: 180,
        }
    }

    fn full_round() -> Round {
        Round {
            id: "inst-1:r0002".into(),
            branch_id: "inst-1:b0001".into(),
            number: 2,
            status: RoundStatus::Superseded,
            total_count: 3,
            completed_count: 1,
            rejected_count: 1,
            opened_at: 130,
            closed_at: Some(210),
        }
    }

    #[test]
    fn test_model_serde_roundtrip_full_instance() {
        let original = full_instance();
        let back: Instance = roundtrip(original.clone());
        assert_eq!(back, original);

        let original = full_branch();
        let back: Branch = roundtrip(original.clone());
        assert_eq!(back, original);

        let original = full_task();
        let back: Task = roundtrip(original.clone());
        assert_eq!(back, original);

        let original = full_round();
        let back: Round = roundtrip(original.clone());
        assert_eq!(back, original);

        // 字段名逐字固定：JSON 键集合即 v2 实例文档的形状（少一个/多一个都是契约变更）。
        let value = serde_json::to_value(full_instance()).expect("serialize instance");
        let mut keys: Vec<&str> = value
            .as_object()
            .expect("instance is a JSON object")
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                "audit",
                "context",
                "created_at",
                "definition_hash",
                "definition_name",
                "definition_ns",
                "definition_version",
                "engine_version",
                "id",
                "initiator",
                "jump_count",
                "returns",
                "revision",
                "root_branch_id",
                "status",
                "updated_at",
            ]
        );

        // 全部状态枚举的每个变体都要无损往返。
        for status in [
            InstanceStatus::Pending,
            InstanceStatus::Running,
            InstanceStatus::Completed,
            InstanceStatus::Rejected,
            InstanceStatus::Terminated,
            InstanceStatus::Cancelled,
            InstanceStatus::Faulted,
        ] {
            assert_eq!(roundtrip(status), status);
        }
        for status in [
            BranchStatus::Open,
            BranchStatus::Completed,
            BranchStatus::Rejected,
            BranchStatus::Returned,
            BranchStatus::Superseded,
            BranchStatus::Cancelled,
            BranchStatus::Terminated,
        ] {
            assert_eq!(roundtrip(status), status);
        }
        for status in [
            TaskStatus::Created,
            TaskStatus::Claimed,
            TaskStatus::Completed,
            TaskStatus::Rejected,
            TaskStatus::Cancelled,
            TaskStatus::Expired,
        ] {
            assert_eq!(roundtrip(status), status);
        }
        for status in [
            RoundStatus::Open,
            RoundStatus::Passed,
            RoundStatus::Rejected,
            RoundStatus::Superseded,
            RoundStatus::Cancelled,
        ] {
            assert_eq!(roundtrip(status), status);
        }
        for join in [
            JoinPolicy::All,
            JoinPolicy::Any,
            JoinPolicy::Quorum(2),
            JoinPolicy::Ratio { num: 2, den: 3 },
        ] {
            assert_eq!(roundtrip(join), join);
        }
        for reject in [RejectPolicy::AnyReject, RejectPolicy::OnExhausted] {
            assert_eq!(roundtrip(reject), reject);
        }
        for kind in [TaskKind::Approval, TaskKind::ReadAck] {
            assert_eq!(roundtrip(kind), kind);
        }
        for decision in [Decision::Approve, Decision::Reject] {
            assert_eq!(roundtrip(decision), decision);
        }
        for kind in [
            AssignKind::Claim,
            AssignKind::Transfer,
            AssignKind::Delegate,
        ] {
            assert_eq!(roundtrip(kind), kind);
        }
    }

    #[test]
    fn test_enum_serde_names_are_snake_case() {
        assert_eq!(json!(InstanceStatus::Rejected), json!("rejected"));
        assert_eq!(json!(JoinPolicy::Quorum(2)), json!({"quorum": 2}));
        assert_eq!(
            json!(JoinPolicy::Ratio { num: 2, den: 3 }),
            json!({"ratio": {"num": 2, "den": 3}})
        );
        assert_eq!(json!(RejectPolicy::AnyReject), json!("any_reject"));
        assert_eq!(json!(TaskKind::ReadAck), json!("read_ack"));
        assert_eq!(
            json!(Effect::TaskAssigned {
                id: "inst-1:t0003".into(),
                assignee: "u-proxy".into(),
                kind: AssignKind::Transfer,
            }),
            json!({
                "type": "task_assigned",
                "id": "inst-1:t0003",
                "assignee": "u-proxy",
                "kind": "transfer",
            })
        );
    }

    #[test]
    fn test_effect_event_serde_shape() {
        let effect = Effect::Event {
            name: "com.china-workflow.approval.round.passed".into(),
            data: json!({"a": 1}),
        };
        assert_eq!(
            serde_json::to_value(&effect).expect("serialize effect"),
            json!({
                "type": "event",
                "name": "com.china-workflow.approval.round.passed",
                "data": {"a": 1},
            })
        );
        assert_eq!(roundtrip(effect.clone()), effect);
    }

    #[test]
    fn test_approval_engine_version_pinned() {
        assert_eq!(APPROVAL_ENGINE_VERSION, "v2");
    }

    #[test]
    fn test_approval_error_variants_and_display_are_verbatim() {
        // Display 文案是调用方与测试定位失败对象的锚点，逐字固定。
        assert_eq!(
            ApprovalError::NotFound("inst-1:t0003".into()).to_string(),
            "not found: inst-1:t0003"
        );
        assert_eq!(
            ApprovalError::InvalidTransition("open branch remains".into()).to_string(),
            "invalid transition: open branch remains"
        );
        assert_eq!(
            ApprovalError::NotAuthorized("u-x is not the initiator".into()).to_string(),
            "not authorized: u-x is not the initiator"
        );
        assert_eq!(
            ApprovalError::InvalidArgument("id".into()).to_string(),
            "invalid argument: id"
        );
        assert_eq!(
            ApprovalError::StaleRevision {
                expected: 3,
                actual: 4,
            }
            .to_string(),
            "stale revision: expected 3, actual 4"
        );

        assert_eq!(
            serde_json::to_value(ApprovalError::NotFound("inst-1:t0003".into()))
                .expect("serialize error"),
            json!({"not_found": "inst-1:t0003"})
        );
        assert_eq!(
            serde_json::to_value(ApprovalError::StaleRevision {
                expected: 3,
                actual: 4,
            })
            .expect("serialize error"),
            json!({"stale_revision": {"expected": 3, "actual": 4}})
        );
        assert_eq!(
            ApprovalError::SerializationError("bad snapshot".into()).to_string(),
            "serialization error: bad snapshot"
        );
        assert_eq!(
            serde_json::to_value(ApprovalError::SerializationError("bad snapshot".into()))
                .expect("serialize error"),
            json!({"serialization_error": "bad snapshot"})
        );
        let as_std_error: &dyn std::error::Error = &ApprovalError::InvalidArgument("id".into());
        assert_eq!(as_std_error.to_string(), "invalid argument: id");
    }
}
