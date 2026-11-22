//! v2 审批引擎的场景：核心生命周期 / 席位 / 计票判定，以及管理命令（驳回至节点 / 转办 /
//! 委派 / 撤回 / 终止）与变量作用域。
//!
//! 用例取自 Spike 1 的冻结场景文件（`spike/2026-10/spike-1-state-model/crate/tests/scenarios.rs`）：
//! 用例名与断言逐字保留，只把 `Engine::start(ns, name, version, initiator, context, now)` 改为
//! 调用方注入实例 id 与定义哈希的 `Engine::start(StartSpec { .. }, now)`。
//! 判定顺序见 `spike/2026-10/spike-1-state-model/docs/TRANSITIONS.md` §3。

use coord_core::workflow::approval::{engine::*, model::*};

/// 固定的实例 id：子实体 ID 前缀断言要逐字比对。
const INSTANCE_ID: &str = "inst-t2";

fn start() -> Engine {
    Engine::start(
        StartSpec {
            id: INSTANCE_ID.into(),
            definition_ns: "eis".into(),
            definition_name: "leave-approval".into(),
            definition_version: "1.0.0".into(),
            definition_hash: "sha256:t2".into(),
            initiator: "zhangsan".into(),
            initiator_acting_service: None,
            context: Scope::new(),
        },
        1_000,
    )
    .expect("start instance")
}

/// 推进注入时钟：引擎不读时钟，超时判定按参数 `now` 生效。
fn tick(t: &mut i64) -> i64 {
    *t += 100;
    *t
}

fn has_event(out: &[Effect], name: &str) -> bool {
    out.iter()
        .any(|e| matches!(e, Effect::Event { name: n, .. } if n == name))
}

fn find_task(e: &Engine, assignee: &str) -> Id {
    e.tasks
        .values()
        .find(|t| t.assignee.as_deref() == Some(assignee) && t.status.is_open())
        .unwrap_or_else(|| panic!("no open task for {assignee}"))
        .id
        .clone()
}

/// 用一个通过的单席位轮次关闭分支：`enter_branch` 要求同一时刻只有一个 Open 分支。
fn pass_branch(e: &mut Engine, branch: &str, principal: &str, now: &mut i64) {
    let (_, seats, _) = e
        .open_approval(
            e.instance.revision,
            branch,
            vec![SeatSpec::approver(principal)],
            None,
            tick(now),
        )
        .unwrap();
    e.approve(e.instance.revision, &seats[0], principal, None, tick(now))
        .unwrap();
}

// ---------------------------------------------------------------- P0 场景

/// 会签 + 加签：3 人会签全部通过后天，运行中追加 1 人，总票数 3 → 4。
#[test]
fn p0_huiqian_with_add_sign() {
    let mut e = start();
    let mut t = 1_000;
    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "apply",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (r, seats, _) = e
        .open_approval(
            e.instance.revision,
            &b,
            vec![
                SeatSpec::approver("alice"),
                SeatSpec::approver("bob"),
                SeatSpec::approver("carol"),
            ],
            None,
            tick(&mut t),
        )
        .unwrap();
    assert_eq!(seats.len(), 3);
    assert_eq!(e.rounds[&r].total_count, 3);

    let out = e
        .approve(
            e.instance.revision,
            &seats[0],
            "alice",
            Some("同意"),
            tick(&mut t),
        )
        .unwrap();
    assert!(out.iter().any(|x| matches!(
        x,
        Effect::RoundCounted {
            completed: 1,
            total: 3,
            ..
        }
    )));
    assert_eq!(e.rounds[&r].status, RoundStatus::Open);

    let (added, out) = e
        .add_sign(
            e.instance.revision,
            &b,
            vec![SeatSpec::approver("dave")],
            "admin",
            Some("追加分管领导"),
            tick(&mut t),
        )
        .unwrap();
    assert_eq!(added.len(), 1);
    assert!(out
        .iter()
        .any(|x| matches!(x, Effect::TaskCreated { assignee: Some(a), .. } if a == "dave")));
    assert_eq!(e.rounds[&r].total_count, 4);
    assert!(e
        .instance
        .audit
        .iter()
        .any(|a| a.action == "add_sign" && a.by == "admin"));

    e.approve(e.instance.revision, &seats[1], "bob", None, tick(&mut t))
        .unwrap();
    e.approve(e.instance.revision, &seats[2], "carol", None, tick(&mut t))
        .unwrap();
    assert_eq!(e.rounds[&r].status, RoundStatus::Open);
    assert_eq!(e.rounds[&r].completed_count, 3);

    let dave_task = find_task(&e, "dave");
    let out = e
        .approve(e.instance.revision, &dave_task, "dave", None, tick(&mut t))
        .unwrap();
    assert_eq!(e.rounds[&r].status, RoundStatus::Passed);
    assert_eq!(e.branches[&b].status, BranchStatus::Completed);
    assert!(has_event(
        &out,
        "com.china-workflow.approval.round.passed.v1"
    ));
    assert_eq!(e.instance.status, InstanceStatus::Running);
    let out = e.finish(e.instance.revision, tick(&mut t)).unwrap();
    assert_eq!(e.instance.status, InstanceStatus::Completed);
    assert!(has_event(&out, "com.china-workflow.instance.completed.v1"));
}

/// 一票否决：任一人拒绝 → 轮次/分支不通过，其余席位取消，实例 Rejected。
#[test]
fn p0_one_vote_veto() {
    let mut e = start();
    let mut t = 1_000;
    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "approve",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (r, seats, _) = e
        .open_approval(
            e.instance.revision,
            &b,
            vec![
                SeatSpec::approver("alice"),
                SeatSpec::approver("bob"),
                SeatSpec::approver("carol"),
            ],
            None,
            tick(&mut t),
        )
        .unwrap();
    e.approve(e.instance.revision, &seats[0], "alice", None, tick(&mut t))
        .unwrap();
    let out = e
        .reject(
            e.instance.revision,
            &seats[1],
            "bob",
            Some("不同意"),
            tick(&mut t),
        )
        .unwrap();

    assert_eq!(e.rounds[&r].status, RoundStatus::Rejected);
    assert_eq!(e.rounds[&r].rejected_count, 1);
    assert_eq!(e.tasks[&seats[2]].status, TaskStatus::Cancelled);
    assert_eq!(e.branches[&b].status, BranchStatus::Rejected);
    assert_eq!(e.instance.status, InstanceStatus::Rejected);
    assert!(has_event(
        &out,
        "com.china-workflow.approval.round.rejected.v1"
    ));
    assert!(has_event(&out, "com.china-workflow.instance.rejected.v1"));
}

/// 或签：任一人通过 → 其余席位取消，节点通过。
#[test]
fn p0_any_sign_cancels_rest() {
    let mut e = start();
    let mut t = 1_000;
    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "approve",
            None,
            JoinPolicy::Any,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (r, seats, _) = e
        .open_approval(
            e.instance.revision,
            &b,
            vec![
                SeatSpec::approver("alice"),
                SeatSpec::approver("bob"),
                SeatSpec::approver("carol"),
            ],
            None,
            tick(&mut t),
        )
        .unwrap();
    let out = e
        .approve(
            e.instance.revision,
            &seats[1],
            "bob",
            Some("同意"),
            tick(&mut t),
        )
        .unwrap();
    assert_eq!(e.rounds[&r].status, RoundStatus::Passed);
    assert_eq!(e.tasks[&seats[0]].status, TaskStatus::Cancelled);
    assert_eq!(e.tasks[&seats[2]].status, TaskStatus::Cancelled);
    assert_eq!(e.branches[&b].status, BranchStatus::Completed);
    assert_eq!(e.instance.status, InstanceStatus::Running);
    assert!(out.iter().any(|x| matches!(
        x,
        Effect::TaskStatus {
            to: TaskStatus::Cancelled,
            ..
        }
    )));
    e.finish(e.instance.revision, tick(&mut t)).unwrap();
    assert_eq!(e.instance.status, InstanceStatus::Completed);
}

/// 票签：达到比例即通过并取消余席；全部投完未达比例则拒绝。
#[test]
fn ratio_pass_then_exhausted_reject() {
    let mut e = start();
    let mut t = 1_000;
    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "vote",
            None,
            JoinPolicy::Ratio { num: 2, den: 3 },
            RejectPolicy::OnExhausted,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (r, seats, _) = e
        .open_approval(
            e.instance.revision,
            &b,
            vec![
                SeatSpec::approver("a"),
                SeatSpec::approver("b"),
                SeatSpec::approver("c"),
            ],
            None,
            tick(&mut t),
        )
        .unwrap();
    e.approve(e.instance.revision, &seats[0], "a", None, tick(&mut t))
        .unwrap();
    assert_eq!(e.rounds[&r].status, RoundStatus::Open);
    e.approve(e.instance.revision, &seats[1], "b", None, tick(&mut t))
        .unwrap();
    assert_eq!(e.rounds[&r].status, RoundStatus::Passed);
    assert_eq!(e.tasks[&seats[2]].status, TaskStatus::Cancelled);
    assert_eq!(e.instance.status, InstanceStatus::Running);
    e.finish(e.instance.revision, tick(&mut t)).unwrap();
    assert_eq!(e.instance.status, InstanceStatus::Completed);

    let mut e2 = start();
    let mut t2 = 1_000;
    let (b2, _) = e2
        .enter_branch(
            e2.instance.revision,
            "vote",
            None,
            JoinPolicy::Ratio { num: 2, den: 3 },
            RejectPolicy::OnExhausted,
            ApprovalPolicy::default(),
            tick(&mut t2),
        )
        .unwrap();
    let (r2, seats2, _) = e2
        .open_approval(
            e2.instance.revision,
            &b2,
            vec![
                SeatSpec::approver("a"),
                SeatSpec::approver("b"),
                SeatSpec::approver("c"),
            ],
            None,
            tick(&mut t2),
        )
        .unwrap();
    e2.reject(e2.instance.revision, &seats2[0], "a", None, tick(&mut t2))
        .unwrap();
    e2.reject(e2.instance.revision, &seats2[1], "b", None, tick(&mut t2))
        .unwrap();
    assert_eq!(e2.rounds[&r2].status, RoundStatus::Open);
    e2.approve(e2.instance.revision, &seats2[2], "c", None, tick(&mut t2))
        .unwrap();
    assert_eq!(e2.rounds[&r2].status, RoundStatus::Rejected);
    assert_eq!(e2.instance.status, InstanceStatus::Rejected);
}

/// 阅签：只读确认不改变计票；只有阅签席位的节点即刻通过。
#[test]
fn read_ack_does_not_gate_and_reader_only_branch_passes() {
    let mut e = start();
    let mut t = 1_000;
    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "approve",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (r, seats, _) = e
        .open_approval(
            e.instance.revision,
            &b,
            vec![SeatSpec::approver("alice"), SeatSpec::reader("bob")],
            None,
            tick(&mut t),
        )
        .unwrap();
    assert_eq!(e.rounds[&r].total_count, 1);
    e.acknowledge(e.instance.revision, &seats[1], "bob", tick(&mut t))
        .unwrap();
    assert_eq!(e.rounds[&r].status, RoundStatus::Open);
    assert_eq!(e.tasks[&seats[1]].status, TaskStatus::Completed);
    e.approve(e.instance.revision, &seats[0], "alice", None, tick(&mut t))
        .unwrap();
    assert_eq!(e.instance.status, InstanceStatus::Running);
    e.finish(e.instance.revision, tick(&mut t)).unwrap();
    assert_eq!(e.instance.status, InstanceStatus::Completed);

    let mut e2 = start();
    let mut t2 = 1_000;
    let (b2, _) = e2
        .enter_branch(
            e2.instance.revision,
            "fyi",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t2),
        )
        .unwrap();
    let (r2, _seats2, _) = e2
        .open_approval(
            e2.instance.revision,
            &b2,
            vec![SeatSpec::reader("bob")],
            None,
            tick(&mut t2),
        )
        .unwrap();
    assert_eq!(e2.rounds[&r2].status, RoundStatus::Passed);
    assert_eq!(e2.branches[&b2].status, BranchStatus::Completed);
    e2.finish(e2.instance.revision, tick(&mut t2)).unwrap();
    assert_eq!(e2.instance.status, InstanceStatus::Completed);
}

// ------------------------------------------------------------ 其余不变量

/// 乐观并发守卫：陈旧 revision 的写入被拒绝且不改状态。
#[test]
fn stale_revision_is_rejected() {
    let mut e = start();
    let mut t = 1_000;
    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "approve",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (_, seats, _) = e
        .open_approval(
            e.instance.revision,
            &b,
            vec![SeatSpec::approver("alice"), SeatSpec::approver("bob")],
            None,
            tick(&mut t),
        )
        .unwrap();
    let stale = e.instance.revision;
    e.approve(e.instance.revision, &seats[0], "alice", None, tick(&mut t))
        .unwrap();
    let err = e
        .approve(stale, &seats[1], "bob", None, tick(&mut t))
        .unwrap_err();
    assert!(matches!(err, ApprovalError::StaleRevision { .. }));
    assert_eq!(e.tasks[&seats[1]].status, TaskStatus::Created);
    assert_eq!(e.rounds.values().next().unwrap().completed_count, 1);
    e.approve(e.instance.revision, &seats[1], "bob", None, tick(&mut t))
        .unwrap();
    assert_eq!(e.instance.status, InstanceStatus::Running);
    e.finish(e.instance.revision, tick(&mut t)).unwrap();
    assert_eq!(e.instance.status, InstanceStatus::Completed);
}

/// 候选人池：认领/隐式认领；非候选人无权操作。
#[test]
fn pool_claim_rules() {
    let mut e = start();
    let mut t = 1_000;
    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "approve",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (_, seats, _) = e
        .open_approval(
            e.instance.revision,
            &b,
            vec![SeatSpec::pool(vec!["alice".into(), "bob".into()])],
            None,
            tick(&mut t),
        )
        .unwrap();
    let task = seats[0].clone();
    let err = e
        .claim(e.instance.revision, &task, "carol", tick(&mut t))
        .unwrap_err();
    assert!(matches!(err, ApprovalError::NotAuthorized(_)));
    e.claim(e.instance.revision, &task, "alice", tick(&mut t))
        .unwrap();
    let err = e
        .claim(e.instance.revision, &task, "bob", tick(&mut t))
        .unwrap_err();
    assert!(matches!(err, ApprovalError::InvalidTransition(_)));
    let err = e
        .approve(e.instance.revision, &task, "bob", None, tick(&mut t))
        .unwrap_err();
    assert!(matches!(err, ApprovalError::NotAuthorized(_)));
    e.approve(e.instance.revision, &task, "alice", None, tick(&mut t))
        .unwrap();
    assert_eq!(e.instance.status, InstanceStatus::Running);
    e.finish(e.instance.revision, tick(&mut t)).unwrap();
    assert_eq!(e.instance.status, InstanceStatus::Completed);
}

/// 超时：过期席位不计票；未到期不允许过期。
#[test]
fn deadline_expire_excludes_seat_from_quorum() {
    let mut e = start();
    let mut t = 1_000;
    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "approve",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (r, seats, _) = e
        .open_approval(
            e.instance.revision,
            &b,
            vec![SeatSpec::approver("alice"), SeatSpec::approver("bob")],
            Some(2_000),
            tick(&mut t),
        )
        .unwrap();
    let err = e.expire(e.instance.revision, &seats[0], 1_500).unwrap_err();
    assert!(matches!(err, ApprovalError::InvalidArgument(_)));
    e.expire(e.instance.revision, &seats[0], 2_000).unwrap();
    assert_eq!(e.tasks[&seats[0]].status, TaskStatus::Expired);
    assert_eq!(e.rounds[&r].total_count, 1);
    e.approve(e.instance.revision, &seats[1], "bob", None, tick(&mut t))
        .unwrap();
    assert_eq!(e.rounds[&r].status, RoundStatus::Passed);
    assert_eq!(e.instance.status, InstanceStatus::Running);
    e.finish(e.instance.revision, tick(&mut t)).unwrap();
    assert_eq!(e.instance.status, InstanceStatus::Completed);
}

// ------------------------------------------------- 生产适配新增的模型用例

/// `start` 的字段校验：空必填字段与非法实例 id 一律 fail-closed。
#[test]
fn start_rejects_empty_required_fields() {
    let spec = |id: &str, hash: &str| StartSpec {
        id: id.into(),
        definition_ns: "eis".into(),
        definition_name: "leave-approval".into(),
        definition_version: "1.0.0".into(),
        definition_hash: hash.into(),
        initiator: "zhangsan".into(),
        initiator_acting_service: None,
        context: Scope::new(),
    };

    let err = Engine::start(spec("", "sha256:t2"), 1_000).unwrap_err();
    assert_eq!(err, ApprovalError::InvalidArgument("id".into()));
    assert_eq!(err.to_string(), "invalid argument: id");

    let err = Engine::start(spec(INSTANCE_ID, ""), 1_000).unwrap_err();
    assert_eq!(
        err,
        ApprovalError::InvalidArgument("definition_hash".into())
    );
    assert_eq!(err.to_string(), "invalid argument: definition_hash");

    let err = Engine::start(spec("inst/t2", "sha256:t2"), 1_000).unwrap_err();
    assert_eq!(err, ApprovalError::InvalidArgument("id".into()));

    // 同一 id 校验分支的长度上界（128 字节）：超限同样拒绝。
    let long_id = "i".repeat(129);
    let err = Engine::start(spec(&long_id, "sha256:t2"), 1_000).unwrap_err();
    assert_eq!(err, ApprovalError::InvalidArgument("id".into()));
}

/// 子实体 ID 由实例 id + 持久化序号派生；聚合往返后序号与节点进入次数都不倒退。
#[test]
fn generated_ids_are_derived_from_instance_and_survive_reload() {
    let mut e = start();
    let mut t = 1_000;

    // 序号由 `gen` 统一分配（round / task 也占用序号），故分支序号之间有间隔。
    let (b1, _) = e
        .enter_branch(
            e.instance.revision,
            "apply",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    assert_eq!(b1, "inst-t2:branch-0001");
    pass_branch(&mut e, &b1, "alice", &mut t);

    let (b2, _) = e
        .enter_branch(
            e.instance.revision,
            "apply",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    assert_eq!(b2, "inst-t2:branch-0004");
    pass_branch(&mut e, &b2, "alice", &mut t);

    // 持久化往返：`seq`（下一个 ID 序号）与 `node_attempts`（节点进入次数）随聚合走；
    // 若不随聚合持久化，下一个分支会退回 `branch-0001` 并与既有分支撞 ID。
    let encoded = serde_json::to_value(&e).expect("serialize engine");
    let restored: Engine = serde_json::from_value(encoded).expect("deserialize engine");
    assert_eq!(restored, e);

    let mut e = restored;
    let (b3, _) = e
        .enter_branch(
            e.instance.revision,
            "apply",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    assert_eq!(b3, "inst-t2:branch-0007");
    assert_eq!(e.branches[&b3].attempt, 3);

    let mut ids = vec![b1.clone(), b2.clone(), b3.clone()];
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), 3, "三次进入节点必须产生互不相同的 branch id");
    for id in [&b1, &b2, &b3] {
        assert!(id.starts_with(&e.instance.id), "{id} 必须以实例 id 为前缀");
    }
}

/// 终态实例 seal 一切变更命令：`finish` 收口后连进入节点都被拒绝。
#[test]
fn terminal_instance_rejects_commands() {
    let mut e = start();
    let mut t = 1_000;
    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "apply",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (_, seats, _) = e
        .open_approval(
            e.instance.revision,
            &b,
            vec![SeatSpec::approver("alice")],
            None,
            tick(&mut t),
        )
        .unwrap();
    e.approve(e.instance.revision, &seats[0], "alice", None, tick(&mut t))
        .unwrap();
    e.finish(e.instance.revision, tick(&mut t)).unwrap();
    assert_eq!(e.instance.status, InstanceStatus::Completed);

    let err = e
        .enter_branch(
            e.instance.revision,
            "apply",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap_err();
    assert_eq!(
        err,
        ApprovalError::InvalidTransition("instance is Completed".into())
    );
    assert_eq!(e.branches.len(), 1, "被拒绝的命令不得改动状态");
}

/// 证据快照：`snapshot_json` 以 `Ok` 返回（序列化失败走错误而非 panic），键集合固定。
#[test]
fn snapshot_json_failure_is_error_not_panic() {
    let mut e = start();
    let mut t = 1_000;
    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "apply",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    e.open_approval(
        e.instance.revision,
        &b,
        vec![SeatSpec::approver("alice")],
        None,
        tick(&mut t),
    )
    .unwrap();

    let snap = e.snapshot_json().expect("snapshot");
    let value = serde_json::to_value(&snap).expect("snapshot is JSON");
    let mut keys: Vec<&str> = value
        .as_object()
        .expect("snapshot is a JSON object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(keys, vec!["branches", "instance", "rounds", "tasks"]);
}

// ------------------------------------------------- 管理命令与变量作用域

/// 驳回至节点：当前节点关闭为 `Returned`，目标节点旧尝试作废，重开新尝试并记 `returns`。
#[test]
fn p0_return_to_node_reopens_history() {
    let mut e = start();
    let mut t = 1_000;
    let (b1, _) = e
        .enter_branch(
            e.instance.revision,
            "apply",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (_, ts1, _) = e
        .open_approval(
            e.instance.revision,
            &b1,
            vec![SeatSpec::approver("alice")],
            None,
            tick(&mut t),
        )
        .unwrap();
    e.approve(e.instance.revision, &ts1[0], "alice", None, tick(&mut t))
        .unwrap();
    assert_eq!(e.branches[&b1].status, BranchStatus::Completed);

    let (b2, _) = e
        .enter_branch(
            e.instance.revision,
            "review",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (r2, _ts2, _) = e
        .open_approval(
            e.instance.revision,
            &b2,
            vec![SeatSpec::approver("bob")],
            None,
            tick(&mut t),
        )
        .unwrap();

    let out = e
        .return_to_node(
            e.instance.revision,
            &b2,
            "apply",
            "bob",
            Some("材料不全，退回重填"),
            tick(&mut t),
        )
        .unwrap();
    let b3 = out.0;
    assert_eq!(e.branches[&b2].status, BranchStatus::Returned);
    assert_eq!(e.branches[&b1].status, BranchStatus::Superseded);
    assert_eq!(e.rounds[&r2].status, RoundStatus::Superseded);
    assert_eq!(e.branches[&b3].node, "apply");
    assert_eq!(e.branches[&b3].attempt, 2);
    assert_eq!(e.instance.jump_count, 1);
    assert_eq!(e.instance.returns.len(), 1);
    assert_eq!(e.instance.returns[0].from_node, "review");
    assert_eq!(e.instance.returns[0].to_node, "apply");
    assert_eq!(e.instance.returns[0].by, "bob");
    assert_eq!(
        e.instance.returns[0].comment.as_deref(),
        Some("材料不全，退回重填")
    );
    assert!(has_event(&out.1, "com.china-workflow.instance.returned.v1"));
    assert_eq!(e.instance.status, InstanceStatus::Running);

    // 重开 A、走完 B
    let (_, ts3, _) = e
        .open_approval(
            e.instance.revision,
            &b3,
            vec![SeatSpec::approver("alice")],
            None,
            tick(&mut t),
        )
        .unwrap();
    e.approve(e.instance.revision, &ts3[0], "alice", None, tick(&mut t))
        .unwrap();
    let (b4, _) = e
        .enter_branch(
            e.instance.revision,
            "review",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    assert_eq!(e.branches[&b4].attempt, 2);
    let (_, ts4, _) = e
        .open_approval(
            e.instance.revision,
            &b4,
            vec![SeatSpec::approver("bob")],
            None,
            tick(&mut t),
        )
        .unwrap();
    e.approve(e.instance.revision, &ts4[0], "bob", None, tick(&mut t))
        .unwrap();
    assert_eq!(e.instance.status, InstanceStatus::Running);
    e.finish(e.instance.revision, tick(&mut t)).unwrap();
    assert_eq!(e.instance.status, InstanceStatus::Completed);
}

/// 转办：责任人变更；原审批人失去审批权（保留审计链），新责任人可批。
#[test]
fn p0_transfer_moves_authority() {
    let mut e = start();
    let mut t = 1_000;
    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "approve",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (_r, seats, _) = e
        .open_approval(
            e.instance.revision,
            &b,
            vec![SeatSpec::pool(vec!["alice".into(), "bob".into()])],
            None,
            tick(&mut t),
        )
        .unwrap();
    let task = seats[0].clone();
    assert_eq!(e.open_tasks_for("alice").len(), 1);
    assert_eq!(e.open_tasks_for("bob").len(), 1);

    e.claim(e.instance.revision, &task, "alice", tick(&mut t))
        .unwrap();
    assert_eq!(e.tasks[&task].status, TaskStatus::Claimed);
    let out = e
        .transfer(e.instance.revision, &task, "carol", "alice", tick(&mut t))
        .unwrap();
    assert_eq!(e.tasks[&task].assignee.as_deref(), Some("carol"));
    assert_eq!(e.tasks[&task].transferred_from, vec!["alice".to_string()]);
    assert!(out.iter().any(|x| matches!(
        x,
        Effect::TaskAssigned {
            kind: AssignKind::Transfer,
            ..
        }
    )));

    let err = e
        .approve(e.instance.revision, &task, "alice", None, tick(&mut t))
        .unwrap_err();
    assert!(matches!(err, ApprovalError::NotAuthorized(_)));
    e.approve(e.instance.revision, &task, "carol", None, tick(&mut t))
        .unwrap();
    assert_eq!(e.instance.status, InstanceStatus::Running);
    e.finish(e.instance.revision, tick(&mut t)).unwrap();
    assert_eq!(e.instance.status, InstanceStatus::Completed);
}

/// 委派：被委派人代批（记录 `on_behalf_of`）；原审批人权限保留。
#[test]
fn delegate_allows_proxy_and_keeps_principal() {
    let mut e = start();
    let mut t = 1_000;
    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "approve",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (_, seats, _) = e
        .open_approval(
            e.instance.revision,
            &b,
            vec![SeatSpec::approver("alice")],
            None,
            tick(&mut t),
        )
        .unwrap();
    e.delegate(
        e.instance.revision,
        &seats[0],
        "dave",
        None,
        "alice",
        tick(&mut t),
    )
    .unwrap();
    assert_eq!(e.open_tasks_for("dave").len(), 1);
    assert_eq!(e.open_tasks_for("alice").len(), 1);
    e.approve(e.instance.revision, &seats[0], "dave", None, tick(&mut t))
        .unwrap();
    let v = &e.tasks[&seats[0]].votes[0];
    assert_eq!(v.actor, "dave");
    assert_eq!(v.on_behalf_of.as_deref(), Some("alice"));

    // 原审批人保留权限：独立实例里 alice 在被委派后仍可自己批
    let mut e2 = start();
    let mut t2 = 1_000;
    let (b2, _) = e2
        .enter_branch(
            e2.instance.revision,
            "approve",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t2),
        )
        .unwrap();
    let (_, seats2, _) = e2
        .open_approval(
            e2.instance.revision,
            &b2,
            vec![SeatSpec::approver("alice")],
            None,
            tick(&mut t2),
        )
        .unwrap();
    e2.delegate(
        e2.instance.revision,
        &seats2[0],
        "dave",
        None,
        "alice",
        tick(&mut t2),
    )
    .unwrap();
    e2.approve(
        e2.instance.revision,
        &seats2[0],
        "alice",
        None,
        tick(&mut t2),
    )
    .unwrap();
    assert_eq!(e2.tasks[&seats2[0]].votes[0].on_behalf_of, None);
}

/// 减签批量零副作用：`[open, 未知]` 混合批次在任何变更前被整批拒绝，状态逐字不变。
///
/// 这是对 Spike 原型错误路径行为的**裁定偏离**：原实现在变更循环内校验，会先取消第一个
/// 席位再报错；命令错误必须零状态变化（模块不变量），故改为先全量校验。已终态目标不再报错
/// 而是被跳过（见 `remove_sign_skips_terminal_targets`），故这里用不存在的目标触发错误路径。
#[test]
fn remove_sign_mixed_batch_is_all_or_nothing() {
    let mut e = start();
    let mut t = 1_000;
    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "approve",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (r, seats, _) = e
        .open_approval(
            e.instance.revision,
            &b,
            vec![SeatSpec::approver("alice"), SeatSpec::approver("bob")],
            None,
            tick(&mut t),
        )
        .unwrap();
    assert_eq!(e.rounds[&r].status, RoundStatus::Open);

    let before_rev = e.instance.revision;
    let before_alice = e.tasks[&seats[0]].status;
    let before_bob = e.tasks[&seats[1]].status;
    let before_round = e.rounds[&r].clone();
    let before_snapshot = e.snapshot_json().unwrap();

    // 批次顺序 [open, 未知]：不得先取消 alice 再报错。
    let err = e
        .remove_sign(
            e.instance.revision,
            &b,
            &[seats[0].clone(), "inst-t2:absent".into()],
            "admin",
            tick(&mut t),
        )
        .unwrap_err();
    assert_eq!(err, ApprovalError::NotFound("inst-t2:absent".into()));

    assert_eq!(e.instance.revision, before_rev);
    assert_eq!(e.tasks[&seats[0]].status, before_alice);
    assert_eq!(e.tasks[&seats[1]].status, before_bob);
    assert_eq!(e.rounds[&r], before_round);
    assert_eq!(e.snapshot_json().unwrap(), before_snapshot);
}

/// 撤回：仅发起人；级联取消未决席位并 seal 实例。
#[test]
fn withdraw_only_by_initiator() {
    let mut e = start();
    let mut t = 1_000;
    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "approve",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (_, seats, _) = e
        .open_approval(
            e.instance.revision,
            &b,
            vec![SeatSpec::approver("alice")],
            None,
            tick(&mut t),
        )
        .unwrap();
    let err = e
        .withdraw(e.instance.revision, "lisi", None, None, tick(&mut t))
        .unwrap_err();
    assert!(matches!(err, ApprovalError::NotAuthorized(_)));
    let out = e
        .withdraw(e.instance.revision, "zhangsan", None, None, tick(&mut t))
        .unwrap();
    assert_eq!(e.instance.status, InstanceStatus::Cancelled);
    assert_eq!(e.branches[&b].status, BranchStatus::Cancelled);
    assert_eq!(e.tasks[&seats[0]].status, TaskStatus::Withdrawn);
    assert!(has_event(&out, "com.china-workflow.instance.cancelled.v1"));
}

/// 终止：管理路径级联取消 open 分支/轮次/席位，终态实例拒绝后续命令。
#[test]
fn terminate_cascades_and_seals_instance() {
    let mut e = start();
    let mut t = 1_000;
    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "approve",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (_, seats, _) = e
        .open_approval(
            e.instance.revision,
            &b,
            vec![SeatSpec::approver("alice")],
            None,
            tick(&mut t),
        )
        .unwrap();
    let out = e
        .terminate(
            e.instance.revision,
            "admin",
            "policy violation",
            tick(&mut t),
        )
        .unwrap();
    assert_eq!(e.instance.status, InstanceStatus::Terminated);
    assert_eq!(e.branches[&b].status, BranchStatus::Terminated);
    assert_eq!(e.tasks[&seats[0]].status, TaskStatus::Cancelled);
    assert!(has_event(&out, "com.china-workflow.instance.terminated.v1"));
    let err = e
        .approve(e.instance.revision, &seats[0], "alice", None, tick(&mut t))
        .unwrap_err();
    assert!(matches!(err, ApprovalError::InvalidTransition(_)));
}

/// 驳回白名单与 jump 上限：未进入过的节点不可作为目标；超限拒绝且状态零变化。
#[test]
fn return_target_whitelist_and_jump_limit() {
    let mut e = start();
    let mut t = 1_000;
    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "apply",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let err = e
        .return_to_node(
            e.instance.revision,
            &b,
            "never-visited",
            "alice",
            None,
            tick(&mut t),
        )
        .unwrap_err();
    assert!(matches!(err, ApprovalError::InvalidArgument(_)));
    assert_eq!(e.branches[&b].status, BranchStatus::Open);

    e.instance.jump_count = MAX_JUMPS;
    let err = e
        .return_to_node(
            e.instance.revision,
            &b,
            "apply",
            "alice",
            None,
            tick(&mut t),
        )
        .unwrap_err();
    assert!(matches!(err, ApprovalError::InvalidArgument(_)));
}

/// 三级作用域：task 覆盖 branch 覆盖 instance。
#[test]
fn variable_scope_precedence() {
    let mut e = start();
    let mut t = 1_000;
    e.instance
        .context
        .insert("region".into(), serde_json::json!("cn-north-1"));
    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "approve",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    e.branches
        .get_mut(&b)
        .unwrap()
        .scope
        .insert("region".into(), serde_json::json!("cn-east-2"));
    let (_, seats, _) = e
        .open_approval(
            e.instance.revision,
            &b,
            vec![SeatSpec::approver("alice")],
            None,
            tick(&mut t),
        )
        .unwrap();
    e.tasks
        .get_mut(&seats[0])
        .unwrap()
        .scope
        .insert("region".into(), serde_json::json!("cn-south-1"));
    assert_eq!(
        e.resolve_var(&seats[0], "region").unwrap(),
        &serde_json::json!("cn-south-1")
    );
    assert_eq!(e.resolve_var(&seats[0], "absent"), None);
    e.tasks.get_mut(&seats[0]).unwrap().scope.remove("region");
    assert_eq!(
        e.resolve_var(&seats[0], "region").unwrap(),
        &serde_json::json!("cn-east-2")
    );
    e.branches.get_mut(&b).unwrap().scope.remove("region");
    assert_eq!(
        e.resolve_var(&seats[0], "region").unwrap(),
        &serde_json::json!("cn-north-1")
    );
}

/// 证据快照：完整场景可 `serde_json::to_value`，且固定含四键。
#[test]
fn evidence_snapshot_serializable() {
    let mut e = start();
    let mut t = 1_000;
    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "apply",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (_, seats, _) = e
        .open_approval(
            e.instance.revision,
            &b,
            vec![SeatSpec::approver("alice")],
            None,
            tick(&mut t),
        )
        .unwrap();
    e.approve(
        e.instance.revision,
        &seats[0],
        "alice",
        Some("同意"),
        tick(&mut t),
    )
    .unwrap();
    e.finish(e.instance.revision, tick(&mut t)).unwrap();
    let snap = e.snapshot_json().expect("snapshot");
    serde_json::to_value(&snap).expect("snapshot is JSON");
    assert!(snap.get("instance").is_some());
    assert!(snap.get("branches").is_some());
    assert!(snap.get("tasks").is_some());
    assert!(snap.get("rounds").is_some());
    assert_eq!(snap["instance"]["status"].as_str(), Some("completed"));
}

/// 驳回目标不在白名单：错误消息点名目标节点，且分支保持 `Open`。
#[test]
fn return_to_node_rejects_non_whitelisted_target() {
    let mut e = start();
    let mut t = 1_000;
    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "apply",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let err = e
        .return_to_node(
            e.instance.revision,
            &b,
            "never-visited",
            "alice",
            None,
            tick(&mut t),
        )
        .unwrap_err();
    let ApprovalError::InvalidArgument(msg) = &err else {
        panic!("expected InvalidArgument, got {err:?}");
    };
    assert!(
        msg.contains("never-visited"),
        "错误消息须点名目标节点: {msg}"
    );
    assert_eq!(e.branches[&b].status, BranchStatus::Open);
    assert_eq!(e.instance.jump_count, 0);
    assert!(e.instance.returns.is_empty());
}

/// 驳回之后无 completed 根分支：`finish` 拒绝且实例保持 `Running`。
#[test]
fn finish_after_returned_branch_is_invalid_transition() {
    let mut e = start();
    let mut t = 1_000;
    let (b1, _) = e
        .enter_branch(
            e.instance.revision,
            "apply",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    pass_branch(&mut e, &b1, "alice", &mut t);
    assert_eq!(e.branches[&b1].status, BranchStatus::Completed);

    let (b2, _) = e
        .enter_branch(
            e.instance.revision,
            "review",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    e.open_approval(
        e.instance.revision,
        &b2,
        vec![SeatSpec::approver("bob")],
        None,
        tick(&mut t),
    )
    .unwrap();
    let (b3, _) = e
        .return_to_node(e.instance.revision, &b2, "apply", "bob", None, tick(&mut t))
        .unwrap();
    // 根分支链：b1 已作废、b2 已退回、b3 重开且 Open —— 无 completed 根分支。
    assert_eq!(e.branches[&b1].status, BranchStatus::Superseded);
    assert_eq!(e.branches[&b2].status, BranchStatus::Returned);
    assert_eq!(e.branches[&b3].status, BranchStatus::Open);

    let err = e.finish(e.instance.revision, tick(&mut t)).unwrap_err();
    assert!(matches!(err, ApprovalError::InvalidTransition(_)));
    assert_eq!(e.instance.status, InstanceStatus::Running);
}

/// 判定顺序：`AnyReject` 一票否决先于「全通过」与「耗尽」判定。
///
/// 3 席位 + `JoinPolicy::All`：A 通过后 `completed=1, total=3`（既不满足全通过，也未耗尽）；
/// B 否决时，正确顺序（先一票否决）→ `Rejected`，若否决规则排在耗尽判定之后，则
/// `completed+rejected=2 ≠ total=3` 会让轮次停在 `Open`。
#[test]
fn any_reject_takes_precedence_over_exhaustion_and_pass_rules() {
    let mut e = start();
    let mut t = 1_000;
    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "approve",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (r, seats, _) = e
        .open_approval(
            e.instance.revision,
            &b,
            vec![
                SeatSpec::approver("alice"),
                SeatSpec::approver("bob"),
                SeatSpec::approver("carol"),
            ],
            None,
            tick(&mut t),
        )
        .unwrap();
    e.approve(e.instance.revision, &seats[0], "alice", None, tick(&mut t))
        .unwrap();
    assert_eq!(e.rounds[&r].status, RoundStatus::Open);
    assert_eq!(e.rounds[&r].completed_count, 1);
    assert_eq!(e.tasks[&seats[2]].status, TaskStatus::Created);

    let out = e
        .reject(
            e.instance.revision,
            &seats[1],
            "bob",
            Some("不同意"),
            tick(&mut t),
        )
        .unwrap();
    assert_eq!(e.rounds[&r].status, RoundStatus::Rejected);
    assert_eq!(e.rounds[&r].rejected_count, 1);
    assert_eq!(e.branches[&b].status, BranchStatus::Rejected);
    assert_eq!(e.instance.status, InstanceStatus::Rejected);
    assert_eq!(e.tasks[&seats[2]].status, TaskStatus::Cancelled);
    assert!(has_event(
        &out,
        "com.china-workflow.approval.round.rejected.v1"
    ));
}

// --------------------------------------------- M2-C 硬化（超时 / 减签 / 阅签收口）

/// 超时语义：唯一可计票席位过期后 `total==0`，轮次保持 `Open`，绝不自动通过
/// （Q4 无自动判定）。断言轮次/实例状态与效果种类，不绑定事件名字面量。
#[test]
fn all_seats_expired_keeps_round_open() {
    let mut e = start();
    let mut t = 1_000;
    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "approve",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (r, seats, _) = e
        .open_approval(
            e.instance.revision,
            &b,
            vec![SeatSpec::approver("alice")],
            Some(2_000),
            tick(&mut t),
        )
        .unwrap();
    let out = e.expire(e.instance.revision, &seats[0], 2_000).unwrap();

    assert_eq!(e.tasks[&seats[0]].status, TaskStatus::Expired);
    assert_eq!(e.rounds[&r].total_count, 0);
    assert_eq!(e.rounds[&r].status, RoundStatus::Open);
    assert_eq!(e.branches[&b].status, BranchStatus::Open);
    assert_eq!(e.instance.status, InstanceStatus::Running);
    assert!(!out.iter().any(|ef| matches!(
        ef,
        Effect::RoundStatus {
            to: RoundStatus::Passed,
            ..
        }
    )));
}

/// 减签至零席：`total==0` 不等于通过，轮次保持 `Open`（减签耗尽绝不自动判定）。
#[test]
fn remove_all_unvoted_seats_keeps_round_open() {
    let mut e = start();
    let mut t = 1_000;
    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "approve",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (r, seats, _) = e
        .open_approval(
            e.instance.revision,
            &b,
            vec![SeatSpec::approver("alice"), SeatSpec::approver("bob")],
            None,
            tick(&mut t),
        )
        .unwrap();
    let out = e
        .remove_sign(
            e.instance.revision,
            &b,
            &[seats[0].clone(), seats[1].clone()],
            "admin",
            tick(&mut t),
        )
        .unwrap();

    assert_eq!(e.tasks[&seats[0]].status, TaskStatus::Cancelled);
    assert_eq!(e.tasks[&seats[1]].status, TaskStatus::Cancelled);
    assert_eq!(e.rounds[&r].total_count, 0);
    assert_eq!(e.rounds[&r].status, RoundStatus::Open);
    assert_eq!(e.branches[&b].status, BranchStatus::Open);
    assert_eq!(e.instance.status, InstanceStatus::Running);
    assert!(!out.iter().any(|ef| matches!(
        ef,
        Effect::RoundStatus {
            to: RoundStatus::Passed,
            ..
        }
    )));
}

/// 减签重复目标：入口去重，重复 id 在任何变更前整批拒绝（零写入）。
#[test]
fn remove_sign_duplicate_targets_rejected() {
    let mut e = start();
    let mut t = 1_000;
    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "approve",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (r, seats, _) = e
        .open_approval(
            e.instance.revision,
            &b,
            vec![SeatSpec::approver("alice"), SeatSpec::approver("bob")],
            None,
            tick(&mut t),
        )
        .unwrap();
    let before_rev = e.instance.revision;
    let before_snapshot = e.snapshot_json().unwrap();

    let err = e
        .remove_sign(
            e.instance.revision,
            &b,
            &[seats[0].clone(), seats[0].clone()],
            "admin",
            tick(&mut t),
        )
        .unwrap_err();
    assert_eq!(err, ApprovalError::InvalidArgument("task_ids".into()));

    assert_eq!(e.instance.revision, before_rev);
    assert_eq!(e.tasks[&seats[0]].status, TaskStatus::Created);
    assert_eq!(e.tasks[&seats[1]].status, TaskStatus::Created);
    assert_eq!(e.rounds[&r].status, RoundStatus::Open);
    assert_eq!(e.snapshot_json().unwrap(), before_snapshot);
}

/// 减签跳过已终态目标：终态席位零效果（无 `Cancelled→Cancelled` 冗余效果），
/// 其余 open 席正常取消；整批皆终态则零效果返回且状态不变。
#[test]
fn remove_sign_skips_terminal_targets() {
    let mut e = start();
    let mut t = 1_000;
    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "approve",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (r, seats, _) = e
        .open_approval(
            e.instance.revision,
            &b,
            vec![
                SeatSpec::approver("alice"),
                SeatSpec::approver("bob"),
                SeatSpec::approver("carol"),
            ],
            None,
            tick(&mut t),
        )
        .unwrap();
    // bob 已表决（Completed）；批次 [bob, carol] 只取消仍未决的 carol。
    e.approve(e.instance.revision, &seats[1], "bob", None, tick(&mut t))
        .unwrap();
    let out = e
        .remove_sign(
            e.instance.revision,
            &b,
            &[seats[1].clone(), seats[2].clone()],
            "admin",
            tick(&mut t),
        )
        .unwrap();

    assert!(!out.iter().any(|ef| matches!(
        ef,
        Effect::TaskStatus { id, .. } if id == &seats[1]
    )));
    assert!(out.iter().any(|ef| matches!(
        ef,
        Effect::TaskStatus {
            id,
            from: TaskStatus::Created,
            to: TaskStatus::Cancelled,
        } if id == &seats[2]
    )));
    assert_eq!(e.tasks[&seats[1]].status, TaskStatus::Completed);
    assert_eq!(e.tasks[&seats[2]].status, TaskStatus::Cancelled);
    assert_eq!(e.rounds[&r].status, RoundStatus::Open);

    // 整批目标皆已终态 ⇒ 零效果，且 revision / 快照不变。
    let before_rev = e.instance.revision;
    let before_snapshot = e.snapshot_json().unwrap();
    let out2 = e
        .remove_sign(before_rev, &b, &[seats[1].clone()], "admin", tick(&mut t))
        .unwrap();
    assert!(out2.is_empty());
    assert_eq!(e.instance.revision, before_rev);
    assert_eq!(e.snapshot_json().unwrap(), before_snapshot);
}

/// 轮次收口取消该轮全部 open 席位：阅签席与审批席一并 `Cancelled`，不留 open。
/// 覆盖「驳回重开（superseded 轮次）」与「票决关闭」两条路径。
#[test]
fn round_close_cancels_open_read_ack_seats() {
    let mut e = start();
    let mut t = 1_000;
    // 先通过一个节点，使 "apply" 进入驳回白名单。
    let (b1, _) = e
        .enter_branch(
            e.instance.revision,
            "apply",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (_, ts1, _) = e
        .open_approval(
            e.instance.revision,
            &b1,
            vec![SeatSpec::approver("alice")],
            None,
            tick(&mut t),
        )
        .unwrap();
    e.approve(e.instance.revision, &ts1[0], "alice", None, tick(&mut t))
        .unwrap();

    // review 节点：审批席 + 阅签席；驳回到 apply 会作废 review 轮次。
    let (b2, _) = e
        .enter_branch(
            e.instance.revision,
            "review",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (r2, seats, _) = e
        .open_approval(
            e.instance.revision,
            &b2,
            vec![SeatSpec::approver("bob"), SeatSpec::reader("carol")],
            None,
            tick(&mut t),
        )
        .unwrap();
    assert_eq!(e.tasks[&seats[1]].status, TaskStatus::Created);
    let out = e
        .return_to_node(
            e.instance.revision,
            &b2,
            "apply",
            "bob",
            Some("材料不全，退回重填"),
            tick(&mut t),
        )
        .unwrap();

    assert_eq!(e.rounds[&r2].status, RoundStatus::Superseded);
    assert_eq!(e.tasks[&seats[0]].status, TaskStatus::Cancelled);
    assert_eq!(e.tasks[&seats[1]].status, TaskStatus::Cancelled);
    assert!(out.1.iter().any(|ef| matches!(
        ef,
        Effect::TaskStatus {
            id,
            to: TaskStatus::Cancelled,
            ..
        } if id == &seats[1]
    )));

    // 票决关闭同样收口阅签席。
    let mut e2 = start();
    let mut t2 = 1_000;
    let (b3, _) = e2
        .enter_branch(
            e2.instance.revision,
            "approve",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t2),
        )
        .unwrap();
    let (r3, seats2, _) = e2
        .open_approval(
            e2.instance.revision,
            &b3,
            vec![SeatSpec::approver("alice"), SeatSpec::reader("bob")],
            None,
            tick(&mut t2),
        )
        .unwrap();
    e2.approve(
        e2.instance.revision,
        &seats2[0],
        "alice",
        None,
        tick(&mut t2),
    )
    .unwrap();
    assert_eq!(e2.rounds[&r3].status, RoundStatus::Passed);
    assert_eq!(e2.tasks[&seats2[1]].status, TaskStatus::Cancelled);
}

// ------------------------------------------------- 驳回判定（#382）

/// 驳回即回退：一次命令内完成否决投票、关闭轮次/分支、重开目标节点。
#[test]
fn reject_to_node_votes_and_rolls_back_atomically() {
    let mut e = start();
    let mut t = 1_000;
    let (apply, _) = e
        .enter_branch(
            e.instance.revision,
            "apply",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (_, apply_seats, _) = e
        .open_approval(
            e.instance.revision,
            &apply,
            vec![SeatSpec::approver("alice")],
            None,
            tick(&mut t),
        )
        .unwrap();
    e.approve(
        e.instance.revision,
        &apply_seats[0],
        "alice",
        None,
        tick(&mut t),
    )
    .unwrap();
    assert_eq!(e.branches[&apply].status, BranchStatus::Completed);

    let (review, _) = e
        .enter_branch(
            e.instance.revision,
            "review",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (round, seats, _) = e
        .open_approval(
            e.instance.revision,
            &review,
            vec![SeatSpec::approver("bob"), SeatSpec::approver("carol")],
            None,
            tick(&mut t),
        )
        .unwrap();

    let (rollback_branch, out) = e
        .reject_to_node(
            e.instance.revision,
            &seats[0],
            "bob",
            Some("资料不全"),
            "apply",
            tick(&mut t),
        )
        .unwrap();
    let new_branch = rollback_branch.expect("reject must roll back");
    assert_ne!(new_branch, apply);
    assert_eq!(e.branches[&new_branch].node, "apply");
    assert_eq!(e.branches[&new_branch].attempt, 2);
    assert_eq!(e.branches[&new_branch].status, BranchStatus::Open);
    assert_eq!(e.branches[&review].status, BranchStatus::Rejected);
    assert_eq!(e.tasks[&seats[0]].status, TaskStatus::Rejected);
    assert_eq!(e.tasks[&seats[0]].votes.len(), 1, "回退不得撤销否决记录");
    assert_eq!(e.tasks[&seats[0]].votes[0].decision, Decision::Reject);
    assert_eq!(e.tasks[&seats[1]].status, TaskStatus::Cancelled);
    assert_eq!(e.rounds[&round].status, RoundStatus::Rejected);
    assert_eq!(
        e.rounds[&round].reject_reason.as_deref(),
        Some("any_reject")
    );
    assert_eq!(e.instance.status, InstanceStatus::Running);
    assert_eq!(e.instance.returns.len(), 1);
    assert!(has_event(&out, "com.china-workflow.instance.returned.v1"));
    let returned = out
        .iter()
        .find_map(|x| match x {
            Effect::Event { name, data } if name == "com.china-workflow.instance.returned.v1" => {
                Some(data.clone())
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(returned["reason"], "any_reject");
    assert_eq!(returned["to_node"], "apply");
}

/// `AtLeastRejects(n)`：否决票数达到阈值前保持轮次 Open。
#[test]
fn at_least_rejects_threshold_closes_round() {
    let mut e = start();
    let mut t = 1_000;
    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "review",
            None,
            JoinPolicy::All,
            RejectPolicy::AtLeastRejects(2),
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (r, seats, _) = e
        .open_approval(
            e.instance.revision,
            &b,
            vec![
                SeatSpec::approver("a"),
                SeatSpec::approver("b"),
                SeatSpec::approver("c"),
            ],
            None,
            tick(&mut t),
        )
        .unwrap();

    e.reject(e.instance.revision, &seats[0], "a", None, tick(&mut t))
        .unwrap();
    assert_eq!(e.rounds[&r].status, RoundStatus::Open);
    assert_eq!(e.rounds[&r].rejected_count, 1);

    e.reject(e.instance.revision, &seats[1], "b", None, tick(&mut t))
        .unwrap();
    assert_eq!(e.rounds[&r].status, RoundStatus::Rejected);
    assert_eq!(
        e.rounds[&r].reject_reason.as_deref(),
        Some("reject_threshold_reached")
    );
}

/// `RecordAndContinue`：剩余席位已不可能满足会签条件时立即回退。
#[test]
fn record_and_continue_deadlock_detects_unsatisfiable_all() {
    let mut e = start();
    let mut t = 1_000;
    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "review",
            None,
            JoinPolicy::All,
            RejectPolicy::RecordAndContinue,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (r, seats, _) = e
        .open_approval(
            e.instance.revision,
            &b,
            vec![
                SeatSpec::approver("a"),
                SeatSpec::approver("b"),
                SeatSpec::approver("c"),
            ],
            None,
            tick(&mut t),
        )
        .unwrap();

    e.reject(e.instance.revision, &seats[0], "a", None, tick(&mut t))
        .unwrap();
    assert_eq!(e.rounds[&r].status, RoundStatus::Rejected);
    assert_eq!(
        e.rounds[&r].reject_reason.as_deref(),
        Some("sign_completion_unsatisfiable")
    );
}

/// `RecordAndContinue`：剩余席位仍可满足通过条件时保留轮次。
#[test]
fn record_and_continue_keeps_open_when_still_satisfiable() {
    let mut e = start();
    let mut t = 1_000;
    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "review",
            None,
            JoinPolicy::Any,
            RejectPolicy::RecordAndContinue,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (r, seats, _) = e
        .open_approval(
            e.instance.revision,
            &b,
            vec![
                SeatSpec::approver("a"),
                SeatSpec::approver("b"),
                SeatSpec::approver("c"),
            ],
            None,
            tick(&mut t),
        )
        .unwrap();

    e.reject(e.instance.revision, &seats[0], "a", None, tick(&mut t))
        .unwrap();
    assert_eq!(e.rounds[&r].status, RoundStatus::Open);
    assert!(e.rounds[&r].reject_reason.is_none());

    e.approve(e.instance.revision, &seats[1], "b", None, tick(&mut t))
        .unwrap();
    assert_eq!(e.rounds[&r].status, RoundStatus::Passed);
}

/// 比例票签按「向上取整所需通过数」判定：4 席 × 2/3 ⇒ 需要 3 票。
#[test]
fn ratio_rounds_up_required_approvals() {
    let mut e = start();
    let mut t = 1_000;
    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "vote",
            None,
            JoinPolicy::Ratio { num: 2, den: 3 },
            RejectPolicy::RecordAndContinue,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (r, seats, _) = e
        .open_approval(
            e.instance.revision,
            &b,
            vec![
                SeatSpec::approver("a"),
                SeatSpec::approver("b"),
                SeatSpec::approver("c"),
                SeatSpec::approver("d"),
            ],
            None,
            tick(&mut t),
        )
        .unwrap();

    e.approve(e.instance.revision, &seats[0], "a", None, tick(&mut t))
        .unwrap();
    e.approve(e.instance.revision, &seats[1], "b", None, tick(&mut t))
        .unwrap();
    e.reject(e.instance.revision, &seats[2], "c", None, tick(&mut t))
        .unwrap();
    // 已通过 2 + 未投 1 = 3 ⇒ 仍可能达到向上取整后的 3 票。
    assert_eq!(e.rounds[&r].status, RoundStatus::Open);

    e.reject(e.instance.revision, &seats[3], "d", None, tick(&mut t))
        .unwrap();
    assert_eq!(e.rounds[&r].status, RoundStatus::Rejected);
    assert_eq!(
        e.rounds[&r].reject_reason.as_deref(),
        Some("sign_completion_unsatisfiable")
    );
}

/// 带回退目标的 `RecordAndContinue`：仍可满足时不回退，仅记录否决。
#[test]
fn reject_to_node_keeps_running_when_policy_continues() {
    let mut e = start();
    let mut t = 1_000;
    let (apply, _) = e
        .enter_branch(
            e.instance.revision,
            "apply",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (_, apply_seats, _) = e
        .open_approval(
            e.instance.revision,
            &apply,
            vec![SeatSpec::approver("alice")],
            None,
            tick(&mut t),
        )
        .unwrap();
    e.approve(
        e.instance.revision,
        &apply_seats[0],
        "alice",
        None,
        tick(&mut t),
    )
    .unwrap();

    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "vote",
            None,
            JoinPolicy::Ratio { num: 2, den: 3 },
            RejectPolicy::RecordAndContinue,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (r, seats, _) = e
        .open_approval(
            e.instance.revision,
            &b,
            vec![
                SeatSpec::approver("a"),
                SeatSpec::approver("b"),
                SeatSpec::approver("c"),
                SeatSpec::approver("d"),
            ],
            None,
            tick(&mut t),
        )
        .unwrap();

    let (rolled_back, _) = e
        .reject_to_node(
            e.instance.revision,
            &seats[0],
            "a",
            Some("有异议"),
            "apply",
            tick(&mut t),
        )
        .unwrap();
    assert!(rolled_back.is_none());
    assert_eq!(e.rounds[&r].status, RoundStatus::Open);
    assert_eq!(e.branches[&b].status, BranchStatus::Open);
    assert_eq!(e.instance.status, InstanceStatus::Running);
}

/// 非白名单目标在投票前拒绝：零副作用。
#[test]
fn reject_to_node_rejects_non_whitelisted_target_without_side_effects() {
    let mut e = start();
    let mut t = 1_000;
    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "review",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (_, seats, _) = e
        .open_approval(
            e.instance.revision,
            &b,
            vec![SeatSpec::approver("a")],
            None,
            tick(&mut t),
        )
        .unwrap();
    let rev = e.instance.revision;
    let err = e
        .reject_to_node(
            e.instance.revision,
            &seats[0],
            "a",
            None,
            "nope",
            tick(&mut t),
        )
        .unwrap_err();
    assert!(matches!(err, ApprovalError::InvalidArgument(_)));
    assert_eq!(e.instance.revision, rev);
    assert_eq!(e.tasks[&seats[0]].status, TaskStatus::Created);
    assert_eq!(e.rounds.values().next().unwrap().status, RoundStatus::Open);
}

// ------------------------------------------------- 动态动作（#381）

/// 催办只写审计，不改席位状态/计票，也不产生事件。
#[test]
fn urge_records_audit_without_state_change() {
    let mut e = start();
    let mut t = 1_000;
    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "review",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (r, seats, _) = e
        .open_approval(
            e.instance.revision,
            &b,
            vec![SeatSpec::approver("alice")],
            None,
            tick(&mut t),
        )
        .unwrap();
    let rev = e.instance.revision;
    let out = e
        .urge(rev, &seats[0], "zhangsan", "请尽快处理", tick(&mut t))
        .unwrap();
    assert!(out.is_empty(), "催办不得产生状态效果");
    assert_eq!(e.instance.revision, rev + 1);
    assert_eq!(e.tasks[&seats[0]].status, TaskStatus::Created);
    assert_eq!(e.rounds[&r].completed_count, 0);
    let last = e.instance.audit.last().unwrap();
    assert_eq!(last.action, "urge");
    assert_eq!(last.task.as_deref(), Some(seats[0].as_str()));
}

/// 终态席位催办 → 冲突；非参与者 → 拒绝。
#[test]
fn urge_rejects_terminal_task_and_outsider() {
    let mut e = start();
    let mut t = 1_000;
    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "review",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (_, seats, _) = e
        .open_approval(
            e.instance.revision,
            &b,
            vec![SeatSpec::approver("alice")],
            None,
            tick(&mut t),
        )
        .unwrap();
    assert!(matches!(
        e.urge(
            e.instance.revision,
            &seats[0],
            "mallory",
            "hi",
            tick(&mut t)
        )
        .unwrap_err(),
        ApprovalError::NotAuthorized(_)
    ));
    e.approve(e.instance.revision, &seats[0], "alice", None, tick(&mut t))
        .unwrap();
    assert!(matches!(
        e.urge(
            e.instance.revision,
            &seats[0],
            "zhangsan",
            "hi",
            tick(&mut t)
        )
        .unwrap_err(),
        ApprovalError::InvalidTransition(_)
    ));
}

/// 抄送受节点 `allowCc` 约束；终态席位拒绝。
#[test]
fn cc_requires_allow_cc_and_open_task() {
    let mut e = start();
    let mut t = 1_000;
    let policy = ApprovalPolicy {
        allow_cc: true,
        ..Default::default()
    };
    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "review",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            policy,
            tick(&mut t),
        )
        .unwrap();
    let (_, seats, _) = e
        .open_approval(
            e.instance.revision,
            &b,
            vec![SeatSpec::approver("alice")],
            None,
            tick(&mut t),
        )
        .unwrap();
    e.cc(
        e.instance.revision,
        Some(&seats[0]),
        &["observer".into()],
        Some("请知悉"),
        "zhangsan",
        tick(&mut t),
    )
    .unwrap();
    let last = e.instance.audit.last().unwrap();
    assert_eq!(last.action, "cc");
    assert_eq!(last.task.as_deref(), Some(seats[0].as_str()));

    // 未开启 allowCc 的节点：任务级抄送 fail-closed。
    let mut e2 = start();
    let mut t2 = 1_000;
    let (b2, _) = e2
        .enter_branch(
            e2.instance.revision,
            "review",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t2),
        )
        .unwrap();
    let (_, seats2, _) = e2
        .open_approval(
            e2.instance.revision,
            &b2,
            vec![SeatSpec::approver("alice")],
            None,
            tick(&mut t2),
        )
        .unwrap();
    assert!(matches!(
        e2.cc(
            e2.instance.revision,
            Some(&seats2[0]),
            &["observer".into()],
            None,
            "zhangsan",
            tick(&mut t2)
        )
        .unwrap_err(),
        ApprovalError::NotAuthorized(_)
    ));

    // 终态席位拒绝。
    e.approve(e.instance.revision, &seats[0], "alice", None, tick(&mut t))
        .unwrap();
    assert!(matches!(
        e.cc(
            e.instance.revision,
            Some(&seats[0]),
            &["observer".into()],
            None,
            "zhangsan",
            tick(&mut t)
        )
        .unwrap_err(),
        ApprovalError::InvalidTransition(_)
    ));
}

/// 任务级撤回：保留通过记录并回退到目标节点；否决记录不可撤销。
#[test]
fn withdraw_task_keeps_record_and_rolls_back() {
    let mut e = start();
    let mut t = 1_000;
    let (apply, _) = e
        .enter_branch(
            e.instance.revision,
            "apply",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (_, apply_seats, _) = e
        .open_approval(
            e.instance.revision,
            &apply,
            vec![SeatSpec::approver("alice")],
            None,
            tick(&mut t),
        )
        .unwrap();
    e.approve(
        e.instance.revision,
        &apply_seats[0],
        "alice",
        None,
        tick(&mut t),
    )
    .unwrap();

    let (review, _) = e
        .enter_branch(
            e.instance.revision,
            "review",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (_, seats, _) = e
        .open_approval(
            e.instance.revision,
            &review,
            vec![SeatSpec::approver("bob"), SeatSpec::approver("carol")],
            None,
            tick(&mut t),
        )
        .unwrap();
    e.approve(e.instance.revision, &seats[0], "bob", None, tick(&mut t))
        .unwrap();

    let (new_branch, out) = e
        .withdraw_task(
            e.instance.revision,
            &seats[0],
            "bob",
            "apply",
            Some("口径错了"),
            tick(&mut t),
        )
        .unwrap();
    assert_eq!(e.branches[&new_branch].node, "apply");
    assert_eq!(e.branches[&new_branch].attempt, 2);
    assert_eq!(e.branches[&new_branch].status, BranchStatus::Open);
    assert_eq!(e.branches[&review].status, BranchStatus::Returned);
    assert_eq!(e.tasks[&seats[0]].status, TaskStatus::Completed);
    assert_eq!(e.tasks[&seats[0]].votes.len(), 1, "撤回不得删除通过记录");
    assert_eq!(e.instance.status, InstanceStatus::Running);
    assert_eq!(e.instance.returns.last().unwrap().to_node, "apply");
    let last = e.instance.audit.last().unwrap();
    assert_eq!(last.action, "withdraw_task");
    assert_eq!(last.task.as_deref(), Some(seats[0].as_str()));
    assert!(has_event(&out, "com.china-workflow.instance.returned.v1"));
}

/// 带否决票的席位不可撤回；非原审批人不可撤回。
#[test]
fn withdraw_task_rejects_reject_record_and_outsider() {
    let mut e = start();
    let mut t = 1_000;
    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "review",
            None,
            JoinPolicy::All,
            RejectPolicy::OnExhausted,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (_, seats, _) = e
        .open_approval(
            e.instance.revision,
            &b,
            vec![SeatSpec::approver("alice"), SeatSpec::approver("bob")],
            None,
            tick(&mut t),
        )
        .unwrap();
    e.approve(e.instance.revision, &seats[0], "alice", None, tick(&mut t))
        .unwrap();
    assert!(matches!(
        e.withdraw_task(
            e.instance.revision,
            &seats[0],
            "bob",
            "review",
            None,
            tick(&mut t)
        )
        .unwrap_err(),
        ApprovalError::NotAuthorized(_)
    ));

    e.reject(e.instance.revision, &seats[1], "bob", None, tick(&mut t))
        .unwrap();
    assert!(matches!(
        e.withdraw_task(
            e.instance.revision,
            &seats[1],
            "bob",
            "review",
            None,
            tick(&mut t)
        )
        .unwrap_err(),
        ApprovalError::InvalidTransition(_)
    ));
}

// ------------------------------------------------- 前后加签 / assignment 状态机

/// 前加签：挂起原席位（对外仍属原任务、但不可见/不可投票），前加签完成后恢复。
#[test]
fn before_add_suspends_original_and_resumes_after_completion() {
    let mut e = start();
    let mut t = 1_000;
    let (branch, _) = e
        .enter_branch(
            e.instance.revision,
            "review",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (_, seats, _) = e
        .open_approval(
            e.instance.revision,
            &branch,
            vec![SeatSpec::approver("u1")],
            None,
            tick(&mut t),
        )
        .unwrap();
    let original = seats[0].clone();

    let (derived, _) = e
        .add_assignee(
            e.instance.revision,
            &original,
            &["u2".into()],
            AddAssigneePosition::Before,
            Some("action-1"),
            "admin",
            Some("pre"),
            tick(&mut t),
        )
        .unwrap();
    assert_eq!(derived.len(), 1);
    assert_eq!(e.tasks[&original].status, TaskStatus::Suspended);
    assert_eq!(e.tasks[&original].suspended_from, Some(TaskStatus::Created));
    assert!(
        e.open_tasks_for_at("u1", t).is_empty(),
        "挂起席位不得进入待办"
    );
    let before = &e.tasks[&derived[0]];
    assert_eq!(before.origin, TaskOrigin::BeforeAdded);
    assert_eq!(before.predecessor.as_deref(), Some(original.as_str()));
    assert_eq!(before.spawned_by.as_deref(), Some("action-1"));
    assert_eq!(e.open_tasks_for_at("u2", t).len(), 1);

    e.approve(e.instance.revision, &derived[0], "u2", None, tick(&mut t))
        .unwrap();
    assert_eq!(e.tasks[&original].status, TaskStatus::Created);
    assert_eq!(e.open_tasks_for_at("u1", t).len(), 1);
    assert_eq!(e.instance.status, InstanceStatus::Running);
    assert_eq!(e.branches[&branch].status, BranchStatus::Open);
    assert!(e
        .instance
        .audit
        .iter()
        .any(|a| a.action == "task_suspended"));
    assert!(e.instance.audit.iter().any(|a| a.action == "task_resumed"));

    e.approve(e.instance.revision, &original, "u1", None, tick(&mut t))
        .unwrap();
    assert_eq!(e.branches[&branch].status, BranchStatus::Completed);
    e.finish(e.instance.revision, tick(&mut t)).unwrap();
    assert_eq!(e.instance.status, InstanceStatus::Completed);
}

/// 后加签：原轮次通过后、下一业务节点前执行；默认 Any，任一通过即取消其余。
#[test]
fn after_add_runs_after_original_and_defaults_to_any() {
    let mut e = start();
    let mut t = 1_000;
    let (branch, _) = e
        .enter_branch(
            e.instance.revision,
            "review",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (_, seats, _) = e
        .open_approval(
            e.instance.revision,
            &branch,
            vec![SeatSpec::approver("u1"), SeatSpec::approver("u2")],
            None,
            tick(&mut t),
        )
        .unwrap();

    let (derived, _) = e
        .add_assignee(
            e.instance.revision,
            &seats[0],
            &["u4".into(), "u5".into()],
            AddAssigneePosition::After,
            Some("action-2"),
            "admin",
            None,
            tick(&mut t),
        )
        .unwrap();
    assert_eq!(derived.len(), 2);
    assert_eq!(
        e.tasks[&seats[0]].successor.as_deref(),
        Some(derived[0].as_str())
    );
    assert!(derived
        .iter()
        .all(|id| e.tasks[id].status == TaskStatus::Scheduled));
    assert!(e.open_tasks_for_at("u4", t).is_empty(), "待激活席位不可见");

    e.approve(e.instance.revision, &seats[0], "u1", None, tick(&mut t))
        .unwrap();
    e.approve(e.instance.revision, &seats[1], "u2", None, tick(&mut t))
        .unwrap();
    assert_eq!(
        e.branches[&branch].status,
        BranchStatus::Open,
        "后加签未完成前不得关闭分支"
    );
    assert_eq!(e.open_tasks_for_at("u4", t).len(), 1);
    assert_eq!(e.open_tasks_for_at("u5", t).len(), 1);

    e.approve(e.instance.revision, &derived[0], "u4", None, tick(&mut t))
        .unwrap();
    assert_eq!(e.tasks[&derived[1]].status, TaskStatus::Cancelled);
    assert_eq!(e.branches[&branch].status, BranchStatus::Completed);
}

/// 原任务被否决：后加签派生任务丢弃并写审计原因。
#[test]
fn after_spawn_is_discarded_when_original_is_rejected() {
    let mut e = start();
    let mut t = 1_000;
    let (branch, _) = e
        .enter_branch(
            e.instance.revision,
            "review",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (_, seats, _) = e
        .open_approval(
            e.instance.revision,
            &branch,
            vec![SeatSpec::approver("u1"), SeatSpec::approver("u2")],
            None,
            tick(&mut t),
        )
        .unwrap();
    let (derived, _) = e
        .add_assignee(
            e.instance.revision,
            &seats[0],
            &["u5".into()],
            AddAssigneePosition::After,
            None,
            "admin",
            None,
            tick(&mut t),
        )
        .unwrap();

    e.reject(e.instance.revision, &seats[0], "u1", None, tick(&mut t))
        .unwrap();
    assert_eq!(e.tasks[&derived[0]].status, TaskStatus::Cancelled);
    assert_eq!(e.branches[&branch].status, BranchStatus::Rejected);
    let discarded = e
        .instance
        .audit
        .iter()
        .find(|a| a.action == "after_spawn_discarded")
        .expect("discard audit");
    assert!(discarded.detail.contains("REJECTED"));
}

/// 后加签先挂起、前加签完成后仍保留；前加签被否决时按上游回退丢弃。
#[test]
fn after_pending_survives_before_completion_and_discards_on_before_reject() {
    let mut e = start();
    let mut t = 1_000;
    let (branch, _) = e
        .enter_branch(
            e.instance.revision,
            "review",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (_, seats, _) = e
        .open_approval(
            e.instance.revision,
            &branch,
            vec![SeatSpec::approver("u1")],
            None,
            tick(&mut t),
        )
        .unwrap();
    let (after, _) = e
        .add_assignee(
            e.instance.revision,
            &seats[0],
            &["u3".into()],
            AddAssigneePosition::After,
            None,
            "admin",
            None,
            tick(&mut t),
        )
        .unwrap();
    let (before, _) = e
        .add_assignee(
            e.instance.revision,
            &seats[0],
            &["u2".into()],
            AddAssigneePosition::Before,
            None,
            "admin",
            None,
            tick(&mut t),
        )
        .unwrap();
    e.approve(e.instance.revision, &before[0], "u2", None, tick(&mut t))
        .unwrap();
    assert_eq!(
        e.tasks[&after[0]].status,
        TaskStatus::Scheduled,
        "前加签完成不得丢弃后加签"
    );
    assert!(e
        .instance
        .audit
        .iter()
        .all(|a| !(a.action == "after_spawn_discarded")));

    // 第二条实例：前加签被否决 ⇒ 后加签丢弃（UPSTREAM_ROLLBACK）。
    let mut rejected = start();
    let mut t2 = 1_000;
    let (b2, _) = rejected
        .enter_branch(
            rejected.instance.revision,
            "review",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t2),
        )
        .unwrap();
    let (_, s2, _) = rejected
        .open_approval(
            rejected.instance.revision,
            &b2,
            vec![SeatSpec::approver("u1")],
            None,
            tick(&mut t2),
        )
        .unwrap();
    let (after2, _) = rejected
        .add_assignee(
            rejected.instance.revision,
            &s2[0],
            &["u3".into()],
            AddAssigneePosition::After,
            None,
            "admin",
            None,
            tick(&mut t2),
        )
        .unwrap();
    let (before2, _) = rejected
        .add_assignee(
            rejected.instance.revision,
            &s2[0],
            &["u2".into()],
            AddAssigneePosition::Before,
            None,
            "admin",
            None,
            tick(&mut t2),
        )
        .unwrap();
    rejected
        .reject(
            rejected.instance.revision,
            &before2[0],
            "u2",
            None,
            tick(&mut t2),
        )
        .unwrap();
    assert_eq!(rejected.tasks[&after2[0]].status, TaskStatus::Cancelled);
    let discarded = rejected
        .instance
        .audit
        .iter()
        .find(|a| a.action == "after_spawn_discarded")
        .expect("discard audit");
    assert!(discarded.detail.contains("UPSTREAM_ROLLBACK"));
}

/// 按 assignee 减签：只移除 PENDING 席位并按剩余席位重算。
#[test]
fn remove_assignee_by_principal_recounts_round() {
    let mut e = start();
    let mut t = 1_000;
    let (branch, _) = e
        .enter_branch(
            e.instance.revision,
            "review",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (_, seats, _) = e
        .open_approval(
            e.instance.revision,
            &branch,
            vec![SeatSpec::approver("u1"), SeatSpec::approver("u2")],
            None,
            tick(&mut t),
        )
        .unwrap();
    let (removed, round, out) = e
        .remove_assignee(
            e.instance.revision,
            &seats[0],
            &["u2".into()],
            "admin",
            tick(&mut t),
        )
        .unwrap();
    assert_eq!(removed, vec![seats[1].clone()]);
    assert_eq!(round.total_count, 1);
    assert!(has_event(&out, "com.china-workflow.approval.round.passed.v1") == false);
    assert_eq!(e.tasks[&seats[1]].status, TaskStatus::Cancelled);
    e.approve(e.instance.revision, &seats[0], "u1", None, tick(&mut t))
        .unwrap();
    assert_eq!(e.branches[&branch].status, BranchStatus::Completed);
}

/// 委派到期：代理权撤销，原审批人继续可批；assignment 读面给出 DELEGATED / CANCELLED。
#[test]
fn delegation_expiry_restores_original_and_terminal_cancels_assignment() {
    let mut e = start();
    let mut t = 1_000;
    let (branch, _) = e
        .enter_branch(
            e.instance.revision,
            "review",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (_, seats, _) = e
        .open_approval(
            e.instance.revision,
            &branch,
            vec![SeatSpec::approver("u1")],
            None,
            tick(&mut t),
        )
        .unwrap();
    let task = seats[0].clone();
    e.delegate(
        e.instance.revision,
        &task,
        "u2",
        Some(1_500),
        "u1",
        tick(&mut t),
    )
    .unwrap();
    assert!(e.open_tasks_for_at("u2", 1_400).len() == 1);
    assert!(e.open_tasks_for_at("u2", 1_600).is_empty());
    assert_eq!(
        e.tasks[&task].assignments(1_400)[0].status,
        AssignmentStatus::Delegated
    );

    e.expire_delegations(e.instance.revision, 1_600).unwrap();
    let delegation = e.tasks[&task].delegation.as_ref().unwrap();
    assert_eq!(
        delegation.revoke_reason.as_deref(),
        Some("DELEGATION_EXPIRED")
    );
    assert_eq!(
        e.tasks[&task].assignments(1_600)[0].status,
        AssignmentStatus::Cancelled
    );
    assert!(e.open_tasks_for_at("u2", 1_600).is_empty());
    assert_eq!(e.open_tasks_for_at("u1", 1_600).len(), 1);

    e.approve(e.instance.revision, &task, "u1", None, tick(&mut t))
        .unwrap();
    assert_eq!(e.tasks[&task].status, TaskStatus::Completed);
}

/// 终态取消代理 assignment：完成时原责任人 assignment 变 CANCELLED（RULE_SATISFIED）。
#[test]
fn terminal_task_cancels_delegated_assignment() {
    let mut e = start();
    let mut t = 1_000;
    let (branch, _) = e
        .enter_branch(
            e.instance.revision,
            "review",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (_, seats, _) = e
        .open_approval(
            e.instance.revision,
            &branch,
            vec![SeatSpec::approver("u1"), SeatSpec::approver("u3")],
            None,
            tick(&mut t),
        )
        .unwrap();
    let task = seats[0].clone();
    e.delegate(
        e.instance.revision,
        &task,
        "u2",
        Some(t + 30_000),
        "u1",
        tick(&mut t),
    )
    .unwrap();
    e.approve(e.instance.revision, &task, "u2", None, tick(&mut t))
        .unwrap();
    e.approve(e.instance.revision, &seats[1], "u3", None, tick(&mut t))
        .unwrap();
    assert_eq!(e.tasks[&task].status, TaskStatus::Completed);
    let assignments = e.tasks[&task].assignments(t);
    let original = assignments
        .iter()
        .find(|a| a.principal == "u1")
        .expect("original assignment");
    assert_eq!(original.status, AssignmentStatus::Cancelled);
    assert_eq!(original.resolved_reason.as_deref(), Some("RULE_SATISFIED"));
}

/// 超时策略 fail / waiting + Resume：失败收口、等待态重建任务。
#[test]
fn timeout_policy_fail_and_waiting_resume() {
    let mut e = start();
    let mut t = 1_000;
    let (branch, _) = e
        .enter_branch(
            e.instance.revision,
            "review",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy {
                timeout_policy: TimeoutPolicy::Fail,
                timeout_ms: Some(10_000),
                ..Default::default()
            },
            tick(&mut t),
        )
        .unwrap();
    e.open_approval(
        e.instance.revision,
        &branch,
        vec![SeatSpec::approver("u1")],
        Some(1_500),
        tick(&mut t),
    )
    .unwrap();
    e.timeout_fail(e.instance.revision, "TASK_OVERDUE", 1_600)
        .unwrap();
    assert_eq!(e.instance.status, InstanceStatus::Terminated);

    let mut w = start();
    let mut tw = 1_000;
    let (wb, _) = w
        .enter_branch(
            w.instance.revision,
            "review",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy {
                timeout_policy: TimeoutPolicy::Waiting,
                timeout_ms: Some(10_000),
                ..Default::default()
            },
            tick(&mut tw),
        )
        .unwrap();
    let (_, old, _) = w
        .open_approval(
            w.instance.revision,
            &wb,
            vec![SeatSpec::approver("u1")],
            Some(1_500),
            tick(&mut tw),
        )
        .unwrap();
    w.timeout_wait(w.instance.revision, "TASK_OVERDUE", 1_600)
        .unwrap();
    assert_eq!(w.instance.status, InstanceStatus::Waiting);
    assert_eq!(w.tasks[&old[0]].status, TaskStatus::Cancelled);
    assert!(matches!(
        w.approve(w.instance.revision, &old[0], "u1", None, 1_700)
            .unwrap_err(),
        ApprovalError::InvalidTransition(_)
    ));

    let (new_tasks, _) = w
        .resume_waiting(w.instance.revision, 5_000, "u1", "extended", 2_000)
        .unwrap();
    assert_eq!(w.instance.status, InstanceStatus::Running);
    assert_eq!(new_tasks.len(), 1);
    assert_ne!(new_tasks[0], old[0]);
    assert_eq!(w.tasks[&new_tasks[0]].deadline, Some(7_000));
    assert_eq!(w.open_tasks_for_at("u1", 2_000).len(), 1);
}

/// Resume 仅 Waiting 有效：Running 实例恢复返回冲突。
#[test]
fn resume_requires_waiting_instance() {
    let mut e = start();
    let mut t = 1_000;
    let (branch, _) = e
        .enter_branch(
            e.instance.revision,
            "review",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    e.open_approval(
        e.instance.revision,
        &branch,
        vec![SeatSpec::approver("u1")],
        None,
        tick(&mut t),
    )
    .unwrap();
    assert!(matches!(
        e.resume_waiting(e.instance.revision, 1_000, "u1", "x", tick(&mut t))
            .unwrap_err(),
        ApprovalError::InvalidTransition(_)
    ));
}

/// 前加签席位被整批减签：门禁视为满足，原轮次恢复而不是永久挂起。
#[test]
fn removing_before_spawn_resumes_original_round() {
    let mut e = start();
    let mut t = 1_000;
    let (branch, _) = e
        .enter_branch(
            e.instance.revision,
            "review",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (_, seats, _) = e
        .open_approval(
            e.instance.revision,
            &branch,
            vec![SeatSpec::approver("u1")],
            None,
            tick(&mut t),
        )
        .unwrap();
    let original = seats[0].clone();
    let (before, _) = e
        .add_assignee(
            e.instance.revision,
            &original,
            &["u2".into()],
            AddAssigneePosition::Before,
            None,
            "admin",
            None,
            tick(&mut t),
        )
        .unwrap();

    e.remove_assignee(
        e.instance.revision,
        &before[0],
        &["u2".into()],
        "admin",
        tick(&mut t),
    )
    .unwrap();
    assert_eq!(e.tasks[&original].status, TaskStatus::Created);
    assert_eq!(e.open_tasks_for_at("u1", t).len(), 1);
    e.approve(e.instance.revision, &original, "u1", None, tick(&mut t))
        .unwrap();
    assert_eq!(e.branches[&branch].status, BranchStatus::Completed);
}

/// 等待恢复后仍应执行已排队的后加签轮次（恢复轮次序号大于后加签）。
#[test]
fn resume_waiting_keeps_scheduled_after_alive() {
    let mut e = start();
    let mut t = 1_000;
    let (branch, _) = e
        .enter_branch(
            e.instance.revision,
            "review",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy {
                timeout_policy: TimeoutPolicy::Waiting,
                timeout_ms: Some(60_000),
                ..Default::default()
            },
            tick(&mut t),
        )
        .unwrap();
    let (_, seats, _) = e
        .open_approval(
            e.instance.revision,
            &branch,
            vec![SeatSpec::approver("u1")],
            Some(1_500),
            tick(&mut t),
        )
        .unwrap();
    let (after, _) = e
        .add_assignee(
            e.instance.revision,
            &seats[0],
            &["u3".into()],
            AddAssigneePosition::After,
            None,
            "admin",
            None,
            tick(&mut t),
        )
        .unwrap();
    e.timeout_wait(e.instance.revision, "TASK_OVERDUE", 1_600)
        .unwrap();
    assert_eq!(e.tasks[&after[0]].status, TaskStatus::Scheduled);
    let (new_seats, _) = e
        .resume_waiting(e.instance.revision, 5_000, "u1", "extended", 2_000)
        .unwrap();
    e.approve(e.instance.revision, &new_seats[0], "u1", None, tick(&mut t))
        .unwrap();
    assert_eq!(
        e.tasks[&after[0]].status,
        TaskStatus::Created,
        "恢复后的原轮次通过时必须激活排队的后加签"
    );
    assert_eq!(e.branches[&branch].status, BranchStatus::Open);
}

/// 前加签挂起期间允许催办（只写审计），终态席位拒绝。
#[test]
fn urge_allowed_while_suspended_and_rejected_when_terminal() {
    let mut e = start();
    let mut t = 1_000;
    let (branch, _) = e
        .enter_branch(
            e.instance.revision,
            "review",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            ApprovalPolicy::default(),
            tick(&mut t),
        )
        .unwrap();
    let (_, seats, _) = e
        .open_approval(
            e.instance.revision,
            &branch,
            vec![SeatSpec::approver("u1")],
            None,
            tick(&mut t),
        )
        .unwrap();
    let original = seats[0].clone();
    let (before, _) = e
        .add_assignee(
            e.instance.revision,
            &original,
            &["u2".into()],
            AddAssigneePosition::Before,
            None,
            "admin",
            None,
            tick(&mut t),
        )
        .unwrap();

    e.urge(e.instance.revision, &original, "u1", "hurry", tick(&mut t))
        .unwrap();
    let audit = e
        .instance
        .audit
        .iter()
        .find(|a| a.action == "urge")
        .expect("urge audit");
    assert!(audit.detail.contains("SUSPENDED"));

    e.approve(e.instance.revision, &before[0], "u2", None, tick(&mut t))
        .unwrap();
    e.approve(e.instance.revision, &original, "u1", None, tick(&mut t))
        .unwrap();
    assert!(matches!(
        e.urge(e.instance.revision, &original, "u1", "again", tick(&mut t))
            .unwrap_err(),
        ApprovalError::InvalidTransition(_)
    ));
}
