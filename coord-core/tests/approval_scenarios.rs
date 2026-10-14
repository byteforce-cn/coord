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
    assert!(has_event(&out, "com.china-workflow.approval.round.passed"));
    assert_eq!(e.instance.status, InstanceStatus::Running);
    let out = e.finish(e.instance.revision, tick(&mut t)).unwrap();
    assert_eq!(e.instance.status, InstanceStatus::Completed);
    assert!(has_event(&out, "com.china-workflow.instance.completed"));
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
        "com.china-workflow.approval.round.rejected"
    ));
    assert!(has_event(&out, "com.china-workflow.instance.rejected"));
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
    assert!(has_event(&out.1, "com.china-workflow.instance.returned"));
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
        .withdraw(e.instance.revision, "lisi", tick(&mut t))
        .unwrap_err();
    assert!(matches!(err, ApprovalError::NotAuthorized(_)));
    let out = e
        .withdraw(e.instance.revision, "zhangsan", tick(&mut t))
        .unwrap();
    assert_eq!(e.instance.status, InstanceStatus::Cancelled);
    assert_eq!(e.branches[&b].status, BranchStatus::Cancelled);
    assert_eq!(e.tasks[&seats[0]].status, TaskStatus::Cancelled);
    assert!(has_event(&out, "com.china-workflow.instance.cancelled"));
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
    assert!(has_event(&out, "com.china-workflow.instance.terminated"));
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
        "com.china-workflow.approval.round.rejected"
    ));
}
