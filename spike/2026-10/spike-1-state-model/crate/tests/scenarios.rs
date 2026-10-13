//! Spike 1 P0 场景用例（会签+加签 / 驳回至节点 / 一票否决 / 或签 / 转办）
//! 与模型不变量用例（CAS、阅签、票签、委派、撤回、终止、超时、白名单/jump 守卫）。
//! 运行：`cargo test`（详细日志见 ../evidence/）。

use spike_approval_model::*;

fn start() -> Engine {
    Engine::start(
        "eis",
        "leave-approval",
        "1.0.0",
        "zhangsan",
        Scope::new(),
        1_000,
    )
}

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

/// 驳回至节点：A 通过 → B 驳回至 A → A 重开（attempt=2）→ 走完 B → 实例通过。
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
    assert!(matches!(err, Error::NotAuthorized(_)));
    e.approve(e.instance.revision, &task, "carol", None, tick(&mut t))
        .unwrap();
    assert_eq!(e.instance.status, InstanceStatus::Running);
    e.finish(e.instance.revision, tick(&mut t)).unwrap();
    assert_eq!(e.instance.status, InstanceStatus::Completed);
}

// ------------------------------------------------------------ 其余不变量

/// 委派：被委派人代批（记录 on_behalf_of）；原审批人权限保留。
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

/// 撤回：仅发起人；级联取消未决席位。
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
    assert!(matches!(err, Error::NotAuthorized(_)));
    let out = e
        .withdraw(e.instance.revision, "zhangsan", tick(&mut t))
        .unwrap();
    assert_eq!(e.instance.status, InstanceStatus::Cancelled);
    assert_eq!(e.branches[&b].status, BranchStatus::Cancelled);
    assert_eq!(e.tasks[&seats[0]].status, TaskStatus::Cancelled);
    assert!(has_event(&out, "com.china-workflow.instance.cancelled"));
}

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
    assert!(matches!(err, Error::StaleRevision { .. }));
    assert_eq!(e.tasks[&seats[1]].status, TaskStatus::Created);
    assert_eq!(e.rounds.values().next().unwrap().completed_count, 1);
    e.approve(e.instance.revision, &seats[1], "bob", None, tick(&mut t))
        .unwrap();
    assert_eq!(e.instance.status, InstanceStatus::Running);
    e.finish(e.instance.revision, tick(&mut t)).unwrap();
    assert_eq!(e.instance.status, InstanceStatus::Completed);
}

/// 终止：管理路径级联，终态实例拒绝后续命令。
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
    assert!(matches!(err, Error::InvalidTransition(_)));
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
    assert!(matches!(err, Error::NotAuthorized(_)));
    e.claim(e.instance.revision, &task, "alice", tick(&mut t))
        .unwrap();
    let err = e
        .claim(e.instance.revision, &task, "bob", tick(&mut t))
        .unwrap_err();
    assert!(matches!(err, Error::InvalidTransition(_)));
    let err = e
        .approve(e.instance.revision, &task, "bob", None, tick(&mut t))
        .unwrap_err();
    assert!(matches!(err, Error::NotAuthorized(_)));
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
    assert!(matches!(err, Error::InvalidArgument(_)));
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

/// 驳回白名单与 jump 上限：未进入过的节点不可作为目标；超限拒绝。
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
    assert!(matches!(err, Error::InvalidArgument(_)));
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
    assert!(matches!(err, Error::InvalidArgument(_)));
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

/// 证据快照：完整场景可序列化为 JSON（供报告原始数据）。
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
    let snap = e.snapshot_json();
    assert!(snap.get("instance").is_some());
    assert!(snap.get("branches").is_some());
    assert!(snap.get("tasks").is_some());
    assert!(snap.get("rounds").is_some());
    assert_eq!(snap["instance"]["status"].as_str(), Some("completed"));
}
