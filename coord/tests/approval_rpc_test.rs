//! M2-C 端到端业务验收：审批 v2 命令面驱动真实工作流（真 Raft 存储 + 真 durable 引擎）。
//!
//! 每个用例独立起单节点 Raft（`common::start_test_server`）并在其上装配生产口径的
//! durable 引擎（`WorkflowEngineService::new_with_kv_store_durable`）——定义、实例、
//! 审批聚合、outbox 全部落在真实 KV，证据经 `KvOutboxStore` / `get_instance_detail`
//! 直连核对。测试以 `--test-threads=1` 运行。
//!
//! 传输模式：审批命令经服务方法**进程内**调用并显式传 actor，而非跨线 tonic——
//! `common::start_test_server` 只挂载 KV/Txn/Lease/Watch/Maintenance，未注册 workflow
//! 数据面；且 server 侧鉴权关闭时跨线请求拿不到 `GatewayIdentity`，actor 退化为
//! `"anonymous"`。身份抽取由 workflow handler 的 `GatewayIdentity` 单测覆盖，真实 gRPC
//! 传输由 Java SDK 契约测试覆盖。

mod common;

use std::sync::Arc;
use std::time::Duration;

use coord_agent::cache::AgentCache;
use coord_agent::service::BaseService;
use coord_agent::services::mq::MessageQueueService;
use coord_agent::services::outbox_kv::KvOutboxStore;
use coord_agent::services::workflow::phase4::{DurableEngineOptions, WorkflowEngineService};
use coord_agent::AgentInner;
use coord_agent::WorkflowDispatchConfig;
use coord_client::config::Config;
use coord_client::Client;
use coord_core::workflow::approval::bridge::{ApprovalCommand, BridgeError};
use coord_core::workflow::approval::model::{BranchStatus, SeatSpec, TaskKind, TaskStatus};
use coord_core::workflow::durable::OutboxStore;
use coord_core::workflow::model::InstanceStatus;
use serde_json::json;

/// 审批侧通过事件（`events::APPROVAL_EVENT_TYPES`）。
const ROUND_PASSED: &str = "com.china-workflow.approval.round.passed.v1";
/// 审批实例收口事件（`finish_if_complete` → `Engine::finish`）。
const APPROVAL_COMPLETED: &str = "com.china-workflow.instance.completed.v1";

/// 进程内单节点服务 + durable 引擎；**必须持有到测试结束**（drop 会关闭 gRPC/raft）。
struct TestEnv {
    svc: WorkflowEngineService,
    outbox: Arc<KvOutboxStore>,
    _server_tmp: tempfile::TempDir,
    _mq_tmp: tempfile::TempDir,
    _shutdown: tokio::sync::oneshot::Sender<()>,
    _grpc: tokio::task::JoinHandle<()>,
    _raft: tokio::task::JoinHandle<()>,
}

impl TestEnv {
    /// 部署定义并以 `initiator` 启动实例，返回 workflow 实例 id。
    async fn deploy_start(&self, namespace: &str, yaml: &str, initiator: &str) -> String {
        let def_id = self
            .svc
            .deploy_definition(namespace, yaml)
            .await
            .expect("deploy definition");
        let inst = self
            .svc
            .start_instance_as(&def_id, json!({}), Some(initiator))
            .await
            .expect("start instance");
        inst.id
    }

    /// 有界等待实例进入目标状态（线性一致读经 store，绕过瞬时缓存滞后）。
    async fn await_status(&self, id: &str, want: InstanceStatus) {
        common::wait_until(
            || async { matches!(self.svc.get_instance(id).await, Ok(Some(i)) if i.status == want) },
            Duration::from_secs(20),
            &format!("instance {id} reach {want:?}"),
        )
        .await;
    }

    /// 某实例在 outbox 中指定事件类型的记录数。
    async fn event_count(&self, instance_id: &str, event_type: &str) -> usize {
        OutboxStore::list_all(&*self.outbox)
            .await
            .expect("list outbox")
            .iter()
            .filter(|r| r.aggregate_id == instance_id && r.event_type == event_type)
            .count()
    }
}

async fn setup() -> TestEnv {
    let (addr, shutdown, grpc, raft, server_tmp) = common::start_test_server().await;
    common::wait_tcp_ready(&addr, Duration::from_secs(10)).await;
    let client = Client::connect_direct(Config::new(vec![addr]))
        .await
        .expect("connect direct");
    let inner = Arc::new(AgentInner {
        client,
        cache: AgentCache::new(256, 0, 32, 0),
    });

    // durable 装配早于 `start_all()`：MQ 必须先 start()，引擎 `start()` 才有 topic 可建。
    let mq_tmp = tempfile::tempdir().expect("mq tmpdir");
    let mq = Arc::new(MessageQueueService::new(
        mq_tmp.path().to_path_buf(),
        64 * 1024 * 1024,
    ));
    mq.start().await.expect("start mq");

    let svc = WorkflowEngineService::new_with_kv_store_durable(
        Arc::clone(&inner),
        &WorkflowDispatchConfig::default(),
        DurableEngineOptions {
            mq,
            topic: "com.china-workflow.acceptance.events".to_string(),
            // 本套件核对落库 outbox，不依赖 relay 投递。
            relay_enabled: false,
            relay_interval_ms: 50,
            approval_roles: std::collections::HashMap::new(),
        },
    );
    svc.start().await.expect("engine start");

    let outbox = Arc::new(KvOutboxStore::new(inner));
    TestEnv {
        svc,
        outbox,
        _server_tmp: server_tmp,
        _mq_tmp: mq_tmp,
        _shutdown: shutdown,
        _grpc: grpc,
        _raft: raft,
    }
}

// ───────────────────────────────────────────────────────────────── 定义模板

/// 会签（u1+u2, mode=all）+ `onPass` 跳转：跳过一个中间任务以证明走了 `on_pass` 路由。
const PASS_YAML: &str = r#"
document:
  dsl: "1.0.3"
  namespace: acceptance
  name: approve-pass
  version: "1.0.0"
do:
  - review:
      approve:
        approvers: ["u1", "u2"]
        mode: "all"
        onPass: "landed"
  - skipped:
      set:
        variable: skipped_marker
        value: '"should-not-run"'
  - landed:
      set:
        variable: routed
        value: '"on-pass-target"'
"#;

/// 一票否决 + `onReject` 分支。
const REJECT_YAML: &str = r#"
document:
  dsl: "1.0.3"
  namespace: acceptance
  name: approve-reject
  version: "1.0.0"
do:
  - review:
      approve:
        approvers: ["u1", "u2"]
        mode: "all"
        veto: true
        onReject: "rejected"
  - rejected:
      set:
        variable: reject_branch
        value: '"taken"'
  - after_reject:
      set:
        variable: after_reject
        value: '"ran"'
"#;

/// 单席位 + `signAdmins`：撤回 / 终止授权矩阵用例。
const AUTHZ_YAML: &str = r#"
document:
  dsl: "1.0.3"
  namespace: acceptance
  name: approve-authz
  version: "1.0.0"
do:
  - review:
      approve:
        approvers: ["u1"]
        signAdmins: ["admin"]
  - done:
      set:
        variable: done
        value: '"yes"'
"#;

/// 单席位会签：加签后新席位可批。
const ADD_SIGN_YAML: &str = r#"
document:
  dsl: "1.0.3"
  namespace: acceptance-addsign
  name: approve-add-sign
  version: "1.0.0"
do:
  - review:
      approve:
        approvers: ["u1"]
        mode: "all"
  - done:
      set:
        variable: done
        value: '"yes"'
"#;

/// 两个串联 approve 节点：`ReturnToNode` 从后节点驳回至前节点（attempt 递增）。
const RETURN_YAML: &str = r#"
document:
  dsl: "1.0.3"
  namespace: acceptance-return
  name: approve-return
  version: "1.0.0"
do:
  - first:
      approve:
        approvers: ["u1"]
        mode: "all"
  - second:
      approve:
        approvers: ["u2"]
        mode: "all"
  - done:
      set:
        variable: done
        value: '"yes"'
"#;

/// 双席位会签（并发 / 幂等判据共用）。
const QUORUM_YAML: &str = r#"
document:
  dsl: "1.0.3"
  namespace: acceptance-quorum
  name: approve-quorum
  version: "1.0.0"
do:
  - review:
      approve:
        approvers: ["u1", "u2"]
        mode: "all"
  - done:
      set:
        variable: done
        value: '"yes"'
"#;

/// 秒级截止：座位到期后无人为动作绝不自动通过（Q4）。
const DEADLINE_YAML: &str = r#"
document:
  dsl: "1.0.3"
  namespace: acceptance-deadline
  name: approve-deadline
  version: "1.0.0"
do:
  - review:
      approve:
        approvers: ["u1"]
        mode: "all"
        deadline:
          seconds: 2
  - done:
      set:
        variable: done
        value: '"yes"'
"#;

/// 单节点会签 + 驳回重开：重开时复用目标节点上一次次尝试的席位。
const SELF_RETURN_YAML: &str = r#"
document:
  dsl: "1.0.3"
  namespace: acceptance-return-seats
  name: approve-return-seats
  version: "1.0.0"
do:
  - review:
      approve:
        approvers: ["u1"]
        mode: "all"
  - done:
      set:
        variable: done
        value: '"yes"'
"#;

/// 审批通过后进入下游 `wait`：工作流仍非终态、审批聚合未收口（取消用例）。
const HOLD_AFTER_APPROVE_YAML: &str = r#"
document:
  dsl: "1.0.3"
  namespace: acceptance-hold
  name: approve-then-hold
  version: "1.0.0"
do:
  - review:
      approve:
        approvers: ["u1"]
        mode: "all"
        onPass: "hold"
  - hold:
      wait:
        seconds: 60
"#;

/// 审批通过后进入下游 `raise`：工作流 fault（聚合收口用例）。
const FAULT_AFTER_APPROVE_YAML: &str = r#"
document:
  dsl: "1.0.3"
  namespace: acceptance-fault
  name: approve-then-fault
  version: "1.0.0"
do:
  - review:
      approve:
        approvers: ["u1"]
        mode: "all"
        onPass: "boom"
  - boom:
      raise:
        type: "business_error"
        title: "downstream fault"
        status: 500
"#;

/// 取某实例在 detail 中的席位 id（按 assignee 匹配）。
fn task_id_for(
    detail: &coord_agent::services::workflow::phase4::InstanceDetailView,
    assignee: &str,
) -> String {
    detail
        .tasks
        .iter()
        .find(|t| t.assignee.as_deref() == Some(assignee))
        .unwrap_or_else(|| panic!("no seat assigned to {assignee}: {:?}", detail.tasks))
        .id
        .clone()
}

/// 取某实例在 detail 中**指定分支**、指定责任人的席位 id（重开 attempt 下同责任人有多席，
/// 单按 assignee 会匹配到被取代的旧 attempt）。
fn task_id_for_branch(
    detail: &coord_agent::services::workflow::phase4::InstanceDetailView,
    branch_id: &str,
    assignee: &str,
) -> String {
    detail
        .tasks
        .iter()
        .find(|t| t.branch_id == branch_id && t.assignee.as_deref() == Some(assignee))
        .unwrap_or_else(|| {
            panic!(
                "no seat on branch {branch_id} assigned to {assignee}: {:?}",
                detail.tasks
            )
        })
        .id
        .clone()
}

// ─────────────────────────────────────────────────────────── 判据 1：通过收口

/// 会签全票通过 → 工作流 `COMPLETED`、审批实例 `COMPLETED`、`on_pass` 路由生效，
/// outbox 含 `round.passed.v1` + `instance.completed.v1` 各一条。
#[tokio::test(flavor = "multi_thread")]
async fn approve_rpc_flow_completes_workflow() {
    let env = setup().await;
    let wf_id = env.deploy_start("acceptance", PASS_YAML, "origin").await;
    env.await_status(&wf_id, InstanceStatus::Suspended).await;

    let detail = env.svc.get_instance_detail(&wf_id).await.expect("detail");
    assert_eq!(detail.status, "RUNNING", "审批实例开启后为 Running");
    assert_eq!(detail.initiator, "origin");
    assert_eq!(detail.tasks.len(), 2, "u1/u2 两个可计票席位");

    let pending = env
        .svc
        .list_pending_tasks("u1", 0, None)
        .await
        .expect("list pending")
        .0;
    let mine: Vec<_> = pending.iter().filter(|t| t.instance_id == wf_id).collect();
    assert_eq!(mine.len(), 1, "u1 恰见 1 条待办");
    assert_eq!(mine[0].node, "review");

    let u1_task = task_id_for(&detail, "u1");
    let u2_task = task_id_for(&detail, "u2");

    env.svc
        .approval_command(
            &wf_id,
            "u1",
            ApprovalCommand::Approve {
                task_id: &u1_task,
                comment: Some("ok"),
            },
            None,
        )
        .await
        .expect("u1 approve");
    // 单人通过不会收口（mode=all）。
    let after_u1 = env.svc.get_instance_detail(&wf_id).await.expect("detail");
    assert_eq!(after_u1.status, "RUNNING");
    let round = &after_u1.rounds[0];
    assert_eq!((round.total_count, round.completed_count), (2, 1));

    env.svc
        .approval_command(
            &wf_id,
            "u2",
            ApprovalCommand::Approve {
                task_id: &u2_task,
                comment: None,
            },
            None,
        )
        .await
        .expect("u2 approve");

    env.await_status(&wf_id, InstanceStatus::Completed).await;

    let final_detail = env.svc.get_instance_detail(&wf_id).await.expect("detail");
    assert_eq!(
        final_detail.status, "COMPLETED",
        "工作流走到末端时收口审批实例"
    );

    // on_pass 路由：跳到 `landed`，中间任务 `skipped` 从未执行。
    let inst = env
        .svc
        .get_instance(&wf_id)
        .await
        .unwrap()
        .expect("instance");
    assert_eq!(inst.context["routed"], json!("on-pass-target"));
    assert!(
        inst.context.get("skipped_marker").is_none(),
        "on_pass 必须跳过中间任务: {:?}",
        inst.context
    );

    assert_eq!(env.event_count(&wf_id, ROUND_PASSED).await, 1);
    assert_eq!(env.event_count(&wf_id, APPROVAL_COMPLETED).await, 1);
}

// ─────────────────────────────────────────────────────── 判据 2：否决走分支

/// 一票否决 → 审批实例 `REJECTED`，工作流按 `on_reject` 分支继续并最终 `COMPLETED`。
#[tokio::test(flavor = "multi_thread")]
async fn reject_routes_to_on_reject_branch() {
    let env = setup().await;
    let wf_id = env.deploy_start("acceptance", REJECT_YAML, "origin").await;
    env.await_status(&wf_id, InstanceStatus::Suspended).await;

    let detail = env.svc.get_instance_detail(&wf_id).await.expect("detail");
    let u1_task = task_id_for(&detail, "u1");
    let (_, status) = env
        .svc
        .approval_command(
            &wf_id,
            "u1",
            ApprovalCommand::Reject {
                task_id: &u1_task,
                comment: Some("reject"),
            },
            None,
        )
        .await
        .expect("u1 reject");
    assert_eq!(status, "REJECTED", "否决即审批实例终态");

    env.await_status(&wf_id, InstanceStatus::Completed).await;
    let inst = env
        .svc
        .get_instance(&wf_id)
        .await
        .unwrap()
        .expect("instance");
    assert_eq!(inst.context["reject_branch"], json!("taken"));
    assert_eq!(inst.context["after_reject"], json!("ran"));

    let final_detail = env.svc.get_instance_detail(&wf_id).await.expect("detail");
    assert_eq!(final_detail.status, "REJECTED");
}

// ────────────────────────────────────────────── 判据 3：加签 + 驳回至节点

/// `AddSign` 后新席位可批并推动节点通过；`ReturnToNode` 重开前节点（attempt 递增）。
#[tokio::test(flavor = "multi_thread")]
async fn add_sign_and_return_to_node_paths() {
    let env = setup().await;

    // 加签：单席位 u1，加签 u3 后两人都需通过（mode=all）。
    let wf_id = env
        .deploy_start("acceptance-addsign", ADD_SIGN_YAML, "origin")
        .await;
    env.await_status(&wf_id, InstanceStatus::Suspended).await;
    let detail = env.svc.get_instance_detail(&wf_id).await.expect("detail");
    let branch_id = detail.branches[0].id.clone();

    env.svc
        .approval_command(
            &wf_id,
            "origin",
            ApprovalCommand::AddSign {
                branch_id: &branch_id,
                seats: vec![SeatSpec::approver("u3")],
                comment: Some("add"),
            },
            None,
        )
        .await
        .expect("add sign by initiator");

    let after_add = env.svc.get_instance_detail(&wf_id).await.expect("detail");
    assert_eq!(after_add.rounds[0].total_count, 2, "加签席位计入本轮");
    let u1_task = task_id_for(&after_add, "u1");
    let u3_task = task_id_for(&after_add, "u3");

    env.svc
        .approval_command(
            &wf_id,
            "u1",
            ApprovalCommand::Approve {
                task_id: &u1_task,
                comment: None,
            },
            None,
        )
        .await
        .expect("u1 approve");
    let mid = env.svc.get_instance_detail(&wf_id).await.expect("detail");
    assert_eq!(mid.status, "RUNNING", "加签后单票不足以通过");
    assert_eq!(mid.rounds[0].completed_count, 1);

    env.svc
        .approval_command(
            &wf_id,
            "u3",
            ApprovalCommand::Approve {
                task_id: &u3_task,
                comment: None,
            },
            None,
        )
        .await
        .expect("u3 approve");
    env.await_status(&wf_id, InstanceStatus::Completed).await;

    // 驳回至节点：两个串联 approve，从后节点驳回至前节点 ⇒ 前节点重开且 attempt 递增。
    let ret_id = env
        .deploy_start("acceptance-return", RETURN_YAML, "origin")
        .await;
    env.await_status(&ret_id, InstanceStatus::Suspended).await;
    let first = env.svc.get_instance_detail(&ret_id).await.expect("detail");
    let first_task = task_id_for(&first, "u1");
    env.svc
        .approval_command(
            &ret_id,
            "u1",
            ApprovalCommand::Approve {
                task_id: &first_task,
                comment: None,
            },
            None,
        )
        .await
        .expect("first pass");

    // 等到后节点开启。
    common::wait_until(
        || async {
            env.svc
                .get_instance_detail(&ret_id)
                .await
                .map(|d| {
                    d.branches
                        .iter()
                        .any(|b| b.node == "second" && b.status == BranchStatus::Open)
                })
                .unwrap_or(false)
        },
        Duration::from_secs(20),
        "second approve node opens",
    )
    .await;

    let second = env.svc.get_instance_detail(&ret_id).await.expect("detail");
    let second_branch = second
        .branches
        .iter()
        .find(|b| b.node == "second")
        .expect("second branch");
    assert_eq!(second_branch.attempt, 1);

    let (_, status) = env
        .svc
        .approval_command(
            &ret_id,
            "origin",
            ApprovalCommand::ReturnToNode {
                branch_id: &second_branch.id,
                target_node: "first",
                comment: Some("rework"),
            },
            None,
        )
        .await
        .expect("return to node");
    assert_eq!(status, "RUNNING", "驳回重开不产生实例终态");

    let returned = env.svc.get_instance_detail(&ret_id).await.expect("detail");
    let first_attempts: Vec<u32> = returned
        .branches
        .iter()
        .filter(|b| b.node == "first")
        .map(|b| b.attempt)
        .collect();
    assert!(
        first_attempts.contains(&1) && first_attempts.contains(&2),
        "前节点必须重开：attempts={first_attempts:?}"
    );
    let reopened = returned
        .branches
        .iter()
        .find(|b| b.node == "first" && b.attempt == 2)
        .expect("reopened branch");
    assert_eq!(reopened.status, BranchStatus::Open);
    let old = returned
        .branches
        .iter()
        .find(|b| b.node == "first" && b.attempt == 1)
        .expect("superseded branch");
    assert_eq!(old.status, BranchStatus::Superseded);
}

// ────────────────────────────────────────── 判据 4：撤回 / 终止授权矩阵

/// 非发起人 `Withdraw` ⇒ `PERMISSION_DENIED`；发起人成功且工作流 fault；
/// 非 `sign_admins` `Terminate` ⇒ `PERMISSION_DENIED`。
#[tokio::test(flavor = "multi_thread")]
async fn withdraw_and_terminate_authorization() {
    let env = setup().await;

    let wf_id = env.deploy_start("acceptance", AUTHZ_YAML, "origin").await;
    env.await_status(&wf_id, InstanceStatus::Suspended).await;

    let denied = env
        .svc
        .approval_command(&wf_id, "u1", ApprovalCommand::Withdraw, None)
        .await
        .expect_err("non-initiator withdraw must be denied");
    assert!(
        matches!(denied, BridgeError::NotAuthorized(_)),
        "expected NotAuthorized, got {denied:?}"
    );

    // 撤回未被授权 ⇒ 状态零变化，实例仍挂起。
    assert_eq!(
        env.svc.get_instance_detail(&wf_id).await.unwrap().status,
        "RUNNING"
    );

    let (_, status) = env
        .svc
        .approval_command(&wf_id, "origin", ApprovalCommand::Withdraw, None)
        .await
        .expect("initiator withdraw");
    assert_eq!(status, "CANCELLED");
    // 无 on_reject 目标 ⇒ 服务层以 approval_cancelled 置 fault。
    env.await_status(&wf_id, InstanceStatus::Failed).await;

    // 终止授权：sign_admins 之外拒绝。
    let term_id = env.deploy_start("acceptance", AUTHZ_YAML, "origin").await;
    env.await_status(&term_id, InstanceStatus::Suspended).await;
    let denied = env
        .svc
        .approval_command(
            &term_id,
            "u1",
            ApprovalCommand::Terminate { reason: "x" },
            None,
        )
        .await
        .expect_err("non sign-admin terminate must be denied");
    assert!(
        matches!(denied, BridgeError::NotAuthorized(_)),
        "expected NotAuthorized, got {denied:?}"
    );

    let (_, status) = env
        .svc
        .approval_command(
            &term_id,
            "admin",
            ApprovalCommand::Terminate { reason: "ops" },
            None,
        )
        .await
        .expect("sign admin terminate");
    assert_eq!(status, "TERMINATED");
    env.await_status(&term_id, InstanceStatus::Failed).await;
}

// ──────────────────────────────────────────────────────── 判据 5：幂等键重放

/// 同幂等键连发两次 `Approve` ⇒ 两次响应一致、轮次计数只加一、不产生重复收口事件。
#[tokio::test(flavor = "multi_thread")]
async fn idempotency_key_replay_single_write() {
    let env = setup().await;
    let wf_id = env
        .deploy_start("acceptance-quorum", QUORUM_YAML, "origin")
        .await;
    env.await_status(&wf_id, InstanceStatus::Suspended).await;

    let detail = env.svc.get_instance_detail(&wf_id).await.expect("detail");
    let u1_task = task_id_for(&detail, "u1");

    let first = env
        .svc
        .approval_command(
            &wf_id,
            "u1",
            ApprovalCommand::Approve {
                task_id: &u1_task,
                comment: None,
            },
            Some("idem-key-1"),
        )
        .await
        .expect("first approve");
    let replay = env
        .svc
        .approval_command(
            &wf_id,
            "u1",
            ApprovalCommand::Approve {
                task_id: &u1_task,
                comment: None,
            },
            Some("idem-key-1"),
        )
        .await
        .expect("replayed approve");
    assert_eq!(first, replay, "同键重放必须返回首次响应");

    let after = env.svc.get_instance_detail(&wf_id).await.expect("detail");
    assert_eq!(after.rounds[0].completed_count, 1, "重放不得重复计数");
    assert_eq!(
        env.event_count(&wf_id, ROUND_PASSED).await,
        0,
        "双席位仅一票，轮次未通过"
    );
}

// ────────────────────────────────────────── 判据 6：并发命令不双重应用

/// 同一席位并发两票（同 actor、不同 comment）：恰一成功，另一条落在既有错误矩阵
/// （`StaleRevision→ABORTED` / `InvalidTransition→FAILED_PRECONDITION`），轮次计数只加一。
///
/// 命令请求面不携带修订号（身份从 CCT 取），因此最近的可复现并发情形就是同席位同时投票；
/// `StaleRevision→ABORTED` 的逐字映射由 workflow handler 的 `map_approval_error_matrix`
/// 单测覆盖。
#[tokio::test(flavor = "multi_thread")]
async fn stale_revision_maps_aborted() {
    let env = setup().await;
    let wf_id = env
        .deploy_start("acceptance-quorum", QUORUM_YAML, "origin")
        .await;
    env.await_status(&wf_id, InstanceStatus::Suspended).await;

    let detail = env.svc.get_instance_detail(&wf_id).await.expect("detail");
    let u1_task = task_id_for(&detail, "u1");

    let (a, b) = tokio::join!(
        env.svc.approval_command(
            &wf_id,
            "u1",
            ApprovalCommand::Approve {
                task_id: &u1_task,
                comment: Some("first"),
            },
            None,
        ),
        env.svc.approval_command(
            &wf_id,
            "u1",
            ApprovalCommand::Approve {
                task_id: &u1_task,
                comment: Some("second"),
            },
            None,
        ),
    );
    let outcomes = [a, b];
    let ok = outcomes.iter().filter(|r| r.is_ok()).count();
    assert_eq!(ok, 1, "并发同票恰一成功: {outcomes:?}");
    let loser = outcomes
        .iter()
        .find(|r| r.is_err())
        .expect("a loser")
        .as_ref()
        .unwrap_err();
    assert!(
        matches!(
            loser,
            BridgeError::InvalidTransition(_) | BridgeError::Stale(_)
        ),
        "输家必须落在既有错误矩阵（StaleRevision→ABORTED / InvalidTransition）：{loser:?}"
    );

    let after = env.svc.get_instance_detail(&wf_id).await.expect("detail");
    assert_eq!(after.rounds[0].completed_count, 1, "不得双重应用");
    assert_eq!(after.rounds[0].rejected_count, 0);
}

// ────────────────────────────────────────────── 判据 7：过期不自动通过

/// 秒级截止：座位到期后保持 `Expired`，工作流仍 `SUSPENDED`，审批实例不被自动收口。
#[tokio::test(flavor = "multi_thread")]
async fn expired_seat_never_auto_passes() {
    let env = setup().await;
    let wf_id = env
        .deploy_start("acceptance-deadline", DEADLINE_YAML, "origin")
        .await;
    env.await_status(&wf_id, InstanceStatus::Suspended).await;

    common::wait_until(
        || async {
            env.svc
                .get_instance_detail(&wf_id)
                .await
                .map(|d| d.tasks.iter().any(|t| t.status == TaskStatus::Expired))
                .unwrap_or(false)
        },
        Duration::from_secs(20),
        "seat deadline expires",
    )
    .await;

    let detail = env.svc.get_instance_detail(&wf_id).await.expect("detail");
    assert_eq!(
        detail.status, "RUNNING",
        "全员过期绝不自动收口审批实例（Q4）"
    );

    // 定时器触发后工作流仍挂在审批上（无人为动作不完成）。
    let inst = env
        .svc
        .get_instance(&wf_id)
        .await
        .unwrap()
        .expect("instance");
    assert_eq!(inst.status, InstanceStatus::Suspended);
    assert_eq!(env.event_count(&wf_id, ROUND_PASSED).await, 0);
    assert_eq!(env.event_count(&wf_id, APPROVAL_COMPLETED).await, 0);
}

// ──────────────────────────────────────── 判据 3b：驳回重开必须重开席位

/// `ReturnToNode` 重开后必须同批重开目标节点席位：新席位可查、可批，工作流恢复并完成。
///
/// 若只重开分支不开轮次，节点会停在「Open 但零席位」——无待办、无 deadline、无人可投，
/// 工作流永久 `SUSPENDED`。本用例证明重开后仍有可恢复的投票路径。
#[tokio::test(flavor = "multi_thread")]
async fn return_to_node_reopens_seats_and_can_pass_again() {
    let env = setup().await;
    let wf_id = env
        .deploy_start("acceptance-return-seats", SELF_RETURN_YAML, "origin")
        .await;
    env.await_status(&wf_id, InstanceStatus::Suspended).await;

    let before = env.svc.get_instance_detail(&wf_id).await.expect("detail");
    let review_branch = before
        .branches
        .iter()
        .find(|b| b.node == "review" && b.attempt == 1)
        .expect("review attempt 1")
        .id
        .clone();

    env.svc
        .approval_command(
            &wf_id,
            "origin",
            ApprovalCommand::ReturnToNode {
                branch_id: &review_branch,
                target_node: "review",
                comment: Some("rework"),
            },
            None,
        )
        .await
        .expect("return to node");

    // 重开分支 Open 且带可计票席位（不再是无席可批的死分支）。
    let reopened = env.svc.get_instance_detail(&wf_id).await.expect("detail");
    let attempt2 = reopened
        .branches
        .iter()
        .find(|b| b.node == "review" && b.attempt == 2)
        .expect("review attempt 2");
    assert_eq!(attempt2.status, BranchStatus::Open);
    let seat = reopened
        .tasks
        .iter()
        .find(|t| t.branch_id == attempt2.id && t.assignee.as_deref() == Some("u1"))
        .expect("reopened seat for u1");
    assert_eq!(seat.kind, TaskKind::Approval);
    assert!(seat.deadline.is_none(), "重开席位不带 deadline");

    // 待办可见：重开席位进入 u1 的待办投影。
    let pending = env
        .svc
        .list_pending_tasks("u1", 0, None)
        .await
        .expect("list pending")
        .0;
    assert!(
        pending
            .iter()
            .any(|t| t.instance_id == wf_id && t.task_id == seat.id),
        "重开席位必须进入 u1 待办: {pending:?}"
    );

    // 新席位可批 ⇒ 工作流恢复并走到终态。
    let (_, status) = env
        .svc
        .approval_command(
            &wf_id,
            "u1",
            ApprovalCommand::Approve {
                task_id: &seat.id,
                comment: None,
            },
            None,
        )
        .await
        .expect("approve reopened seat");
    assert_eq!(status, "RUNNING", "重开通过不产生审批实例终态");
    env.await_status(&wf_id, InstanceStatus::Completed).await;

    let inst = env
        .svc
        .get_instance(&wf_id)
        .await
        .unwrap()
        .expect("instance");
    assert_eq!(inst.context["done"], json!("yes"));
    let final_detail = env.svc.get_instance_detail(&wf_id).await.expect("detail");
    assert_eq!(final_detail.status, "COMPLETED");
}

// ───────────────────────────────── 判据 I1：驳回至更早节点不得跳过中间审批

/// `ReturnToNode` 把后节点驳回至**更早**节点后，工作流必须重新驱动到目标节点（而非停在
/// 返回节点）：否则目标分支通过时会按返回节点的 `onPass` 恢复，跳过二者之间的审批节点。
///
/// `first → second → done`：`second` 驳回至 `first`，通过重开的 `first` ⇒ 必须重新挂起在
/// `second`（attempt 2），再通过 `second` 才收口——证明中间节点未被跳过。
#[tokio::test(flavor = "multi_thread")]
async fn return_to_earlier_node_does_not_skip_intervening_approval() {
    let env = setup().await;
    let wf_id = env
        .deploy_start("acceptance-return", RETURN_YAML, "origin")
        .await;
    env.await_status(&wf_id, InstanceStatus::Suspended).await;

    // first 通过 → 驱动到 second 并挂起。
    let first = env.svc.get_instance_detail(&wf_id).await.expect("detail");
    let first_task = task_id_for(&first, "u1");
    env.svc
        .approval_command(
            &wf_id,
            "u1",
            ApprovalCommand::Approve {
                task_id: &first_task,
                comment: None,
            },
            None,
        )
        .await
        .expect("first pass");
    common::wait_until(
        || async {
            env.svc
                .get_instance_detail(&wf_id)
                .await
                .map(|d| {
                    d.branches
                        .iter()
                        .any(|b| b.node == "second" && b.status == BranchStatus::Open)
                })
                .unwrap_or(false)
        },
        Duration::from_secs(20),
        "second approve node opens",
    )
    .await;

    // second 驳回至 first。
    let second = env.svc.get_instance_detail(&wf_id).await.expect("detail");
    let second_branch = second
        .branches
        .iter()
        .find(|b| b.node == "second" && b.attempt == 1)
        .expect("second attempt 1")
        .id
        .clone();
    env.svc
        .approval_command(
            &wf_id,
            "origin",
            ApprovalCommand::ReturnToNode {
                branch_id: &second_branch,
                target_node: "first",
                comment: Some("rework"),
            },
            None,
        )
        .await
        .expect("return to first");

    // 重新驱动到 first attempt 2 并重新挂起（而非停在 second / 直接跑完）。
    common::wait_until(
        || async {
            let suspended = matches!(
                env.svc.get_instance(&wf_id).await,
                Ok(Some(i)) if i.status == InstanceStatus::Suspended
            );
            let reopened = env
                .svc
                .get_instance_detail(&wf_id)
                .await
                .map(|d| {
                    d.branches.iter().any(|b| {
                        b.node == "first" && b.attempt == 2 && b.status == BranchStatus::Open
                    })
                })
                .unwrap_or(false);
            suspended && reopened
        },
        Duration::from_secs(20),
        "workflow re-suspends on reopened first node",
    )
    .await;
    let inst = env
        .svc
        .get_instance(&wf_id)
        .await
        .unwrap()
        .expect("instance");
    assert_eq!(
        inst.status,
        InstanceStatus::Suspended,
        "ReturnToNode 后必须重新挂起（不得停在返回节点，也不得跳过中间审批）"
    );
    assert!(
        inst.context.get("done").is_none(),
        "不得跳过 second 直接进 done"
    );

    let detail = env.svc.get_instance_detail(&wf_id).await.expect("detail");
    let first2 = detail
        .branches
        .iter()
        .find(|b| b.node == "first" && b.attempt == 2)
        .expect("first attempt 2")
        .id
        .clone();
    let first2_task = task_id_for_branch(&detail, &first2, "u1");

    // 通过重开的 first → 必须重新挂起在 second attempt 2。
    env.svc
        .approval_command(
            &wf_id,
            "u1",
            ApprovalCommand::Approve {
                task_id: &first2_task,
                comment: None,
            },
            None,
        )
        .await
        .expect("approve reopened first");
    common::wait_until(
        || async {
            let suspended = matches!(
                env.svc.get_instance(&wf_id).await,
                Ok(Some(i)) if i.status == InstanceStatus::Suspended
            );
            let reopened = env
                .svc
                .get_instance_detail(&wf_id)
                .await
                .map(|d| {
                    d.branches.iter().any(|b| {
                        b.node == "second" && b.attempt == 2 && b.status == BranchStatus::Open
                    })
                })
                .unwrap_or(false);
            suspended && reopened
        },
        Duration::from_secs(20),
        "workflow re-suspends on second attempt 2",
    )
    .await;

    let detail = env.svc.get_instance_detail(&wf_id).await.expect("detail");
    let second2 = detail
        .branches
        .iter()
        .find(|b| b.node == "second" && b.attempt == 2)
        .expect("second attempt 2")
        .id
        .clone();
    let second2_task = task_id_for_branch(&detail, &second2, "u2");

    // 通过重开的 second → 工作流完成。
    env.svc
        .approval_command(
            &wf_id,
            "u2",
            ApprovalCommand::Approve {
                task_id: &second2_task,
                comment: None,
            },
            None,
        )
        .await
        .expect("approve second attempt 2");
    env.await_status(&wf_id, InstanceStatus::Completed).await;
    let inst = env
        .svc
        .get_instance(&wf_id)
        .await
        .unwrap()
        .expect("instance");
    assert_eq!(inst.context["done"], json!("yes"));
}

// ───────────────────────────── 判据 I3：工作流终态时收口审批聚合

/// 审批节点通过、工作流在下游 `wait` 挂起（审批聚合仍 `RUNNING`）时取消：取消必须收口
/// 审批聚合为终态，不得残留 `RUNNING`。修复前仅在「挂起在审批上」时终止聚合，此场景成孤儿。
#[tokio::test(flavor = "multi_thread")]
async fn cancel_running_instance_after_passed_approval_finalizes_aggregate() {
    let env = setup().await;
    let wf_id = env
        .deploy_start("acceptance-hold", HOLD_AFTER_APPROVE_YAML, "origin")
        .await;
    env.await_status(&wf_id, InstanceStatus::Suspended).await;
    let detail = env.svc.get_instance_detail(&wf_id).await.expect("detail");
    let task = task_id_for(&detail, "u1");
    env.svc
        .approval_command(
            &wf_id,
            "u1",
            ApprovalCommand::Approve {
                task_id: &task,
                comment: None,
            },
            None,
        )
        .await
        .expect("approve");

    // 等到工作流在下游 wait 挂起（非审批挂起）：审批节点已通过，聚合仍未收口。
    common::wait_until(
        || async {
            matches!(env.svc.get_instance(&wf_id).await, Ok(Some(i))
                if i.status == InstanceStatus::Waiting
                    && i.suspension_meta.as_ref().is_some_and(|m| m.reason == "wait"))
        },
        Duration::from_secs(20),
        "workflow suspends on downstream wait",
    )
    .await;
    let mid = env.svc.get_instance_detail(&wf_id).await.expect("detail");
    assert_eq!(
        mid.status, "RUNNING",
        "审批节点已通过、工作流在下游，审批聚合此时仍是 RUNNING"
    );

    env.svc.cancel_instance(&wf_id).await.expect("cancel");
    env.await_status(&wf_id, InstanceStatus::Cancelled).await;
    let after = env.svc.get_instance_detail(&wf_id).await.expect("detail");
    assert!(
        matches!(after.status.as_str(), "COMPLETED" | "TERMINATED"),
        "取消后审批聚合必须收口为终态，不得残留 RUNNING: {}",
        after.status
    );
}

/// 审批节点通过后下游 `raise` fault：工作流置 `FAILED`，审批聚合最佳努力收口。
///
/// 收口失败（存储异常）时聚合会残留 `RUNNING` 留给管理员 `Terminate`；本用例钉住正常路径
/// 的可见稳态（节点已通过、无 open 分支 ⇒ `finish_if_complete` 收口为 `COMPLETED`）。
#[tokio::test(flavor = "multi_thread")]
async fn workflow_fault_after_passed_approval_finalizes_or_documents_aggregate() {
    let env = setup().await;
    let wf_id = env
        .deploy_start("acceptance-fault", FAULT_AFTER_APPROVE_YAML, "origin")
        .await;
    env.await_status(&wf_id, InstanceStatus::Suspended).await;
    let detail = env.svc.get_instance_detail(&wf_id).await.expect("detail");
    let task = task_id_for(&detail, "u1");
    env.svc
        .approval_command(
            &wf_id,
            "u1",
            ApprovalCommand::Approve {
                task_id: &task,
                comment: None,
            },
            None,
        )
        .await
        .expect("approve");

    env.await_status(&wf_id, InstanceStatus::Failed).await;
    common::wait_until(
        || async {
            matches!(env.svc.get_instance_detail(&wf_id).await, Ok(d) if d.status == "COMPLETED")
        },
        Duration::from_secs(20),
        "approval aggregate finalized to COMPLETED after downstream fault",
    )
    .await;
}
