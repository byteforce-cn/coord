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
use coord_core::workflow::approval::model::{BranchStatus, SeatSpec, TaskStatus};
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
