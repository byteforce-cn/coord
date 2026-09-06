// T5.12（PD 可观测与安全：调度暂停开关 + operator 审计日志）验收测试
//
// R-MR-08（D1-a，P4b 后）：operator 队列恒经 region 0 system raft 承载（全局
// 队列模式——本地队列路径已退役）。本文件经 public API
// （PlacementDriver / OperatorExecutor / FakeSystemRaft / attach_observability）
// 验证全局队列模式下的可观测语义：
//   - **调度暂停开关**：`set_scheduler_paused(true)` 后 scheduler tick 不再产生
//     新 operator（不再经 raft `Enqueue`），恢复后继续调度（可追踪可暂停）；
//   - **operator 审计日志**：调度 Enqueue（result=pending）、执行器认领
//     （result=claimed）、执行成功/失败（result=success/failed，含原因）与
//     Running 超时重认领（result=requeued）经 `AuditLogger` 各记一条
//     （actor=pd）——enqueue→claim→complete→requeue 全生命周期可追溯；
//   - **未接钩子不记录**：无 audit logger 时全流程不 panic、队列状态正确。
//
// 指标计数（coord_pd_operator_total）在 Metrics 单测内验证（metrics.rs
// test_pd_operator_counter 同款路径）；本文件聚焦开关语义与审计事件。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use coord_core::types::{NodeID, Peer, PeerRole, RegionEpoch, RegionMeta};
use coord_server::audit::AuditLogger;
use coord_server::pd::meta_store::PdMetaStore;
use coord_server::pd::operator::OperatorStatus;
use coord_server::pd::{
    NodeState, Operator, OperatorExecutor, PdConfig, PdObservability, PlacementDriver,
};
use coord_server::raft::region_runtime::RegionRaftHandle;
use coord_server::raft::system_raft::SystemRaftHandle;
use coord_server::raft::type_config::{PdOp, PdQueueEntry};
use tokio::sync::watch;

/// 可编程 region 0 system raft 替身：与 `apply_pd_op` 同构的 CAS 状态机
/// （Enqueue 幂等去重、Claim/Complete CAS、Requeue）+ 记录 propose 命令 + 提供
/// 测试用预置（seed_pending/seed_running）与 drain（把 Pending 置 Success，供
/// 暂停测试观察"重新生成"）。
struct FakeSystemRaft {
    leader: Mutex<Option<NodeID>>,
    queue: Mutex<Vec<PdQueueEntry>>,
    next_op_id: AtomicU64,
}

impl FakeSystemRaft {
    fn new(leader: Option<NodeID>) -> Self {
        Self {
            leader: Mutex::new(leader),
            queue: Mutex::new(Vec::new()),
            next_op_id: AtomicU64::new(1),
        }
    }

    /// 预置一条 Pending 条目（返回分配 op_id）
    fn seed_pending(&self, op: Operator, requester: NodeID) -> u64 {
        let id = self.next_op_id.fetch_add(1, Ordering::SeqCst);
        self.queue
            .lock()
            .unwrap()
            .push(PdQueueEntry::new_pending(id, op, requester, 1_700_000_000));
        id
    }

    /// 预置一条 Running 条目（认领者/认领墙钟可指定；P3 超时重认领测试用）
    fn seed_running(
        &self,
        op_id: u64,
        op: Operator,
        requester: NodeID,
        claimed_by: NodeID,
        claimed_at_unix: i64,
    ) {
        self.queue.lock().unwrap().push(PdQueueEntry {
            op_id,
            op,
            status: OperatorStatus::Running,
            requester,
            claimed_by,
            proposed_at_unix: claimed_at_unix,
            claimed_at_unix,
            error: String::new(),
        });
    }

    /// 把全部 Pending 条目直接置 Success 并返回数量（模拟执行器 drain——测试
    /// 不关心执行细节，只观察调度是否产生新 operator）。meta 未变 → 下 tick
    /// ReplicaChecker 会重新生成（供暂停对比）。
    fn drain_pending(&self) -> usize {
        let mut q = self.queue.lock().unwrap();
        let mut n = 0;
        for e in q.iter_mut() {
            if e.is_pending() {
                e.status = OperatorStatus::Success;
                n += 1;
            }
        }
        n
    }
}

#[async_trait]
impl SystemRaftHandle for FakeSystemRaft {
    async fn current_leader(&self) -> Option<NodeID> {
        *self.leader.lock().unwrap()
    }

    async fn propose_pd(&self, op: PdOp) -> coord_core::error::Result<u64> {
        let mut queue = self.queue.lock().unwrap();
        match &op {
            PdOp::Enqueue {
                op,
                requester,
                proposed_at_unix,
            } => {
                let dup = queue
                    .iter()
                    .any(|e| (e.is_pending() || e.is_running()) && &e.op == op);
                if !dup {
                    let id = self.next_op_id.fetch_add(1, Ordering::SeqCst);
                    queue.push(PdQueueEntry::new_pending(
                        id,
                        op.clone(),
                        *requester,
                        *proposed_at_unix,
                    ));
                }
                Ok(1) // 日志 index 非断言点
            }
            PdOp::Claim {
                op_id,
                node_id,
                claimed_at_unix,
            } => {
                if let Some(e) = queue.iter_mut().find(|e| e.op_id == *op_id) {
                    e.try_claim(*node_id, *claimed_at_unix);
                }
                Ok(1)
            }
            PdOp::Complete {
                op_id,
                node_id,
                success,
                error,
            } => {
                if let Some(e) = queue.iter_mut().find(|e| e.op_id == *op_id) {
                    e.try_complete(*node_id, *success, error);
                }
                Ok(1)
            }
            PdOp::Requeue { op_id } => {
                if let Some(e) = queue.iter_mut().find(|e| e.op_id == *op_id) {
                    e.try_requeue();
                }
                Ok(1)
            }
        }
    }

    fn pd_queue(&self) -> coord_core::error::Result<Vec<PdQueueEntry>> {
        Ok(self.queue.lock().unwrap().clone())
    }
}

/// Region raft 替身：leader = 本节点（1）；记录成员变更调用。
struct FakeRegionRaft {
    leader: Mutex<Option<NodeID>>,
    members: Mutex<Vec<Peer>>,
    add_learner_calls: Mutex<Vec<(NodeID, String)>>,
    promote_calls: Mutex<Vec<NodeID>>,
    remove_calls: Mutex<Vec<NodeID>>,
    transfer_calls: Mutex<Vec<NodeID>>,
}

impl FakeRegionRaft {
    fn new(leader: NodeID) -> Self {
        Self {
            leader: Mutex::new(Some(leader)),
            members: Mutex::new(vec![Peer {
                node_id: 1,
                raft_addr: "node1:50052".into(),
                role: PeerRole::Voter,
            }]),
            add_learner_calls: Mutex::new(Vec::new()),
            promote_calls: Mutex::new(Vec::new()),
            remove_calls: Mutex::new(Vec::new()),
            transfer_calls: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl RegionRaftHandle for FakeRegionRaft {
    async fn current_leader(&self) -> Option<NodeID> {
        *self.leader.lock().unwrap()
    }

    async fn current_members(&self) -> coord_core::error::Result<Vec<Peer>> {
        Ok(self.members.lock().unwrap().clone())
    }

    async fn add_learner(
        &self,
        node_id: NodeID,
        raft_addr: &str,
    ) -> coord_core::error::Result<()> {
        self.add_learner_calls
            .lock()
            .unwrap()
            .push((node_id, raft_addr.to_string()));
        let mut members = self.members.lock().unwrap();
        if !members.iter().any(|p| p.node_id == node_id) {
            members.push(Peer {
                node_id,
                raft_addr: raft_addr.to_string(),
                role: PeerRole::Learner,
            });
        }
        Ok(())
    }

    async fn promote_to_voter(&self, node_id: NodeID) -> coord_core::error::Result<()> {
        self.promote_calls.lock().unwrap().push(node_id);
        let mut members = self.members.lock().unwrap();
        if let Some(p) = members.iter_mut().find(|p| p.node_id == node_id) {
            p.role = PeerRole::Voter;
        }
        Ok(())
    }

    async fn remove_voter(&self, node_id: NodeID) -> coord_core::error::Result<()> {
        self.remove_calls.lock().unwrap().push(node_id);
        let mut members = self.members.lock().unwrap();
        if let Some(p) = members.iter_mut().find(|p| p.node_id == node_id) {
            p.role = PeerRole::Learner;
        }
        Ok(())
    }

    async fn transfer_leader(&self, to: NodeID) -> coord_core::error::Result<()> {
        self.transfer_calls.lock().unwrap().push(to);
        *self.leader.lock().unwrap() = Some(to);
        Ok(())
    }
}

/// 构造全局队列模式 driver：node 1 = region 0 leader（FakeSystemRaft），
/// 3 在线节点 + region 1（仅 node1 1 个 voter，目标副本可配）。ReplicaChecker
/// 在欠副本时会生成 AddPeer——用于验证暂停开关与审计事件。
fn make_pd(
    cfg: PdConfig,
    audit: Option<Arc<AuditLogger>>,
) -> (Arc<PlacementDriver>, Arc<FakeSystemRaft>, watch::Sender<bool>) {
    let store = Arc::new(PdMetaStore::new());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let pd = Arc::new(PlacementDriver::new(cfg, store, shutdown_rx, 1));
    pd.attach_observability(PdObservability::new(audit, None));

    for id in 1..=3u64 {
        pd.handle_node_heartbeat(NodeState::new(
            id,
            format!("127.0.0.1:5{:04}", id),
            format!("127.0.0.1:5{:03}1", id),
        ));
    }
    let region = RegionMeta {
        region_id: 1,
        start_key: vec![],
        end_key: vec![],
        epoch: RegionEpoch::initial(),
        peers: vec![Peer {
            node_id: 1,
            raft_addr: "127.0.0.1:5001".into(),
            role: PeerRole::Voter,
        }],
        approximate_size: 0,
        approximate_keys: 0,
    };
    pd.meta_store().create_region(region).expect("create region");

    // 全局队列模式：region 0 leader = 本节点
    let system = Arc::new(FakeSystemRaft::new(Some(1)));
    pd.attach_system_raft(system.clone());
    (pd, system, shutdown_tx)
}

/// 轮询直到累计 drain（把 Pending 置 Success，模拟消费）≥ want——每个被消费的
/// operator 会促使下一 tick 重新 Enqueue（meta 未变），从而验证"持续产生"。
async fn wait_produced(system: &FakeSystemRaft, want: usize, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut drained = 0usize;
    loop {
        drained += system.drain_pending();
        if drained >= want {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "expected >= {want} produced operators, got {drained} (scheduler not producing?)"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// 轮询 audit 日志直到出现指定 (actor=pd, action, result) 事件（或超时 panic）。
async fn wait_audit(
    audit: &AuditLogger,
    action: &str,
    result: &str,
    timeout: Duration,
) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let events = audit.recent(100);
        if events
            .iter()
            .any(|e| e.actor == "pd" && e.action == action && e.result == result)
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no audit event {action}/{result}: {events:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// 轮询 fake 全局队列直到出现一条 Pending 的 add-peer（调度已 Enqueue）。
async fn wait_pending_add_peer(system: &FakeSystemRaft, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let q = system.pd_queue().unwrap();
        if q.iter()
            .any(|e| e.op.name() == "add-peer" && e.is_pending())
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scheduler never enqueued pending add-peer: {q:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// 轮询 fake 全局队列直到 op_id 条目回到 Pending（Running 超时已被 Requeue）。
async fn wait_requeued(system: &FakeSystemRaft, op_id: u64, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let q = system.pd_queue().unwrap();
        if q.iter().any(|e| e.op_id == op_id && e.is_pending()) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "op {op_id} never requeued to pending: {q:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn test_scheduler_pause_freezes_and_resumes_production() {
    let mut cfg = PdConfig::default();
    cfg.target_replicas = 3;
    cfg.balance_interval = 1; // 1s tick，测试窗口可覆盖 ≥2 tick
    cfg.max_concurrent_operators = 10;
    let (pd, system, shutdown_tx) = make_pd(cfg, None);
    assert!(!pd.is_scheduler_paused(), "default: unpaused");

    let handle = pd.start_scheduler_loop();

    // Phase 1：未暂停 → 1s 内应产生并消费 ≥1 个 operator
    wait_produced(&system, 1, Duration::from_secs(8)).await;

    // 再跨一个 tick 并清空队列：确保暂停起点队列为空（无"暂停前遗留"噪音）
    tokio::time::sleep(Duration::from_millis(1100)).await;
    system.drain_pending();

    // Phase 2：暂停 → 跨 ≥2 个 tick（2.5s）不应再产生任何新 operator
    pd.set_scheduler_paused(true);
    assert!(pd.is_scheduler_paused());
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let produced_while_paused = system.drain_pending();
    assert_eq!(
        produced_while_paused, 0,
        "paused scheduler must not produce new operators (got {produced_while_paused})"
    );

    // Phase 3：恢复 → 应继续产生（可追踪可暂停语义闭环）
    pd.set_scheduler_paused(false);
    assert!(!pd.is_scheduler_paused());
    wait_produced(&system, 1, Duration::from_secs(8)).await;

    // 优雅关闭
    shutdown_tx.send(true).expect("send shutdown");
    let _ = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("scheduler loop should exit on shutdown signal");
}

/// 全局队列模式全生命周期审计：Enqueue(pending) → Claim(claimed) →
/// Complete(success/failed 含原因) 与 Running 超时 Requeue(requeued) 各记一条。
#[test]
fn test_operator_audit_events_recorded_global_queue_mode() {
    let dir = tempfile::tempdir().unwrap();
    let audit = Arc::new(AuditLogger::file_logger(dir.path()).expect("audit logger"));
    let mut cfg = PdConfig::default();
    cfg.target_replicas = 3; // region1 欠副本 → ReplicaChecker 生成 AddPeer
    cfg.balance_interval = 1;
    let (pd, system, shutdown_tx) = make_pd(cfg, Some(Arc::clone(&audit)));

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let handle = pd.start_scheduler_loop();

        // 1) 调度 tick Enqueue AddPeer（region1 欠副本）→ audit: pending
        wait_pending_add_peer(&system, Duration::from_secs(8)).await;
        wait_audit(&audit, "operator.add-peer", "pending", Duration::from_secs(8)).await;

        // 执行器（node1 = region1 leader）认领执行 → audit: claimed + success
        let ex = OperatorExecutor::new(Arc::clone(&pd), 1).with_add_peer_resolver(Arc::new(
            |_rid| Some((2, "node2:50052".to_string())),
        ));
        let region = Arc::new(FakeRegionRaft::new(1));
        let r2: Arc<dyn RegionRaftHandle> = region.clone();
        let resolve: Arc<coord_server::pd::executor::RegionRaftResolver> =
            Arc::new(move |_rid| Some(r2.clone()));
        let ran = ex.execute_one(&*resolve).await;
        assert!(ran.is_some(), "executor should run leader-owned add-peer");

        // 2) 失败路径：SplitRegion（v1 不支持）→ audit: failed（含原因）
        system.seed_pending(
            Operator::SplitRegion {
                region_id: 7,
                split_key: b"m".to_vec(),
                new_region_id: 8,
            },
            1,
        );
        let ran = ex.execute_one(&*resolve).await;
        assert!(ran.is_some(), "executor should attempt split-region (then fail)");

        // 3) Running 超时重认领 → audit: requeued（stale Running 由 region0
        //    leader 每 tick 扫描 Requeue）
        let now = now_unix_secs();
        let op_id = system.seed_running(
            900,
            Operator::TransferLeader {
                region_id: 7,
                to_node: 3,
            },
            1,
            2, // 认领者 node2 失联
            now - pd.config().operator_running_timeout as i64 - 100,
        );
        let _ = op_id;
        wait_requeued(&system, 900, Duration::from_secs(8)).await;
        wait_audit(
            &audit,
            "operator.transfer-leader",
            "requeued",
            Duration::from_secs(8),
        )
        .await;

        // 优雅停止
        shutdown_tx.send(true).expect("send shutdown");
        let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
    });

    let events = audit.recent(100);
    let has = |action: &str, result: &str| -> bool {
        events
            .iter()
            .any(|e| e.actor == "pd" && e.action == action && e.result == result)
    };
    assert!(
        has("operator.add-peer", "pending"),
        "missing pending audit for add-peer: {events:?}"
    );
    assert!(
        has("operator.add-peer", "claimed"),
        "missing claimed audit for add-peer: {events:?}"
    );
    assert!(
        has("operator.add-peer", "success"),
        "missing success audit for add-peer: {events:?}"
    );
    let failed: Vec<_> = events
        .iter()
        .filter(|e| e.action == "operator.split-region" && e.result == "failed")
        .collect();
    assert_eq!(failed.len(), 1, "expected one failed audit: {events:?}");
    assert!(
        failed[0].detail.contains("not supported"),
        "failed audit must carry reason: {}",
        failed[0].detail
    );
    assert_eq!(failed[0].resource, "region/7", "resource = region/{{id}}");
    assert!(
        has("operator.transfer-leader", "requeued"),
        "missing requeued audit for stale running transfer-leader: {events:?}"
    );
}

#[test]
fn test_operator_audit_suppressed_without_hook() {
    // 未 attach observability → 不 panic、不记录（行为与现状一致）；
    // 队列状态仍正确收敛（add-peer Success）。
    let mut cfg = PdConfig::default();
    cfg.target_replicas = 3;
    cfg.balance_interval = 1;
    let (pd, system, shutdown_tx) = make_pd(cfg, None);

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let handle = pd.start_scheduler_loop();
        // Enqueue（无 audit logger → no-op 不 panic）
        wait_pending_add_peer(&system, Duration::from_secs(8)).await;

        let ex = OperatorExecutor::new(Arc::clone(&pd), 1).with_add_peer_resolver(Arc::new(
            |_rid| Some((2, "node2:50052".to_string())),
        ));
        let region = Arc::new(FakeRegionRaft::new(1));
        let r2: Arc<dyn RegionRaftHandle> = region.clone();
        let resolve: Arc<coord_server::pd::executor::RegionRaftResolver> =
            Arc::new(move |_rid| Some(r2.clone()));
        ex.execute_one(&*resolve).await;

        // Running 超时重认领路径无 audit logger 也不 panic
        let now = now_unix_secs();
        system.seed_running(
            901,
            Operator::TransferLeader {
                region_id: 7,
                to_node: 3,
            },
            1,
            2,
            now - pd.config().operator_running_timeout as i64 - 100,
        );
        wait_requeued(&system, 901, Duration::from_secs(8)).await;

        // 优雅停止
        shutdown_tx.send(true).expect("send shutdown");
        let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
    });

    // 队列状态正确：add-peer 已 Success、stale running 已回 Pending
    let q = system.pd_queue().unwrap();
    assert!(
        q.iter().any(|e| {
            e.op.name() == "add-peer" && matches!(e.status, OperatorStatus::Success)
        }),
        "add-peer should have completed: {q:?}"
    );
    assert!(
        q.iter()
            .any(|e| e.op_id == 901 && e.is_pending()),
        "stale running should have been requeued to pending: {q:?}"
    );
}

/// 当前墙钟 Unix 秒（重认领判定用；与 pd 模块 `now_unix_secs` 同语义）。
fn now_unix_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
