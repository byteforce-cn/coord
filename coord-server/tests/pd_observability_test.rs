// T5.12（PD 可观测与安全：调度暂停开关 + operator 审计日志）验收测试
//
// 经 public API（PlacementDriver / start_scheduler_loop / enqueue / complete /
// attach_observability）验证：
//   - **调度暂停开关**：`set_scheduler_paused(true)` 后 scheduler tick 不再产生
//     新 operator（已排队/执行的仍可 drain），恢复后继续调度（可追踪可暂停）；
//   - **operator 审计日志**：enqueue(pending) / complete(success|failed，含原因)
//     经 `AuditLogger` 各记一条（actor=pd），`recent()` 可查询——事后追溯调度
//     行为（谁、何时、对哪个 Region 做了什么、结果如何）。
//
// 指标计数（coord_pd_operator_total）在 Metrics 单测内验证（metrics.rs:
// test_pd_operator_counter 同款路径）；本文件聚焦开关语义与审计事件。

use std::sync::Arc;
use std::time::Duration;

use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionMeta};
use coord_server::audit::AuditLogger;
use coord_server::pd::meta_store::PdMetaStore;
use coord_server::pd::{NodeState, Operator, PdConfig, PdObservability, PlacementDriver};
use tokio::sync::watch;

/// 构造调度场景：3 在线节点 × 1 Region（仅 node1 1 个 voter，目标副本 3）。
/// ReplicaChecker 每 tick 都会为欠副本 Region 生成 AddPeer，直到元数据更新——
/// 用于验证暂停开关对"新 operator 产生"的冻结/恢复。
fn make_replica_imbalance_pd(
    cfg: PdConfig,
) -> (Arc<PlacementDriver>, watch::Sender<bool>) {
    let store = Arc::new(PdMetaStore::new());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let pd = Arc::new(PlacementDriver::new(cfg, store, shutdown_rx, 1));

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
    (pd, shutdown_tx)
}

/// 弹出并完成（Success）全部可执行 operator；返回数量。
/// 完成后 meta_store 未变 → ReplicaChecker 下一 tick 仍会重新生成（供暂停对比）。
fn drain_and_complete(pd: &PlacementDriver) -> usize {
    let mut n = 0;
    while let Some(op) = pd.take_next_operator() {
        pd.complete_operator(&op, true, None);
        n += 1;
    }
    n
}

/// 轮询直到 drain 数量 ≥ want（或超时 panic）。
async fn wait_produced(pd: &PlacementDriver, want: usize, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let got = drain_and_complete(pd);
        if got >= want {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "expected >= {want} produced operators, got {got} (scheduler not producing?)"
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
    let (pd, shutdown_tx) = make_replica_imbalance_pd(cfg);
    assert!(!pd.is_scheduler_paused(), "default: unpaused");

    let handle = pd.start_scheduler_loop();

    // Phase 1：未暂停 → 1s 内应产生并 drain 掉 ≥1 个 operator
    wait_produced(&pd, 1, Duration::from_secs(8)).await;

    // 再跨一个 tick 并清空队列：确保暂停起点队列为空（无"暂停前遗留"噪音）
    tokio::time::sleep(Duration::from_millis(1100)).await;
    drain_and_complete(&pd);

    // Phase 2：暂停 → 跨 ≥2 个 tick（2.5s）不应再产生任何新 operator
    pd.set_scheduler_paused(true);
    assert!(pd.is_scheduler_paused());
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let produced_while_paused = drain_and_complete(&pd);
    assert_eq!(
        produced_while_paused, 0,
        "paused scheduler must not produce new operators (got {produced_while_paused})"
    );

    // Phase 3：恢复 → 应继续产生（可追踪可暂停语义闭环）
    pd.set_scheduler_paused(false);
    assert!(!pd.is_scheduler_paused());
    wait_produced(&pd, 1, Duration::from_secs(8)).await;

    // 优雅关闭
    shutdown_tx.send(true).expect("send shutdown");
    let _ = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("scheduler loop should exit on shutdown signal");
}

#[test]
fn test_operator_audit_events_recorded() {
    let dir = tempfile::tempdir().unwrap();
    let audit = Arc::new(AuditLogger::file_logger(dir.path()).expect("audit logger"));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    drop(shutdown_tx); // 本测试不启动循环

    let pd = PlacementDriver::new(PdConfig::default(), Arc::new(PdMetaStore::new()), shutdown_rx, 1);
    pd.attach_observability(PdObservability::new(Some(Arc::clone(&audit)), None));

    // 成功路径：add-peer
    let add = Operator::AddPeer {
        region_id: 7,
        node_id: 2,
        raft_addr: "127.0.0.1:5002".into(),
    };
    pd.enqueue_operator(add.clone());
    pd.complete_operator(&add, true, None);

    // 失败路径：transfer-leader（含原因）
    let transfer = Operator::TransferLeader {
        region_id: 7,
        to_node: 3,
    };
    pd.enqueue_operator(transfer.clone());
    pd.complete_operator(&transfer, false, Some("node 3 is not a voter".into()));

    let events = audit.recent(10);
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
        has("operator.add-peer", "success"),
        "missing success audit for add-peer: {events:?}"
    );
    assert!(
        has("operator.transfer-leader", "pending"),
        "missing pending audit for transfer-leader: {events:?}"
    );
    let failed: Vec<_> = events
        .iter()
        .filter(|e| e.action == "operator.transfer-leader" && e.result == "failed")
        .collect();
    assert_eq!(failed.len(), 1, "expected one failed audit: {events:?}");
    assert!(
        failed[0].detail.contains("not a voter"),
        "failed audit must carry reason: {}",
        failed[0].detail
    );
    assert_eq!(failed[0].resource, "region/7", "resource = region/{{id}}");
}

#[test]
fn test_operator_audit_suppressed_without_hook() {
    // 未 attach observability → 不 panic、不记录（行为与现状一致）
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    drop(shutdown_tx);
    let pd = PlacementDriver::new(PdConfig::default(), Arc::new(PdMetaStore::new()), shutdown_rx, 1);
    let op = Operator::RemovePeer {
        region_id: 1,
        node_id: 2,
    };
    pd.enqueue_operator(op.clone());
    pd.complete_operator(&op, false, Some("boom".into()));
    pd.requeue_operator(&op);
    // 无 audit logger：不应 panic；队列状态仍正确
    assert_eq!(pd.operator_stats().failed, 1);
}
