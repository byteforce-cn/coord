// R-MR-08（D1-a P2）：PD 全局队列 executor 真实 region 0 raft 端到端验收
//
// 单节点 region 0 raft（与 pd_region0_channel_test 同型 harness）+ 真实
// `CoordSystemRaftHandle`，验证全局队列模式下执行器的完整链路：
//
//   Test 1（认领→执行→完成）：经 region 0 raft `Enqueue` 一条 AddPeer（目标
//   Region 1，其 raft handle 上报 leader = 本节点）→ `execute_one` 扫描全局
//   队列、认领该条目（`PdOp::Claim` apply CAS）→ 在 region 1 raft handle 上
//   执行（add_learner + promote）→ `PdOp::Complete(success)` → region 0 MVCC
//   队列条目终态 Success、claimed_by = 本节点；PD meta 同步新 voter。
//
//   Test 2（他节点 Region leader → 不认领）：队列中 Pending 条目目标 Region 的
//   leader 上报为 node2（非本节点）→ executor 跳过，条目保持 Pending、无任何
//   propose（认领留给他节点）。
//
//   Test 3（真实 raft 去重 + 本地队列不参与）：重复 Enqueue 同一 operator →
//   全局队列仅一条（apply 幂等去重）；本地 pending_operators 恒空。

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use coord_core::storage::StorageBackend;
use coord_core::types::{NodeID, Peer, PeerRole, RegionEpoch, RegionId, RegionMeta, StorageConfig};
use coord_server::pd::meta_store::PdMetaStore;
use coord_server::pd::{Operator, OperatorExecutor, PdConfig, PlacementDriver};
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::RaftNetworkFactoryImpl;
use coord_server::raft::region_runtime::RegionRaftHandle;
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::raft::system_raft::{CoordSystemRaftHandle, SystemRaftHandle};
use coord_server::raft::type_config::PdOp;
use coord_server::raft::{new_basic_node, new_raft, RaftConfig, WatchReceiver};
use coord_server::storage::mvcc::MvccStorage;
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::storage::snapshot::SnapshotTracker;
use tokio::sync::watch;

fn find_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn add_peer_op(region_id: u64, node_id: u64) -> Operator {
    Operator::AddPeer {
        region_id,
        node_id,
        raft_addr: format!("node{node_id}:50052"),
    }
}

/// region 1 的 raft handle 替身：可配置 leader 视图，记录成员变更调用。
struct FakeRegionRaft {
    leader: Mutex<Option<NodeID>>,
    add_learner_calls: Mutex<Vec<(NodeID, String)>>,
    promote_calls: Mutex<Vec<NodeID>>,
    remove_calls: Mutex<Vec<NodeID>>,
    transfer_calls: Mutex<Vec<NodeID>>,
    /// transfer_leader 后自动切换 leader
    auto_switch: AtomicBool,
}

impl FakeRegionRaft {
    fn new(region_id: RegionId, leader: NodeID) -> Self {
        let _ = region_id;
        Self {
            leader: Mutex::new(Some(leader)),
            add_learner_calls: Mutex::new(Vec::new()),
            promote_calls: Mutex::new(Vec::new()),
            remove_calls: Mutex::new(Vec::new()),
            transfer_calls: Mutex::new(Vec::new()),
            auto_switch: AtomicBool::new(true),
        }
    }

    fn set_leader(&self, leader: NodeID) {
        *self.leader.lock().unwrap() = Some(leader);
    }
}

#[async_trait]
impl RegionRaftHandle for FakeRegionRaft {
    async fn current_leader(&self) -> Option<NodeID> {
        *self.leader.lock().unwrap()
    }

    async fn current_members(&self) -> coord_core::error::Result<Vec<Peer>> {
        // 单节点成员表（node_id=本 region 节点 1）；executor 的 current_members
        // 用于 add-peer 幂等判断——测试不依赖精确成员（执行后 meta 由 executor
        // 写穿）。
        Ok(vec![Peer {
            node_id: 1,
            raft_addr: "node1:50052".into(),
            role: PeerRole::Voter,
        }])
    }

    async fn add_learner(&self, node_id: NodeID, raft_addr: &str) -> coord_core::error::Result<()> {
        self.add_learner_calls
            .lock()
            .unwrap()
            .push((node_id, raft_addr.to_string()));
        Ok(())
    }

    async fn promote_to_voter(&self, node_id: NodeID) -> coord_core::error::Result<()> {
        self.promote_calls.lock().unwrap().push(node_id);
        Ok(())
    }

    async fn remove_voter(&self, node_id: NodeID) -> coord_core::error::Result<()> {
        self.remove_calls.lock().unwrap().push(node_id);
        Ok(())
    }

    async fn transfer_leader(&self, to: NodeID) -> coord_core::error::Result<()> {
        self.transfer_calls.lock().unwrap().push(to);
        if self.auto_switch.load(Ordering::SeqCst) {
            self.set_leader(to);
        }
        Ok(())
    }
}

struct Region0Host {
    mvcc: Arc<MvccStorage<RedbBackend>>,
    system: Arc<CoordSystemRaftHandle>,
    // 持有 tempdir 生命周期（字段不读，仅防目录提前删除）
    _dir: tempfile::TempDir,
}

/// 装配单节点 region 0 raft（真实 PD 队列承载 raft）
async fn start_single_region0() -> Region0Host {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().to_path_buf();

    let storage_config = StorageConfig::default();
    let backend = RedbBackend::open(&base, &storage_config).expect("open region0 backend");
    let mvcc = Arc::new(MvccStorage::new(backend).expect("create region0 mvcc"));
    let tracker = Arc::new(SnapshotTracker::default());
    let log_store = LogStore::new(&base)
        .await
        .expect("region0 log store")
        .with_snapshot_tracker(Arc::clone(&tracker));
    let sm_store = StateMachineStore::new(Arc::clone(&mvcc), base.join("snapshots"), tracker);

    let factory = RaftNetworkFactoryImpl::new(1);
    let raft_addr = format!("127.0.0.1:{}", find_port());
    factory.register_node(1, raft_addr.clone());
    let raft = new_raft(
        1,
        Arc::new(RaftConfig::default()),
        factory,
        log_store,
        sm_store,
    )
    .await
    .expect("create region0 raft");
    let mut members = BTreeMap::new();
    members.insert(1, new_basic_node(&raft_addr));
    raft.initialize(members).await.expect("initialize region0 raft");
    let raft = Arc::new(raft);

    // region 0 leader 就绪（单节点 quorum=1）
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let m = raft.metrics();
        let m = m.borrow_watched();
        if m.current_leader == Some(1) && m.last_quorum_acked.is_some() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "region0 leader never ready"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let system = Arc::new(CoordSystemRaftHandle::new(raft.as_ref().clone(), Arc::clone(&mvcc)));
    Region0Host {
        mvcc,
        system,
        _dir: dir,
    }
}

/// 构造 driver（region 1 注册进 PD meta；节点 1 voter）+
/// 执行器（node 1）+ region-1 raft handle 替身。
fn make_driver_and_region_handle() -> (Arc<PlacementDriver>, watch::Sender<bool>, Arc<FakeRegionRaft>) {
    let meta_store = Arc::new(PdMetaStore::new());
    meta_store
        .create_region(RegionMeta {
            region_id: 1,
            start_key: vec![],
            end_key: vec![],
            epoch: RegionEpoch::initial(),
            peers: vec![Peer {
                node_id: 1,
                raft_addr: "node1:50052".into(),
                role: PeerRole::Voter,
            }],
            approximate_size: 0,
            approximate_keys: 0,
        })
        .expect("create region meta");
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let driver = Arc::new(PlacementDriver::new(
        PdConfig::default(),
        meta_store,
        shutdown_rx,
        1,
    ));
    let region_raft = Arc::new(FakeRegionRaft::new(1, 1));
    (driver, shutdown_tx, region_raft)
}

/// Test 1：真实 region 0 raft 上 Enqueue → executor 认领 → region handle 执行
/// → Complete → 队列终态 Success、meta 同步。
#[tokio::test]
async fn test_global_queue_executor_claims_executes_completes_via_region0_raft() {
    let host = start_single_region0().await;
    let (driver, shutdown_tx, region_raft) = make_driver_and_region_handle();
    driver.attach_system_raft(host.system.clone());

    // 调度器生成（模拟）：经 region 0 raft Enqueue AddPeer(region1, node2)
    let op = add_peer_op(1, 2);
    let rev = host
        .system
        .propose_pd(PdOp::Enqueue {
            op: op.clone(),
            requester: 1,
            proposed_at_unix: 1_700_000_000,
        })
        .await
        .expect("enqueue via region0 raft");
    let entries = host.mvcc.pd_queue_entries().expect("read queue");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].op_id, rev);
    assert!(entries[0].is_pending());

    // 执行器（node 1，region1 leader = 本节点替身）
    let ex = OperatorExecutor::new(Arc::clone(&driver), 1);
    let raft: Arc<dyn RegionRaftHandle> = region_raft.clone();
    let resolve: Arc<coord_server::pd::executor::RegionRaftResolver> =
        Arc::new(move |rid| {
            if rid == 1 {
                Some(raft.clone())
            } else {
                None
            }
        });
    let ran = ex.execute_one(&*resolve).await;
    assert!(ran.is_some(), "executor should process leader-owned op");
    assert_eq!(ran.unwrap().name(), "add-peer");

    // region raft handle：add_learner + promote 各一次
    assert_eq!(region_raft.add_learner_calls.lock().unwrap().len(), 1);
    assert_eq!(*region_raft.promote_calls.lock().unwrap(), vec![2]);

    // PD meta 同步（executor 写穿）
    let meta = driver.meta_store().get_region(1).expect("region 1 meta");
    assert!(
        meta.peers.iter().any(|p| p.node_id == 2 && p.role == PeerRole::Voter),
        "meta peers must include new voter: {:?}",
        meta.peers
    );

    // region 0 MVCC 队列终态 Success + claimed_by=1
    let entries = host.mvcc.pd_queue_entries().expect("read queue");
    assert_eq!(entries.len(), 1);
    assert!(
        matches!(
            entries[0].status,
            coord_server::pd::operator::OperatorStatus::Success
        ),
        "queue entry must be Success: {:?}",
        entries[0]
    );
    assert_eq!(entries[0].claimed_by, 1);
    assert_eq!(entries[0].op, op);

    // 本地队列不参与（恒空）
    assert!(driver.take_next_operator().is_none());

    let _ = shutdown_tx.send(true);
}

/// Test 2：Pending 条目目标 Region leader 是他节点（node2）→ executor 跳过、
/// 保持 Pending、无 propose（认领由他节点完成）。
#[tokio::test]
async fn test_global_queue_executor_leaves_op_for_other_region_leader() {
    let host = start_single_region0().await;
    let (driver, shutdown_tx, region_raft) = make_driver_and_region_handle();
    driver.attach_system_raft(host.system.clone());

    // 区域 1 的 leader 视图切到 node2（本 executor 节点 1 不是 leader）
    region_raft.set_leader(2);

    let op = add_peer_op(1, 3);
    host.system
        .propose_pd(PdOp::Enqueue {
            op: op.clone(),
            requester: 2,
            proposed_at_unix: 1_700_000_000,
        })
        .await
        .expect("enqueue via region0 raft");

    let ex = OperatorExecutor::new(Arc::clone(&driver), 1);
    let raft: Arc<dyn RegionRaftHandle> = region_raft.clone();
    let resolve: Arc<coord_server::pd::executor::RegionRaftResolver> =
        Arc::new(move |rid| {
            if rid == 1 {
                Some(raft.clone())
            } else {
                None
            }
        });

    let ran = ex.execute_one(&*resolve).await;
    assert!(ran.is_none(), "op led by node2 must not run on node1");

    // 队列保持 Pending；无成员变更
    let entries = host.mvcc.pd_queue_entries().expect("read queue");
    assert_eq!(entries.len(), 1);
    assert!(entries[0].is_pending(), "entry must stay pending: {:?}", entries[0]);
    assert_eq!(entries[0].claimed_by, 0);
    assert_eq!(region_raft.add_learner_calls.lock().unwrap().len(), 0);
    assert!(region_raft.promote_calls.lock().unwrap().is_empty());

    let _ = shutdown_tx.send(true);
}

/// Test 3：真实 raft apply 幂等去重（重复 Enqueue 同 operator → 单条目）；
/// 本地队列恒空。
#[tokio::test]
async fn test_global_queue_dedup_real_raft_and_no_local_queue() {
    let host = start_single_region0().await;
    let (driver, shutdown_tx, _region_raft) = make_driver_and_region_handle();
    driver.attach_system_raft(host.system.clone());

    let op = add_peer_op(1, 2);
    let r1 = host
        .system
        .propose_pd(PdOp::Enqueue {
            op: op.clone(),
            requester: 1,
            proposed_at_unix: 1_700_000_000,
        })
        .await
        .expect("enqueue #1");
    let r2 = host
        .system
        .propose_pd(PdOp::Enqueue {
            op: op.clone(),
            requester: 2,
            proposed_at_unix: 1_700_000_010,
        })
        .await
        .expect("enqueue #2 (dup)");
    assert!(r2 > r1);

    let entries = host.mvcc.pd_queue_entries().expect("read queue");
    assert_eq!(entries.len(), 1, "apply 层幂等去重：重复 Enqueue 仅一条");
    assert_eq!(entries[0].op_id, r1);
    assert_eq!(entries[0].requester, 1, "requester 保留首次提出方");

    // 执行器在 Pending 条目上运行一次 → 完成（Success）
    let ex = OperatorExecutor::new(Arc::clone(&driver), 1);
    let raft = Arc::new(FakeRegionRaft::new(1, 1));
    let rr: Arc<dyn RegionRaftHandle> = raft.clone();
    let resolve: Arc<coord_server::pd::executor::RegionRaftResolver> =
        Arc::new(move |rid| if rid == 1 { Some(rr.clone()) } else { None });
    let ran = ex.execute_one(&*resolve).await;
    assert!(ran.is_some());
    let entries = host.mvcc.pd_queue_entries().expect("read queue");
    assert!(matches!(
        entries[0].status,
        coord_server::pd::operator::OperatorStatus::Success
    ));
    assert!(driver.take_next_operator().is_none(), "本地队列不参与");

    let _ = shutdown_tx.send(true);
}

/// Test 4（P3 failover）：真实 region 0 raft 上——Running 条目认领者失联
/// （认领墙钟远超 `operator_running_timeout`，无 Complete）→ region 0 leader
/// 的调度 tick 经 raft `Requeue` 把它放回 Pending；存活 Region leader（本节点）
/// 随后重认领执行并 Complete 成功——认领者故障的端到端自愈。
#[tokio::test]
async fn test_global_queue_requeues_stale_running_and_reclaims_via_region0_raft() {
    let host = start_single_region0().await;

    // driver（node 1）：region 0 leader = 本节点；target_replicas=1 屏蔽
    // ReplicaChecker 噪音（region 1 单 voter 已达标）；balance_interval=1s
    let meta_store = Arc::new(PdMetaStore::new());
    meta_store
        .create_region(RegionMeta {
            region_id: 1,
            start_key: vec![],
            end_key: vec![],
            epoch: RegionEpoch::initial(),
            peers: vec![Peer {
                node_id: 1,
                raft_addr: "node1:50052".into(),
                role: PeerRole::Voter,
            }],
            approximate_size: 0,
            approximate_keys: 0,
        })
        .expect("create region meta");
    let cfg = PdConfig {
        balance_interval: 1,
        target_replicas: 1,
        operator_running_timeout: 300,
        ..PdConfig::default()
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let driver = Arc::new(PlacementDriver::new(cfg, meta_store, shutdown_rx, 1));
    driver.attach_system_raft(host.system.clone());

    // Enqueue AddPeer(region1, node2)，随后模拟"认领者 node 2 失联"：认领墙钟
    // 设在 10000s 前（>> operator_running_timeout=300），无 Complete
    let op = add_peer_op(1, 2);
    let rev = host
        .system
        .propose_pd(PdOp::Enqueue {
            op: op.clone(),
            requester: 1,
            proposed_at_unix: 1_700_000_000,
        })
        .await
        .expect("enqueue");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before epoch")
        .as_secs() as i64;
    host.system
        .propose_pd(PdOp::Claim {
            op_id: rev,
            node_id: 2,
            claimed_at_unix: now - 10_000,
        })
        .await
        .expect("claim (stale)");
    let entries = host.mvcc.pd_queue_entries().expect("read queue");
    assert_eq!(entries.len(), 1);
    assert!(entries[0].is_running());
    assert_eq!(entries[0].claimed_by, 2);

    // 启动调度循环（node 1 是 region 0 leader）→ 首个 tick 把卡死 Running
    // 条目 Requeue 回 Pending（真实 raft 日志 apply）
    let sched = driver.start_scheduler_loop();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let entries = host.mvcc.pd_queue_entries().expect("read queue");
        if entries.len() == 1 && entries[0].is_pending() && entries[0].claimed_by == 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "stale running op never requeued: {:?}",
            entries
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    shutdown_tx.send(true).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(3), sched)
        .await
        .expect("scheduler loop should exit");

    // failover 收尾：存活 Region leader（本节点 1）重认领 → 执行 → Complete
    let region_raft = Arc::new(FakeRegionRaft::new(1, 1));
    let ex = OperatorExecutor::new(Arc::clone(&driver), 1);
    let raft: Arc<dyn RegionRaftHandle> = region_raft.clone();
    let resolve: Arc<coord_server::pd::executor::RegionRaftResolver> =
        Arc::new(move |rid| if rid == 1 { Some(raft.clone()) } else { None });
    let ran = ex.execute_one(&*resolve).await;
    assert!(
        ran.is_some(),
        "requeued op must be reclaimed & executed by region leader"
    );
    assert_eq!(region_raft.add_learner_calls.lock().unwrap().len(), 1);

    let entries = host.mvcc.pd_queue_entries().expect("read queue");
    assert!(
        matches!(
            entries[0].status,
            coord_server::pd::operator::OperatorStatus::Success
        ),
        "entry must end Success after failover reclaim: {:?}",
        entries[0]
    );
    assert_eq!(entries[0].claimed_by, 1, "最终认领者是存活 leader");
}
