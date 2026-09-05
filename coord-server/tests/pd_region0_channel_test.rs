// R-MR-08（D1-a）：PD 命令经 region 0 system raft 承载——P1 端到端验收
//
// 单节点 region 0 raft（与 `CoordNode.node.raft` 同型：root MVCC +
// StateMachineStore），直接经 raft `client_write(Command::Pd(...))` 提出命令，
// 验证 D1-a 基座（docs/coord-multi-raft-production-plan-2026-09-05.md §4.5 P1）：
//
//   Test 1：Enqueue → region 0 MVCC `/_pd/ops/` 队列可见；`op_id` == 该命令的
//           日志 index（raft 返回的 revision）；重复 Enqueue 同 operator →
//           队列仅一条（全局去重）；Claim → Running + claimed_by；Complete →
//           Success（终态）。
//   Test 2：队列状态随 redb 重开持久（不依赖 raft 重启）；重放/重启后一致。
//
// 单节点 raft 语义：self-only 成员（quorum=1），无 raft RPC server 亦可
// client_write（与 region_lease_test / restart_recovery_test 同模式）。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use coord_core::storage::StorageBackend;
use coord_core::types::StorageConfig;
use coord_server::pd::Operator;
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::RaftNetworkFactoryImpl;
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::raft::type_config::{Command, PdOp, Response};
use coord_server::raft::{new_basic_node, new_raft, RaftConfig, WatchReceiver};
use coord_server::storage::mvcc::MvccStorage;
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::storage::snapshot::SnapshotTracker;

struct Region0Host {
    raft: Arc<coord_server::raft::CoordRaft>,
    mvcc: Arc<MvccStorage<RedbBackend>>,
    // 持有 tempdir 生命周期（字段不读，仅防目录提前删除）
    _dir: tempfile::TempDir,
}

/// 装配单节点 region 0 raft（D1-a：PD 命令的全局承载 raft）
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

    Region0Host {
        raft,
        mvcc,
        _dir: dir,
    }
}

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

/// 经 region 0 raft 提出一条 PD 命令并返回日志 index（Response::Put.revision）
async fn propose(raft: &coord_server::raft::CoordRaft, op: PdOp) -> u64 {
    let resp = raft
        .client_write(Command::Pd(op))
        .await
        .expect("client_write Pd command");
    match resp.response() {
        Response::Put { revision } => *revision,
        other => panic!("unexpected pd response: {other:?}"),
    }
}

/// Test 1：Enqueue → 队列可见（op_id == 日志 index）；去重；Claim/Complete。
#[tokio::test]
async fn test_pd_region0_channel_enqueue_dedup_and_lifecycle() {
    let host = start_single_region0().await;

    // Enqueue AddPeer(region=1, node=3)
    let rev = propose(
        &host.raft,
        PdOp::Enqueue {
            op: add_peer_op(1, 3),
            requester: 1,
            proposed_at_unix: 1_700_000_000,
        },
    )
    .await;

    let entries = host.mvcc.pd_queue_entries().expect("read queue");
    assert_eq!(entries.len(), 1, "enqueue must produce exactly one queue entry");
    assert_eq!(entries[0].op_id, rev, "op_id must equal enqueue log index");
    assert!(entries[0].is_pending());
    assert_eq!(entries[0].op, add_peer_op(1, 3));

    // 重复 Enqueue 同 operator（不同日志 index）→ 全局去重 no-op
    let rev2 = propose(
        &host.raft,
        PdOp::Enqueue {
            op: add_peer_op(1, 3),
            requester: 2,
            proposed_at_unix: 1_700_000_010,
        },
    )
    .await;
    assert!(rev2 > rev);
    let entries = host.mvcc.pd_queue_entries().expect("read queue");
    assert_eq!(entries.len(), 1, "duplicate enqueue must be deduped (global)");
    assert_eq!(entries[0].op_id, rev);

    // Claim 由 node 3（目标 leader）→ Running
    propose(&host.raft, PdOp::Claim { op_id: rev, node_id: 3 }).await;
    let entries = host.mvcc.pd_queue_entries().expect("read queue");
    assert_eq!(entries.len(), 1);
    assert!(entries[0].is_running());
    assert_eq!(entries[0].claimed_by, 3);

    // 他节点（2）Complete → no-op（保持 Running）
    propose(
        &host.raft,
        PdOp::Complete {
            op_id: rev,
            node_id: 2,
            success: true,
            error: String::new(),
        },
    )
    .await;
    assert!(host.mvcc.pd_queue_entries().unwrap()[0].is_running());

    // 认领者（3）Complete success → Success（终态）
    propose(
        &host.raft,
        PdOp::Complete {
            op_id: rev,
            node_id: 3,
            success: true,
            error: String::new(),
        },
    )
    .await;
    let entries = host.mvcc.pd_queue_entries().expect("read queue");
    assert_eq!(entries.len(), 1);
    assert!(entries[0].is_terminal(), "entry must reach terminal state");
}

/// Test 2：队列状态 redb 持久（drop raft/存储 → 重开同目录后端仍可读）。
#[tokio::test]
async fn test_pd_region0_channel_queue_persists_across_backend_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().to_path_buf();

    // 先直接经 apply 路径造一条 Running 队列（无 raft；等价于 raft apply 后的状态）
    let config = StorageConfig::default();
    let backend = RedbBackend::open(&base, &config).expect("open backend");
    let storage = MvccStorage::new(backend).expect("mvcc");
    storage
        .apply_pd_op(
            &PdOp::Enqueue {
                op: add_peer_op(2, 5),
                requester: 1,
                proposed_at_unix: 1_700_000_000,
            },
            1,
            coord_server::storage::mvcc::AppliedLogId::standalone(1),
        )
        .expect("enqueue");
    storage
        .apply_pd_op(
            &PdOp::Claim {
                op_id: 1,
                node_id: 5,
            },
            2,
            coord_server::storage::mvcc::AppliedLogId::standalone(2),
        )
        .expect("claim");
    drop(storage);

    // 重开同目录 redb → 队列仍可读且状态一致
    let backend = RedbBackend::open(&base, &config).expect("reopen backend");
    let reopened = MvccStorage::new(backend).expect("reopen mvcc");
    let entries = reopened.pd_queue_entries().expect("read queue");
    assert_eq!(entries.len(), 1, "pd queue must survive backend reopen");
    assert_eq!(entries[0].op_id, 1);
    assert!(entries[0].is_running());
    assert_eq!(entries[0].claimed_by, 5);
}
