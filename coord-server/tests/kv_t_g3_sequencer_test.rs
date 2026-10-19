//! KV-T G3（#146）Commit Sequencer 验收：
//!
//! - 冲突事务按日志顺序决策：前一条 Committed、后一条 Conflict，冲突不写决策；
//! - 幂等重试返回首次 commit_ts，冲突重试换新 start_ts 后可成功；
//! - 条数上限切批（256/批），commit_ts 严格递增；apply 指标可观测；
//! - 两节点 region 0 raft：follower 不能提案（NotLeader），决策复制到 follower；
//! - 全部用户级结果都不让 raft apply 返回 `Err`（由 raft 提案成功本身证明）。

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use common::bind_listener;
use coord_core::error::Error;
use coord_core::storage::StorageBackend;
use coord_core::types::StorageConfig;
use coord_server::kvt::sequencer::{CommitCandidate, CommitSequencer};
use coord_server::metrics::Metrics;
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcServer, RaftRpcService};
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::raft::tso::{TsoAllocator, TsoClient};
use coord_server::raft::type_config::{SequencerMutation, SequencerOutcome};
use coord_server::raft::{new_basic_node, new_raft, CoordRaft, RaftConfig, RaftNode};
use coord_server::storage::mvcc::{MvccStorage, RevisionMode};
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::storage::snapshot::SnapshotTracker;
use tempfile::TempDir;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

fn raft_config() -> RaftConfig {
    RaftConfig {
        heartbeat_interval: 100,
        election_timeout_min: 400,
        election_timeout_max: 800,
        ..Default::default()
    }
}

fn candidate(
    txn_id: &[u8],
    start_ts: u64,
    request_id: &[u8],
    reads: &[&[u8]],
    writes: &[(&[u8], &[u8])],
) -> CommitCandidate {
    CommitCandidate {
        txn_id: txn_id.to_vec(),
        start_ts,
        request_id: request_id.to_vec(),
        read_set: reads
            .iter()
            .map(|key| key.to_vec())
            .collect::<BTreeSet<_>>(),
        mutations: writes
            .iter()
            .map(|(key, value)| SequencerMutation::Put {
                key: key.to_vec(),
                value: value.to_vec(),
                lease_id: None,
            })
            .collect(),
    }
}

struct SingleNode {
    _raft: Arc<CoordRaft>,
    mvcc: Arc<MvccStorage<RedbBackend>>,
    tso: Arc<TsoClient>,
    sequencer: CommitSequencer,
    metrics: Arc<Metrics>,
    _dir: TempDir,
}

impl SingleNode {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().to_path_buf();
        let backend = RedbBackend::open(&base, &StorageConfig::default()).unwrap();
        let mvcc =
            Arc::new(MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs).unwrap());
        let tracker = Arc::new(SnapshotTracker::default());
        let log_store = LogStore::new(&base).await.unwrap();
        let mut sm = StateMachineStore::new(Arc::clone(&mvcc), base.join("snapshots"), tracker);
        let metrics = Arc::new(Metrics::new());
        sm.metrics = Some(Arc::clone(&metrics));

        let addr = format!("127.0.0.1:{}", free_port());
        let factory = RaftNetworkFactoryImpl::new(1);
        factory.register_node(1, addr.clone());
        let raft = Arc::new(
            new_raft(1, Arc::new(raft_config()), factory, log_store, sm)
                .await
                .unwrap(),
        );
        raft.initialize(BTreeMap::from([(1u64, new_basic_node(&addr))]))
            .await
            .unwrap();
        wait_for_leader(&raft, 1, Duration::from_secs(10)).await;

        let tso = TsoClient::new(
            Arc::new(TsoAllocator::new(raft.as_ref().clone(), 1)),
            8,
            Some(Arc::clone(&metrics)),
        );
        let sequencer = CommitSequencer::new(
            raft.as_ref().clone(),
            1,
            Arc::clone(&tso),
            Arc::clone(&mvcc),
            256,
            1024 * 1024,
            coord_server::kvt::sequencer::SEQUENCER_PROPOSE_TIMEOUT,
        );

        Self {
            _raft: raft,
            mvcc,
            tso,
            sequencer,
            metrics,
            _dir: dir,
        }
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn wait_for_leader(raft: &Arc<CoordRaft>, node_id: u64, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let current = raft.current_leader().await;
        if current == Some(node_id) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "node {node_id} did not become leader (current={current:?})"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_sequencer_conflict_order_and_retry() {
    let node = SingleNode::start().await;

    let start1 = node.tso.get_ts().await.unwrap();
    let start2 = node.tso.get_ts().await.unwrap();

    let first = node
        .sequencer
        .validate_and_commit(vec![candidate(
            b"t1",
            start1,
            b"commit-1",
            &[b"k"],
            &[(b"k", b"v1")],
        )])
        .await
        .unwrap();
    let commit1 = match first.as_slice() {
        [SequencerOutcome::Committed { commit_ts }] => *commit_ts,
        other => panic!("unexpected first outcome: {other:?}"),
    };
    assert!(node.mvcc.get_sequencer_decision(commit1).unwrap().is_some());

    // t2 在 start2 时读 k；t1 已以 commit1 决策，必须冲突。
    let second = node
        .sequencer
        .validate_and_commit(vec![candidate(
            b"t2",
            start2,
            b"commit-2",
            &[b"k"],
            &[(b"k", b"v2")],
        )])
        .await
        .unwrap();
    let conflict_commit_ts = match second.as_slice() {
        [SequencerOutcome::Conflict {
            commit_ts,
            conflict_key,
            conflict_ts,
        }] => {
            assert_eq!(conflict_key, b"k");
            assert_eq!(*conflict_ts, commit1, "t2 must conflict with t1's decision");
            *commit_ts
        }
        other => panic!("expected a conflict, got {other:?}"),
    };
    assert!(
        node.mvcc
            .get_sequencer_decision(conflict_commit_ts)
            .unwrap()
            .is_none(),
        "conflict must not write a decision"
    );

    // 同 txn/request 重试：返回首次 commit_ts，不重复决策。
    let retry = node
        .sequencer
        .validate_and_commit(vec![candidate(
            b"t1",
            start2,
            b"commit-1",
            &[b"k"],
            &[(b"k", b"v1-retry")],
        )])
        .await
        .unwrap();
    assert_eq!(
        retry,
        vec![SequencerOutcome::Duplicate { commit_ts: commit1 }]
    );

    // 冲突后换新 start_ts 重试（新快照已包含 t1）：成功且时间戳更大。
    let fresh_start = node.tso.get_ts().await.unwrap();
    let retry_conflict = node
        .sequencer
        .validate_and_commit(vec![candidate(
            b"t2",
            fresh_start,
            b"commit-2",
            &[b"k"],
            &[(b"k", b"v2")],
        )])
        .await
        .unwrap();
    let commit2 = match retry_conflict.as_slice() {
        [SequencerOutcome::Committed { commit_ts }] => *commit_ts,
        other => panic!("unexpected retry outcome: {other:?}"),
    };
    assert!(commit2 > commit1);
    assert_eq!(
        node.mvcc
            .get_sequencer_decision(commit2)
            .unwrap()
            .unwrap()
            .mutations,
        vec![SequencerMutation::Put {
            key: b"k".to_vec(),
            value: b"v2".to_vec(),
            lease_id: None,
        }]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_sequencer_batches_256_and_metrics() {
    let node = SingleNode::start().await;
    let start_ts = node.tso.get_ts().await.unwrap();

    let candidates = (0..260u32)
        .map(|i| {
            let key = format!("key-{i:04}").into_bytes();
            let value = format!("value-{i}").into_bytes();
            candidate(
                format!("txn-{i:04}").as_bytes(),
                start_ts,
                format!("req-{i:04}").as_bytes(),
                &[],
                &[(&key, &value)],
            )
        })
        .collect::<Vec<_>>();

    let outcomes = node
        .sequencer
        .validate_and_commit(candidates)
        .await
        .unwrap();
    assert_eq!(outcomes.len(), 260);

    let mut last = 0u64;
    for (index, outcome) in outcomes.iter().enumerate() {
        let commit_ts = match outcome {
            SequencerOutcome::Committed { commit_ts } => *commit_ts,
            other => panic!("txn {index} unexpected outcome: {other:?}"),
        };
        assert!(
            commit_ts > last,
            "commit_ts must strictly increase (index {index}: {commit_ts} <= {last})"
        );
        last = commit_ts;
    }

    let rendered = node.metrics.render_prometheus_text();
    assert!(
        rendered.contains("coord_kvt_sequencer_batches_total 2"),
        "260 txns at 256/batch must take two raft batches:\n{rendered}"
    );
    assert!(
        rendered.contains("coord_kvt_sequencer_txns_total 260"),
        "txn counter must count every validated transaction"
    );
    assert!(rendered.contains("coord_kvt_sequencer_conflicts_total 0"));
}

struct ClusterNode {
    node_id: u64,
    raft: Arc<CoordRaft>,
    mvcc: Arc<MvccStorage<RedbBackend>>,
    _dir: TempDir,
    _factory: RaftNetworkFactoryImpl,
    _server: tokio::task::JoinHandle<()>,
}

async fn start_two_node_region0() -> Vec<ClusterNode> {
    let (listener1, addr1) = bind_listener().await;
    let (listener2, addr2) = bind_listener().await;
    let addrs = BTreeMap::from([(1u64, addr1), (2u64, addr2)]);
    let mut nodes = Vec::new();

    for (node_id, listener) in [(1u64, listener1), (2u64, listener2)] {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().to_path_buf();
        let backend = RedbBackend::open(&base, &StorageConfig::default()).unwrap();
        let mvcc =
            Arc::new(MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs).unwrap());
        let tracker = Arc::new(SnapshotTracker::default());
        let log_store = LogStore::new(&base).await.unwrap();
        let sm = StateMachineStore::new(Arc::clone(&mvcc), base.join("snapshots"), tracker);
        let factory = RaftNetworkFactoryImpl::new(node_id);
        for (id, addr) in &addrs {
            factory.register_node(*id, addr.clone());
        }
        let raft = Arc::new(
            new_raft(
                node_id,
                Arc::new(raft_config()),
                factory.clone(),
                log_store,
                sm,
            )
            .await
            .unwrap(),
        );
        let service = RaftRpcService::new();
        // region 0 system raft：出站消息 region_id=0，走默认实例。
        service.set_raft(raft.as_ref().clone());
        let server = tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(RaftRpcServer::new(service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await;
        });
        nodes.push(ClusterNode {
            node_id,
            raft,
            mvcc,
            _dir: dir,
            _factory: factory,
            _server: server,
        });
    }

    let members = addrs
        .iter()
        .map(|(id, addr)| (*id, new_basic_node(addr)))
        .collect::<BTreeMap<u64, RaftNode>>();
    nodes[0].raft.initialize(members).await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(leader) = nodes[0].raft.current_leader().await {
            if nodes[1].raft.current_leader().await == Some(leader) {
                return nodes;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "two-node region0 did not elect a common leader"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_two_node_sequencer_leader_only_and_replication() {
    let nodes = start_two_node_region0().await;
    let leader_id = nodes[0].raft.current_leader().await.expect("leader");
    let leader = nodes
        .iter()
        .find(|node| node.node_id == leader_id)
        .expect("leader host");
    let follower = nodes
        .iter()
        .find(|node| node.node_id != leader_id)
        .expect("follower host");

    let tso = TsoClient::new(
        Arc::new(TsoAllocator::new(leader.raft.as_ref().clone(), leader_id)),
        8,
        None,
    );
    let start_ts = tso.get_ts().await.unwrap();
    let sequencer = CommitSequencer::new(
        leader.raft.as_ref().clone(),
        leader_id,
        Arc::clone(&tso),
        Arc::clone(&leader.mvcc),
        256,
        1024 * 1024,
        coord_server::kvt::sequencer::SEQUENCER_PROPOSE_TIMEOUT,
    );
    let outcomes = sequencer
        .validate_and_commit(vec![candidate(
            b"t-leader",
            start_ts,
            b"r-leader",
            &[],
            &[(b"a", b"1")],
        )])
        .await
        .unwrap();
    let commit_ts = match outcomes.as_slice() {
        [SequencerOutcome::Committed { commit_ts }] => *commit_ts,
        other => panic!("unexpected leader outcome: {other:?}"),
    };

    // follower 的 TSO 客户端拒绝发放；sequencer 必须报 NotLeader，而不是本地提案。
    let follower_tso = TsoClient::new(
        Arc::new(TsoAllocator::new(
            follower.raft.as_ref().clone(),
            follower.node_id,
        )),
        8,
        None,
    );
    let follower_sequencer = CommitSequencer::new(
        follower.raft.as_ref().clone(),
        follower.node_id,
        follower_tso,
        Arc::clone(&follower.mvcc),
        256,
        1024 * 1024,
        coord_server::kvt::sequencer::SEQUENCER_PROPOSE_TIMEOUT,
    );
    let err = follower_sequencer
        .validate_and_commit(vec![candidate(
            b"t-follower",
            1,
            b"r-follower",
            &[],
            &[(b"b", b"2")],
        )])
        .await
        .expect_err("follower must not run the sequencer");
    assert!(
        matches!(err, Error::NotLeader { .. }),
        "follower sequencer must fail closed with NotLeader, got {err:?}"
    );

    // 决策随 region 0 raft 复制到 follower 的状态机。
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if follower
            .mvcc
            .get_sequencer_decision(commit_ts)
            .unwrap()
            .is_some()
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "follower did not apply the sequencer decision"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        follower
            .mvcc
            .get_sequencer_decision(commit_ts)
            .unwrap()
            .unwrap()
            .txn_id,
        b"t-leader".to_vec()
    );
}
