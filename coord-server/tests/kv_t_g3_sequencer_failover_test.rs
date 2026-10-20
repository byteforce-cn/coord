//! KV-T G3（#152）Commit Sequencer 故障切换验收：
//!
//! - kill region 0 leader 后，新 leader 对已决策事务返回首次 `commit_ts`；
//! - 新事务在新 leader 上拿到更大的 `commit_ts`，决策随 raft 复制；
//! - 决策日志里同一 `txn_id` 只有一条，所有 `commit_ts` 唯一；
//! - 中断的多批提交在重试后每个事务仍只有一条决策；
//! - 被 kill 的节点重启后追平决策并可继续服务；
//! - leader 被分区后其余节点继续提交，愈合后收敛、无重复决策。
//!
//! 进程级 SIGSTOP / 真实网络分区的证据归 #175（G6 混沌套件）；本文件用
//! openraft 核心 shutdown + 既有 partition-sim 黑名单覆盖同一不变量。

mod common;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use common::bind_listener;
use coord_core::error::Error as CoordError;
use coord_core::storage::StorageBackend;
use coord_core::types::StorageConfig;
use coord_server::kvt::sequencer::{CommitCandidate, CommitSequencer, SEQUENCER_PROPOSE_TIMEOUT};
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcServer, RaftRpcService};
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::raft::tso::{TsoAllocator, TsoClient};
use coord_server::raft::type_config::{
    SequencerMutation, SequencerOutcome, SequencerReadRange, SequencerValidationLimits,
};
use coord_server::raft::{new_basic_node, new_raft, CoordRaft, RaftConfig, RaftNode};
use coord_server::storage::mvcc::{MvccStorage, RevisionMode};
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::storage::snapshot::SnapshotTracker;
use tempfile::TempDir;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

const NODE_IDS: [u64; 3] = [1, 2, 3];
const DEADLINE: Duration = Duration::from_secs(30);

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
        read_set: reads.iter().map(|key| key.to_vec()).collect(),
        read_ranges: Vec::<SequencerReadRange>::new(),
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

fn committed_ts(outcomes: &[SequencerOutcome]) -> u64 {
    match outcomes {
        [SequencerOutcome::Committed { commit_ts }] => *commit_ts,
        other => panic!("expected one Committed outcome, got {other:?}"),
    }
}

/// 只重试可重试的传输/选举错误；其他错误（Internal/存储）直接失败。
async fn failover_commit_until(
    sequencer: Arc<CommitSequencer>,
    candidates: Vec<CommitCandidate>,
    deadline: Duration,
) -> Vec<SequencerOutcome> {
    let stop = tokio::time::Instant::now() + deadline;
    loop {
        match sequencer.validate_and_commit(candidates.clone()).await {
            Ok(outcomes) => return outcomes,
            Err(CoordError::NotLeader { .. } | CoordError::ClusterUnavailable(_))
                if tokio::time::Instant::now() < stop =>
            {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(err) => panic!("unexpected sequencer error: {err}"),
        }
    }
}

struct FailoverNode {
    id: u64,
    dir: PathBuf,
    factory: RaftNetworkFactoryImpl,
    /// 与服务任务共享同一张 region→raft 表；重启时替换实例而不重绑 listener。
    service: RaftRpcService,
    raft: Option<Arc<CoordRaft>>,
    mvcc: Option<Arc<MvccStorage<RedbBackend>>>,
    _server: tokio::task::JoinHandle<()>,
}

impl FailoverNode {
    async fn boot(
        id: u64,
        dir: PathBuf,
        listener: tokio::net::TcpListener,
        rendezvous: &BTreeMap<u64, String>,
    ) -> Self {
        let factory = RaftNetworkFactoryImpl::new(id);
        for (peer, peer_addr) in rendezvous {
            factory.register_node(*peer, peer_addr.clone());
        }
        let service = RaftRpcService::new();
        let (raft, mvcc) = open_node(id, &dir, factory.clone(), &service).await;

        let service_for_server = service.clone();
        let server = tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(RaftRpcServer::new(service_for_server))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await;
        });

        Self {
            id,
            dir,
            factory,
            service,
            raft: Some(raft),
            mvcc: Some(mvcc),
            _server: server,
        }
    }
}

/// 打开（或重开）一个节点的 redb/log store/state machine，并注册到 `service`。
async fn open_node(
    id: u64,
    dir: &std::path::Path,
    factory: RaftNetworkFactoryImpl,
    service: &RaftRpcService,
) -> (Arc<CoordRaft>, Arc<MvccStorage<RedbBackend>>) {
    let backend = RedbBackend::open(dir, &StorageConfig::default()).expect("open redb backend");
    let mvcc = Arc::new(
        MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs).expect("create mvcc"),
    );
    let tracker = Arc::new(SnapshotTracker::default());
    let log_store = LogStore::new(dir).await.expect("open raft log store");
    let sm = StateMachineStore::new(Arc::clone(&mvcc), dir.join("snapshots"), tracker);
    let raft = Arc::new(
        new_raft(id, Arc::new(raft_config()), factory, log_store, sm)
            .await
            .expect("create raft instance"),
    );
    service.set_raft(raft.as_ref().clone());
    (raft, mvcc)
}

struct FailoverCluster {
    _dir: TempDir,
    nodes: Vec<FailoverNode>,
}

impl FailoverCluster {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut addrs = BTreeMap::new();
        let mut listeners = Vec::new();
        for id in NODE_IDS {
            let (listener, addr) = bind_listener().await;
            addrs.insert(id, addr);
            listeners.push((id, listener));
        }

        let mut nodes = Vec::new();
        for (id, listener) in listeners {
            let node_dir = dir.path().join(format!("node-{id}"));
            std::fs::create_dir_all(&node_dir).expect("create node dir");
            nodes.push(FailoverNode::boot(id, node_dir, listener, &addrs).await);
        }

        let members = addrs
            .iter()
            .map(|(id, addr)| (*id, new_basic_node(addr)))
            .collect::<BTreeMap<u64, RaftNode>>();
        nodes[0]
            .raft
            .as_ref()
            .unwrap()
            .initialize(members)
            .await
            .unwrap();

        let cluster = Self { _dir: dir, nodes };
        cluster
            .wait_leader_among(&NODE_IDS, Duration::from_secs(15))
            .await;
        cluster
    }

    fn ids(&self) -> Vec<u64> {
        self.nodes.iter().map(|node| node.id).collect()
    }

    fn node(&self, id: u64) -> &FailoverNode {
        self.nodes.iter().find(|node| node.id == id).expect("node")
    }

    fn node_mut(&mut self, id: u64) -> &mut FailoverNode {
        self.nodes
            .iter_mut()
            .find(|node| node.id == id)
            .expect("node")
    }

    async fn leader(&self) -> u64 {
        self.wait_leader_among(&self.ids(), Duration::from_secs(15))
            .await
    }

    /// 等待 `ids` 中所有存活节点对同一 leader（且该 leader 在 `ids` 内）达成一致。
    async fn wait_leader_among(&self, ids: &[u64], timeout: Duration) -> u64 {
        let stop = tokio::time::Instant::now() + timeout;
        loop {
            let mut views = Vec::new();
            for id in ids {
                let view = match self.node(*id).raft.as_ref() {
                    Some(raft) => raft.current_leader().await,
                    None => None,
                };
                views.push(view);
            }
            if let Some(leader) = views[0] {
                if ids.contains(&leader) && views.iter().all(|view| *view == Some(leader)) {
                    return leader;
                }
            }
            assert!(
                tokio::time::Instant::now() < stop,
                "no agreed leader among {ids:?}: {views:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn tso(&self, id: u64) -> Arc<TsoClient> {
        let raft = self.node(id).raft.as_ref().expect("node running");
        TsoClient::new(
            Arc::new(TsoAllocator::new(raft.as_ref().clone(), id)),
            8,
            None,
        )
    }

    fn sequencer(&self, id: u64) -> Arc<CommitSequencer> {
        self.sequencer_with_batch(id, 256)
    }

    fn sequencer_with_batch(&self, id: u64, max_batch_count: usize) -> Arc<CommitSequencer> {
        let node = self.node(id);
        let raft = node.raft.as_ref().expect("node running");
        let mvcc = node.mvcc.as_ref().expect("node running");
        Arc::new(CommitSequencer::new(
            raft.as_ref().clone(),
            id,
            self.tso(id),
            Arc::clone(mvcc),
            max_batch_count,
            1024 * 1024,
            SequencerValidationLimits {
                max_read_set: 4_096,
                max_range: 10_000,
            },
            SEQUENCER_PROPOSE_TIMEOUT,
        ))
    }

    /// 模拟进程崩溃/失联：停止 raft 核心并从 RPC 表摘除实例，listener 保留。
    async fn kill(&mut self, id: u64) {
        let node = self.node_mut(id);
        node.service.remove_region_raft(0);
        let raft = node.raft.take().expect("node running");
        let _ = raft.shutdown().await;
        drop(raft);
        drop(node.mvcc.take());
        // redb 文件锁由 raft 后台任务持有；与 restart_recovery_test 同窗口。
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    /// 用同一数据目录/监听地址拉起新实例，替换 RPC 表里的旧 raft。
    async fn restart(&mut self, id: u64) {
        let dir = self.node(id).dir.clone();
        let factory = self.node(id).factory.clone();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let (raft, mvcc) = loop {
            match open_node_retry(id, &dir, factory.clone(), &self.node(id).service).await {
                Ok(opened) => break opened,
                Err(err) => {
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "reopen node {id} failed: {err}"
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        };
        let node = self.node_mut(id);
        node.raft = Some(raft);
        node.mvcc = Some(mvcc);
    }

    /// 双向分区：本节点到所有对端、所有对端到本节点都不可达。
    fn partition(&self, id: u64) {
        for node in &self.nodes {
            if node.id == id {
                for peer in self.ids().into_iter().filter(|peer| *peer != id) {
                    node.factory.block_node(peer);
                }
            } else {
                node.factory.block_node(id);
            }
        }
    }

    fn heal(&self, id: u64) {
        for node in &self.nodes {
            if node.id == id {
                for peer in self.ids().into_iter().filter(|peer| *peer != id) {
                    node.factory.unblock_node(peer);
                }
            } else {
                node.factory.unblock_node(id);
            }
        }
    }

    fn decisions(&self, id: u64) -> Vec<(u64, Vec<u8>)> {
        let mut decisions = self
            .node(id)
            .mvcc
            .as_ref()
            .expect("node running")
            .sequencer_decisions_after(0, 4_096)
            .expect("scan decisions")
            .into_iter()
            .map(|decision| (decision.commit_ts, decision.txn_id))
            .collect::<Vec<_>>();
        decisions.sort();
        decisions
    }

    async fn wait_decisions(&self, id: u64, expected: &[(u64, Vec<u8>)], timeout: Duration) {
        let stop = tokio::time::Instant::now() + timeout;
        loop {
            if self.decisions(id) == expected {
                return;
            }
            assert!(
                tokio::time::Instant::now() < stop,
                "node {id} decisions never converged: {:?} != {expected:?}",
                self.decisions(id)
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// redb 文件锁释放存在短窗口；重试用同一路径打开。
async fn open_node_retry(
    id: u64,
    dir: &std::path::Path,
    factory: RaftNetworkFactoryImpl,
    service: &RaftRpcService,
) -> Result<(Arc<CoordRaft>, Arc<MvccStorage<RedbBackend>>), String> {
    let backend = RedbBackend::open(dir, &StorageConfig::default())
        .map_err(|err| format!("open redb backend: {err}"))?;
    let mvcc = Arc::new(
        MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs)
            .map_err(|err| format!("create mvcc: {err}"))?,
    );
    let tracker = Arc::new(SnapshotTracker::default());
    let log_store = LogStore::new(dir)
        .await
        .map_err(|err| format!("open raft log store: {err}"))?;
    let sm = StateMachineStore::new(Arc::clone(&mvcc), dir.join("snapshots"), tracker);
    let raft = new_raft(id, Arc::new(raft_config()), factory, log_store, sm)
        .await
        .map_err(|err| format!("create raft instance: {err}"))?;
    let raft = Arc::new(raft);
    service.set_raft(raft.as_ref().clone());
    Ok((raft, mvcc))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_sequencer_failover_replays_and_continues() {
    let mut cluster = FailoverCluster::start().await;
    let leader = cluster.leader().await;
    let start = cluster.tso(leader).get_ts().await.unwrap();
    let ts_a = committed_ts(
        &failover_commit_until(
            cluster.sequencer(leader),
            vec![candidate(
                b"failover-a",
                start,
                b"req-a",
                &[],
                &[(b"a", b"1")],
            )],
            DEADLINE,
        )
        .await,
    );

    cluster.kill(leader).await;
    let alive = cluster
        .ids()
        .into_iter()
        .filter(|id| *id != leader)
        .collect::<Vec<_>>();
    let new_leader = cluster
        .wait_leader_among(&alive, Duration::from_secs(10))
        .await;
    assert_ne!(new_leader, leader);

    // 已决策不丢：同一 txn_id + request_id 在新 leader 上返回首次 commit_ts。
    let replay = failover_commit_until(
        cluster.sequencer(new_leader),
        vec![candidate(
            b"failover-a",
            start,
            b"req-a",
            &[],
            &[(b"a", b"1")],
        )],
        DEADLINE,
    )
    .await;
    assert_eq!(
        replay,
        vec![SequencerOutcome::Duplicate { commit_ts: ts_a }]
    );

    // 继续服务：新事务拿到更大的 commit_ts。
    let start_b = cluster.tso(new_leader).get_ts().await.unwrap();
    let ts_b = committed_ts(
        &failover_commit_until(
            cluster.sequencer(new_leader),
            vec![candidate(
                b"failover-b",
                start_b,
                b"req-b",
                &[],
                &[(b"b", b"2")],
            )],
            DEADLINE,
        )
        .await,
    );
    assert!(ts_b > ts_a, "commit_ts must keep increasing after failover");

    let expected = vec![
        (ts_a, b"failover-a".to_vec()),
        (ts_b, b"failover-b".to_vec()),
    ];
    for id in &alive {
        cluster
            .wait_decisions(*id, &expected, Duration::from_secs(10))
            .await;
    }
}
