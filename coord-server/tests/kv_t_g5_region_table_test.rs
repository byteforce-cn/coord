//! KV-T G5（#159）权威 Region 表多节点验收：
//!
//! - 3 节点 region 0 raft；leader 提交 `Init`，各节点本地 apply 后表一致；
//! - split 批（旧区缩界 + 新区插入）在一个 raft 条目里原子生效；
//! - 非法区间批在 leader 被拒，三个副本的表版本与内容都不变（确定性）；
//! - 各节点 `CoordNode::refresh_region_table` 把表收敛进 `RegionManager`，
//!   key 路由按新区间生效；
//! - `[kv_t]` 关闭时不读表、不刷新（v1 静态装配不变）。

mod common;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use common::bind_listener;

use coord_core::storage::StorageBackend;
use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionMeta};
use coord_server::kvt::{KvTRuntimeConfig, KvTState};
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcServer, RaftRpcService};
use coord_server::raft::region::RegionManager;
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::raft::type_config::{Command, RegionMetaOp, RegionMetaOutcome, Response};
use coord_server::raft::{new_basic_node, new_raft, CoordRaft, RaftConfig, RaftNode};
use coord_server::server::CoordNode;
use coord_server::storage::mvcc::{MvccStorage, RevisionMode};
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::storage::region_table::{RegionChange, RegionTable};
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

fn peers(addrs: &BTreeMap<u64, String>) -> Vec<Peer> {
    addrs
        .iter()
        .map(|(node_id, addr)| Peer {
            node_id: *node_id,
            raft_addr: addr.clone(),
            role: PeerRole::Voter,
        })
        .collect()
}

fn region(id: u64, start: &[u8], end: &[u8], conf_ver: u64, version: u64) -> RegionMeta {
    RegionMeta {
        region_id: id,
        start_key: start.to_vec(),
        end_key: end.to_vec(),
        epoch: RegionEpoch { conf_ver, version },
        peers: vec![Peer {
            node_id: 1,
            raft_addr: "127.0.0.1:1".to_string(),
            role: PeerRole::Voter,
        }],
        approximate_size: 0,
        approximate_keys: 0,
    }
}

struct ClusterNode {
    id: u64,
    raft: Arc<CoordRaft>,
    mvcc: Arc<MvccStorage<RedbBackend>>,
    manager: Arc<RegionManager>,
    _server: tokio::task::JoinHandle<()>,
}

impl ClusterNode {
    async fn boot(
        id: u64,
        dir: PathBuf,
        listener: tokio::net::TcpListener,
        addrs: &BTreeMap<u64, String>,
    ) -> Self {
        let factory = RaftNetworkFactoryImpl::new(id);
        for (peer, peer_addr) in addrs {
            factory.register_node(*peer, peer_addr.clone());
        }
        let service = RaftRpcService::new();

        let backend = RedbBackend::open(&dir, &Default::default()).expect("open region 0");
        let mvcc =
            Arc::new(MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs).unwrap());
        let log = LogStore::new(&dir).await.expect("open region 0 log");
        let sm = StateMachineStore::new(
            Arc::clone(&mvcc),
            dir.join("snapshots"),
            Arc::new(SnapshotTracker::default()),
        );
        let raft = Arc::new(
            new_raft(id, Arc::new(raft_config()), factory, log, sm)
                .await
                .expect("create region 0 raft"),
        );
        service.set_raft(raft.as_ref().clone());

        // v1 静态装配：每个节点先按 initial_regions 注册好静态区间。
        let manager = Arc::new(RegionManager::new(id));
        manager
            .register_region(RegionMeta {
                region_id: 1,
                start_key: b"".to_vec(),
                end_key: b"".to_vec(),
                epoch: RegionEpoch::initial(),
                peers: peers(addrs),
                approximate_size: 0,
                approximate_keys: 0,
            })
            .expect("register static region");

        let service_for_server = service.clone();
        let server = tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(RaftRpcServer::new(service_for_server))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await;
        });

        Self {
            id,
            raft,
            mvcc,
            manager,
            _server: server,
        }
    }

    fn table(&self) -> RegionTable {
        self.mvcc.region_table().expect("read region table")
    }

    /// 模拟请求入口的按需刷新（`[kv_t]` 开启）。
    fn refresh(&self) -> bool {
        let mut node = CoordNode::new(Arc::clone(&self.mvcc));
        node.node_id = self.id;
        node.region_manager = Some(Arc::clone(&self.manager));
        node.set_kvt_state(KvTState::new(true, KvTRuntimeConfig::default()));
        node.refresh_region_table().expect("refresh region table")
    }
}

struct RegionTableCluster {
    _dir: TempDir,
    nodes: Vec<ClusterNode>,
    leader: u64,
}

impl RegionTableCluster {
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
            std::fs::create_dir_all(&node_dir).unwrap();
            nodes.push(ClusterNode::boot(id, node_dir, listener, &addrs).await);
        }

        let members: BTreeMap<u64, RaftNode> = addrs
            .iter()
            .map(|(id, addr)| (*id, new_basic_node(addr)))
            .collect();
        nodes[0]
            .raft
            .initialize(members)
            .await
            .expect("initialize region 0 cluster");

        let cluster = Self {
            _dir: dir,
            nodes,
            leader: 0,
        };
        let leader = cluster.wait_leader().await;
        Self { leader, ..cluster }
    }

    fn node(&self, id: u64) -> &ClusterNode {
        self.nodes.iter().find(|node| node.id == id).expect("node")
    }

    fn leader_node(&self) -> &ClusterNode {
        self.node(self.leader)
    }

    async fn wait_leader(&self) -> u64 {
        let stop = tokio::time::Instant::now() + DEADLINE;
        loop {
            let mut views = Vec::new();
            for node in &self.nodes {
                views.push(node.raft.current_leader().await);
            }
            if let Some(leader) = views[0] {
                if NODE_IDS.contains(&leader) && views.iter().all(|view| *view == Some(leader)) {
                    return leader;
                }
            }
            assert!(
                tokio::time::Instant::now() < stop,
                "region 0 leader not agreed: {views:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn propose(&self, op: RegionMetaOp) -> RegionMetaOutcome {
        let response = self
            .leader_node()
            .raft
            .client_write(Command::RegionMeta(op))
            .await
            .expect("propose region meta op");
        match response.response() {
            Response::RegionMeta { outcome } => outcome.clone(),
            other => panic!("unexpected response: {other}"),
        }
    }

    async fn wait_table(&self, version: u64) -> RegionTable {
        let stop = tokio::time::Instant::now() + DEADLINE;
        loop {
            let tables: Vec<RegionTable> = self.nodes.iter().map(|node| node.table()).collect();
            if tables.iter().all(|table| table == &tables[0]) && tables[0].version == version {
                return tables[0].clone();
            }
            assert!(
                tokio::time::Instant::now() < stop,
                "region table did not converge to version {version}: {:?}",
                tables.iter().map(|table| table.version).collect::<Vec<_>>()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_region_table_converges_and_routes_split() {
    let cluster = RegionTableCluster::start().await;

    let init = RegionMetaOp::Init {
        regions: vec![region(1, b"", b"", 1, 1)],
    };
    assert_eq!(
        cluster.propose(init).await,
        RegionMetaOutcome::Applied { version: 1 }
    );
    assert_eq!(cluster.wait_table(1).await.regions.len(), 1);

    let split = RegionMetaOp::Apply {
        changes: vec![
            RegionChange::Update {
                meta: region(1, b"", b"m", 1, 2),
            },
            RegionChange::Add {
                meta: region(2, b"m", b"", 1, 1),
            },
        ],
    };
    assert_eq!(
        cluster.propose(split).await,
        RegionMetaOutcome::Applied { version: 2 }
    );
    let table = cluster.wait_table(2).await;
    assert_eq!(table.regions.len(), 2);

    // 非法批：区间悬空 → 拒绝，三个副本的表都不变。
    let bad = RegionMetaOp::Apply {
        changes: vec![RegionChange::Add {
            meta: region(3, b"zz", b"", 1, 1),
        }],
    };
    let outcome = cluster.propose(bad).await;
    assert!(
        matches!(outcome, RegionMetaOutcome::Rejected { .. }),
        "expected rejection, got {outcome:?}"
    );
    assert_eq!(cluster.wait_table(2).await, table);

    // 请求入口按需刷新：每个节点的路由表都收敛到同一份区间。
    for node in &cluster.nodes {
        assert!(node.refresh(), "node {} refresh", node.id);
        assert_eq!(node.manager.table_version(), 2);
        assert_eq!(node.manager.route(b"a").unwrap().region_id(), 1);
        assert_eq!(node.manager.route(b"z").unwrap().region_id(), 2);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_region_table_refresh_is_off_when_kv_t_disabled() {
    let cluster = RegionTableCluster::start().await;
    let init = RegionMetaOp::Init {
        regions: vec![region(1, b"", b"", 1, 1)],
    };
    cluster.propose(init).await;
    cluster.wait_table(1).await;

    let node = cluster.node(cluster.leader);
    let mut disabled = CoordNode::new(Arc::clone(&node.mvcc));
    disabled.region_manager = Some(Arc::clone(&node.manager));
    assert!(
        !disabled.refresh_region_table().unwrap(),
        "kv_t disabled must not read/apply the region table"
    );
    assert_eq!(node.manager.table_version(), 0);
}
