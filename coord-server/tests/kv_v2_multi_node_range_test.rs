//! KV-T G2（#188）跨 Region Range 的跨节点读（本地快照读）：
//!
//! - 3 节点 × 3 数据 Region，leader 分散（r1→n1、r2→n2、r3→n3）；
//! - 任一节点都能在显式 `read_ts` 上服务跨 Region 快照，不要求本节点是 leader；
//! - `read_ts=0` 取**本节点** safe_ts；
//! - 本地 applied 不足 → `SAFE_TS_LAG`；Region 运行时缺失 → 整请求失败，
//!   不返回部分分段结果。

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use coord_core::error_code::error_code_of;
use coord_core::storage::StorageBackend;
use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionId, RegionMeta, StorageConfig};
use coord_proto::kv::v2::kv_server::Kv as KvV2;
use coord_proto::kv::v2::{RangeRequest as RangeRequestV2, RangeResponse as RangeResponseV2};
use coord_server::kvt::{KvTCapability, KvTRuntimeConfig, KvTState};
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcServer, RaftRpcService};
use coord_server::raft::region::RegionManager;
use coord_server::raft::region_runtime::{region_data_dir, RegionRuntimeSpec};
use coord_server::raft::type_config::{Command, Response};
use coord_server::raft::{new_basic_node, RaftConfig, RaftNode, WatchReceiver};
use coord_server::server::CoordNode;
use coord_server::storage::mvcc::{MvccStorage, RevisionMode};
use coord_server::storage::redb_backend::RedbBackend;
use tempfile::TempDir;
use tonic::Code;

use crate::common::bind_listener;

const NODES: [u64; 3] = [1, 2, 3];
const REGIONS: [(RegionId, &[u8], &[u8]); 3] = [(1, b"", b"d"), (2, b"d", b"n"), (3, b"n", b"")];

struct NodeHost {
    node_id: u64,
    node: Arc<CoordNode>,
    manager: Arc<RegionManager>,
    _dir: TempDir,
    _factory: RaftNetworkFactoryImpl,
    _raft_handle: tokio::task::JoinHandle<()>,
}

struct Cluster {
    hosts: Vec<NodeHost>,
}

fn region_meta(
    region_id: RegionId,
    start: &[u8],
    end: &[u8],
    raft_addrs: &BTreeMap<u64, String>,
) -> RegionMeta {
    RegionMeta {
        region_id,
        start_key: start.to_vec(),
        end_key: end.to_vec(),
        epoch: RegionEpoch::initial(),
        peers: raft_addrs
            .iter()
            .map(|(node_id, raft_addr)| Peer {
                node_id: *node_id,
                raft_addr: raft_addr.clone(),
                role: PeerRole::Voter,
            })
            .collect(),
        approximate_size: 0,
        approximate_keys: 0,
    }
}

impl Cluster {
    async fn start() -> Self {
        // 监听器预绑定并由 serve_with_incoming 接管（见 common::bind_listener）。
        let mut raft_listeners = BTreeMap::new();
        let mut raft_addrs: BTreeMap<u64, String> = BTreeMap::new();
        for node_id in NODES {
            let (listener, addr) = bind_listener().await;
            raft_listeners.insert(node_id, listener);
            raft_addrs.insert(node_id, addr);
        }

        let mut hosts = Vec::new();
        for node_id in NODES {
            let dir = tempfile::tempdir().unwrap();
            let base = dir.path().to_path_buf();
            let factory = RaftNetworkFactoryImpl::new(node_id);
            for (id, addr) in &raft_addrs {
                factory.register_node(*id, addr.clone());
            }
            let rpc = RaftRpcService::new();
            let manager = Arc::new(RegionManager::new(node_id));
            for (region_id, start, end) in REGIONS {
                manager
                    .spawn_region(
                        &factory,
                        &rpc,
                        RegionRuntimeSpec {
                            meta: region_meta(region_id, start, end, &raft_addrs),
                            data_dir: region_data_dir(&base, region_id),
                            raft_config: Arc::new(RaftConfig {
                                heartbeat_interval: 100,
                                election_timeout_min: 300,
                                election_timeout_max: 600,
                                ..Default::default()
                            }),
                            object_store: None,
                            revision_mode: RevisionMode::CommitTs,
                        },
                        false,
                    )
                    .await
                    .expect("spawn data region");
            }

            // region 模式下 KV 全量走 manager 路由；scratch 只满足构造签名。
            let backend = RedbBackend::open(&base, &StorageConfig::default()).unwrap();
            let scratch = Arc::new(MvccStorage::new(backend).unwrap());
            let mut node = CoordNode::new(scratch);
            node.node_id = node_id;
            node.region_manager = Some(Arc::clone(&manager));
            node.set_kvt_state(
                KvTState::new(
                    true,
                    KvTRuntimeConfig {
                        max_range: 100,
                        read_ts_wait_ms: 50,
                        ..KvTRuntimeConfig::default()
                    },
                )
                .with_implemented([KvTCapability::Kv]),
            );
            let node = Arc::new(node);

            let listener = raft_listeners.remove(&node_id).expect("listener pre-bound");
            let svc = RaftRpcServer::new(rpc);
            let raft_handle = tokio::spawn(async move {
                let _ = tonic::transport::Server::builder()
                    .add_service(svc)
                    .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                    .await;
            });

            hosts.push(NodeHost {
                node_id,
                node,
                manager,
                _dir: dir,
                _factory: factory,
                _raft_handle: raft_handle,
            });
        }

        // 所有 raft 监听器就绪后再初始化成员（否则 initialize 等不到 quorum）。
        for addr in raft_addrs.values() {
            tokio::net::TcpStream::connect(addr)
                .await
                .expect("raft server ready");
        }
        let members: BTreeMap<u64, RaftNode> = raft_addrs
            .iter()
            .map(|(id, addr)| (*id, new_basic_node(addr)))
            .collect();
        for (region_id, _, _) in REGIONS {
            hosts[0]
                .manager
                .runtime(region_id)
                .unwrap()
                .raft
                .initialize(members.clone())
                .await
                .expect("initialize 3-voter region");
        }

        let cluster = Self { hosts };
        cluster.spread_leaders().await;
        cluster
    }

    fn host(&self, node_id: u64) -> &NodeHost {
        self.hosts
            .iter()
            .find(|h| h.node_id == node_id)
            .expect("node exists")
    }

    async fn leader_node(&self, region_id: RegionId) -> u64 {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            for host in &self.hosts {
                let rt = host.manager.runtime(region_id).expect("region runtime");
                let m = rt.raft.metrics();
                let m = m.borrow_watched();
                if m.last_quorum_acked.is_some() && m.current_leader == Some(host.node_id) {
                    return host.node_id;
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "region {region_id} has no leader"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// r1→n1、r2→n2、r3→n3，使任一请求节点都至少对一个命中 Region 是 follower。
    async fn spread_leaders(&self) {
        for (region_id, target) in [(1u64, 1u64), (2, 2), (3, 3)] {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
            loop {
                let leader = self.leader_node(region_id).await;
                if leader == target {
                    break;
                }
                self.host(leader)
                    .manager
                    .runtime(region_id)
                    .unwrap()
                    .raft
                    .trigger()
                    .transfer_leader(target)
                    .await
                    .expect("request leader transfer");
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "region {region_id} leadership did not move to node {target}"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    async fn put(&self, region_id: RegionId, key: &[u8], value: &[u8], commit_ts: u64) {
        let leader = self.leader_node(region_id).await;
        let rt = self.host(leader).manager.runtime(region_id).unwrap();
        let resp = rt
            .raft
            .client_write(Command::PutV2 {
                key: key.to_vec(),
                value: value.to_vec(),
                lease_id: None,
                commit_ts,
            })
            .await
            .expect("put v2");
        assert!(
            matches!(resp.response(), Response::Put { revision } if *revision == commit_ts),
            "region {region_id} put must apply at its commit_ts"
        );
    }

    async fn seed(&self) {
        // 每 Region 内 commit_ts 严格递增；min(400, 420, 400) = 400。
        self.put(1, b"a", b"a100", 100).await;
        self.put(1, b"b", b"b200", 200).await;
        self.put(1, b"a2", b"a240", 240).await;
        self.put(1, b"a3", b"a400", 400).await;
        self.put(2, b"d", b"d100", 100).await;
        self.put(2, b"e", b"e250", 250).await;
        self.put(2, b"e2", b"e420", 420).await;
        self.put(3, b"n", b"n100", 100).await;
        self.put(3, b"z", b"z400", 400).await;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let applied_everywhere = self.hosts.iter().all(|h| {
                REGIONS.iter().all(|(region_id, _, _)| {
                    h.manager
                        .runtime(*region_id)
                        .unwrap()
                        .mvcc
                        .current_revision()
                        >= 400
                })
            });
            if applied_everywhere {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "v2 writes did not converge to all nodes"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        for host in &self.hosts {
            assert_eq!(host.node.safe_ts(), 400, "node {} safe_ts", host.node_id);
        }
    }

    async fn range(&self, node_id: u64, read_ts: i64) -> Result<RangeResponseV2, tonic::Status> {
        KvV2::range(
            &*self.host(node_id).node,
            tonic::Request::new(RangeRequestV2 {
                key: b"a".to_vec(),
                range_end: b"zzz".to_vec(),
                read_ts,
                ..Default::default()
            }),
        )
        .await
        .map(|resp| resp.into_inner())
    }
}

fn keys(resp: &RangeResponseV2) -> Vec<Vec<u8>> {
    resp.kvs.iter().map(|kv| kv.key.clone()).collect()
}

/// read_ts=400 时可见：r1 a/a2/a3/b，r2 d/e（e2@420 排除），r3 n/z。
fn expected_keys() -> Vec<Vec<u8>> {
    [b"a".as_slice(), b"a2", b"a3", b"b", b"d", b"e", b"n", b"z"]
        .iter()
        .map(|k| k.to_vec())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_any_node_serves_cross_region_range_snapshot() {
    let cluster = Cluster::start().await;
    cluster.seed().await;

    // leader 分散：请求节点对至少一个命中 Region 只是 follower。
    assert_eq!(cluster.leader_node(1).await, 1);
    assert_eq!(cluster.leader_node(2).await, 2);
    assert_eq!(cluster.leader_node(3).await, 3);

    for node_id in NODES {
        let resp = cluster
            .range(node_id, 400)
            .await
            .unwrap_or_else(|e| panic!("node {node_id} must serve the snapshot: {e:?}"));
        assert_eq!(resp.read_ts, 400);
        assert_eq!(
            keys(&resp),
            expected_keys(),
            "node {node_id} explicit read_ts"
        );

        // read_ts=0 → 本节点 safe_ts（三节点已收敛，同为 400）。
        let resp0 = cluster.range(node_id, 0).await.unwrap();
        assert_eq!(
            resp0.read_ts, 400,
            "node {node_id} read_ts=0 must echo local safe_ts"
        );
        assert_eq!(keys(&resp0), expected_keys(), "node {node_id} read_ts=0");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_range_fails_closed_when_local_snapshot_is_missing() {
    let cluster = Cluster::start().await;
    cluster.seed().await;

    // read_ts 超过本地 safe_ts：等待超时 → SAFE_TS_LAG（可重试），无部分结果。
    let err = cluster
        .range(2, 10_000)
        .await
        .expect_err("above safe_ts must fail");
    assert_eq!(err.code(), Code::Unavailable, "{err:?}");
    assert_eq!(error_code_of(&err).as_deref(), Some("SAFE_TS_LAG"));

    // 请求节点缺一个命中 Region 的运行时：整请求失败，不返回其他分段。
    cluster.host(3).manager.remove_region_runtime(2).unwrap();
    let err = cluster
        .range(3, 400)
        .await
        .expect_err("route gap must fail the whole request");
    assert_eq!(err.code(), Code::Unavailable, "{err:?}");

    // 其他节点不受影响：仍是同一快照。
    let resp = cluster.range(1, 400).await.unwrap();
    assert_eq!(resp.read_ts, 400);
    assert_eq!(keys(&resp), expected_keys());
}
