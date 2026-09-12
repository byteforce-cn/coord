// 服务端 KV 路由验收测试
//
// 在 RegionManager 装配的 2 节点 × 2 Region 集群上，把每节点的 CoordNode 挂上
// RegionManager（region 1 = ["", "m")、region 2 = ["m", ∞)），直接调用
// Kv/Txn gRPC handler（async_trait 方法）验证：
//   - Put/Range/Delete/Txn 按 key 前缀路由到正确 Region 的 Raft/MVCC；
//     各 Region 独立读写并复制收敛到两节点对应 Region 存储（目录隔离）；
//   - 写非 leader 节点 → RegionNotLeader（UNAVAILABLE）+ `coord-leader-hint`
//     gRPC metadata 指向该 Region 真实 leader 的 KV 地址（forward 语义）；
//   - 跨 Region 的 Range / Delete-range / Txn → INVALID_ARGUMENT
//     （v1 明确不支持跨 Region 范围/事务，宁可拒绝不可静默错答）。
//
// 说明：raft gRPC 为真实网络（节点间复制/选举必需）；KV handler 以直接调用
// 驱动（与单 Raft CoordNode 测试相同的 gRPC 语义层之上，路由逻辑为本测试对象）。

use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::Duration;

use coord_core::storage::StorageBackend;
use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionId, RegionMeta, StorageConfig};
use coord_proto::kv::kv_server::Kv;
use coord_proto::kv::{DeleteRequest, PutRequest, RangeRequest};
use coord_proto::txn::txn_server::Txn;
use coord_proto::txn::TxnRequest;
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcServer, RaftRpcService};
use coord_server::raft::region::RegionManager;
use coord_server::raft::region_runtime::region_data_dir;
use coord_server::raft::{RaftConfig, RegionRuntimeSpec, WatchReceiver};
use coord_server::server::CoordNode;
use coord_server::storage::mvcc::MvccStorage;
use coord_server::storage::redb_backend::RedbBackend;

const LEADER_HINT_KEY: &str = "coord-leader-hint";

fn find_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// 构造 Region 元数据（voter peers = 全部节点，raft_addr 取自共享地址表）
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

struct NodeHost {
    node_id: u64,
    /// CoordNode（region_manager 已装配；KV handler 直接调用）
    node: Arc<CoordNode>,
    manager: Arc<RegionManager>,
    /// KV gRPC 地址（注册进各 node 的 node_grpc_addrs，供 leader hint；不真实 serve）
    kv_grpc_addr: String,
    // 保持临时数据目录存活
    _dir: tempfile::TempDir,
    _factory: RaftNetworkFactoryImpl,
    _raft_handle: tokio::task::JoinHandle<()>,
}

/// 启动 2 节点 × 2 Region 集群，每节点 CoordNode 挂 region_manager。
///
/// region 1 覆盖 ["", "m")，region 2 覆盖 ["m", ∞)。node 1 是 bootstrap。
async fn start_two_node_two_region_kv_cluster() -> Vec<NodeHost> {
    let raft_addrs: BTreeMap<u64, String> = (1..=2)
        .map(|id| (id, format!("127.0.0.1:{}", find_port())))
        .collect();
    let kv_addrs: BTreeMap<u64, String> = (1..=2)
        .map(|id| (id, format!("127.0.0.1:{}", find_port())))
        .collect();
    let regions: [(RegionId, &[u8], &[u8]); 2] = [(1, b"", b"m"), (2, b"m", b"")];

    let mut hosts = Vec::new();
    for node_id in 1..=2u64 {
        let tmpdir = tempfile::tempdir().unwrap();
        let base = tmpdir.path().to_path_buf();

        let factory = RaftNetworkFactoryImpl::new(node_id);
        for (id, addr) in &raft_addrs {
            factory.register_node(*id, addr.clone());
        }
        let rpc = RaftRpcService::new();
        let manager = Arc::new(RegionManager::new(node_id));

        for (region_id, start, end) in regions {
            let spec = RegionRuntimeSpec {
                meta: region_meta(region_id, start, end, &raft_addrs),
                data_dir: region_data_dir(&base, region_id),
                raft_config: Arc::new(RaftConfig {
                    heartbeat_interval: 200,
                    election_timeout_min: 800,
                    election_timeout_max: 1500,
                    ..Default::default()
                }),
                object_store: None,
            };
            manager
                .spawn_region(&factory, &rpc, spec, node_id == 1)
                .await
                .expect("spawn region");
        }

        // CoordNode 的 node 级 legacy storage（region 模式下 KV 走 manager 路由，
        // 该 scratch 仅满足构造签名；region≥1 数据在 base/regions/ 下互不干扰）
        let storage_config = StorageConfig::default();
        let backend = RedbBackend::open(&base, &storage_config).expect("open scratch backend");
        let scratch_mvcc = Arc::new(MvccStorage::new(backend).expect("create scratch mvcc"));
        let mut node = CoordNode::new(scratch_mvcc);
        node.node_id = node_id;
        node.region_manager = Some(Arc::clone(&manager));
        for (id, addr) in &kv_addrs {
            node.register_grpc_addr(*id, addr);
        }
        let node = Arc::new(node);

        // 每节点一个 raft gRPC server，承载全部 Region（按 region_id 解复用）
        let raft_addr: SocketAddr = raft_addrs[&node_id].parse().expect("parse raft addr");
        let raft_svc = RaftRpcServer::new(rpc);
        let raft_handle = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(raft_svc)
                .serve(raft_addr)
                .await;
        });

        hosts.push(NodeHost {
            node_id,
            node,
            manager,
            kv_grpc_addr: kv_addrs[&node_id].clone(),
            _dir: tmpdir,
            _factory: factory,
            _raft_handle: raft_handle,
        });
    }
    hosts
}

/// 等待某 Region 选出 leader（leader lease 就绪，可接受 client_write）。
async fn wait_leader_for_region(
    hosts: &[NodeHost],
    region_id: RegionId,
    timeout: Duration,
) -> Option<usize> {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        for (host_idx, host) in hosts.iter().enumerate() {
            if let Some(rt) = host.manager.runtime(region_id) {
                let m = rt.raft.metrics();
                let m = m.borrow_watched();
                if m.last_quorum_acked.is_some() && m.current_leader == Some(host.node_id) {
                    return Some(host_idx);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}

// ──── KV/Txn 请求构造 ────

fn put_req(key: &[u8], value: &[u8]) -> PutRequest {
    PutRequest {
        key: key.to_vec(),
        value: value.to_vec(),
        lease_id: 0,
        prev_kv: false,
        request_id: vec![],
    }
}

fn range_req(key: &[u8], range_end: &[u8]) -> RangeRequest {
    RangeRequest {
        key: key.to_vec(),
        range_end: range_end.to_vec(),
        limit: 0,
        revision: 0,
        keys_only: false,
        count_only: false,
    }
}

fn delete_req(key: &[u8], range_end: &[u8]) -> DeleteRequest {
    DeleteRequest {
        key: key.to_vec(),
        range_end: range_end.to_vec(),
        prev_kv: false,
        request_id: vec![],
    }
}

fn range_value(
    resp: &tonic::Response<coord_proto::kv::RangeResponse>,
    key: &[u8],
) -> Option<Vec<u8>> {
    resp.get_ref()
        .kvs
        .iter()
        .find(|kv| kv.key == key)
        .map(|kv| kv.value.clone())
}

/// 验收 1：Put/Range/Delete/Txn 按 key 路由到正确 Region；各 Region 独立且收敛。
#[tokio::test]
async fn test_kv_ops_route_to_correct_region_and_converge() {
    let hosts = start_two_node_two_region_kv_cluster().await;

    let l1 = wait_leader_for_region(&hosts, 1, Duration::from_secs(25))
        .await
        .expect("region 1 leader");
    let l2 = wait_leader_for_region(&hosts, 2, Duration::from_secs(25))
        .await
        .expect("region 2 leader");
    eprintln!(
        "region1 leader=node{}, region2 leader=node{}",
        hosts[l1].node_id, hosts[l2].node_id
    );

    // ── Put 路由：apple(region1) 走 l1、peach(region2) 走 l2 ──
    let resp = hosts[l1]
        .node
        .put(tonic::Request::new(put_req(b"apple", b"v1")))
        .await
        .expect("put apple via region1 leader");
    let rev_apple = resp.get_ref().revision;
    assert!(rev_apple >= 1, "region1 put revision");
    hosts[l2]
        .node
        .put(tonic::Request::new(put_req(b"peach", b"v2")))
        .await
        .expect("put peach via region2 leader");

    // ── 收敛 + 隔离：每节点每 Region 的 MVCC 只见本 Region 的 key ──
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let mut converged = true;
        for host in &hosts {
            for (rid, own, other) in [(1u64, b"apple", b"peach"), (2u64, b"peach", b"apple")] {
                let rt = host.manager.runtime(rid).expect("runtime");
                if rt.mvcc.get(own).unwrap()
                    != Some(if rid == 1 {
                        b"v1".to_vec()
                    } else {
                        b"v2".to_vec()
                    })
                {
                    converged = false;
                    continue;
                }
                assert_eq!(
                    rt.mvcc.get(other).unwrap(),
                    None,
                    "node {} region {rid} must not see other region's key",
                    host.node_id
                );
            }
        }
        if converged {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "regions did not converge"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // ── Range 路由：单点读走各自 Region leader ──
    let r = hosts[l1]
        .node
        .range(tonic::Request::new(range_req(b"apple", b"")))
        .await
        .expect("range apple via region1 leader");
    assert_eq!(range_value(&r, b"apple").as_deref(), Some(b"v1".as_slice()));
    assert!(r.get_ref().revision >= rev_apple);

    let r = hosts[l2]
        .node
        .range(tonic::Request::new(range_req(b"peach", b"")))
        .await
        .expect("range peach via region2 leader");
    assert_eq!(range_value(&r, b"peach").as_deref(), Some(b"v2".as_slice()));

    // ── 同 Region 有界范围读 [apple, m) 只含 region1 的 key ──
    let r = hosts[l1]
        .node
        .range(tonic::Request::new(range_req(b"apple", b"m")))
        .await
        .expect("bounded range within region1");
    let keys: Vec<Vec<u8>> = r.get_ref().kvs.iter().map(|kv| kv.key.clone()).collect();
    assert_eq!(
        keys,
        vec![b"apple".to_vec()],
        "bounded range only region1 keys"
    );

    // ── 单键 Delete 路由（region1）──
    let d = hosts[l1]
        .node
        .delete(tonic::Request::new(delete_req(b"apple", b"")))
        .await
        .expect("delete apple via region1 leader");
    assert_eq!(d.get_ref().deleted, 1);
    // 复制到另一节点 region1；region2 的 peach 不受影响
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let mut ok = true;
        for host in &hosts {
            if host
                .manager
                .runtime(1)
                .unwrap()
                .mvcc
                .get(b"apple")
                .unwrap()
                .is_some()
            {
                ok = false;
            }
        }
        if ok {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "delete did not converge"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // ── 同 Region Txn 路由：无条件成功分支写 region2 ──
    let txn_req = TxnRequest {
        compare: vec![],
        success: vec![coord_proto::txn::RequestOp {
            op: Some(coord_proto::txn::request_op::Op::RequestPut(put_req(
                b"peach", b"v2b",
            ))),
        }],
        failure: vec![],
        request_id: vec![],
    };
    let tr = hosts[l2]
        .node
        .txn(tonic::Request::new(txn_req))
        .await
        .expect("txn put peach (region2) via region2 leader");
    assert!(tr.get_ref().succeeded);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let mut ok = true;
        for host in &hosts {
            if host.manager.runtime(2).unwrap().mvcc.get(b"peach").unwrap() != Some(b"v2b".to_vec())
            {
                ok = false;
            }
        }
        if ok {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "txn did not converge"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// 验收 2：写非 leader 节点 → RegionNotLeader（UNAVAILABLE）+ leader hint。
#[tokio::test]
async fn test_write_via_non_leader_returns_region_not_leader_with_hint() {
    let hosts = start_two_node_two_region_kv_cluster().await;

    let l1 = wait_leader_for_region(&hosts, 1, Duration::from_secs(25))
        .await
        .expect("region 1 leader");
    let follower_idx = hosts
        .iter()
        .position(|h| h.node_id != hosts[l1].node_id)
        .expect("follower node");

    // follower 上写 region1 的 key：必须 UNAVAILABLE + hint = region1 leader 的 KV 地址。
    // （选举刚结束的窄窗口内 leader_id 可能尚为 None → 重试几次）
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let res = hosts[follower_idx]
            .node
            .put(tonic::Request::new(put_req(b"apple", b"x")))
            .await;
        match res {
            Ok(_) => panic!("write on non-leader must not succeed"),
            Err(status) => {
                assert_eq!(
                    status.code(),
                    tonic::Code::Unavailable,
                    "region not-leader write must be UNAVAILABLE: {status}"
                );
                let hint = status
                    .metadata()
                    .get(LEADER_HINT_KEY)
                    .and_then(|v| v.to_str().ok())
                    .expect("coord-leader-hint metadata present");
                if hint == hosts[l1].kv_grpc_addr {
                    return;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "leader hint never matches"
                );
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

/// 验收 3：跨 Region 的 Range / Delete-range / Txn 明确拒绝（INVALID_ARGUMENT）。
#[tokio::test]
async fn test_cross_region_operations_rejected() {
    let hosts = start_two_node_two_region_kv_cluster().await;
    let l1 = wait_leader_for_region(&hosts, 1, Duration::from_secs(25))
        .await
        .expect("region 1 leader");

    // region1 = ["", "m")：range [apple, z) 越过 end_key "m" → 拒绝。
    // （range_end 为空 = 单点读语义，无法表达无界跨区扫描；跨区只经显式 end 表达）
    let err = hosts[l1]
        .node
        .range(tonic::Request::new(range_req(b"apple", b"z")))
        .await
        .expect_err("cross-region range must be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument, "range: {err}");

    // delete-range [apple, z) 越界 → 拒绝
    let err = hosts[l1]
        .node
        .delete(tonic::Request::new(delete_req(b"apple", b"z")))
        .await
        .expect_err("cross-region delete-range must be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument, "delete: {err}");

    // txn 同时引用 region1(apple) 与 region2(peach) 的 key → 拒绝
    let cross_txn = TxnRequest {
        compare: vec![],
        success: vec![
            coord_proto::txn::RequestOp {
                op: Some(coord_proto::txn::request_op::Op::RequestPut(put_req(
                    b"apple", b"a2",
                ))),
            },
            coord_proto::txn::RequestOp {
                op: Some(coord_proto::txn::request_op::Op::RequestPut(put_req(
                    b"peach", b"p2",
                ))),
            },
        ],
        failure: vec![],
        request_id: vec![],
    };
    let err = hosts[l1]
        .node
        .txn(tonic::Request::new(cross_txn))
        .await
        .expect_err("cross-region txn must be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument, "txn: {err}");
}
