// T5.6（R-MR-03）Watch per-Region 路由验收测试
//
// 在 RegionManager 装配的 2 节点 × 2 Region 集群上（region 1 = ["", "m")、
// region 2 = ["m", ∞)），每节点挂 CoordNode + region_manager，经真实 gRPC
// Watch/KV 服务验证：
//   - region ≥1 订阅可收到本 Region 的变更事件（apply 经 per-Region dispatcher
//     分发——RegionRuntime.watch_dispatcher 已挂到 per-Region 状态机）；
//   - 各 Region 独立：订阅 region1 只收 region1 的 key 事件，region2 写入不误报；
//   - 跨 Region 区间/前缀显式拒绝（INVALID_ARGUMENT）：
//       * range [apple, z) 越过 region1 end "m" → 拒绝；
//       * 空 key 前缀（匹配全 keyspace）→ 拒绝；
//       * 单 Region 内前缀/区间 → 放行。
//
// 说明：raft gRPC 为真实网络（复制/选举必需）；Watch/KV 走真实 gRPC 服务
// （Watch 为双向流，需真实 server 端承载）。

use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::Duration;

use coord_core::storage::StorageBackend;
use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionId, RegionMeta, StorageConfig};
use coord_proto::kv::kv_client::KvClient;
use coord_proto::kv::kv_server::KvServer;
use coord_proto::kv::PutRequest;
use coord_proto::watch::watch_client::WatchClient;
use coord_proto::watch::watch_server::WatchServer;
use coord_proto::watch::{WatchCreateRequest, WatchRequest};
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcServer, RaftRpcService};
use coord_server::raft::region::RegionManager;
use coord_server::raft::region_runtime::region_data_dir;
use coord_server::raft::{RaftConfig, RegionRuntimeSpec, WatchReceiver};
use coord_server::server::CoordNode;
use coord_server::storage::mvcc::MvccStorage;
use coord_server::storage::redb_backend::RedbBackend;

fn find_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
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

struct NodeHost {
    node_id: u64,
    manager: Arc<RegionManager>,
    kv_addr: String,
    _dir: tempfile::TempDir,
    _factory: RaftNetworkFactoryImpl,
    _raft_handle: tokio::task::JoinHandle<()>,
    _grpc_handle: tokio::task::JoinHandle<()>,
}

async fn start_two_node_two_region_watch_cluster() -> Vec<NodeHost> {
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
            };
            manager
                .spawn_region(&factory, &rpc, spec, node_id == 1)
                .await
                .expect("spawn region");
        }

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

        // raft gRPC（承载全部 Region）
        let raft_addr: SocketAddr = raft_addrs[&node_id].parse().expect("parse raft addr");
        let raft_svc = RaftRpcServer::new(rpc);
        let raft_handle = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(raft_svc)
                .serve(raft_addr)
                .await;
        });

        // KV + Watch gRPC（真实 handler）
        let kv_svc = KvServer::from_arc(Arc::clone(&node));
        let watch_svc = WatchServer::from_arc(Arc::clone(&node));
        let grpc_addr: SocketAddr = kv_addrs[&node_id].parse().expect("parse kv addr");
        let grpc_handle = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(kv_svc)
                .add_service(watch_svc)
                .serve(grpc_addr)
                .await;
        });

        hosts.push(NodeHost {
            node_id,
            manager,
            kv_addr: kv_addrs[&node_id].clone(),
            _dir: tmpdir,
            _factory: factory,
            _raft_handle: raft_handle,
            _grpc_handle: grpc_handle,
        });
    }
    hosts
}

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

async fn connect(addr: &str) -> tonic::transport::Channel {
    tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .expect("connect")
}

/// 打开一条 create watch，返回事件流（`tonic::Streaming`）。
async fn open_watch(
    watch_client: &mut WatchClient<tonic::transport::Channel>,
    key: Vec<u8>,
    range_end: Vec<u8>,
    start_revision: i64,
) -> tonic::Streaming<coord_proto::watch::WatchResponse> {
    let (req_tx, req_rx) = tokio::sync::mpsc::channel::<WatchRequest>(2);
    let stream_in = tokio_stream::wrappers::ReceiverStream::new(req_rx);
    req_tx
        .try_send(WatchRequest {
            request: Some(coord_proto::watch::watch_request::Request::Create(
                WatchCreateRequest {
                    key,
                    range_end,
                    start_revision,
                    prev_kv: false,
                },
            )),
        })
        .expect("send create");
    drop(req_tx); // create-only：handler 只消费首个消息
    let resp = watch_client.watch(tonic::Request::new(stream_in)).await;
    resp.expect("watch open ok").into_inner()
}

/// T5.6 验收 1：region ≥1 订阅可收到本 Region 变更；各 Region 事件独立。
#[tokio::test]
async fn test_region_watch_receives_own_region_events_only() {
    let hosts = start_two_node_two_region_watch_cluster().await;
    let l1 = wait_leader_for_region(&hosts, 1, Duration::from_secs(25))
        .await
        .expect("region 1 leader");
    let l2 = wait_leader_for_region(&hosts, 2, Duration::from_secs(25))
        .await
        .expect("region 2 leader");

    // region1 上开 watch（key "apple"，range_end 空 = 本仓 watch 语义的 key watch）
    let mut wc1 = WatchClient::new(connect(&hosts[l1].kv_addr).await);
    let mut stream1 = open_watch(&mut wc1, b"apple".to_vec(), vec![], 0).await;
    // 等待 Server 侧注册完成
    tokio::time::sleep(Duration::from_millis(500)).await;

    // region2 的写入不应打扰 region1 的 watch
    let mut kv2 = KvClient::new(connect(&hosts[l2].kv_addr).await);
    kv2.put(PutRequest {
        key: b"peach".to_vec(),
        value: b"v2".to_vec(),
        lease_id: 0,
        prev_kv: false,
        request_id: vec![],
    })
    .await
    .expect("put peach (region2)");

    // 短窗口内 region1 watch 不应收到事件
    if let Ok(Ok(Some(resp))) =
        tokio::time::timeout(Duration::from_millis(1200), stream1.message()).await
    {
        let keys: Vec<Vec<u8>> = resp
            .events
            .iter()
            .flat_map(|e| e.kvs.iter().map(|kv| kv.key.clone()))
            .collect();
        assert!(
            keys.iter().all(|k| k != b"peach"),
            "region1 watch must not receive region2 key: {keys:?}"
        );
    }

    // region1 写入 "apple" → region1 watch 收到
    let mut kv1 = KvClient::new(connect(&hosts[l1].kv_addr).await);
    kv1.put(PutRequest {
        key: b"apple".to_vec(),
        value: b"v1".to_vec(),
        lease_id: 0,
        prev_kv: false,
        request_id: vec![],
    })
    .await
    .expect("put apple (region1)");

    let mut saw_apple = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(8), stream1.message()).await {
            Ok(Ok(Some(resp))) => {
                let has_apple = resp
                    .events
                    .iter()
                    .any(|e| e.kvs.iter().any(|kv| kv.key == b"apple"));
                if has_apple {
                    saw_apple = true;
                    break;
                }
            }
            Ok(Ok(None)) => break,
            Ok(Err(e)) => panic!("watch stream error: {e}"),
            Err(_) => break,
        }
    }
    assert!(saw_apple, "region1 watch must receive its own region put");

    // region2 侧开 watch "peach" key，put "peach" 应收到（region ≥1 两条验证）
    let mut wc2 = WatchClient::new(connect(&hosts[l2].kv_addr).await);
    let mut stream2 = open_watch(&mut wc2, b"peach".to_vec(), vec![], 0).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    kv2.put(PutRequest {
        key: b"peach".to_vec(),
        value: b"v2b".to_vec(),
        lease_id: 0,
        prev_kv: false,
        request_id: vec![],
    })
    .await
    .expect("put peach again (region2)");

    let mut saw_peach = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(8), stream2.message()).await {
            Ok(Ok(Some(resp))) => {
                if resp
                    .events
                    .iter()
                    .any(|e| e.kvs.iter().any(|kv| kv.key == b"peach"))
                {
                    saw_peach = true;
                    break;
                }
            }
            Ok(Ok(None)) => break,
            Ok(Err(e)) => panic!("watch stream error: {e}"),
            Err(_) => break,
        }
    }
    assert!(saw_peach, "region2 watch must receive its own region put");
}

/// T5.6 验收 2：跨 Region 区间/前缀 watch 显式拒绝（INVALID_ARGUMENT）。
#[tokio::test]
async fn test_cross_region_watch_rejected() {
    let hosts = start_two_node_two_region_watch_cluster().await;
    let l1 = wait_leader_for_region(&hosts, 1, Duration::from_secs(25))
        .await
        .expect("region 1 leader");

    // 1) range [apple, z)：apple 在 region1，z > region1 end "m" → 拒绝
    let mut wc = WatchClient::new(connect(&hosts[l1].kv_addr).await);
    let (req_tx, req_rx) = tokio::sync::mpsc::channel::<WatchRequest>(2);
    let stream_in = tokio_stream::wrappers::ReceiverStream::new(req_rx);
    req_tx
        .try_send(WatchRequest {
            request: Some(coord_proto::watch::watch_request::Request::Create(
                WatchCreateRequest {
                    key: b"apple".to_vec(),
                    range_end: b"z".to_vec(),
                    start_revision: 0,
                    prev_kv: false,
                },
            )),
        })
        .expect("send create");
    drop(req_tx);
    let err = wc
        .watch(tonic::Request::new(stream_in))
        .await
        .expect_err("cross-region range watch must be rejected");
    assert_eq!(
        err.code(),
        tonic::Code::InvalidArgument,
        "cross-region range watch: {err}"
    );

    // 2) 空 key 前缀（匹配全 keyspace，必然跨区）→ 拒绝
    let mut wc2 = WatchClient::new(connect(&hosts[l1].kv_addr).await);
    let (req_tx, req_rx) = tokio::sync::mpsc::channel::<WatchRequest>(2);
    let stream_in = tokio_stream::wrappers::ReceiverStream::new(req_rx);
    req_tx
        .try_send(WatchRequest {
            request: Some(coord_proto::watch::watch_request::Request::Create(
                WatchCreateRequest {
                    key: vec![],
                    range_end: vec![],
                    start_revision: 0,
                    prev_kv: false,
                },
            )),
        })
        .expect("send create");
    drop(req_tx);
    let err = wc2
        .watch(tonic::Request::new(stream_in))
        .await
        .expect_err("empty prefix (all keyspace) watch must be rejected");
    assert_eq!(
        err.code(),
        tonic::Code::InvalidArgument,
        "empty prefix watch: {err}"
    );

    // 3) region 内单键/前缀仍可打开（region1 内 "apple" 前缀）
    let mut wc3 = WatchClient::new(connect(&hosts[l1].kv_addr).await);
    let (req_tx, req_rx) = tokio::sync::mpsc::channel::<WatchRequest>(2);
    let stream_in = tokio_stream::wrappers::ReceiverStream::new(req_rx);
    req_tx
        .try_send(WatchRequest {
            request: Some(coord_proto::watch::watch_request::Request::Create(
                WatchCreateRequest {
                    key: b"apple".to_vec(),
                    range_end: vec![],
                    start_revision: 0,
                    prev_kv: false,
                },
            )),
        })
        .expect("send create");
    drop(req_tx);
    wc3.watch(tonic::Request::new(stream_in))
        .await
        .expect("intra-region prefix watch must be accepted");
}
