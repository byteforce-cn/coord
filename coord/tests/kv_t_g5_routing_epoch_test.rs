//! KV-T G5（#161）`coord-routing-epoch` 入口校验：
//!
//! - 全部 v2 数据面服务（KV/Txn/Watch/Lease/Storage）在 handler 入口校验 metadata；
//!   与本地已应用 Region 表 `version` 不一致 → `ABORTED` + `EPOCH_STALE`，
//!   trailing metadata 回传本节点当前版本（ADR-0015 D12.5）；
//! - metadata 缺失不校验（单 Region / 尚未学到版本的客户端）；
//! - 非法值 → `INVALID_ARGUMENT`（不静默忽略）。

use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use coord_core::error_code::error_code_of;
use coord_core::storage::StorageBackend;
use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionMeta, StorageConfig};
use coord_proto::kv::v2::kv_client::KvClient;
use coord_proto::kv::v2::kv_server::KvServer;
use coord_proto::kv::v2::{DeleteRequest, PutRequest, RangeRequest};
use coord_proto::lease::v2::lease_client::LeaseClient;
use coord_proto::lease::v2::lease_server::LeaseServer;
use coord_proto::lease::v2::{
    LeaseGrantRequest, LeaseKeepAliveRequest, LeaseRevokeRequest, LeaseTimeToLiveRequest,
};
use coord_proto::storage::v2::storage_client::StorageClient;
use coord_proto::storage::v2::storage_server::StorageServer;
use coord_proto::storage::v2::{
    DeleteRequest as StorageDeleteRequest, GetRequest as StorageGetRequest, PutMeta,
    PutRequest as StoragePutRequest, StatRequest,
};
use coord_proto::txn::v2::txn_client::TxnClient;
use coord_proto::txn::v2::txn_server::TxnServer;
use coord_proto::txn::v2::{
    AbortRequest, BeginRequest, CommitRequest, ReadRequest as TxnReadRequest,
};
use coord_proto::watch::v2::watch_client::WatchClient;
use coord_proto::watch::v2::watch_server::WatchServer;
use coord_proto::watch::v2::WatchRequest;
use coord_server::kvt::{KvTCapability, KvTRuntimeConfig, KvTState};
use coord_server::raft::region::RegionManager;
use coord_server::server::CoordNode;
use coord_server::storage::mvcc::{MvccStorage, RevisionMode};
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::storage::region_table::RegionTable;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Server};
use tonic::{Code, Request};

const NODE_ID: u64 = 1;
const TABLE_VERSION: u64 = 7;

/// 只在进程内登记路由版本：不装配 raft 运行时——epoch 校验发生在路由之前，
/// 正/负用例都不需要真实数据面。
struct TestServer {
    addr: SocketAddr,
    handle: tokio::task::JoinHandle<()>,
    _dir: TempDir,
    _node: Arc<CoordNode>,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

fn region_table(version: u64) -> RegionTable {
    RegionTable {
        version,
        regions: vec![RegionMeta {
            region_id: 1,
            start_key: vec![],
            end_key: vec![],
            epoch: RegionEpoch::initial(),
            peers: vec![Peer {
                node_id: NODE_ID,
                raft_addr: "127.0.0.1:1".to_string(),
                role: PeerRole::Voter,
            }],
            approximate_size: 0,
            approximate_keys: 0,
        }],
    }
}

async fn start_server() -> TestServer {
    let dir = tempfile::tempdir().expect("temp dir");
    let backend = RedbBackend::open(dir.path(), &StorageConfig::default()).expect("open storage");
    let mvcc = Arc::new(
        MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs).expect("mvcc"),
    );

    let manager = Arc::new(RegionManager::new(NODE_ID));
    manager
        .apply_region_table(&region_table(TABLE_VERSION))
        .expect("seed route table version");
    assert_eq!(manager.table_version(), TABLE_VERSION);

    let mut node = CoordNode::new(Arc::clone(&mvcc));
    node.node_id = NODE_ID;
    node.region_manager = Some(Arc::clone(&manager));
    node.object_limits = Some(Arc::new(
        coord_server::storage::object_store::ObjectLimits::default(),
    ));
    node.set_kvt_state(
        KvTState::new(true, KvTRuntimeConfig::default()).with_implemented([
            KvTCapability::Kv,
            KvTCapability::Txn,
            KvTCapability::Lease,
            KvTCapability::Watch,
            KvTCapability::Storage,
        ]),
    );
    let node = Arc::new(node);

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let serving = Arc::clone(&node);
    let handle = tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(KvServer::from_arc(Arc::clone(&serving)))
            .add_service(TxnServer::from_arc(Arc::clone(&serving)))
            .add_service(LeaseServer::from_arc(Arc::clone(&serving)))
            .add_service(WatchServer::from_arc(Arc::clone(&serving)))
            .add_service(StorageServer::from_arc(Arc::clone(&serving)))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    TestServer {
        addr,
        handle,
        _dir: dir,
        _node: node,
    }
}

/// 带 `coord-routing-epoch` 的请求（十进制 u64）。
fn with_epoch<T>(message: T, epoch: &str) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "coord-routing-epoch",
        MetadataValue::from_str(epoch).expect("ascii metadata"),
    );
    request
}

fn assert_epoch_stale<T: std::fmt::Debug>(result: Result<T, tonic::Status>) {
    let status = result.expect_err("expected EPOCH_STALE");
    assert_eq!(
        status.code(),
        Code::Aborted,
        "unexpected status: {}",
        status.message()
    );
    assert_eq!(
        error_code_of(&status).as_deref(),
        Some("EPOCH_STALE"),
        "structured code"
    );
    let epoch = status
        .metadata()
        .get("coord-routing-epoch")
        .expect("trailer carries the server epoch")
        .to_str()
        .expect("ascii");
    assert_eq!(epoch, TABLE_VERSION.to_string(), "trailer epoch");
}

fn assert_not_epoch_stale<T: std::fmt::Debug>(result: Result<T, tonic::Status>) {
    if let Err(status) = result {
        assert_ne!(
            status.code(),
            Code::Aborted,
            "guard rejected a request it should have passed: {}",
            status.message()
        );
        assert_ne!(error_code_of(&status).as_deref(), Some("EPOCH_STALE"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_routing_epoch_is_rejected_on_every_v2_data_entry() {
    let server = start_server().await;
    let channel = Channel::from_shared(format!("http://{}", server.addr))
        .expect("endpoint")
        .connect()
        .await
        .expect("connect");
    let stale = (TABLE_VERSION - 1).to_string();

    let mut kv = KvClient::new(channel.clone());
    assert_epoch_stale(
        kv.put(with_epoch(
            PutRequest {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                commit_ts: 1,
                ..Default::default()
            },
            &stale,
        ))
        .await,
    );
    assert_epoch_stale(
        kv.range(with_epoch(
            RangeRequest {
                key: b"k".to_vec(),
                ..Default::default()
            },
            &stale,
        ))
        .await,
    );
    assert_epoch_stale(
        kv.delete(with_epoch(
            DeleteRequest {
                key: b"k".to_vec(),
                commit_ts: 1,
                ..Default::default()
            },
            &stale,
        ))
        .await,
    );

    let mut txn = TxnClient::new(channel.clone());
    assert_epoch_stale(txn.begin(with_epoch(BeginRequest::default(), &stale)).await);
    assert_epoch_stale(
        txn.read(with_epoch(TxnReadRequest::default(), &stale))
            .await,
    );
    assert_epoch_stale(
        txn.commit(with_epoch(CommitRequest::default(), &stale))
            .await,
    );
    assert_epoch_stale(txn.abort(with_epoch(AbortRequest::default(), &stale)).await);

    let mut lease = LeaseClient::new(channel.clone());
    assert_epoch_stale(
        lease
            .lease_grant(with_epoch(
                LeaseGrantRequest {
                    ttl_seconds: 30,
                    ..Default::default()
                },
                &stale,
            ))
            .await,
    );
    assert_epoch_stale(
        lease
            .lease_time_to_live(with_epoch(LeaseTimeToLiveRequest::default(), &stale))
            .await,
    );
    assert_epoch_stale(
        lease
            .lease_keep_alive(with_epoch(
                tokio_stream::iter(vec![LeaseKeepAliveRequest { lease_id: 1 }]),
                &stale,
            ))
            .await,
    );
    assert_epoch_stale(
        lease
            .lease_revoke(with_epoch(
                LeaseRevokeRequest {
                    lease_id: 1,
                    request_id: b"revoke".to_vec(),
                },
                &stale,
            ))
            .await,
    );

    let mut storage = StorageClient::new(channel.clone());
    assert_epoch_stale(
        storage
            .stat(with_epoch(
                StatRequest {
                    bucket: "b".to_string(),
                    object_id: b"o".to_vec(),
                    read_ts: 0,
                },
                &stale,
            ))
            .await,
    );
    assert_epoch_stale(
        storage
            .delete(with_epoch(
                StorageDeleteRequest {
                    bucket: "b".to_string(),
                    object_id: b"o".to_vec(),
                    request_id: b"delete".to_vec(),
                },
                &stale,
            ))
            .await,
    );
    assert_epoch_stale(
        storage
            .get(with_epoch(
                StorageGetRequest {
                    bucket: "b".to_string(),
                    object_id: b"o".to_vec(),
                    read_ts: 0,
                },
                &stale,
            ))
            .await
            .map(|response| response.into_inner()),
    );
    assert_epoch_stale(
        storage
            .put(with_epoch(
                tokio_stream::iter(vec![StoragePutRequest {
                    part: Some(coord_proto::storage::v2::put_request::Part::Meta(PutMeta {
                        bucket: "b".to_string(),
                        object_id: b"o".to_vec(),
                        total_size: 1,
                    })),
                }]),
                &stale,
            ))
            .await,
    );

    let mut watch = WatchClient::new(channel);
    assert_epoch_stale(
        watch
            .watch(with_epoch(
                tokio_stream::iter(vec![WatchRequest::default()]),
                &stale,
            ))
            .await,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_or_matching_epoch_passes_the_guard() {
    let server = start_server().await;
    let channel = Channel::from_shared(format!("http://{}", server.addr))
        .expect("endpoint")
        .connect()
        .await
        .expect("connect");
    let mut kv = KvClient::new(channel);

    let range = || RangeRequest {
        key: b"k".to_vec(),
        ..Default::default()
    };

    // 缺失 metadata：不校验（随后按路由状态失败，而不是 EPOCH_STALE）。
    assert_not_epoch_stale(kv.range(Request::new(range())).await);
    // 与本地已应用版本一致：放行。
    assert_not_epoch_stale(
        kv.range(with_epoch(range(), &TABLE_VERSION.to_string()))
            .await,
    );
    // 非法值：fail-closed 的 INVALID_ARGUMENT，不静默忽略。
    let status = kv
        .range(with_epoch(range(), "not-a-number"))
        .await
        .expect_err("invalid epoch must be rejected");
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(error_code_of(&status).as_deref(), Some("INVALID_ARGUMENT"));
}
