//! KV-T G6（#173）v1 数据面冻结验收：
//!
//! `[kv_t].enabled=true` 的集群只服务 `coord.<domain>.v2`；v1 KV/Txn/Lease/Watch
//! 的每个 RPC 在 handler 入口返回 `FAILED_PRECONDITION`（策略拒绝，不是
//! `UNIMPLEMENTED`/`UNAVAILABLE`），结构化 trailer 为
//! `x-coord-error-code: FAILED_PRECONDITION`；`[kv_t].enabled=false` 的 v1 行为不变。

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use coord_core::error_code::error_code_of;
use coord_core::storage::StorageBackend;
use coord_core::types::StorageConfig;
use coord_proto::kv::kv_client::KvClient;
use coord_proto::kv::kv_server::KvServer;
use coord_proto::kv::{DeleteRequest, PutRequest, RangeRequest};
use coord_proto::lease::lease_client::LeaseClient;
use coord_proto::lease::lease_server::LeaseServer;
use coord_proto::lease::{LeaseGrantRequest, LeaseKeepAliveRequest, LeaseRevokeRequest};
use coord_proto::storage::storage_client::StorageClient;
use coord_proto::storage::storage_server::StorageServer;
use coord_proto::storage::StatRequest;
use coord_proto::txn::txn_client::TxnClient;
use coord_proto::txn::txn_server::TxnServer;
use coord_proto::txn::TxnRequest;
use coord_proto::watch::watch_client::WatchClient;
use coord_proto::watch::watch_server::WatchServer;
use coord_proto::watch::{WatchCreateRequest, WatchRequest};
use coord_server::kvt::{KvTRuntimeConfig, KvTState};
use coord_server::server::CoordNode;
use coord_server::storage::mvcc::MvccStorage;
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::watch::WatchDispatcher;
use tokio::net::TcpListener;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::Server;
use tonic::Code;

#[path = "support/kv_t_storage_node.rs"]
mod storage_node;

use storage_node::StorageNode;

/// 稳定诊断锚点（runbook / 客户端排查依赖逐字匹配）。
const FREEZE: &str = "v1 data plane is frozen on a v2 cluster";

async fn start_server(kv_t_enabled: bool) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let tmpdir = tempfile::tempdir().unwrap();
    let data_dir = tmpdir.path().to_path_buf();
    let backend = RedbBackend::open(&data_dir, &StorageConfig::default()).unwrap();
    let mvcc = Arc::new(MvccStorage::new(backend).unwrap());

    let mut node = CoordNode::new(Arc::clone(&mvcc));
    node.watch_dispatcher = Some(Arc::new(WatchDispatcher::start()));
    node.set_kvt_state(KvTState::new(kv_t_enabled, KvTRuntimeConfig::default()));
    let node = Arc::new(node);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(KvServer::from_arc(Arc::clone(&node)))
            .add_service(TxnServer::from_arc(Arc::clone(&node)))
            .add_service(LeaseServer::from_arc(Arc::clone(&node)))
            .add_service(WatchServer::from_arc(Arc::clone(&node)))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(150)).await;
    (addr, handle)
}

fn assert_frozen(status: &tonic::Status) {
    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert!(
        status.message().contains(FREEZE),
        "v1 冻结必须给出稳定文案：{:?}",
        status.message()
    );
    assert_eq!(
        error_code_of(status).as_deref(),
        Some("FAILED_PRECONDITION"),
        "结构化 trailer 必须存活到线上"
    );
}

#[tokio::test]
async fn test_v2_cluster_rejects_v1_kv() {
    let (addr, handle) = start_server(true).await;
    let mut kv = KvClient::connect(format!("http://{addr}")).await.unwrap();

    assert_frozen(
        &kv.put(PutRequest {
            key: b"frozen/key".to_vec(),
            value: b"v".to_vec(),
            ..Default::default()
        })
        .await
        .expect_err("v1 Put must be frozen"),
    );
    assert_frozen(
        &kv.range(RangeRequest {
            key: b"frozen/key".to_vec(),
            range_end: b"frozen/key\x00".to_vec(),
            ..Default::default()
        })
        .await
        .expect_err("v1 Range must be frozen"),
    );
    assert_frozen(
        &kv.delete(DeleteRequest {
            key: b"frozen/key".to_vec(),
            ..Default::default()
        })
        .await
        .expect_err("v1 Delete must be frozen"),
    );

    handle.abort();
}

#[tokio::test]
async fn test_v2_cluster_rejects_v1_txn_lease_watch() {
    let (addr, handle) = start_server(true).await;

    let mut txn = TxnClient::connect(format!("http://{addr}")).await.unwrap();
    assert_frozen(
        &txn.txn(TxnRequest::default())
            .await
            .expect_err("v1 Txn must be frozen"),
    );

    let mut lease = LeaseClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    assert_frozen(
        &lease
            .lease_grant(LeaseGrantRequest { ttl: 30, id: 0 })
            .await
            .expect_err("v1 LeaseGrant must be frozen"),
    );
    assert_frozen(
        &lease
            .lease_revoke(LeaseRevokeRequest { id: 1 })
            .await
            .expect_err("v1 LeaseRevoke must be frozen"),
    );
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    tx.send(LeaseKeepAliveRequest { id: 1 }).await.unwrap();
    drop(tx);
    assert_frozen(
        &lease
            .lease_keep_alive(ReceiverStream::new(rx))
            .await
            .expect_err("v1 LeaseKeepAlive must be frozen"),
    );

    let mut watch = WatchClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    tx.send(WatchRequest {
        request: Some(coord_proto::watch::watch_request::Request::Create(
            WatchCreateRequest {
                key: b"frozen/".to_vec(),
                range_end: b"frozen0".to_vec(),
                ..Default::default()
            },
        )),
    })
    .await
    .unwrap();
    drop(tx);
    assert_frozen(
        &watch
            .watch(ReceiverStream::new(rx))
            .await
            .expect_err("v1 Watch must be frozen before the first frame"),
    );

    handle.abort();
}

#[tokio::test]
async fn test_v2_cluster_rejects_v1_storage() {
    let storage = StorageNode::start().await;
    let node = Arc::clone(&storage.node);
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(StorageServer::from_arc(node))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(150)).await;

    let mut client = StorageClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let status = client
        .stat(StatRequest {
            bucket: "frozen".to_string(),
            object_id: b"missing".to_vec(),
        })
        .await
        .expect_err("v1 Storage must be frozen");
    assert_frozen(&status);
    // gate 必须先于对象查找：不能退化成 NOT_FOUND。
    assert_ne!(status.code(), Code::NotFound);

    handle.abort();
}

#[tokio::test]
async fn test_v1_cluster_still_serves_v1() {
    let (addr, handle) = start_server(false).await;
    let mut kv = KvClient::connect(format!("http://{addr}")).await.unwrap();

    kv.put(PutRequest {
        key: b"v1/key".to_vec(),
        value: b"v".to_vec(),
        ..Default::default()
    })
    .await
    .expect("v1 Put must keep working when [kv_t] is disabled");
    let resp = kv
        .range(RangeRequest {
            key: b"v1/key".to_vec(),
            range_end: b"v1/key\x00".to_vec(),
            ..Default::default()
        })
        .await
        .expect("v1 Range must keep working when [kv_t] is disabled")
        .into_inner();
    assert_eq!(resp.kvs.len(), 1);
    assert_eq!(resp.kvs[0].value, b"v");

    handle.abort();
}
