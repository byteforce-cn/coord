//! Agent 的 KV-T v2 数据面代理（TSO/KV/Lease 切片）端到端测试。
//!
//! 这里直接构造 `AgentInner` 指向一个进程内 v2 Server（`coord.kv.v2` +
//! `coord.tso.v2`），验证代理**真的转发**而不是只返回骨架占位；同时验证
//! `COMMIT_REJECTED` 的 wire 码没有被压成 gRPC 状态码兜底（Java SDK 按码分支）。
//!
//! gRPC 服务注册（`coord.kv.v2.KV` 等出现在 Agent 监听端口上）由
//! `agent_grpc_test::test_agent_registers_kvt_v2_services_fail_closed` 覆盖。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use coord_agent::cache::AgentCache;
use coord_agent::{AgentInner, KvV2Proxy, TsoV2Proxy};
use coord_core::error::Result as CoordResult;
use coord_core::error_code::error_code_of;
use coord_core::storage::StorageBackend;
use coord_core::types::StorageConfig;
use coord_proto::kv::v2::kv_server::Kv as KvV2Service;
use coord_proto::kv::v2::kv_server::KvServer as KvV2Server;
use coord_proto::tso::v2::tso_server::Tso as TsoV2Service;
use coord_proto::tso::v2::tso_server::TsoServer;
use coord_server::kvt::{KvTCapability, KvTRuntimeConfig, KvTState};
use coord_server::raft::tso::{TsoBatch, TsoSource};
use coord_server::server::CoordNode;
use coord_server::storage::mvcc::{MvccStorage, RevisionMode};
use coord_server::storage::redb_backend::RedbBackend;
use tokio::net::TcpListener;

struct FakeTso {
    high_water: AtomicU64,
}

impl FakeTso {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            high_water: AtomicU64::new(1),
        })
    }
}

#[async_trait]
impl TsoSource for FakeTso {
    async fn is_leader(&self) -> bool {
        true
    }

    async fn allocate(&self, batch_size: u32) -> CoordResult<TsoBatch> {
        let start = self
            .high_water
            .fetch_add(u64::from(batch_size), Ordering::SeqCst);
        Ok(TsoBatch {
            start_ts: start,
            end_ts: start + u64::from(batch_size),
        })
    }
}

struct TestServer {
    addr: String,
    handle: tokio::task::JoinHandle<()>,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
    _tmpdir: tempfile::TempDir,
}

async fn start_v2_server() -> TestServer {
    let tmpdir = tempfile::tempdir().unwrap();
    let backend = RedbBackend::open(tmpdir.path(), &StorageConfig::default()).unwrap();
    let mvcc =
        Arc::new(MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs).unwrap());

    let mut node = CoordNode::new(Arc::clone(&mvcc));
    node.set_kvt_state(
        KvTState::new(true, KvTRuntimeConfig::default())
            .with_implemented([KvTCapability::Tso, KvTCapability::Kv]),
    );
    node.set_tso_client(coord_server::raft::tso::TsoClient::new(
        FakeTso::new(),
        8,
        None,
    ));
    let node = Arc::new(node);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server_node = Arc::clone(&node);
    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(KvV2Server::from_arc(Arc::clone(&server_node)))
            .add_service(TsoServer::from_arc(server_node))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    let _ = shutdown_rx.await;
                },
            )
            .await
            .unwrap();
    });

    TestServer {
        addr,
        handle,
        shutdown_tx,
        _tmpdir: tmpdir,
    }
}

impl TestServer {
    async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        let _ = self.handle.await;
    }
}

async fn agent_inner(addr: &str) -> Arc<AgentInner> {
    let client = coord_client::Client::connect_via_agent(addr)
        .await
        .expect("connect agent-style client");
    Arc::new(AgentInner {
        client,
        cache: AgentCache::new(0, 0, 0, 0),
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn proxy_forwards_tso_and_kv_and_keeps_commit_rejected_code() {
    let server = start_v2_server().await;
    let inner = agent_inner(&server.addr).await;

    // TSO：拿一个 ts，KV 写入必须用它（服务端不替调用方分配）。
    let ts = TsoV2Proxy::new(Some(Arc::clone(&inner)))
        .get_ts(tonic::Request::new(coord_proto::tso::v2::GetTsRequest {}))
        .await
        .expect("TSO GetTs through proxy")
        .into_inner()
        .ts;
    assert!(ts > 0, "TSO must return a positive ts");

    let kv = KvV2Proxy::new(Some(Arc::clone(&inner)));
    let put = kv
        .put(tonic::Request::new(coord_proto::kv::v2::PutRequest {
            key: b"/agent/v2/k".to_vec(),
            value: b"v".to_vec(),
            commit_ts: ts,
            ..Default::default()
        }))
        .await
        .expect("KV Put through proxy")
        .into_inner();
    assert_eq!(put.commit_ts, ts);

    let range = kv
        .range(tonic::Request::new(coord_proto::kv::v2::RangeRequest {
            key: b"/agent/v2/k".to_vec(),
            ..Default::default()
        }))
        .await
        .expect("KV Range through proxy")
        .into_inner();
    assert_eq!(range.kvs.len(), 1);
    assert_eq!(range.kvs[0].value, b"v");

    // 过期 commit_ts：服务端 COMMIT_REJECTED，代理必须保留码（不能变成 INTERNAL）。
    let err = kv
        .put(tonic::Request::new(coord_proto::kv::v2::PutRequest {
            key: b"/agent/v2/stale".to_vec(),
            value: b"v".to_vec(),
            commit_ts: ts,
            ..Default::default()
        }))
        .await
        .expect_err("stale commit_ts must be rejected");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition, "{err:?}");
    assert_eq!(
        error_code_of(&err).as_deref(),
        Some(coord_core::error_code::CoordErrorCode::CommitRejected.as_str()),
        "{err:?}"
    );

    server.shutdown().await;
}
