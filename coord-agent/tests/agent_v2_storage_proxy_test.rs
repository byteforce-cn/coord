//! Agent 的 `coord.storage.v2` 代理端到端测试。
//!
//! 假 `coord.storage.v2` 服务记录调用，验证代理**真的转发**（`total_size`/`read_ts`
//! 原样到上游、`commit_ts` 与 `deleted` 原样回调用方），并钉住 v2 与 v1 的口径差异：
//! - 声明长度与实际字节不符在**代理层**拒绝（不消耗上游 `commit_ts`）；
//! - `Stat` 的「不存在」是 `exists=false` 的零 stat，不是 `NOT_FOUND`。

use std::sync::Arc;

use coord_agent::cache::AgentCache;
use coord_agent::{AgentInner, StorageV2Proxy};
use coord_client::Client;
use coord_core::error_code::error_code_of;
use coord_proto::storage::v2 as storage_v2;
use coord_proto::storage::v2::storage_client::StorageClient;
use coord_proto::storage::v2::storage_server::{Storage as StorageV2, StorageServer};
use parking_lot::Mutex;
use tokio::net::TcpListener;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};

// ──── 假 coord.storage.v2 ────

#[derive(Default)]
struct StorageCalls {
    puts: Mutex<Vec<(String, Vec<u8>, Vec<u8>, i64)>>,
    get_read_ts: Mutex<Vec<i64>>,
    stat_read_ts: Mutex<Vec<i64>>,
    deletes: Mutex<Vec<(String, Vec<u8>, Vec<u8>)>>,
}

struct FakeStorage {
    calls: Arc<StorageCalls>,
    object: Vec<u8>,
}

impl FakeStorage {
    fn new(object: &[u8]) -> (Self, Arc<StorageCalls>) {
        let calls = Arc::new(StorageCalls::default());
        (
            Self {
                calls: Arc::clone(&calls),
                object: object.to_vec(),
            },
            calls,
        )
    }
}

fn stat_of(bucket: &str, object_id: &[u8], size: i64, commit_ts: i64) -> storage_v2::ObjectStat {
    storage_v2::ObjectStat {
        bucket: bucket.to_string(),
        object_id: object_id.to_vec(),
        size,
        chunks: if size > 0 { 1 } else { 0 },
        commit_ts,
        create_ts: if size > 0 { 70 } else { 0 },
        exists: size > 0,
        committed: size > 0,
    }
}

#[tonic::async_trait]
impl StorageV2 for FakeStorage {
    async fn put(
        &self,
        request: tonic::Request<tonic::Streaming<storage_v2::PutRequest>>,
    ) -> Result<tonic::Response<storage_v2::PutResponse>, tonic::Status> {
        let mut stream = request.into_inner();
        let meta = match stream.message().await? {
            Some(msg) => match msg.part {
                Some(storage_v2::put_request::Part::Meta(meta)) => meta,
                _ => {
                    return Err(tonic::Status::invalid_argument(
                        "first PutRequest must carry meta",
                    ))
                }
            },
            None => return Err(tonic::Status::invalid_argument("empty Put stream")),
        };
        let mut data = Vec::new();
        while let Some(msg) = stream.message().await? {
            match msg.part {
                Some(storage_v2::put_request::Part::Chunk(chunk)) => data.extend_from_slice(&chunk),
                _ => {
                    return Err(tonic::Status::invalid_argument(
                        "meta must only appear as the first message",
                    ))
                }
            }
        }
        self.calls
            .puts
            .lock()
            .push((meta.bucket, meta.object_id, data.clone(), meta.total_size));
        Ok(tonic::Response::new(storage_v2::PutResponse {
            commit_ts: 77,
            size: data.len() as i64,
            chunks: 1,
        }))
    }

    type GetStream = ReceiverStream<Result<storage_v2::GetResponse, tonic::Status>>;

    async fn get(
        &self,
        request: tonic::Request<storage_v2::GetRequest>,
    ) -> Result<tonic::Response<Self::GetStream>, tonic::Status> {
        let req = request.into_inner();
        self.calls.get_read_ts.lock().push(req.read_ts);
        let data = self.object.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        tokio::spawn(async move {
            let stat = stat_of(&req.bucket, &req.object_id, data.len() as i64, 77);
            if tx
                .send(Ok(storage_v2::GetResponse {
                    part: Some(storage_v2::get_response::Part::Stat(stat)),
                }))
                .await
                .is_err()
            {
                return;
            }
            // 故意拆成两段，断言代理按流拼接而不是只转第一段。
            for chunk in data.chunks(2) {
                if tx
                    .send(Ok(storage_v2::GetResponse {
                        part: Some(storage_v2::get_response::Part::Chunk(chunk.to_vec())),
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }

    async fn stat(
        &self,
        request: tonic::Request<storage_v2::StatRequest>,
    ) -> Result<tonic::Response<storage_v2::StatResponse>, tonic::Status> {
        let req = request.into_inner();
        self.calls.stat_read_ts.lock().push(req.read_ts);
        let missing = req.object_id.as_slice() == b"missing";
        let stat = if missing {
            stat_of(&req.bucket, &req.object_id, 0, 0)
        } else {
            stat_of(&req.bucket, &req.object_id, self.object.len() as i64, 77)
        };
        Ok(tonic::Response::new(storage_v2::StatResponse {
            stat: Some(stat),
        }))
    }

    async fn delete(
        &self,
        request: tonic::Request<storage_v2::DeleteRequest>,
    ) -> Result<tonic::Response<storage_v2::DeleteResponse>, tonic::Status> {
        let req = request.into_inner();
        self.calls
            .deletes
            .lock()
            .push((req.bucket, req.object_id, req.request_id));
        Ok(tonic::Response::new(storage_v2::DeleteResponse {
            deleted: true,
            commit_ts: 88,
        }))
    }
}

// ──── 装配 ────

struct FakeServer {
    addr: String,
    handle: tokio::task::JoinHandle<()>,
    shutdown: tokio::sync::oneshot::Sender<()>,
}

impl FakeServer {
    async fn shutdown(self) {
        let _ = self.shutdown.send(());
        let _ = self.handle.await;
    }
}

async fn serve_upstream(object: &[u8]) -> (FakeServer, Arc<StorageCalls>) {
    let (fake, calls) = FakeStorage::new(object);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(StorageServer::new(fake))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });
    (
        FakeServer {
            addr,
            handle,
            shutdown: shutdown_tx,
        },
        calls,
    )
}

async fn agent_inner_via(addr: &str) -> Arc<AgentInner> {
    let client = Client::connect_via_agent(addr)
        .await
        .expect("connect agent-style client");
    Arc::new(AgentInner {
        client,
        cache: AgentCache::new(0, 0, 0, 0),
    })
}

/// 把 `StorageV2Proxy` 挂到真实 gRPC 端点上，让测试走生产同一条解码/流式路径。
async fn serve_proxy(inner: Option<Arc<AgentInner>>) -> FakeServer {
    let proxy = StorageV2Proxy::new(inner);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(StorageServer::new(proxy))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });
    FakeServer {
        addr,
        handle,
        shutdown: shutdown_tx,
    }
}

async fn connect(addr: &str) -> StorageClient<tonic::transport::Channel> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .expect("connect proxy");
    StorageClient::new(channel)
}

fn put_stream(
    bucket: &str,
    object_id: &[u8],
    declared: i64,
    chunks: &[&[u8]],
) -> Vec<storage_v2::PutRequest> {
    let mut msgs = vec![storage_v2::PutRequest {
        part: Some(storage_v2::put_request::Part::Meta(storage_v2::PutMeta {
            bucket: bucket.to_string(),
            object_id: object_id.to_vec(),
            total_size: declared,
        })),
    }];
    for chunk in chunks {
        msgs.push(storage_v2::PutRequest {
            part: Some(storage_v2::put_request::Part::Chunk(chunk.to_vec())),
        });
    }
    msgs
}

// ──── 用例 ────

#[tokio::test(flavor = "multi_thread")]
async fn proxy_forwards_storage_v2_and_preserves_commit_ts() {
    let (upstream, calls) = serve_upstream(b"hello").await;
    let inner = agent_inner_via(&upstream.addr).await;
    let proxy = serve_proxy(Some(inner)).await;
    let mut client = connect(&proxy.addr).await;

    let put = client
        .put(tokio_stream::iter(put_stream(
            "bucket",
            b"obj",
            5,
            &[b"he", b"llo"],
        )))
        .await
        .expect("Put through proxy")
        .into_inner();
    assert_eq!(put.commit_ts, 77);
    assert_eq!((put.size, put.chunks), (5, 1));
    assert_eq!(
        calls.puts.lock().as_slice(),
        &[("bucket".to_string(), b"obj".to_vec(), b"hello".to_vec(), 5)]
    );

    let mut stream = client
        .get(storage_v2::GetRequest {
            bucket: "bucket".to_string(),
            object_id: b"obj".to_vec(),
            read_ts: 9,
        })
        .await
        .expect("Get through proxy")
        .into_inner();
    let mut data = Vec::new();
    let mut stat = None;
    while let Some(msg) = stream.message().await.expect("Get frame") {
        match msg.part {
            Some(storage_v2::get_response::Part::Stat(s)) => stat = Some(s),
            Some(storage_v2::get_response::Part::Chunk(chunk)) => data.extend_from_slice(&chunk),
            None => panic!("empty GetResponse frame"),
        }
    }
    assert_eq!(data, b"hello");
    assert_eq!(stat.expect("Get must lead with stat").commit_ts, 77);
    assert_eq!(calls.get_read_ts.lock().as_slice(), &[9]);

    // v2 的「不存在」是零 stat，代理不能把它改写成 NOT_FOUND。
    let stat = client
        .stat(storage_v2::StatRequest {
            bucket: "bucket".to_string(),
            object_id: b"missing".to_vec(),
            read_ts: 0,
        })
        .await
        .expect("Stat through proxy")
        .into_inner()
        .stat
        .expect("Stat response must carry a stat");
    assert!(!stat.exists);
    assert!(!stat.committed);
    assert_eq!(stat.commit_ts, 0);

    let delete = client
        .delete(storage_v2::DeleteRequest {
            bucket: "bucket".to_string(),
            object_id: b"obj".to_vec(),
            request_id: b"req-1".to_vec(),
        })
        .await
        .expect("Delete through proxy")
        .into_inner();
    assert!(delete.deleted);
    assert_eq!(delete.commit_ts, 88);
    assert_eq!(
        calls.deletes.lock().as_slice(),
        &[("bucket".to_string(), b"obj".to_vec(), b"req-1".to_vec())]
    );

    proxy.shutdown().await;
    upstream.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn proxy_rejects_declared_size_mismatch_before_put() {
    let (upstream, calls) = serve_upstream(b"hello").await;
    let inner = agent_inner_via(&upstream.addr).await;
    let proxy = serve_proxy(Some(inner)).await;
    let mut client = connect(&proxy.addr).await;

    let err = client
        .put(tokio_stream::iter(put_stream(
            "bucket",
            b"obj",
            10,
            &[b"abc"],
        )))
        .await
        .expect_err("declared size mismatch must be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument, "{err:?}");
    assert_eq!(error_code_of(&err).as_deref(), Some("INVALID_ARGUMENT"));
    assert!(
        calls.puts.lock().is_empty(),
        "a mismatched Put must not consume an upstream commit_ts"
    );

    proxy.shutdown().await;
    upstream.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn skeleton_storage_v2_fails_closed() {
    let proxy = serve_proxy(None).await;
    let mut client = connect(&proxy.addr).await;

    let err = client
        .put(tokio_stream::iter(put_stream("bucket", b"obj", 1, &[b"v"])))
        .await
        .expect_err("skeleton Put must fail closed");
    assert_eq!(err.code(), tonic::Code::Unavailable, "{err:?}");
    assert_eq!(error_code_of(&err).as_deref(), Some("UNAVAILABLE"));

    // 服务端流 RPC 的首帧错误可能以 RPC 错误或流首帧错误到达，两者都必须是 UNAVAILABLE。
    match client
        .get(storage_v2::GetRequest {
            bucket: "bucket".to_string(),
            object_id: b"obj".to_vec(),
            read_ts: 0,
        })
        .await
    {
        Ok(response) => {
            let err = response
                .into_inner()
                .message()
                .await
                .expect_err("skeleton Get must fail closed");
            assert_eq!(err.code(), tonic::Code::Unavailable, "{err:?}");
        }
        Err(err) => {
            assert_eq!(err.code(), tonic::Code::Unavailable, "{err:?}");
        }
    }

    let err = client
        .stat(storage_v2::StatRequest {
            bucket: "bucket".to_string(),
            object_id: b"obj".to_vec(),
            read_ts: 0,
        })
        .await
        .expect_err("skeleton Stat must fail closed");
    assert_eq!(err.code(), tonic::Code::Unavailable, "{err:?}");

    let err = client
        .delete(storage_v2::DeleteRequest {
            bucket: "bucket".to_string(),
            object_id: b"obj".to_vec(),
            request_id: b"req".to_vec(),
        })
        .await
        .expect_err("skeleton Delete must fail closed");
    assert_eq!(err.code(), tonic::Code::Unavailable, "{err:?}");

    proxy.shutdown().await;
}
