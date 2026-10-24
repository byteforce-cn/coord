//! Agent 的 `coord.watch.v2` 代理端到端测试。
//!
//! 判据不是「有转发」：客户端包装隐藏了服务端 `watch_id`，代理必须给每条订阅
//! 分配**本地稳定 id**（跨上游重连不漂移），并让同一客户端流上的多个 create
//! 各自只收到自己的事件。取消同样按本地 id 生效——它关闭对应的上游流，而不是
//! 把整条客户端流打断；未知 id 的取消必须幂等。
//!
//! scope 判定在 handler 首帧完成（见 `proxy_v2::tests`）：鉴权层看不到流 body，
//! 越界订阅必须在**建立任何上游订阅之前**拒绝。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use coord_agent::cache::AgentCache;
use coord_agent::{AgentInner, WatchV2Proxy};
use coord_client::Client;
use coord_core::error_code::error_code_of;
use coord_proto::watch::v2 as watch_v2;
use coord_proto::watch::v2::watch_client::WatchClient as WatchV2Client;
use coord_proto::watch::v2::watch_server::{Watch as WatchV2, WatchServer as WatchV2Server};
use parking_lot::Mutex;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};

// ──── 假 coord.watch.v2 ────

#[derive(Default)]
struct WatchCalls {
    /// `(服务端流内 id, 订阅 key)`；代理的本地 id 与这里无关。
    creates: Mutex<Vec<(i64, Vec<u8>)>>,
    cancels: Mutex<Vec<i64>>,
    closed_streams: AtomicU64,
}

struct FakeWatch {
    calls: Arc<WatchCalls>,
}

impl FakeWatch {
    fn new() -> (Self, Arc<WatchCalls>) {
        let calls = Arc::new(WatchCalls::default());
        (
            Self {
                calls: Arc::clone(&calls),
            },
            calls,
        )
    }
}

fn batch(watch_id: i64, value: &'static [u8]) -> watch_v2::WatchResponse {
    watch_v2::WatchResponse {
        watch_id,
        events: vec![watch_v2::WatchEvent {
            commit_ts: 42,
            changes: vec![watch_v2::Change {
                r#type: watch_v2::change::Type::Put as i32,
                key: b"/app/k".to_vec(),
                value: value.to_vec(),
                prev_kv: None,
            }],
        }],
        cursor: Some(watch_v2::WatchCursor {
            shard_id: 3,
            commit_ts: 42,
        }),
        watermark: 50,
        sealed: true,
    }
}

#[tonic::async_trait]
impl WatchV2 for FakeWatch {
    type WatchStream = ReceiverStream<Result<watch_v2::WatchResponse, tonic::Status>>;

    async fn watch(
        &self,
        request: tonic::Request<tonic::Streaming<watch_v2::WatchRequest>>,
    ) -> Result<tonic::Response<Self::WatchStream>, tonic::Status> {
        let mut input = request.into_inner();
        let calls = Arc::clone(&self.calls);
        let (tx, rx) = mpsc::channel(8);
        tokio::spawn(async move {
            let mut next_id = 1i64;
            while let Ok(Some(frame)) = input.message().await {
                match frame.request {
                    Some(watch_v2::watch_request::Request::Create(create)) => {
                        let id = next_id;
                        next_id += 1;
                        calls.creates.lock().push((id, create.key.clone()));
                        let tx = tx.clone();
                        let calls = Arc::clone(&calls);
                        // 每 10ms 一批，直到代理关掉订阅（远端流断开 → send 失败）。
                        tokio::spawn(async move {
                            loop {
                                if tx.send(Ok(batch(id, b"v"))).await.is_err() {
                                    calls.closed_streams.fetch_add(1, Ordering::SeqCst);
                                    return;
                                }
                                tokio::time::sleep(Duration::from_millis(10)).await;
                            }
                        });
                    }
                    Some(watch_v2::watch_request::Request::Cancel(cancel)) => {
                        calls.cancels.lock().push(cancel.watch_id);
                    }
                    None => {}
                }
            }
        });
        Ok(tonic::Response::new(ReceiverStream::new(rx)))
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

async fn serve_upstream() -> (FakeServer, Arc<WatchCalls>) {
    let (fake, calls) = FakeWatch::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(WatchV2Server::new(fake))
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

async fn serve_proxy(inner: Option<Arc<AgentInner>>) -> FakeServer {
    let proxy = WatchV2Proxy::new(inner);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(WatchV2Server::new(proxy))
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

async fn connect(addr: &str) -> WatchV2Client<tonic::transport::Channel> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .expect("connect proxy");
    WatchV2Client::new(channel)
}

fn create(key: &[u8]) -> watch_v2::WatchRequest {
    watch_v2::WatchRequest {
        request: Some(watch_v2::watch_request::Request::Create(
            watch_v2::WatchCreateRequest {
                key: key.to_vec(),
                ..Default::default()
            },
        )),
    }
}

fn cancel(watch_id: i64) -> watch_v2::WatchRequest {
    watch_v2::WatchRequest {
        request: Some(watch_v2::watch_request::Request::Cancel(
            watch_v2::WatchCancelRequest { watch_id },
        )),
    }
}

async fn next_batch(
    stream: &mut tonic::Streaming<watch_v2::WatchResponse>,
) -> watch_v2::WatchResponse {
    tokio::time::timeout(Duration::from_secs(3), stream.message())
        .await
        .expect("watch batch timed out")
        .expect("watch stream error")
        .expect("watch stream ended")
}

async fn wait_for_closed_stream(calls: &Arc<WatchCalls>) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while calls.closed_streams.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("cancel must tear the upstream stream down");
}

// ──── 用例 ────

#[tokio::test(flavor = "multi_thread")]
async fn proxy_forwards_watch_v2_with_local_ids_cursor_and_idempotent_cancel() {
    let (upstream, calls) = serve_upstream().await;
    let inner = agent_inner_via(&upstream.addr).await;
    let proxy = serve_proxy(Some(inner)).await;
    let mut client = connect(&proxy.addr).await;

    let (req_tx, req_rx) = mpsc::channel(8);
    req_tx.send(create(b"/app/a")).await.unwrap();
    let mut resp = client
        .watch(ReceiverStream::new(req_rx))
        .await
        .expect("watch through proxy")
        .into_inner();

    let first = next_batch(&mut resp).await;
    assert_eq!(first.watch_id, 1, "local ids start at 1");
    assert_eq!(first.watermark, 50);
    assert!(first.sealed);
    assert_eq!(first.cursor.unwrap().commit_ts, 42);
    assert_eq!(first.events.len(), 1);
    assert_eq!(first.events[0].commit_ts, 42);
    assert_eq!(first.events[0].changes[0].value, b"v");

    // 同一条客户端流上的第二个 create 必须得到自己的本地 id，事件不串号。
    req_tx.send(create(b"/app/b")).await.unwrap();
    let second = loop {
        let msg = next_batch(&mut resp).await;
        if msg.watch_id == 2 {
            break msg;
        }
        assert_eq!(
            msg.watch_id, 1,
            "unexpected watch id before the second create"
        );
    };
    assert_eq!(second.events.len(), 1);

    // 取消 1 只关闭 1 的上游订阅；2 继续收批。
    req_tx.send(cancel(1)).await.unwrap();
    wait_for_closed_stream(&calls).await;
    // 排空取消前已在管道里的批，之后只能看到 id 2。
    tokio::time::sleep(Duration::from_millis(150)).await;
    for _ in 0..3 {
        let msg = next_batch(&mut resp).await;
        assert_eq!(msg.watch_id, 2, "cancelled subscription must stop");
    }

    // 未知 id 的取消幂等：不报错、不打断仍在运行的订阅。
    req_tx.send(cancel(999)).await.unwrap();
    let msg = next_batch(&mut resp).await;
    assert_eq!(msg.watch_id, 2);

    // 代理把取消实现为关闭上游流，不依赖服务端 cancel 帧。
    assert!(
        calls.cancels.lock().is_empty(),
        "proxy cancel must tear down the stream, not send a watch_id the wrapper hides"
    );
    assert_eq!(calls.creates.lock().len(), 2);

    // 先关客户端与请求流，再停服务器：stream 未断时 graceful shutdown 会等连接。
    drop(resp);
    drop(client);
    drop(req_tx);
    proxy.shutdown().await;
    upstream.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn skeleton_watch_v2_fails_closed() {
    let proxy = serve_proxy(None).await;
    let mut client = connect(&proxy.addr).await;

    let (req_tx, req_rx) = mpsc::channel(4);
    req_tx.send(create(b"/app/a")).await.unwrap();
    // 首帧错误可能以两种形式到达（RPC 错误或流首帧错误），都必须是 UNAVAILABLE。
    match client.watch(ReceiverStream::new(req_rx)).await {
        Ok(response) => {
            let err = response
                .into_inner()
                .message()
                .await
                .expect_err("skeleton watch must fail closed");
            assert_eq!(err.code(), tonic::Code::Unavailable, "{err:?}");
            assert_eq!(error_code_of(&err).as_deref(), Some("UNAVAILABLE"));
        }
        Err(err) => {
            assert_eq!(err.code(), tonic::Code::Unavailable, "{err:?}");
            assert_eq!(error_code_of(&err).as_deref(), Some("UNAVAILABLE"));
        }
    }

    proxy.shutdown().await;
}
