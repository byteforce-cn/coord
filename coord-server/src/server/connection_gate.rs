// 客户端 gRPC 口的**连接维度**全局闸（B-CX-1）
//
// `max_concurrent_streams` 限的是**每连接**的流数，不是连接数；慢连接 / 半开连接
// 可长期占用 fd 与连接级内存。本模块在 accept 层加连接数量上限：连接数达上限时，
// 新连接在 accept 后**立即断开**（不排队），使 fd / 任务占用有界。
//
// 语义对齐 agent health 监听器加固（`HealthLimits`）：
// - 配额与连接生命周期绑定（IO 包装类型持有 permit，连接 drop 即归还）；
// - 超限立即 close——不用排队掩盖过载；被拒连接计数进指标，日志用 debug
//   （被扫描 / 攻击时 warn 日志本身是洪泛面）。
//
// 只用于**客户端口**：raft 口是节点间 mTLS 通道，连接规模由集群成员数决定，
// 不设连接闸（避免误伤复制 / 选举流量）。

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::Stream;
use tonic::transport::server::{Connected, TcpConnectInfo};

use crate::metrics::Metrics;

/// 客户端 gRPC 口的连接闸。
pub struct ConnectionGate {
    semaphore: Arc<Semaphore>,
    metrics: Metrics,
    max_connections: usize,
}

impl ConnectionGate {
    /// 创建连接闸。
    ///
    /// `max_connections` 由配置层（`network.max_connections`，校验 >= 1）保证；
    /// 这里再取 `max(1)` 兜底，避免库调用方传 0 构造出「永远拒绝」的闸。
    pub fn new(max_connections: usize, metrics: Metrics) -> Self {
        let max_connections = max_connections.max(1);
        Self {
            semaphore: Arc::new(Semaphore::new(max_connections)),
            metrics,
            max_connections,
        }
    }

    /// 包装客户端口 listener 流：超限连接在 accept 后立即断开。
    ///
    /// 返回的流同一实例可跨 TLS 证书热加载的多次 `serve` 复用（Semaphore 共享），
    /// 使上限在热加载前后保持全局一致。
    pub fn wrap(&self, listener: TcpListenerStream) -> GatedListenerStream {
        GatedListenerStream {
            inner: listener,
            semaphore: Arc::clone(&self.semaphore),
            metrics: self.metrics.clone(),
            max_connections: self.max_connections,
        }
    }
}

/// 包装后的 listener 流；超限连接不产生 item（直接被断开）。
pub struct GatedListenerStream {
    inner: TcpListenerStream,
    semaphore: Arc<Semaphore>,
    metrics: Metrics,
    max_connections: usize,
}

impl Stream for GatedListenerStream {
    type Item = io::Result<GatedConnection>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match Pin::new(&mut this.inner).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(Some(Ok(socket))) => {
                    match Arc::clone(&this.semaphore).try_acquire_owned() {
                        Ok(permit) => {
                            this.metrics.inc_grpc_conn_active();
                            return Poll::Ready(Some(Ok(GatedConnection {
                                inner: socket,
                                _permit: permit,
                                metrics: this.metrics.clone(),
                            })));
                        }
                        Err(_) => {
                            // 超限：立即断开，不排队。debug 而非 warn（见模块头注释）。
                            this.metrics.inc_grpc_conn_rejected();
                            tracing::debug!(
                                max_connections = this.max_connections,
                                "client gRPC connection limit reached; closing new connection"
                            );
                            drop(socket);
                            // 继续接受下一条：拒绝不产生 item，等同于 accept 循环的 continue
                        }
                    }
                }
            }
        }
    }
}

/// 持有连接配额的客户端连接：drop 时归还 permit 并回写活跃 gauge。
pub struct GatedConnection {
    inner: TcpStream,
    _permit: OwnedSemaphorePermit,
    metrics: Metrics,
}

impl Drop for GatedConnection {
    fn drop(&mut self) {
        self.metrics.dec_grpc_conn_active();
    }
}

impl AsyncRead for GatedConnection {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for GatedConnection {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

impl Connected for GatedConnection {
    // 与 `Connected for TcpStream` 保持同一 ConnectInfo 类型，
    // 使请求 extensions 中的连接信息不因闸门而改变形态。
    type ConnectInfo = TcpConnectInfo;

    fn connect_info(&self) -> TcpConnectInfo {
        TcpConnectInfo {
            local_addr: self.inner.local_addr().ok(),
            remote_addr: self.inner.peer_addr().ok(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::AsyncReadExt;
    use tokio_stream::StreamExt;

    /// 读取 render 输出中的指定指标值（同时验证指标已接上导出面）。
    fn metric_value(m: &Metrics, name: &str) -> i64 {
        m.render_prometheus_text()
            .lines()
            .find_map(|line| {
                line.strip_prefix(name)
                    .and_then(|rest| rest.strip_prefix(' '))
                    .and_then(|v| v.trim().parse().ok())
            })
            .unwrap_or_else(|| panic!("render 输出缺少指标 {name}"))
    }

    fn active_connections(m: &Metrics) -> i64 {
        metric_value(m, "coord_grpc_connections_active")
    }

    fn rejected_connections(m: &Metrics) -> i64 {
        metric_value(m, "coord_grpc_connections_rejected_total")
    }

    async fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !cond() {
            assert!(tokio::time::Instant::now() < deadline, "等待超时：{what}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// 接受后保持连接存活，读到 EOF/错误即 drop（归还配额）。
    fn hold(conn: GatedConnection) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut conn = conn;
            let mut buf = [0u8; 64];
            loop {
                match conn.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
        })
    }

    /// 驱动 accept 流：每条被接受的连接转交 `hold`（句柄留存到任务结束）。
    fn drive(mut incoming: GatedListenerStream) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Some(item) = incoming.next().await {
                match item {
                    Ok(conn) => held.push(hold(conn)),
                    Err(_) => break,
                }
            }
            drop(held);
        })
    }

    async fn bind_gate(limit: usize, metrics: &Metrics) -> (std::net::SocketAddr, ConnectionGate) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let gate = ConnectionGate::new(limit, metrics.clone());
        drive(gate.wrap(TcpListenerStream::new(listener)));
        (addr, gate)
    }

    /// 负控制：把闸门去掉（`ConnectionGate::new` 用无限配额）⇒ 本测试必须变红
    /// （第三条连接不再被立即断开、活跃 gauge 变 3）。
    #[tokio::test]
    async fn over_limit_connection_is_closed_immediately() {
        let metrics = Metrics::new();
        let (addr, _gate) = bind_gate(2, &metrics).await;

        let mut c1 = TcpStream::connect(addr).await.unwrap();
        let mut c2 = TcpStream::connect(addr).await.unwrap();
        wait_until("前两条连接被接受", || {
            active_connections(&metrics) == 2
        })
        .await;

        // 第三条：超限 ⇒ accept 后立即断开，而不是排队挂起。
        let mut c3 = TcpStream::connect(addr).await.unwrap();
        let mut buf = [0u8; 1];
        let r = tokio::time::timeout(Duration::from_secs(2), c3.read(&mut buf))
            .await
            .expect("超限连接必须被立即断开（读挂起 = 排队，不允许）");
        assert!(
            matches!(r, Ok(0)) || r.is_err(),
            "超限连接应以 EOF/RST 结束，实得 {r:?}"
        );

        // 上限内的连接不得被误伤：短超时读应为"挂起"而非 EOF。
        let r1 = tokio::time::timeout(Duration::from_millis(200), c1.read(&mut buf)).await;
        assert!(r1.is_err(), "上限内连接不得被关闭，实得 {r1:?}");
        let r2 = tokio::time::timeout(Duration::from_millis(200), c2.read(&mut buf)).await;
        assert!(r2.is_err(), "上限内连接不得被关闭，实得 {r2:?}");

        // 活跃数保持在上限之内；被拒连接计数 >= 1。
        assert_eq!(active_connections(&metrics), 2, "活跃连接数必须 <= 上限");
        assert!(rejected_connections(&metrics) >= 1, "超限拒绝必须计数");
    }

    /// 负控制：把 permit 泄漏（永不归还）⇒ 本测试在"drop 后新连接仍被接受"处变红。
    #[tokio::test]
    async fn permit_is_released_when_connection_dropped() {
        let metrics = Metrics::new();
        let (addr, _gate) = bind_gate(1, &metrics).await;

        let c1 = TcpStream::connect(addr).await.unwrap();
        wait_until("第一条连接被接受", || {
            active_connections(&metrics) == 1
        })
        .await;

        // 客户端断开 ⇒ 服务端读到 EOF ⇒ 连接对象 drop ⇒ 配额归还。
        drop(c1);
        wait_until("配额随连接 drop 归还", || {
            active_connections(&metrics) == 0
        })
        .await;

        // 配额归还后，新连接必须能顶满上限（被接受并保持打开）。
        let mut c2 = TcpStream::connect(addr).await.unwrap();
        wait_until("新连接被接受", || active_connections(&metrics) == 1).await;
        let mut buf = [0u8; 1];
        let r = tokio::time::timeout(Duration::from_millis(200), c2.read(&mut buf)).await;
        assert!(r.is_err(), "配额归还后新连接应保持打开，实得 {r:?}");
        assert_eq!(rejected_connections(&metrics), 0, "未超限不得计入拒绝");
    }
}
