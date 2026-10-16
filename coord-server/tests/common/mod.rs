//! coord-server 集成测试共享夹具。
//!
//! 「bind(:0) 取号 → 释放 → 稍后 `serve(addr)` 重绑」存在 TOCTOU 竞态：
//! 并行测试可能拿到同一端口，落败方的 `let _ = serve(..)` 静默失败 →
//! raft 无 quorum / 客户端连到别的监听器（症状：region leader 等待超时、
//! 空 message 的 `Unimplemented`）。进程内起 server 一律用 [`bind_listener`]
//! 预绑定并由 `serve_with_incoming[_shutdown]` 接管。

/// 绑定一个本地监听器并返回 `(listener, "127.0.0.1:port")`。
///
/// 监听器**保持绑定**直到交给 `tonic::transport::Server::serve_with_incoming`
/// 接管，消除「取端口→释放→重绑」窗口——并行测试不可能再撞到同一端口。
/// 不得改成「先取端口号、稍后按地址 `serve`」，那会重新引入上述竞态。
pub async fn bind_listener() -> (tokio::net::TcpListener, String) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback listener");
    let addr = listener
        .local_addr()
        .expect("listener local_addr")
        .to_string();
    (listener, addr)
}
