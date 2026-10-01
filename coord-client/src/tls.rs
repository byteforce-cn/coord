// coord-client: TLS/mTLS Channel 构建
//
// 统一 ConnectionPool 与 Leader 发现的 Channel 构建路径：
// `tls` 为 Some 时走 https + tonic TLS 配置，None 时保持明文 http（仅限开发）。

use std::time::Duration;

use tonic::transport::Channel;

use crate::config::TlsConfig;

/// 构建到指定端点的 Channel（`endpoint` 为 `host:port`，不含 scheme）。
///
/// `tls` 为 Some 时使用 `https` scheme 并应用 TLS/mTLS 配置；
/// 否则使用 `http`。错误原样上抛，由调用方分类。
pub(crate) async fn connect(
    endpoint: &str,
    connect_timeout: Option<Duration>,
    tls: Option<&TlsConfig>,
) -> Result<Channel, Box<dyn std::error::Error + Send + Sync>> {
    let scheme = if tls.is_some() { "https" } else { "http" };
    let endpoint = Channel::from_shared(format!("{scheme}://{endpoint}"))?;
    let endpoint = match tls {
        Some(t) => endpoint.tls_config(t.to_tonic())?,
        None => endpoint,
    };
    let endpoint = match connect_timeout {
        Some(timeout) => endpoint.connect_timeout(timeout),
        None => endpoint,
    };
    Ok(endpoint.connect().await?)
}
