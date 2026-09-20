// coord-agent: 协议版本协商服务（`coord.agent.Handshake`）
//
// 背景（P0-4 / D6）：`agent_api.proto` 里 `Handshake.Negotiate` 的**两端此前都是
// 空头承诺** —— Rust 侧只有一个能力表登记行（注释原文"声明未实现，仍登记以免落到
// '未知 RPC'"，`coord-core/src/grpc_auth.rs:121`），Java 侧
// `ProtocolNegotiator.isVersionSupported` 只被单元测试调用，`AgentChannelManager`
// 建连后再无使用。类注释却写着"Agent 返回支持的版本列表；不在列表里则拒绝连接" ——
// 该行为在两侧都不存在。
//
// 这个空头本身是**切换点的可诊断性问题**：D2 采用一次性改名（不设双服务期），
// 旧 SDK（`coord-agent-api-v1`）调用 `/coord.agent.Registry/Register` 会拿到裸的
// gRPC `UNIMPLEMENTED`（未知服务），错误信息**不指向"版本不兼容"**。
// 因此本服务必须真的实现，且客户端必须真的调用（见 Java 侧
// `AgentChannelManager`）。
//
// 版本口径（`WHITEPAPER.md` 防空头承诺）：
// - **单一事实来源**是本模块的 [`SUPPORTED_PROTOCOL_VERSIONS`]；
// - 客户端版本常量见 Java 侧 `ProtocolNegotiator.SDK_PROTOCOL_VERSION`，
//   两侧由 `coord/tests/agent_handshake_test.rs` 对表，防止再次漂移成空头。

use coord_proto::agent::handshake_server::Handshake;
use coord_proto::agent::{HandshakeRequest, HandshakeResponse};
use tonic::{Request, Response, Status};

/// agent 支持的协议版本列表（**唯一事实来源**）。
///
/// `coord-agent-api-v1` **不在**列表中：D2 一次性改名后 agent 只讲新协议，
/// 老客户端必须拿到"不支持"的**明确**答复，而不是被静默当作可用。
pub const SUPPORTED_PROTOCOL_VERSIONS: &[&str] = &["coord-agent-api-v2"];

/// 协议协商服务。
///
/// 语义（刻意保持极简且无状态）：
/// - 返回**全部**支持版本，无论客户端报什么版本 —— 由客户端判定自己是否在其中，
///   这样"不支持"的原因在客户端侧是可诊断的（而不是一个含糊的 RPC 错误）；
/// - 客户端版本不在列表内时**只记日志**，不返回错误：响应 schema 里没有错误字段，
///   硬塞一个 `Status` 会让"能协商但版本不对"与"服务不可用"混为一谈。
#[derive(Debug, Default)]
pub struct HandshakeService;

impl HandshakeService {
    /// 新建（无状态）。
    pub fn new() -> Self {
        Self
    }

    /// 支持的版本列表（供测试与装配期日志使用）。
    pub fn supported_versions(&self) -> Vec<String> {
        SUPPORTED_PROTOCOL_VERSIONS
            .iter()
            .map(|v| (*v).to_string())
            .collect()
    }
}

#[tonic::async_trait]
impl Handshake for HandshakeService {
    async fn negotiate(
        &self,
        request: Request<HandshakeRequest>,
    ) -> Result<Response<HandshakeResponse>, Status> {
        let client_version = request.into_inner().client_version;
        let supported = self.supported_versions();

        if client_version.is_empty() {
            tracing::info!(
                "Handshake.Negotiate: client did not declare a version; offering {:?}",
                supported
            );
        } else if !SUPPORTED_PROTOCOL_VERSIONS.contains(&client_version.as_str()) {
            // 可诊断性就落在这里：日志与响应**同时**给出"你报了什么"与"我支持什么"。
            tracing::warn!(
                client_version = %client_version,
                supported = ?supported,
                "Handshake.Negotiate: client protocol version is NOT supported; \
                 the client must upgrade (its calls to the renamed services would \
                 otherwise fail as `UNIMPLEMENTED` with no version hint)"
            );
        } else {
            tracing::debug!(
                client_version = %client_version,
                "Handshake.Negotiate: client protocol version accepted"
            );
        }

        Ok(Response::new(HandshakeResponse {
            supported_versions: supported,
        }))
    }
}

/// 生命周期适配：与其余内建服务一致地经 `PluginManager` 注册/启停。
///
/// `start`/`stop` 刻意**不做事**：协商服务完全无状态、无后台任务，
/// 但经同一注册表挂载保证"gRPC 服务面的单一注册点"这一架构约束不被绕过
/// （见 `coord-agent/src/plugin/grpc.rs` 顶部说明）。
#[async_trait::async_trait]
impl crate::service::BaseService for HandshakeService {
    fn name(&self) -> &'static str {
        "handshake"
    }

    async fn start(&self) -> crate::service::ServiceResult<()> {
        tracing::info!(
            supported = ?SUPPORTED_PROTOCOL_VERSIONS,
            "HandshakeService: protocol negotiation endpoint serving"
        );
        Ok(())
    }

    async fn stop(&self) -> crate::service::ServiceResult<()> {
        Ok(())
    }

    fn health_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn negotiate_returns_all_supported_versions() {
        let svc = HandshakeService::new();
        let resp = svc
            .negotiate(Request::new(HandshakeRequest {
                client_version: "coord-agent-api-v2".into(),
            }))
            .await
            .expect("negotiate must succeed")
            .into_inner();
        assert_eq!(resp.supported_versions, vec!["coord-agent-api-v2".to_string()]);
    }

    /// 不支持的版本**也要**返回支持列表（而不是错误）：客户端据此产出可诊断错误。
    #[tokio::test]
    async fn negotiate_still_lists_versions_for_unsupported_client() {
        let svc = HandshakeService::new();
        let resp = svc
            .negotiate(Request::new(HandshakeRequest {
                client_version: "coord-agent-api-v1".into(),
            }))
            .await
            .expect("negotiate must not fail on an unknown client version")
            .into_inner();
        assert!(
            !resp
                .supported_versions
                .iter()
                .any(|v| v == "coord-agent-api-v1"),
            "v1 must not be advertised: the rename is a one-shot switch (D2)"
        );
        assert_eq!(resp.supported_versions, vec!["coord-agent-api-v2".to_string()]);
    }

    /// 空版本（老客户端 / 探测工具）不得导致失败。
    #[tokio::test]
    async fn negotiate_tolerates_missing_client_version() {
        let svc = HandshakeService::new();
        let resp = svc
            .negotiate(Request::new(HandshakeRequest {
                client_version: String::new(),
            }))
            .await
            .expect("negotiate must tolerate an empty client version")
            .into_inner();
        assert_eq!(resp.supported_versions.len(), SUPPORTED_PROTOCOL_VERSIONS.len());
    }
}
