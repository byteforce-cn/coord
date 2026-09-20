// 协议版本协商（P0-4 / D6）—— **跨语言对表卡口**
//
// 背景：`coord.agent.Handshake/Negotiate` 此前在**两端都是空头承诺** ——
// Rust 侧只有一个能力表登记行（`coord-core/src/grpc_auth.rs:121`，注释原文
// "声明未实现，仍登记以免落到'未知 RPC'"），Java 侧 `ProtocolNegotiator`
// 的判定方法只被单元测试调用。两端的类注释/文档都写着"会协商并拒绝不兼容版本"，
// 而该行为不存在。
//
// 单纯"把两边都实现"不足以防止复发：**版本字符串本身**是两侧各自硬编码的，
// 谁改了都不会有人发现 -- 直到运行期出现一个含糊的 `UNIMPLEMENTED`。
// 本测试把两侧版本口径绑成一条可执行判据（与仓库既有的源码扫描型卡口同族：
// `scripts/check-error-code-contract.sh`、`scripts/check-panics.sh`）：
//
//   1. agent 侧 `SUPPORTED_PROTOCOL_VERSIONS` 必须**包含** Java 侧
//      `SDK_PROTOCOL_VERSION`（否则新 SDK 天生连不上）；
//   2. Java 侧不得再出现已退役的 `coord-agent-api-v1` 作为**版本常量**
//      （D2 是一次性改名，v1 无服务期）；
//   3. agent 侧必须真的注册/暴露 `Handshake` 服务（防止"实现被删掉、
//      文档还在"的空头复发）。

use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    // `CARGO_MANIFEST_DIR` = <repo>/coord
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate dir has a parent")
        .to_path_buf()
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

/// Java 侧 SDK 协议版本常量（从源码解析，而不是在测试里再写一份 —— 再写一份
/// 就又多了一个会漂移的副本）。
fn java_sdk_protocol_version() -> String {
    let path = repo_root().join(
        "coord-java-sdk/src/main/java/cn/byteforce/coord/sdk/internal/channel/ProtocolNegotiator.java",
    );
    let text = read(&path);
    let marker = "SDK_PROTOCOL_VERSION = \"";
    let start = text
        .find(marker)
        .unwrap_or_else(|| panic!("{marker:?} not found in {}", path.display()))
        + marker.len();
    let rest = &text[start..];
    let end = rest.find('"').expect("unterminated version literal");
    rest[..end].to_string()
}

/// ① 版本对表：agent 必须支持 SDK 声明的版本。
#[test]
fn agent_advertises_the_version_the_java_sdk_speaks() {
    let sdk_version = java_sdk_protocol_version();
    let supported = coord_agent::services::handshake::SUPPORTED_PROTOCOL_VERSIONS;
    assert!(
        supported.contains(&sdk_version.as_str()),
        "agent advertises {supported:?} but the Java SDK speaks {sdk_version:?} — \
         every SDK call would fail as PROTOCOL_MISMATCH (or, before D6, as an \
         un-diagnosable UNIMPLEMENTED)"
    );
}

/// ② v1 已退役：SDK 里不得再把它作为协议版本常量（D2 一次性改名，无服务期）。
#[test]
fn java_sdk_does_not_still_speak_the_retired_v1_protocol() {
    let path = repo_root().join(
        "coord-java-sdk/src/main/java/cn/byteforce/coord/sdk/internal/channel/ProtocolNegotiator.java",
    );
    let text = read(&path);
    assert!(
        !text.contains("SDK_PROTOCOL_VERSION = \"coord-agent-api-v1\""),
        "the SDK still declares the retired v1 protocol version; the agent's services \
         were renamed in contracts/v1.2.0 in a one-shot switch (D2)"
    );
    assert!(
        !coord_agent::services::handshake::SUPPORTED_PROTOCOL_VERSIONS
            .contains(&"coord-agent-api-v1"),
        "the agent must NOT advertise v1: there is no dual-serving period (D2)"
    );
}

/// ③ agent 必须真的把 Handshake 挂进 gRPC 服务链（而不是只有实现类）。
///
/// 断言的落点：注册点（`lib.rs`）与 gRPC 链（`plugin/grpc.rs`）。
/// 这两处任一被删掉，"实现了但没人能调到"就会复发 —— 那与空头承诺等价。
#[test]
fn agent_registers_the_handshake_service() {
    let lib = read(&repo_root().join("coord-agent/src/lib.rs"));
    assert!(
        lib.contains("AgentGrpcService::Handshake("),
        "coord-agent does not register the Handshake service; \
         Handshake.Negotiate would be unreachable and protocol mismatches \
         would again surface as un-diagnosable UNIMPLEMENTED"
    );
    let grpc = read(&repo_root().join("coord-agent/src/plugin/grpc.rs"));
    assert!(
        grpc.contains("handshake_server::HandshakeServer::from_arc"),
        "the Handshake service is not added to the agent's tonic service chain"
    );
}

/// ④ Java 侧必须真的**调用**协商（防止"实现了但客户端从不调用"的空头复发）。
#[test]
fn java_channel_manager_actually_negotiates() {
    let path = repo_root().join(
        "coord-java-sdk/src/main/java/cn/byteforce/coord/sdk/internal/channel/AgentChannelManager.java",
    );
    let text = read(&path);
    assert!(
        text.contains("HandshakeGrpc.newBlockingStub"),
        "AgentChannelManager never calls Handshake.Negotiate — the Java side would be \
         an empty promise again"
    );
    let client = read(
        &repo_root().join("coord-java-sdk/src/main/java/cn/byteforce/coord/sdk/CoordClient.java"),
    );
    assert!(
        client.contains("connectAndNegotiate"),
        "CoordClient never triggers negotiation; nothing in the production path would \
         call it"
    );
}
