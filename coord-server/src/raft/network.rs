// Raft Network — RaftNetworkFactory + RaftNetworkV2 实现
//
// 基于 Tonic gRPC 实现节点间 Raft RPC 通信：
// - RaftNetworkFactory：为每个目标节点创建 Raft 网络客户端
// - RaftNetworkV2：发送 AppendEntries / Vote / FullSnapshot RPC
// - RaftRpcServer：接收并处理来自其他节点的 Raft RPC
//
// 通信使用 raft_addr 端口（与客户端 gRPC 端口分离），消息体使用 postcard 序列化
// （payload_codec=1；ADR-0007）。
// 支持可选的 TLS/mTLS 加密节点间通信（差距 #14）。

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use openraft::error::{RPCError, ReplicationClosed, StreamingError};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, TransferLeaderRequest,
    TransferLeaderResponse, VoteRequest, VoteResponse,
};
use openraft::type_config::alias::{SnapshotOf, VoteOf};
use openraft::OptionalSend;
use openraft::RaftNetworkFactory;
use openraft::RaftNetworkV2;
use parking_lot::RwLock;
use tonic::transport::Channel;

use super::type_config::{PdOp, TypeConfig};
use super::CoordRaft;
use crate::storage::snapshot_limiter::SnapshotRateLimiter;

// Re-export for raft_rpc_server
pub use coord_proto::raft::raft_client::RaftClient;
pub use coord_proto::raft::raft_server::{Raft as RaftRpcTrait, RaftServer as RaftRpcServer};
use coord_proto::raft::RaftMessage as RaftMessageProto;

use crate::tls;

// ──── 载荷编码标记与序列化工具 ────
//
// ADR-0007：RaftMessage.payload 的编码由协议层标记 `payload_codec` 显式决定；
// P3 后唯一受支持标记为 postcard（1）——历史 bincode（0）已退役，出现即
// 显式拒绝（不猜测、不回落、禁试错解码）。

/// postcard 载荷标记（唯一受支持；历史 bincode=0 已退役）。
pub const PAYLOAD_CODEC_POSTCARD: u32 = 1;

/// 写路径标记：恒为 postcard（写侧无运行期开关）。
const WRITE_PAYLOAD_CODEC: u32 = PAYLOAD_CODEC_POSTCARD;

/// codec=1 的 HMAC 域分离标签（ADR-0007 D3）。
///
/// 输入字节规范（两侧实现必须逐字节一致，防漂移）：`DOMAIN || codec_u32_be ||
/// payload`。
const RAFT_AUTH_DOMAIN_V2: &[u8] = b"coord-raft-payload-v2";

/// 校验 wire 标记：P3 后只接受 postcard；其余（含退役的 bincode=0）显式拒绝。
fn ensure_supported_payload_codec(codec: u32) -> Result<(), tonic::Status> {
    if codec == PAYLOAD_CODEC_POSTCARD {
        Ok(())
    } else {
        Err(tonic::Status::invalid_argument(format!(
            "unsupported raft payload codec: {codec} (expected {PAYLOAD_CODEC_POSTCARD})"
        )))
    }
}

/// 编码出站载荷（写路径唯一编码器：postcard）。
fn serialize_payload<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, tonic::Status> {
    postcard::to_allocvec(value)
        .map_err(|e| tonic::Status::internal(format!("postcard serialize failed: {e}")))
}

/// 解码载荷（唯一 postcard 腿；精确消费——拒绝尾随字节）。
fn deserialize_payload<T: serde::de::DeserializeOwned>(data: &[u8]) -> Result<T, tonic::Status> {
    let (value, remainder) = postcard::take_from_bytes::<T>(data)
        .map_err(|e| tonic::Status::internal(format!("postcard deserialize failed: {e}")))?;
    if !remainder.is_empty() {
        return Err(tonic::Status::internal(format!(
            "trailing bytes after raft payload: {}",
            remainder.len()
        )));
    }
    Ok(value)
}

/// 读取消息标记并解码载荷（所有接收侧的公共入口：先验标记，再解码）。
fn decode_message_payload<T: serde::de::DeserializeOwned>(
    msg: &RaftMessageProto,
) -> Result<T, tonic::Status> {
    ensure_supported_payload_codec(msg.payload_codec)?;
    deserialize_payload(&msg.payload)
}

/// 构建 RaftMessageProto（v6.0 新增 region_id 和 trace_context 字段）
///
/// 从当前 tracing span 中提取 trace context，注入到 Raft 消息中，
/// 实现跨节点的分布式追踪。`auth_tag` 由调用方按需计算。
/// 单 Raft 模式 region_id=0；Multi-Raft 下由调用方传入具体 Region。
fn make_raft_message_for_region(payload: Vec<u8>, region_id: u64) -> RaftMessageProto {
    let trace_context = extract_trace_context();
    RaftMessageProto {
        payload,
        region_id,
        trace_context,
        auth_tag: Vec::new(),
        payload_codec: WRITE_PAYLOAD_CODEC,
    }
}

/// 单 Raft 模式消息构造（region_id=0，兼容既有调用点）
#[cfg(test)]
fn make_raft_message(payload: Vec<u8>) -> RaftMessageProto {
    make_raft_message_for_region(payload, 0)
}

/// 对 payload 计算 HMAC-SHA256 认证标签（无 mTLS 时的共享密钥认证）。
///
/// 认证输入为域分离形式（ADR-0007 D3）：`HMAC(secret, DOMAIN || codec_be ||
/// payload)`——标记被纳入认证覆盖；P3 后唯一 codec=1。
fn compute_raft_auth_tag(payload: &[u8], secret: &[u8]) -> Result<Vec<u8>, tonic::Status> {
    use hmac::{Hmac, Mac};
    type HmacSha256 = Hmac<sha2::Sha256>;
    let mut mac = HmacSha256::new_from_slice(secret)
        .map_err(|e| tonic::Status::internal(format!("raft HMAC key invalid: {e}")))?;
    mac.update(RAFT_AUTH_DOMAIN_V2);
    mac.update(&PAYLOAD_CODEC_POSTCARD.to_be_bytes());
    mac.update(payload);
    Ok(mac.finalize().into_bytes().to_vec())
}

/// 校验入站 raft 消息的认证标签（配置了共享密钥时强制；fail-closed）。
///
/// 不受支持的标记（含退役 bincode=0）在认证/解码前显式拒绝（ADR-0007 D2）。
fn verify_raft_auth(msg: &RaftMessageProto, secret: Option<&[u8]>) -> Result<(), tonic::Status> {
    ensure_supported_payload_codec(msg.payload_codec)?;
    let Some(secret) = secret else {
        return Ok(());
    };
    if msg.auth_tag.is_empty() {
        return Err(tonic::Status::unauthenticated(
            "missing raft auth tag (shared secret configured)",
        ));
    }
    use hmac::{Hmac, Mac};
    type HmacSha256 = Hmac<sha2::Sha256>;
    let mut mac = HmacSha256::new_from_slice(secret)
        .map_err(|e| tonic::Status::internal(format!("raft HMAC key invalid: {e}")))?;
    mac.update(RAFT_AUTH_DOMAIN_V2);
    mac.update(&PAYLOAD_CODEC_POSTCARD.to_be_bytes());
    mac.update(&msg.payload);
    mac.verify_slice(&msg.auth_tag)
        .map_err(|_| tonic::Status::unauthenticated("invalid raft auth tag"))
}

/// 从当前 tracing span 提取 W3C Trace Context
///
/// 使用 tracing 的 span ID 构造简易的 trace context 字节。
/// 生产环境应使用 opentelemetry 的完整 W3C traceparent 格式。
fn extract_trace_context() -> Vec<u8> {
    let span = tracing::Span::current();
    if span.is_none() {
        return vec![];
    }

    // 使用 span 的 id 作为 trace context
    // 格式: [version=0x00][trace_id:16B][span_id:8B][flags:1B]
    let mut ctx = Vec::with_capacity(25);
    ctx.push(0x00); // version

    // 使用 tracing span 的 field 来获取 id（简化实现）
    // 实际生产环境中应使用 opentelemetry Context 传播
    // 此处写入占位符，确保字段非空以表示 tracing 已启用
    let span_id = span.id();
    if let Some(id) = span_id {
        // span ID 为 u64，放入字段
        ctx.extend_from_slice(&[0u8; 16]); // trace_id placeholder
        ctx.extend_from_slice(&id.into_u64().to_be_bytes());
        ctx.push(0x01); // flags: sampled
    }

    ctx
}

/// 从 RaftMessageProto 中恢复 trace context 并创建子 span
///
/// 在服务端收到 Raft RPC 时调用，将上游的 trace context 注入到当前 span。
fn inject_received_trace_context(msg: &RaftMessageProto) {
    if msg.trace_context.is_empty() {
        return;
    }
    // 记录收到 trace context（简化实现）
    // 生产环境应解析 W3C traceparent 并创建关联的 span
    tracing::debug!(
        trace_context_len = msg.trace_context.len(),
        "Received Raft RPC with trace context"
    );
}

// ──── RaftNetworkFactory ────

/// 单个目标节点的共享 gRPC 客户端槽位（惰性连接；`Option` 为空即未连接）。
type SharedRaftClient = Arc<tokio::sync::Mutex<Option<RaftClient<Channel>>>>;

/// 连接池：目标节点 ID → 共享客户端槽位。
type SharedClientCache = Arc<tokio::sync::Mutex<HashMap<u64, SharedRaftClient>>>;

/// Raft 网络工厂
///
/// 维护集群中所有节点的 Raft 地址映射，为每个目标节点创建 gRPC 客户端。
/// 支持可选的 TLS/mTLS 配置用于节点间加密通信。
///
/// 内置连接池：为每个目标节点维护一个共享的 gRPC Channel（Arc + Mutex），
/// 避免每次 RPC 都重新建立 TCP/TLS 连接。这对 Leader 选举期间的 Vote RPC
/// 至关重要，因为选举超时较短，连接建立开销会导致 Vote RPC 超时。
pub struct RaftNetworkFactoryImpl {
    /// 本节点 ID
    #[allow(dead_code)]
    node_id: u64,
    /// 节点 ID → Raft 地址 映射
    node_addrs: Arc<RwLock<HashMap<u64, String>>>,
    /// Raft 节点间 TLS 配置（可选）
    raft_tls_config: Option<Arc<tls::TlsConfig>>,
    /// raft 节点间共享密钥（无 mTLS 时的 HMAC 认证，可选）
    shared_secret: Option<Arc<Vec<u8>>>,
    /// 连接池：目标节点 ID → 共享的 gRPC 客户端槽位（惰性连接）。
    /// 外层 `Arc<tokio::sync::Mutex<...>>` 使工厂可 Clone——Multi-Raft 各 Region
    /// 的 per-region 网络工厂共享同一底层连接池。
    /// 使用 tokio::sync::Mutex 因为临界区包含 async 连接操作。
    client_cache: SharedClientCache,
    /// 模拟网络分区的黑名单：此节点无法与黑名单中的节点通信
    /// 用于测试网络分区和对称分区场景
    blocked_nodes: Arc<RwLock<HashSet<u64>>>,
    /// R-RFT-19：快照传输限速器（token bucket，跨目标节点共享；None = 不限速）
    snapshot_rate_limiter: Option<Arc<SnapshotRateLimiter>>,
}

impl Clone for RaftNetworkFactoryImpl {
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id,
            node_addrs: Arc::clone(&self.node_addrs),
            raft_tls_config: self.raft_tls_config.clone(),
            shared_secret: self.shared_secret.clone(),
            client_cache: Arc::clone(&self.client_cache),
            blocked_nodes: Arc::clone(&self.blocked_nodes),
            snapshot_rate_limiter: self.snapshot_rate_limiter.clone(),
        }
    }
}

impl RaftNetworkFactoryImpl {
    pub fn new(node_id: u64) -> Self {
        Self {
            node_id,
            node_addrs: Arc::new(RwLock::new(HashMap::new())),
            raft_tls_config: None,
            shared_secret: None,
            client_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            blocked_nodes: Arc::new(RwLock::new(HashSet::new())),
            snapshot_rate_limiter: None,
        }
    }

    /// 使用共享的 blocklist 创建工厂（用于测试网络分区）
    ///
    /// 多个工厂可以共享同一个 blocklist，测试代码可以通过 blocklist
    /// 动态控制哪些节点之间的通信被阻止。
    pub fn with_shared_blocklist(node_id: u64, blocked_nodes: Arc<RwLock<HashSet<u64>>>) -> Self {
        Self {
            node_id,
            node_addrs: Arc::new(RwLock::new(HashMap::new())),
            raft_tls_config: None,
            shared_secret: None,
            client_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            blocked_nodes,
            snapshot_rate_limiter: None,
        }
    }

    /// 获取共享的 blocklist 引用（测试用）
    pub fn shared_blocklist(&self) -> Arc<RwLock<HashSet<u64>>> {
        Arc::clone(&self.blocked_nodes)
    }

    /// 注册节点 Raft 地址
    pub fn register_node(&self, node_id: u64, raft_addr: String) {
        self.node_addrs.write().insert(node_id, raft_addr);
    }

    /// 设置 Raft 节点间 TLS 配置
    ///
    /// 若配置了 TLS，所有节点间 Raft RPC（AppendEntries/Vote/InstallSnapshot）
    /// 将通过 TLS 加密传输。若同时配置了 CA 证书，则启用 mTLS 双向验证。
    pub fn set_raft_tls(&mut self, tls_config: tls::TlsConfig) {
        self.raft_tls_config = Some(Arc::new(tls_config));
    }

    /// 设置 raft 节点间共享密钥（无 mTLS 时的 HMAC 认证）。
    pub fn set_raft_shared_secret(&mut self, secret: &str) {
        self.shared_secret = Some(Arc::new(secret.as_bytes().to_vec()));
    }

    /// R-RFT-19：设置快照传输限速器（0 = 不限速）。
    ///
    /// 快照分块发送前按 token bucket 申请许可，避免快照同步占满节点间带宽
    /// 影响正常 AppendEntries/Vote 通信（限速器**必须**真实接线，不得是死代码）。
    pub fn set_snapshot_rate_limiter(&mut self, max_bytes_per_sec: u64) {
        self.snapshot_rate_limiter = Some(Arc::new(if max_bytes_per_sec == 0 {
            SnapshotRateLimiter::unlimited()
        } else {
            SnapshotRateLimiter::with_burst(max_bytes_per_sec, SNAPSHOT_CHUNK_SIZE as u64)
        }));
    }

    /// 检查 Raft 节点间 TLS 是否已配置
    pub fn has_raft_tls(&self) -> bool {
        self.raft_tls_config.is_some()
    }

    /// 模拟网络分区：阻止本节点与 target_node 的通信（测试用）
    ///
    /// 调用后，本节点到 target_node 的所有 Raft RPC（AppendEntries/Vote/Snapshot）
    /// 将返回 Unreachable 错误，模拟网络分区。
    pub fn block_node(&self, target_node: u64) {
        tracing::info!(
            "[partition-sim] node {} blocking communication to node {}",
            self.node_id,
            target_node
        );
        self.blocked_nodes.write().insert(target_node);
    }

    /// 解除对 target_node 的通信阻止（测试用）
    pub fn unblock_node(&self, target_node: u64) {
        tracing::info!(
            "[partition-sim] node {} unblocking communication to node {}",
            self.node_id,
            target_node
        );
        self.blocked_nodes.write().remove(&target_node);
    }

    /// 检查目标节点是否被阻止
    fn is_blocked(&self, target: u64) -> bool {
        self.blocked_nodes.read().contains(&target)
    }

    /// 解析目标节点地址：优先走节点注册表（静态/初始节点），查不到返回 None。
    fn resolve_addr(&self, target: u64, node: &openraft::impls::BasicNode) -> Option<String> {
        if !node.addr.is_empty() {
            Some(node.addr.clone())
        } else {
            self.node_addrs.read().get(&target).cloned()
        }
    }

    /// 取或建目标节点的共享客户端槽位（跨 Multi-Raft Region 共享连接池）。
    async fn get_or_create_client_slot(
        &self,
        target: u64,
    ) -> Arc<tokio::sync::Mutex<Option<RaftClient<Channel>>>> {
        let mut cache = self.client_cache.lock().await;
        cache
            .entry(target)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(None)))
            .clone()
    }

    /// 按（target, region_id）构建一个真实网络客户端（不检查分区黑名单；
    /// 分区模拟由调用方在 GroupRouter 层做）。region_id=0 等价单 Raft 模式。
    async fn build_network_impl(
        &self,
        target: u64,
        node: &openraft::impls::BasicNode,
        region_id: u64,
    ) -> Result<RaftNetworkImpl, RPCError<TypeConfig>> {
        let addr = self.resolve_addr(target, node).ok_or_else(|| {
            RPCError::Unreachable(openraft::error::Unreachable::new(&tonic::Status::internal(
                format!(
                    "no known raft address for node {target} (membership addr empty, \
                     static table miss); refusing to fabricate an address"
                ),
            )))
        })?;
        let client_slot = self.get_or_create_client_slot(target).await;
        Ok(RaftNetworkImpl {
            target_id: target,
            target_addr: addr,
            client_slot,
            tls_config: self.raft_tls_config.clone(),
            shared_secret: self.shared_secret.clone(),
            snapshot_rate_limiter: self.snapshot_rate_limiter.clone(),
            region_id,
        })
    }

    /// SubmitPdOp：把一条 PD 队列命令提交到 `target`
    /// 节点的 region 0 raft（目标应为 region 0 leader——PD 执行器在非 region 0
    /// leader 节点认领 operator 时，把 Claim/Complete/Requeue 经本方法转发到
    /// region 0 leader 提出；openraft `client_write` 仅 leader 可本地提出）。
    ///
    /// 复用 RaftNetworkImpl 的出站路径（连接池 + TLS/mTLS + 共享密钥
    /// HMAC），region_id = 0。返回日志 index；接收方非 leader/提出失败时返回
    /// `Err`（调用方解析 leader 变化后重试）。
    pub async fn submit_pd_op(&self, target: u64, op: PdOp) -> Result<u64, String> {
        let addr = self
            .node_addrs
            .read()
            .get(&target)
            .cloned()
            .ok_or_else(|| format!("no known raft address for node {target}"))?;
        let client_slot = self.get_or_create_client_slot(target).await;
        let net = RaftNetworkImpl {
            target_id: target,
            target_addr: addr,
            client_slot,
            tls_config: self.raft_tls_config.clone(),
            shared_secret: self.shared_secret.clone(),
            snapshot_rate_limiter: None,
            region_id: 0,
        };
        let mut client = net
            .get_client()
            .await
            .map_err(|e| format!("connect to node {target}: {e}"))?;
        let req_payload = serialize_payload(&PdSubmitPayload { op })
            .map_err(|e| format!("serialize pd submit: {e}"))?;
        let req = tonic::Request::new(
            net.build_authed_message(req_payload)
                .map_err(|e| format!("auth pd submit: {e}"))?,
        );
        let resp = client
            .submit_pd_op(req)
            .await
            .map_err(|e| format!("submit_pd_op to node {target}: {e}"))?
            .into_inner();
        let reply: PdSubmitReply =
            decode_message_payload(&resp).map_err(|e| format!("decode pd submit reply: {e}"))?;
        if !reply.error.is_empty() {
            return Err(format!("node {target} rejected pd submit: {}", reply.error));
        }
        Ok(reply.index)
    }

    /// SubmitRegionCommand：把一条数据 Region 内部命令转发到该 Region 的
    /// leader 节点提出（`client_write` 只有 leader 能本地提出）。
    ///
    /// 与 `submit_pd_op` 同一条出站路径（连接池 + TLS/mTLS + 共享密钥 HMAC，
    /// region_id = 目标 Region）。返回 apply 响应；接收方非 leader 或提出失败
    /// 时返回 `Err`（调用方刷新 leader 视图后重试；导入命令幂等，重放安全）。
    pub async fn submit_region_command(
        &self,
        target: u64,
        region_id: u64,
        command: crate::raft::type_config::Command,
    ) -> Result<crate::raft::type_config::Response, String> {
        if self.is_blocked(target) {
            return Err(format!("node {target} is blocked by the test partition"));
        }
        let addr = self
            .node_addrs
            .read()
            .get(&target)
            .cloned()
            .ok_or_else(|| format!("no known raft address for node {target}"))?;
        let client_slot = self.get_or_create_client_slot(target).await;
        let net = RaftNetworkImpl {
            target_id: target,
            target_addr: addr,
            client_slot,
            tls_config: self.raft_tls_config.clone(),
            shared_secret: self.shared_secret.clone(),
            snapshot_rate_limiter: None,
            region_id,
        };
        let mut client = net
            .get_client()
            .await
            .map_err(|e| format!("connect to node {target}: {e}"))?;
        let req_payload = serialize_payload(&RegionCommandPayload { region_id, command })
            .map_err(|e| format!("serialize region command: {e}"))?;
        let req = tonic::Request::new(
            net.build_authed_message(req_payload)
                .map_err(|e| format!("auth region command: {e}"))?,
        );
        let resp = client
            .submit_region_command(req)
            .await
            .map_err(|e| format!("submit_region_command to node {target}: {e}"))?
            .into_inner();
        let reply: RegionCommandReply = decode_message_payload(&resp)
            .map_err(|e| format!("decode region command reply: {e}"))?;
        if !reply.error.is_empty() {
            return Err(format!(
                "node {target} rejected region {region_id} command: {}",
                reply.error
            ));
        }
        reply
            .response
            .ok_or_else(|| format!("node {target} returned no response for region {region_id}"))
    }

    /// ReportRegionStats：把一条 Region 心跳快照发到 `target` 节点的 PD。
    ///
    /// `target` 应为 region 0 leader（operator 生成源）；复用 raft 出站连接池 /
    /// TLS / 共享密钥 HMAC 路径。统计是瞬态数据：失败不需要重试语义，调用方
    /// 下一拍重报即可。
    pub async fn report_region_stats(
        &self,
        target: u64,
        report: RegionStatsPayload,
    ) -> Result<(), String> {
        if self.is_blocked(target) {
            return Err(format!("node {target} is blocked by the test partition"));
        }
        let addr = self
            .node_addrs
            .read()
            .get(&target)
            .cloned()
            .ok_or_else(|| format!("no known raft address for node {target}"))?;
        let client_slot = self.get_or_create_client_slot(target).await;
        let net = RaftNetworkImpl {
            target_id: target,
            target_addr: addr,
            client_slot,
            tls_config: self.raft_tls_config.clone(),
            shared_secret: self.shared_secret.clone(),
            snapshot_rate_limiter: None,
            region_id: 0,
        };
        let mut client = net
            .get_client()
            .await
            .map_err(|e| format!("connect to node {target}: {e}"))?;
        let req_payload = serialize_payload(&report)
            .map_err(|e| format!("serialize region stats report: {e}"))?;
        let req = tonic::Request::new(
            net.build_authed_message(req_payload)
                .map_err(|e| format!("auth region stats report: {e}"))?,
        );
        let resp = client
            .report_region_stats(req)
            .await
            .map_err(|e| format!("report_region_stats to node {target}: {e}"))?
            .into_inner();
        let reply: RegionStatsReply =
            decode_message_payload(&resp).map_err(|e| format!("decode region stats reply: {e}"))?;
        if !reply.error.is_empty() {
            return Err(format!(
                "node {target} rejected region stats report: {}",
                reply.error
            ));
        }
        Ok(())
    }

    /// DrainRange：请 `target` 节点的本地会话表 drain 一段区间。
    ///
    /// 返回 `Ok(Drained)` / `Ok(Blocked)` 区分「对端可达但仍有会话」与
    /// 连接失败——调用方对前者 fail-closed，对后者按节点失联处理。
    pub async fn drain_range(
        &self,
        target: u64,
        start: &[u8],
        end: &[u8],
        timeout: Duration,
    ) -> Result<DrainRpcOutcome, String> {
        if self.is_blocked(target) {
            return Err(format!("node {target} is blocked by the test partition"));
        }
        let addr = self
            .node_addrs
            .read()
            .get(&target)
            .cloned()
            .ok_or_else(|| format!("no known raft address for node {target}"))?;
        let client_slot = self.get_or_create_client_slot(target).await;
        let net = RaftNetworkImpl {
            target_id: target,
            target_addr: addr,
            client_slot,
            tls_config: self.raft_tls_config.clone(),
            shared_secret: self.shared_secret.clone(),
            snapshot_rate_limiter: None,
            region_id: 0,
        };
        let timeout_ms = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX);
        let req_payload = serialize_payload(&DrainRangePayload {
            start_key: start.to_vec(),
            end_key: end.to_vec(),
            timeout_ms,
        })
        .map_err(|e| format!("serialize drain range: {e}"))?;
        let req = tonic::Request::new(
            net.build_authed_message(req_payload)
                .map_err(|e| format!("auth drain range: {e}"))?,
        );
        let mut client = net
            .get_client()
            .await
            .map_err(|e| format!("connect to node {target}: {e}"))?;
        // 对端自己的 drain 有同一个 timeout；多留 1s 给应用/网络开销。
        let rpc_wait = timeout.saturating_add(Duration::from_secs(1));
        let resp = tokio::time::timeout(rpc_wait, client.drain_range(req))
            .await
            .map_err(|_| format!("drain_range to node {target} timed out after {rpc_wait:?}"))?
            .map_err(|e| format!("drain_range to node {target}: {e}"))?
            .into_inner();
        let reply: DrainRangeReply =
            decode_message_payload(&resp).map_err(|e| format!("decode drain range reply: {e}"))?;
        if reply.drained {
            Ok(DrainRpcOutcome::Drained)
        } else {
            Ok(DrainRpcOutcome::Blocked(reply.error))
        }
    }
}

/// 到单个目标节点的 Raft 网络客户端（实现 RaftNetworkV2）
///
/// 支持通过 TLS/mTLS 连接到目标节点（当配置了 Raft TLS 时）。
/// Channel 由 RaftNetworkFactoryImpl 的连接池管理，多个 RaftNetworkImpl
/// 实例（对应不同的 RPC 调用）共享同一个底层 TCP 连接。
pub struct RaftNetworkImpl {
    #[allow(dead_code)]
    target_id: u64,
    target_addr: String,
    /// 共享的 gRPC 客户端槽位（惰性连接，跨实例共享）
    client_slot: Arc<tokio::sync::Mutex<Option<RaftClient<Channel>>>>,
    /// Raft 节点间 TLS 配置（可选）
    tls_config: Option<Arc<tls::TlsConfig>>,
    /// 共享密钥（可选，HMAC 认证出站消息）
    shared_secret: Option<Arc<Vec<u8>>>,
    /// R-RFT-19：快照传输限速器（可选；分块发送前申请许可）
    snapshot_rate_limiter: Option<Arc<SnapshotRateLimiter>>,
    /// 出站 Raft RPC 所属 Region（单 Raft = 0；Multi-Raft = RegionId）
    region_id: u64,
}

impl RaftNetworkImpl {
    /// 构造出站消息（配置共享密钥时计算 HMAC 标签）。
    /// 携带 region_id（Multi-Raft：按 region 解复用）。
    fn build_authed_message(&self, payload: Vec<u8>) -> Result<RaftMessageProto, tonic::Status> {
        let mut msg = make_raft_message_for_region(payload, self.region_id);
        if let Some(secret) = &self.shared_secret {
            msg.auth_tag = compute_raft_auth_tag(&msg.payload, secret)?;
        }
        Ok(msg)
    }

    /// 获取或建立到目标节点的 gRPC 连接（惰性、共享）
    async fn get_client(&self) -> Result<RaftClient<Channel>, tonic::Status> {
        let mut slot = self.client_slot.lock().await;
        if let Some(ref client) = *slot {
            return Ok(client.clone());
        }

        // Connect
        let use_tls = self
            .tls_config
            .as_ref()
            .map(|c| c.is_configured())
            .unwrap_or(false);
        let scheme = if use_tls { "https" } else { "http" };
        let endpoint = format!("{}://{}", scheme, self.target_addr);

        let mut channel_builder = Channel::from_shared(endpoint)
            .map_err(|e| tonic::Status::internal(format!("invalid raft addr: {e}")))?;

        if let Some(cfg) = self.tls_config.as_ref().filter(|_| use_tls) {
            if let Some(client_tls) = tls::build_client_tls(
                Some(&cfg.cert_path),
                Some(&cfg.key_path),
                cfg.ca_path.as_deref(),
            ) {
                channel_builder = channel_builder
                    .tls_config(client_tls)
                    .map_err(|e| tonic::Status::internal(format!("raft TLS config: {e}")))?;
                tracing::debug!(
                    "Raft network: TLS enabled for node {} at {}",
                    self.target_id,
                    self.target_addr
                );
            }
        }

        let channel = channel_builder.connect().await.map_err(|e| {
            tonic::Status::unavailable(format!(
                "connect to node {} at {}: {e}",
                self.target_id, self.target_addr
            ))
        })?;

        tracing::debug!(
            "Raft network: connected to node {} at {}",
            self.target_id,
            self.target_addr
        );
        let client = RaftClient::new(channel);
        *slot = Some(client.clone());
        Ok(client)
    }
}

fn to_rpc_error(e: tonic::Status) -> RPCError<TypeConfig> {
    RPCError::Unreachable(openraft::error::Unreachable::new(&e))
}

// ──── RaftNetwork (enum: 正常 或 分区阻止) ────

/// Raft 网络客户端枚举，统一正常通信和分区模拟两种模式。
///
/// - `Real`: 正常 gRPC 通信，每次 RPC 前动态检查 blocklist——分区期间创建
///   的网络对象在 unblock 后自动恢复（真实分区愈合不会更换传输）。
/// - `Blocked`: 目标地址不可解析（配置错误），所有 RPC 返回 Unreachable。
///   不得用它表示「当前被 blocklist 阻止」：那会把一次分区固化成永久故障。
pub enum RaftNetwork {
    Real {
        inner: RaftNetworkImpl,
        target_id: u64,
        blocked_nodes: Arc<RwLock<HashSet<u64>>>,
    },
    Blocked {
        target_id: u64,
    },
}

impl RaftNetwork {
    /// 每次 RPC 调用前检查目标是否被动态阻止（分区模拟）
    fn ensure_not_blocked(&self) -> Result<(), RPCError<TypeConfig>> {
        match self {
            RaftNetwork::Real {
                target_id,
                blocked_nodes,
                ..
            } => {
                if blocked_nodes.read().contains(target_id) {
                    let status = tonic::Status::unavailable(format!(
                        "simulated network partition: node {} is unreachable",
                        target_id
                    ));
                    return Err(RPCError::Unreachable(openraft::error::Unreachable::new(
                        &status,
                    )));
                }
                Ok(())
            }
            RaftNetwork::Blocked { target_id } => {
                let status = tonic::Status::unavailable(format!(
                    "raft target node {target_id} is unreachable (no resolved address)"
                ));
                Err(RPCError::Unreachable(openraft::error::Unreachable::new(
                    &status,
                )))
            }
        }
    }

    fn to_streaming_error(&self) -> StreamingError<TypeConfig> {
        let status = match self {
            RaftNetwork::Real { target_id, .. } => tonic::Status::unavailable(format!(
                "simulated network partition: node {target_id} is unreachable"
            )),
            RaftNetwork::Blocked { target_id } => tonic::Status::unavailable(format!(
                "raft target node {target_id} is unreachable (no resolved address)"
            )),
        };
        StreamingError::Unreachable(openraft::error::Unreachable::new(&status))
    }

    /// `Blocked` 状态下的 RPC 错误（地址不可解析；供 match 的兜底分支使用，
    /// 返回错误而非 panic）。
    fn blocked_rpc_error(&self) -> RPCError<TypeConfig> {
        let target_id = match self {
            RaftNetwork::Real { target_id, .. } => target_id,
            RaftNetwork::Blocked { target_id } => target_id,
        };
        let status = tonic::Status::unavailable(format!(
            "raft target node {target_id} is unreachable (no resolved address)"
        ));
        to_rpc_error(status)
    }
}

impl RaftNetworkV2<TypeConfig> for RaftNetwork {
    type SnapshotData = super::RaftSnapshotData;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.ensure_not_blocked()?;
        match self {
            RaftNetwork::Real { inner, .. } => inner.append_entries(rpc, option).await,
            RaftNetwork::Blocked { .. } => Err(self.blocked_rpc_error()),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.ensure_not_blocked()?;
        match self {
            RaftNetwork::Real { inner, .. } => inner.vote(rpc, option).await,
            RaftNetwork::Blocked { .. } => Err(self.blocked_rpc_error()),
        }
    }

    async fn transfer_leader(
        &mut self,
        rpc: TransferLeaderRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<TransferLeaderResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.ensure_not_blocked()?;
        match self {
            RaftNetwork::Real { inner, .. } => inner.transfer_leader(rpc, option).await,
            RaftNetwork::Blocked { .. } => Err(self.blocked_rpc_error()),
        }
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<TypeConfig>,
        snapshot: SnapshotOf<TypeConfig, super::RaftSnapshotData>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        // Check blocklist before snapshot transfer
        match self {
            RaftNetwork::Real {
                target_id,
                blocked_nodes,
                ..
            } => {
                if blocked_nodes.read().contains(target_id) {
                    return Err(self.to_streaming_error());
                }
            }
            RaftNetwork::Blocked { .. } => {
                return Err(self.to_streaming_error());
            }
        }
        match self {
            RaftNetwork::Real { inner, .. } => {
                inner.full_snapshot(vote, snapshot, cancel, option).await
            }
            RaftNetwork::Blocked { .. } => Err(self.to_streaming_error()),
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for RaftNetworkFactoryImpl {
    type Network = RaftNetwork;

    async fn new_client(
        &mut self,
        target: u64,
        node: &openraft::impls::BasicNode,
    ) -> Self::Network {
        // 被 blocklist 阻止时仍返回 Real：拒绝发生在每次 RPC 的动态检查里，
        // unblock 后同一网络对象即可恢复（heartbeat worker / 复制流会跨分区
        // 复用对象，若在创建时固化成 Blocked，愈合后永远不可达）。
        if self.is_blocked(target) {
            tracing::debug!(
                "[partition-sim] node {} → node {}: BLOCKED (simulated partition)",
                self.node_id,
                target
            );
        }

        // 优先使用 openraft 传入的 membership 地址（BasicNode.addr），
        //         静态表仅作 bootstrap 前兑底；查不到返回明确错误，不再伪造地址。
        let addr = if !node.addr.is_empty() {
            node.addr.clone()
        } else {
            match self.node_addrs.read().get(&target).cloned() {
                Some(addr) => addr,
                None => {
                    tracing::error!(
                        "no known raft address for node {target} (membership addr empty, \
                         static table miss); refusing to fabricate an address"
                    );
                    return RaftNetwork::Blocked { target_id: target };
                }
            }
        };

        // Get or create shared client slot (lazy connection, shared across instances
        // AND across Multi-Raft regions via the Arc-backed client_cache)
        let client_slot = self.get_or_create_client_slot(target).await;

        // Build the Real variant with a clone of the blocklist so that
        // subsequent RPCs on the same network object can detect dynamically
        // added blocks (e.g., symmetric network partition simulation where
        // the block is added after the initial connection was established).
        RaftNetwork::Real {
            inner: RaftNetworkImpl {
                target_id: target,
                target_addr: addr,
                client_slot,
                tls_config: self.raft_tls_config.clone(),
                shared_secret: self.shared_secret.clone(),
                snapshot_rate_limiter: self.snapshot_rate_limiter.clone(),
                region_id: 0, // 单 Raft 模式：region_id=0
            },
            target_id: target,
            blocked_nodes: Arc::clone(&self.blocked_nodes),
        }
    }
}

// ──── 可序列化的 Snapshot 包装 ────

/// 可序列化的 Snapshot 包装（用于网络传输）
#[derive(serde::Serialize, serde::Deserialize)]
struct SerializableSnapshot {
    /// Snapshot 元数据
    meta: openraft::type_config::alias::SnapshotMetaOf<TypeConfig>,
    /// Snapshot 数据
    data: Vec<u8>,
}

impl SerializableSnapshot {
    fn from_openraft(snapshot: &SnapshotOf<TypeConfig, super::RaftSnapshotData>) -> Self {
        use std::io::Read;
        let mut data = Vec::new();
        let mut cursor = snapshot.snapshot.clone();
        cursor.read_to_end(&mut data).ok();
        Self {
            meta: snapshot.meta.clone(),
            data,
        }
    }

    fn into_openraft(self) -> SnapshotOf<TypeConfig, super::RaftSnapshotData> {
        SnapshotOf::<TypeConfig, super::RaftSnapshotData> {
            meta: self.meta,
            snapshot: std::io::Cursor::new(self.data),
        }
    }
}

// ──── R-RFT-06：快照流式分块传输 ────

/// 快照分块大小（2MiB，低于 gRPC 默认 4MiB 解码上限，留出序列化头部余量）
const SNAPSHOT_CHUNK_SIZE: usize = 2 * 1024 * 1024;

/// 流式快照单帧（每帧承载一个分块）
#[derive(serde::Serialize, serde::Deserialize)]
struct SnapshotStreamMessage {
    /// Leader vote（follower 校验 leader 仍有效；每帧携带便于流式校验）
    vote: VoteOf<TypeConfig>,
    /// 分块序号（从 0 递增）
    chunk_index: u32,
    /// 总分块数
    total_chunks: u32,
    /// 分块数据
    data: Vec<u8>,
}

// ──── / SubmitPdOp（PD 命令跨节点转发提出）────

/// SubmitPdOp 请求载荷：一条 PD 队列命令（postcard）。经 raft_addr 节点间
/// RPC（Raft 服务）携带——认证（共享密钥 HMAC / mTLS）与其余 Raft RPC 同口径。
#[derive(serde::Serialize, serde::Deserialize)]
pub struct PdSubmitPayload {
    pub op: PdOp,
}

/// SubmitPdOp 响应：本地 propose 的日志 index；`error` 非空 = 提出失败
/// （如接收方不再是 region 0 leader——调用方解析后重试到新 leader）。
#[derive(serde::Serialize, serde::Deserialize)]
pub struct PdSubmitReply {
    pub index: u64,
    pub error: String,
}

// ──── / SubmitRegionCommand（数据 Region 内部命令跨节点转发提出）────

/// SubmitRegionCommand 请求载荷：目标 Region + 一条内部命令（postcard）。
/// 与 `PdSubmitOp` 同一条 raft_addr 节点间 RPC（共享密钥 HMAC / mTLS）。
#[derive(serde::Serialize, serde::Deserialize)]
pub struct RegionCommandPayload {
    pub region_id: u64,
    pub command: crate::raft::type_config::Command,
}

/// SubmitRegionCommand 响应：`error` 非空 = 提出失败（接收方不是该组 leader
/// 时也在 `error` 里回报，调用方刷新 leader 视图后重试）；成功时 `response`
/// 是该命令的 apply 响应。
#[derive(serde::Serialize, serde::Deserialize)]
pub struct RegionCommandReply {
    pub error: String,
    pub response: Option<crate::raft::type_config::Response>,
}

// ──── / ReportRegionStats（Region 心跳统计/采样上报）────

/// ReportRegionStats 请求载荷：Region leader 的一次心跳快照（postcard）。
/// 只承载内存瞬态数据，不是 raft 提案。
#[derive(serde::Serialize, serde::Deserialize)]
pub struct RegionStatsPayload {
    pub region_id: u64,
    pub size: u64,
    pub keys: u64,
    pub storage_bytes: u64,
    pub leader_node_id: u64,
    pub sample_keys: Vec<Vec<u8>>,
}

/// ReportRegionStats 响应：`error` 非空 = 接收方拒绝了上报（如本地未装配 PD）。
#[derive(serde::Serialize, serde::Deserialize)]
pub struct RegionStatsReply {
    pub error: String,
}

// ──── / DrainRange（split/merge 区间 drain 扇出）────

/// DrainRange 请求载荷：半开区间 `[start_key, end_key)`（`end_key` 空 = +∞）
/// 与等待上界。接收方在 RPC 生命周期内持有本地 drain 标记。
#[derive(serde::Serialize, serde::Deserialize)]
pub struct DrainRangePayload {
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub timeout_ms: u64,
}

/// DrainRange 响应：`drained=false` 时 `error` 是接收方的 drain 失败原因
/// （仍有相交会话 / 本地校验失败），与传输层错误区分。
#[derive(serde::Serialize, serde::Deserialize)]
pub struct DrainRangeReply {
    pub drained: bool,
    pub error: String,
}

/// 单次对端 drain 的结果：`Err` 是连接/协议失败（节点不可达），
/// `Blocked` 是节点可达但区间仍有活跃会话（迁移必须失败重试）。
#[derive(Debug)]
pub enum DrainRpcOutcome {
    Drained,
    Blocked(String),
}

/// 接收方对 drain 等待上界的硬上限：节点间 RPC 的调用方不可信，一个超大
/// timeout 不得把一段区间长期变成不可读。执行器默认 30s，留一倍余量。
const MAX_REMOTE_DRAIN_TIMEOUT: Duration = Duration::from_secs(60);

impl RaftNetworkV2<TypeConfig> for RaftNetworkImpl {
    type SnapshotData = super::RaftSnapshotData;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        let payload = serialize_payload(&rpc)
            .map_err(|e| RPCError::Unreachable(openraft::error::Unreachable::new(&e)))?;
        let req = tonic::Request::new(
            self.build_authed_message(payload)
                .map_err(|e| RPCError::Unreachable(openraft::error::Unreachable::new(&e)))?,
        );
        let mut client = self.get_client().await.map_err(to_rpc_error)?;
        let resp = client.append_entries(req).await.map_err(to_rpc_error)?;
        decode_message_payload(&resp.into_inner())
            .map_err(|e| RPCError::Unreachable(openraft::error::Unreachable::new(&e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        let payload = serialize_payload(&rpc)
            .map_err(|e| RPCError::Unreachable(openraft::error::Unreachable::new(&e)))?;
        let req = tonic::Request::new(
            self.build_authed_message(payload)
                .map_err(|e| RPCError::Unreachable(openraft::error::Unreachable::new(&e)))?,
        );
        let mut client = self.get_client().await.map_err(to_rpc_error)?;
        let resp = client.vote(req).await.map_err(to_rpc_error)?;
        decode_message_payload(&resp.into_inner())
            .map_err(|e| RPCError::Unreachable(openraft::error::Unreachable::new(&e)))
    }

    /// TransferLeader：把领导权转移请求发给目标节点。
    ///
    /// openraft `Raft::trigger().transfer_leader()` 由 leader 向每个 voter
    /// 广播本 RPC；目标节点收到后调用本地 `handle_transfer_leader` 立即接管
    /// （无此 RPC 时 openraft 只能退化为"等待 leader lease 超时后自由选举"，
    /// 无法保证转移到指定目标）。消息携带 region_id（Multi-Raft 解复用）。
    async fn transfer_leader(
        &mut self,
        rpc: TransferLeaderRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<TransferLeaderResponse<TypeConfig>, RPCError<TypeConfig>> {
        let payload = serialize_payload(&rpc)
            .map_err(|e| RPCError::Unreachable(openraft::error::Unreachable::new(&e)))?;
        let req = tonic::Request::new(
            self.build_authed_message(payload)
                .map_err(|e| RPCError::Unreachable(openraft::error::Unreachable::new(&e)))?,
        );
        let mut client = self.get_client().await.map_err(to_rpc_error)?;
        let resp = client.transfer_leader(req).await.map_err(to_rpc_error)?;
        decode_message_payload(&resp.into_inner())
            .map_err(|e| RPCError::Unreachable(openraft::error::Unreachable::new(&e)))
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<TypeConfig>,
        snapshot: SnapshotOf<TypeConfig, super::RaftSnapshotData>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let serializable = SerializableSnapshot::from_openraft(&snapshot);
        let payload = serialize_payload(&(&vote, &serializable))
            .map_err(|e| StreamingError::Unreachable(openraft::error::Unreachable::new(&e)))?;

        // R-RFT-06：分块流式传输（整包单条 gRPC 会超出 4MiB 解码上限，
        // follower 永久无法追赶）
        let total_chunks = payload.len().div_ceil(SNAPSHOT_CHUNK_SIZE).max(1) as u32;
        let mut client = self
            .get_client()
            .await
            .map_err(|e| StreamingError::Unreachable(openraft::error::Unreachable::new(&e)))?;

        let (tx, rx) = tokio::sync::mpsc::channel::<RaftMessageProto>(16);
        for (i, chunk) in payload.chunks(SNAPSHOT_CHUNK_SIZE).enumerate() {
            // R-RFT-19：分块发送前按 token bucket 限速（配置后生效，避免快照
            // 同步占满节点间带宽影响正常 Raft 通信）
            if let Some(limiter) = &self.snapshot_rate_limiter {
                limiter.acquire(chunk.len() as u64).await;
            }
            let frame = SnapshotStreamMessage {
                vote,
                chunk_index: i as u32,
                total_chunks,
                data: chunk.to_vec(),
            };
            let frame_payload = serialize_payload(&frame)
                .map_err(|e| StreamingError::Unreachable(openraft::error::Unreachable::new(&e)))?;
            let frame_msg = self
                .build_authed_message(frame_payload)
                .map_err(|e| StreamingError::Unreachable(openraft::error::Unreachable::new(&e)))?;
            if tx.send(frame_msg).await.is_err() {
                return Err(StreamingError::Unreachable(
                    openraft::error::Unreachable::new(&tonic::Status::internal(
                        "snapshot stream receiver dropped",
                    )),
                ));
            }
        }
        drop(tx);

        let resp = client
            .install_snapshot_streaming(tonic::Request::new(
                tokio_stream::wrappers::ReceiverStream::new(rx),
            ))
            .await
            .map_err(|e| StreamingError::Unreachable(openraft::error::Unreachable::new(&e)))?;
        decode_message_payload(&resp.into_inner())
            .map_err(|e| StreamingError::Unreachable(openraft::error::Unreachable::new(&e)))
    }
}

// ──── Multi-Raft 网络共享层────
//
// 依赖 openraft-multi 0.10.0-alpha.34（workspace 锁定，仅本 raft/ 模块内使用，
// 隔离边界）。官方用法见 databendlabs/openraft examples/multi-raft-kv：
// 共享 Router 实现 GroupRouter → per-region factory 包 GroupNetworkFactory →
// new_client 返回 GroupNetworkAdapter。Coord 侧把既有 RaftNetworkFactoryImpl 连接池
// 升级为 Arc 共享（见上），一个进程内所有 Region 的 Raft 实例共享同一出站连接池；
// 出站 RaftMessage 携带 region_id，服务端按 region 解复用。

impl RaftNetworkFactoryImpl {
    /// 为（target, region_id）构建一个真实网络客户端（成员地址 + 共享连接池）。
    /// region_id=0 等价单 Raft；供 GroupRouter 出站路径复用。
    async fn build_group_network(
        &self,
        target: u64,
        group_id: u64,
        node: &openraft::impls::BasicNode,
    ) -> Result<RaftNetworkImpl, RPCError<TypeConfig>> {
        // 分区模拟：目标被阻止时直接返回 Unreachable
        if self.is_blocked(target) {
            let status = tonic::Status::unavailable(format!(
                "simulated network partition: node {} is unreachable",
                target
            ));
            return Err(RPCError::Unreachable(openraft::error::Unreachable::new(
                &status,
            )));
        }
        self.build_network_impl(target, node, group_id).await
    }
}

/// Multi-Raft 出站路由：把 (target, group_id) 绑定到共享连接池发送，
/// 并让所有 Region 的 Raft 实例复用同一连接。
impl openraft_multi::GroupRouter<TypeConfig, u64> for RaftNetworkFactoryImpl {
    type SnapshotData = super::RaftSnapshotData;

    async fn append_entries(
        &self,
        target: u64,
        group_id: u64,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        // GroupRouter 无 node 参数，按 target 从静态地址表解析
        let node = openraft::impls::BasicNode::new(
            self.node_addrs
                .read()
                .get(&target)
                .cloned()
                .unwrap_or_default(),
        );
        let mut net = self.build_group_network(target, group_id, &node).await?;
        net.append_entries(rpc, option).await
    }

    async fn vote(
        &self,
        target: u64,
        group_id: u64,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        let node = openraft::impls::BasicNode::new(
            self.node_addrs
                .read()
                .get(&target)
                .cloned()
                .unwrap_or_default(),
        );
        let mut net = self.build_group_network(target, group_id, &node).await?;
        net.vote(rpc, option).await
    }

    /// Multi-Raft 领导权转移 RPC（openraft-multi `GroupRouter` 覆盖；
    /// `GroupNetworkAdapter::transfer_leader` 委托到本方法）。
    async fn transfer_leader(
        &self,
        target: u64,
        group_id: u64,
        req: TransferLeaderRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<TransferLeaderResponse<TypeConfig>, RPCError<TypeConfig>> {
        let node = openraft::impls::BasicNode::new(
            self.node_addrs
                .read()
                .get(&target)
                .cloned()
                .unwrap_or_default(),
        );
        let mut net = self.build_group_network(target, group_id, &node).await?;
        net.transfer_leader(req, option).await
    }

    async fn full_snapshot(
        &self,
        target: u64,
        group_id: u64,
        vote: VoteOf<TypeConfig>,
        snapshot: SnapshotOf<TypeConfig, super::RaftSnapshotData>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        if self.is_blocked(target) {
            let status = tonic::Status::unavailable(format!(
                "simulated network partition: node {} is unreachable",
                target
            ));
            return Err(StreamingError::Unreachable(
                openraft::error::Unreachable::new(&status),
            ));
        }
        let node = openraft::impls::BasicNode::new(
            self.node_addrs
                .read()
                .get(&target)
                .cloned()
                .unwrap_or_default(),
        );
        let mut net = self
            .build_network_impl(target, &node, group_id)
            .await
            .map_err(|e| StreamingError::Unreachable(openraft::error::Unreachable::new(&e)))?;
        net.full_snapshot(vote, snapshot, cancel, option).await
    }
}

/// Multi-Raft per-region 网络工厂：绑定一个 RegionId，实现
/// `RaftNetworkFactory`。每个 Region 的 Raft 实例用它创建到各目标的
/// `GroupNetworkAdapter`（出站消息自动携带本 region 的 region_id）。
///
/// 连接池由共享的 [`RaftNetworkFactoryImpl`]（即 GroupRouter）持有，因此
/// N 个 Region 只维护 N 份对端连接（每节点一份），而非 N×Region 份。
#[derive(Clone)]
pub struct RegionRaftNetworkFactory {
    router: RaftNetworkFactoryImpl,
    region_id: u64,
}

impl RegionRaftNetworkFactory {
    /// 用共享 router + region_id 构造 per-region 工厂。
    pub fn new(router: RaftNetworkFactoryImpl, region_id: u64) -> Self {
        Self { router, region_id }
    }

    /// 访问底层共享 router（注册节点地址等仍走它）。
    pub fn router(&self) -> &RaftNetworkFactoryImpl {
        &self.router
    }
}

impl RaftNetworkFactory<TypeConfig> for RegionRaftNetworkFactory {
    type Network = openraft_multi::GroupNetworkAdapter<TypeConfig, u64, RaftNetworkFactoryImpl>;

    async fn new_client(
        &mut self,
        target: u64,
        node: &openraft::impls::BasicNode,
    ) -> Self::Network {
        // 把 membership 携带的地址同步进共享地址表（GroupRouter 按 target 查表）
        if !node.addr.is_empty() {
            self.router.register_node(target, node.addr.clone());
        }
        openraft_multi::GroupNetworkAdapter::new(self.router.clone(), target, self.region_id)
    }
}

// ──── Raft RPC Server (gRPC Service 实现) ────

/// 接收来自其他节点的 Raft RPC 并转发给本地 Raft 实例。
///
/// Multi-Raft：入站 RaftMessage 携带 `region_id`，按 region
/// 解复用到对应 Raft 实例。单 Raft 模式 region_id=0 使用默认实例（`set_raft`），
/// 兼容既有调用方。
///
/// Clone 共享同一张 region→raft 表（字段均为 `Arc`）：故障恢复测试需要在
/// gRPC 服务不重绑监听器的前提下替换本节点的 raft 实例。
#[derive(Clone)]
pub struct RaftRpcService {
    /// 本地 Raft 实例表：region_id → Raft（region 0 = 单 Raft 默认实例）
    rafts: Arc<RwLock<HashMap<u64, CoordRaft>>>,
    /// 共享密钥（配置后强制验签，无标签拒绝）
    shared_secret: Option<Arc<Vec<u8>>>,
    /// split/merge 区间 drain 端口（本节点 `coord.txn.v2` 会话表）。
    /// `None` = `[kv_t]` 未启用/未装配：DrainRange RPC 返回 FAILED_PRECONDITION。
    split_drain: Arc<RwLock<Option<Arc<dyn crate::split_merge::SplitDrain>>>>,
    /// Region 心跳上报接收端（本节点 PD；`None` = 未启用 PD，
    /// ReportRegionStats 返回 FAILED_PRECONDITION）。
    region_heartbeat_sink: Arc<RwLock<Option<Arc<dyn crate::pd::RegionHeartbeatSink>>>>,
}

impl Default for RaftRpcService {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftRpcService {
    pub fn new() -> Self {
        Self {
            rafts: Arc::new(RwLock::new(HashMap::new())),
            shared_secret: None,
            split_drain: Arc::new(RwLock::new(None)),
            region_heartbeat_sink: Arc::new(RwLock::new(None)),
        }
    }

    /// 设置共享密钥（配置后所有入站 raft 消息强制 HMAC 验签）。
    pub fn with_shared_secret(mut self, secret: Option<&str>) -> Self {
        self.shared_secret = secret.map(|s| Arc::new(s.as_bytes().to_vec()));
        self
    }

    /// 设置默认 Raft 实例（region 0，单 Raft 模式；在 Raft 初始化后调用）
    pub fn set_raft(&self, raft: CoordRaft) {
        self.rafts.write().insert(0, raft);
    }

    /// Multi-Raft：注册某 Region 的 Raft 实例（解复用）。
    pub fn set_region_raft(&self, region_id: u64, raft: CoordRaft) {
        self.rafts.write().insert(region_id, raft);
    }

    /// 注册 split/merge 区间 drain 端口（本节点会话表；`None` = 未启用）。
    ///
    /// handler 在 RPC 生命周期内持有 drain guard，所以一个节点可以同时
    /// 响应多个区间/多个执行器的 drain；同区间重复登记是幂等的（本地
    /// `TxnSessions` 按 guard 计数）。
    pub fn set_split_drain(&self, drain: Option<Arc<dyn crate::split_merge::SplitDrain>>) {
        *self.split_drain.write() = drain;
    }

    /// 注册 Region 心跳上报接收端（本节点 PD；`None` = 未启用 PD）。
    ///
    /// 多节点装配时每个节点都装上，但只有 region 0 leader 节点的 PD 视图会被
    /// 调度读取（见 `PlacementDriver::run_schedule_tick`）；其它节点接收上报
    /// 不产生调度副作用。
    pub fn set_region_heartbeat_sink(&self, sink: Option<Arc<dyn crate::pd::RegionHeartbeatSink>>) {
        *self.region_heartbeat_sink.write() = sink;
    }

    /// 移除某 Region 的 Raft 实例（Region 下线/注销时）。
    pub fn remove_region_raft(&self, region_id: u64) {
        self.rafts.write().remove(&region_id);
    }

    /// 按 region_id 取 Raft 实例（仅纯单 Raft 模式回退到默认实例）。
    ///
    /// 回退仅当表里**只有** region 0（legacy 单 Raft：所有请求都按 region 0
    /// 寻址）；多 Raft 模式下未知 region 一律拒绝——否则发往尚未装配的 shard
    /// 组的 raft 消息会被错误地交给 region 0 状态机（跨组串流）。
    fn get_raft_for_region(&self, region_id: u64) -> Result<CoordRaft, tonic::Status> {
        let rafts = self.rafts.read();
        if let Some(r) = rafts.get(&region_id) {
            return Ok(r.clone());
        }
        if region_id != 0 && rafts.len() == 1 {
            if let Some(r) = rafts.get(&0) {
                return Ok(r.clone());
            }
        }
        Err(tonic::Status::internal(format!(
            "raft not initialized for region {region_id}"
        )))
    }

    /// 验签（配置了共享密钥时 fail-closed）
    fn verify_incoming(&self, msg: &RaftMessageProto) -> Result<(), tonic::Status> {
        verify_raft_auth(msg, self.shared_secret.as_deref().map(|v| v.as_slice()))
    }
}

/// 单发 RPC 辅助：读 region_id、验签、取对应 Raft、按标记反序列化请求。
/// 返回 (raft, region_id, rpc)。
macro_rules! dispatch_raft_rpc {
    ($self:ident, $msg:ident, $ty:ty) => {{
        inject_received_trace_context(&$msg);
        $self.verify_incoming(&$msg)?;
        let region_id = $msg.region_id;
        let raft = $self.get_raft_for_region(region_id)?;
        let rpc: $ty = decode_message_payload(&$msg)?;
        (raft, region_id, rpc)
    }};
}

#[tonic::async_trait]
impl coord_proto::raft::raft_server::Raft for RaftRpcService {
    async fn append_entries(
        &self,
        request: tonic::Request<RaftMessageProto>,
    ) -> Result<tonic::Response<RaftMessageProto>, tonic::Status> {
        let msg = request.into_inner();
        let (raft, region_id, rpc) =
            dispatch_raft_rpc!(self, msg, AppendEntriesRequest<TypeConfig>);
        let resp = raft
            .append_entries(rpc)
            .await
            .map_err(|e| tonic::Status::internal(format!("append_entries failed: {e}")))?;
        let payload = serialize_payload(&resp)?;
        Ok(tonic::Response::new(make_raft_message_for_region(
            payload, region_id,
        )))
    }

    async fn vote(
        &self,
        request: tonic::Request<RaftMessageProto>,
    ) -> Result<tonic::Response<RaftMessageProto>, tonic::Status> {
        let msg = request.into_inner();
        let (raft, region_id, rpc) = dispatch_raft_rpc!(self, msg, VoteRequest<TypeConfig>);
        let resp = raft
            .vote(rpc)
            .await
            .map_err(|e| tonic::Status::internal(format!("vote failed: {e}")))?;
        let payload = serialize_payload(&resp)?;
        Ok(tonic::Response::new(make_raft_message_for_region(
            payload, region_id,
        )))
    }

    /// TransferLeader：leader 广播的领导权转移请求。
    ///
    /// 目标节点经 openraft `handle_transfer_leader` 立即发起带 leadership_transfer
    /// 的选举（其余 voter 仅重置 lease，让目标接管）；按 region_id 解复用。
    async fn transfer_leader(
        &self,
        request: tonic::Request<RaftMessageProto>,
    ) -> Result<tonic::Response<RaftMessageProto>, tonic::Status> {
        let msg = request.into_inner();
        let (raft, region_id, rpc) =
            dispatch_raft_rpc!(self, msg, TransferLeaderRequest<TypeConfig>);
        let resp = raft
            .handle_transfer_leader(rpc)
            .await
            .map_err(|e| tonic::Status::internal(format!("handle_transfer_leader failed: {e}")))?;
        let payload = serialize_payload(&resp)?;
        Ok(tonic::Response::new(make_raft_message_for_region(
            payload, region_id,
        )))
    }

    async fn install_snapshot(
        &self,
        request: tonic::Request<RaftMessageProto>,
    ) -> Result<tonic::Response<RaftMessageProto>, tonic::Status> {
        let msg = request.into_inner();
        inject_received_trace_context(&msg);
        self.verify_incoming(&msg)?;
        let region_id = msg.region_id;
        let raft = self.get_raft_for_region(region_id)?;
        let (vote, serializable): (VoteOf<TypeConfig>, SerializableSnapshot) =
            decode_message_payload(&msg)?;
        let snapshot = serializable.into_openraft();
        let resp = raft
            .install_full_snapshot(vote, snapshot)
            .await
            .map_err(|e| tonic::Status::internal(format!("install_full_snapshot failed: {e}")))?;
        let payload = serialize_payload(&resp)?;
        Ok(tonic::Response::new(make_raft_message_for_region(
            payload, region_id,
        )))
    }

    /// R-RFT-06：流式快照接收——按序收集分块，校验完整性后重组安装。
    /// Multi-Raft：所有分块必须携带同一 region_id，重组后按 region 解复用。
    async fn install_snapshot_streaming(
        &self,
        request: tonic::Request<tonic::Streaming<RaftMessageProto>>,
    ) -> Result<tonic::Response<RaftMessageProto>, tonic::Status> {
        let mut stream = request.into_inner();
        let mut chunks: Vec<Vec<u8>> = Vec::new();
        let mut expected_index: u32 = 0;
        let mut total_chunks: Option<u32> = None;
        let mut region_id: Option<u64> = None;

        while let Some(msg) = stream
            .message()
            .await
            .map_err(|e| tonic::Status::internal(format!("snapshot stream recv: {e}")))?
        {
            inject_received_trace_context(&msg);
            self.verify_incoming(&msg)?;
            let frame: SnapshotStreamMessage = deserialize_payload(&msg.payload)?;
            // 流式快照必须绑定单一 region（fail-closed）
            match region_id {
                None => region_id = Some(msg.region_id),
                Some(rid) if rid != msg.region_id => {
                    return Err(tonic::Status::invalid_argument(format!(
                        "snapshot stream region mismatch: expected {rid}, got {}",
                        msg.region_id
                    )));
                }
                _ => {}
            }
            // 分块必须严格按序（fail-closed）
            if frame.chunk_index != expected_index {
                return Err(tonic::Status::invalid_argument(format!(
                    "snapshot chunk out of order: expected {expected_index}, got {}",
                    frame.chunk_index
                )));
            }
            expected_index += 1;
            total_chunks = Some(frame.total_chunks);
            chunks.push(frame.data);
        }

        let total = total_chunks
            .ok_or_else(|| tonic::Status::invalid_argument("snapshot stream missing chunks"))?;
        if expected_index != total || chunks.is_empty() {
            return Err(tonic::Status::invalid_argument(format!(
                "incomplete snapshot stream: received {expected_index}/{total} chunks"
            )));
        }

        // 重组完整快照字节（唯一 postcard 腿）
        let mut data = Vec::with_capacity(chunks.iter().map(|c| c.len()).sum());
        for chunk in &chunks {
            data.extend_from_slice(chunk);
        }
        let (vote, serializable): (VoteOf<TypeConfig>, SerializableSnapshot) =
            deserialize_payload(&data)?;
        let snapshot = serializable.into_openraft();

        let region_id = region_id.unwrap_or(0);
        let raft = self.get_raft_for_region(region_id)?;
        let resp = raft
            .install_full_snapshot(vote, snapshot)
            .await
            .map_err(|e| tonic::Status::internal(format!("install_full_snapshot failed: {e}")))?;
        let payload = serialize_payload(&resp)?;
        Ok(tonic::Response::new(make_raft_message_for_region(
            payload, region_id,
        )))
    }

    /// SubmitPdOp：接收方在**本机 region 0 raft** 本地提出
    /// 一条 PD 队列命令并等待 apply。openraft `client_write` 仅 leader 可本地提出
    /// （follower 返回 ForwardToLeader）——PD 执行器在非 region 0 leader 节点认领
    /// 目标 Region 的 operator 时，把 Claim/Complete/Requeue 经本 RPC 转发到
    /// region 0 leader 节点提出（apply CAS 语义不变，见 raft/system_raft.rs）。
    async fn submit_pd_op(
        &self,
        request: tonic::Request<RaftMessageProto>,
    ) -> Result<tonic::Response<RaftMessageProto>, tonic::Status> {
        let msg = request.into_inner();
        inject_received_trace_context(&msg);
        self.verify_incoming(&msg)?;
        let payload: PdSubmitPayload = decode_message_payload(&msg)?;
        let raft = self.get_raft_for_region(0)?;
        let reply = match raft
            .client_write(crate::raft::type_config::Command::Pd(payload.op))
            .await
        {
            Ok(resp) => match resp.response() {
                crate::raft::type_config::Response::Put { revision } => PdSubmitReply {
                    index: *revision,
                    error: String::new(),
                },
                other => PdSubmitReply {
                    index: 0,
                    error: format!("unexpected region0 submit_pd response: {other:?}"),
                },
            },
            Err(e) => PdSubmitReply {
                index: 0,
                error: format!("region0 raft pd propose failed: {e}"),
            },
        };
        let resp_payload = serialize_payload(&reply)?;
        Ok(tonic::Response::new(make_raft_message_for_region(
            resp_payload,
            0,
        )))
    }

    /// SubmitRegionCommand：接收方在**本机该 Region 的 raft** 本地提出
    /// 一条内部命令并等待 apply。
    ///
    /// 只接受本机是该组 leader 的提案：follower 会把 openraft 的
    /// `ForwardToLeader` 暴露为 `error`，由调用方（搬迁器）按 leader 视图重试——
    /// 不在节点间把命令再转发一次（避免两个 follower 互相踢回）。
    async fn submit_region_command(
        &self,
        request: tonic::Request<RaftMessageProto>,
    ) -> Result<tonic::Response<RaftMessageProto>, tonic::Status> {
        let msg = request.into_inner();
        inject_received_trace_context(&msg);
        self.verify_incoming(&msg)?;
        let payload: RegionCommandPayload = decode_message_payload(&msg)?;
        let region_id = payload.region_id;
        let reply = match self.get_raft_for_region(region_id) {
            Ok(raft) => match raft.client_write(payload.command).await {
                Ok(resp) => RegionCommandReply {
                    error: String::new(),
                    response: Some(resp.response().clone()),
                },
                Err(e) => RegionCommandReply {
                    error: format!("region {region_id} command propose failed: {e}"),
                    response: None,
                },
            },
            Err(status) => RegionCommandReply {
                error: status.message().to_string(),
                response: None,
            },
        };
        let resp_payload = serialize_payload(&reply)?;
        Ok(tonic::Response::new(make_raft_message_for_region(
            resp_payload,
            region_id,
        )))
    }

    /// ReportRegionStats：接收方把上报应用到**本机 PD**（内存瞬态视图）。
    ///
    /// 不是 raft 提案：统计不落盘、不进日志，失败只需下一拍重报。接收方即使
    /// 不是 region 0 leader 也接受（PD 视图所有节点可写，调度只读 leader 节点的）。
    async fn report_region_stats(
        &self,
        request: tonic::Request<RaftMessageProto>,
    ) -> Result<tonic::Response<RaftMessageProto>, tonic::Status> {
        let msg = request.into_inner();
        inject_received_trace_context(&msg);
        self.verify_incoming(&msg)?;
        let payload: RegionStatsPayload = decode_message_payload(&msg)?;
        let sink = self.region_heartbeat_sink.read().clone();
        let reply = match sink {
            Some(sink) => {
                let report = crate::pd::RegionHeartbeatReport {
                    region_id: payload.region_id,
                    size: payload.size,
                    keys: payload.keys,
                    storage_bytes: payload.storage_bytes,
                    leader_node_id: payload.leader_node_id,
                    sample_keys: payload.sample_keys,
                };
                match sink.apply_region_heartbeat(report) {
                    Ok(()) => RegionStatsReply {
                        error: String::new(),
                    },
                    Err(e) => RegionStatsReply {
                        error: e.to_string(),
                    },
                }
            }
            None => RegionStatsReply {
                error: "PD is not assembled on this node".to_string(),
            },
        };
        let resp_payload = serialize_payload(&reply)?;
        Ok(tonic::Response::new(make_raft_message_for_region(
            resp_payload,
            0,
        )))
    }

    /// DrainRange：在本节点用 `SplitDrain` 等待区间内会话收尾。
    ///
    /// `drained=false` 是应用级失败（仍有相交会话），用响应载荷回传给
    /// 调用方；未启用 `[kv_t]` 的节点没有会话表，返回 FAILED_PRECONDITION。
    async fn drain_range(
        &self,
        request: tonic::Request<RaftMessageProto>,
    ) -> Result<tonic::Response<RaftMessageProto>, tonic::Status> {
        let msg = request.into_inner();
        inject_received_trace_context(&msg);
        self.verify_incoming(&msg)?;
        let payload: DrainRangePayload = decode_message_payload(&msg)?;
        let drain = self.split_drain.read().clone().ok_or_else(|| {
            tonic::Status::failed_precondition("split drain is not assembled on this node")
        })?;
        let result = drain
            .drain(
                &payload.start_key,
                &payload.end_key,
                Duration::from_millis(payload.timeout_ms).min(MAX_REMOTE_DRAIN_TIMEOUT),
            )
            .await;
        let reply = DrainRangeReply {
            drained: result.is_ok(),
            error: result.err().unwrap_or_default(),
        };
        let resp_payload = serialize_payload(&reply)?;
        Ok(tonic::Response::new(make_raft_message_for_region(
            resp_payload,
            0,
        )))
    }
}

// ──── 测试 ────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_make_raft_message_includes_trace_context() {
        let msg = make_raft_message(b"test_payload".to_vec());
        // trace_context 应该被填充（即使为空，也是合法的空 Vec）
        assert_eq!(msg.payload, b"test_payload");
        // region_id 默认为 0
        assert_eq!(msg.region_id, 0);
    }

    #[test]
    fn test_extract_trace_context_without_span() {
        // 在无 active span 的环境中调用
        let ctx = extract_trace_context();
        // 无 active span 时返回空 Vec
        assert!(ctx.is_empty());
    }

    #[test]
    fn test_inject_received_trace_context_empty() {
        // 空的 trace_context 不应 panic
        let msg = RaftMessageProto {
            payload: vec![],
            region_id: 0,
            trace_context: vec![],
            auth_tag: Vec::new(),
            payload_codec: PAYLOAD_CODEC_POSTCARD,
        };
        inject_received_trace_context(&msg);
        // 不应 panic
    }

    #[test]
    fn test_inject_received_trace_context_non_empty() {
        // 非空的 trace_context 不应 panic
        let msg = RaftMessageProto {
            payload: vec![],
            region_id: 0,
            trace_context: vec![0x00, 0x01, 0x02],
            auth_tag: Vec::new(),
            payload_codec: PAYLOAD_CODEC_POSTCARD,
        };
        inject_received_trace_context(&msg);
        // 不应 panic
    }

    #[test]
    fn test_make_raft_message_with_region_id() {
        // 验证 region_id 可被正确设置
        let msg = RaftMessageProto {
            payload: b"region_payload".to_vec(),
            region_id: 42,
            trace_context: vec![0x01, 0x02, 0x03],
            auth_tag: Vec::new(),
            payload_codec: PAYLOAD_CODEC_POSTCARD,
        };
        assert_eq!(msg.region_id, 42);
        assert_eq!(msg.payload, b"region_payload");
        assert_eq!(msg.trace_context, vec![0x01, 0x02, 0x03]);
    }

    // ──── 共享密钥 HMAC 认证 ────

    #[test]
    fn test_raft_shared_secret_roundtrip() {
        let secret = b"test-raft-secret-16chars";
        let payload = b"raft-payload".to_vec();
        let payload_tag = compute_raft_auth_tag(&payload, secret).unwrap();
        let mut msg = make_raft_message(payload);
        msg.auth_tag = payload_tag;
        assert!(verify_raft_auth(&msg, Some(secret)).is_ok());
        // 未配置密钥时不验签（兼容明文 loopback dev/test 路径）
        assert!(verify_raft_auth(&msg, None).is_ok());
    }

    #[test]
    fn test_raft_shared_secret_rejects_tampered_payload() {
        let secret = b"test-raft-secret-16chars";
        let payload = b"raft-payload".to_vec();
        let tag = compute_raft_auth_tag(&payload, secret).unwrap();
        let mut msg = make_raft_message(b"tampered".to_vec());
        msg.auth_tag = tag;
        assert!(verify_raft_auth(&msg, Some(secret)).is_err());
    }

    #[test]
    fn test_raft_shared_secret_requires_tag() {
        let secret = b"test-raft-secret-16chars";
        // 配置了密钥但消息无标签 → 拒绝
        let msg = make_raft_message(b"x".to_vec());
        assert!(verify_raft_auth(&msg, Some(secret)).is_err());
        // 标签错误 → 拒绝
        let mut msg2 = make_raft_message(b"x".to_vec());
        msg2.auth_tag = vec![0u8; 32];
        assert!(verify_raft_auth(&msg2, Some(secret)).is_err());
    }

    // ──── 载荷编码标记（ADR-0007：唯一 postcard；bincode=0 退役）────

    /// 标记校验：仅 postcard（1）接受；退役 bincode=0 与未知值 fail-closed。
    /// 负控制：恢复 0 读腿 ⇒ 本用例必红。
    #[test]
    fn test_payload_codec_fail_closed() {
        assert!(ensure_supported_payload_codec(PAYLOAD_CODEC_POSTCARD).is_ok());
        let err = ensure_supported_payload_codec(0).unwrap_err();
        assert!(
            err.message().contains("unsupported raft payload codec"),
            "退役 bincode=0 必须显式拒绝，got: {err}"
        );
        let err = ensure_supported_payload_codec(2).unwrap_err();
        assert!(
            err.message().contains("unsupported raft payload codec"),
            "未知标记必须显式拒绝，got: {err}"
        );
    }

    /// postcard 解码往返（唯一格式）。
    /// 负控制：写/读路径去掉统一编码 ⇒ 本用例必红。
    #[test]
    fn test_payload_roundtrip() {
        #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
        struct Sample {
            id: u64,
            name: String,
        }
        let value = Sample {
            id: 7,
            name: "coord".into(),
        };

        let bytes = postcard::to_allocvec(&value).unwrap();
        let decoded: Sample = deserialize_payload(&bytes).unwrap();
        assert_eq!(decoded, value);
    }

    /// 精确消费：尾随字节 ⇒ 显式失败。
    /// 负控制：去掉 postcard remainder 断言 ⇒ 本用例必红。
    #[test]
    fn test_payload_decode_exact_consumption() {
        #[derive(Debug, serde::Serialize, serde::Deserialize)]
        struct Sample {
            id: u64,
        }
        let value = Sample { id: 1 };

        let mut bytes = postcard::to_allocvec(&value).unwrap();
        bytes.extend_from_slice(&[0xDE, 0xAD]);
        assert!(deserialize_payload::<Sample>(&bytes).is_err());
    }

    /// MAC 输入字节规范：`DOMAIN || codec_u32_be || payload`
    /// （防两侧实现漂移；负控制：去掉域分离 ⇒ 断言红）。
    #[test]
    fn test_auth_domain_separation_spec() {
        use hmac::{Hmac, Mac};
        type HmacSha256 = Hmac<sha2::Sha256>;

        let secret = b"vector-secret";
        let payload = b"payload-bytes";
        let tag = compute_raft_auth_tag(payload, secret).unwrap();

        let mut mac = HmacSha256::new_from_slice(secret).unwrap();
        mac.update(b"coord-raft-payload-v2");
        mac.update(&1u32.to_be_bytes());
        mac.update(payload);
        let expected = mac.finalize().into_bytes().to_vec();
        assert_eq!(
            tag, expected,
            "MAC 输入必须是 DOMAIN || codec_be || payload"
        );
    }

    /// 标记/载荷篡改 ⇒ 校验失败；退役 codec=0 在认证前显式拒绝。
    /// 负控制：去掉 MAC 域分离 / 恢复 0 读腿 ⇒ 对应用例必红。
    #[test]
    fn test_tamper_and_retired_codec_rejected() {
        let secret = b"test-raft-secret-16chars";
        let payload = b"raft-payload";

        // 合法 codec=1 消息（域分离输入 MAC）
        let tag1 = compute_raft_auth_tag(payload, secret).unwrap();
        let mut msg = make_raft_message(payload.to_vec());
        msg.auth_tag = tag1.clone();
        assert!(verify_raft_auth(&msg, Some(secret)).is_ok());

        // 载荷篡改 ⇒ 校验失败
        let mut tampered = make_raft_message(b"tampered".to_vec());
        tampered.auth_tag = tag1;
        assert!(verify_raft_auth(&tampered, Some(secret)).is_err());

        // 标记篡改为退役 bincode=0 ⇒ 认证前显式拒绝
        let mut retired = make_raft_message(payload.to_vec());
        retired.payload_codec = 0;
        retired.auth_tag = compute_raft_auth_tag(payload, secret).unwrap();
        assert!(
            verify_raft_auth(&retired, Some(secret)).is_err(),
            "退役 codec=0 必须显式拒绝"
        );
    }

    /// 写路径标记恒为 postcard（写侧无运行期开关）。
    #[test]
    fn test_outbound_message_carries_write_codec() {
        let msg = make_raft_message(b"x".to_vec());
        assert_eq!(msg.payload_codec, WRITE_PAYLOAD_CODEC);
        assert_eq!(msg.payload_codec, PAYLOAD_CODEC_POSTCARD);
    }

    /// 写→读完整往返：写侧产出 codec=1 + 域分离 MAC，读侧解出。
    /// 负控制：写侧回退 codec=0（bincode）⇒ 本用例标记/认证断言必红。
    #[test]
    fn test_write_path_roundtrip_with_domain_separated_mac() {
        let secret = b"test-raft-secret-16chars";
        let value = (42u64, "payload".to_string());
        let payload = serialize_payload(&value).unwrap();
        let mut msg = make_raft_message(payload);
        assert_eq!(msg.payload_codec, PAYLOAD_CODEC_POSTCARD);
        msg.auth_tag = compute_raft_auth_tag(&msg.payload, secret).unwrap();
        assert!(verify_raft_auth(&msg, Some(secret)).is_ok());
        let decoded: (u64, String) = decode_message_payload(&msg).unwrap();
        assert_eq!(decoded, value);
    }
}

#[cfg(test)]
mod region_stats_tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    struct RecordingSink {
        reports: StdMutex<Vec<crate::pd::RegionHeartbeatReport>>,
    }

    impl crate::pd::RegionHeartbeatSink for RecordingSink {
        fn apply_region_heartbeat(
            &self,
            report: crate::pd::RegionHeartbeatReport,
        ) -> coord_core::error::Result<()> {
            self.reports.lock().unwrap().push(report);
            Ok(())
        }
    }

    #[tokio::test]
    async fn report_region_stats_applies_payload_to_the_local_pd_sink() {
        let sink = Arc::new(RecordingSink {
            reports: StdMutex::new(Vec::new()),
        });
        let service = RaftRpcService::new();
        service.set_region_heartbeat_sink(Some(Arc::clone(&sink) as _));

        let payload = RegionStatsPayload {
            region_id: 7,
            size: 4096,
            keys: 42,
            storage_bytes: 1024,
            leader_node_id: 3,
            sample_keys: vec![b"a".to_vec(), b"m".to_vec(), b"z".to_vec()],
        };
        let msg = make_raft_message_for_region(serialize_payload(&payload).unwrap(), 0);
        service
            .report_region_stats(tonic::Request::new(msg))
            .await
            .expect("handler must accept an authenticated report");

        let reports = sink.reports.lock().unwrap();
        assert_eq!(reports.len(), 1);
        let report = &reports[0];
        assert_eq!(report.region_id, 7);
        assert_eq!(report.size, 4096);
        assert_eq!(report.keys, 42);
        assert_eq!(report.storage_bytes, 1024);
        assert_eq!(report.leader_node_id, 3);
        assert_eq!(
            report.sample_keys,
            vec![b"a".to_vec(), b"m".to_vec(), b"z".to_vec()]
        );
    }

    #[tokio::test]
    async fn report_region_stats_without_pd_fails_closed() {
        let service = RaftRpcService::new();
        let payload = RegionStatsPayload {
            region_id: 7,
            size: 1,
            keys: 1,
            storage_bytes: 0,
            leader_node_id: 1,
            sample_keys: Vec::new(),
        };
        let msg = make_raft_message_for_region(serialize_payload(&payload).unwrap(), 0);
        let resp = service
            .report_region_stats(tonic::Request::new(msg))
            .await
            .expect("transport-level reply");
        let reply: RegionStatsReply = decode_message_payload(&resp.into_inner()).unwrap();
        assert!(
            !reply.error.is_empty(),
            "a node without PD must reject the report instead of silently dropping it"
        );
    }
}
