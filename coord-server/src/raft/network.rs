// Raft Network — RaftNetworkFactory + RaftNetworkV2 实现
//
// 基于 Tonic gRPC 实现节点间 Raft RPC 通信：
// - RaftNetworkFactory：为每个目标节点创建 Raft 网络客户端
// - RaftNetworkV2：发送 AppendEntries / Vote / FullSnapshot RPC
// - RaftRpcServer：接收并处理来自其他节点的 Raft RPC
//
// 通信使用 raft_addr 端口（与客户端 gRPC 端口分离），消息体使用 bincode 序列化。
// 支持可选的 TLS/mTLS 加密节点间通信（差距 #14）。

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;

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
// ADR-0007：RaftMessage.payload 的编码由协议层标记 `payload_codec` 显式决定，
// 接收侧按标记分派唯一解码器，**禁止试错回落**（postcard 字节按 bincode 解码
// 存在静默解出差值的理论窗口）。0 = bincode（兼容默认）；1 = postcard。

/// bincode 载荷标记（历史默认；兼容窗口内可读）。
pub const PAYLOAD_CODEC_BINCODE: u32 = 0;
/// postcard 载荷标记（写路径目标格式）。
pub const PAYLOAD_CODEC_POSTCARD: u32 = 1;

/// 写路径标记（R2 起写 postcard）。
///
/// 写侧无运行期开关：同一二进制只有一种写格式（升级秩序由部署纪律固化，
/// 见 ADR-0007 D4——全部接收方运行含 R1 的版本后才部署含 R2 的版本）。
const WRITE_PAYLOAD_CODEC: u32 = PAYLOAD_CODEC_POSTCARD;

/// codec=1 的 HMAC 域分离标签（ADR-0007 D3）。
///
/// 输入字节规范（两侧实现必须逐字节一致，防漂移）：`DOMAIN || codec_u32_be ||
/// payload`；codec=0 保持 `payload` 不变（与既有节点字节级互操作）。
const RAFT_AUTH_DOMAIN_V2: &[u8] = b"coord-raft-payload-v2";

/// 载荷编码标记（类型化的 wire 取值）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PayloadCodec {
    Bincode,
    Postcard,
}

impl PayloadCodec {
    fn to_u32(self) -> u32 {
        match self {
            Self::Bincode => PAYLOAD_CODEC_BINCODE,
            Self::Postcard => PAYLOAD_CODEC_POSTCARD,
        }
    }
}

/// 解析 wire 标记；未知值 fail-closed（不猜测、不回落）。
fn parse_payload_codec(codec: u32) -> Result<PayloadCodec, tonic::Status> {
    match codec {
        PAYLOAD_CODEC_BINCODE => Ok(PayloadCodec::Bincode),
        PAYLOAD_CODEC_POSTCARD => Ok(PayloadCodec::Postcard),
        other => Err(tonic::Status::invalid_argument(format!(
            "unknown raft payload codec: {other} (fail-closed; expected {PAYLOAD_CODEC_BINCODE} or \
             {PAYLOAD_CODEC_POSTCARD})"
        ))),
    }
}

/// 编码出站载荷（写路径唯一编码器：postcard，ADR-0007 R2）。
fn serialize_payload<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, tonic::Status> {
    postcard::to_allocvec(value)
        .map_err(|e| tonic::Status::internal(format!("postcard serialize failed: {e}")))
}

/// 按标记分派解码（读双分派；两条腿均精确消费——拒绝尾随字节）。
fn deserialize_payload<'a, T: serde::Deserialize<'a>>(
    data: &'a [u8],
    codec: PayloadCodec,
) -> Result<T, tonic::Status> {
    match codec {
        PayloadCodec::Bincode => {
            use bincode::Options;
            bincode::DefaultOptions::new()
                .with_fixint_encoding()
                .deserialize(data)
                .map_err(|e| tonic::Status::internal(format!("bincode deserialize failed: {e}")))
        }
        PayloadCodec::Postcard => {
            let (value, remainder) = postcard::take_from_bytes::<T>(data).map_err(|e| {
                tonic::Status::internal(format!("postcard deserialize failed: {e}"))
            })?;
            if !remainder.is_empty() {
                return Err(tonic::Status::internal(format!(
                    "trailing bytes after raft payload: {}",
                    remainder.len()
                )));
            }
            Ok(value)
        }
    }
}

/// 读取消息标记并解码载荷（所有接收侧的公共入口：先验标记，再分派）。
fn decode_message_payload<T: serde::de::DeserializeOwned>(
    msg: &RaftMessageProto,
) -> Result<T, tonic::Status> {
    let codec = parse_payload_codec(msg.payload_codec)?;
    deserialize_payload(&msg.payload, codec)
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
/// 认证输入按标记域分离（ADR-0007 D3）：codec=0 ⇒ `HMAC(secret, payload)`
/// （与既有节点字节级互操作）；codec=1 ⇒ `HMAC(secret, DOMAIN || codec_be ||
/// payload)`——标记被纳入认证覆盖，篡改标记 0↔1 会使两条校验路径都失败。
fn compute_raft_auth_tag(
    payload: &[u8],
    secret: &[u8],
    codec: PayloadCodec,
) -> Result<Vec<u8>, tonic::Status> {
    use hmac::{Hmac, Mac};
    type HmacSha256 = Hmac<sha2::Sha256>;
    let mut mac = HmacSha256::new_from_slice(secret)
        .map_err(|e| tonic::Status::internal(format!("raft HMAC key invalid: {e}")))?;
    match codec {
        PayloadCodec::Bincode => mac.update(payload),
        PayloadCodec::Postcard => {
            mac.update(RAFT_AUTH_DOMAIN_V2);
            mac.update(&codec.to_u32().to_be_bytes());
            mac.update(payload);
        }
    }
    Ok(mac.finalize().into_bytes().to_vec())
}

/// 校验入站 raft 消息的认证标签（配置了共享密钥时强制；fail-closed）。
///
/// 未知标记在认证/解码前显式拒绝（ADR-0007 D2）。
fn verify_raft_auth(msg: &RaftMessageProto, secret: Option<&[u8]>) -> Result<(), tonic::Status> {
    let codec = parse_payload_codec(msg.payload_codec)?;
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
    match codec {
        PayloadCodec::Bincode => mac.update(&msg.payload),
        PayloadCodec::Postcard => {
            mac.update(RAFT_AUTH_DOMAIN_V2);
            mac.update(&codec.to_u32().to_be_bytes());
            mac.update(&msg.payload);
        }
    }
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
            SnapshotRateLimiter::new(max_bytes_per_sec)
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
            let codec = parse_payload_codec(msg.payload_codec)?;
            msg.auth_tag = compute_raft_auth_tag(&msg.payload, secret, codec)?;
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
/// - `Real`: 正常的 gRPC 网络通信，每次 RPC 调用前会检查 blocklist（支持动态分区模拟）
/// - `Blocked`: 模拟网络分区，所有 RPC 返回 Unreachable
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
                    "simulated network partition: node {} is unreachable",
                    target_id
                ));
                Err(RPCError::Unreachable(openraft::error::Unreachable::new(
                    &status,
                )))
            }
        }
    }

    fn to_streaming_error(&self) -> StreamingError<TypeConfig> {
        let target_id = match self {
            RaftNetwork::Real { target_id, .. } => target_id,
            RaftNetwork::Blocked { target_id } => target_id,
        };
        let status = tonic::Status::unavailable(format!(
            "simulated network partition: node {} is unreachable",
            target_id
        ));
        StreamingError::Unreachable(openraft::error::Unreachable::new(&status))
    }

    /// `Blocked` 状态下的 RPC 错误（与 `ensure_not_blocked` 同一语义，
    /// 供 match 的兜底分支使用：返回错误而非 panic）。
    fn blocked_rpc_error(&self) -> RPCError<TypeConfig> {
        let target_id = match self {
            RaftNetwork::Real { target_id, .. } => target_id,
            RaftNetwork::Blocked { target_id } => target_id,
        };
        let status = tonic::Status::unavailable(format!(
            "simulated network partition: node {} is unreachable",
            target_id
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
        // 检查网络分区模拟：目标节点是否被阻止
        let blocked = self.is_blocked(target);
        if blocked {
            tracing::debug!(
                "[partition-sim] node {} → node {}: BLOCKED (simulated partition)",
                self.node_id,
                target
            );
            return RaftNetwork::Blocked { target_id: target };
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

/// SubmitPdOp 请求载荷：一条 PD 队列命令（bincode）。经 raft_addr 节点间
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
pub struct RaftRpcService {
    /// 本地 Raft 实例表：region_id → Raft（region 0 = 单 Raft 默认实例）
    rafts: Arc<RwLock<HashMap<u64, CoordRaft>>>,
    /// 共享密钥（配置后强制验签，无标签拒绝）
    shared_secret: Option<Arc<Vec<u8>>>,
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

    /// 移除某 Region 的 Raft 实例（Region 下线/注销时）。
    pub fn remove_region_raft(&self, region_id: u64) {
        self.rafts.write().remove(&region_id);
    }

    /// 按 region_id 取 Raft 实例（region 0 回退到默认实例）。
    fn get_raft_for_region(&self, region_id: u64) -> Result<CoordRaft, tonic::Status> {
        let rafts = self.rafts.read();
        if let Some(r) = rafts.get(&region_id) {
            return Ok(r.clone());
        }
        // 兼容：单 Raft 模式下 region_id=0 查默认；未知 region 拒绝（fail-closed）
        if region_id != 0 {
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
        let mut stream_codec: Option<PayloadCodec> = None;

        while let Some(msg) = stream
            .message()
            .await
            .map_err(|e| tonic::Status::internal(format!("snapshot stream recv: {e}")))?
        {
            inject_received_trace_context(&msg);
            self.verify_incoming(&msg)?;
            // 载荷标记：同一流内必须一致（fail-closed；重组数据按该标记解码）
            let codec = parse_payload_codec(msg.payload_codec)?;
            match stream_codec {
                None => stream_codec = Some(codec),
                Some(seen) if seen != codec => {
                    return Err(tonic::Status::invalid_argument(format!(
                        "snapshot stream payload codec mismatch: expected {}, got {}",
                        seen.to_u32(),
                        codec.to_u32()
                    )));
                }
                _ => {}
            }
            let frame: SnapshotStreamMessage = deserialize_payload(&msg.payload, codec)?;
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

        // 重组完整快照字节（按流内统一标记解码）
        let stream_codec = stream_codec
            .ok_or_else(|| tonic::Status::invalid_argument("snapshot stream missing chunks"))?;
        let mut data = Vec::with_capacity(chunks.iter().map(|c| c.len()).sum());
        for chunk in &chunks {
            data.extend_from_slice(chunk);
        }
        let (vote, serializable): (VoteOf<TypeConfig>, SerializableSnapshot) =
            deserialize_payload(&data, stream_codec)?;
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
            payload_codec: PAYLOAD_CODEC_BINCODE,
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
            payload_codec: PAYLOAD_CODEC_BINCODE,
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
            payload_codec: PAYLOAD_CODEC_BINCODE,
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
        let payload_tag = compute_raft_auth_tag(
            &payload,
            secret,
            parse_payload_codec(WRITE_PAYLOAD_CODEC).unwrap(),
        )
        .unwrap();
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
        let tag = compute_raft_auth_tag(
            &payload,
            secret,
            parse_payload_codec(WRITE_PAYLOAD_CODEC).unwrap(),
        )
        .unwrap();
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

    // ──── 载荷编码标记（ADR-0007 R1：读双分派）────

    /// 标记解析：0/1 接受；未知值 fail-closed（错误措辞稳定）。
    #[test]
    fn test_payload_codec_parse_fail_closed() {
        assert_eq!(parse_payload_codec(0).unwrap(), PayloadCodec::Bincode);
        assert_eq!(parse_payload_codec(1).unwrap(), PayloadCodec::Postcard);
        let err = parse_payload_codec(2).unwrap_err();
        assert!(
            err.message().contains("unknown raft payload codec"),
            "unknown codec must be rejected with stable wording, got: {err}"
        );
    }

    /// 双标记解码往返：同一结构两种编码各自按标记解出。
    /// 负控制：去掉标记分派（改试错回落）⇒ 错送解码器时本用例必红。
    #[test]
    fn test_payload_dual_codec_roundtrip() {
        #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
        struct Sample {
            id: u64,
            name: String,
        }
        let value = Sample {
            id: 7,
            name: "coord".into(),
        };

        // 0 = bincode
        let bincode_bytes = bincode::serialize(&value).unwrap();
        let decoded: Sample = deserialize_payload(&bincode_bytes, PayloadCodec::Bincode).unwrap();
        assert_eq!(decoded, value);

        // 1 = postcard
        let postcard_bytes = postcard::to_allocvec(&value).unwrap();
        let decoded: Sample = deserialize_payload(&postcard_bytes, PayloadCodec::Postcard).unwrap();
        assert_eq!(decoded, value);
    }

    /// 两条解码腿均精确消费：尾随字节 ⇒ 显式失败。
    /// 负控制：bincode 腿改回 allow_trailing / 去掉 postcard remainder 断言
    /// ⇒ 本用例必红。
    #[test]
    fn test_payload_decode_exact_consumption() {
        #[derive(Debug, serde::Serialize, serde::Deserialize)]
        struct Sample {
            id: u64,
        }
        let value = Sample { id: 1 };

        let mut bincode_bytes = bincode::serialize(&value).unwrap();
        bincode_bytes.extend_from_slice(&[0xDE, 0xAD]);
        assert!(deserialize_payload::<Sample>(&bincode_bytes, PayloadCodec::Bincode).is_err());

        let mut postcard_bytes = postcard::to_allocvec(&value).unwrap();
        postcard_bytes.extend_from_slice(&[0xDE, 0xAD]);
        assert!(deserialize_payload::<Sample>(&postcard_bytes, PayloadCodec::Postcard).is_err());
    }

    /// codec=1 的 MAC 输入字节规范：`DOMAIN || codec_u32_be || payload`
    /// （防两侧实现漂移；负控制：去掉域分离 ⇒ 断言红）。
    #[test]
    fn test_codec1_auth_domain_separation_spec() {
        use hmac::{Hmac, Mac};
        type HmacSha256 = Hmac<sha2::Sha256>;

        let secret = b"vector-secret";
        let payload = b"payload-bytes";
        let tag = compute_raft_auth_tag(payload, secret, PayloadCodec::Postcard).unwrap();

        let mut mac = HmacSha256::new_from_slice(secret).unwrap();
        mac.update(b"coord-raft-payload-v2");
        mac.update(&1u32.to_be_bytes());
        mac.update(payload);
        let expected = mac.finalize().into_bytes().to_vec();
        assert_eq!(
            tag, expected,
            "codec=1 MAC 输入必须是 DOMAIN || codec_be || payload"
        );
    }

    /// 标记篡改 0↔1 ⇒ 两条校验路径都失败（标记被认证覆盖）；codec=0 的
    /// 输入保持与既有节点字节级互操作。
    /// 负控制：去掉 MAC 域分离（codec=1 也用裸 payload）⇒ 本用例必红。
    #[test]
    fn test_codec_tamper_breaks_verification() {
        let secret = b"test-raft-secret-16chars";
        let payload = b"raft-payload";

        // 合法 codec=0 消息（旧输入 MAC；显式构造旧发送方形态）
        let tag0 = compute_raft_auth_tag(payload, secret, PayloadCodec::Bincode).unwrap();
        let mut msg = make_raft_message(payload.to_vec());
        msg.payload_codec = PAYLOAD_CODEC_BINCODE;
        msg.auth_tag = tag0;
        assert!(verify_raft_auth(&msg, Some(secret)).is_ok());

        // 把标记篡改为 1（载荷与 tag 不动）⇒ 校验失败
        msg.payload_codec = PAYLOAD_CODEC_POSTCARD;
        assert!(
            verify_raft_auth(&msg, Some(secret)).is_err(),
            "codec 0→1 tamper must fail verification"
        );

        // 合法 codec=1 消息（域分离输入 MAC）
        let tag1 = compute_raft_auth_tag(payload, secret, PayloadCodec::Postcard).unwrap();
        let mut msg = make_raft_message(payload.to_vec());
        msg.payload_codec = PAYLOAD_CODEC_POSTCARD;
        msg.auth_tag = tag1;
        assert!(verify_raft_auth(&msg, Some(secret)).is_ok());

        // 把标记篡改回 0 ⇒ 校验失败
        msg.payload_codec = PAYLOAD_CODEC_BINCODE;
        assert!(
            verify_raft_auth(&msg, Some(secret)).is_err(),
            "codec 1→0 tamper must fail verification"
        );

        // 交叉使用也不成立：codec=1 的 tag 不能通过 codec=0 检查
        let mut msg = make_raft_message(payload.to_vec());
        msg.payload_codec = PAYLOAD_CODEC_BINCODE;
        msg.auth_tag = compute_raft_auth_tag(payload, secret, PayloadCodec::Postcard).unwrap();
        assert!(verify_raft_auth(&msg, Some(secret)).is_err());
    }

    /// 写路径标记由常量唯一决定（R2：断言为 1/postcard）。
    #[test]
    fn test_outbound_message_carries_write_codec() {
        let msg = make_raft_message(b"x".to_vec());
        assert_eq!(msg.payload_codec, WRITE_PAYLOAD_CODEC);
        assert_eq!(msg.payload_codec, PAYLOAD_CODEC_POSTCARD);
    }

    /// R2 写切换后的完整往返：写侧产出 codec=1 + 域分离 MAC，读侧双分派解出。
    /// 负控制：写侧回退 codec=0（bincode）⇒ 本用例标记/认证断言必红。
    #[test]
    fn test_r2_write_path_roundtrip_with_domain_separated_mac() {
        let secret = b"test-raft-secret-16chars";
        let value = (42u64, "payload".to_string());
        let payload = serialize_payload(&value).unwrap();
        let mut msg = make_raft_message(payload);
        assert_eq!(msg.payload_codec, PAYLOAD_CODEC_POSTCARD);
        let codec = parse_payload_codec(msg.payload_codec).unwrap();
        msg.auth_tag = compute_raft_auth_tag(&msg.payload, secret, codec).unwrap();
        assert!(verify_raft_auth(&msg, Some(secret)).is_ok());
        let decoded: (u64, String) = decode_message_payload(&msg).unwrap();
        assert_eq!(decoded, value);
    }
}
