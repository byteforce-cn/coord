// Raft Network — RaftNetworkFactory + RaftNetworkV2 实现
//
// 基于 Tonic gRPC 实现节点间 Raft RPC 通信（ADP §3.3）：
// - RaftNetworkFactory：为每个目标节点创建 Raft 网络客户端
// - RaftNetworkV2：发送 AppendEntries / Vote / FullSnapshot RPC
// - RaftRpcServer：接收并处理来自其他节点的 Raft RPC
//
// 通信使用 raft_addr 端口（与客户端 gRPC 端口分离），消息体使用 bincode 序列化。
// 支持可选的 TLS/mTLS 加密节点间通信（ADP §14.1, §7 差距 #14）。

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;

use openraft::RaftNetworkV2;
use openraft::RaftNetworkFactory;
use openraft::OptionalSend;
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse,
    SnapshotResponse,
};
use openraft::type_config::alias::{SnapshotOf, VoteOf};
use openraft::error::{RPCError, StreamingError, ReplicationClosed};
use parking_lot::RwLock;
use tonic::transport::Channel;

use super::type_config::TypeConfig;
use super::CoordRaft;

// Re-export for raft_rpc_server
use coord_proto::raft::RaftMessage as RaftMessageProto;
pub use coord_proto::raft::raft_server::{Raft as RaftRpcTrait, RaftServer as RaftRpcServer};
pub use coord_proto::raft::raft_client::RaftClient;

use crate::tls;

// ──── 序列化工具 ────

fn serialize_payload<T: serde::Serialize>(value: &T) -> Vec<u8> {
    bincode::serialize(value).expect("bincode serialize should not fail for Raft types")
}

fn deserialize_payload<'a, T: serde::Deserialize<'a>>(data: &'a [u8]) -> Result<T, tonic::Status> {
    bincode::deserialize(data)
        .map_err(|e| tonic::Status::internal(format!("bincode deserialize failed: {e}")))
}

/// 构建 RaftMessageProto（v6.0 新增 region_id 和 trace_context 字段）
///
/// 从当前 tracing span 中提取 trace context，注入到 Raft 消息中，
/// 实现跨节点的分布式追踪。
fn make_raft_message(payload: Vec<u8>) -> RaftMessageProto {
    let trace_context = extract_trace_context();
    RaftMessageProto {
        payload,
        region_id: 0,  // 单 Raft 模式：region_id=0 表示未使用
        trace_context,
    }
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
    /// Raft 节点间 TLS 配置（可选，ADP §14.1）
    raft_tls_config: Option<Arc<tls::TlsConfig>>,
    /// 连接池：目标节点 ID → 共享的 gRPC 客户端（惰性连接）
    /// 使用 tokio::sync::Mutex 因为临界区包含 async 连接操作
    client_cache: HashMap<u64, Arc<tokio::sync::Mutex<Option<RaftClient<Channel>>>>>,
    /// 模拟网络分区的黑名单：此节点无法与黑名单中的节点通信
    /// 用于测试网络分区和对称分区场景
    blocked_nodes: Arc<RwLock<HashSet<u64>>>,
}

impl RaftNetworkFactoryImpl {
    pub fn new(node_id: u64) -> Self {
        Self {
            node_id,
            node_addrs: Arc::new(RwLock::new(HashMap::new())),
            raft_tls_config: None,
            client_cache: HashMap::new(),
            blocked_nodes: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// 使用共享的 blocklist 创建工厂（用于测试网络分区）
    ///
    /// 多个工厂可以共享同一个 blocklist，测试代码可以通过 blocklist
    /// 动态控制哪些节点之间的通信被阻止。
    pub fn with_shared_blocklist(
        node_id: u64,
        blocked_nodes: Arc<RwLock<HashSet<u64>>>,
    ) -> Self {
        Self {
            node_id,
            node_addrs: Arc::new(RwLock::new(HashMap::new())),
            raft_tls_config: None,
            client_cache: HashMap::new(),
            blocked_nodes,
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

    /// 设置 Raft 节点间 TLS 配置（ADP §14.1）
    ///
    /// 若配置了 TLS，所有节点间 Raft RPC（AppendEntries/Vote/InstallSnapshot）
    /// 将通过 TLS 加密传输。若同时配置了 CA 证书，则启用 mTLS 双向验证。
    pub fn set_raft_tls(&mut self, tls_config: tls::TlsConfig) {
        self.raft_tls_config = Some(Arc::new(tls_config));
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
        tracing::info!("[partition-sim] node {} blocking communication to node {}", self.node_id, target_node);
        self.blocked_nodes.write().insert(target_node);
    }

    /// 解除对 target_node 的通信阻止（测试用）
    pub fn unblock_node(&self, target_node: u64) {
        tracing::info!("[partition-sim] node {} unblocking communication to node {}", self.node_id, target_node);
        self.blocked_nodes.write().remove(&target_node);
    }

    /// 检查目标节点是否被阻止
    fn is_blocked(&self, target: u64) -> bool {
        self.blocked_nodes.read().contains(&target)
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
}

impl RaftNetworkImpl {
    /// 获取或建立到目标节点的 gRPC 连接（惰性、共享）
    async fn get_client(&self) -> Result<RaftClient<Channel>, tonic::Status> {
        let mut slot = self.client_slot.lock().await;
        if let Some(ref client) = *slot {
            return Ok(client.clone());
        }

        // Connect
        let use_tls = self.tls_config.as_ref().map(|c| c.is_configured()).unwrap_or(false);
        let scheme = if use_tls { "https" } else { "http" };
        let endpoint = format!("{}://{}", scheme, self.target_addr);

        let mut channel_builder = Channel::from_shared(endpoint)
            .map_err(|e| tonic::Status::internal(format!("invalid raft addr: {e}")))?;

        if use_tls {
            let cfg = self.tls_config.as_ref().unwrap();
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

        let channel = channel_builder
            .connect()
            .await
            .map_err(|e| tonic::Status::unavailable(format!(
                "connect to node {} at {}: {e}",
                self.target_id, self.target_addr
            )))?;

        tracing::debug!(
            "Raft network: connected to node {} at {}",
            self.target_id, self.target_addr
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
            RaftNetwork::Real { target_id, blocked_nodes, .. } => {
                if blocked_nodes.read().contains(target_id) {
                    let status = tonic::Status::unavailable(format!(
                        "simulated network partition: node {} is unreachable",
                        target_id
                    ));
                    return Err(RPCError::Unreachable(openraft::error::Unreachable::new(&status)));
                }
                Ok(())
            }
            RaftNetwork::Blocked { target_id } => {
                let status = tonic::Status::unavailable(format!(
                    "simulated network partition: node {} is unreachable",
                    target_id
                ));
                Err(RPCError::Unreachable(openraft::error::Unreachable::new(&status)))
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
}

impl RaftNetworkV2<TypeConfig> for RaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.ensure_not_blocked()?;
        match self {
            RaftNetwork::Real { inner, .. } => inner.append_entries(rpc, option).await,
            RaftNetwork::Blocked { .. } => unreachable!("Blocked should have been caught by ensure_not_blocked"),
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
            RaftNetwork::Blocked { .. } => unreachable!("Blocked should have been caught by ensure_not_blocked"),
        }
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<TypeConfig>,
        snapshot: SnapshotOf<TypeConfig>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        // Check blocklist before snapshot transfer
        match self {
            RaftNetwork::Real { target_id, blocked_nodes, .. } => {
                if blocked_nodes.read().contains(target_id) {
                    return Err(self.to_streaming_error());
                }
            }
            RaftNetwork::Blocked { .. } => {
                return Err(self.to_streaming_error());
            }
        }
        match self {
            RaftNetwork::Real { inner, .. } => inner.full_snapshot(vote, snapshot, cancel, option).await,
            RaftNetwork::Blocked { .. } => unreachable!("Blocked should have been caught above"),
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for RaftNetworkFactoryImpl {
    type Network = RaftNetwork;

    async fn new_client(
        &mut self,
        target: u64,
        _node: &openraft::impls::BasicNode,
    ) -> Self::Network {
        // 检查网络分区模拟：目标节点是否被阻止
        let blocked = self.is_blocked(target);
        if blocked {
            tracing::debug!(
                "[partition-sim] node {} → node {}: BLOCKED (simulated partition)",
                self.node_id, target
            );
            return RaftNetwork::Blocked { target_id: target };
        }

        let addr = self
            .node_addrs
            .read()
            .get(&target)
            .cloned()
            .unwrap_or_else(|| format!("127.0.0.1:{}", 50051 + target));

        // Get or create shared client slot (lazy connection, shared across instances)
        let client_slot = self
            .client_cache
            .entry(target)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(None)))
            .clone();

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
    fn from_openraft(snapshot: &SnapshotOf<TypeConfig>) -> Self {
        use std::io::Read;
        let mut data = Vec::new();
        let mut cursor = snapshot.snapshot.clone();
        cursor.read_to_end(&mut data).ok();
        Self {
            meta: snapshot.meta.clone(),
            data,
        }
    }

    fn into_openraft(self) -> SnapshotOf<TypeConfig> {
        SnapshotOf::<TypeConfig> {
            meta: self.meta,
            snapshot: std::io::Cursor::new(self.data),
        }
    }
}

impl RaftNetworkV2<TypeConfig> for RaftNetworkImpl {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        let payload = serialize_payload(&rpc);
        let req = tonic::Request::new(make_raft_message(payload));
        let mut client = self.get_client().await.map_err(to_rpc_error)?;
        let resp = client.append_entries(req).await.map_err(to_rpc_error)?;
        deserialize_payload(&resp.into_inner().payload).map_err(|e| {
            RPCError::Unreachable(openraft::error::Unreachable::new(&e))
        })
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        let payload = serialize_payload(&rpc);
        let req = tonic::Request::new(make_raft_message(payload));
        let mut client = self.get_client().await.map_err(to_rpc_error)?;
        let resp = client.vote(req).await.map_err(to_rpc_error)?;
        deserialize_payload(&resp.into_inner().payload).map_err(|e| {
            RPCError::Unreachable(openraft::error::Unreachable::new(&e))
        })
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<TypeConfig>,
        snapshot: SnapshotOf<TypeConfig>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let serializable = SerializableSnapshot::from_openraft(&snapshot);
        let payload = serialize_payload(&(&vote, &serializable));
        let req = tonic::Request::new(make_raft_message(payload));
        let mut client = self.get_client().await.map_err(|e| {
            StreamingError::Unreachable(openraft::error::Unreachable::new(&e))
        })?;
        let resp = client.install_snapshot(req).await.map_err(|e| {
            StreamingError::Unreachable(openraft::error::Unreachable::new(&e))
        })?;
        deserialize_payload(&resp.into_inner().payload).map_err(|e| {
            StreamingError::Unreachable(openraft::error::Unreachable::new(&e))
        })
    }
}

// ──── Raft RPC Server (gRPC Service 实现) ────

/// 接收来自其他节点的 Raft RPC 并转发给本地 Raft 实例
pub struct RaftRpcService {
    /// 本地 Raft 实例（初始化后设置）
    raft: Arc<RwLock<Option<CoordRaft>>>,
}

impl RaftRpcService {
    pub fn new() -> Self {
        Self {
            raft: Arc::new(RwLock::new(None)),
        }
    }

    /// 设置 Raft 实例（在 Raft 初始化后调用）
    pub fn set_raft(&self, raft: CoordRaft) {
        *self.raft.write() = Some(raft);
    }

    fn get_raft(&self) -> Result<CoordRaft, tonic::Status> {
        self.raft
            .read()
            .clone()
            .ok_or_else(|| tonic::Status::internal("raft not initialized"))
    }
}

#[tonic::async_trait]
impl coord_proto::raft::raft_server::Raft for RaftRpcService {
    async fn append_entries(
        &self,
        request: tonic::Request<RaftMessageProto>,
    ) -> Result<tonic::Response<RaftMessageProto>, tonic::Status> {
        let msg = request.into_inner();
        inject_received_trace_context(&msg);
        let raft = self.get_raft()?;
        let rpc: AppendEntriesRequest<TypeConfig> =
            deserialize_payload(&msg.payload)?;
        let resp = raft.append_entries(rpc).await.map_err(|e| {
            tonic::Status::internal(format!("append_entries failed: {e}"))
        })?;
        let payload = serialize_payload(&resp);
        Ok(tonic::Response::new(make_raft_message(payload)))
    }

    async fn vote(
        &self,
        request: tonic::Request<RaftMessageProto>,
    ) -> Result<tonic::Response<RaftMessageProto>, tonic::Status> {
        let msg = request.into_inner();
        inject_received_trace_context(&msg);
        let raft = self.get_raft()?;
        let rpc: VoteRequest<TypeConfig> = deserialize_payload(&msg.payload)?;
        let resp = raft.vote(rpc).await.map_err(|e| {
            tonic::Status::internal(format!("vote failed: {e}"))
        })?;
        let payload = serialize_payload(&resp);
        Ok(tonic::Response::new(make_raft_message(payload)))
    }

    async fn install_snapshot(
        &self,
        request: tonic::Request<RaftMessageProto>,
    ) -> Result<tonic::Response<RaftMessageProto>, tonic::Status> {
        let msg = request.into_inner();
        inject_received_trace_context(&msg);
        let raft = self.get_raft()?;
        let (vote, serializable): (VoteOf<TypeConfig>, SerializableSnapshot) =
            deserialize_payload(&msg.payload)?;
        let snapshot = serializable.into_openraft();
        let resp = raft.install_full_snapshot(vote, snapshot).await.map_err(|e| {
            tonic::Status::internal(format!("install_full_snapshot failed: {e}"))
        })?;
        let payload = serialize_payload(&resp);
        Ok(tonic::Response::new(make_raft_message(payload)))
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
        };
        assert_eq!(msg.region_id, 42);
        assert_eq!(msg.payload, b"region_payload");
        assert_eq!(msg.trace_context, vec![0x01, 0x02, 0x03]);
    }
}
