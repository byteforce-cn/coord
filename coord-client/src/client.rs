// coord-client: 主客户端
//
// 封装 gRPC 连接管理、Leader 发现、重试逻辑，提供类型安全的 KV/Lease/Watch/Txn API。
// 定义完整的 Client SDK 行为。

use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

use coord_core::error::{Error, Result};
use coord_proto::kv::{kv_client::KvClient as KvStub, DeleteRequest, PutRequest, RangeRequest};
use coord_proto::lease::{
    lease_client::LeaseClient as LeaseStub, LeaseGrantRequest, LeaseKeepAliveRequest,
    LeaseRevokeRequest,
};
use coord_proto::maintenance::{
    maintenance_client::MaintenanceClient as MaintenanceStub, MemberListRequest, SealRequest,
    StatusRequest, StatusResponse, UnsealRequest, UnsealResponse,
};
use coord_proto::storage::storage_client::StorageClient as StorageStub;
use coord_proto::storage::{
    get_response, put_request, DeleteRequest as StorageDeleteRequest,
    GetRequest as StorageGetRequest, GetResponse as StorageGetResponse, ObjectStat, PutMeta,
    PutRequest as StoragePutRequest, StatRequest as StorageStatRequest,
};
use coord_proto::txn::{
    txn_client::TxnClient as TxnStub, Compare, RequestOp, TxnRequest, TxnResponse,
};
use coord_proto::watch::{
    watch_client::WatchClient as WatchStub, WatchCreateRequest, WatchEvent, WatchRequest,
};

use crate::config::Config;
use crate::credential::AuthedChannel;
use crate::leader::LeaderDiscovery;
use crate::pool::ConnectionPool;
use crate::retry::{classify_error, RetryDecision, RetryState};

/// R-SVC-08：服务端 follower 返回 leader 地址 hint 的 gRPC metadata key
/// （与 coord-server::server::LEADER_HINT_METADATA_KEY 保持一致）
pub const LEADER_HINT_METADATA_KEY: &str = "coord-leader-hint";

// ──── Error conversion ────

/// Convert tonic::Status to coord_core::Error
pub(crate) fn from_status(status: tonic::Status) -> Error {
    let msg = status.message().to_string();
    match status.code() {
        tonic::Code::NotFound => Error::NotFound {
            resource: "key",
            key: msg,
        },
        tonic::Code::PermissionDenied => Error::PermissionDenied(msg),
        tonic::Code::Unauthenticated => Error::Unauthenticated(msg),
        tonic::Code::Unavailable => Error::ClusterUnavailable(msg),
        tonic::Code::DeadlineExceeded => Error::RequestTimeout,
        tonic::Code::InvalidArgument => Error::InvalidArgument(msg),
        tonic::Code::AlreadyExists => Error::AlreadyExists {
            resource: "resource",
            key: msg,
        },
        _ => Error::Internal(msg),
    }
}

/// R-SVC-08：按 gRPC 状态码分类错误，决定是否重试
fn classify_tonic(status: &tonic::Status) -> RetryDecision {
    let msg = status.message().to_lowercase();
    if msg.contains("sealed") || msg.contains("unsealing") {
        return RetryDecision::Abort;
    }
    match status.code() {
        // follower 重定向（not leader）与连接级不可用：立即重试（换 leader）
        tonic::Code::Unavailable => RetryDecision::RetryImmediately,
        // 失去 quorum 的写超时：指数退避后重试
        tonic::Code::DeadlineExceeded => {
            RetryDecision::RetryAfter(std::time::Duration::from_millis(200))
        }
        // 不可恢复错误
        tonic::Code::NotFound
        | tonic::Code::PermissionDenied
        | tonic::Code::Unauthenticated
        | tonic::Code::InvalidArgument
        | tonic::Code::AlreadyExists => RetryDecision::Abort,
        // 未知错误：谨慎退避重试一次
        _ => RetryDecision::RetryAfter(std::time::Duration::from_millis(100)),
    }
}

/// Coord 分布式协调服务客户端。
///
/// # 线程安全
/// `Client` 内部使用 `Arc`，可安全地在多线程间共享和克隆。
///
/// # 生命周期
/// ```ignore
/// let client = Client::new(Config::new(vec!["127.0.0.1:50051".into()]))?;
/// let kv = client.kv();
/// kv.put(b"/key", b"value").await?;
/// ```
#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

struct ClientInner {
    config: Config,
    leader: LeaderDiscovery,
    /// gRPC connection pool
    pool: ConnectionPool,
}

impl Client {
    /// Agent 模式：连接本地 Agent（Java 应用推荐路径的 Rust 等价）
    ///
    /// 单连接，无需 Leader 发现、连接池、RouteCache。
    /// Agent 已处理 Leader 发现和请求路由，对应用完全透明。
    pub async fn connect_via_agent(agent_addr: impl Into<String>) -> Result<Self> {
        let addr = agent_addr.into();
        let endpoint_url = format!("http://{addr}");
        let channel = Channel::from_shared(endpoint_url)
            .map_err(|e| Error::InvalidArgument(format!("invalid agent address {addr}: {e}")))?
            .connect()
            .await
            .map_err(|e| Error::Internal(format!("failed to connect to agent at {addr}: {e}")))?;

        // Agent 模式：使用虚拟配置（仅用于日志/调试）
        let config = Config::new(vec![addr.clone()]);
        Self::new_with_channel(config, channel)
    }

    /// Direct 模式：直连 Server 集群（测试、运维、Rust 原生服务）
    ///
    /// 完整的 Leader 发现 + 连接池 + 重试 + 路由缓存。
    pub async fn connect_direct(config: Config) -> Result<Self> {
        Self::new(config).await
    }

    /// 创建新客户端并建立到所有端点的连接。
    ///
    /// 向后兼容别名，等同于 `connect_direct`。
    pub async fn new(config: Config) -> Result<Self> {
        let endpoints = config.endpoints.clone();
        let leader = LeaderDiscovery::new(endpoints);
        let pool = ConnectionPool::new(&config);

        let client = Self {
            inner: Arc::new(ClientInner {
                config,
                leader,
                pool,
            }),
        };

        // 初始 Leader 发现：尝试连接所有端点
        let _ = client.discover_leader().await;

        Ok(client)
    }

    /// 从单个预建 Channel 创建客户端（Agent 模式内部使用）
    fn new_with_channel(config: Config, channel: Channel) -> Result<Self> {
        // 在 Agent 模式下，将 Channel 注册到连接池中以保持一致性
        let pool = ConnectionPool::new(&config);
        // 将 channel 放入池中以便后续使用（包装出站凭据拦截器）
        let endpoint = config
            .endpoints
            .first()
            .cloned()
            .unwrap_or_else(|| "unknown".into());
        pool.put(&endpoint, pool.wrap(channel));

        let leader = LeaderDiscovery::new(config.endpoints.clone());
        // Agent 模式下，将唯一端点设为 Leader
        leader.set_leader(endpoint);

        Ok(Self {
            inner: Arc::new(ClientInner {
                config,
                leader,
                pool,
            }),
        })
    }

    /// 返回 KV 客户端（键值操作）
    pub fn kv(&self) -> KvClient {
        KvClient::new(self.clone())
    }

    /// 返回 Lease 客户端（租约管理）
    pub fn lease(&self) -> LeaseClient {
        LeaseClient::new(self.clone())
    }

    /// 返回 Watch 客户端（变更监听）
    pub fn watch(&self) -> WatchClient {
        WatchClient::new(self.clone())
    }

    /// 返回 Txn 客户端（原子事务）
    pub fn txn(&self) -> TxnClient {
        TxnClient::new(self.clone())
    }

    /// 返回 Maintenance 客户端（运维操作：Seal/Unseal/Status/Snapshot）
    pub fn maintenance(&self) -> MaintenanceClient {
        MaintenanceClient::new(self.clone())
    }

    /// 返回对象存储客户端（coord.storage；EXPERIMENTAL）
    pub fn storage(&self) -> StorageClient {
        StorageClient::new(self.clone())
    }

    /// 返回 Auth 客户端（认证 / refresh / 用户角色能力管理）
    pub fn auth(&self) -> crate::auth::AuthClient {
        crate::auth::AuthClient::new(self.clone())
    }

    // ──── 内部方法 ────

    /// 获取当前 Leader 地址
    pub(crate) async fn leader_addr(&self) -> Result<String> {
        match self.inner.leader.get_leader() {
            Some(addr) => Ok(addr),
            None => self.discover_leader().await,
        }
    }

    /// 获取到当前 Leader 的 gRPC Channel 和端点地址。
    /// 从连接池中获取复用的连接。
    pub(crate) async fn get_leader_channel(&self) -> Result<(String, AuthedChannel)> {
        let leader_addr = self.leader_addr().await?;
        let channel = self.inner.pool.get(&leader_addr).await?;
        Ok((leader_addr, channel))
    }

    /// 将 Channel 归还到连接池（供子客户端使用后调用）
    pub(crate) fn return_channel(&self, endpoint: &str, channel: AuthedChannel) {
        self.inner.pool.put(endpoint, channel);
    }

    /// 获取到当前 Leader 的 Watch 专用 Channel 和端点地址
    pub(crate) async fn get_leader_watch_channel(&self) -> Result<(String, AuthedChannel)> {
        let leader_addr = self.leader_addr().await?;
        let channel = self.inner.pool.get_watch(&leader_addr).await?;
        Ok((leader_addr, channel))
    }

    /// Leader 发现：轮询所有端点，通过 Status RPC 检测 Leader 节点。
    async fn discover_leader(&self) -> Result<String> {
        let endpoints = self.inner.leader.endpoints();
        for _ in 0..endpoints.len() {
            let endpoint = match self.inner.leader.next_endpoint() {
                Some(ep) => ep,
                None => break,
            };

            // TLS/mTLS 感知的通道构建（Config.tls 为 Some 时走 https）
            let channel = match crate::tls::connect(
                &endpoint,
                Some(self.inner.config.connect_timeout),
                self.inner.config.tls.as_ref(),
            )
            .await
            {
                Ok(ch) => ch,
                Err(_) => continue,
            };

            // 通过 Status RPC 检测 Leader（同样携带出站凭据）
            let mut stub = MaintenanceStub::new(self.inner.pool.wrap(channel));
            let request = tonic::Request::new(StatusRequest {});
            match stub.status(request).await {
                Ok(resp) => {
                    let status = resp.into_inner();
                    // 检查该节点是否是 Leader：raft_leader 非空且 seal_status = "unsealed"
                    if !status.raft_leader.is_empty() {
                        // leader 字段是节点 ID 字符串，需要匹配
                        // 当前简化：首个返回有效 Status 的节点即为候选 Leader
                        self.inner.leader.set_leader(endpoint.clone());
                        return Ok(endpoint);
                    }
                }
                // 端点可达但**无权**调用 Status（受限 CCT / 未鉴权 / 未实现）：
                // `Maintenance/Status` 属 admin 能力点，数据面客户端（插件账户、
                // 普通用户、agent 共享客户端）不该为发现 leader 而持有它。
                // 此时把该端点作为**候选**返回：若它其实是 follower，后续 RPC 会
                // 拿到 `NotLeader` + `coord-leader-hint`，由重试路径纠正。
                Err(status)
                    if matches!(
                        status.code(),
                        tonic::Code::PermissionDenied
                            | tonic::Code::Unauthenticated
                            | tonic::Code::Unimplemented
                    ) =>
                {
                    self.inner.leader.set_leader(endpoint.clone());
                    return Ok(endpoint);
                }
                Err(_) => continue,
            }
        }

        Err(Error::ClusterUnavailable(
            "no leader found; all endpoints unreachable".into(),
        ))
    }

    /// 创建重试状态
    #[allow(dead_code)]
    fn new_retry_state(&self) -> RetryState {
        RetryState::new(&self.inner.config)
    }

    /// R-SVC-08：写请求执行器——自动 leader 重定向 + 指数退避重试。
    ///
    /// 流程：
    /// 1. 获取 leader 通道（失败则清缓存重新发现）；
    /// 2. 执行单次请求；
    /// 3. 失败时解析 `coord-leader-hint` metadata 更新 leader 缓存；
    /// 4. 按错误分类决定重试（`RetryState` 指数退避，上限 `config.max_retries`）。
    pub(crate) async fn execute_write_with_retry<T, Fut, F>(&self, mut attempt: F) -> Result<T>
    where
        F: FnMut(AuthedChannel) -> Fut,
        Fut: std::future::Future<Output = std::result::Result<T, tonic::Status>>,
    {
        let mut retry = RetryState::new(&self.inner.config);
        loop {
            // 获取 leader 通道；连接失败则清缓存并重新发现（leader 可能已切换）
            let (endpoint, channel) = match self.get_leader_channel().await {
                Ok(pair) => pair,
                Err(_) => {
                    self.inner.leader.clear_leader();
                    match self.discover_leader().await {
                        Ok(addr) => {
                            let ch = self.inner.pool.get(&addr).await?;
                            (addr, ch)
                        }
                        Err(e) => return Err(e),
                    }
                }
            };

            match attempt(channel.clone()).await {
                Ok(v) => {
                    self.return_channel(&endpoint, channel);
                    return Ok(v);
                }
                Err(status) => {
                    // 解析 leader hint：follower 返回 UNAVAILABLE + coord-leader-hint
                    let hint = status
                        .metadata()
                        .get(LEADER_HINT_METADATA_KEY)
                        .and_then(|v| v.to_str().ok())
                        .map(|s| s.to_string());
                    match hint {
                        Some(h) if !h.is_empty() => {
                            self.inner.leader.set_leader(h);
                        }
                        _ => {
                            // 无 hint：连接级不可用，清除缓存强制重新发现
                            if status.code() == tonic::Code::Unavailable {
                                self.inner.leader.clear_leader();
                            }
                        }
                    }
                    self.return_channel(&endpoint, channel);

                    match classify_tonic(&status) {
                        RetryDecision::Abort => return Err(from_status(status)),
                        RetryDecision::RetryImmediately | RetryDecision::RetryAfter(_) => {
                            match retry.next_attempt() {
                                Some(wait) => {
                                    tokio::time::sleep(wait).await;
                                    continue;
                                }
                                None => return Err(from_status(status)),
                            }
                        }
                    }
                }
            }
        }
    }

    /// 在 Leader hint 更新后重试
    fn handle_not_leader_hint(&self, hint: Option<&str>) {
        self.inner.leader.try_update_from_hint(hint);
    }

    /// 将 tonic::Status 的错误消息用于重试分类
    #[allow(dead_code)]
    fn classify_tonic_error(&self, status: &tonic::Status) -> crate::retry::RetryDecision {
        classify_error(status.message())
    }
}

// ──── KV Client ────

/// KV 操作客户端（Put / Range / Delete）
#[derive(Clone)]
pub struct KvClient {
    client: Client,
}

impl KvClient {
    fn new(client: Client) -> Self {
        Self { client }
    }

    /// 写入键值对（完整选项）。
    ///
    /// # 参数
    /// - `key`: 键（任意 bytes）
    /// - `value`: 值（任意 bytes）
    /// - `lease_id`: 关联的 Lease ID（0 表示不绑定）
    /// - `request_id`: 幂等去重 ID（空表示不去重）
    ///
    /// # 返回
    /// 写入后的全局 Revision
    pub async fn put_full(
        &self,
        key: &[u8],
        value: &[u8],
        lease_id: i64,
        request_id: &[u8],
    ) -> Result<u64> {
        // R-SVC-08：带 leader 重定向 + 指数退避重试
        let client = self.client.clone();
        let key = key.to_vec();
        let value = value.to_vec();
        let request_id = request_id.to_vec();
        client
            .execute_write_with_retry(move |channel| {
                let mut stub = KvStub::new(channel);
                let request = tonic::Request::new(PutRequest {
                    key: key.clone(),
                    value: value.clone(),
                    lease_id,
                    prev_kv: false,
                    request_id: request_id.clone(),
                });
                async move {
                    let resp = stub.put(request).await?;
                    Ok(resp.into_inner().revision as u64)
                }
            })
            .await
    }

    /// 写入键值对（简单调用）。
    ///
    /// # 参数
    /// - `key`: 键（任意 bytes）
    /// - `value`: 值（任意 bytes）
    ///
    /// # 返回
    /// 写入后的全局 Revision
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<u64> {
        self.put_full(key, value, 0, &[]).await
    }

    /// 范围读取键值对（完整选项）。
    ///
    /// # 参数
    /// - `key`: 起始键
    /// - `range_end`: 结束键（空 = 单键精确查询）
    /// - `limit`: 最大返回条数（0 = 无限制）
    /// - `revision`: 历史 Revision（0 = 最新）
    /// - `keys_only`: 仅返回 Key
    /// - `count_only`: 仅返回计数
    ///
    /// # 返回
    /// (kvs, count, revision)
    pub async fn range_full(
        &self,
        key: &[u8],
        range_end: &[u8],
        limit: i64,
        revision: i64,
        keys_only: bool,
        count_only: bool,
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, i64, i64)> {
        let (endpoint, channel) = self.client.get_leader_channel().await?;
        let mut stub = KvStub::new(channel.clone());

        let request = tonic::Request::new(RangeRequest {
            key: key.to_vec(),
            range_end: range_end.to_vec(),
            limit,
            revision,
            keys_only,
            count_only,
        });

        match stub.range(request).await {
            Ok(resp) => {
                self.client.return_channel(&endpoint, channel);
                let inner = resp.into_inner();
                let kvs: Vec<(Vec<u8>, Vec<u8>)> =
                    inner.kvs.into_iter().map(|kv| (kv.key, kv.value)).collect();
                Ok((kvs, inner.count, inner.revision))
            }
            Err(status) => Err(from_status(status)),
        }
    }

    /// 范围读取键值对。
    ///
    /// # 参数
    /// - `key`: 起始键
    /// - `range_end`: 结束键（空 = 单键精确查询）
    /// - `limit`: 最大返回条数（0 = 无限制）
    /// - `revision`: 历史 Revision（0 = 最新）
    pub async fn range(
        &self,
        key: &[u8],
        range_end: &[u8],
        limit: i64,
        revision: i64,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let (kvs, _count, _rev) = self
            .range_full(key, range_end, limit, revision, false, false)
            .await?;
        Ok(kvs)
    }

    /// 范围读取键值对（含 lease_id，完整选项）。
    ///
    /// 返回 `(key, value, lease_id, count, revision)` 元组。
    /// 用于 Agent 代理层需要透传 lease_id 的场景。
    pub async fn range_with_lease(
        &self,
        key: &[u8],
        range_end: &[u8],
        limit: i64,
        revision: i64,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>, i64)>> {
        let (kvs, _count, _rev) = self
            .range_with_lease_full(key, range_end, limit, revision, false, false)
            .await?;
        Ok(kvs
            .into_iter()
            .map(|(k, v, lid, _ver)| (k, v, lid))
            .collect())
    }

    /// 范围读取键值对（含 lease_id 和 version，完整选项，含 count）。
    ///
    /// 返回 `(kvs, count, revision)` 其中 kvs 为 `(key, value, lease_id, version)`。
    pub async fn range_with_lease_full(
        &self,
        key: &[u8],
        range_end: &[u8],
        limit: i64,
        revision: i64,
        keys_only: bool,
        count_only: bool,
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>, i64, i64)>, i64, i64)> {
        let (endpoint, channel) = self.client.get_leader_channel().await?;
        let mut stub = KvStub::new(channel.clone());

        let request = tonic::Request::new(RangeRequest {
            key: key.to_vec(),
            range_end: range_end.to_vec(),
            limit,
            revision,
            keys_only,
            count_only,
        });

        match stub.range(request).await {
            Ok(resp) => {
                self.client.return_channel(&endpoint, channel);
                let inner = resp.into_inner();
                let kvs: Vec<(Vec<u8>, Vec<u8>, i64, i64)> = inner
                    .kvs
                    .into_iter()
                    .map(|kv| (kv.key, kv.value, kv.lease_id, kv.version))
                    .collect();
                Ok((kvs, inner.count, inner.revision))
            }
            Err(status) => Err(from_status(status)),
        }
    }

    /// 删除键值对（完整选项）。
    ///
    /// # 返回
    /// (deleted_count, revision)
    pub async fn delete_full(
        &self,
        key: &[u8],
        range_end: &[u8],
        prev_kv: bool,
        request_id: &[u8],
    ) -> Result<(i64, i64)> {
        // R-SVC-08：带 leader 重定向 + 指数退避重试
        let client = self.client.clone();
        let key = key.to_vec();
        let range_end = range_end.to_vec();
        let request_id = request_id.to_vec();
        client
            .execute_write_with_retry(move |channel| {
                let mut stub = KvStub::new(channel);
                let request = tonic::Request::new(DeleteRequest {
                    key: key.clone(),
                    range_end: range_end.clone(),
                    prev_kv,
                    request_id: request_id.clone(),
                });
                async move {
                    let resp = stub.delete(request).await?;
                    let inner = resp.into_inner();
                    Ok((inner.deleted, inner.revision))
                }
            })
            .await
    }

    /// 删除键值对（简单调用）。
    pub async fn delete(&self, key: &[u8]) -> Result<u64> {
        let (_, revision) = self.delete_full(key, &[], false, &[]).await?;
        Ok(revision as u64)
    }

    /// 写入键值对，绑定 Lease（用于服务注册等场景）。
    ///
    /// Key 在 Lease 过期后自动删除。
    pub async fn put_lease(&self, key: &[u8], value: &[u8], lease_id: i64) -> Result<u64> {
        self.put_full(key, value, lease_id, &[]).await
    }
}

// ──── Lease Client ────

/// Lease 操作客户端（Grant / Revoke / KeepAlive）
#[derive(Clone)]
pub struct LeaseClient {
    client: Client,
}

impl LeaseClient {
    fn new(client: Client) -> Self {
        Self { client }
    }

    /// 授予租约（支持指定 ID）。
    ///
    /// # 参数
    /// - `ttl`: 租约 TTL（秒）
    /// - `id`: 指定 Lease ID（0=自动分配）
    ///
    /// # 返回
    /// 租约 ID
    pub async fn grant_with_id(&self, ttl: i64, id: i64) -> Result<i64> {
        let (endpoint, channel) = self.client.get_leader_channel().await?;
        let mut stub = LeaseStub::new(channel.clone());

        let request = tonic::Request::new(LeaseGrantRequest { ttl, id });

        match stub.lease_grant(request).await {
            Ok(resp) => {
                self.client.return_channel(&endpoint, channel);
                Ok(resp.into_inner().id)
            }
            Err(status) => Err(from_status(status)),
        }
    }

    /// 授予租约（自动分配 ID）。
    pub async fn grant(&self, ttl: i64) -> Result<i64> {
        self.grant_with_id(ttl, 0).await
    }

    /// 撤销租约。
    pub async fn revoke(&self, lease_id: i64) -> Result<()> {
        let (endpoint, channel) = self.client.get_leader_channel().await?;
        let mut stub = LeaseStub::new(channel.clone());

        let request = tonic::Request::new(LeaseRevokeRequest { id: lease_id });

        match stub.lease_revoke(request).await {
            Ok(_) => {
                self.client.return_channel(&endpoint, channel);
                Ok(())
            }
            Err(status) => Err(from_status(status)),
        }
    }

    /// 续约（KeepAlive），延长租约 TTL。
    /// 发送单次续约请求，返回续约后的 TTL。
    pub async fn keep_alive(&self, lease_id: i64) -> Result<i64> {
        let (endpoint, channel) = self.client.get_leader_channel().await?;
        let mut stub = LeaseStub::new(channel.clone());

        // KeepAlive 是双向流：发送 LeaseKeepAliveRequest，接收 LeaseKeepAliveResponse
        let request =
            tonic::Request::new(tokio_stream::once(LeaseKeepAliveRequest { id: lease_id }));

        match stub.lease_keep_alive(request).await {
            Ok(resp) => {
                self.client.return_channel(&endpoint, channel);
                let mut stream = resp.into_inner();
                // 读取至少一个响应确认续约成功
                match stream.message().await {
                    Ok(Some(msg)) => Ok(msg.ttl),
                    Ok(None) => Err(Error::Internal("keep-alive stream closed".into())),
                    Err(e) => Err(from_status(e)),
                }
            }
            Err(status) => Err(from_status(status)),
        }
    }

    /// 启动后台 KeepAlive 任务，定期续约。
    ///
    /// 返回一个 `LeaseKeeper` 句柄，Drop 时自动停止续约并撤销租约。
    pub async fn keep_alive_background(&self, lease_id: i64) -> Result<LeaseKeeper> {
        let (_endpoint, channel) = self.client.get_leader_channel().await?;
        let mut stub = LeaseStub::new(channel);

        // 打开双向流
        let (tx, rx) = mpsc::channel::<LeaseKeepAliveRequest>(4);
        let stream_in = tokio_stream::wrappers::ReceiverStream::new(rx);

        let response = stub
            .lease_keep_alive(tonic::Request::new(stream_in))
            .await
            .map_err(from_status)?;

        let mut stream_out = response.into_inner();

        // 发送初始续约请求（有界队列 + try_send）
        tx.try_send(LeaseKeepAliveRequest { id: lease_id })
            .map_err(|e| Error::Internal(format!("keep-alive channel error: {e}")))?;

        // 启动后台续约任务
        let (stop_tx, mut stop_rx) = mpsc::channel::<()>(1);
        let lease_id_copy = lease_id;
        let ttl_secs = self.client.inner.config.request_timeout.as_secs() as i64 / 3;
        let interval = std::cmp::max(ttl_secs, 1);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(interval as u64)) => {
                        // try_send——队列满时跳过本拍（周期续约，下一拍补偿）；
                        // 通道关闭才退出，不再无限阻塞挂起续约循环
                        match tx.try_send(LeaseKeepAliveRequest { id: lease_id_copy }) {
                            Ok(()) => {}
                            Err(TrySendError::Full(_)) => {
                                tracing::debug!(
                                    "keep-alive queue full, skipping one beat (lease {})",
                                    lease_id_copy
                                );
                            }
                            Err(TrySendError::Closed(_)) => break,
                        }
                    }
                    Some(_) = stop_rx.recv() => {
                        break;
                    }
                    result = stream_out.message() => {
                        match result {
                            Ok(Some(_resp)) => {
                                // 续约成功
                            }
                            Ok(None) | Err(_) => {
                                break;
                            }
                        }
                    }
                }
            }
        });

        Ok(LeaseKeeper {
            lease_id,
            stop_tx: Some(stop_tx),
            client: self.client.clone(),
        })
    }
}

/// 后台 Lease 续约句柄
///
/// Drop 时自动停止续约并撤销租约。
pub struct LeaseKeeper {
    pub lease_id: i64,
    stop_tx: Option<mpsc::Sender<()>>,
    client: Client,
}

impl LeaseKeeper {
    /// 停止续约并撤销租约
    pub async fn release(mut self) -> Result<()> {
        self.do_release().await
    }

    async fn do_release(&mut self) -> Result<()> {
        // 发送停止信号
        let _ = self.stop_tx.take();
        // 撤销租约
        self.client.lease().revoke(self.lease_id).await
    }
}

impl Drop for LeaseKeeper {
    fn drop(&mut self) {
        // 发送停止信号（不阻塞 Drop）
        let _ = self.stop_tx.take();
        // 注意：Drop 中不能执行异步操作
        // 调用者应在使用完毕后显式调用 release() 来撤销租约
    }
}

// ──── Watch Client ────

/// Watch 操作客户端（变更监听）
#[derive(Clone)]
pub struct WatchClient {
    client: Client,
}

impl WatchClient {
    fn new(client: Client) -> Self {
        Self { client }
    }

    /// 创建 Watch 订阅，返回事件接收器。
    ///
    /// # 参数
    /// - `key`: 监听的键前缀
    /// - `start_revision`: 起始 Revision（0 = 从最新开始）
    ///
    /// # 返回
    /// Watch 事件接收器（mpsc::Receiver）
    pub async fn watch(
        &self,
        key: &[u8],
        start_revision: i64,
    ) -> Result<mpsc::Receiver<Result<WatchEvent>>> {
        self.watch_full(key, &[], start_revision, false).await
    }

    /// 创建 Watch 订阅（完整选项：范围 + prev_kv），返回事件接收器。
    ///
    /// # 参数
    /// - `key`: 起始键 / 前缀
    /// - `range_end`: 范围结束（空 = 单键精确监听，`0x00` 等前缀语义由调用方构造）
    /// - `start_revision`: 起始 Revision（0 = 从最新开始）
    /// - `prev_kv`: 事件是否携带旧值
    pub async fn watch_full(
        &self,
        key: &[u8],
        range_end: &[u8],
        start_revision: i64,
        prev_kv: bool,
    ) -> Result<mpsc::Receiver<Result<WatchEvent>>> {
        let (_endpoint, channel) = self.client.get_leader_watch_channel().await?;
        let mut stub = WatchStub::new(channel);

        // 创建双向流
        let (req_tx, req_rx) = mpsc::channel::<WatchRequest>(2);
        let stream_in = tokio_stream::wrappers::ReceiverStream::new(req_rx);

        // 发送 Create 请求（必须在 stub.watch() 之前，避免死锁：
        // Server Watch 服务需要先读取 Create 才能响应）
        let create_req = WatchRequest {
            request: Some(coord_proto::watch::watch_request::Request::Create(
                WatchCreateRequest {
                    key: key.to_vec(),
                    range_end: range_end.to_vec(),
                    start_revision,
                    prev_kv,
                },
            )),
        };
        req_tx
            .try_send(create_req)
            .map_err(|e| Error::Internal(format!("watch channel error: {e}")))?;

        let response = stub
            .watch(tonic::Request::new(stream_in))
            .await
            .map_err(from_status)?;

        let mut stream_out = response.into_inner();

        // 后台任务：持续接收事件并转发（有界队列 + try_send；
        // 满时置溢出标记丢弃事件，队列有空间时优先补发 Backpressure 合成信号——对齐服务端语义）
        let (event_tx, event_rx) = mpsc::channel::<Result<WatchEvent>>(256);
        tokio::spawn(async move {
            let mut overflow = false;
            loop {
                match stream_out.message().await {
                    Ok(Some(resp)) => {
                        for event in resp.events {
                            if !forward_watch_event(&event_tx, event, &mut overflow).await {
                                return; // 接收端已关闭
                            }
                        }
                    }
                    Ok(None) => {
                        let _ = event_tx
                            .send(Err(Error::Internal("watch stream closed by server".into())))
                            .await;
                        return;
                    }
                    Err(e) => {
                        let _ = event_tx.send(Err(from_status(e))).await;
                        return;
                    }
                }
            }
        });

        Ok(event_rx)
    }
}

/// Watch 事件转发（有界队列 + 溢出信号，对齐服务端语义）。
///
/// - 队列满：置溢出标记并丢弃当前事件（内存有界，不无限阻塞）；
/// - 溢出标记为真时：优先补发一条 `Error::Backpressure` 合成事件（必达溢出信号），再送正常事件；
/// - 接收端关闭：返回 `false`，调用方终止转发。
async fn forward_watch_event(
    tx: &mpsc::Sender<Result<WatchEvent>>,
    event: WatchEvent,
    overflow: &mut bool,
) -> bool {
    if *overflow {
        let marker = Err(Error::Backpressure(
            "watch event buffer full: some events were dropped".to_string(),
        ));
        // 修复：marker 必须非阻塞补发——队列满时 `send().await` 会永久阻塞
        // 生产者（消费者尚未排空）。保持溢出标记，等待后续调用在队列有空位时补发。
        match tx.try_send(marker) {
            Ok(()) => {
                *overflow = false;
                // 本次事件处于溢出窗口内被丢弃；marker 已补发（先于后续事件）
                return true;
            }
            Err(TrySendError::Full(_)) => {
                // 队列仍满：保持溢出标记，下次调用再试
                return true;
            }
            Err(TrySendError::Closed(_)) => return false,
        }
    }
    match tx.try_send(Ok(event)) {
        Ok(()) => true,
        Err(TrySendError::Full(_)) => {
            *overflow = true;
            true
        }
        Err(TrySendError::Closed(_)) => false,
    }
}

// ──── Txn Client ────

/// Txn 操作客户端（原子事务）
#[derive(Clone)]
pub struct TxnClient {
    client: Client,
}

impl TxnClient {
    fn new(client: Client) -> Self {
        Self { client }
    }

    /// 执行原子事务（Compare-And-Swap，支持幂等 ID）。
    ///
    /// # 参数
    /// - `compares`: 条件列表（AND 语义，全部满足才执行 success）
    /// - `success_ops`: 条件满足时执行的操作
    /// - `failure_ops`: 条件不满足时执行的操作
    /// - `request_id`: 幂等去重 ID（空表示不去重）
    ///
    /// # 返回
    /// 事务执行结果
    pub async fn txn_full(
        &self,
        compares: Vec<Compare>,
        success_ops: Vec<RequestOp>,
        failure_ops: Vec<RequestOp>,
        request_id: Vec<u8>,
    ) -> Result<TxnResponse> {
        // R-SVC-08：带 leader 重定向 + 指数退避重试
        let client = self.client.clone();
        client
            .execute_write_with_retry(move |channel| {
                let mut stub = TxnStub::new(channel);
                let request = tonic::Request::new(TxnRequest {
                    compare: compares.clone(),
                    success: success_ops.clone(),
                    failure: failure_ops.clone(),
                    request_id: request_id.clone(),
                });
                async move { Ok(stub.txn(request).await?.into_inner()) }
            })
            .await
    }

    /// 执行原子事务（Compare-And-Swap）。
    pub async fn txn(
        &self,
        compares: Vec<Compare>,
        success_ops: Vec<RequestOp>,
        failure_ops: Vec<RequestOp>,
    ) -> Result<TxnResponse> {
        self.txn_full(compares, success_ops, failure_ops, Vec::new())
            .await
    }

    /// 简化的 CAS 操作：比较 key 的值，相等则写入新值。
    ///
    /// # 返回
    /// `Ok(true)` 表示 CAS 成功，`Ok(false)` 表示值不匹配（未写入）
    pub async fn cas(&self, key: &[u8], expected_value: &[u8], new_value: &[u8]) -> Result<bool> {
        use coord_proto::txn::compare::{CompareResult, Target};

        let compare = Compare {
            result: CompareResult::Equal as i32,
            target: Target::Value as i32,
            key: key.to_vec(),
            target_value: Some(coord_proto::txn::compare::TargetValue::Value(
                expected_value.to_vec(),
            )),
        };

        let put_op = RequestOp {
            op: Some(coord_proto::txn::request_op::Op::RequestPut(PutRequest {
                key: key.to_vec(),
                value: new_value.to_vec(),
                lease_id: 0,
                prev_kv: false,
                request_id: Vec::new(),
            })),
        };

        let result = self.txn(vec![compare], vec![put_op], vec![]).await?;
        Ok(result.succeeded)
    }
}

// ──── Maintenance Client ────

/// Maintenance 操作客户端（运维管理）
#[derive(Clone)]
pub struct MaintenanceClient {
    client: Client,
}

impl MaintenanceClient {
    fn new(client: Client) -> Self {
        Self { client }
    }

    /// 封存集群（所有数据不可读写）
    pub async fn seal(&self) -> Result<()> {
        let (endpoint, channel) = self.client.get_leader_channel().await?;
        let mut stub = MaintenanceStub::new(channel.clone());

        let request = tonic::Request::new(SealRequest {});

        match stub.seal(request).await {
            Ok(_) => {
                self.client.return_channel(&endpoint, channel);
                Ok(())
            }
            Err(status) => Err(from_status(status)),
        }
    }

    /// 解封集群（需提供 Shamir 分片）
    pub async fn unseal(&self, shares: Vec<Vec<u8>>) -> Result<UnsealResponse> {
        let (endpoint, channel) = self.client.get_leader_channel().await?;
        let mut stub = MaintenanceStub::new(channel.clone());

        let request = tonic::Request::new(UnsealRequest { shares });

        match stub.unseal(request).await {
            Ok(resp) => {
                self.client.return_channel(&endpoint, channel);
                Ok(resp.into_inner())
            }
            Err(status) => Err(from_status(status)),
        }
    }

    /// 查询集群状态
    pub async fn status(&self) -> Result<StatusResponse> {
        let (endpoint, channel) = self.client.get_leader_channel().await?;
        let mut stub = MaintenanceStub::new(channel.clone());

        let request = tonic::Request::new(StatusRequest {});

        match stub.status(request).await {
            Ok(resp) => {
                self.client.return_channel(&endpoint, channel);
                Ok(resp.into_inner())
            }
            Err(status) => {
                if status.code() == tonic::Code::Unavailable
                    || status.message().contains("not leader")
                {
                    self.client.handle_not_leader_hint(None);
                }
                Err(from_status(status))
            }
        }
    }

    /// 查询集群成员列表
    pub async fn member_list(&self) -> Result<coord_proto::maintenance::MemberListResponse> {
        let (endpoint, channel) = self.client.get_leader_channel().await?;
        let mut stub = MaintenanceStub::new(channel.clone());

        let request = tonic::Request::new(MemberListRequest {});

        let resp = stub.member_list(request).await.map_err(from_status)?;
        self.client.return_channel(&endpoint, channel);
        Ok(resp.into_inner())
    }
}

// ──── 对象存储客户端（coord.storage；EXPERIMENTAL 数据面） ────

/// 默认 chunk 分片字节数（对齐服务端 `[object_storage].chunk_size_bytes` 默认
/// 4MiB；服务端配置更小或对象超 `max_object_size_bytes` 时 Put 返回
/// INVALID_ARGUMENT）。
pub const DEFAULT_OBJECT_CHUNK_SIZE: usize = 4 * 1024 * 1024;

/// 单消息解码上限：单 chunk 4MiB 的 protobuf 编码消息（字段头 + varint 长度
/// 前缀）略超 4MiB → 客户端解码上限留余量（服务端 chunk ≤ 4MiB 恒满足）。
const STORAGE_DECODE_LIMIT: usize = 8 * 1024 * 1024;

/// 对象上传结果
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutObjectResult {
    /// 对象 Commit 所在 raft revision
    pub revision: u64,
    /// 实际落盘字节数
    pub size: u64,
    /// 实际 chunk 数
    pub chunks: u64,
}

/// 对象下载结果（Get 首条 stat + 全量数据）
#[derive(Debug, Clone)]
pub struct ObjectData {
    pub stat: ObjectStat,
    pub data: Vec<u8>,
}

/// 对象存储客户端（coord.storage.Storage）。
///
/// - 对象 = (bucket, object_id)：bucket 为非空 utf8、≤255B、不含 `/`；
///   object_id 任意字节 ≤1024B；
/// - `put` 为客户端流式上传（meta 首条 + 逐 chunk，默认 chunk 4MiB）；
///   已存在对象 Put → `Error::AlreadyExists`（v1 无覆盖写）；上传中断残留由
///   服务端 GC 按 upload_timeout 回收，客户端可 Delete 后整体重试；
/// - `get` 为服务端流式下载（ReadIndex 强一致读，自动路由 leader）；
/// - `delete`/`stat` unary。
#[derive(Clone)]
pub struct StorageClient {
    client: Client,
}

impl StorageClient {
    fn new(client: Client) -> Self {
        Self { client }
    }

    /// 上传整对象（默认 chunk 4MiB）。
    ///
    /// # Errors
    /// - `Error::AlreadyExists`：对象已存在（Committed 或上传中）；
    /// - `Error::InvalidArgument`：chunk 超服务端配置 / 字节与声明不符。
    pub async fn put(
        &self,
        bucket: &str,
        object_id: &[u8],
        data: &[u8],
    ) -> Result<PutObjectResult> {
        self.put_chunked(bucket, object_id, data, DEFAULT_OBJECT_CHUNK_SIZE)
            .await
    }

    /// 上传整对象（自定义 chunk 分片；≤ 4MiB 且 ≤ 服务端 chunk_size_bytes）。
    pub async fn put_chunked(
        &self,
        bucket: &str,
        object_id: &[u8],
        data: &[u8],
        chunk_size: usize,
    ) -> Result<PutObjectResult> {
        if chunk_size == 0 || chunk_size > 4 * 1024 * 1024 {
            return Err(Error::InvalidArgument(
                "object chunk_size must be in (0, 4MiB]".into(),
            ));
        }
        if data.is_empty() {
            return Err(Error::InvalidArgument(
                "object data must not be empty".into(),
            ));
        }
        let client = self.client.clone();
        let bucket = bucket.to_string();
        let object_id = object_id.to_vec();
        let data = data.to_vec();
        client
            .execute_write_with_retry(move |channel| {
                let mut stub =
                    StorageStub::new(channel).max_decoding_message_size(STORAGE_DECODE_LIMIT);
                // 消息流在 closure 体内（每次调用）重建——async 块只 move 成品
                let mut msgs = Vec::with_capacity(data.len() / chunk_size + 2);
                msgs.push(StoragePutRequest {
                    part: Some(put_request::Part::Meta(PutMeta {
                        bucket: bucket.clone(),
                        object_id: object_id.clone(),
                        total_size: data.len() as i64,
                    })),
                });
                for c in data.chunks(chunk_size) {
                    msgs.push(StoragePutRequest {
                        part: Some(put_request::Part::Chunk(c.to_vec())),
                    });
                }
                async move {
                    let resp = stub
                        .put(tonic::Request::new(tokio_stream::iter(msgs)))
                        .await?;
                    let inner = resp.into_inner();
                    Ok(PutObjectResult {
                        revision: inner.revision as u64,
                        size: inner.size as u64,
                        chunks: inner.chunks as u64,
                    })
                }
            })
            .await
    }

    /// 下载整对象（server-streaming；首条 stat + 逐 chunk 数据）。
    ///
    /// # Errors
    /// - `Error::NotFound`：对象不存在/已删除；
    /// - `Error::FailedPrecondition` 等：上传进行中（服务端语义透传）。
    pub async fn get(&self, bucket: &str, object_id: &[u8]) -> Result<ObjectData> {
        let (endpoint, channel) = self.client.get_leader_channel().await?;
        let mut stub =
            StorageStub::new(channel.clone()).max_decoding_message_size(STORAGE_DECODE_LIMIT);
        let request = tonic::Request::new(StorageGetRequest {
            bucket: bucket.to_string(),
            object_id: object_id.to_vec(),
        });
        let resp = match stub.get(request).await {
            Ok(r) => r,
            Err(status) => {
                self.client.return_channel(&endpoint, channel);
                return Err(from_status(status));
            }
        };
        let mut stream = resp.into_inner();
        let mut stat: Option<ObjectStat> = None;
        let mut data = Vec::new();
        loop {
            match stream.message().await {
                Ok(Some(msg)) => match msg.part {
                    Some(get_response::Part::Stat(s)) => stat = Some(s),
                    Some(get_response::Part::Chunk(c)) => data.extend_from_slice(&c),
                    None => {}
                },
                Ok(None) => break,
                Err(status) => {
                    self.client.return_channel(&endpoint, channel);
                    return Err(from_status(status));
                }
            }
        }
        self.client.return_channel(&endpoint, channel);
        let stat = stat.ok_or_else(|| Error::Internal("Get stream missing stat".into()))?;
        Ok(ObjectData { stat, data })
    }

    /// 查询对象元数据。不存在/已删除 → `Ok(None)`。
    pub async fn stat(&self, bucket: &str, object_id: &[u8]) -> Result<Option<ObjectStat>> {
        let (endpoint, channel) = self.client.get_leader_channel().await?;
        let mut stub =
            StorageStub::new(channel.clone()).max_decoding_message_size(STORAGE_DECODE_LIMIT);
        let request = tonic::Request::new(StorageStatRequest {
            bucket: bucket.to_string(),
            object_id: object_id.to_vec(),
        });
        let resp = match stub.stat(request).await {
            Ok(r) => r,
            Err(status) if status.code() == tonic::Code::NotFound => {
                self.client.return_channel(&endpoint, channel);
                return Ok(None);
            }
            Err(status) => {
                self.client.return_channel(&endpoint, channel);
                return Err(from_status(status));
            }
        };
        self.client.return_channel(&endpoint, channel);
        Ok(resp.into_inner().stat)
    }

    /// 删除对象（tombstone + 服务端同步删 chunk 文件），返回 (是否实际删除,
    /// tombstone revision)。
    ///
    /// # Errors
    /// `Error::NotFound`：对象不存在。
    pub async fn delete_full(&self, bucket: &str, object_id: &[u8]) -> Result<(bool, u64)> {
        let client = self.client.clone();
        let bucket = bucket.to_string();
        let object_id = object_id.to_vec();
        client
            .execute_write_with_retry(move |channel| {
                let mut stub =
                    StorageStub::new(channel).max_decoding_message_size(STORAGE_DECODE_LIMIT);
                let request = tonic::Request::new(StorageDeleteRequest {
                    bucket: bucket.clone(),
                    object_id: object_id.clone(),
                });
                async move {
                    let resp = stub.delete(request).await?;
                    let inner = resp.into_inner();
                    Ok((inner.deleted, inner.revision as u64))
                }
            })
            .await
    }

    /// 删除对象。`true` = 本次实际删除；对象不存在 → `Error::NotFound`。
    pub async fn delete(&self, bucket: &str, object_id: &[u8]) -> Result<bool> {
        Ok(self.delete_full(bucket, object_id).await?.0)
    }

    // ──── 流式会话（分块写 / 分块读；不整块驻留内存）────

    /// 打开**客户端流式**上传会话。
    ///
    /// 与 [`put`](Self::put) 的区别：完整对象**不需要**先驻留调用方内存——
    /// 逐块 [`ObjectWriter::write_chunk`]（宿主随写随发，mpsc 背压有界），
    /// 最后由 [`ObjectWriter::finish`] 收 Commit 响应。
    ///
    /// `total_size` 必须与后续实际写入字节数**完全一致**：server 的
    /// `PutMeta.total_size` 是提交前的强校验（不一致 → 上传作废，残留由服务端
    /// GC 回收），这也是对象存储「无覆盖写」语义的一部分。
    ///
    /// **不做 leader 重试**：客户端流一旦开始就无法回放。`open_put` 只做一次
    /// leader 发现；中途 `NotLeader` → `finish()` 返回 `ClusterUnavailable`，
    /// 调用方需重新 `open_put` 并重传（残留上传由服务端 GC 兜底）。
    pub async fn open_put(
        &self,
        bucket: &str,
        object_id: &[u8],
        total_size: u64,
    ) -> Result<ObjectWriter> {
        if total_size == 0 {
            return Err(Error::InvalidArgument(
                "total_size must be > 0; use open_put_unknown for a stream-determined size".into(),
            ));
        }
        self.open_put_inner(bucket, object_id, total_size as i64, Some(total_size))
            .await
    }

    /// 打开**未知长度**的客户端流式上传会话。
    ///
    /// 与 [`open_put`](Self::open_put) 相同，但**不需要**预先知道对象总长度：
    /// 线上 `PutMeta.total_size = -1` 进入未知长度模式，server 侧按
    /// `max_object_size` 封顶累计写入，`finish()` 时以实际字节数定长提交。
    ///
    /// 因此 [`ObjectWriter::total_size`] 返回 `None`，`write_chunk` 无本地上限
    /// （仅受 server 的 `max_object_size` 约束；超限 → `finish()` 报错）。
    pub async fn open_put_unknown(&self, bucket: &str, object_id: &[u8]) -> Result<ObjectWriter> {
        self.open_put_inner(bucket, object_id, -1, None).await
    }

    /// 流式上传公共路径：`meta_total` = 线上 `PutMeta.total_size`（`-1` = 未知），
    /// `bound` = 本地累计上限（`None` = 不限，由 server 兜底）。
    async fn open_put_inner(
        &self,
        bucket: &str,
        object_id: &[u8],
        meta_total: i64,
        bound: Option<u64>,
    ) -> Result<ObjectWriter> {
        if bucket.is_empty() || bucket.len() > 255 || bucket.contains('/') {
            return Err(Error::InvalidArgument(
                "bucket must be non-empty, <=255B and must not contain '/'".into(),
            ));
        }
        if object_id.is_empty() || object_id.len() > 1024 {
            return Err(Error::InvalidArgument(
                "object_id must be non-empty and <=1024B".into(),
            ));
        }

        let (endpoint, channel) = self.client.get_leader_channel().await?;
        let (tx, rx) = mpsc::channel::<StoragePutRequest>(UPLOAD_CHANNEL_CAPACITY);
        // 首条必须是 meta（server 语义）；mpsc 有容量 → 此时尚无接收者也可缓冲。
        tx.send(StoragePutRequest {
            part: Some(put_request::Part::Meta(PutMeta {
                bucket: bucket.to_string(),
                object_id: object_id.to_vec(),
                total_size: meta_total,
            })),
        })
        .await
        .map_err(|_| Error::Internal("storage upload stream closed early".into()))?;

        let client = self.client.clone();
        let ep = endpoint.clone();
        let task = tokio::spawn(async move {
            let mut stub =
                StorageStub::new(channel.clone()).max_decoding_message_size(STORAGE_DECODE_LIMIT);
            let result = match stub.put(tonic::Request::new(ReceiverStream::new(rx))).await {
                Ok(resp) => {
                    let inner = resp.into_inner();
                    Ok(PutObjectResult {
                        revision: inner.revision as u64,
                        size: inner.size as u64,
                        chunks: inner.chunks as u64,
                    })
                }
                Err(status) => Err(from_status(status)),
            };
            // 流结束后归还连接（连接池复用）
            client.return_channel(&ep, channel);
            result
        });

        Ok(ObjectWriter {
            tx: Some(tx),
            task: Some(task),
            written: 0,
            total: bound,
        })
    }

    /// 打开**服务端流式**下载会话（首条 stat 已就绪）。
    ///
    /// 与 [`get`](Self::get) 的区别：完整对象**不需要**一次读进调用方内存——
    /// 逐块 [`ObjectReader::read_chunk`]。
    pub async fn open_get(&self, bucket: &str, object_id: &[u8]) -> Result<ObjectReader> {
        let (endpoint, channel) = self.client.get_leader_channel().await?;
        let mut stub =
            StorageStub::new(channel.clone()).max_decoding_message_size(STORAGE_DECODE_LIMIT);
        let request = tonic::Request::new(StorageGetRequest {
            bucket: bucket.to_string(),
            object_id: object_id.to_vec(),
        });
        let mut stream = match stub.get(request).await {
            Ok(r) => r.into_inner(),
            Err(status) => {
                self.client.return_channel(&endpoint, channel);
                return Err(from_status(status));
            }
        };
        // 首条消息必须是 stat（server 语义）。
        let stat = match stream.message().await {
            Ok(Some(msg)) => match msg.part {
                Some(get_response::Part::Stat(s)) => s,
                _ => {
                    self.client.return_channel(&endpoint, channel);
                    return Err(Error::Internal(
                        "Get stream first message is not stat".into(),
                    ));
                }
            },
            Ok(None) => {
                self.client.return_channel(&endpoint, channel);
                return Err(Error::Internal("Get stream missing stat".into()));
            }
            Err(status) => {
                self.client.return_channel(&endpoint, channel);
                return Err(from_status(status));
            }
        };
        Ok(ObjectReader {
            client: self.client.clone(),
            endpoint,
            channel: Some(channel),
            stream: Some(stream),
            stat,
        })
    }
}

// ──── 对象存储流式会话（不整块驻留内存）────

/// 上传会话的 mpsc 背压容量（块数）：写满即 `write_chunk` 挂起，
/// 把「调用方写多快」与「网络发多快」解耦且**有界**。
const UPLOAD_CHANNEL_CAPACITY: usize = 4;

/// **客户端流式**对象上传会话。
///
/// 生命周期：`open_put` → `write_chunk` × N → `finish`（提交）。
/// 中途放弃用 [`abort`](Self::abort)；`Drop` 同样中止（未提交 = 服务端 GC 回收）。
pub struct ObjectWriter {
    tx: Option<mpsc::Sender<StoragePutRequest>>,
    task: Option<tokio::task::JoinHandle<Result<PutObjectResult>>>,
    written: u64,
    total: Option<u64>,
}

impl ObjectWriter {
    /// 已写入字节数。
    pub fn bytes_written(&self) -> u64 {
        self.written
    }

    /// 声明的总字节数；**未知长度**上传（[`open_put_unknown`](Self::open_put_unknown)）
    /// 返回 `None`（长度在 `finish` 时由实际写入量决定）。
    pub fn total_size(&self) -> Option<u64> {
        self.total
    }

    /// 追加一个 chunk（随写随发；声明模式下超过声明总量 → `InvalidArgument`）。
    ///
    /// 返回**累计**已写字节数。空 chunk 非法（server 侧同样拒绝）。
    pub async fn write_chunk(&mut self, data: &[u8]) -> Result<u64> {
        let Some(tx) = self.tx.as_ref() else {
            return Err(Error::InvalidArgument(
                "upload session is already finished".into(),
            ));
        };
        if data.is_empty() {
            return Err(Error::InvalidArgument("chunk must not be empty".into()));
        }
        let len = data.len() as u64;
        if let Some(total) = self.total {
            if self.written.saturating_add(len) > total {
                return Err(Error::InvalidArgument(format!(
                    "chunk overflows declared total_size ({} + {len} > {total})",
                    self.written
                )));
            }
        }
        tx.send(StoragePutRequest {
            part: Some(put_request::Part::Chunk(data.to_vec())),
        })
        .await
        .map_err(|_| Error::Internal("storage upload stream closed early".into()))?;
        self.written += len;
        Ok(self.written)
    }

    /// 结束上传并提交（返回 Commit 结果）。
    ///
    /// 写入字节数与 `total_size` 不符 → `InvalidArgument`（server 拒绝提交）。
    pub async fn finish(mut self) -> Result<PutObjectResult> {
        self.close_stream();
        let Some(task) = self.task.take() else {
            return Err(Error::InvalidArgument(
                "upload session is already finished".into(),
            ));
        };
        match task.await {
            Ok(result) => result,
            Err(e) => Err(Error::Internal(format!("upload task failed: {e}"))),
        }
    }

    /// 放弃上传（幂等）：关闭流并中止任务；服务端按 upload_timeout 回收残留。
    pub fn abort(mut self) {
        self.close_stream();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }

    /// 关闭客户端流（drop sender → 接收端读到 EOF）。
    fn close_stream(&mut self) {
        self.tx = None;
    }
}

impl Drop for ObjectWriter {
    fn drop(&mut self) {
        // 未 finish 就析构：关闭流 + 中止任务（不在 Drop 里阻塞等 async 收尾）。
        if self.tx.take().is_some() {
            if let Some(task) = self.task.take() {
                task.abort();
            }
        }
    }
}

impl std::fmt::Debug for ObjectWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectWriter")
            .field("written", &self.written)
            .field("total", &self.total)
            .field("active", &self.tx.is_some())
            .finish()
    }
}

/// **服务端流式**对象下载会话。
///
/// 生命周期：`open_get`（首条 stat 就绪）→ `read_chunk` × N（`None` = 读完）。
/// `Drop` 关闭流并归还连接。
pub struct ObjectReader {
    client: Client,
    endpoint: String,
    channel: Option<AuthedChannel>,
    stream: Option<tonic::Streaming<StorageGetResponse>>,
    stat: ObjectStat,
}

impl ObjectReader {
    /// 对象元数据（`open_get` 时已随首条消息取得）。
    pub fn stat(&self) -> &ObjectStat {
        &self.stat
    }

    /// 读取下一个 chunk（服务端块大小 ≤ 4MiB）；`Ok(None)` = 已读完。
    pub async fn read_chunk(&mut self) -> Result<Option<Vec<u8>>> {
        loop {
            let Some(stream) = self.stream.as_mut() else {
                return Ok(None);
            };
            match stream.message().await {
                Ok(Some(msg)) => match msg.part {
                    Some(get_response::Part::Chunk(c)) => return Ok(Some(c)),
                    // 重复 stat：更新元数据后继续（server 只在首条发 stat）
                    Some(get_response::Part::Stat(s)) => self.stat = s,
                    None => continue,
                },
                Ok(None) => {
                    self.close_stream();
                    return Ok(None);
                }
                Err(status) => {
                    self.close_stream();
                    return Err(from_status(status));
                }
            }
        }
    }

    /// 提前关闭（幂等）：不再消费剩余 chunk。
    pub fn close(&mut self) {
        self.close_stream();
    }

    fn close_stream(&mut self) {
        self.stream = None;
        if let Some(channel) = self.channel.take() {
            self.client.return_channel(&self.endpoint, channel);
        }
    }
}

impl Drop for ObjectReader {
    fn drop(&mut self) {
        self.close_stream();
    }
}

impl std::fmt::Debug for ObjectReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectReader")
            .field("endpoint", &self.endpoint)
            .field("stat", &self.stat)
            .field("active", &self.stream.is_some())
            .finish()
    }
}

// ──── 高级 API（Lock） ────

/// 分布式锁（基于 Lease）
///
/// ```ignore
/// let lock = client.lock("/my-lock", 10).await?;
/// // ... 执行业务逻辑 ...
/// lock.release().await?;
/// ```
pub struct Lock {
    key: Vec<u8>,
    lease_id: i64,
    client: Client,
}

impl Lock {
    /// 获取锁的 key
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// 获取锁关联的 Lease ID
    pub fn lease_id(&self) -> i64 {
        self.lease_id
    }

    /// 释放锁（撤销底层 Lease）
    pub async fn release(self) -> Result<()> {
        self.client.lease().revoke(self.lease_id).await
    }
}

impl Client {
    /// 获取分布式锁。
    ///
    /// 使用 Lease + Txn CAS 实现：
    /// 1. 授予 Lease
    /// 2. 通过 Txn CAS 原子性地检查 key 不存在后写入（绑定 Lease）
    ///
    /// # 参数
    /// - `key`: 锁的键名
    /// - `ttl_secs`: 锁的 TTL（秒），超时自动释放
    pub async fn lock(&self, key: &str, ttl_secs: i64) -> Result<Lock> {
        // 1. 授予 Lease
        let lease_id = self.lease().grant(ttl_secs).await?;

        // 2. 通过 Txn CAS 原子性获取锁
        //    比较: key 的 version == 0（不存在）
        //    成功: put key 并绑定 lease
        //    失败: 锁已被占用
        use coord_proto::txn::compare::{CompareResult, Target};

        let compare = Compare {
            result: CompareResult::Equal as i32,
            target: Target::Version as i32,
            key: key.as_bytes().to_vec(),
            target_value: Some(coord_proto::txn::compare::TargetValue::Version(0)),
        };

        let put_op = RequestOp {
            op: Some(coord_proto::txn::request_op::Op::RequestPut(PutRequest {
                key: key.as_bytes().to_vec(),
                value: b"locked".to_vec(),
                lease_id,
                prev_kv: false,
                request_id: Vec::new(),
            })),
        };

        let result = self.txn().txn(vec![compare], vec![put_op], vec![]).await?;

        if result.succeeded {
            Ok(Lock {
                key: key.as_bytes().to_vec(),
                lease_id,
                client: self.clone(),
            })
        } else {
            // 锁已被占用，撤销 Lease
            let _ = self.lease().revoke(lease_id).await;
            Err(Error::AlreadyExists {
                resource: "lock",
                key: key.to_string(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_status_not_found() {
        let status = tonic::Status::not_found("key not found");
        let err = from_status(status);
        match err {
            Error::NotFound { resource: _, key } => {
                assert!(key.contains("key not found"));
            }
            _ => panic!("expected NotFound"),
        }
    }

    #[test]
    fn test_from_status_permission_denied() {
        let status = tonic::Status::permission_denied("access denied");
        let err = from_status(status);
        match err {
            Error::PermissionDenied(msg) => assert!(msg.contains("access denied")),
            _ => panic!("expected PermissionDenied"),
        }
    }

    #[test]
    fn test_from_status_unavailable() {
        let status = tonic::Status::unavailable("cluster unavailable");
        let err = from_status(status);
        match err {
            Error::ClusterUnavailable(msg) => assert!(msg.contains("cluster unavailable")),
            _ => panic!("expected ClusterUnavailable"),
        }
    }

    #[test]
    fn test_from_status_deadline_exceeded() {
        let status = tonic::Status::deadline_exceeded("timeout");
        let err = from_status(status);
        match err {
            Error::RequestTimeout => {}
            _ => panic!("expected RequestTimeout"),
        }
    }

    #[test]
    fn test_from_status_internal() {
        let status = tonic::Status::internal("something broke");
        let err = from_status(status);
        match err {
            Error::Internal(msg) => assert!(msg.contains("something broke")),
            _ => panic!("expected Internal"),
        }
    }

    #[test]
    fn test_from_status_invalid_argument() {
        let status = tonic::Status::invalid_argument("bad input");
        let err = from_status(status);
        match err {
            Error::InvalidArgument(msg) => assert!(msg.contains("bad input")),
            _ => panic!("expected InvalidArgument"),
        }
    }

    // ──── 客户端背压（有界队列 + try_send + 溢出信号）────

    fn dummy_event() -> WatchEvent {
        WatchEvent::default()
    }

    #[tokio::test]
    async fn test_forward_watch_event_no_overflow() {
        let (tx, mut rx) = mpsc::channel::<Result<WatchEvent>>(4);
        let mut overflow = false;
        assert!(forward_watch_event(&tx, dummy_event(), &mut overflow).await);
        assert!(!overflow);
        assert!(rx.recv().await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_forward_watch_event_overflow_signal_delivered() {
        // 容量 1：首件入队，后续事件置溢出标记并丢弃（不阻塞）；
        // 消费者排空后，下一次转发补发必达的 Backpressure 合成信号。
        let (tx, mut rx) = mpsc::channel::<Result<WatchEvent>>(1);
        let mut overflow = false;
        assert!(forward_watch_event(&tx, dummy_event(), &mut overflow).await);
        assert!(forward_watch_event(&tx, dummy_event(), &mut overflow).await);
        assert!(overflow, "second event should overflow the cap-1 queue");
        assert!(forward_watch_event(&tx, dummy_event(), &mut overflow).await);
        assert!(overflow, "queue still full: marker not yet deliverable");

        let first = rx.recv().await.unwrap();
        assert!(first.is_ok(), "first event must be delivered normally");

        // 队列排空后，下次转发补发 Backpressure 信号并清除溢出标记（本次事件丢弃）
        assert!(forward_watch_event(&tx, dummy_event(), &mut overflow).await);
        assert!(!overflow, "overflow flag must clear after marker delivery");
        let marker = rx.recv().await.unwrap();
        assert!(
            matches!(marker, Err(Error::Backpressure(_))),
            "overflow signal must be delivered: {marker:?}"
        );

        // 后续事件恢复正常投递（marker 之后）
        assert!(forward_watch_event(&tx, dummy_event(), &mut overflow).await);
        let normal = rx.recv().await.unwrap();
        assert!(normal.is_ok());
    }

    #[tokio::test]
    async fn test_forward_watch_event_overflow_then_recover() {
        // 溢出后队列有空间：先补发 Backpressure 信号（本次事件丢弃），
        // 再排空 marker，后续事件恢复正常投递。
        let (tx, mut rx) = mpsc::channel::<Result<WatchEvent>>(1);
        let mut overflow = false;
        assert!(forward_watch_event(&tx, dummy_event(), &mut overflow).await);
        assert!(forward_watch_event(&tx, dummy_event(), &mut overflow).await);
        assert!(overflow);

        let marker_or_first = rx.recv().await.unwrap(); // 排空
        assert!(marker_or_first.is_ok());

        assert!(forward_watch_event(&tx, dummy_event(), &mut overflow).await);
        assert!(!overflow, "overflow flag must clear after marker delivery");
        let marker = rx.recv().await.unwrap();
        assert!(
            matches!(marker, Err(Error::Backpressure(_))),
            "marker must precede the next event"
        );

        // marker 排空后，下一事件正常投递
        assert!(forward_watch_event(&tx, dummy_event(), &mut overflow).await);
        let normal = rx.recv().await.unwrap();
        assert!(normal.is_ok());
    }

    #[tokio::test]
    async fn test_forward_watch_event_receiver_closed() {
        let (tx, rx) = mpsc::channel::<Result<WatchEvent>>(4);
        drop(rx);
        let mut overflow = false;
        assert!(
            !forward_watch_event(&tx, dummy_event(), &mut overflow).await,
            "closed receiver must stop forwarding"
        );
    }
}
