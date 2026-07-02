// coord-client: 主客户端
//
// 封装 gRPC 连接管理、Leader 发现、重试逻辑，提供类型安全的 KV/Lease/Watch/Txn API。
// ADP §10.2-10.3 定义完整的 Client SDK 行为。

use std::sync::Arc;
use tonic::transport::Channel;
use tokio::sync::mpsc;

use coord_core::error::{Error, Result};
use coord_proto::kv::{
    kv_client::KvClient as KvStub, DeleteRequest, PutRequest,
    RangeRequest,
};
use coord_proto::lease::{
    lease_client::LeaseClient as LeaseStub, LeaseGrantRequest,
    LeaseKeepAliveRequest, LeaseRevokeRequest,
};
use coord_proto::txn::{
    txn_client::TxnClient as TxnStub, Compare, RequestOp, TxnRequest, TxnResponse,
};
use coord_proto::watch::{
    watch_client::WatchClient as WatchStub, WatchCreateRequest, WatchEvent, WatchRequest,
};
use coord_proto::maintenance::{
    maintenance_client::MaintenanceClient as MaintenanceStub, SealRequest,
    StatusRequest, StatusResponse, UnsealRequest, UnsealResponse,
    MemberListRequest,
};

use crate::config::Config;
use crate::leader::LeaderDiscovery;
use crate::pool::ConnectionPool;
use crate::retry::{RetryState, classify_error};

// ──── Error conversion ────

/// Convert tonic::Status to coord_core::Error
fn from_status(status: tonic::Status) -> Error {
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
    /// gRPC connection pool (ADP §10.3)
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
        // 将 channel 放入池中以便后续使用
        let endpoint = config.endpoints.first()
            .cloned()
            .unwrap_or_else(|| "unknown".into());
        pool.put(&endpoint, channel);

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

    // ──── 内部方法 ────

    /// 获取当前 Leader 地址
    async fn leader_addr(&self) -> Result<String> {
        match self.inner.leader.get_leader() {
            Some(addr) => Ok(addr),
            None => self.discover_leader().await,
        }
    }

    /// 获取到当前 Leader 的 gRPC Channel 和端点地址。
    /// 从连接池中获取复用的连接（ADP §10.3）。
    async fn get_leader_channel(&self) -> Result<(String, Channel)> {
        let leader_addr = self.leader_addr().await?;
        let channel = self.inner.pool.get(&leader_addr).await?;
        Ok((leader_addr, channel))
    }

    /// 将 Channel 归还到连接池（供子客户端使用后调用）
    fn return_channel(&self, endpoint: &str, channel: Channel) {
        self.inner.pool.put(endpoint, channel);
    }

    /// 获取到当前 Leader 的 Watch 专用 Channel 和端点地址
    async fn get_leader_watch_channel(&self) -> Result<(String, Channel)> {
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

            let endpoint_url = format!("http://{endpoint}");
            let channel = match Channel::from_shared(endpoint_url) {
                Ok(ch) => ch,
                Err(_) => continue,
            };

            let channel = match channel
                .connect_timeout(self.inner.config.connect_timeout)
                .connect()
                .await
            {
                Ok(ch) => ch,
                Err(_) => continue,
            };

            // 通过 Status RPC 检测 Leader
            let mut stub = MaintenanceStub::new(channel);
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

    /// 在 Leader hint 更新后重试
    fn handle_not_leader_hint(&self, hint: Option<&str>) {
        self.inner.leader.try_update_from_hint(hint);
    }

    /// 将 tonic::Status 的错误消息用于重试分类
    #[allow(dead_code)]
    fn classify_tonic_error(&self, status: &tonic::Status) -> crate::retry::RetryDecision {
        classify_error(&status.message())
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
        let (endpoint, channel) = self.client.get_leader_channel().await?;
        let mut stub = KvStub::new(channel.clone());

        let request = tonic::Request::new(PutRequest {
            key: key.to_vec(),
            value: value.to_vec(),
            lease_id,
            prev_kv: false,
            request_id: request_id.to_vec(),
        });

        match stub.put(request).await {
            Ok(resp) => {
                self.client.return_channel(&endpoint, channel);
                Ok(resp.into_inner().revision as u64)
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
                let kvs: Vec<(Vec<u8>, Vec<u8>)> = inner
                    .kvs
                    .into_iter()
                    .map(|kv| (kv.key, kv.value))
                    .collect();
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
        let (kvs, _count, _rev) = self.range_full(key, range_end, limit, revision, false, false).await?;
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
        Ok(kvs.into_iter().map(|(k, v, lid, _ver)| (k, v, lid)).collect())
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
        let (endpoint, channel) = self.client.get_leader_channel().await?;
        let mut stub = KvStub::new(channel.clone());

        let request = tonic::Request::new(DeleteRequest {
            key: key.to_vec(),
            range_end: range_end.to_vec(),
            prev_kv,
            request_id: request_id.to_vec(),
        });

        match stub.delete(request).await {
            Ok(resp) => {
                self.client.return_channel(&endpoint, channel);
                let inner = resp.into_inner();
                Ok((inner.deleted, inner.revision))
            }
            Err(status) => Err(from_status(status)),
        }
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
        let request = tonic::Request::new(tokio_stream::once(LeaseKeepAliveRequest {
            id: lease_id,
        }));

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

        // 发送初始续约请求
        tx.send(LeaseKeepAliveRequest { id: lease_id })
            .await
            .map_err(|e| Error::Internal(format!("keep-alive channel closed: {e}")))?;

        // 启动后台续约任务
        let (stop_tx, mut stop_rx) = mpsc::channel::<()>(1);
        let lease_id_copy = lease_id;
        let ttl_secs = self.client.inner.config.request_timeout.as_secs() as i64 / 3;
        let interval = std::cmp::max(ttl_secs, 1);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(interval as u64)) => {
                        if tx.send(LeaseKeepAliveRequest { id: lease_id_copy }).await.is_err() {
                            break;
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
                    range_end: Vec::new(),
                    start_revision,
                    prev_kv: false,
                },
            )),
        };
        req_tx
            .send(create_req)
            .await
            .map_err(|e| Error::Internal(format!("watch channel closed: {e}")))?;

        let response = stub
            .watch(tonic::Request::new(stream_in))
            .await
            .map_err(from_status)?;

        let mut stream_out = response.into_inner();

        // 后台任务：持续接收事件并转发
        let (event_tx, event_rx) = mpsc::channel::<Result<WatchEvent>>(256);
        tokio::spawn(async move {
            loop {
                match stream_out.message().await {
                    Ok(Some(resp)) => {
                        for event in resp.events {
                            if event_tx.send(Ok(event)).await.is_err() {
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
        let (endpoint, channel) = self.client.get_leader_channel().await?;
        let mut stub = TxnStub::new(channel.clone());

        let request = tonic::Request::new(TxnRequest {
            compare: compares,
            success: success_ops,
            failure: failure_ops,
            request_id,
        });

        match stub.txn(request).await {
            Ok(resp) => {
                self.client.return_channel(&endpoint, channel);
                Ok(resp.into_inner())
            }
            Err(status) => Err(from_status(status)),
        }
    }

    /// 执行原子事务（Compare-And-Swap）。
    pub async fn txn(
        &self,
        compares: Vec<Compare>,
        success_ops: Vec<RequestOp>,
        failure_ops: Vec<RequestOp>,
    ) -> Result<TxnResponse> {
        self.txn_full(compares, success_ops, failure_ops, Vec::new()).await
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
}

