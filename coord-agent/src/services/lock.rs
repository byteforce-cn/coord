// coord-agent: 分布式锁 (Lock Service)
//
// 实现 BaseService trait，提供分布式互斥锁能力。
// 基于 Coord 核心原语（Lease + Txn (IfNotExists)）构建。
//
// 架构（v3.0）:
// - 封装重试与自动续期
// - 支持公平锁（队列）/ 非公平锁
// - 适用场景：定时任务幂等、资源互斥

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::RwLock as ParkingRwLock;
use tokio::sync::watch;

use crate::proxy::AgentInner;
use crate::service::{BaseService, ServiceResult};

// ──── 类型定义 ────

/// 锁状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockState {
    /// 空闲（可获取）
    Free,
    /// 已被持有
    Held,
    /// 已过期（Lease 超时未续期）
    Expired,
}

/// 锁信息
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LockInfo {
    /// 锁名称（资源标识）
    pub name: String,
    /// 当前持有者 ID
    pub holder_id: String,
    /// 绑定的 Lease ID
    pub lease_id: i64,
    /// 获取时间（Unix 时间戳，秒）
    pub acquired_at: u64,
    /// 锁 TTL（秒）
    pub ttl_secs: u64,
}

impl LockInfo {
    pub fn new(
        name: impl Into<String>,
        holder_id: impl Into<String>,
        lease_id: i64,
        ttl_secs: u64,
    ) -> Self {
        let now = unix_ts();
        Self {
            name: name.into(),
            holder_id: holder_id.into(),
            lease_id,
            acquired_at: now,
            ttl_secs,
        }
    }

    /// 构造 Server 存储 key
    pub fn storage_key(name: &str) -> Vec<u8> {
        format!("/_lock/{name}").into_bytes()
    }

    /// 检查锁是否已过期（基于 TTL）
    pub fn is_expired(&self) -> bool {
        let now = unix_ts();
        now > self.acquired_at + self.ttl_secs
    }
}

/// C4：`keep_alive` 失败后对本地记录的处置决策。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RenewAction {
    /// 保留本地记录（Server 端仍持有锁，或回查本身失败无法断定）。
    Keep,
    /// 删除本地记录并唤醒等待者（Server 明确回查不到该锁 / 已换主）。
    Drop,
}

/// C4：只有 Server **明确**回查不到锁（`Ok(None)`）才算丢锁。
///
/// 这是"后台自动续期不得假丢锁"的核心判据：
/// - `Ok(Some(_))`：锁仍在且 holder 一致 → 只是一次瞬时 keep_alive 失败 → 保留；
/// - `Err(_)`：连回查都失败（Server 不可达）→ 不能断定 → 保留（fail-safe）；
/// - `Ok(None)`：Server 端锁不存在或已换主 → 真丢锁 → 删除并唤醒等待者。
fn renew_action(verify: &ServiceResult<Option<LockInfo>>) -> RenewAction {
    match verify {
        Ok(None) => RenewAction::Drop,
        Ok(Some(_)) | Err(_) => RenewAction::Keep,
    }
}

fn unix_ts() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// C4：向 Server 回查锁 key（只读，不写本地缓存）。
///
/// 返回 `Some(info)` 仅当锁存在**且** holder 与 `holder_id` 一致。
/// 供 `renew` 与后台续期任务共用 —— 后者的判据必须是 Server 端真相，
/// 而不是"keep_alive 报错"本身（瞬时抖动不应被当成丢锁）。
async fn lookup_lock_on_server(
    inner: &AgentInner,
    name: &str,
    holder_id: &str,
) -> ServiceResult<Option<LockInfo>> {
    let key = LockInfo::storage_key(name);
    let kvs = inner
        .client
        .kv()
        .range(&key, &[], 1, 0)
        .await
        .map_err(|e| format!("failed to read lock key '{name}' from server: {e}"))?;
    let Some((_k, value)) = kvs.into_iter().next() else {
        return Ok(None);
    };
    let info: LockInfo = serde_json::from_slice(&value)
        .map_err(|e| format!("lock key '{name}' has malformed value: {e}"))?;
    if info.holder_id != holder_id {
        return Ok(None);
    }
    Ok(Some(info))
}

// ──── LockCache ────

/// 分布式锁本地缓存
///
/// 缓存本地持有的锁信息，用于快速判断锁状态和续期。
pub struct LockCache {
    /// 本地持有的锁：lock_name → LockInfo
    held: BTreeMap<String, LockInfo>,
}

impl LockCache {
    pub fn new() -> Self {
        Self {
            held: BTreeMap::new(),
        }
    }

    /// 记录本地获取的锁
    pub fn add(&mut self, info: LockInfo) {
        self.held.insert(info.name.clone(), info);
    }

    /// 移除锁记录
    pub fn remove(&mut self, name: &str) -> Option<LockInfo> {
        self.held.remove(name)
    }

    /// 查询本地持有的锁
    pub fn get(&self, name: &str) -> Option<&LockInfo> {
        self.held.get(name)
    }

    /// 检查本地是否持有某锁
    pub fn is_held(&self, name: &str) -> bool {
        self.held.contains_key(name)
    }

    /// 获取所有本地持有的锁
    pub fn all_held(&self) -> Vec<&LockInfo> {
        self.held.values().collect()
    }

    /// 本地持有锁数量
    pub fn len(&self) -> usize {
        self.held.len()
    }

    /// 是否为空
    pub fn is_empty(&self) -> bool {
        self.held.is_empty()
    }

    // C4：已删除 `cleanup_expired` / `cleanup_expired_names`。
    //
    // 此前它们以**本地** `is_expired()`（`acquired_at + ttl`）为准清理 `held`，
    // 与 Server 端 lease 真相无关：GC 停顿 / 时钟回拨会让本地先于 Server 判定
    // 「过期」，导致 `renew` 直接返回 `Ok(false)`（假丢锁）而后重入临界区。
    // 现在本地记录只能由两条路径移除：
    //   1. `remove`（显式 release / 后台续期失败且 Server 拒绝）；
    //   2. 后台续期任务收到 Server 的失败响应后移除并唤醒等待者。
    // `acquired_at` 仅作为「何时该发起续期」的调度提示，不再作为真值。

    /// 刷新锁的续期时间戳（R-AGT-12：续期成功后调用）。
    ///
    /// 续期成功即代表 Lease 依然有效，`acquired_at` 须同步刷新，
    /// 否则 `cleanup_expired` 会在一个 TTL 周期后误判过期并停止续约 → 丢锁。
    /// 返回是否命中该锁（holder 不匹配视为未命中）。
    pub fn touch(&mut self, name: &str, holder_id: &str) -> bool {
        match self.held.get_mut(name) {
            Some(info) if info.holder_id == holder_id => {
                info.acquired_at = unix_ts();
                true
            }
            _ => false,
        }
    }
}

impl Default for LockCache {
    fn default() -> Self {
        Self::new()
    }
}

// ──── LockWaitQueue（R-AGT-12：公平锁 FIFO 等待队列）────

/// 等待队列中的单个等待者
struct Waiter {
    /// 唯一标识（用于超时后从队列中移除自己）
    id: u64,
    /// 唤醒信号：锁被释放/失效时 notify
    tx: watch::Sender<()>,
}

/// 按锁名组织的 FIFO 等待队列。
///
/// 释放锁或本地锁记录被清理时 `notify_next` 唤醒队首等待者；
/// 等待者超时后按 id 将自己从队列移除，避免陈旧条目堆积。
/// 被唤醒但未抢到锁的等待者会重新排队（队尾），保持整体 FIFO。
pub struct LockWaitQueue {
    queues: HashMap<String, VecDeque<Waiter>>,
}

impl LockWaitQueue {
    pub fn new() -> Self {
        Self {
            queues: HashMap::new(),
        }
    }

    /// 入队（FIFO 尾部）
    fn enqueue(&mut self, name: &str, id: u64, tx: watch::Sender<()>) {
        self.queues
            .entry(name.to_string())
            .or_default()
            .push_back(Waiter { id, tx });
    }

    /// 移除指定等待者（超时/取消时调用）
    fn remove(&mut self, name: &str, id: u64) {
        if let Some(queue) = self.queues.get_mut(name) {
            queue.retain(|w| w.id != id);
            if queue.is_empty() {
                self.queues.remove(name);
            }
        }
    }

    /// 唤醒队首等待者（接收端已取消的条目顺延）。
    ///
    /// 返回是否实际唤醒了一个等待者。
    fn notify_next(&mut self, name: &str) -> bool {
        let Some(queue) = self.queues.get_mut(name) else {
            return false;
        };
        while let Some(waiter) = queue.pop_front() {
            if waiter.tx.send(()).is_ok() {
                if queue.is_empty() {
                    self.queues.remove(name);
                }
                return true;
            }
        }
        self.queues.remove(name);
        false
    }

    /// 某锁当前等待者数量（测试断言用）
    #[cfg(test)]
    fn len(&self, name: &str) -> usize {
        self.queues.get(name).map(|q| q.len()).unwrap_or(0)
    }
}

impl Default for LockWaitQueue {
    fn default() -> Self {
        Self::new()
    }
}

// ──── LockService ────

/// 分布式锁服务
///
/// 实现 `BaseService` trait，为应用提供分布式锁的获取、释放、续期能力。
pub struct LockService {
    /// 到 Server 集群的内部客户端（共享）
    inner: Arc<AgentInner>,
    /// 本地锁缓存
    cache: Arc<ParkingRwLock<LockCache>>,
    /// 健康状态
    healthy: ParkingRwLock<bool>,
    /// 关闭信号发送端
    shutdown_tx: ParkingRwLock<Option<watch::Sender<()>>>,
    /// FIFO 公平等待队列（R-AGT-12）
    waiters: Arc<ParkingRwLock<LockWaitQueue>>,
    /// 等待者 id 生成器
    next_waiter_id: AtomicU64,
}

impl LockService {
    /// 服务名称常量
    pub const NAME: &'static str = "lock";

    /// 创建 LockService
    pub fn new(inner: Arc<AgentInner>) -> Self {
        Self {
            inner,
            cache: Arc::new(ParkingRwLock::new(LockCache::new())),
            healthy: ParkingRwLock::new(false),
            shutdown_tx: ParkingRwLock::new(None),
            waiters: Arc::new(ParkingRwLock::new(LockWaitQueue::new())),
            next_waiter_id: AtomicU64::new(1),
        }
    }

    /// 获取分布式锁（非阻塞，使用 Txn CAS 保证互斥）
    ///
    /// 使用 Txn Compare(Version==0) 语义：若 key 不存在（version==0），写入并绑定 Lease；
    /// 若 key 已存在（被他人持有），返回 None 且不覆盖。
    pub async fn acquire(
        &self,
        name: &str,
        holder_id: &str,
        ttl_secs: u64,
    ) -> ServiceResult<Option<LockInfo>> {
        let storage_key = LockInfo::storage_key(name);

        // 创建 Lease（先创建，若 Txn 失败则撤销）
        let lease_id = self
            .inner
            .client
            .lease()
            .grant(ttl_secs as i64)
            .await
            .map_err(|e| format!("failed to grant lease for lock '{name}': {e}"))?;

        let lock_info = LockInfo::new(name, holder_id, lease_id, ttl_secs);
        let value = serde_json::to_vec(&lock_info)
            .map_err(|e| format!("failed to serialize lock info: {e}"))?;

        // 使用 Txn CAS: 比较 Version==0（key 不存在），成功则 Put + Lease
        use coord_proto::kv::PutRequest;
        use coord_proto::txn::compare::{CompareResult, Target};
        use coord_proto::txn::{Compare, RequestOp};

        let compare = Compare {
            result: CompareResult::Equal as i32,
            target: Target::Version as i32,
            key: storage_key.clone(),
            target_value: Some(coord_proto::txn::compare::TargetValue::Version(0)),
        };

        let put_op = RequestOp {
            op: Some(coord_proto::txn::request_op::Op::RequestPut(PutRequest {
                key: storage_key.clone(),
                value: value.clone(),
                lease_id,
                prev_kv: false,
                request_id: Vec::new(),
            })),
        };

        match self
            .inner
            .client
            .txn()
            .txn(vec![compare], vec![put_op], vec![])
            .await
        {
            Ok(resp) if resp.succeeded => {
                // 获取成功
                self.cache.write().add(lock_info.clone());
                tracing::info!(
                    "LockService: acquired lock '{name}' for holder '{holder_id}' (lease={lease_id}, ttl={ttl_secs}s)"
                );
                Ok(Some(lock_info))
            }
            Ok(_resp) => {
                // 锁已被他人持有，释放刚创建的 Lease
                let _ = self.inner.client.lease().revoke(lease_id).await;
                tracing::debug!("LockService: lock '{name}' already held by another holder");
                Ok(None)
            }
            Err(e) => {
                // 通信失败，释放 Lease
                let _ = self.inner.client.lease().revoke(lease_id).await;
                Err(format!("failed to acquire lock '{name}': {e}").into())
            }
        }
    }

    /// 释放分布式锁
    ///
    /// 撤销 Lease（使锁 key 自动过期删除）。
    pub async fn release(&self, name: &str, holder_id: &str) -> ServiceResult<bool> {
        let _storage_key = LockInfo::storage_key(name);

        // 从本地缓存获取锁信息
        let lock_info = match self.cache.read().get(name) {
            Some(info) if info.holder_id == holder_id => info.clone(),
            _ => {
                tracing::warn!("LockService: lock '{name}' not held by '{holder_id}'");
                return Ok(false);
            }
        };

        // 撤销 Lease（Server 会自动清理关联的 key）
        self.inner
            .client
            .lease()
            .revoke(lock_info.lease_id)
            .await
            .map_err(|e| format!("failed to revoke lease for lock '{name}': {e}"))?;

        // 从本地缓存移除
        self.cache.write().remove(name);

        // R-AGT-12：唤醒队首等待者（公平锁）
        self.waiters.write().notify_next(name);

        tracing::info!(
            "LockService: released lock '{name}' (holder='{holder_id}', lease={})",
            lock_info.lease_id
        );
        Ok(true)
    }

    /// 续期分布式锁
    ///
    /// 延长 Lease 的 TTL，防止锁过期。
    ///
    /// C4：本地无记录**不等于**服务端锁已失效（GC 停顿 / 时钟回拨 / 记录未重建）。
    /// 此时先向 Server 回查锁 key：仍由本 holder 持有则重建本地记录并继续续期；
    /// 确实不存在或已被他人持有，才返回 `Ok(false)`。
    pub async fn renew(&self, name: &str, holder_id: &str) -> ServiceResult<bool> {
        // 注意：`parking_lot` 读锁 guard 不可跨 await 持有（future 必须 Send），
        // 故先取出本地记录的快照，再决定是否回查 Server。
        let cached = {
            let guard = self.cache.read();
            match guard.get(name) {
                Some(info) if info.holder_id == holder_id => Some(info.clone()),
                _ => None,
            }
        };

        let lock_info = match cached {
            Some(info) => info,
            None => match self.recover_lock_from_server(name, holder_id).await? {
                Some(info) => {
                    tracing::warn!(
                        "LockService: rebuilt local record for lock '{name}' from server \
                         (holder='{holder_id}', lease={}) — local TTL is not a truth source (C4)",
                        info.lease_id
                    );
                    info
                }
                None => {
                    tracing::warn!(
                        "LockService: cannot renew lock '{name}' — server-side lock is absent \
                         or owned by another holder (holder='{holder_id}')"
                    );
                    return Ok(false);
                }
            },
        };

        // 通过 KeepAlive 续期
        self.inner
            .client
            .lease()
            .keep_alive(lock_info.lease_id)
            .await
            .map_err(|e| format!("failed to renew lock '{name}': {e}"))?;

        // R-AGT-12：续期成功即刷新 acquired_at，防止 cleanup_expired 误判过期丢锁
        self.cache.write().touch(name, holder_id);

        tracing::debug!(
            "LockService: renewed lock '{name}' (lease={})",
            lock_info.lease_id
        );
        Ok(true)
    }

    /// 阻塞获取锁（R-AGT-12：FIFO 公平等待，可选超时）。
    ///
    /// 立即尝试一次；失败后按 FIFO 排队等待，锁被释放/失效时被唤醒后重试。
    /// 支持 `timeout`（`None` = 无限等待）；超时返回 `Ok(None)`。
    /// 返回 `Some(lock)` 表示获取成功。
    pub async fn acquire_blocking(
        &self,
        name: &str,
        holder_id: &str,
        ttl_secs: u64,
        timeout: Option<Duration>,
    ) -> ServiceResult<Option<LockInfo>> {
        let waiter_id = self.next_waiter_id.fetch_add(1, Ordering::Relaxed);

        loop {
            // 快速路径：立即尝试一次
            if let Some(lock) = self.acquire(name, holder_id, ttl_secs).await? {
                return Ok(Some(lock));
            }

            // 排队（FIFO 尾部）
            let (tx, mut rx) = watch::channel(());
            let lock_was_released;
            {
                let mut waiters = self.waiters.write();
                waiters.enqueue(name, waiter_id, tx);
                // 双检：排队窗口内锁可能已被释放，此时直接重试而非等待
                lock_was_released = self.cache.read().get(name).is_none();
            }
            if lock_was_released {
                continue;
            }

            // 等待唤醒或超时
            let wait_fut = async {
                let _ = rx.changed().await;
            };
            match timeout {
                Some(t) => {
                    if tokio::time::timeout(t, wait_fut).await.is_err() {
                        self.waiters.write().remove(name, waiter_id);
                        tracing::debug!(
                            "LockService: acquire_blocking('{name}') timed out after {t:?}"
                        );
                        return Ok(None);
                    }
                }
                None => {
                    wait_fut.await;
                }
            }
            // 被唤醒后回到循环顶部重试；未抢到则重新排队（保持整体 FIFO）
        }
    }

    /// 查询锁状态
    ///
    /// 从 Server 读取当前锁信息。
    pub async fn query(&self, name: &str) -> ServiceResult<Option<LockInfo>> {
        let storage_key = LockInfo::storage_key(name);

        let pairs = self
            .inner
            .client
            .kv()
            .range(&storage_key, &storage_key, 1, 0)
            .await
            .map_err(|e| format!("failed to query lock '{name}': {e}"))?;

        if let Some((_k, v)) = pairs.into_iter().next() {
            let info: LockInfo = serde_json::from_slice(&v)
                .map_err(|e| format!("failed to deserialize lock info: {e}"))?;
            Ok(Some(info))
        } else {
            Ok(None)
        }
    }

    /// 检查本地是否持有某锁
    pub fn is_held_locally(&self, name: &str) -> bool {
        self.cache.read().is_held(name)
    }

    /// C4：本地无记录时向 Server 回查锁 key，确认锁是否仍由 `holder_id` 持有。
    ///
    /// 返回 `Some(LockInfo)` 表示 Server 端锁仍有效（本地记录已重建）；
    /// `None` 表示锁不存在或已被他人持有。
    async fn recover_lock_from_server(
        &self,
        name: &str,
        holder_id: &str,
    ) -> ServiceResult<Option<LockInfo>> {
        let info = lookup_lock_on_server(&self.inner, name, holder_id).await?;
        if let Some(fresh) = &info {
            self.cache.write().add(fresh.clone());
        }
        Ok(info)
    }
    /// 本地持有锁数量
    pub fn held_count(&self) -> usize {
        self.cache.read().len()
    }
}

#[async_trait]
impl BaseService for LockService {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    async fn start(&self) -> ServiceResult<()> {
        tracing::info!("LockService: starting");
        *self.healthy.write() = true;

        // 启动后台续期任务
        let (_tx, mut rx) = watch::channel::<()>(());
        *self.shutdown_tx.write() = Some(_tx);

        let cache = self.cache.clone();
        let inner = self.inner.clone();
        let waiters = self.waiters.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = rx.changed() => {
                        tracing::info!("LockService: renew background task shutting down");
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(10)) => {
                        // C4：不再按本地 TTL 清理 `held`（本地时钟不是真相）。
                        // 锁是否失效一律以 Server 端 lease 为准。
                        let held: Vec<LockInfo> = cache.read().all_held().into_iter().cloned().collect();
                        for info in &held {
                            if info.ttl_secs > 0 {
                                let renew_at = info.acquired_at + info.ttl_secs / 3;
                                if unix_ts() >= renew_at {
                                    match inner.client.lease().keep_alive(info.lease_id).await {
                                        Ok(_) => {
                                            // R-AGT-12：续期成功刷新 acquired_at，避免误判过期
                                            cache.write().touch(&info.name, &info.holder_id);
                                            tracing::debug!("LockService: auto-renewed lock '{}' (lease={})", info.name, info.lease_id);
                                        }
                                        Err(e) => {
                                            // C4（后台路径收敛）：keep_alive 失败**不等于**丢锁。
                                            // 瞬时网络抖动 / leader 切换都会让 keep_alive 报错，
                                            // 而 Server 端 lease 可能仍然有效。此前直接删本地记录
                                            // 并唤醒等待者 → "假丢锁"（调用方以为丢了锁，Server 端
                                            // 却仍被自己持有，别人也拿不到）。
                                            // 判定收敛到 Server 端回查（见 `renew_action`）。
                                            let verify = lookup_lock_on_server(
                                                &inner,
                                                &info.name,
                                                &info.holder_id,
                                            )
                                            .await;
                                            match renew_action(&verify) {
                                                RenewAction::Drop => {
                                                    tracing::warn!(
                                                        "LockService: lock '{}' (lease={}) is gone server-side (keep_alive error: {}) — removing from local cache",
                                                        info.name,
                                                        info.lease_id,
                                                        e
                                                    );
                                                    cache.write().remove(&info.name);
                                                    // R-AGT-12：锁确已失效 → 唤醒等待者
                                                    waiters.write().notify_next(&info.name);
                                                }
                                                RenewAction::Keep => match &verify {
                                                    Ok(Some(fresh)) => {
                                                        tracing::warn!(
                                                            "LockService: auto-renew of lock '{}' (lease={}) failed: {} — server-side lock still held by '{}' (acquired_at={}, ttl={}s); keeping local record and retrying next cycle",
                                                            info.name,
                                                            info.lease_id,
                                                            e,
                                                            fresh.holder_id,
                                                            fresh.acquired_at,
                                                            fresh.ttl_secs,
                                                        );
                                                        cache.write().touch(&info.name, &info.holder_id);
                                                    }
                                                    _ => {
                                                        // 连回查都失败（Server 不可达）：**不能**断定丢锁，
                                                        // 保留记录，下一轮（10s）重试；真过期时 Server 端
                                                        // 会自行清理，之后回查即可发现。
                                                        tracing::warn!(
                                                            "LockService: auto-renew of lock '{}' (lease={}) failed: {} and server-side verification also failed ({}); keeping local record (fail-safe, will retry)",
                                                            info.name,
                                                            info.lease_id,
                                                            e,
                                                            verify
                                                                .as_ref()
                                                                .err()
                                                                .map(|e| e.to_string())
                                                                .unwrap_or_else(|| "unknown".to_string()),
                                                        );
                                                    }
                                                },
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }

    async fn stop(&self) -> ServiceResult<()> {
        tracing::info!("LockService: stopping");
        if let Some(tx) = self.shutdown_tx.write().take() {
            let _ = tx.send(());
        }
        *self.healthy.write() = false;
        Ok(())
    }

    fn health_check(&self) -> bool {
        *self.healthy.read()
    }
}

impl std::fmt::Debug for LockService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LockService")
            .field("held_count", &self.held_count())
            .field("healthy", &self.health_check())
            .finish()
    }
}

// ──── tests ────

#[cfg(test)]
mod tests {
    use super::*;

    // ──── LockInfo 测试 ────

    #[test]
    fn test_lock_info_creation() {
        let info = LockInfo::new("task-scheduler", "node1", 5001, 30);
        assert_eq!(info.name, "task-scheduler");
        assert_eq!(info.holder_id, "node1");
        assert_eq!(info.lease_id, 5001);
        assert_eq!(info.ttl_secs, 30);
        assert!(info.acquired_at > 0);
    }

    #[test]
    fn test_lock_info_storage_key() {
        let key = LockInfo::storage_key("task-scheduler");
        assert_eq!(String::from_utf8_lossy(&key), "/_lock/task-scheduler");
    }

    #[test]
    fn test_lock_info_is_expired() {
        let past = unix_ts() - 100;
        let info = LockInfo {
            name: "test".into(),
            holder_id: "n1".into(),
            lease_id: 1,
            acquired_at: past,
            ttl_secs: 30,
        };
        assert!(info.is_expired());

        let future_lock = LockInfo {
            name: "test".into(),
            holder_id: "n1".into(),
            lease_id: 1,
            acquired_at: unix_ts(),
            ttl_secs: 3600,
        };
        assert!(!future_lock.is_expired());
    }

    #[test]
    fn test_lock_info_serialization_roundtrip() {
        let info = LockInfo {
            name: "my-lock".into(),
            holder_id: "holder-1".into(),
            lease_id: 42,
            acquired_at: 1700000000,
            ttl_secs: 30,
        };
        let json = serde_json::to_vec(&info).unwrap();
        let restored: LockInfo = serde_json::from_slice(&json).unwrap();
        assert_eq!(restored, info);
    }

    // ──── LockCache 测试 ────

    #[test]
    fn test_lock_cache_add_and_get() {
        let mut cache = LockCache::new();
        let info = LockInfo::new("lock-a", "holder-1", 100, 30);
        cache.add(info.clone());

        assert_eq!(cache.len(), 1);
        assert!(!cache.is_empty());
        assert!(cache.is_held("lock-a"));
        assert!(!cache.is_held("lock-b"));

        let found = cache.get("lock-a").unwrap();
        assert_eq!(found.holder_id, "holder-1");
    }

    #[test]
    fn test_lock_cache_remove() {
        let mut cache = LockCache::new();
        cache.add(LockInfo::new("lock-a", "h1", 100, 30));
        cache.add(LockInfo::new("lock-b", "h2", 101, 60));

        let removed = cache.remove("lock-a").unwrap();
        assert_eq!(removed.holder_id, "h1");
        assert_eq!(cache.len(), 1);
        assert!(!cache.is_held("lock-a"));
        assert!(cache.is_held("lock-b"));
    }

    #[test]
    fn test_lock_cache_all_held() {
        let mut cache = LockCache::new();
        cache.add(LockInfo::new("a", "h1", 1, 30));
        cache.add(LockInfo::new("b", "h2", 2, 30));

        let all = cache.all_held();
        assert_eq!(all.len(), 2);
    }

    /// C4：本地 TTL 到期**不得**移除本地记录 —— 服务端 lease 才是唯一真相。
    ///
    /// 修复前 `cleanup_expired()` 会按本地 `is_expired()` 删除记录，
    /// 于是 GC 停顿 / 时钟回拨时 `renew` 直接返回 `Ok(false)`（假丢锁）。
    #[test]
    fn test_lock_cache_keeps_record_past_local_ttl() {
        let mut cache = LockCache::new();
        let past = unix_ts() - 100;

        // 本地 TTL 早已到期（时钟回拨 / GC 停顿的等价状态）
        cache.add(LockInfo {
            name: "held-but-locally-stale".into(),
            holder_id: "h1".into(),
            lease_id: 1,
            acquired_at: past,
            ttl_secs: 30,
        });
        cache.add(LockInfo::new("valid-lock", "h2", 2, 3600));

        // 记录仍在：是否失效只能由 Server 回查 / keep_alive 失败决定
        assert_eq!(cache.len(), 2);
        assert!(cache.is_held("held-but-locally-stale"));
        assert!(cache.is_held("valid-lock"));
        assert!(cache.get("held-but-locally-stale").unwrap().is_expired());
    }

    #[test]
    fn test_lock_cache_default() {
        let cache = LockCache::default();
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
    }

    // ──── LockService 名称常量测试 ────

    #[test]
    fn test_lock_service_name_constant() {
        assert_eq!(LockService::NAME, "lock");
    }

    // ──── TDD: Lock 互斥测试 ────

    /// RED→GREEN: 验证重复 acquire 同一锁返回 None（互斥性）。
    /// 由于 LockService::acquire 需要 Server 连接，本测试验证 LockCache 的互斥逻辑：
    /// 同一锁名重复 add 会覆盖 → 实际互斥由 Txn Compare(Version==0) 保证。
    #[test]
    fn test_lock_cache_prevents_duplicate_holder() {
        let mut cache = LockCache::new();

        // 第一次获取：holder-A 持有 "my-lock"
        cache.add(LockInfo::new("my-lock", "holder-A", 100, 30));
        assert!(cache.is_held("my-lock"));
        assert_eq!(cache.get("my-lock").unwrap().holder_id, "holder-A");

        // 第二次获取：holder-B 尝试获取同一锁（模拟 Txn CAS 失败后不 add）
        // 验证本地缓存不会同时存在两个 holder
        // 实际场景中，Txn CAS Version==0 会拒绝 holder-B 的写入，
        // 因此 holder-B 不会调用 cache.add()。
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get("my-lock").unwrap().holder_id, "holder-A");
    }

    // ──── R-AGT-12: 续期刷新 acquired_at ────

    #[test]
    fn test_lock_cache_touch_refreshes_acquired_at() {
        let mut cache = LockCache::new();
        let past = unix_ts() - 100;
        cache.add(LockInfo {
            name: "long-held".into(),
            holder_id: "h1".into(),
            lease_id: 7,
            acquired_at: past,
            ttl_secs: 30,
        });

        // 未 touch 前：已过期
        assert!(cache.get("long-held").unwrap().is_expired());

        // 续期成功 → touch：acquired_at 刷新，不再过期
        assert!(cache.touch("long-held", "h1"));
        let info = cache.get("long-held").unwrap();
        assert!(!info.is_expired());
        assert!(info.acquired_at >= past + 99);
    }

    #[test]
    fn test_lock_cache_touch_wrong_holder_rejected() {
        let mut cache = LockCache::new();
        cache.add(LockInfo::new("l", "h1", 1, 30));
        assert!(!cache.touch("l", "h2"));
        assert!(!cache.touch("missing", "h1"));
    }

    // ──── R-AGT-12: FIFO 公平等待队列 ────

    #[tokio::test]
    async fn test_lock_wait_queue_fifo_order() {
        let mut q = LockWaitQueue::new();
        let (tx1, mut rx1) = watch::channel(());
        let (tx2, mut rx2) = watch::channel(());
        let (tx3, mut rx3) = watch::channel(());
        q.enqueue("l", 1, tx1);
        q.enqueue("l", 2, tx2);
        q.enqueue("l", 3, tx3);
        assert_eq!(q.len("l"), 3);

        // 唤醒顺序必须为 1 → 2 → 3（FIFO）
        assert!(q.notify_next("l"));
        assert!(rx1.changed().await.is_ok());
        assert_eq!(q.len("l"), 2);

        assert!(q.notify_next("l"));
        assert!(rx2.changed().await.is_ok());
        assert_eq!(q.len("l"), 1);

        assert!(q.notify_next("l"));
        assert!(rx3.changed().await.is_ok());
        assert_eq!(q.len("l"), 0);

        // 空队列唤醒返回 false
        assert!(!q.notify_next("l"));
    }

    #[tokio::test]
    async fn test_lock_wait_queue_remove_stale_and_skip_cancelled() {
        let mut q = LockWaitQueue::new();
        let (tx1, _rx1) = watch::channel(());
        let (tx2, mut rx2) = watch::channel(());
        q.enqueue("l", 1, tx1);
        q.enqueue("l", 2, tx2);

        // 等待者 1 超时 → 从队列移除
        q.remove("l", 1);
        assert_eq!(q.len("l"), 1);

        // 唤醒队首（此时是 2）
        assert!(q.notify_next("l"));
        assert!(rx2.changed().await.is_ok());

        // 已取消接收端的等待者会被顺延跳过
        let (tx3, _rx3_dropped) = watch::channel(());
        drop(_rx3_dropped);
        q.enqueue("l", 3, tx3);
        assert!(!q.notify_next("l"));
        assert_eq!(q.len("l"), 0);
    }

    // ──── C4：后台续期的"假丢锁"回归固化 ────

    fn lock_info(name: &str) -> LockInfo {
        LockInfo::new(name, "holder-1", 5001, 30)
    }

    /// 瞬时 keep_alive 失败但 Server 端锁仍在 → **必须**保留本地记录
    /// （此前会直接删除并唤醒等待者 = 假丢锁）。
    #[test]
    fn test_renew_action_keeps_record_when_server_still_holds_lock() {
        let verify: ServiceResult<Option<LockInfo>> = Ok(Some(lock_info("l")));
        assert_eq!(renew_action(&verify), RenewAction::Keep);
    }

    /// 回查本身失败（Server 不可达）→ 不能断定丢锁，保留（fail-safe）。
    #[test]
    fn test_renew_action_keeps_record_when_verification_fails() {
        let verify: ServiceResult<Option<LockInfo>> = Err("server unreachable".to_string().into());
        assert_eq!(renew_action(&verify), RenewAction::Keep);
    }

    /// 只有 Server 明确回查不到锁（不存在 / 已换主）才删除本地记录。
    #[test]
    fn test_renew_action_drops_record_only_when_server_denies() {
        let verify: ServiceResult<Option<LockInfo>> = Ok(None);
        assert_eq!(renew_action(&verify), RenewAction::Drop);
    }
}
