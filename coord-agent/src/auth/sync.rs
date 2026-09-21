// Auth Sync Scheduler
//
// Orchestrates periodic synchronization between Agent and Server:
// - Role mapping: full sync every 5 minutes (configurable)
// - Revocation delta: incremental sync every 10 seconds
// - High-sensitivity role detection: forces server lookup each request

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use super::role_cache::RoleCache;

// ──── Sync Configuration ────

/// Configuration for the auth sync scheduler.
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Full role mapping sync interval (default: 5 minutes)
    pub role_sync_interval_secs: u64,
    /// Revocation delta sync interval (default: 10 seconds)
    pub revocation_sync_interval_secs: u64,
    /// Whether auto-sync is enabled
    pub auto_sync_enabled: bool,
    /// Maximum retries before logging a warning
    pub max_retries: u32,
    /// Retry backoff base duration
    pub retry_backoff_base_secs: u64,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            role_sync_interval_secs: 300, // 5 minutes
            revocation_sync_interval_secs: 10,
            auto_sync_enabled: true,
            max_retries: 3,
            retry_backoff_base_secs: 1,
        }
    }
}

// ──── Sync Stats ────

/// Statistics about synchronization operations.
#[derive(Debug, Clone, Default)]
pub struct SyncStats {
    /// Number of successful role syncs
    pub role_syncs_succeeded: u64,
    /// Number of failed role syncs
    pub role_syncs_failed: u64,
    /// Number of successful revocation delta syncs
    pub revocation_syncs_succeeded: u64,
    /// Number of failed revocation delta syncs
    pub revocation_syncs_failed: u64,
    /// Time of last successful role sync (Unix seconds)
    pub last_role_sync: i64,
    /// Time of last successful revocation sync (Unix seconds)
    pub last_revocation_sync: i64,
    /// Whether the sync loop is currently running
    pub is_running: bool,
}

// ──── Sync Scheduler ────

/// Manages periodic synchronization of role mappings and revocation lists.
///
/// This is designed to be run as a background task (tokio or dedicated thread).
/// It coordinates the sync intervals and retry logic.
pub struct SyncScheduler {
    /// Shared role cache to update
    role_cache: Arc<RoleCache>,
    /// Configuration
    config: SyncConfig,
    /// Sync statistics
    stats: Arc<Mutex<SyncStats>>,
    /// Stop signal for the sync loop
    stop_flag: Arc<AtomicBool>,
}

impl SyncScheduler {
    /// Create a new sync scheduler.
    pub fn new(role_cache: Arc<RoleCache>, config: SyncConfig) -> Self {
        Self {
            role_cache,
            config,
            stats: Arc::new(Mutex::new(SyncStats::default())),
            stop_flag: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Create with default configuration.
    pub fn with_defaults(role_cache: Arc<RoleCache>) -> Self {
        Self::new(role_cache, SyncConfig::default())
    }

    /// Get the role sync interval.
    pub fn role_sync_interval(&self) -> Duration {
        Duration::from_secs(self.config.role_sync_interval_secs)
    }

    /// Get the revocation sync interval.
    pub fn revocation_sync_interval(&self) -> Duration {
        Duration::from_secs(self.config.revocation_sync_interval_secs)
    }

    /// Get current sync statistics.
    pub fn stats(&self) -> SyncStats {
        self.stats.lock().clone()
    }

    /// Signal the sync loop to stop.
    pub fn stop(&self) {
        self.stop_flag.store(true, Ordering::Release);
    }

    /// Check if the sync loop is running.
    pub fn is_running(&self) -> bool {
        !self.stop_flag.load(Ordering::Acquire)
    }

    /// Determine the next sync action and its scheduled time.
    ///
    /// This helps the caller decide when to run the next sync. Returns
    /// `(action, delay_until)` where `action` describes what to sync.
    pub fn next_sync(&self) -> (SyncAction, Duration) {
        let _now = Instant::now();
        let role_sync_interval = self.role_sync_interval();
        let rev_sync_interval = self.revocation_sync_interval();

        let last_role = self.role_cache.last_sync_time();

        // Calculate time since last role sync
        let time_since_role = if last_role > 0 {
            let now_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            Duration::from_secs((now_secs - last_role).max(0) as u64)
        } else {
            Duration::MAX // Never synced — should sync immediately
        };

        let role_needed = time_since_role >= role_sync_interval;

        if role_needed {
            (SyncAction::RoleFullSync, Duration::ZERO)
        } else {
            let until_role = role_sync_interval.saturating_sub(time_since_role);
            let next = until_role.min(rev_sync_interval);
            (SyncAction::RevocationDelta, next)
        }
    }

    /// Record a successful role sync.
    pub fn record_role_sync_success(&self) {
        let mut stats = self.stats.lock();
        stats.role_syncs_succeeded += 1;
        stats.last_role_sync = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
    }

    /// Record a failed role sync.
    pub fn record_role_sync_failure(&self) {
        let mut stats = self.stats.lock();
        stats.role_syncs_failed += 1;
    }

    /// Record a successful revocation delta sync.
    pub fn record_revocation_sync_success(&self) {
        let mut stats = self.stats.lock();
        stats.revocation_syncs_succeeded += 1;
        stats.last_revocation_sync = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
    }

    /// Record a failed revocation delta sync.
    pub fn record_revocation_sync_failure(&self) {
        let mut stats = self.stats.lock();
        stats.revocation_syncs_failed += 1;
    }

    /// Calculate retry backoff for a given attempt number.
    pub fn retry_backoff(&self, attempt: u32) -> Duration {
        let base = Duration::from_secs(self.config.retry_backoff_base_secs);
        base * 2u32.pow(attempt.min(10))
    }
}

// ──── Sync Action ────

/// Describes what type of sync should be performed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncAction {
    /// Full role→capability mapping sync
    RoleFullSync,
    /// Incremental revocation delta sync
    RevocationDelta,
    /// No sync needed yet
    Idle,
}

// ──── 生产接线（A3）：真正驱动 RoleCache ────

use crate::auth::role_cache::{CapabilityGrant, RoleEntry};

/// 把 Server 的 `Role` 映射为本地 `RoleEntry`（本地授权判定的依据）。
pub fn role_entry_from_proto(role: coord_proto::auth::Role) -> RoleEntry {
    RoleEntry {
        name: role.name,
        grants: role
            .capability_grants
            .into_iter()
            .map(|g| CapabilityGrant {
                capability_id: g.capability_id,
                scope: g.scope,
            })
            .collect(),
        high_sensitive: role.high_sensitive,
    }
}

/// 角色同步任务：按 `SyncScheduler` 的节奏从 Server 拉取角色→能力映射并写入
/// `RoleCache`。
///
/// 修复背景（A3）：此前 `RoleCache` 在启动路径上是 `Arc::new(RoleCache::new())`
/// 的一次性空缓存，`sync_full` 只有测试调用方 —— 生产开启 agent 鉴权后缓存恒空，
/// 全量 RPC 被拒（且失败原因不可见）。本任务把「同步」接到生产路径上，并在同步
/// 失败时**保持缓存不变**（空缓存 = 全拒 = fail-closed），同时打出可观测日志。
pub struct RoleSyncTask {
    role_cache: Arc<RoleCache>,
    client: coord_client::Client,
    scheduler: SyncScheduler,
}

impl RoleSyncTask {
    pub fn new(role_cache: Arc<RoleCache>, client: coord_client::Client) -> Self {
        let scheduler = SyncScheduler::with_defaults(Arc::clone(&role_cache));
        Self {
            role_cache,
            client,
            scheduler,
        }
    }

    /// 同步统计（供指标/健康检查读取）。
    pub fn stats(&self) -> SyncStats {
        self.scheduler.stats()
    }

    /// 执行一次全量角色同步；成功返回同步到的角色数。
    ///
    /// 只有**完整成功**才写入缓存 —— 部分/失败结果不得污染本地授权视图。
    pub async fn sync_once(&self) -> Result<usize, String> {
        let resp = self
            .client
            .auth()
            .list_roles()
            .await
            .map_err(|e| format!("ListRoles failed: {e}"))?;
        let entries: Vec<RoleEntry> = resp.roles.into_iter().map(role_entry_from_proto).collect();
        let count = entries.len();
        self.role_cache.sync_full(entries);
        self.scheduler.record_role_sync_success();
        Ok(count)
    }

    /// 后台循环：`spawn_role_sync` 已完成首次同步时**直接进入周期同步**（不重复拉取）；
    /// 若那次失败/超时（缓存仍未初始化）则立即补一次，之后按 `role_sync_interval`
    /// 周期同步；连续失败时按指数退避重试（上限见 `SyncScheduler::retry_backoff`）。
    pub async fn run(self) {
        let mut failures: u32 = 0;
        // 启动路径已成功写入缓存 → 本轮不再拉取（否则启动瞬间会连发两次 ListRoles）。
        let mut skip_initial_sync = self.role_cache.is_initialized();
        while self.scheduler.is_running() {
            if skip_initial_sync {
                skip_initial_sync = false;
            } else {
                match self.sync_once().await {
                    Ok(n) => {
                        failures = 0;
                        tracing::info!("agent role sync complete: {n} role(s) cached");
                    }
                    Err(e) => {
                        self.scheduler.record_role_sync_failure();
                        failures = failures.saturating_add(1);
                        tracing::error!(
                            "agent role sync failed: {e} (attempt {failures}); local authorization \
                             remains fail-closed until the role mapping is available"
                        );
                    }
                }
            }
            let delay = if failures == 0 {
                self.scheduler.role_sync_interval()
            } else {
                self.scheduler.retry_backoff(failures.min(6))
            };
            tokio::time::sleep(delay).await;
        }
    }
}

/// 构造同步客户端并 spawn [`RoleSyncTask`]（A3：生产启动路径调用）。
///
/// `token_provider` 为 agent 出站凭据句柄（引导 CCT / 持久化账户会话）；角色映射
/// 端点需要 `admin:auth:role_list`（已包含在 `agent-bootstrap` 最小能力集内）。
///
/// **返回前会先完成一次全量同步**（带上限），见 [`INITIAL_ROLE_SYNC_TIMEOUT`]。
pub async fn spawn_role_sync(
    role_cache: Arc<RoleCache>,
    static_peers: Vec<String>,
    tls: Option<coord_client::config::TlsConfig>,
    token_provider: Arc<coord_client::credential::CachedTokenProvider>,
) -> Result<(), String> {
    if static_peers.is_empty() {
        return Err("no static server endpoints configured".to_string());
    }
    let mut config = coord_client::Config::new(static_peers)
        .with_token_provider(token_provider as Arc<dyn coord_client::TokenProvider>);
    if let Some(t) = tls {
        config = config.with_tls(t);
    }
    let client = coord_client::Client::connect_direct(config)
        .await
        .map_err(|e| format!("failed to build role sync client: {e}"))?;
    let task = RoleSyncTask::new(role_cache, client);

    // 首次同步**必须在对外提供鉴权服务之前完成**（所以在这里 await，而不是只依赖
    // 后台循环）。否则「已启动但 RoleCache 仍为空」的窗口内，中间件会把**合法**的
    // scope 内请求 fail-closed 拒绝，症状是：
    //   Unauthenticated: role(s) ["app-reader"] do not have capability 'data:kv:read'
    // 即 agent 重启后有一段时间不可用（`RoleCache::is_initialized()` 此前在生产路径
    // 上没有任何调用方，正是这个窗口存在的原因）。
    //
    // 超时/失败都**不阻塞启动**：保留「server 不可达时降级但不停机」的既有语义，
    // 此时继续 fail-closed（拒绝一切角色相关 RPC），并由后台循环按退避重试。
    match tokio::time::timeout(INITIAL_ROLE_SYNC_TIMEOUT, task.sync_once()).await {
        Ok(Ok(n)) => {
            tracing::info!("agent role sync (initial) complete: {n} role(s) cached before serving")
        }
        Ok(Err(e)) => tracing::error!(
            "agent role sync (initial) failed: {e}; serving fail-closed until the role \
             mapping is available (background retry with backoff)"
        ),
        Err(_) => tracing::error!(
            "agent role sync (initial) timed out after {:?}; serving fail-closed until the \
             role mapping is available (background retry with backoff)",
            INITIAL_ROLE_SYNC_TIMEOUT
        ),
    }

    tokio::spawn(task.run());
    Ok(())
}

/// 启动路径上等待**首次**角色同步的上限。
///
/// 取值考量：正常情况（server 可达）首次同步是毫秒级；只有在 server 不可达/极慢时
/// 才会接近该上限，此时早一点进入 fail-closed 降级比继续等更有价值。测试套件给
/// agent 的就绪预算是 45s，故该上限必须明显小于它。
pub const INITIAL_ROLE_SYNC_TIMEOUT: Duration = Duration::from_secs(10);

// ──── Tests ────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::role_cache::RoleEntry;
    // 注意：本模块**不得**无条件 `use std::thread;` —— 该 import 在只跑
    // `--lib`（test target）时是未使用的，会给每次 `cargo test` 留一条 warning，
    // 让"新引入的 warning"这种信号被淹没。需要 thread 的用例自行在函数内 import。

    // ──── TDD Tests ────

    #[test]
    fn test_sync_scheduler_default_config() {
        let cache = Arc::new(RoleCache::new());
        let scheduler = SyncScheduler::with_defaults(cache);

        assert_eq!(scheduler.role_sync_interval(), Duration::from_secs(300));
        assert_eq!(
            scheduler.revocation_sync_interval(),
            Duration::from_secs(10)
        );
        assert!(scheduler.is_running());
    }

    #[test]
    fn test_sync_scheduler_custom_config() {
        let cache = Arc::new(RoleCache::new());
        let config = SyncConfig {
            role_sync_interval_secs: 60,
            revocation_sync_interval_secs: 5,
            auto_sync_enabled: true,
            max_retries: 5,
            retry_backoff_base_secs: 2,
        };
        let scheduler = SyncScheduler::new(cache, config);

        assert_eq!(scheduler.role_sync_interval(), Duration::from_secs(60));
        assert_eq!(scheduler.revocation_sync_interval(), Duration::from_secs(5));
    }

    #[test]
    fn test_sync_scheduler_initial_next_sync_is_role() {
        let cache = Arc::new(RoleCache::new());
        let scheduler = SyncScheduler::with_defaults(cache);

        // Cache is not initialized, so first sync should be role full sync
        let (action, delay) = scheduler.next_sync();
        assert_eq!(action, SyncAction::RoleFullSync);
        assert_eq!(delay, Duration::ZERO);
    }

    #[test]
    fn test_sync_scheduler_after_role_sync_next_is_revocation() {
        let cache = Arc::new(RoleCache::new());

        // Simulate having completed a role sync
        cache.sync_full(vec![]);

        let scheduler = SyncScheduler::with_defaults(cache);

        let (action, _) = scheduler.next_sync();
        // After role sync, next should be revocation delta (since role was just synced)
        assert_eq!(action, SyncAction::RevocationDelta);
    }

    #[test]
    fn test_sync_scheduler_stop_and_running() {
        let cache = Arc::new(RoleCache::new());
        let scheduler = SyncScheduler::with_defaults(cache);

        assert!(scheduler.is_running());
        scheduler.stop();
        assert!(!scheduler.is_running());
    }

    #[test]
    fn test_sync_stats_initial_state() {
        let cache = Arc::new(RoleCache::new());
        let scheduler = SyncScheduler::with_defaults(cache);

        let stats = scheduler.stats();
        assert_eq!(stats.role_syncs_succeeded, 0);
        assert_eq!(stats.role_syncs_failed, 0);
        assert_eq!(stats.revocation_syncs_succeeded, 0);
        assert_eq!(stats.revocation_syncs_failed, 0);
    }

    #[test]
    fn test_sync_stats_record_role_sync_success() {
        let cache = Arc::new(RoleCache::new());
        let scheduler = SyncScheduler::with_defaults(cache);

        scheduler.record_role_sync_success();
        scheduler.record_role_sync_success();

        let stats = scheduler.stats();
        assert_eq!(stats.role_syncs_succeeded, 2);
        assert!(stats.last_role_sync > 0);
    }

    #[test]
    fn test_sync_stats_record_role_sync_failure() {
        let cache = Arc::new(RoleCache::new());
        let scheduler = SyncScheduler::with_defaults(cache);

        scheduler.record_role_sync_failure();
        scheduler.record_role_sync_failure();
        scheduler.record_role_sync_failure();

        let stats = scheduler.stats();
        assert_eq!(stats.role_syncs_failed, 3);
    }

    #[test]
    fn test_sync_stats_record_revocation_events() {
        let cache = Arc::new(RoleCache::new());
        let scheduler = SyncScheduler::with_defaults(cache);

        scheduler.record_revocation_sync_success();
        scheduler.record_revocation_sync_failure();

        let stats = scheduler.stats();
        assert_eq!(stats.revocation_syncs_succeeded, 1);
        assert_eq!(stats.revocation_syncs_failed, 1);
        assert!(stats.last_revocation_sync > 0);
    }

    #[test]
    fn test_retry_backoff_grows_exponentially() {
        let cache = Arc::new(RoleCache::new());
        let scheduler = SyncScheduler::with_defaults(cache);

        let b0 = scheduler.retry_backoff(0);
        let b1 = scheduler.retry_backoff(1);
        let b2 = scheduler.retry_backoff(2);
        let b3 = scheduler.retry_backoff(3);

        assert_eq!(b0, Duration::from_secs(1)); // base * 2^0
        assert_eq!(b1, Duration::from_secs(2)); // base * 2^1
        assert_eq!(b2, Duration::from_secs(4)); // base * 2^2
        assert_eq!(b3, Duration::from_secs(8)); // base * 2^3
    }

    #[test]
    fn test_retry_backoff_capped() {
        let cache = Arc::new(RoleCache::new());
        let config = SyncConfig {
            retry_backoff_base_secs: 2,
            max_retries: 10,
            ..Default::default()
        };
        let scheduler = SyncScheduler::new(cache, config);

        // At attempt 10, backoff = 2 * 2^10 = 2048 seconds
        let b10 = scheduler.retry_backoff(10);
        assert_eq!(b10, Duration::from_secs(2048)); // 2 * 2^10

        // Cap: the min(attempt, 10) limits us
        let b20 = scheduler.retry_backoff(20);
        assert_eq!(b20, Duration::from_secs(2048)); // same as attempt 10
    }

    #[test]
    fn test_role_cache_sync_full_updates_sync_time() {
        let cache = RoleCache::new();

        let before = cache.last_sync_time();
        assert_eq!(before, 0); // never synced

        cache.sync_full(vec![RoleEntry {
            name: "test".to_string(),
            grants: vec![],
            high_sensitive: false,
        }]);

        let after = cache.last_sync_time();
        assert!(after > 0); // should be updated
    }
}
