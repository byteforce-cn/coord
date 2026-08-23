// DEK 自动轮换（P2-05）
//
// - 策略：`DekRotationPolicy`（默认 90 天，决策文档 §6.4 P2-05）；
// - 判定：`should_rotate`（无上次轮换记录视为到期——启动即轮换一次并落盘记录）；
// - 循环：`run_dek_rotation_loop` 周期检查，到期调用 `Keyring::rotate()`（原子切换，
//   旧 DEK 入 LruCache 解密历史数据），`EncryptedDek` 经 `DekRotationStore::persist`
//   持久化到 `/_meta/dek/{key_id}`（由接入方提供存储实现）；
// - Seal 期间轮换失败仅告警并跳过，不使任务退出（fail-closed 语义由 Keyring 保证）。

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::sync::watch;

use super::key_management::{EncryptedDek, Keyring};

/// 轮换策略
#[derive(Debug, Clone, Copy)]
pub struct DekRotationPolicy {
    /// 轮换间隔（默认 90 天）
    pub interval: Duration,
    /// 到期检查周期（默认 1 小时）
    pub check_interval: Duration,
}

impl Default for DekRotationPolicy {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(90 * 24 * 3600),
            check_interval: Duration::from_secs(3600),
        }
    }
}

/// 依据上次轮换时间判定是否需要轮换。
///
/// - `last_rotation == None`：无持久化记录（如全新节点/首次接入）→ 立即轮换一次以建立记录；
/// - 距上次超过 `interval` → 轮换。
pub fn should_rotate(
    last_rotation: Option<SystemTime>,
    interval: Duration,
    now: SystemTime,
) -> bool {
    match last_rotation {
        None => true,
        Some(last) => now
            .duration_since(last)
            .map(|d| d >= interval)
            .unwrap_or(true),
    }
}

/// DEK 轮换持久化接口（由接入方实现，如 MvccStorage 的 `/_meta/dek/` 布局）。
pub trait DekRotationStore: Send + Sync {
    /// 返回上次轮换的墙钟时间（无记录返回 `None`）。
    fn last_rotation(&self) -> Option<SystemTime>;
    /// 持久化新 DEK 与轮换时间（`encrypted_dek.key_id` 为全局唯一版本号）。
    fn persist(
        &self,
        encrypted_dek: &EncryptedDek,
        rotated_at: SystemTime,
    ) -> coord_core::error::Result<()>;
}

/// 启动 DEK 自动轮换循环任务。
///
/// 每 `policy.check_interval` 检查一次；到期则 `keyring.rotate()` 并经 store 持久化。
/// 关闭信号（`shutdown.changed()`）后退出。任何单次失败仅记录日志，不退出循环。
pub fn spawn_dek_rotation_loop(
    keyring: Arc<Keyring>,
    store: Arc<dyn DekRotationStore>,
    policy: DekRotationPolicy,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(policy.check_interval);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let now = SystemTime::now();
                    if should_rotate(store.last_rotation(), policy.interval, now) {
                        match keyring.rotate() {
                            Ok(encrypted_dek) => {
                                if let Err(e) = store.persist(&encrypted_dek, now) {
                                    tracing::warn!(
                                        "DEK rotation persist failed (key_id={}): {e}",
                                        encrypted_dek.key_id
                                    );
                                } else {
                                    tracing::info!(
                                        "DEK rotated: new key_id={} (interval={}s)",
                                        encrypted_dek.key_id,
                                        policy.interval.as_secs()
                                    );
                                }
                            }
                            Err(e) => {
                                tracing::warn!("DEK rotation skipped: {e}");
                            }
                        }
                    }
                }
                _ = shutdown.changed() => {
                    tracing::debug!("DEK rotation loop shutting down");
                    break;
                }
            }
        }
    })
}

// ──── 测试 ────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    #[test]
    fn test_should_rotate_no_record() {
        let now = SystemTime::now();
        assert!(should_rotate(None, Duration::from_secs(1), now));
    }

    #[test]
    fn test_should_rotate_fresh() {
        let now = SystemTime::now();
        assert!(!should_rotate(
            Some(now),
            Duration::from_secs(90 * 24 * 3600),
            now
        ));
        assert!(!should_rotate(
            Some(now - Duration::from_secs(10)),
            Duration::from_secs(90 * 24 * 3600),
            now
        ));
    }

    #[test]
    fn test_should_rotate_expired() {
        let now = SystemTime::now();
        assert!(should_rotate(
            Some(now - Duration::from_secs(91 * 24 * 3600)),
            Duration::from_secs(90 * 24 * 3600),
            now
        ));
    }

    #[test]
    fn test_default_policy_is_90_days() {
        let p = DekRotationPolicy::default();
        assert_eq!(p.interval, Duration::from_secs(90 * 24 * 3600));
        assert_eq!(p.check_interval, Duration::from_secs(3600));
    }

    struct FakeStore {
        persisted: AtomicUsize,
        last: Mutex<Option<SystemTime>>,
    }

    impl FakeStore {
        fn new() -> Self {
            Self {
                persisted: AtomicUsize::new(0),
                last: Mutex::new(None),
            }
        }
    }

    impl DekRotationStore for FakeStore {
        fn last_rotation(&self) -> Option<SystemTime> {
            *self.last.lock().unwrap()
        }

        fn persist(
            &self,
            _encrypted_dek: &EncryptedDek,
            rotated_at: SystemTime,
        ) -> coord_core::error::Result<()> {
            self.persisted.fetch_add(1, Ordering::SeqCst);
            *self.last.lock().unwrap() = Some(rotated_at);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_rotation_loop_rotates_and_persists_when_overdue() {
        let (keyring, _) = Keyring::bootstrap().expect("bootstrap keyring");
        let keyring = Arc::new(keyring);
        let initial_key_id = keyring.active_key_id();

        let raw_store = Arc::new(FakeStore::new());
        let store: Arc<dyn DekRotationStore> = Arc::clone(&raw_store) as Arc<dyn DekRotationStore>;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let policy = DekRotationPolicy {
            interval: Duration::from_secs(10),
            check_interval: Duration::from_millis(100),
        };

        let handle = spawn_dek_rotation_loop(
            Arc::clone(&keyring),
            Arc::clone(&store),
            policy,
            shutdown_rx,
        );

        // 无记录 → 首个检查周期即轮换（interval tick 首次立即触发）
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(raw_store.persisted.load(Ordering::SeqCst), 1);
        assert!(keyring.active_key_id() > initial_key_id);

        // 记录新鲜 → 后续检查不再轮换
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(raw_store.persisted.load(Ordering::SeqCst), 1);

        let _ = shutdown_tx.send(true);
        handle.await.expect("rotation loop must exit on shutdown");
    }

    #[tokio::test]
    async fn test_rotation_loop_rotates_again_after_interval() {
        let (keyring, _) = Keyring::bootstrap().expect("bootstrap keyring");
        let keyring = Arc::new(keyring);
        let first_key_id = keyring.active_key_id();

        let raw_store = Arc::new(FakeStore::new());
        let store: Arc<dyn DekRotationStore> = Arc::clone(&raw_store) as Arc<dyn DekRotationStore>;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let policy = DekRotationPolicy {
            // 间隔 100ms：每个检查周期都到期 → 连续轮换
            interval: Duration::from_millis(100),
            check_interval: Duration::from_millis(100),
        };

        let handle = spawn_dek_rotation_loop(
            Arc::clone(&keyring),
            Arc::clone(&store),
            policy,
            shutdown_rx,
        );

        tokio::time::sleep(Duration::from_millis(450)).await;
        let persisted = raw_store.persisted.load(Ordering::SeqCst);
        assert!(
            persisted >= 2,
            "must rotate repeatedly once interval elapses"
        );
        assert!(keyring.active_key_id() > first_key_id);

        let _ = shutdown_tx.send(true);
        handle.await.expect("rotation loop must exit on shutdown");
    }
}
