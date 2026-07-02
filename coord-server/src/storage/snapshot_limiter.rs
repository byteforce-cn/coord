// Snapshot 传输限速器
//
// 基于 Token Bucket 算法，控制 Snapshot 传输速率，避免
// 快照传输占用过多网络带宽影响正常 Raft 通信。
//
// 设计要点（ADP §10.3）：
// - Token Bucket 算法：固定速度生成 token，发送前消耗 token
// - 原子操作：无锁实现，适合高频调用
// - 可配置速率限制

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

// ============================================================================
// SnapshotRateLimiter
// ============================================================================

/// Snapshot 传输速率限制器（Token Bucket）
///
/// 控制快照传输的最大带宽，防止快照同步占用过多网络资源。
pub struct SnapshotRateLimiter {
    /// 最大发送带宽（字节/秒），0 表示不限速
    max_bytes_per_sec: AtomicU64,
    /// 当前可用 token 数（原子操作）
    token_bucket: AtomicU64,
    /// 上次补充 token 的时间
    last_refill: parking_lot::Mutex<Instant>,
}

impl SnapshotRateLimiter {
    /// 创建新的限速器
    ///
    /// # Arguments
    /// * `max_bytes_per_sec` - 最大字节/秒，0 表示不限速
    pub fn new(max_bytes_per_sec: u64) -> Self {
        Self {
            max_bytes_per_sec: AtomicU64::new(max_bytes_per_sec),
            token_bucket: AtomicU64::new(max_bytes_per_sec), // 初始满桶
            last_refill: parking_lot::Mutex::new(Instant::now()),
        }
    }

    /// 不限速的限速器
    pub fn unlimited() -> Self {
        Self::new(0)
    }

    /// 获取配置的最大速率
    pub fn max_rate(&self) -> u64 {
        self.max_bytes_per_sec.load(Ordering::Relaxed)
    }

    /// 当前可用 token 数
    pub fn available_tokens(&self) -> u64 {
        self.token_bucket.load(Ordering::Relaxed)
    }

    /// 补充 token（按时间比例）
    fn refill(&self) {
        let max_rate = self.max_bytes_per_sec.load(Ordering::Relaxed);
        if max_rate == 0 {
            return; // 不限速
        }

        let mut last = self.last_refill.lock();
        let now = Instant::now();
        let elapsed = now.duration_since(*last);

        if elapsed < Duration::from_millis(10) {
            return; // 最小补充间隔 10ms
        }

        // 计算应补充的 token 数
        let refill_tokens = (max_rate as f64 * elapsed.as_secs_f64()) as u64;

        if refill_tokens > 0 {
            let current = self.token_bucket.load(Ordering::Relaxed);
            let new = (current + refill_tokens).min(max_rate);
            self.token_bucket.store(new, Ordering::Relaxed);
        }

        *last = now;
    }

    /// 申请发送 `bytes` 字节的许可
    ///
    /// 同步版本：如果有足够的 token 则立即返回 true，
    /// 否则返回 false（调用者应等待后重试）。
    pub fn try_acquire(&self, bytes: u64) -> bool {
        if self.max_bytes_per_sec.load(Ordering::Relaxed) == 0 {
            return true; // 不限速
        }

        self.refill();

        loop {
            let available = self.token_bucket.load(Ordering::Relaxed);
            if available < bytes {
                return false; // token 不足
            }
            if self
                .token_bucket
                .compare_exchange_weak(
                    available,
                    available - bytes,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return true;
            }
            // CAS 失败，重试
        }
    }

    /// 异步申请发送 `bytes` 字节的许可
    ///
    /// 如果 token 不足，会自动等待直到有足够的 token。
    pub async fn acquire(&self, bytes: u64) {
        if self.max_bytes_per_sec.load(Ordering::Relaxed) == 0 {
            return;
        }

        loop {
            if self.try_acquire(bytes) {
                return;
            }
            // 等待一小段时间后重试
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// 更新限速配置
    pub fn set_rate(&self, max_bytes_per_sec: u64) {
        self.max_bytes_per_sec.store(max_bytes_per_sec, Ordering::SeqCst);
        // 重置 token bucket
        self.token_bucket.store(max_bytes_per_sec, Ordering::SeqCst);
    }
}

impl Default for SnapshotRateLimiter {
    fn default() -> Self {
        // 默认 50 MB/s
        Self::new(50 * 1024 * 1024)
    }
}

// ============================================================================
// 测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unlimited_always_acquires() {
        let limiter = SnapshotRateLimiter::unlimited();
        assert!(limiter.try_acquire(1024 * 1024 * 1024)); // 1 GB
        assert!(limiter.try_acquire(u64::MAX));
    }

    #[test]
    fn test_limited_acquire_small() {
        let limiter = SnapshotRateLimiter::new(1024 * 1024); // 1 MB/s
        // 初始 token bucket 是满的
        assert!(limiter.try_acquire(512 * 1024)); // 512 KB
    }

    #[test]
    fn test_limited_exhaustion() {
        let limiter = SnapshotRateLimiter::new(1024 * 1024); // 1 MB/s bucket
        // 快速消耗所有 token
        let mut acquired = 0u64;
        while limiter.try_acquire(128 * 1024) {
            acquired += 128 * 1024;
        }
        // 至少获得了初始 tokens
        assert!(acquired >= 512 * 1024, "should acquire at least half bucket");
    }

    #[test]
    fn test_default_rate() {
        let limiter = SnapshotRateLimiter::default();
        assert_eq!(limiter.max_rate(), 50 * 1024 * 1024);
    }

    #[test]
    fn test_set_rate() {
        let limiter = SnapshotRateLimiter::new(1024 * 1024);
        assert_eq!(limiter.max_rate(), 1024 * 1024);
        limiter.set_rate(10 * 1024 * 1024);
        assert_eq!(limiter.max_rate(), 10 * 1024 * 1024);
    }

    #[test]
    fn test_zero_rate_unlimited() {
        let limiter = SnapshotRateLimiter::new(0);
        assert!(limiter.try_acquire(u64::MAX));
    }

    #[tokio::test]
    async fn test_async_acquire_unlimited() {
        let limiter = SnapshotRateLimiter::unlimited();
        // 应该立即返回
        let start = Instant::now();
        limiter.acquire(1024 * 1024).await;
        assert!(start.elapsed() < Duration::from_millis(5));
    }

    #[tokio::test]
    async fn test_async_acquire_limited() {
        let limiter = SnapshotRateLimiter::new(100 * 1024 * 1024); // 100 MB/s
        // 初始 bucket 满，应能立即获取
        let start = Instant::now();
        limiter.acquire(10 * 1024 * 1024).await;
        // 应该很快完成（< 1s）
        assert!(start.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn test_multiple_acquires() {
        let limiter = SnapshotRateLimiter::new(10 * 1024 * 1024); // 10 MB/s bucket
        let mut total: u64 = 0;
        for _ in 0..100 {
            if limiter.try_acquire(1024 * 1024) {
                total += 1024 * 1024;
            }
        }
        // 至少获取了初始 token 对应的量
        assert!(total >= 5 * 1024 * 1024);
    }
}
