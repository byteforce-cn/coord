// coord-agent: 限流服务 (Rate Limiter) — Phase H
//
// 基于令牌桶算法的分布式限流。
// v8.2 §4.13: 派生能力 — "分布式限流（最终一致近似，误差约5-10%）"

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;

// ──── 配置 ────

/// 限流器配置
#[derive(Debug, Clone)]
pub struct RateLimiterConfig {
    /// 最大令牌数
    pub max_tokens: u32,
    /// 每秒补充令牌数
    pub refill_rate: f64,
}

/// 限流器统计
#[derive(Debug, Clone)]
pub struct RateLimiterStats {
    pub max_tokens: u32,
    pub tokens_consumed: u64,
    pub requests_denied: u64,
}

// ──── RateLimiterService ────

/// 令牌桶限流器
///
/// 线程安全：使用 Mutex 保护令牌状态。
pub struct RateLimiterService {
    config: RateLimiterConfig,
    /// 当前令牌数
    tokens: Arc<Mutex<f64>>,
    /// 上次补充时间
    last_refill: Arc<Mutex<Instant>>,
    /// 已消费令牌总数
    tokens_consumed: Arc<AtomicU64>,
    /// 拒绝请求数
    requests_denied: Arc<AtomicU64>,
}

impl RateLimiterService {
    /// 使用配置创建限流器
    pub fn new(config: RateLimiterConfig) -> Self {
        Self {
            tokens: Arc::new(Mutex::new(config.max_tokens as f64)),
            last_refill: Arc::new(Mutex::new(Instant::now())),
            tokens_consumed: Arc::new(AtomicU64::new(0)),
            requests_denied: Arc::new(AtomicU64::new(0)),
            config,
        }
    }

    /// 获取当前可用令牌数
    pub fn available_tokens(&self) -> u32 {
        self.refill();
        (*self.tokens.lock()) as u32
    }

    /// 尝试获取一个令牌
    ///
    /// 成功返回 Ok(())，失败（令牌不足）返回 Err。
    pub fn try_acquire(&self) -> Result<(), RateLimitError> {
        self.refill();

        let mut tokens = self.tokens.lock();
        if *tokens >= 1.0 {
            *tokens -= 1.0;
            drop(tokens);
            self.tokens_consumed.fetch_add(1, Ordering::Relaxed);
            Ok(())
        } else {
            drop(tokens);
            self.requests_denied.fetch_add(1, Ordering::Relaxed);
            Err(RateLimitError)
        }
    }

    /// 获取统计信息
    pub fn stats(&self) -> RateLimiterStats {
        RateLimiterStats {
            max_tokens: self.config.max_tokens,
            tokens_consumed: self.tokens_consumed.load(Ordering::Relaxed),
            requests_denied: self.requests_denied.load(Ordering::Relaxed),
        }
    }

    // ──── 内部 ────

    /// 按速率补充令牌
    fn refill(&self) {
        let mut tokens = self.tokens.lock();
        let mut last = self.last_refill.lock();

        let elapsed = last.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            let new_tokens = elapsed * self.config.refill_rate;
            *tokens = (*tokens + new_tokens).min(self.config.max_tokens as f64);
            *last = Instant::now();
        }
    }
}

/// 限流错误
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateLimitError;

impl std::fmt::Display for RateLimitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "rate limit exceeded")
    }
}

impl std::error::Error for RateLimitError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_rate_limit() {
        let config = RateLimiterConfig { max_tokens: 2, refill_rate: 0.0 };
        let rl = RateLimiterService::new(config);
        assert!(rl.try_acquire().is_ok());
        assert!(rl.try_acquire().is_ok());
        assert!(rl.try_acquire().is_err());
    }
}
