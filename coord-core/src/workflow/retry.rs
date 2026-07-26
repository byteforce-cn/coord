// coord-core/workflow/retry.rs
// 重试策略模块 —— 基于 CNCF Serverless Workflow retry 规范
//
// 支持三种退避策略 + jitter + 最大延迟上限：
// - Constant:    delay_ms (固定延迟)
// - Linear:      delay_ms * attempt (线性增长)
// - Exponential: delay_ms * 2^(attempt-1) (指数退避)
// - Jitter:      delay * (1 ± jitter_factor * random)
//
// 与 model::RetryPolicy 互转。

use rand::Rng;

use super::model::{BackoffStrategy, RetryPolicy};

// ─── RetryStrategy ───

/// 退避策略
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryStrategy {
    Constant,
    Linear,
    Exponential,
}

impl From<BackoffStrategy> for RetryStrategy {
    fn from(bs: BackoffStrategy) -> Self {
        match bs {
            BackoffStrategy::Constant => RetryStrategy::Constant,
            BackoffStrategy::Linear => RetryStrategy::Linear,
            BackoffStrategy::Exponential => RetryStrategy::Exponential,
        }
    }
}

// ─── RetryConfig ───

/// 重试配置
#[derive(Debug, Clone, PartialEq)]
pub struct RetryConfig {
    /// 基础延迟（毫秒）
    pub delay_ms: u64,
    /// 退避策略
    pub backoff: RetryStrategy,
    /// 最大尝试次数（含首次执行，即最多执行 max_attempts 次）
    pub max_attempts: u32,
    /// 抖动因子 (0.0 ~ 1.0)，0 表示无抖动
    pub jitter_factor: f64,
    /// 单次延迟上限（毫秒），None 表示无上限
    pub max_delay_ms: Option<u64>,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            delay_ms: 3_000, // 3 秒
            backoff: RetryStrategy::Constant,
            max_attempts: 3,
            jitter_factor: 0.0,
            max_delay_ms: None,
        }
    }
}

impl RetryConfig {
    /// 从模型层 RetryPolicy 构造（需要解析 ISO 8601 duration → ms）
    pub fn from_policy(policy: &RetryPolicy, jitter_factor: Option<f64>) -> Self {
        let delay_ms = parse_duration_ms(&policy.delay).unwrap_or(3_000);
        let backoff = policy
            .backoff
            .as_ref()
            .map(|b| RetryStrategy::from(b.clone()))
            .unwrap_or(RetryStrategy::Constant);
        let jitter = jitter_factor.unwrap_or_else(|| {
            policy
                .jitter
                .as_ref()
                .map(|j| j.factor)
                .unwrap_or(0.0)
        });

        Self {
            delay_ms,
            backoff,
            max_attempts: policy.limit,
            jitter_factor: jitter,
            max_delay_ms: None,
        }
    }
}

// ─── RetryScheduler ───

/// 重试调度器 —— 跟踪当前尝试次数，计算下次重试等待时间
pub struct RetryScheduler {
    config: RetryConfig,
    /// 当前尝试次数（1-indexed，即 attempt=1 表示首次/第一次执行）
    attempt: u32,
}

impl RetryScheduler {
    /// 创建新的调度器（attempt 初始为 1，即首次执行已算在内）
    pub fn new(config: RetryConfig) -> Self {
        Self {
            config,
            attempt: 1,
        }
    }

    /// 当前尝试次数（1-indexed）
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// 是否还可以重试（当前 attempt < max_attempts）
    pub fn can_retry(&self) -> bool {
        self.attempt < self.config.max_attempts
    }

    /// 返回下一次重试前应等待的毫秒数，同时推进 attempt 计数器
    ///
    /// 返回 None 表示已达最大尝试次数，不应再重试。
    /// 调用者应在每次执行失败后调用此方法获取等待时间。
    pub fn next_delay_ms(&mut self) -> Option<u64> {
        if !self.can_retry() {
            return None;
        }

        // 计算基础延迟
        let base_delay = match self.config.backoff {
            RetryStrategy::Constant => self.config.delay_ms,
            RetryStrategy::Linear => self.config.delay_ms.saturating_mul(self.attempt as u64),
            RetryStrategy::Exponential => {
                // delay_ms * 2^(attempt-1)
                let exp = 2u64.saturating_pow(self.attempt.saturating_sub(1));
                self.config.delay_ms.saturating_mul(exp)
            }
        };

        // 应用最大延迟上限
        let capped = if let Some(max) = self.config.max_delay_ms {
            base_delay.min(max)
        } else {
            base_delay
        };

        // 应用 jitter
        let delay = apply_jitter(capped, self.config.jitter_factor);

        // 推进计数器（本次重试对应第 attempt+1 次执行）
        self.attempt += 1;

        Some(delay)
    }

    /// 重置调度器到初始状态
    pub fn reset(&mut self) {
        self.attempt = 1;
    }
}

// ─── Jitter ───

/// 对延迟应用抖动
///
/// jitter_factor=0.0 → 不抖动，返回原值
/// jitter_factor=0.1 → 在 [0.9*delay, 1.1*delay] 范围内随机
/// jitter_factor=0.5 → 在 [0.5*delay, 1.5*delay] 范围内随机
fn apply_jitter(delay_ms: u64, jitter_factor: f64) -> u64 {
    if jitter_factor <= 0.0 || delay_ms == 0 {
        return delay_ms;
    }

    let factor = jitter_factor.clamp(0.0, 1.0);
    let mut rng = rand::thread_rng();

    // 随机范围: [1.0 - factor, 1.0 + factor]
    let min_factor = 1.0 - factor;
    let max_factor = 1.0 + factor;
    let rand_factor: f64 = rng.gen_range(min_factor..max_factor);

    let jittered = (delay_ms as f64 * rand_factor).round() as u64;
    // 至少返回 1ms
    jittered.max(1)
}

// ─── Duration 解析辅助 ───

/// 解析 ISO 8601 duration 简单格式为毫秒
///
/// 支持: PTnS, PTnM, PTnH, PnD, PnW
fn parse_duration_ms(duration: &str) -> Option<u64> {
    let s = duration.trim();

    // PnW (weeks)
    if let Some(rest) = s.strip_prefix('P') {
        if let Some(week_str) = rest.strip_suffix('W') {
            if let Ok(weeks) = week_str.parse::<f64>() {
                return Some((weeks * 7.0 * 24.0 * 60.0 * 60.0 * 1000.0) as u64);
            }
        }
    }

    // Parse P...T... format
    let (date_part, time_part) = if let Some(t_pos) = s.find('T') {
        (&s[1..t_pos], Some(&s[t_pos + 1..]))
    } else if s.starts_with('P') {
        (&s[1..], None)
    } else {
        return None;
    };

    let mut total_ms: f64 = 0.0;

    // Date components
    if !date_part.is_empty() {
        total_ms += extract_component(date_part, 'D', 24.0 * 60.0 * 60.0 * 1000.0);
    }

    // Time components
    if let Some(tp) = time_part {
        total_ms += extract_component(tp, 'H', 60.0 * 60.0 * 1000.0);
        total_ms += extract_component(tp, 'M', 60.0 * 1000.0);
        total_ms += extract_component(tp, 'S', 1000.0);
    }

    if total_ms > 0.0 {
        Some(total_ms as u64)
    } else {
        None
    }
}

fn extract_component(s: &str, unit: char, multiplier: f64) -> f64 {
    let mut result = 0.0;
    let mut current_num = String::new();

    for ch in s.chars() {
        if ch.is_ascii_digit() || ch == '.' {
            current_num.push(ch);
        } else if ch == unit {
            if let Ok(val) = current_num.parse::<f64>() {
                result += val * multiplier;
            }
            current_num.clear();
        } else {
            current_num.clear();
        }
    }

    result
}

// ─── 测试 ───

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::model::RetryPolicy;

    // ─── Constant 退避 ───

    #[test]
    fn constant_retry_no_jitter() {
        let config = RetryConfig {
            delay_ms: 1000,
            backoff: RetryStrategy::Constant,
            max_attempts: 5,
            jitter_factor: 0.0,
            max_delay_ms: None,
        };
        let mut scheduler = RetryScheduler::new(config);

        assert_eq!(scheduler.attempt(), 1);
        assert!(scheduler.can_retry());

        // attempt 1 → retry → 等待 1000ms, attempt 变为 2
        let d1 = scheduler.next_delay_ms().unwrap();
        assert_eq!(d1, 1000);
        assert_eq!(scheduler.attempt(), 2);

        // attempt 2 → retry → 等待 1000ms
        let d2 = scheduler.next_delay_ms().unwrap();
        assert_eq!(d2, 1000);
        assert_eq!(scheduler.attempt(), 3);

        let d3 = scheduler.next_delay_ms().unwrap();
        assert_eq!(d3, 1000);
        assert_eq!(scheduler.attempt(), 4);

        let d4 = scheduler.next_delay_ms().unwrap();
        assert_eq!(d4, 1000);
        assert_eq!(scheduler.attempt(), 5);

        // attempt 5 是最后一次，不能再重试
        assert!(!scheduler.can_retry());
        assert_eq!(scheduler.next_delay_ms(), None);
    }

    #[test]
    fn constant_retry_max_attempts() {
        let config = RetryConfig {
            delay_ms: 500,
            backoff: RetryStrategy::Constant,
            max_attempts: 2,
            jitter_factor: 0.0,
            max_delay_ms: None,
        };
        let mut scheduler = RetryScheduler::new(config);

        // 只有 1 次重试机会
        assert!(scheduler.can_retry());
        scheduler.next_delay_ms().unwrap();
        assert!(!scheduler.can_retry());
        assert_eq!(scheduler.next_delay_ms(), None);
    }

    // ─── Linear 退避 ───

    #[test]
    fn linear_retry_no_jitter() {
        let config = RetryConfig {
            delay_ms: 1000,
            backoff: RetryStrategy::Linear,
            max_attempts: 5,
            jitter_factor: 0.0,
            max_delay_ms: None,
        };
        let mut scheduler = RetryScheduler::new(config);

        // attempt 1 → 1 * 1000 = 1000
        assert_eq!(scheduler.next_delay_ms().unwrap(), 1000);
        // attempt 2 → 2 * 1000 = 2000
        assert_eq!(scheduler.next_delay_ms().unwrap(), 2000);
        // attempt 3 → 3 * 1000 = 3000
        assert_eq!(scheduler.next_delay_ms().unwrap(), 3000);
        // attempt 4 → 4 * 1000 = 4000
        assert_eq!(scheduler.next_delay_ms().unwrap(), 4000);
        // attempt 5 → 不可重试
        assert_eq!(scheduler.next_delay_ms(), None);
    }

    // ─── Exponential 退避 ───

    #[test]
    fn exponential_retry_no_jitter() {
        let config = RetryConfig {
            delay_ms: 1000,
            backoff: RetryStrategy::Exponential,
            max_attempts: 6,
            jitter_factor: 0.0,
            max_delay_ms: None,
        };
        let mut scheduler = RetryScheduler::new(config);

        // attempt 1 → 1000 * 2^0 = 1000
        assert_eq!(scheduler.next_delay_ms().unwrap(), 1000);
        // attempt 2 → 1000 * 2^1 = 2000
        assert_eq!(scheduler.next_delay_ms().unwrap(), 2000);
        // attempt 3 → 1000 * 2^2 = 4000
        assert_eq!(scheduler.next_delay_ms().unwrap(), 4000);
        // attempt 4 → 1000 * 2^3 = 8000
        assert_eq!(scheduler.next_delay_ms().unwrap(), 8000);
        // attempt 5 → 1000 * 2^4 = 16000
        assert_eq!(scheduler.next_delay_ms().unwrap(), 16000);
        // attempt 6 → 不可重试
        assert_eq!(scheduler.next_delay_ms(), None);
    }

    // ─── Jitter ───

    #[test]
    fn retry_with_jitter_range() {
        let config = RetryConfig {
            delay_ms: 1000,
            backoff: RetryStrategy::Constant,
            max_attempts: 100,
            jitter_factor: 0.2,
            max_delay_ms: None,
        };
        let mut scheduler = RetryScheduler::new(config);

        // 采样多次确保在合理范围
        for _ in 0..50 {
            if let Some(delay) = scheduler.next_delay_ms() {
                // jitter_factor=0.2 → [800, 1200]
                assert!(
                    delay >= 800 && delay <= 1200,
                    "delay {delay} not in [800, 1200]"
                );
            }
        }
    }

    // ─── Max Delay Cap ───

    #[test]
    fn retry_max_delay_cap() {
        let config = RetryConfig {
            delay_ms: 1000,
            backoff: RetryStrategy::Exponential,
            max_attempts: 10,
            jitter_factor: 0.0,
            max_delay_ms: Some(5000),
        };
        let mut scheduler = RetryScheduler::new(config);

        // attempt 1 → 1000 (under cap)
        assert_eq!(scheduler.next_delay_ms().unwrap(), 1000);
        // attempt 2 → 2000 (under cap)
        assert_eq!(scheduler.next_delay_ms().unwrap(), 2000);
        // attempt 3 → 4000 (under cap)
        assert_eq!(scheduler.next_delay_ms().unwrap(), 4000);
        // attempt 4 → 8000 → capped at 5000
        assert_eq!(scheduler.next_delay_ms().unwrap(), 5000);
        // attempt 5 → 16000 → capped at 5000
        assert_eq!(scheduler.next_delay_ms().unwrap(), 5000);
    }

    // ─── can_retry ───

    #[test]
    fn retry_can_retry_false_at_limit() {
        let config = RetryConfig {
            delay_ms: 100,
            backoff: RetryStrategy::Constant,
            max_attempts: 1,
            jitter_factor: 0.0,
            max_delay_ms: None,
        };
        let scheduler = RetryScheduler::new(config);
        // max_attempts=1，首次执行即不可重试
        assert!(!scheduler.can_retry());
    }

    #[test]
    fn retry_can_retry_true_before_limit() {
        let config = RetryConfig {
            delay_ms: 100,
            backoff: RetryStrategy::Constant,
            max_attempts: 3,
            jitter_factor: 0.0,
            max_delay_ms: None,
        };
        let mut scheduler = RetryScheduler::new(config);
        assert!(scheduler.can_retry());
        scheduler.next_delay_ms();
        assert!(scheduler.can_retry());
        scheduler.next_delay_ms();
        assert!(!scheduler.can_retry());
    }

    // ─── From RetryPolicy ───

    #[test]
    fn retry_from_retry_policy_constant() {
        let policy = RetryPolicy {
            delay: "PT5S".to_string(),
            backoff: Some(BackoffStrategy::Constant),
            limit: 3,
            jitter: None,
        };
        let config = RetryConfig::from_policy(&policy, None);
        assert_eq!(config.delay_ms, 5000);
        assert_eq!(config.backoff, RetryStrategy::Constant);
        assert_eq!(config.max_attempts, 3);
        assert_eq!(config.jitter_factor, 0.0);
    }

    #[test]
    fn retry_from_retry_policy_exponential_with_jitter() {
        let policy = RetryPolicy {
            delay: "PT1S".to_string(),
            backoff: Some(BackoffStrategy::Exponential),
            limit: 5,
            jitter: Some(crate::workflow::model::JitterConfig { factor: 0.1 }),
        };
        let config = RetryConfig::from_policy(&policy, None);
        assert_eq!(config.delay_ms, 1000);
        assert_eq!(config.backoff, RetryStrategy::Exponential);
        assert_eq!(config.max_attempts, 5);
        assert_eq!(config.jitter_factor, 0.1);
    }

    // ─── Default ───

    #[test]
    fn default_retry_config() {
        let config = RetryConfig::default();
        assert_eq!(config.delay_ms, 3000);
        assert_eq!(config.backoff, RetryStrategy::Constant);
        assert_eq!(config.max_attempts, 3);
        assert_eq!(config.jitter_factor, 0.0);
        assert_eq!(config.max_delay_ms, None);
    }

    // ─── Reset ───

    #[test]
    fn retry_scheduler_reset() {
        let config = RetryConfig {
            delay_ms: 1000,
            backoff: RetryStrategy::Linear,
            max_attempts: 5,
            jitter_factor: 0.0,
            max_delay_ms: None,
        };
        let mut scheduler = RetryScheduler::new(config);

        scheduler.next_delay_ms(); // attempt 1 → 2
        scheduler.next_delay_ms(); // attempt 2 → 3
        assert_eq!(scheduler.attempt(), 3);

        scheduler.reset();
        assert_eq!(scheduler.attempt(), 1);
        assert!(scheduler.can_retry());
    }

    // ─── Duration Parsing ───

    #[test]
    fn parse_duration_seconds() {
        assert_eq!(parse_duration_ms("PT30S"), Some(30_000));
        assert_eq!(parse_duration_ms("PT1S"), Some(1_000));
    }

    #[test]
    fn parse_duration_minutes() {
        assert_eq!(parse_duration_ms("PT5M"), Some(300_000));
    }

    #[test]
    fn parse_duration_hours() {
        assert_eq!(parse_duration_ms("PT1H"), Some(3_600_000));
    }

    #[test]
    fn parse_duration_days() {
        assert_eq!(parse_duration_ms("P1D"), Some(86_400_000));
    }

    #[test]
    fn parse_duration_weeks() {
        assert_eq!(parse_duration_ms("P1W"), Some(604_800_000));
    }

    #[test]
    fn parse_duration_combined() {
        let ms = parse_duration_ms("PT1H30M").unwrap();
        assert_eq!(ms, 3_600_000 + 1_800_000);
    }
}
