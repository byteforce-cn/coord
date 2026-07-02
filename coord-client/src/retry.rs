// coord-client: 重试策略
//
// 实现指数退避重试，区分不同错误类型的重试行为。

use std::time::Duration;

use crate::config::Config;

/// 重试决策：是否应该重试以及等待多久
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryDecision {
    /// 立即重试（用于 NotLeader 等可快速恢复的错误）
    RetryImmediately,
    /// 等待指定时长后重试（用于网络超时等）
    RetryAfter(Duration),
    /// 放弃重试（用于 Sealed、认证失败等不可恢复的错误）
    Abort,
}

/// 重试状态机：追踪重试次数和退避时间
#[derive(Debug)]
pub struct RetryState {
    /// 已重试次数
    attempts: u32,
    /// 允许的最大重试次数
    max_retries: u32,
    /// 当前退避间隔
    current_backoff: Duration,
    /// 初始退避间隔
    initial_backoff: Duration,
    /// 最大退避间隔
    max_backoff: Duration,
}

impl RetryState {
    /// 从 Config 创建新的重试状态
    pub fn new(config: &Config) -> Self {
        Self {
            attempts: 0,
            max_retries: config.max_retries,
            current_backoff: config.retry_initial_backoff,
            initial_backoff: config.retry_initial_backoff,
            max_backoff: config.retry_max_backoff,
        }
    }

    /// 记录一次重试，返回是否应该继续重试以及等待时长
    pub fn next_attempt(&mut self) -> Option<Duration> {
        if self.attempts >= self.max_retries {
            return None;
        }
        self.attempts += 1;
        let wait = self.current_backoff;
        // 指数退避：每次翻倍，上限 max_backoff
        self.current_backoff =
            std::cmp::min(self.current_backoff * 2, self.max_backoff);
        Some(wait)
    }

    /// 重置重试计数器（成功请求后调用）
    pub fn reset(&mut self) {
        self.attempts = 0;
        self.current_backoff = self.initial_backoff;
    }

    /// 当前重试次数
    pub fn attempts(&self) -> u32 {
        self.attempts
    }
}

/// 根据错误类型决定重试策略（ADP §10.3.2）
pub fn classify_error(error_msg: &str) -> RetryDecision {
    let msg = error_msg.to_lowercase();

    if msg.contains("not leader") || msg.contains("notleader") {
        RetryDecision::RetryImmediately
    } else if msg.contains("sealed") || msg.contains("unsealing") {
        RetryDecision::Abort
    } else if msg.contains("unavailable")
        || msg.contains("deadline exceeded")
        || msg.contains("timeout")
        || msg.contains("connection")
    {
        // 网络错误使用指数退避
        RetryDecision::RetryAfter(Duration::from_millis(100))
    } else if msg.contains("permission denied")
        || msg.contains("unauthenticated")
        || msg.contains("invalid")
    {
        RetryDecision::Abort
    } else {
        // 未知错误：谨慎重试一次
        RetryDecision::RetryAfter(Duration::from_millis(100))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_state_exponential_backoff() {
        let config = Config::new(vec!["localhost:50051".into()]);
        let mut state = RetryState::new(&config);

        // 第 1 次重试：100ms
        assert_eq!(state.next_attempt(), Some(Duration::from_millis(100)));
        // 第 2 次重试：200ms
        assert_eq!(state.next_attempt(), Some(Duration::from_millis(200)));
        // 第 3 次重试：400ms
        assert_eq!(state.next_attempt(), Some(Duration::from_millis(400)));
        // 第 4 次重试：800ms
        assert_eq!(state.next_attempt(), Some(Duration::from_millis(800)));
        // 第 5 次重试：1600ms
        assert_eq!(state.next_attempt(), Some(Duration::from_millis(1600)));
        // 第 6 次：超限，返回 None
        assert_eq!(state.next_attempt(), None);
    }

    #[test]
    fn test_retry_state_reset() {
        let config = Config::new(vec!["localhost:50051".into()]);
        let mut state = RetryState::new(&config);

        state.next_attempt(); // 100ms
        state.next_attempt(); // 200ms
        state.reset();

        // 重置后回到初始退避
        assert_eq!(state.next_attempt(), Some(Duration::from_millis(100)));
    }

    #[test]
    fn test_classify_not_leader() {
        let decision = classify_error("not leader; leader is Some(\"addr\")");
        assert_eq!(decision, RetryDecision::RetryImmediately);
    }

    #[test]
    fn test_classify_sealed() {
        let decision = classify_error("cluster is sealed");
        assert_eq!(decision, RetryDecision::Abort);
    }

    #[test]
    fn test_classify_unavailable() {
        let decision = classify_error("cluster unavailable: no leader");
        assert_eq!(decision, RetryDecision::RetryAfter(Duration::from_millis(100)));
    }

    #[test]
    fn test_classify_permission_denied() {
        let decision = classify_error("permission denied: admin required");
        assert_eq!(decision, RetryDecision::Abort);
    }
}
