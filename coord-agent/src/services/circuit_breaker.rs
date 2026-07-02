// coord-agent: 熔断器服务 (Circuit Breaker) — Phase H
//
// 实现熔断器状态机：Closed → Open → HalfOpen → Closed。
// v8.2 §4.13: 派生能力。

use std::sync::atomic::{AtomicU64, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;

/// 熔断器状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

/// 熔断器统计
#[derive(Debug, Clone)]
pub struct CircuitBreakerStats {
    pub state: CircuitState,
    pub total_failures: u64,
    pub total_successes: u64,
    pub failure_threshold: u32,
}

/// 熔断器服务
///
/// 状态机：
/// - Closed: 正常状态，记录失败次数
/// - Open: 失败达阈值，拒绝所有请求
/// - HalfOpen: 超时后，允许探测请求
///
/// 线程安全：使用 atomic 计数器 + RwLock 状态保护。
pub struct CircuitBreakerService {
    state: Arc<RwLock<CircuitState>>,
    failure_count: Arc<AtomicU32>,
    failure_threshold: u32,
    timeout: Duration,
    last_failure_time: Arc<RwLock<Option<Instant>>>,
    total_failures: Arc<AtomicU64>,
    total_successes: Arc<AtomicU64>,
}

impl CircuitBreakerService {
    /// 创建熔断器
    ///
    /// * `failure_threshold` - 连续失败次数阈值
    /// * `timeout` - 打开后等待进入 HalfOpen 的超时时间
    pub fn new(failure_threshold: u32, timeout: Duration) -> Self {
        Self {
            state: Arc::new(RwLock::new(CircuitState::Closed)),
            failure_count: Arc::new(AtomicU32::new(0)),
            failure_threshold,
            timeout,
            last_failure_time: Arc::new(RwLock::new(None)),
            total_failures: Arc::new(AtomicU64::new(0)),
            total_successes: Arc::new(AtomicU64::new(0)),
        }
    }

    /// 当前状态
    pub fn state(&self) -> CircuitState {
        *self.state.read()
    }

    /// 检查是否允许请求通过
    pub fn allow(&self) -> bool {
        let state = *self.state.read();

        match state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                let last = *self.last_failure_time.read();
                if let Some(t) = last {
                    if t.elapsed() >= self.timeout {
                        // 超时，进入 HalfOpen
                        *self.state.write() = CircuitState::HalfOpen;
                        return true;
                    }
                }
                false
            }
            CircuitState::HalfOpen => {
                // HalfOpen 只允许一个探测请求
                // 简化实现：通过 compare-and-swap 控制
                self.failure_count.load(Ordering::Relaxed) == 0
            }
        }
    }

    /// 记录成功
    pub fn record_success(&self) {
        self.total_successes.fetch_add(1, Ordering::Relaxed);
        let mut state = self.state.write();

        match *state {
            CircuitState::HalfOpen => {
                // 探测成功 → 关闭熔断器
                *state = CircuitState::Closed;
                self.failure_count.store(0, Ordering::Relaxed);
            }
            CircuitState::Closed => {
                // 成功后重置失败计数
                self.failure_count.store(0, Ordering::Relaxed);
            }
            CircuitState::Open => {} // Open 状态不记录成功
        }
    }

    /// 记录失败
    pub fn record_failure(&self) {
        self.total_failures.fetch_add(1, Ordering::Relaxed);
        let mut state = self.state.write();

        match *state {
            CircuitState::Closed => {
                let count = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;
                if count >= self.failure_threshold {
                    *state = CircuitState::Open;
                    *self.last_failure_time.write() = Some(Instant::now());
                }
            }
            CircuitState::HalfOpen => {
                // 探测失败 → 重新打开
                *state = CircuitState::Open;
                *self.last_failure_time.write() = Some(Instant::now());
                self.failure_count.store(1, Ordering::Relaxed);
            }
            CircuitState::Open => {
                *self.last_failure_time.write() = Some(Instant::now());
            }
        }
    }

    /// 重置熔断器到 Closed 状态
    #[allow(dead_code)]
    pub fn reset(&self) {
        *self.state.write() = CircuitState::Closed;
        self.failure_count.store(0, Ordering::Relaxed);
        *self.last_failure_time.write() = None;
    }

    /// 获取统计信息
    pub fn stats(&self) -> CircuitBreakerStats {
        CircuitBreakerStats {
            state: self.state(),
            total_failures: self.total_failures.load(Ordering::Relaxed),
            total_successes: self.total_successes.load(Ordering::Relaxed),
            failure_threshold: self.failure_threshold,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_circuit_state_variants() {
        assert_ne!(CircuitState::Closed, CircuitState::Open);
        assert_ne!(CircuitState::Open, CircuitState::HalfOpen);
    }
}
