// TDD: 熔断器与限流服务测试 (Phase H-Resilience — RED→GREEN)
//
// v8.2 §4.13: 派生能力 — 特性开关、分布式限流、Saga 事务
//
// RED 阶段：CircuitBreakerService 和 RateLimiterService 尚未实现。

use std::time::Duration;

use coord_agent::services::circuit_breaker::{CircuitBreakerService, CircuitState};
use coord_agent::services::rate_limiter::{RateLimiterService, RateLimiterConfig};

// ════════════════════════════════════════════════
// Circuit Breaker Tests
// ════════════════════════════════════════════════

/// CB.1: 初始状态为 Closed
#[test]
fn test_circuit_breaker_initial_state() {
    let cb = CircuitBreakerService::new(3, Duration::from_secs(60));
    assert_eq!(cb.state(), CircuitState::Closed);
}

/// CB.2: 失败次数达到阈值时打开熔断器
#[test]
fn test_circuit_breaker_opens_after_threshold() {
    let cb = CircuitBreakerService::new(2, Duration::from_secs(60));

    assert!(cb.allow()); // 1st — ok
    cb.record_failure();
    assert!(cb.allow()); // 2nd — ok
    cb.record_failure();

    // 达到阈值 (2)，熔断器应打开
    assert_eq!(cb.state(), CircuitState::Open);
    assert!(!cb.allow(), "circuit should be open after threshold failures");
}

/// CB.3: 成功调用重置失败计数
#[test]
fn test_circuit_breaker_success_resets_count() {
    let cb = CircuitBreakerService::new(3, Duration::from_secs(60));

    cb.record_failure();
    cb.record_failure();
    cb.record_success(); // 重置计数

    // 仍需 3 次失败才会打开
    assert_eq!(cb.state(), CircuitState::Closed);
    assert!(cb.allow());
}

/// CB.4: Half-Open 状态下成功调用关闭熔断器
#[test]
fn test_circuit_breaker_half_open_to_closed() {
    let cb = CircuitBreakerService::new(1, Duration::from_millis(10));

    // 触发打开
    cb.record_failure();
    assert_eq!(cb.state(), CircuitState::Open);

    // 等待超时
    std::thread::sleep(Duration::from_millis(20));

    // Half-Open: 允许一次探测
    assert!(cb.allow());
    assert_eq!(cb.state(), CircuitState::HalfOpen);

    // 成功 → 关闭
    cb.record_success();
    assert_eq!(cb.state(), CircuitState::Closed);
}

/// CB.5: Half-Open 状态下失败重新打开熔断器
#[test]
fn test_circuit_breaker_half_open_failure_reopens() {
    let cb = CircuitBreakerService::new(1, Duration::from_millis(10));

    cb.record_failure();
    assert_eq!(cb.state(), CircuitState::Open);

    std::thread::sleep(Duration::from_millis(20));

    assert!(cb.allow());
    assert_eq!(cb.state(), CircuitState::HalfOpen);

    // 失败 → 重新打开
    cb.record_failure();
    assert_eq!(cb.state(), CircuitState::Open);
}

/// CB.6: 获取当前统计信息
#[test]
fn test_circuit_breaker_stats() {
    let cb = CircuitBreakerService::new(5, Duration::from_secs(30));

    cb.record_failure();
    cb.record_failure();
    cb.record_success();

    let stats = cb.stats();
    assert_eq!(stats.total_failures, 2);
    assert_eq!(stats.total_successes, 1);
    assert_eq!(stats.state, CircuitState::Closed);
}

// ════════════════════════════════════════════════
// Rate Limiter Tests
// ════════════════════════════════════════════════

/// RL.1: 令牌桶初始有满容量令牌
#[test]
fn test_rate_limiter_initial_tokens() {
    let config = RateLimiterConfig {
        max_tokens: 10,
        refill_rate: 1.0, // 1 token/sec
    };
    let rl = RateLimiterService::new(config);

    assert_eq!(rl.available_tokens(), 10);
    assert!(rl.try_acquire().is_ok());
}

/// RL.2: 令牌耗尽后拒绝请求
#[test]
fn test_rate_limiter_exhaustion() {
    let config = RateLimiterConfig {
        max_tokens: 3,
        refill_rate: 0.0, // no refill
    };
    let rl = RateLimiterService::new(config);

    assert!(rl.try_acquire().is_ok()); // 1
    assert!(rl.try_acquire().is_ok()); // 2
    assert!(rl.try_acquire().is_ok()); // 3
    assert!(rl.try_acquire().is_err(), "should be rate limited");
    assert_eq!(rl.available_tokens(), 0);
}

/// RL.3: 令牌按速率自动补充
#[test]
fn test_rate_limiter_refill() {
    let config = RateLimiterConfig {
        max_tokens: 10,
        refill_rate: 100.0, // 100 tokens/sec
    };
    let rl = RateLimiterService::new(config);

    // 耗尽令牌
    for _ in 0..10 {
        rl.try_acquire().unwrap();
    }
    assert_eq!(rl.available_tokens(), 0);

    // 等待补充
    std::thread::sleep(Duration::from_millis(50));

    // 应有 ~5 个令牌
    let tokens = rl.available_tokens();
    assert!(tokens > 0, "tokens should have refilled, got {}", tokens);
    assert!(tokens <= 10, "tokens should not exceed max");
}

/// RL.4: 多线程并发获取令牌
#[test]
fn test_rate_limiter_concurrent() {
    use std::sync::Arc;

    let config = RateLimiterConfig {
        max_tokens: 100,
        refill_rate: 1000.0,
    };
    let rl = Arc::new(RateLimiterService::new(config));

    let mut handles = vec![];
    for _ in 0..4 {
        let rl = rl.clone();
        handles.push(std::thread::spawn(move || {
            let mut acquired = 0;
            for _ in 0..25 {
                if rl.try_acquire().is_ok() {
                    acquired += 1;
                }
            }
            acquired
        }));
    }

    let total: u32 = handles.into_iter().map(|h| h.join().unwrap()).sum();
    // 由于没有 refill，4 线程 × 25 请求，最多获取 100 个令牌
    assert!(total <= 100, "total acquired {} should not exceed max", total);
    assert!(total > 0, "should acquire at least some tokens");
}

/// RL.5: 获取限流统计
#[test]
fn test_rate_limiter_stats() {
    let config = RateLimiterConfig {
        max_tokens: 5,
        refill_rate: 1.0,
    };
    let rl = RateLimiterService::new(config);

    rl.try_acquire().unwrap();
    rl.try_acquire().unwrap();
    let _ = rl.try_acquire();

    let stats = rl.stats();
    assert_eq!(stats.max_tokens, 5);
    assert!(stats.tokens_consumed >= 2);
}
