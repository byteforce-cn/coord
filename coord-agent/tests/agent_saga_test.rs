// TDD: Saga 事务服务测试 (Phase H — 待实施)
//
// v8.2 §4.13: Saga 事务 — Workflow 原生编排，自动补偿。
//
// RED stage: SagaService 尚未定义

use coord_agent::saga::{SagaService, SagaConfig, SagaStep, SagaState, SagaContext, StepResult};

/// 验证 SagaConfig 默认值
#[test]
fn test_saga_config_defaults() {
    let config = SagaConfig::default();
    assert_eq!(config.retry_max_attempts, 3);
    assert_eq!(config.retry_backoff_ms, 1000);
}

/// 验证简单 Saga 成功执行
#[test]
fn test_saga_successful_execution() {
    let svc = SagaService::new(SagaConfig::default());

    let saga_id = "test-saga-001";
    let mut ctx = SagaContext::new(saga_id);

    // 定义步骤
    svc.add_step(saga_id, SagaStep {
        name: "step1-reserve".to_string(),
        action: Box::new(|ctx: &mut SagaContext| {
            ctx.set("step1", "done");
            Ok(())
        }),
        compensation: Box::new(|ctx: &mut SagaContext| {
            ctx.set("step1", "rolled_back");
            Ok(())
        }),
    });

    svc.add_step(saga_id, SagaStep {
        name: "step2-confirm".to_string(),
        action: Box::new(|ctx: &mut SagaContext| {
            ctx.set("step2", "done");
            Ok(())
        }),
        compensation: Box::new(|ctx: &mut SagaContext| {
            ctx.set("step2", "rolled_back");
            Ok(())
        }),
    });

    // 执行 Saga
    let result = svc.execute(saga_id, &mut ctx).expect("执行失败");
    assert!(matches!(result, StepResult::Completed));
    assert_eq!(ctx.get("step1"), Some(&"done".to_string()));
    assert_eq!(ctx.get("step2"), Some(&"done".to_string()));
}

/// 验证 Saga 失败时自动补偿
#[test]
fn test_saga_compensation_on_failure() {
    let svc = SagaService::new(SagaConfig::default());
    let saga_id = "test-saga-002";
    let mut ctx = SagaContext::new(saga_id);

    svc.add_step(saga_id, SagaStep {
        name: "step1-ok".to_string(),
        action: Box::new(|ctx: &mut SagaContext| {
            ctx.set("step1", "ok");
            Ok(())
        }),
        compensation: Box::new(|ctx: &mut SagaContext| {
            ctx.set("step1", "compensated");
            Ok(())
        }),
    });

    svc.add_step(saga_id, SagaStep {
        name: "step2-fail".to_string(),
        action: Box::new(|_ctx: &mut SagaContext| {
            Err("step2 intentional failure".to_string())
        }),
        compensation: Box::new(|ctx: &mut SagaContext| {
            ctx.set("step2", "compensated");
            Ok(())
        }),
    });

    svc.add_step(saga_id, SagaStep {
        name: "step3-never-runs".to_string(),
        action: Box::new(|ctx: &mut SagaContext| {
            ctx.set("step3", "should_not_happen");
            Ok(())
        }),
        compensation: Box::new(|ctx: &mut SagaContext| {
            ctx.set("step3", "compensated");
            Ok(())
        }),
    });

    let result = svc.execute(saga_id, &mut ctx).expect("执行失败");
    assert!(matches!(result, StepResult::Failed { .. }));

    // 已执行的步骤应被补偿
    assert_eq!(ctx.get("step1"), Some(&"compensated".to_string()));
    // 失败的步骤也应被补偿
    assert_eq!(ctx.get("step2"), Some(&"compensated".to_string()));
    // 未执行的步骤不应运行补偿
    assert_eq!(ctx.get("step3"), None);
}

/// 验证 Saga 状态跟踪
#[test]
fn test_saga_state_tracking() {
    let svc = SagaService::new(SagaConfig::default());
    let saga_id = "test-saga-003";

    svc.add_step(saga_id, SagaStep {
        name: "simple-step".to_string(),
        action: Box::new(|ctx: &mut SagaContext| {
            ctx.set("key", "value");
            Ok(())
        }),
        compensation: Box::new(|_ctx: &mut SagaContext| Ok(())),
    });

    // 执行前状态
    let state = svc.get_state(saga_id).expect("获取状态失败");
    assert!(matches!(state, SagaState::Pending));

    // 执行
    let mut ctx = SagaContext::new(saga_id);
    svc.execute(saga_id, &mut ctx).expect("执行失败");

    let state = svc.get_state(saga_id).expect("获取状态失败");
    assert!(matches!(state, SagaState::Completed));
}

/// 验证重试机制
#[test]
fn test_saga_retry_on_transient_failure() {
    let svc = SagaService::new(SagaConfig {
        retry_max_attempts: 3,
        retry_backoff_ms: 10,
    });
    let saga_id = "test-saga-retry";

    // 使用计数器模拟瞬态故障
    let attempts = std::sync::atomic::AtomicU32::new(0);

    svc.add_step(saga_id, SagaStep {
        name: "flaky-step".to_string(),
        action: Box::new(move |_ctx: &mut SagaContext| {
            let n = attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n < 2 {
                Err("transient error".to_string())
            } else {
                Ok(())
            }
        }),
        compensation: Box::new(|_ctx: &mut SagaContext| Ok(())),
    });

    let mut ctx = SagaContext::new(saga_id);
    let result = svc.execute(saga_id, &mut ctx).expect("执行失败");
    assert!(matches!(result, StepResult::Completed));
}
