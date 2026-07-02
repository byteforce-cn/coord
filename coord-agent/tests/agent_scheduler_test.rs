// TDD: 分布式调度服务测试 (Phase H-DistScheduler — RED)
//
// 验证 SchedulerService 能够：
// 1. 注册/注销定时任务
// 2. 任务认领（基于 KV CAS + Lease 防止重复执行）
// 3. Exactly-Once 执行保证
// 4. 惊群缓解（随机退避）
// 5. 任务状态查询
//
// v8.2 §4.9: 任务认领机制，Exactly-Once 内部状态，惊群缓解
//
// RED 阶段：SchedulerService 尚未实现，这些测试预期失败。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use coord_agent::service::{BaseService, ServiceConfig};
use coord_agent::services::scheduler::{
    SchedulerService, ScheduleTask, TaskClaim, TaskState, TaskType,
};

// ──── T1: 服务注册 ────

/// H-Sched.1: SchedulerService 实现 BaseService trait
#[test]
fn test_scheduler_service_implements_base_service() {
    let svc = SchedulerService::new(Default::default());
    assert!(!svc.name().is_empty());
    assert!(svc.health_check());
}

/// H-Sched.2: SchedulerService 可通过 ServiceConfig 配置启用
#[test]
fn test_scheduler_service_config() {
    let mut config = ServiceConfig::default();
    config.scheduler = true;
    assert!(config.scheduler);
}

// ──── T2: 任务注册管理 ────

/// H-Sched.3: 注册定时任务
#[test]
fn test_register_scheduled_task() {
    let svc = SchedulerService::new(Default::default());

    let task = ScheduleTask {
        task_id: "cleanup-job".into(),
        task_type: TaskType::Cron {
            expression: "*/5 * * * *".into(),
        },
        description: "Cleanup expired sessions".into(),
        metadata: HashMap::new(),
    };

    let result = svc.register_task(task.clone());
    assert!(result.is_ok(), "register_task should succeed: {:?}", result.err());

    let tasks = svc.list_tasks();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].task_id, "cleanup-job");
}

/// H-Sched.4: 注册重复任务 ID 应失败
#[test]
fn test_register_duplicate_task_fails() {
    let svc = SchedulerService::new(Default::default());

    let task = ScheduleTask {
        task_id: "unique-job".into(),
        task_type: TaskType::FixedRate {
            interval_ms: 1000,
        },
        description: "Test job".into(),
        metadata: HashMap::new(),
    };

    assert!(svc.register_task(task.clone()).is_ok());
    assert!(svc.register_task(task).is_err());
}

/// H-Sched.5: 注销任务
#[test]
fn test_deregister_task() {
    let svc = SchedulerService::new(Default::default());

    let task = ScheduleTask {
        task_id: "temp-job".into(),
        task_type: TaskType::FixedDelay { delay_ms: 500 },
        description: "Temporary".into(),
        metadata: HashMap::new(),
    };

    svc.register_task(task).unwrap();
    assert_eq!(svc.list_tasks().len(), 1);

    svc.deregister_task("temp-job").unwrap();
    assert_eq!(svc.list_tasks().len(), 0);
}

// ──── T3: 任务认领（Claim）───

/// H-Sched.6: 未认领的任务可被认领
#[test]
fn test_claim_unclaimed_task() {
    let svc = SchedulerService::new(Default::default());

    let task = ScheduleTask {
        task_id: "claimable-job".into(),
        task_type: TaskType::Cron {
            expression: "0 * * * *".into(),
        },
        description: "Claimable".into(),
        metadata: HashMap::new(),
    };
    svc.register_task(task).unwrap();

    let claim = svc.try_claim("claimable-job", "worker-1").unwrap();
    assert!(claim.is_some(), "unclaimed task should be claimable");
    let claim = claim.unwrap();
    assert_eq!(claim.task_id, "claimable-job");
    assert_eq!(claim.worker_id, "worker-1");
    assert_eq!(claim.state, TaskState::Running);
}

/// H-Sched.7: 已认领的任务不可被其他 worker 重复认领
#[test]
fn test_claim_already_claimed_task_fails() {
    let svc = SchedulerService::new(Default::default());

    let task = ScheduleTask {
        task_id: "exclusive-job".into(),
        task_type: TaskType::FixedRate { interval_ms: 1000 },
        description: "Exclusive".into(),
        metadata: HashMap::new(),
    };
    svc.register_task(task).unwrap();

    // worker-1 认领成功
    let claim1 = svc.try_claim("exclusive-job", "worker-1").unwrap();
    assert!(claim1.is_some());

    // worker-2 认领同一任务应失败
    let claim2 = svc.try_claim("exclusive-job", "worker-2").unwrap();
    assert!(claim2.is_none(), "already claimed task should not be re-claimed");
}

/// H-Sched.8: worker 可以释放已认领的任务
#[test]
fn test_release_claim() {
    let svc = SchedulerService::new(Default::default());

    let task = ScheduleTask {
        task_id: "releasable-job".into(),
        task_type: TaskType::Cron {
            expression: "*/10 * * * *".into(),
        },
        description: "Releasable".into(),
        metadata: HashMap::new(),
    };
    svc.register_task(task).unwrap();

    let claim = svc.try_claim("releasable-job", "worker-1").unwrap().unwrap();
    assert_eq!(claim.state, TaskState::Running);

    svc.release_claim("releasable-job", "worker-1").unwrap();

    // 释放后应可被其他 worker 认领
    let claim2 = svc.try_claim("releasable-job", "worker-2").unwrap();
    assert!(claim2.is_some(), "released task should be re-claimable");
}

// ──── T4: Exactly-Once 执行 ────

/// H-Sched.9: 任务完成状态变更
#[test]
fn test_mark_task_completed() {
    let svc = SchedulerService::new(Default::default());

    let task = ScheduleTask {
        task_id: "complete-me".into(),
        task_type: TaskType::Once,
        description: "One-shot".into(),
        metadata: HashMap::new(),
    };
    svc.register_task(task).unwrap();

    let claim = svc.try_claim("complete-me", "worker-1").unwrap().unwrap();
    assert_eq!(claim.state, TaskState::Running);

    svc.mark_completed("complete-me", "worker-1").unwrap();

    let state = svc.get_task_state("complete-me").unwrap();
    assert_eq!(state, TaskState::Completed);

    // 已完成任务不可再被认领
    let re_claim = svc.try_claim("complete-me", "worker-2").unwrap();
    assert!(re_claim.is_none(), "completed task should not be re-claimable");
}

/// H-Sched.10: 任务失败状态变更
#[test]
fn test_mark_task_failed() {
    let svc = SchedulerService::new(Default::default());

    let task = ScheduleTask {
        task_id: "fail-me".into(),
        task_type: TaskType::FixedRate { interval_ms: 1000 },
        description: "Will fail".into(),
        metadata: HashMap::new(),
    };
    svc.register_task(task).unwrap();

    svc.try_claim("fail-me", "worker-1").unwrap();
    svc.mark_failed("fail-me", "worker-1", "simulated error").unwrap();

    let state = svc.get_task_state("fail-me").unwrap();
    assert_eq!(state, TaskState::Pending); // FixedRate 失败后回到 Pending 等待重试

    // 固定频率任务失败后应可重试（下次调度时重新认领）
    let re_claim = svc.try_claim("fail-me", "worker-1").unwrap();
    assert!(re_claim.is_some(), "failed FixedRate task should be re-claimable");
}

// ──── T5: 惊群缓解 ────

/// H-Sched.11: 多个 worker 竞争同一任务时仅一个获胜
#[test]
fn test_thundering_herd_only_one_wins() {
    let svc = Arc::new(SchedulerService::new(Default::default()));

    let task = ScheduleTask {
        task_id: "hot-job".into(),
        task_type: TaskType::Cron {
            expression: "* * * * *".into(),
        },
        description: "Hot task".into(),
        metadata: HashMap::new(),
    };
    svc.register_task(task).unwrap();

    // 模拟 10 个 worker 同时竞争
    let svc_ref = svc.clone();
    let mut success_count = 0;
    for i in 0..10 {
        let worker_id = format!("worker-{}", i);
        if svc_ref.try_claim("hot-job", &worker_id).unwrap().is_some() {
            success_count += 1;
        }
    }

    assert_eq!(success_count, 1, "only one worker should claim the task");
}

/// H-Sched.12: 惊群退避延迟在合理范围
#[test]
fn test_backoff_delay_range() {
    let svc = SchedulerService::new(Default::default());

    // 验证退避延迟在 0..max_backoff_ms 范围内
    let backoff = svc.compute_backoff_ms(10); // 10 个竞争者
    assert!(backoff <= 5000, "backoff should not exceed max");
    assert!(backoff >= 0, "backoff should be non-negative");

    // 竞争者越少，退避越小
    let backoff_few = svc.compute_backoff_ms(2);
    let backoff_many = svc.compute_backoff_ms(100);
    assert!(backoff_few <= backoff_many, "more competitors → more backoff");
}

// ──── T6: 任务状态查询 ────

/// H-Sched.13: 查询所有任务状态
#[test]
fn test_query_all_task_states() {
    let svc = SchedulerService::new(Default::default());

    for i in 0..5 {
        svc.register_task(ScheduleTask {
            task_id: format!("job-{}", i),
            task_type: TaskType::FixedRate { interval_ms: 1000 },
            description: format!("Job {}", i),
            metadata: HashMap::new(),
        }).unwrap();
    }

    let states = svc.list_task_states();
    assert_eq!(states.len(), 5);
    for (_, state) in &states {
        assert_eq!(*state, TaskState::Pending);
    }
}

/// H-Sched.14: 查询单个任务详情
#[test]
fn test_query_task_detail() {
    let svc = SchedulerService::new(Default::default());

    let task = ScheduleTask {
        task_id: "detailed-job".into(),
        task_type: TaskType::Cron {
            expression: "0 0 * * *".into(),
        },
        description: "Daily cleanup".into(),
        metadata: {
            let mut m = HashMap::new();
            m.insert("priority".into(), "high".into());
            m
        },
    };
    svc.register_task(task).unwrap();

    let detail = svc.get_task_detail("detailed-job").unwrap();
    assert_eq!(detail.task_id, "detailed-job");
    assert_eq!(detail.task_type, TaskType::Cron {
        expression: "0 0 * * *".into(),
    });
    assert_eq!(detail.description, "Daily cleanup");
    assert_eq!(detail.metadata.get("priority").unwrap(), "high");
}

// ──── T7: Lease 续期（心跳）───

/// H-Sched.15: 任务认领后应自动续期
#[test]
fn test_claim_heartbeat_renewal() {
    let svc = SchedulerService::new(Default::default());

    let task = ScheduleTask {
        task_id: "heartbeat-job".into(),
        task_type: TaskType::FixedRate { interval_ms: 5000 },
        description: "Needs heartbeat".into(),
        metadata: HashMap::new(),
    };
    svc.register_task(task).unwrap();

    let claim = svc.try_claim("heartbeat-job", "worker-1").unwrap().unwrap();

    // 心跳续期
    let renewed = svc.renew_claim("heartbeat-job", "worker-1").unwrap();
    assert!(renewed, "heartbeat should succeed for active claim");

    // 其他 worker 不能为他人续期
    let wrong_renew = svc.renew_claim("heartbeat-job", "worker-2").unwrap();
    assert!(!wrong_renew, "wrong worker should not renew");
}

// ──── T8: 过期认领自动释放 ────

/// H-Sched.16: 过期认领应可被其他 worker 重新认领
#[test]
fn test_expired_claim_reclaimable() {
    let svc = SchedulerService::new_with_ttl(Duration::from_millis(1)); // 1ms TTL

    let task = ScheduleTask {
        task_id: "expire-job".into(),
        task_type: TaskType::Once,
        description: "Will expire".into(),
        metadata: HashMap::new(),
    };
    svc.register_task(task).unwrap();

    svc.try_claim("expire-job", "worker-1").unwrap();

    // 等待认领过期
    std::thread::sleep(Duration::from_millis(10));

    // 过期后其他 worker 可认领
    let claim2 = svc.try_claim("expire-job", "worker-2").unwrap();
    assert!(claim2.is_some(), "expired claim should be re-claimable");
    assert_eq!(claim2.unwrap().worker_id, "worker-2");
}
