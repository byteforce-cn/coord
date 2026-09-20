// 分布式调度服务测试
//
// 验证 SchedulerService 能够：
// 1. 注册/注销定时任务
// 2. 任务认领（基于 KV CAS 防止重复执行）
// 3. Exactly-Once 执行保证
// 4. 惊群缓解（随机退避）
// 5. 任务状态查询
//
// ⚠️ 本文件原为 RED 阶段的 TDD 草稿（注释称"SchedulerService 尚未实现"），
// 且断言的是**同步内存实现**的 API。计划书 P0-10 / E9 整改后：
// - 状态迁至 `SchedulerStore`（生产 = coord-server 共享 KV），全部方法**异步**；
// - `list_tasks` / `get_task_state` / `list_task_states` / `get_task_detail`
//   返回 `ServiceResult<_>`（KV 可能失败，不能再假装"必然成功"）；
// - 新增 claim 句柄路径（`*_any`）与"共享 store 重启存续"用例。
// 语义断言**逐条保留**，不放宽。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use coord_agent::service::{BaseService, ServiceConfig};
use coord_agent::services::scheduler::{ScheduleTask, SchedulerService, TaskState, TaskType};
use coord_agent::services::scheduler_store::{MemorySchedulerStore, SchedulerStore};

/// 共享 store 的测试装配（用于"重启存续"类用例）
fn svc_with_ttl(ttl: Duration) -> (SchedulerService, Arc<dyn SchedulerStore>) {
    let store: Arc<dyn SchedulerStore> = Arc::new(MemorySchedulerStore::new());
    let svc = SchedulerService::with_store(Arc::clone(&store), ttl);
    (svc, store)
}

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
    let config = ServiceConfig {
        scheduler: true,
        ..Default::default()
    };
    assert!(config.scheduler);
}

// ──── T2: 任务注册管理 ────

/// H-Sched.3: 注册定时任务
#[tokio::test]
async fn test_register_scheduled_task() {
    let svc = SchedulerService::new(Default::default());

    let task = ScheduleTask {
        task_id: "cleanup-job".into(),
        task_type: TaskType::Cron {
            expression: "*/5 * * * *".into(),
        },
        description: "Cleanup expired sessions".into(),
        metadata: HashMap::new(),
    };

    let result = svc.register_task(task.clone()).await;
    assert!(
        result.is_ok(),
        "register_task should succeed: {:?}",
        result.err()
    );

    let tasks = svc.list_tasks().await.unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].task_id, "cleanup-job");
}

/// H-Sched.4: 注册重复任务 ID 应失败
#[tokio::test]
async fn test_register_duplicate_task_fails() {
    let svc = SchedulerService::new(Default::default());

    let task = ScheduleTask {
        task_id: "unique-job".into(),
        task_type: TaskType::FixedRate { interval_ms: 1000 },
        description: "Test job".into(),
        metadata: HashMap::new(),
    };

    assert!(svc.register_task(task.clone()).await.is_ok());
    assert!(svc.register_task(task).await.is_err());
}

/// H-Sched.5: 注销任务
#[tokio::test]
async fn test_deregister_task() {
    let svc = SchedulerService::new(Default::default());

    let task = ScheduleTask {
        task_id: "temp-job".into(),
        task_type: TaskType::FixedDelay { delay_ms: 500 },
        description: "Temporary".into(),
        metadata: HashMap::new(),
    };

    svc.register_task(task).await.unwrap();
    assert_eq!(svc.list_tasks().await.unwrap().len(), 1);

    svc.deregister_task("temp-job").await.unwrap();
    assert_eq!(svc.list_tasks().await.unwrap().len(), 0);
}

// ──── T3: 任务认领（Claim）───

/// H-Sched.6: 未认领的任务可被认领
#[tokio::test]
async fn test_claim_unclaimed_task() {
    let svc = SchedulerService::new(Default::default());

    let task = ScheduleTask {
        task_id: "claimable-job".into(),
        task_type: TaskType::Cron {
            expression: "0 * * * *".into(),
        },
        description: "Claimable".into(),
        metadata: HashMap::new(),
    };
    svc.register_task(task).await.unwrap();

    let claim = svc.try_claim("claimable-job", "worker-1").await.unwrap();
    assert!(claim.is_some(), "unclaimed task should be claimable");
    let claim = claim.unwrap();
    assert_eq!(claim.task_id, "claimable-job");
    assert_eq!(claim.worker_id, "worker-1");
    assert_eq!(claim.state, TaskState::Running);
}

/// H-Sched.7: 已认领的任务不可被其他 worker 重复认领
#[tokio::test]
async fn test_claim_already_claimed_task_fails() {
    let svc = SchedulerService::new(Default::default());

    let task = ScheduleTask {
        task_id: "exclusive-job".into(),
        task_type: TaskType::FixedRate { interval_ms: 1000 },
        description: "Exclusive".into(),
        metadata: HashMap::new(),
    };
    svc.register_task(task).await.unwrap();

    // worker-1 认领成功
    let claim1 = svc.try_claim("exclusive-job", "worker-1").await.unwrap();
    assert!(claim1.is_some());

    // worker-2 认领同一任务应失败
    let claim2 = svc.try_claim("exclusive-job", "worker-2").await.unwrap();
    assert!(
        claim2.is_none(),
        "already claimed task should not be re-claimed"
    );
}

/// H-Sched.8: worker 可以释放已认领的任务
#[tokio::test]
async fn test_release_claim() {
    let svc = SchedulerService::new(Default::default());

    let task = ScheduleTask {
        task_id: "releasable-job".into(),
        task_type: TaskType::Cron {
            expression: "*/10 * * * *".into(),
        },
        description: "Releasable".into(),
        metadata: HashMap::new(),
    };
    svc.register_task(task).await.unwrap();

    let claim = svc
        .try_claim("releasable-job", "worker-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.state, TaskState::Running);

    svc.release_claim("releasable-job", "worker-1")
        .await
        .unwrap();

    // 释放后应可被其他 worker 认领
    let claim2 = svc.try_claim("releasable-job", "worker-2").await.unwrap();
    assert!(claim2.is_some(), "released task should be re-claimable");
}

// ──── T4: Exactly-Once 执行 ────

/// H-Sched.9: 任务完成状态变更
#[tokio::test]
async fn test_mark_task_completed() {
    let svc = SchedulerService::new(Default::default());

    let task = ScheduleTask {
        task_id: "complete-me".into(),
        task_type: TaskType::Once,
        description: "One-shot".into(),
        metadata: HashMap::new(),
    };
    svc.register_task(task).await.unwrap();

    let claim = svc
        .try_claim("complete-me", "worker-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.state, TaskState::Running);

    svc.mark_completed("complete-me", "worker-1").await.unwrap();

    let state = svc.get_task_state("complete-me").await.unwrap().unwrap();
    assert_eq!(state, TaskState::Completed);

    // 已完成任务不可再被认领
    let re_claim = svc.try_claim("complete-me", "worker-2").await.unwrap();
    assert!(
        re_claim.is_none(),
        "completed task should not be re-claimable"
    );
}

/// H-Sched.10: 任务失败状态变更
#[tokio::test]
async fn test_mark_task_failed() {
    let svc = SchedulerService::new(Default::default());

    let task = ScheduleTask {
        task_id: "fail-me".into(),
        task_type: TaskType::FixedRate { interval_ms: 1000 },
        description: "Will fail".into(),
        metadata: HashMap::new(),
    };
    svc.register_task(task).await.unwrap();

    svc.try_claim("fail-me", "worker-1").await.unwrap();
    svc.mark_failed("fail-me", "worker-1", "simulated error")
        .await
        .unwrap();

    let state = svc.get_task_state("fail-me").await.unwrap().unwrap();
    assert_eq!(state, TaskState::Pending); // FixedRate 失败后回到 Pending 等待重试

    // 固定频率任务失败后应可重试（下次调度时重新认领）
    let re_claim = svc.try_claim("fail-me", "worker-1").await.unwrap();
    assert!(
        re_claim.is_some(),
        "failed FixedRate task should be re-claimable"
    );
}

// ──── T5: 惊群缓解 ────

/// H-Sched.11: 多个 worker 竞争同一任务时仅一个获胜
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_thundering_herd_only_one_wins() {
    let svc = Arc::new(SchedulerService::new(Default::default()));

    let task = ScheduleTask {
        task_id: "hot-job".into(),
        task_type: TaskType::Cron {
            expression: "* * * * *".into(),
        },
        description: "Hot task".into(),
        metadata: HashMap::new(),
    };
    svc.register_task(task).await.unwrap();

    // 模拟 10 个 worker **并发**竞争（CAS 路径必须保证只有一个成功）
    let mut handles = Vec::new();
    for i in 0..10 {
        let svc = Arc::clone(&svc);
        handles.push(tokio::spawn(async move {
            svc.try_claim("hot-job", &format!("worker-{i}")).await
        }));
    }
    let mut success_count = 0;
    for h in handles {
        let r = h.await.expect("join").expect("no store error");
        if r.is_some() {
            success_count += 1;
        }
    }

    assert_eq!(success_count, 1, "only one worker should claim the task");
}

/// H-Sched.12: 惊群退避延迟在合理范围
#[test]
fn test_backoff_delay_range() {
    let svc = SchedulerService::new(Default::default());

    // 验证退避延迟不超过上限（u64 天然非负，无需冗余的 `>= 0` 断言——
    // 该断言会被 clippy::absurd_extreme_comparisons 判为恒真）。
    let backoff = svc.compute_backoff_ms(10); // 10 个竞争者
    assert!(backoff <= 5000, "backoff should not exceed max");

    // 竞争者越少，退避上限越小。
    // 注意：返回值为随机采样（0..=range），不能比较两次采样的大小（会偶发抖动），
    // 因此按确定性上限断言：单一竞争者无退避；2 个竞争者上限 100ms；100 个竞争者
    // 封顶 5000ms。
    assert_eq!(
        svc.compute_backoff_ms(1),
        0,
        "single competitor → no backoff"
    );
    assert!(
        svc.compute_backoff_ms(2) <= 100,
        "2 competitors → backoff <= 100"
    );
    assert!(
        svc.compute_backoff_ms(100) <= 5000,
        "100 competitors → backoff <= 5000"
    );
}

// ──── T6: 任务状态查询 ────

/// H-Sched.13: 查询所有任务状态
#[tokio::test]
async fn test_query_all_task_states() {
    let svc = SchedulerService::new(Default::default());

    for i in 0..5 {
        svc.register_task(ScheduleTask {
            task_id: format!("job-{}", i),
            task_type: TaskType::FixedRate { interval_ms: 1000 },
            description: format!("Job {}", i),
            metadata: HashMap::new(),
        })
        .await
        .unwrap();
    }

    let states = svc.list_task_states().await.unwrap();
    assert_eq!(states.len(), 5);
    for state in states.values() {
        assert_eq!(*state, TaskState::Pending);
    }
}

/// H-Sched.14: 查询单个任务详情
#[tokio::test]
async fn test_query_task_detail() {
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
    svc.register_task(task).await.unwrap();

    let detail = svc.get_task_detail("detailed-job").await.unwrap().unwrap();
    assert_eq!(detail.task_id, "detailed-job");
    assert_eq!(
        detail.task_type,
        TaskType::Cron {
            expression: "0 0 * * *".into(),
        }
    );
    assert_eq!(detail.description, "Daily cleanup");
    assert_eq!(detail.metadata.get("priority").unwrap(), "high");
}

// ──── T7: Lease 续期（心跳）───

/// H-Sched.15: 任务认领后应自动续期
#[tokio::test]
async fn test_claim_heartbeat_renewal() {
    let svc = SchedulerService::new(Default::default());

    let task = ScheduleTask {
        task_id: "heartbeat-job".into(),
        task_type: TaskType::FixedRate { interval_ms: 5000 },
        description: "Needs heartbeat".into(),
        metadata: HashMap::new(),
    };
    svc.register_task(task).await.unwrap();

    let claim = svc
        .try_claim("heartbeat-job", "worker-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.state, TaskState::Running);

    // 心跳续期
    let renewed = svc.renew_claim("heartbeat-job", "worker-1").await.unwrap();
    assert!(renewed, "heartbeat should succeed for active claim");

    // 其他 worker 不能为他人续期
    let wrong_renew = svc.renew_claim("heartbeat-job", "worker-2").await.unwrap();
    assert!(!wrong_renew, "wrong worker should not renew");
}

// ──── T7b: claim 句柄路径（gRPC 面的真实语义）───

/// H-Sched.15b: wire 无 worker 身份 ⇒ 续期 / 完成走 `job_id` 句柄
///
/// 这条用例锁住的是**两处已修缺陷**：此前 gRPC handler 硬编码 `"worker"`，
/// 与认领时的随机 uuid 永不相等 ⇒ 续期静默失效、CompleteJob 必然报错。
#[tokio::test]
async fn test_claim_handle_renew_and_complete() {
    let svc = SchedulerService::new(Default::default());

    let task = ScheduleTask {
        task_id: "handle-job".into(),
        task_type: TaskType::Once,
        description: "Claim handle".into(),
        metadata: HashMap::new(),
    };
    svc.register_task(task).await.unwrap();

    // 模拟 gRPC ClaimJob：worker 标识由服务端生成，调用方只拿到 job_id
    let server_generated = uuid_like();
    svc.try_claim("handle-job", &server_generated)
        .await
        .unwrap();

    assert!(
        svc.renew_claim_any("handle-job").await.unwrap(),
        "按 claim 句柄续期必须成功"
    );
    svc.mark_completed_any("handle-job").await.unwrap();
    assert_eq!(
        svc.get_task_state("handle-job").await.unwrap(),
        Some(TaskState::Completed)
    );
}

// ──── T8: 过期认领自动释放 ────

/// H-Sched.16: 过期认领应可被其他 worker 重新认领
#[tokio::test]
async fn test_expired_claim_reclaimable() {
    let svc = SchedulerService::new_with_ttl(Duration::from_millis(1)); // 1ms TTL

    let task = ScheduleTask {
        task_id: "expire-job".into(),
        task_type: TaskType::Once,
        description: "Will expire".into(),
        metadata: HashMap::new(),
    };
    svc.register_task(task).await.unwrap();

    svc.try_claim("expire-job", "worker-1").await.unwrap();

    // 等待认领过期（异步等待，勿用 thread::sleep 阻塞 runtime）
    tokio::time::sleep(Duration::from_millis(20)).await;

    // 过期后其他 worker 可认领
    let claim2 = svc.try_claim("expire-job", "worker-2").await.unwrap();
    assert!(claim2.is_some(), "expired claim should be re-claimable");
    assert_eq!(claim2.unwrap().worker_id, "worker-2");
}

// ──── T9: 状态存续（P0-10 的验收核心）───

/// H-Sched.17: 服务实例重建后，任务定义 / 状态 / 认领必须全部存续
///
/// 这是"重启即丢全部调度状态"的直接回归守卫：旧实现用三个进程内 `HashMap`，
/// 换一个实例即全空。现在状态在 `SchedulerStore` 中，生产后端为 coord-server KV
/// （跨进程），本用例用共享 store 表达同一性质。
#[tokio::test]
async fn test_state_survives_across_service_instances() {
    let (svc1, store) = svc_with_ttl(Duration::from_secs(300));

    let task = ScheduleTask {
        task_id: "persist-job".into(),
        task_type: TaskType::FixedRate { interval_ms: 1000 },
        description: "Must survive".into(),
        metadata: HashMap::new(),
    };
    svc1.register_task(task).await.unwrap();
    svc1.try_claim("persist-job", "worker-1").await.unwrap();

    // "重启"：新实例 + 同一 store
    let svc2 = SchedulerService::with_store(store, Duration::from_secs(300));

    assert_eq!(
        svc2.list_tasks().await.unwrap().len(),
        1,
        "任务定义必须存续"
    );
    assert_eq!(
        svc2.get_task_state("persist-job").await.unwrap(),
        Some(TaskState::Running),
        "任务状态必须存续"
    );
    let detail = svc2.get_task_detail("persist-job").await.unwrap().unwrap();
    assert_eq!(
        detail.claimed_by.as_deref(),
        Some("worker-1"),
        "认领记录必须存续"
    );
    // 且存续的认领仍排斥第二个 worker（多节点唯一性）
    assert!(
        svc2.try_claim("persist-job", "worker-2")
            .await
            .unwrap()
            .is_none(),
        "存续的认领必须继续排斥其他 worker"
    );
}

/// 生成一个 uuid 形态的字符串（不引入 uuid 依赖，行为对齐 `helper_uuid()`）
fn uuid_like() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("sched-{nanos:x}")
}

// ════════════════════════════════════════════════════════════════════════════
// gRPC 面的三处「静默失效」—— 2026-09-20 整改的负向对照
//
// 这三处都满足「静态检查全绿、单元测试全绿」，因为缺陷只在 **handler** 层，
// 而此前的用例全部直接调 service 层。判据因此必须打在 handler 上。
// ════════════════════════════════════════════════════════════════════════════

use coord_proto::agent::scheduler_server::Scheduler;
use coord_proto::agent::{
    SchedulerClaimJobRequest, SchedulerCompleteJobRequest, SchedulerHeartbeatRequest,
    SchedulerRegisterJobRequest,
};
use tonic::{Code, Request};

/// ① 注册时携带的 payload 必须**随认领返回**（含非 UTF-8 字节）。
///
/// 修复前：`claim_job` 把它硬编码为 `vec![]` ⇒ 调用方注册了 payload、认领后拿到空。
/// 修复前：`register_job` 用 `from_utf8_lossy` 存 ⇒ 非 UTF-8 被静默替换。
#[tokio::test]
async fn test_grpc_claim_returns_registered_payload_byte_exact() {
    let svc = SchedulerService::new(Default::default());
    // 刻意含非 UTF-8 字节：证明 payload 是二进制安全的，而不是"文本恰好能过"
    let payload: Vec<u8> = vec![0x00, 0xff, 0xfe, b'{', b'}', 0x80];

    let registered = svc
        .register_job(Request::new(SchedulerRegisterJobRequest {
            name: "payload-job".into(),
            cron_expression: "0 0 3 * * ?".into(),
            payload: payload.clone(),
        }))
        .await
        .expect("register 必须成功")
        .into_inner();
    assert_eq!(
        registered.job_id, "payload-job",
        "job_id 必须等于 name（认领键一致）"
    );

    let claimed = svc
        .claim_job(Request::new(SchedulerClaimJobRequest {
            name: "payload-job".into(),
        }))
        .await
        .expect("claim 必须成功")
        .into_inner();

    assert!(claimed.found, "首次认领必须成功");
    assert_eq!(claimed.job_id, "payload-job");
    assert_eq!(
        claimed.payload, payload,
        "payload 必须逐字节一致（不是 lossy 文本）"
    );
}

/// ② 句柄失效时 Heartbeat **必须报错**，不能回空的 OK。
///
/// 修复前：handler 把 `renew_claim_any` 的 `bool` **直接丢掉** ⇒ "句柄已失效"与
/// "续期成功"在 wire 上完全同形，调用方会一直以为自己还持有任务。
#[tokio::test]
async fn test_grpc_heartbeat_fails_loud_when_handle_is_invalid() {
    let svc = SchedulerService::new(Default::default());

    // 未注册的任务：句柄不可能有效
    let err = svc
        .heartbeat(Request::new(SchedulerHeartbeatRequest {
            job_id: "never-registered".into(),
        }))
        .await
        .expect_err("无效句柄必须报错");
    assert_eq!(err.code(), Code::FailedPrecondition);

    // 已注册但尚未认领：同样无效（没有 claim 可续）
    svc.register_job(Request::new(SchedulerRegisterJobRequest {
        name: "renew-job".into(),
        cron_expression: "0 0 3 * * ?".into(),
        payload: vec![],
    }))
    .await
    .expect("register 必须成功");
    let err = svc
        .heartbeat(Request::new(SchedulerHeartbeatRequest {
            job_id: "renew-job".into(),
        }))
        .await
        .expect_err("未认领的任务不能续期");
    assert_eq!(err.code(), Code::FailedPrecondition);

    // 认领之后必须成功（正面对照 —— 否则上面的断言只是在测"永远报错"）
    svc.claim_job(Request::new(SchedulerClaimJobRequest {
        name: "renew-job".into(),
    }))
    .await
    .expect("claim 必须成功");
    svc.heartbeat(Request::new(SchedulerHeartbeatRequest {
        job_id: "renew-job".into(),
    }))
    .await
    .expect("持有认领时续期必须成功");
}

/// ③ 无有效认领时 CompleteJob **必须报错**；已完成的任务保持幂等。
///
/// 修复前：`mark_completed_impl` 对不存在的任务直接 `Ok(())` ⇒ 句柄写错的 worker
/// 会得到"完成成功"，于是既不重试也不上报失败。
#[tokio::test]
async fn test_grpc_complete_job_fails_loud_but_stays_idempotent() {
    let svc = SchedulerService::new(Default::default());

    let err = svc
        .complete_job(Request::new(SchedulerCompleteJobRequest {
            job_id: "never-registered".into(),
            result: vec![],
        }))
        .await
        .expect_err("无认领的任务不能标记完成");
    assert_eq!(err.code(), Code::FailedPrecondition);

    svc.register_job(Request::new(SchedulerRegisterJobRequest {
        name: "done-job".into(),
        cron_expression: "0 0 3 * * ?".into(),
        payload: vec![],
    }))
    .await
    .expect("register 必须成功");
    let err = svc
        .complete_job(Request::new(SchedulerCompleteJobRequest {
            job_id: "done-job".into(),
            result: vec![],
        }))
        .await
        .expect_err("未认领的任务不能标记完成");
    assert_eq!(err.code(), Code::FailedPrecondition);

    svc.claim_job(Request::new(SchedulerClaimJobRequest {
        name: "done-job".into(),
    }))
    .await
    .expect("claim 必须成功");
    svc.complete_job(Request::new(SchedulerCompleteJobRequest {
        job_id: "done-job".into(),
        result: b"ignored".to_vec(),
    }))
    .await
    .expect("持有认领时完成必须成功");
    assert_eq!(
        svc.get_task_state("done-job").await.unwrap(),
        Some(TaskState::Completed)
    );

    // 幂等：重复完成不再报错（消费者重试 / 至少一次投递下的正常形态）
    svc.complete_job(Request::new(SchedulerCompleteJobRequest {
        job_id: "done-job".into(),
        result: vec![],
    }))
    .await
    .expect("已 Completed 的任务重复完成必须幂等");
}

/// ④ `has_live_claim` 必须跟随句柄有效性（complete / 未知任务都必须为 false）
#[tokio::test]
async fn test_has_live_claim_tracks_handle_validity() {
    let svc = SchedulerService::new(Default::default());
    svc.register_job(Request::new(SchedulerRegisterJobRequest {
        name: "live-job".into(),
        cron_expression: "0 0 3 * * ?".into(),
        payload: vec![],
    }))
    .await
    .unwrap();

    assert!(
        !svc.has_live_claim("live-job").await.unwrap(),
        "未认领 = 无有效句柄"
    );
    assert!(
        !svc.has_live_claim("nope").await.unwrap(),
        "未知任务 = 无有效句柄"
    );

    svc.claim_job(Request::new(SchedulerClaimJobRequest {
        name: "live-job".into(),
    }))
    .await
    .unwrap();
    assert!(
        svc.has_live_claim("live-job").await.unwrap(),
        "认领后句柄必须有效"
    );

    svc.complete_job(Request::new(SchedulerCompleteJobRequest {
        job_id: "live-job".into(),
        result: vec![],
    }))
    .await
    .unwrap();
    assert!(
        !svc.has_live_claim("live-job").await.unwrap(),
        "完成后句柄必须失效"
    );
}
