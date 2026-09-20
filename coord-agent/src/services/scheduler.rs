// coord-agent: 分布式调度服务 (Scheduler Service)
//
// 实现分布式任务调度，支持多 worker 竞争认领、Exactly-Once 执行保证、惊群缓解。
//
// 核心机制:
// - 任务认领: **KV CAS** 防止重复执行（跨 Agent 原子；见 `scheduler_store.rs`）
// - Exactly-Once: 任务状态跟踪（Pending → Running → Completed/Failed）
// - 惊群缓解: 随机退避 + 未来可选分片通知
// - 心跳续期: 定期 renew，过期自动释放
//
// ⚠️ 历史缺陷（本次整改，计划书 P0-10 / E9）：本服务此前用三个
// `Arc<RwLock<HashMap<..>>>` 持有 任务 / 认领 / 状态，**模块头却自称 "KV CAS + Lease"**
// —— 属"声明但未实施"（计划书 N8）。后果：① 重启即丢全部调度状态；
// ② 多 Agent 各持一份 ⇒ "多节点唯一调度"不成立；③ 三张表可互相漂移。
// 现已改为 [`SchedulerStore`] 支撑（生产 = coord-server 共享 KV + Txn CAS，
// 见 `services/scheduler_store.rs`），三张表合并为**一条记录**。
//
// ⚠️ wire 无 worker 身份（本次整改的第二处缺陷）：`SchedulerClaimJobResponse`
// 只回 `job_id` / `payload` / `found`，`SchedulerHeartbeatRequest` /
// `SchedulerCompleteJobRequest` 只有 `job_id`。此前 gRPC handler 给续期 / 完成
// 硬编码传入 `"worker"`，而认领时用的是随机 uuid ⇒ **CompleteJob 必然报错、
// Heartbeat 静默失效**。因 wire 无法表达身份，gRPC 面改用 **claim 句柄语义**：
// `job_id` 即凭据（仅发给认领成功者），见 [`SchedulerService::renew_claim_any`] /
// [`SchedulerService::mark_completed_any`]。Rust 侧 `*_claim(task_id, worker_id)`
// 保留**归属校验**语义不变。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::service::{BaseService, ServiceError, ServiceResult};
use crate::services::scheduler_store::{now_ms, ClaimRecord, SchedulerStore, TaskRecord};

/// CAS 冲突重试上限
///
/// 认领是"热点键"（惊群场景），CAS 冲突属正常竞争；超过上限说明持续拥塞，
/// 返回明确错误而非静默失败（与 fail-closed 口径一致）。
const MAX_CAS_RETRIES: usize = 16;

// ──── 类型定义 ────

/// 任务类型
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskType {
    /// 一次性任务
    Once,
    /// 固定频率（毫秒）
    FixedRate { interval_ms: u64 },
    /// 固定延迟（上次完成后延迟 ms）
    FixedDelay { delay_ms: u64 },
    /// Cron 表达式
    Cron { expression: String },
}

/// 任务状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskState {
    /// 等待调度
    Pending,
    /// 执行中
    Running,
    /// 已完成
    Completed,
    /// 已失败
    Failed,
}

/// 调度任务定义
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleTask {
    pub task_id: String,
    pub task_type: TaskType,
    pub description: String,
    pub metadata: HashMap<String, String>,
}

/// 任务认领记录（进程内视图）
///
/// `claimed_at` 为**进程内单调时刻**，仅用于本地观测；落盘 / 跨进程比较一律用
/// [`ClaimRecord::claimed_at_ms`]（墙钟毫秒）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskClaim {
    pub task_id: String,
    pub worker_id: String,
    pub state: TaskState,
    pub claimed_at: Instant,
}

/// 任务详情（含完整信息）
#[derive(Debug, Clone)]
pub struct TaskDetail {
    pub task_id: String,
    pub task_type: TaskType,
    pub description: String,
    pub metadata: HashMap<String, String>,
    pub state: TaskState,
    pub claimed_by: Option<String>,
}

// ──── SchedulerService ────

/// 分布式调度服务
///
/// 管理任务注册、认领、状态跟踪、心跳续期。
/// 支持 Exactly-Once 执行保证和惊群缓解。
///
/// 全部状态存放在 [`SchedulerStore`] 中；本结构自身**无状态**，
/// 因此同一条语义在单机内存与多 Agent 共享 KV 下一致。
pub struct SchedulerService {
    store: Arc<dyn SchedulerStore>,
    /// 认领 TTL（超时自动释放）
    claim_ttl: Duration,
}

impl SchedulerService {
    /// 使用默认配置创建（TTL = 60s，**内存后端**；生产装配见 [`Self::with_store`]）
    pub fn new(_config: DefaultConfig) -> Self {
        Self::new_with_ttl(Duration::from_secs(60))
    }

    /// 使用指定 TTL 创建（内存后端；主要用于测试）
    pub fn new_with_ttl(claim_ttl: Duration) -> Self {
        Self::with_store(
            Arc::new(crate::services::scheduler_store::MemorySchedulerStore::new()),
            claim_ttl,
        )
    }

    /// 使用指定存储后端创建（**生产**：`KvSchedulerStore`）
    pub fn with_store(store: Arc<dyn SchedulerStore>, claim_ttl: Duration) -> Self {
        Self { store, claim_ttl }
    }

    /// 认领 TTL 的毫秒表示（落盘与比较统一用它）
    fn ttl_ms(&self) -> u64 {
        self.claim_ttl.as_millis() as u64
    }

    /// 读取任务记录
    async fn load(&self, task_id: &str) -> ServiceResult<Option<TaskRecord>> {
        self.store.get(task_id).await.map_err(ServiceError::from)
    }

    /// 单个任务的一次 CAS；`false` = 期间被他人改动
    async fn cas(
        &self,
        task_id: &str,
        expected: &TaskRecord,
        next: &TaskRecord,
    ) -> ServiceResult<bool> {
        self.store
            .cas(task_id, Some(expected), next)
            .await
            .map_err(ServiceError::from)
    }

    /// 注册调度任务（重复 task_id 报错）
    pub async fn register_task(&self, task: ScheduleTask) -> ServiceResult<()> {
        let task_id = task.task_id.clone();
        let created = self
            .store
            .create(&task_id, &TaskRecord::new(task))
            .await
            .map_err(ServiceError::from)?;
        if !created {
            return Err(format!("task {task_id} already registered").into());
        }
        Ok(())
    }

    /// 注销调度任务（同时清掉状态与认领 —— 三者同处一条记录）
    pub async fn deregister_task(&self, task_id: &str) -> ServiceResult<()> {
        self.store.delete(task_id).await.map_err(ServiceError::from)
    }

    /// 列出所有注册任务
    pub async fn list_tasks(&self) -> ServiceResult<Vec<ScheduleTask>> {
        let records = self.store.list().await.map_err(ServiceError::from)?;
        let mut tasks: Vec<ScheduleTask> = records.into_iter().map(|r| r.task).collect();
        // KV 的 range 已按键序返回；内存后端无序 ⇒ 统一排序，保证结果稳定可比。
        tasks.sort_by(|a, b| a.task_id.cmp(&b.task_id));
        Ok(tasks)
    }

    /// 尝试认领任务（**跨节点原子 CAS**）
    ///
    /// 返回 `Some(TaskClaim)` 表示认领成功，`None` 表示已被他人认领或状态不允许。
    pub async fn try_claim(
        &self,
        task_id: &str,
        worker_id: &str,
    ) -> ServiceResult<Option<TaskClaim>> {
        let ttl_ms = self.ttl_ms();

        for _ in 0..MAX_CAS_RETRIES {
            let Some(current) = self.load(task_id).await? else {
                // 未注册的任务不可认领（历史内存实现的 `unwrap_or(Completed)` 同义）
                return Ok(None);
            };

            if !current.is_claimable(now_ms(), ttl_ms) {
                return Ok(None);
            }

            let mut next = current.clone();
            next.state = TaskState::Running;
            next.claim = Some(ClaimRecord {
                worker_id: worker_id.to_string(),
                claimed_at_ms: now_ms(),
            });

            if self.cas(task_id, &current, &next).await? {
                return Ok(Some(TaskClaim {
                    task_id: task_id.to_string(),
                    worker_id: worker_id.to_string(),
                    state: TaskState::Running,
                    claimed_at: Instant::now(),
                }));
            }
            // CAS 失败 = 有人抢先或状态已变 ⇒ 重读后按新状态判定
        }

        Err(format!(
            "scheduler: claim contention on '{task_id}' exceeded {MAX_CAS_RETRIES} CAS retries"
        )
        .into())
    }

    /// 释放认领（仅认领者本人可释放；非本人为 no-op）
    pub async fn release_claim(&self, task_id: &str, worker_id: &str) -> ServiceResult<()> {
        self.release_claim_impl(task_id, Some(worker_id)).await
    }

    /// 按 claim 句柄释放（wire 无 worker 身份；`job_id` 即凭据）
    pub async fn release_claim_any(&self, task_id: &str) -> ServiceResult<()> {
        self.release_claim_impl(task_id, None).await
    }

    async fn release_claim_impl(
        &self,
        task_id: &str,
        worker_id: Option<&str>,
    ) -> ServiceResult<()> {
        for _ in 0..MAX_CAS_RETRIES {
            let Some(current) = self.load(task_id).await? else {
                return Ok(());
            };
            let owned = current.claim.as_ref().is_some_and(|c| match worker_id {
                Some(w) => c.worker_id == w,
                None => true,
            });
            if !owned {
                return Ok(()); // 未认领，或非本人 —— 与历史实现同为 no-op
            }

            let mut next = current.clone();
            next.claim = None;
            if next.state == TaskState::Running {
                next.state = TaskState::Pending;
            }

            if self.cas(task_id, &current, &next).await? {
                return Ok(());
            }
        }
        Err(format!("scheduler: release_claim contention on '{task_id}'").into())
    }

    /// 标记任务完成（校验认领归属）
    pub async fn mark_completed(&self, task_id: &str, worker_id: &str) -> ServiceResult<()> {
        self.mark_completed_impl(task_id, Some(worker_id)).await
    }

    /// 按 claim 句柄标记完成（wire 无 worker 身份；`job_id` 即凭据）
    ///
    /// 语义：清空认领、状态置 `Completed`；已完成的任务不可再被认领（Exactly-Once）。
    pub async fn mark_completed_any(&self, task_id: &str) -> ServiceResult<()> {
        self.mark_completed_impl(task_id, None).await
    }

    async fn mark_completed_impl(
        &self,
        task_id: &str,
        worker_id: Option<&str>,
    ) -> ServiceResult<()> {
        for _ in 0..MAX_CAS_RETRIES {
            let Some(current) = self.load(task_id).await? else {
                return Ok(());
            };
            if let (Some(w), Some(c)) = (worker_id, current.claim.as_ref()) {
                if c.worker_id != w {
                    return Err(format!(
                        "task {task_id} claimed by {}, not {w}",
                        c.worker_id
                    )
                    .into());
                }
            }

            let mut next = current.clone();
            next.claim = None;
            next.state = TaskState::Completed;

            if self.cas(task_id, &current, &next).await? {
                return Ok(());
            }
        }
        Err(format!("scheduler: mark_completed contention on '{task_id}'").into())
    }

    /// 标记任务失败（校验认领归属）
    pub async fn mark_failed(
        &self,
        task_id: &str,
        worker_id: &str,
        _error: &str,
    ) -> ServiceResult<()> {
        self.mark_failed_impl(task_id, Some(worker_id)).await
    }

    /// 按 claim 句柄标记失败（wire 无 worker 身份）
    pub async fn mark_failed_any(&self, task_id: &str) -> ServiceResult<()> {
        self.mark_failed_impl(task_id, None).await
    }

    async fn mark_failed_impl(
        &self,
        task_id: &str,
        worker_id: Option<&str>,
    ) -> ServiceResult<()> {
        for _ in 0..MAX_CAS_RETRIES {
            let Some(current) = self.load(task_id).await? else {
                return Ok(());
            };
            if let (Some(w), Some(c)) = (worker_id, current.claim.as_ref()) {
                if c.worker_id != w {
                    return Err(format!(
                        "task {task_id} claimed by {}, not {w}",
                        c.worker_id
                    )
                    .into());
                }
            }

            let mut next = current.clone();
            next.claim = None;
            // FixedRate 任务失败后回到 Pending（可重试）
            next.state = match current.task.task_type {
                TaskType::FixedRate { .. } => TaskState::Pending,
                _ => TaskState::Failed,
            };

            if self.cas(task_id, &current, &next).await? {
                return Ok(());
            }
        }
        Err(format!("scheduler: mark_failed contention on '{task_id}'").into())
    }

    /// 获取任务状态
    pub async fn get_task_state(&self, task_id: &str) -> ServiceResult<Option<TaskState>> {
        Ok(self.load(task_id).await?.map(|r| r.state))
    }

    /// 列出所有任务状态
    pub async fn list_task_states(&self) -> ServiceResult<HashMap<String, TaskState>> {
        let records = self.store.list().await.map_err(ServiceError::from)?;
        Ok(records
            .into_iter()
            .map(|r| (r.task.task_id, r.state))
            .collect())
    }

    /// 获取任务详情
    pub async fn get_task_detail(&self, task_id: &str) -> ServiceResult<Option<TaskDetail>> {
        let Some(record) = self.load(task_id).await? else {
            return Ok(None);
        };
        Ok(Some(TaskDetail {
            task_id: record.task.task_id.clone(),
            task_type: record.task.task_type.clone(),
            description: record.task.description.clone(),
            metadata: record.task.metadata.clone(),
            state: record.state,
            claimed_by: record.claim.as_ref().map(|c| c.worker_id.clone()),
        }))
    }

    /// 心跳续期（重置认领时间；仅认领者本人可续）
    pub async fn renew_claim(&self, task_id: &str, worker_id: &str) -> ServiceResult<bool> {
        self.renew_claim_impl(task_id, Some(worker_id)).await
    }

    /// 按 claim 句柄续期（wire 无 worker 身份；`job_id` 即凭据）
    pub async fn renew_claim_any(&self, task_id: &str) -> ServiceResult<bool> {
        self.renew_claim_impl(task_id, None).await
    }

    async fn renew_claim_impl(
        &self,
        task_id: &str,
        worker_id: Option<&str>,
    ) -> ServiceResult<bool> {
        for _ in 0..MAX_CAS_RETRIES {
            let Some(current) = self.load(task_id).await? else {
                return Ok(false);
            };
            let Some(claim) = current.claim.as_ref() else {
                return Ok(false);
            };
            if let Some(w) = worker_id {
                if claim.worker_id != w {
                    return Ok(false);
                }
            }

            let mut next = current.clone();
            next.claim = Some(ClaimRecord {
                worker_id: claim.worker_id.clone(),
                claimed_at_ms: now_ms(),
            });

            if self.cas(task_id, &current, &next).await? {
                return Ok(true);
            }
        }
        Err(format!("scheduler: renew_claim contention on '{task_id}'").into())
    }

    /// 计算惊群退避延迟（毫秒）
    ///
    /// 竞争者越多，退避范围越大。
    /// 默认 max_backoff_ms = 5000。
    pub fn compute_backoff_ms(&self, competitor_count: usize) -> u64 {
        let max_backoff = 5000u64;
        if competitor_count <= 1 {
            return 0;
        }
        let range = (competitor_count as u64 * 50).min(max_backoff);
        let mut rng = rand::thread_rng();
        rng.gen_range(0..=range)
    }
}

// ──── 默认配置 ────

/// SchedulerService 默认配置
#[derive(Debug, Clone, Default)]
pub struct DefaultConfig;

// ──── BaseService 实现 ────

#[async_trait]
impl BaseService for SchedulerService {
    fn name(&self) -> &'static str {
        "scheduler"
    }

    async fn start(&self) -> ServiceResult<()> {
        // 认领过期是**惰性判定**（读路径按 ttl 判断），不依赖后台清理任务：
        // 因此无需定时器，也就不会因后台任务缺失而"看起来在跑、实际不释放"。
        Ok(())
    }

    async fn stop(&self) -> ServiceResult<()> {
        Ok(())
    }

    fn health_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::scheduler_store::{MemorySchedulerStore, SchedulerStoreError};

    fn svc() -> SchedulerService {
        SchedulerService::new(DefaultConfig)
    }

    fn task(id: &str, tt: TaskType) -> ScheduleTask {
        ScheduleTask {
            task_id: id.to_string(),
            task_type: tt,
            description: "t".into(),
            metadata: HashMap::new(),
        }
    }

    #[test]
    fn test_task_type_equality() {
        assert_eq!(
            TaskType::Cron {
                expression: "*/5 * * * *".into()
            },
            TaskType::Cron {
                expression: "*/5 * * * *".into()
            },
        );
        assert_ne!(TaskType::Once, TaskType::FixedRate { interval_ms: 1000 });
    }

    #[test]
    fn test_task_state_variants() {
        assert!(matches!(TaskState::Pending, TaskState::Pending));
        assert!(matches!(TaskState::Running, TaskState::Running));
        assert!(matches!(TaskState::Completed, TaskState::Completed));
        assert!(matches!(TaskState::Failed, TaskState::Failed));
    }

    #[tokio::test]
    async fn test_register_rejects_duplicate() {
        let s = svc();
        s.register_task(task("j", TaskType::Once)).await.unwrap();
        assert!(
            s.register_task(task("j", TaskType::Once)).await.is_err(),
            "重复注册必须报错"
        );
    }

    #[tokio::test]
    async fn test_claim_then_complete_is_exactly_once() {
        let s = svc();
        s.register_task(task("j", TaskType::Once)).await.unwrap();

        assert!(s.try_claim("j", "w1").await.unwrap().is_some());
        assert!(
            s.try_claim("j", "w2").await.unwrap().is_none(),
            "已认领的任务不可被第二个 worker 认领"
        );

        s.mark_completed("j", "w1").await.unwrap();
        assert_eq!(
            s.get_task_state("j").await.unwrap(),
            Some(TaskState::Completed)
        );
        assert!(
            s.try_claim("j", "w2").await.unwrap().is_none(),
            "已完成任务不可再认领"
        );
    }

    #[tokio::test]
    async fn test_mark_completed_rejects_foreign_worker() {
        let s = svc();
        s.register_task(task("j", TaskType::Once)).await.unwrap();
        s.try_claim("j", "w1").await.unwrap();
        assert!(
            s.mark_completed("j", "w2").await.is_err(),
            "非认领者不得完成该任务"
        );
    }

    #[tokio::test]
    async fn test_fixed_rate_failure_returns_to_pending() {
        let s = svc();
        s.register_task(task("j", TaskType::FixedRate { interval_ms: 1000 }))
            .await
            .unwrap();
        s.try_claim("j", "w1").await.unwrap();
        s.mark_failed("j", "w1", "boom").await.unwrap();

        assert_eq!(
            s.get_task_state("j").await.unwrap(),
            Some(TaskState::Pending)
        );
        assert!(s.try_claim("j", "w1").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_once_failure_is_terminal() {
        let s = svc();
        s.register_task(task("j", TaskType::Once)).await.unwrap();
        s.try_claim("j", "w1").await.unwrap();
        s.mark_failed("j", "w1", "boom").await.unwrap();
        assert_eq!(s.get_task_state("j").await.unwrap(), Some(TaskState::Failed));
    }

    #[tokio::test]
    async fn test_renew_only_by_owner() {
        let s = svc();
        s.register_task(task("j", TaskType::Once)).await.unwrap();
        s.try_claim("j", "w1").await.unwrap();

        assert!(s.renew_claim("j", "w1").await.unwrap());
        assert!(!s.renew_claim("j", "w2").await.unwrap());
        // 未认领任务不可续期
        assert!(!s.renew_claim("ghost", "w1").await.unwrap());
    }

    /// 过期认领可被接管（TTL 判定用墙钟毫秒，因此 1ms TTL 可精确生效）
    #[tokio::test]
    async fn test_expired_claim_is_reclaimable() {
        let store: Arc<dyn SchedulerStore> = Arc::new(MemorySchedulerStore::new());
        let s = SchedulerService::with_store(store, Duration::from_millis(1));
        s.register_task(task("j", TaskType::Once)).await.unwrap();

        assert!(s.try_claim("j", "w1").await.unwrap().is_some());
        tokio::time::sleep(Duration::from_millis(20)).await;
        let c = s.try_claim("j", "w2").await.unwrap();
        assert!(c.is_some(), "过期认领应可被接管");
        assert_eq!(c.unwrap().worker_id, "w2");
    }

    /// 未过期认领不可被接管（防止 1ms TTL 把"未过期"也判成过期）
    #[tokio::test]
    async fn test_unexpired_claim_is_not_reclaimable() {
        let store: Arc<dyn SchedulerStore> = Arc::new(MemorySchedulerStore::new());
        let s = SchedulerService::with_store(store, Duration::from_secs(300));
        s.register_task(task("j", TaskType::Once)).await.unwrap();
        assert!(s.try_claim("j", "w1").await.unwrap().is_some());
        assert!(s.try_claim("j", "w2").await.unwrap().is_none());
    }

    /// 惊群：10 个 worker 竞争，只有一个获胜（经 CAS）
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_thundering_herd_only_one_wins() {
        let s = Arc::new(svc());
        s.register_task(task(
            "hot",
            TaskType::Cron {
                expression: "* * * * *".into(),
            },
        ))
        .await
        .unwrap();

        let mut handles = Vec::new();
        for i in 0..10 {
            let s = Arc::clone(&s);
            handles.push(tokio::spawn(
                async move { s.try_claim("hot", &format!("worker-{i}")).await },
            ));
        }
        let mut wins = 0;
        for h in handles {
            let r = h.await.expect("join").expect("no store error");
            if r.is_some() {
                wins += 1;
            }
        }
        assert_eq!(wins, 1, "并发认领必须恰好一个成功");
    }

    #[tokio::test]
    async fn test_claim_unknown_task_returns_none() {
        let s = svc();
        assert!(s.try_claim("never-registered", "w1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_deregister_clears_state_and_claim() {
        let s = svc();
        s.register_task(task("j", TaskType::Once)).await.unwrap();
        s.try_claim("j", "w1").await.unwrap();
        s.deregister_task("j").await.unwrap();

        assert!(s.list_tasks().await.unwrap().is_empty());
        assert!(s.get_task_state("j").await.unwrap().is_none());
        // 注销后重新注册应成功（且状态回到 Pending）
        s.register_task(task("j", TaskType::Once)).await.unwrap();
        assert_eq!(
            s.get_task_state("j").await.unwrap(),
            Some(TaskState::Pending)
        );
    }

    #[tokio::test]
    async fn test_list_task_states_and_detail() {
        let s = svc();
        for i in 0..5 {
            s.register_task(task(&format!("job-{i}"), TaskType::Once))
                .await
                .unwrap();
        }
        let states = s.list_task_states().await.unwrap();
        assert_eq!(states.len(), 5);
        assert!(states.values().all(|v| *v == TaskState::Pending));

        let mut meta = HashMap::new();
        meta.insert("priority".to_string(), "high".to_string());
        s.register_task(ScheduleTask {
            task_id: "detail".into(),
            task_type: TaskType::Cron {
                expression: "0 0 * * *".into(),
            },
            description: "Daily".into(),
            metadata: meta,
        })
        .await
        .unwrap();

        let d = s.get_task_detail("detail").await.unwrap().expect("detail");
        assert_eq!(d.description, "Daily");
        assert_eq!(d.metadata.get("priority").unwrap(), "high");
        assert!(d.claimed_by.is_none());
        assert!(s.get_task_detail("nope").await.unwrap().is_none());
    }

    /// claim 句柄路径（gRPC 面）：续期 / 完成不再依赖 wire 上不存在的 worker 身份
    #[tokio::test]
    async fn test_claim_handle_path_renew_and_complete() {
        let s = svc();
        s.register_task(task("j", TaskType::Once)).await.unwrap();
        s.try_claim("j", "server-generated-uuid").await.unwrap();

        assert!(s.renew_claim_any("j").await.unwrap());
        s.mark_completed_any("j").await.unwrap();
        assert_eq!(
            s.get_task_state("j").await.unwrap(),
            Some(TaskState::Completed)
        );
    }

    #[tokio::test]
    async fn test_claim_handle_path_release_makes_reclaimable() {
        let s = svc();
        s.register_task(task("j", TaskType::Once)).await.unwrap();
        s.try_claim("j", "u1").await.unwrap();
        s.release_claim_any("j").await.unwrap();
        assert_eq!(
            s.get_task_state("j").await.unwrap(),
            Some(TaskState::Pending)
        );
        assert!(s.try_claim("j", "u2").await.unwrap().is_some());
    }

    /// 「重启存续」的进程内等价物：换一个**新的 SchedulerService 实例**共享同一 store，
    /// 状态必须仍在（KV 后端的跨进程等价性质由 store 层保证）。
    #[tokio::test]
    async fn test_state_survives_service_recreation_on_shared_store() {
        let store: Arc<dyn SchedulerStore> = Arc::new(MemorySchedulerStore::new());
        {
            let s = SchedulerService::with_store(Arc::clone(&store), Duration::from_secs(300));
            s.register_task(task("j", TaskType::FixedRate { interval_ms: 1 }))
                .await
                .unwrap();
            s.try_claim("j", "w1").await.unwrap();
        }
        // "重启"：新实例、同一 store
        let s2 = SchedulerService::with_store(Arc::clone(&store), Duration::from_secs(300));
        assert_eq!(s2.list_tasks().await.unwrap().len(), 1, "任务定义必须存续");
        assert_eq!(
            s2.get_task_state("j").await.unwrap(),
            Some(TaskState::Running),
            "任务状态必须存续"
        );
        let d = s2.get_task_detail("j").await.unwrap().unwrap();
        assert_eq!(d.claimed_by.as_deref(), Some("w1"), "认领必须存续");
        // 且存续的认领仍然排斥第二个 worker
        assert!(s2.try_claim("j", "w2").await.unwrap().is_none());
    }

    #[test]
    fn test_backoff_delay_range() {
        let s = svc();
        assert!(s.compute_backoff_ms(10) <= 5000);
        assert_eq!(s.compute_backoff_ms(1), 0, "single competitor → no backoff");
        assert!(s.compute_backoff_ms(2) <= 100);
        assert!(s.compute_backoff_ms(100) <= 5000);
    }

    #[test]
    fn test_scheduler_store_error_display() {
        let e = SchedulerStoreError::Kv("down".into());
        assert!(e.to_string().contains("down"));
        let e = SchedulerStoreError::Serialization("bad".into());
        assert!(e.to_string().contains("bad"));
    }
}
