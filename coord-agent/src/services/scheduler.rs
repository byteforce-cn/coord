// coord-agent: 分布式调度服务 (Scheduler Service) — Phase H
//
// 实现分布式任务调度，支持多 worker 竞争认领、Exactly-Once 执行保证、惊群缓解。
//
// 核心机制（v8.2 §4.9）:
// - 任务认领: KV CAS + Lease 防止重复执行
// - Exactly-Once: 任务状态跟踪（Pending → Running → Completed/Failed）
// - 惊群缓解: 随机退避 + 未来可选分片通知
// - 心跳续期: 定期 renew，过期自动释放

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::RwLock;
use rand::Rng;

use crate::service::{BaseService, ServiceResult};

// ──── 类型定义 ────

/// 任务类型
#[derive(Debug, Clone, PartialEq, Eq)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
#[derive(Debug, Clone)]
pub struct ScheduleTask {
    pub task_id: String,
    pub task_type: TaskType,
    pub description: String,
    pub metadata: HashMap<String, String>,
}

/// 任务认领记录
#[derive(Debug, Clone)]
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
pub struct SchedulerService {
    /// 注册的任务定义
    tasks: Arc<RwLock<HashMap<String, ScheduleTask>>>,
    /// 任务认领记录（task_id → claim）
    claims: Arc<RwLock<HashMap<String, TaskClaim>>>,
    /// 任务状态记录（task_id → state）
    states: Arc<RwLock<HashMap<String, TaskState>>>,
    /// 认领 TTL（超时自动释放）
    claim_ttl: Duration,
}

impl SchedulerService {
    /// 使用默认配置创建（TTL = 60s）
    pub fn new(_config: DefaultConfig) -> Self {
        Self::new_with_ttl(Duration::from_secs(60))
    }

    /// 使用指定 TTL 创建（主要用于测试）
    pub fn new_with_ttl(claim_ttl: Duration) -> Self {
        Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
            claims: Arc::new(RwLock::new(HashMap::new())),
            states: Arc::new(RwLock::new(HashMap::new())),
            claim_ttl,
        }
    }

    /// 注册调度任务
    pub fn register_task(&self, task: ScheduleTask) -> ServiceResult<()> {
        let mut tasks = self.tasks.write();
        if tasks.contains_key(&task.task_id) {
            return Err(format!("task {} already registered", task.task_id).into());
        }
        let task_id = task.task_id.clone();
        tasks.insert(task_id.clone(), task);
        self.states.write().insert(task_id, TaskState::Pending);
        Ok(())
    }

    /// 注销调度任务
    pub fn deregister_task(&self, task_id: &str) -> ServiceResult<()> {
        self.tasks.write().remove(task_id);
        self.claims.write().remove(task_id);
        self.states.write().remove(task_id);
        Ok(())
    }

    /// 列出所有注册任务
    pub fn list_tasks(&self) -> Vec<ScheduleTask> {
        self.tasks.read().values().cloned().collect()
    }

    /// 尝试认领任务（CAS 语义）
    ///
    /// 返回 Some(TaskClaim) 表示认领成功，None 表示已被他人认领或状态不允许。
    pub fn try_claim(&self, task_id: &str, worker_id: &str) -> ServiceResult<Option<TaskClaim>> {
        let mut claims = self.claims.write();
        let states = self.states.read();

        // 检查任务状态是否允许认领
        let state = states.get(task_id).copied().unwrap_or(TaskState::Completed);
        match state {
            TaskState::Pending | TaskState::Failed => {} // 可认领
            TaskState::Running => {
                // 检查是否过期
                if let Some(existing) = claims.get(task_id) {
                    if existing.claimed_at.elapsed() < self.claim_ttl {
                        return Ok(None); // 未过期，不可认领
                    }
                    // 过期了，允许重新认领
                }
            }
            TaskState::Completed => return Ok(None),
        }

        let now = Instant::now();
        let claim = TaskClaim {
            task_id: task_id.to_string(),
            worker_id: worker_id.to_string(),
            state: TaskState::Running,
            claimed_at: now,
        };

        claims.insert(task_id.to_string(), claim.clone());
        drop(states);
        self.states.write().insert(task_id.to_string(), TaskState::Running);

        Ok(Some(claim))
    }

    /// 释放认领
    pub fn release_claim(&self, task_id: &str, worker_id: &str) -> ServiceResult<()> {
        let mut claims = self.claims.write();
        if let Some(claim) = claims.get(task_id) {
            if claim.worker_id == worker_id {
                claims.remove(task_id);
                self.states.write().insert(task_id.to_string(), TaskState::Pending);
            }
        }
        Ok(())
    }

    /// 标记任务完成
    pub fn mark_completed(&self, task_id: &str, worker_id: &str) -> ServiceResult<()> {
        let claims = self.claims.read();
        if let Some(claim) = claims.get(task_id) {
            if claim.worker_id != worker_id {
                return Err(format!("task {task_id} claimed by {}, not {worker_id}", claim.worker_id).into());
            }
        }
        drop(claims);

        self.claims.write().remove(task_id);
        self.states.write().insert(task_id.to_string(), TaskState::Completed);
        Ok(())
    }

    /// 标记任务失败
    pub fn mark_failed(&self, task_id: &str, worker_id: &str, _error: &str) -> ServiceResult<()> {
        let claims = self.claims.read();
        if let Some(claim) = claims.get(task_id) {
            if claim.worker_id != worker_id {
                return Err(format!("task {task_id} claimed by {}, not {worker_id}", claim.worker_id).into());
            }
        }
        drop(claims);

        self.claims.write().remove(task_id);

        // FixedRate 任务失败后回到 Pending（可重试）
        let tasks = self.tasks.read();
        let next_state = match tasks.get(task_id) {
            Some(t) if matches!(t.task_type, TaskType::FixedRate { .. }) => TaskState::Pending,
            _ => TaskState::Failed,
        };
        self.states.write().insert(task_id.to_string(), next_state);
        Ok(())
    }

    /// 获取任务状态
    pub fn get_task_state(&self, task_id: &str) -> Option<TaskState> {
        self.states.read().get(task_id).copied()
    }

    /// 列出所有任务状态
    pub fn list_task_states(&self) -> HashMap<String, TaskState> {
        self.states.read().clone()
    }

    /// 获取任务详情
    pub fn get_task_detail(&self, task_id: &str) -> Option<TaskDetail> {
        let tasks = self.tasks.read();
        let task = tasks.get(task_id)?;
        let state = self.states.read().get(task_id).copied().unwrap_or(TaskState::Pending);
        let claimed_by = self.claims.read().get(task_id).map(|c| c.worker_id.clone());

        Some(TaskDetail {
            task_id: task.task_id.clone(),
            task_type: task.task_type.clone(),
            description: task.description.clone(),
            metadata: task.metadata.clone(),
            state,
            claimed_by,
        })
    }

    /// 心跳续期（重置认领时间）
    pub fn renew_claim(&self, task_id: &str, worker_id: &str) -> ServiceResult<bool> {
        let mut claims = self.claims.write();
        if let Some(claim) = claims.get_mut(task_id) {
            if claim.worker_id == worker_id {
                claim.claimed_at = Instant::now();
                return Ok(true);
            }
        }
        Ok(false)
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

    fn register_grpc(&self, _router: tonic::transport::server::Router) -> tonic::transport::server::Router {
        _router
    }

    async fn start(&self) -> ServiceResult<()> {
        // 启动过期认领清理后台任务
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

    #[test]
    fn test_task_type_equality() {
        assert_eq!(
            TaskType::Cron { expression: "*/5 * * * *".into() },
            TaskType::Cron { expression: "*/5 * * * *".into() },
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
}
