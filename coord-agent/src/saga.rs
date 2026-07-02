// coord-agent: Saga 事务服务 (Phase H)
//
// v8.2 §4.13: Saga 事务 — Workflow 原生编排，自动补偿。
//
// 核心机制：
// - 定义 Saga 步骤（action + compensation）
// - 顺序执行 action；任意步骤失败 → 逆序执行已执行步骤的 compensation
// - 重试机制：瞬态故障自动重试（可配置次数 + 退避）
// - 状态跟踪：Pending → Running → Completed/Failed/Compensating

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;

// ──── SagaConfig ────

/// Saga 配置
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct SagaConfig {
    /// 最大重试次数
    #[serde(default = "default_retry_max")]
    pub retry_max_attempts: u32,

    /// 重试退避时间（毫秒）
    #[serde(default = "default_retry_backoff")]
    pub retry_backoff_ms: u64,
}

fn default_retry_max() -> u32 { 3 }
fn default_retry_backoff() -> u64 { 1000 }

impl Default for SagaConfig {
    fn default() -> Self {
        Self {
            retry_max_attempts: 3,
            retry_backoff_ms: 1000,
        }
    }
}

// ──── SagaContext ────

/// Saga 执行上下文（键值存储，跨步骤共享状态）
#[derive(Debug, Clone, Default)]
pub struct SagaContext {
    saga_id: String,
    data: HashMap<String, String>,
}

impl SagaContext {
    /// 创建新上下文
    pub fn new(saga_id: impl Into<String>) -> Self {
        Self {
            saga_id: saga_id.into(),
            data: HashMap::new(),
        }
    }

    /// 获取 saga ID
    pub fn saga_id(&self) -> &str {
        &self.saga_id
    }

    /// 设置键值
    pub fn set(&mut self, key: &str, value: &str) {
        self.data.insert(key.to_string(), value.to_string());
    }

    /// 获取键值
    pub fn get(&self, key: &str) -> Option<&String> {
        self.data.get(key)
    }
}

// ──── SagaStep ────

/// Saga 步骤定义
///
/// 每个步骤包含正向操作（action）和补偿操作（compensation）。
/// 使用 trait object 闭包实现可插拔步骤逻辑。
pub struct SagaStep {
    /// 步骤名称
    pub name: String,
    /// 正向操作
    pub action: Box<dyn Fn(&mut SagaContext) -> Result<(), String> + Send + Sync>,
    /// 补偿操作
    pub compensation: Box<dyn Fn(&mut SagaContext) -> Result<(), String> + Send + Sync>,
}

impl std::fmt::Debug for SagaStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SagaStep")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

// ──── SagaState ────

/// Saga 执行状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SagaState {
    Pending,
    Running,
    Completed,
    Compensating,
    Failed,
}

// ──── StepResult ────

/// 步骤执行结果
#[derive(Debug, Clone)]
pub enum StepResult {
    Completed,
    Failed {
        step_name: String,
        error: String,
    },
    Compensated {
        failed_step: String,
    },
}

// ──── SagaDefinition ────

struct SagaDefinition {
    steps: Vec<SagaStep>,
    state: SagaState,
}

// ──── SagaService ────

/// Saga 事务服务
///
/// 管理 Saga 定义、执行和状态跟踪。
pub struct SagaService {
    config: SagaConfig,
    sagas: Arc<RwLock<HashMap<String, SagaDefinition>>>,
}

impl SagaService {
    /// 创建 Saga 服务
    pub fn new(config: SagaConfig) -> Self {
        Self {
            config,
            sagas: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 添加 Saga 步骤
    pub fn add_step(&self, saga_id: &str, step: SagaStep) {
        let mut sagas = self.sagas.write();
        let def = sagas
            .entry(saga_id.to_string())
            .or_insert_with(|| SagaDefinition {
                steps: Vec::new(),
                state: SagaState::Pending,
            });
        def.steps.push(step);
    }

    /// 执行 Saga
    ///
    /// 顺序执行所有步骤的正向操作。
    /// 若某步骤失败（含重试耗尽），逆序执行已执行步骤的补偿操作。
    pub fn execute(
        &self,
        saga_id: &str,
        ctx: &mut SagaContext,
    ) -> Result<StepResult, SagaError> {
        // 获取步骤列表
        let steps = {
            let mut sagas = self.sagas.write();
            let def = sagas
                .get_mut(saga_id)
                .ok_or(SagaError::NotFound(saga_id.to_string()))?;
            def.state = SagaState::Running;
            def.steps.iter().map(|s| s.name.clone()).collect::<Vec<_>>()
            // We'll access steps individually to avoid borrow issues
        };

        let total_steps = steps.len();
        let mut executed_indices: Vec<usize> = Vec::new();

        // 正向执行
        for i in 0..total_steps {
            let step_result = self.execute_step_with_retry(saga_id, i, ctx);

            match step_result {
                Ok(()) => {
                    executed_indices.push(i);
                }
                Err(e) => {
                    // 执行补偿
                    self.compensate(saga_id, &executed_indices, i, ctx);
                    self.set_state(saga_id, SagaState::Failed);
                    return Ok(StepResult::Failed {
                        step_name: steps[i].clone(),
                        error: e,
                    });
                }
            }
        }

        self.set_state(saga_id, SagaState::Completed);
        Ok(StepResult::Completed)
    }

    /// 带重试的步骤执行
    fn execute_step_with_retry(
        &self,
        saga_id: &str,
        step_index: usize,
        ctx: &mut SagaContext,
    ) -> Result<(), String> {
        let mut last_error = String::new();

        for attempt in 0..self.config.retry_max_attempts {
            if attempt > 0 {
                std::thread::sleep(Duration::from_millis(self.config.retry_backoff_ms));
            }

            let sagas = self.sagas.read();
            let def = sagas.get(saga_id).ok_or_else(|| "saga not found".to_string())?;
            let step = def.steps.get(step_index).ok_or_else(|| "step not found".to_string())?;

            match (step.action)(ctx) {
                Ok(()) => return Ok(()),
                Err(e) => {
                    last_error = e;
                }
            }
        }

        Err(last_error)
    }

    /// 执行补偿：逆序执行已执行步骤的补偿操作
    fn compensate(
        &self,
        saga_id: &str,
        executed_indices: &[usize],
        failed_index: usize,
        ctx: &mut SagaContext,
    ) {
        self.set_state(saga_id, SagaState::Compensating);

        // 逆序补偿已执行的步骤
        for &i in executed_indices.iter().rev() {
            let sagas = self.sagas.read();
            if let Some(def) = sagas.get(saga_id) {
                if let Some(step) = def.steps.get(i) {
                    let _ = (step.compensation)(ctx);
                }
            }
        }

        // 也补偿失败的步骤（如果有补偿逻辑）
        let sagas = self.sagas.read();
        if let Some(def) = sagas.get(saga_id) {
            if let Some(step) = def.steps.get(failed_index) {
                let _ = (step.compensation)(ctx);
            }
        }
    }

    /// 设置 Saga 状态
    fn set_state(&self, saga_id: &str, state: SagaState) {
        if let Some(def) = self.sagas.write().get_mut(saga_id) {
            def.state = state;
        }
    }

    /// 获取 Saga 状态
    pub fn get_state(&self, saga_id: &str) -> Result<SagaState, SagaError> {
        let sagas = self.sagas.read();
        sagas
            .get(saga_id)
            .map(|def| def.state)
            .ok_or(SagaError::NotFound(saga_id.to_string()))
    }
}

// ──── SagaError ────

/// Saga 错误类型
#[derive(Debug)]
pub enum SagaError {
    NotFound(String),
}

impl std::fmt::Display for SagaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(id) => write!(f, "saga not found: {id}"),
        }
    }
}

impl std::error::Error for SagaError {}

// ──── tests ────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_saga_success() {
        let svc = SagaService::new(SagaConfig::default());
        let id = "test-1";
        svc.add_step(id, SagaStep {
            name: "s1".into(),
            action: Box::new(|_| Ok(())),
            compensation: Box::new(|_| Ok(())),
        });

        let mut ctx = SagaContext::new(id);
        let r = svc.execute(id, &mut ctx).unwrap();
        assert!(matches!(r, StepResult::Completed));
    }

    #[test]
    fn test_saga_compensation() {
        let svc = SagaService::new(SagaConfig::default());
        let id = "test-2";
        svc.add_step(id, SagaStep {
            name: "s1".into(),
            action: Box::new(|ctx| { ctx.set("s1", "ok"); Ok(()) }),
            compensation: Box::new(|ctx| { ctx.set("s1", "comp"); Ok(()) }),
        });
        svc.add_step(id, SagaStep {
            name: "s2".into(),
            action: Box::new(|_| Err("fail".into())),
            compensation: Box::new(|_| Ok(())),
        });

        let mut ctx = SagaContext::new(id);
        let r = svc.execute(id, &mut ctx).unwrap();
        assert!(matches!(r, StepResult::Failed { .. }));
        assert_eq!(ctx.get("s1"), Some(&"comp".to_string()));
    }
}
