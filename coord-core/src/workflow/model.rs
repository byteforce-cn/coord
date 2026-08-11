// coord-core/workflow/model.rs
// 工作流领域模型 —— 基于 CNCF Serverless Workflow 1.0 DSL
//
// 定义所有工作流相关的强类型数据结构：
// - WorkflowDefinition: 解析后的工作流定义
// - WorkflowInstance: 运行时实例
// - Task: 12 种任务类型枚举
// - TaskFrame: 任务执行栈帧
// - 状态机: InstanceStatus
// - 错误模型: WorkflowFault, ValidationError

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

// ─── Document 元信息 ───

/// 工作流文档元信息（对应 DSL `document` 块）
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Document {
    pub dsl: String,
    pub namespace: String,
    pub name: String,
    pub version: String,
    pub title: Option<String>,
    pub summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, String>>,
}

// ─── 输入/输出配置 ───

/// 输入配置（对应 DSL `input` 块）
///
/// 标准字段：
/// - `schema`: JSON Schema URI，输入校验（失败 → validation 错误，faulted）
/// - `from`:   原始输入变换表达式（${ ... }），输出为初始 context
/// - `default`: 输入缺失/为空时的默认值
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct InputConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    /// 输入变换表达式（纯函数步骤，${ ... }）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<Value>,
}

impl InputConfig {
    pub fn is_empty(&self) -> bool {
        self.schema.is_none() && self.from.is_none() && self.default.is_none()
    }
}

/// 输出配置（对应 DSL `output` 块 / 任务 `output` 块）
///
/// 标准字段：
/// - `as`:    输出变换表达式（${ ... }），把原始输出变换为最终输出
/// - `schema`: JSON Schema URI，输出校验
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct OutputConfig {
    /// 输出变换表达式（DSL key: `as`）
    #[serde(rename = "as", default, skip_serializing_if = "Option::is_none")]
    pub as_expr: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
}

impl OutputConfig {
    pub fn is_empty(&self) -> bool {
        self.as_expr.is_none() && self.schema.is_none()
    }
}

/// 导出配置（任务 `export` 块）—— 把任务输出合并回 context
///
/// 标准字段：
/// - `as`:    变换表达式（${ ... }），输出为要合并回 context 的值
/// - `schema`: JSON Schema URI，合并前校验
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ExportConfig {
    /// 变换表达式（DSL key: `as`）
    #[serde(rename = "as", default, skip_serializing_if = "Option::is_none")]
    pub as_expr: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
}

impl ExportConfig {
    pub fn is_empty(&self) -> bool {
        self.as_expr.is_none() && self.schema.is_none()
    }
}

// ─── 可复用组件 ───

/// use 块（对应 DSL `use` 块）
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UseComponents {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub functions: Option<HashMap<String, FunctionDef>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retries: Option<HashMap<String, RetryPolicy>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeouts: Option<HashMap<String, TimeoutConfig>>,
}

/// 函数定义
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunctionDef {
    pub call: CallType,
    pub with: Option<Value>,
}

/// 调用类型
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "call", rename_all = "lowercase")]
pub enum CallType {
    Http,
    Grpc,
    #[serde(untagged)]
    Function(String),
}

/// 重试策略
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetryPolicy {
    #[serde(default = "default_retry_delay")]
    pub delay: String, // ISO 8601 duration, e.g. "PT3S"
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backoff: Option<BackoffStrategy>,
    #[serde(default = "default_retry_limit")]
    pub limit: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jitter: Option<JitterConfig>,
}

fn default_retry_delay() -> String {
    "PT3S".to_string()
}
fn default_retry_limit() -> u32 {
    3
}

/// 退避策略
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackoffStrategy {
    Constant,
    Linear,
    Exponential,
}

/// Jitter 配置
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JitterConfig {
    #[serde(default = "default_jitter_factor")]
    pub factor: f64,
}

fn default_jitter_factor() -> f64 {
    0.1
}

/// 超时配置
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimeoutConfig {
    pub after: String, // ISO 8601 duration, e.g. "P7D"
}

// ─── 调度 / 认证 / 密钥（标准 §Scheduling / §Authentication / §Secrets） ───

/// 调度配置（顶层 `schedule`）
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ScheduleConfig {
    /// 周期执行：ISO 8601 间隔，如 "PT1H"
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub every: Option<String>,
    /// CRON 表达式（5/6 字段）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cron: Option<String>,
    /// 完成后延迟重启：ISO 8601
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
    /// 事件触发：订阅事件启动新实例
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on: Option<EventFilter>,
}

impl ScheduleConfig {
    pub fn is_empty(&self) -> bool {
        self.every.is_none() && self.cron.is_none() && self.after.is_none() && self.on.is_none()
    }
}

/// 认证配置（顶层 `auth` 或函数 `auth`）
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct AuthConfig {
    /// 认证方式: basic | bearer | oauth2
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheme: Option<String>,
    /// basic: 用户名/密码（支持 ${ $secrets.x } 引用）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    /// bearer / oauth2: token（支持 ${ $secrets.x } 引用）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    /// oauth2: token 端点 / clientId / clientSecret
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
}

impl AuthConfig {
    pub fn is_empty(&self) -> bool {
        self.scheme.is_none()
            && self.username.is_none()
            && self.password.is_none()
            && self.token.is_none()
            && self.token_url.is_none()
            && self.client_id.is_none()
            && self.client_secret.is_none()
            && self.scope.is_none()
    }
}

/// 密钥引用（顶层 `secrets`）—— 求值时安全注入 `$secrets`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SecretsConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keys: Option<Vec<String>>,
}

impl SecretsConfig {
    pub fn is_empty(&self) -> bool {
        self.keys.as_ref().map(|k| k.is_empty()).unwrap_or(true)
    }
}

/// 常量定义（顶层 `constants`）—— 注入 `$constants`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ConstantsConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub values: Option<serde_json::Map<String, Value>>,
}

impl ConstantsConfig {
    pub fn is_empty(&self) -> bool {
        self.values.as_ref().map(|v| v.is_empty()).unwrap_or(true)
    }
}

/// 认证定义集（顶层 `auth`）—— name → AuthConfig
pub type AuthMap = HashMap<String, AuthConfig>;

// ─── 工作流定义（解析后） ───

/// 解析后的工作流定义 —— 可直接执行的强类型模型
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowDefinition {
    /// 部署时指定的唯一标识符（格式："{namespace}-{timestamp_hex}"）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub document: Document,
    pub do_tasks: Vec<NamedTask>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<InputConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<OutputConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<TimeoutConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub use_components: Option<UseComponents>,
    /// 顶层 `schedule`（周期/CRON/after/事件触发）
    #[serde(default, skip_serializing_if = "ScheduleConfig::is_empty")]
    pub schedule: ScheduleConfig,
    /// 顶层 `auth`（认证定义，name → AuthConfig）
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub auth: HashMap<String, AuthConfig>,
    /// 顶层 `secrets`（密钥键声明）
    #[serde(default, skip_serializing_if = "SecretsConfig::is_empty")]
    pub secrets: SecretsConfig,
    /// 顶层 `constants`（常量）
    #[serde(default, skip_serializing_if = "ConstantsConfig::is_empty")]
    pub constants: ConstantsConfig,
    /// 任务级标准字段索引（task name → TaskMeta：if/input/output/export/retry/timeout）
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub task_meta: HashMap<String, TaskMeta>,
    /// 原始 DSL YAML 文本（部署时保存，用于 get_definition 返回）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_yaml: Option<String>,
}

/// 命名任务（带名称的任务）
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NamedTask {
    pub name: String,
    pub task: Task,
}

/// 任务级标准字段（Open Workflow DSL §Task）
///
/// 覆盖：`if`（条件跳过）、`input{from,schema}`（输入变换/校验）、
/// `output{as,schema}`（输出变换/校验）、`export{as,schema}`（合并回 context）、
/// `retry`（重试策略）、`timeout`（任务超时）。
///
/// 存储于 `WorkflowDefinition.task_meta`，按任务名索引
/// （顶层任务名唯一；嵌套任务需保证全定义内名称唯一以正确关联）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct TaskMeta {
    /// `if` 条件（${ ... }）—— 条件为假时跳过该任务
    #[serde(rename = "if", default, skip_serializing_if = "Option::is_none")]
    pub if_condition: Option<String>,
    /// `input` 块（from 变换 + schema 校验）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<InputConfig>,
    /// `output` 块（as 变换 + schema 校验）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<OutputConfig>,
    /// `export` 块（as 变换 + schema 校验，合并回 context）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub export: Option<ExportConfig>,
    /// `retry` 重试策略（可引用 `use.retries`）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<RetryPolicy>,
    /// `timeout` 任务超时
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<TimeoutConfig>,
}

impl TaskMeta {
    pub fn is_empty(&self) -> bool {
        self.if_condition.is_none()
            && self.input.is_none()
            && self.output.is_none()
            && self.export.is_none()
            && self.retry.is_none()
            && self.timeout.is_none()
    }
}

// ─── 任务类型枚举（13 种） ───

/// 任务类型 —— 对应 CNCF Serverless Workflow DSL 的任务类型（含 end 终端任务）
///
/// 不依赖 serde(tag = "type") 枚举标签，而是按 JSON key 存在性推断类型。
/// 序列化时通过 `#[serde(rename_all = "camelCase")]` 保持 camelCase 风格。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Task {
    Call(CallTask),
    Do(DoTask),
    Switch(SwitchTask),
    Fork(ForkTask),
    ForEach(ForEachTask),
    Wait(WaitTask),
    Listen(ListenTask),
    Emit(EmitTask),
    Set(SetTask),
    Raise(RaiseTask),
    TryCatch(TryCatchTask),
    Run(RunTask),
    End(EndTask),
}

/// call 任务 —— HTTP/gRPC/function 调用
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CallTask {
    pub call: CallType,
    pub with: Option<Value>,
}

/// do 任务 —— 顺序执行子任务列表
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DoTask {
    pub tasks: Vec<NamedTask>,
}

/// switch 任务 —— 条件分支
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SwitchTask {
    pub conditions: Vec<SwitchCondition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_condition: Option<SwitchCondition>,
}

/// switch 条件
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SwitchCondition {
    /// jq 表达式，如 `${ .amount > 10000 }`
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub condition: Option<String>,
    /// 匹配后跳转的目标任务名
    pub transition: String,
}

/// fork 任务 —— 并行执行多个分支
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForkTask {
    pub branches: Vec<ForkBranch>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compete: Option<bool>,
}

/// fork 分支
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForkBranch {
    pub name: String,
    pub tasks: Vec<NamedTask>,
}

/// for-each 任务 —— 集合迭代
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForEachTask {
    /// 输入集合表达式，如 `${ .items }`
    pub input: String,
    /// 迭代变量名
    pub iteration: String,
    /// 子任务
    pub tasks: Vec<NamedTask>,
}

/// wait 任务 —— 定时等待
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WaitTask {
    /// ISO 8601 duration，如 "PT1H"
    pub wait: String,
}

/// listen 任务 —— 事件监听
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ListenTask {
    /// 事件过滤器
    pub listen: EventFilter,
}

/// 事件过滤器
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventFilter {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
}

/// emit 任务 —— 事件发布
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EmitTask {
    pub emit: EmitEvent,
}

/// 发布事件定义
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EmitEvent {
    pub event_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

/// set 任务 —— 变量赋值
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SetTask {
    /// 变量名
    pub variable: String,
    /// jq 表达式
    pub value: String,
}

/// raise 任务 —— 错误抛出
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RaiseTask {
    pub raise: ErrorDef,
}

/// 错误定义
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorDef {
    pub r#type: String,
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// try-catch 任务 —— 异常处理
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TryCatchTask {
    pub r#try: Vec<NamedTask>,
    pub catch: Vec<CatchClause>,
}

/// catch 子句
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CatchClause {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub errors: Option<Vec<String>>,
    pub tasks: Vec<NamedTask>,
}

/// run 任务 —— 子流程
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunTask {
    /// 子工作流引用（namespace::name@version）
    pub workflow: WorkflowRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<Value>,
}

/// 工作流引用
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowRef {
    pub namespace: String,
    pub name: String,
    pub version: String,
}

/// end 任务 —— 终止当前工作流
///
/// 对应 CNCF Serverless Workflow 的 end 语义：执行到该任务时立即结束工作流
/// （Runtime 收到 `StepResult::Completed` 后标记实例 Completed 并停止驱动循环）。
/// 由 SW→coord DSL 转换器生成，作为 switch 分支的终端，阻止分支 fall-through。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EndTask {}

// ─── 工作流实例 ───

/// 工作流实例 —— 一次工作流定义的执行
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowInstance {
    /// 实例唯一标识（UUID v4）
    pub id: String,
    /// 关联的定义标识
    pub definition_ns: String,
    pub definition_name: String,
    pub definition_version: String,
    /// 当前状态
    pub status: InstanceStatus,
    /// 运行时上下文（JSON）
    pub context: Value,
    /// 任务执行栈
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub task_stack: Vec<TaskFrame>,
    /// 当前任务索引（do 列表中进度指针）
    pub current_task_index: usize,
    /// 创建时间（Unix ms）
    pub created_at: i64,
    /// 更新时间（Unix ms）
    pub updated_at: i64,
    /// 最终输出
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
    /// 错误信息
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fault: Option<WorkflowFault>,
    /// 挂起元信息
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suspension_meta: Option<SuspensionMeta>,
}

/// 实例状态枚举（对齐标准 §Status Phases）
///
/// 标准相位：pending / running / waiting / suspended / cancelled / faulted / completed
/// - `Pending`:   实例已创建待执行（start 后进入 Running）
/// - `Running`:   执行中
/// - `Waiting`:   等待事件/时长/重试定时器（自动恢复）
/// - `Suspended`: 人工挂起（等待 signal）
/// - `Faulted`:   错误终止（序列化 "faulted"，兼容旧 "failed" 反序列化）
/// - `Cancelled`: 人工取消
/// - `Completed`: 正常完成
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstanceStatus {
    Pending,
    Running,
    Waiting,
    Suspended,
    Completed,
    #[serde(rename = "faulted", alias = "failed")]
    Failed,
    Cancelled,
}

impl InstanceStatus {
    /// 是否为终端状态（不可再恢复执行）
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            InstanceStatus::Completed | InstanceStatus::Failed | InstanceStatus::Cancelled
        )
    }

    /// 是否为可恢复状态（自动恢复的 Waiting + 人工恢复的 Suspended）
    pub fn is_resumable(&self) -> bool {
        matches!(self, InstanceStatus::Suspended | InstanceStatus::Waiting)
    }
}

/// 任务栈帧 —— 记录单个任务的执行信息
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskFrame {
    pub task_name: String,
    pub task_type: String,
    pub status: TaskStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<i64>,
    #[serde(default)]
    pub retry_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_branches: Option<Vec<String>>,
}

/// 任务状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Skipped,
}

/// 挂起元信息 —— 记录暂停原因与恢复所需信息
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SuspensionMeta {
    /// 暂停原因: "wait" | "call" | "listen" | "signal" | "run" | "retry"
    pub reason: String,
    /// wait/retry 到期时间（Unix ms）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until_ms: Option<i64>,
    /// call 目标服务
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
    /// call 调度输入
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
    /// listen 事件过滤器
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_filter: Option<EventFilter>,
    /// signal 名称（人工审批场景）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_signal: Option<String>,
    /// 重试次数（reason="retry" 时记录）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_count: Option<u32>,
    /// 最近一次失败原因（reason="retry" 时记录）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// 工作流错误（RFC 7807 Problem Details）
///
/// 标准字段：type / title / status / detail / instance
/// - `type`:     标准错误类型 URI（见 `errors` 模块常量），如 `.../errors/timeout`
/// - `instance`: 错误定位（JSON Pointer），RFC 7807 要求，缺失可空
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowFault {
    pub r#type: String,
    pub title: String,
    #[serde(default = "default_fault_status")]
    pub status: u16,
    pub detail: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<String>,
}

fn default_fault_status() -> u16 {
    500
}

// ─── 运行时辅助类型 ───

/// 执行步骤结果 —— 执行器单步输出
#[derive(Debug, Clone, PartialEq)]
pub enum StepResult {
    /// 进入下一个任务（顺序推进到 do_tasks 中的下一项）
    NextTask(TaskFrame),
    /// switch 条件匹配，跳转到指定目标任务名
    Goto {
        /// 目标任务名（必须在 do_tasks 中存在）
        target: String,
        /// switch 任务本身的执行帧
        frame: TaskFrame,
    },
    /// 暂停执行（等待外部条件），同时携带当前任务的执行帧
    Suspend {
        reason: SuspendReason,
        /// 当前暂停任务的执行帧
        frame: TaskFrame,
    },
    /// 变量赋值 —— Runtime 将 value 写入 inst.context[variable]
    SetVariable {
        variable: String,
        value: Value,
        frame: TaskFrame,
    },
    /// 并行分支 —— Runtime 负责分支调度与结果合并
    Fork {
        branches: Vec<ForkBranch>,
        /// compete=true 时首个完成的分支胜出，其余取消
        compete: bool,
        frame: TaskFrame,
    },
    /// 集合迭代 —— Runtime 负责求值 input_expr 并逐元素执行
    ForEach {
        input_expr: String,
        iteration: String,
        tasks: Vec<NamedTask>,
        frame: TaskFrame,
    },
    /// try-catch 块 —— Runtime 负责 try 执行与 catch 错误匹配
    TryBlock {
        try_tasks: Vec<NamedTask>,
        catch_clauses: Vec<CatchClause>,
        frame: TaskFrame,
    },
    /// 执行完成
    Completed { output: Value },
    /// 执行失败
    Failed { fault: WorkflowFault },
}

/// 暂停原因
#[derive(Debug, Clone, PartialEq)]
pub enum SuspendReason {
    /// 定时等待
    WaitingForDuration { until_ms: i64 },
    /// 外部调用（HTTP/gRPC/function）
    ExternalCall {
        service: String,
        with: Option<Value>,
        input: Value,
    },
    /// 事件监听
    ListeningForEvent { event_filter: EventFilter },
    /// 等待人工信号
    WaitingForSignal { expected_signal: String },
    /// 子流程执行（等待子实例完成）
    RunSubflow {
        workflow: WorkflowRef,
        input: Option<Value>,
        parent_instance_id: String,
    },
}

impl SuspendReason {
    /// 转换为 SuspensionMeta 存储格式
    pub fn to_meta(&self) -> SuspensionMeta {
        match self {
            SuspendReason::WaitingForDuration { until_ms } => SuspensionMeta {
                reason: "wait".to_string(),
                until_ms: Some(*until_ms),
                service: None,
                payload: None,
                event_filter: None,
                expected_signal: None,
                retry_count: None,
                error: None,
            },
            SuspendReason::ExternalCall {
                service,
                with: _,
                input,
            } => SuspensionMeta {
                reason: "call".to_string(),
                until_ms: None,
                service: Some(service.clone()),
                payload: Some(input.clone()),
                event_filter: None,
                expected_signal: None,
                retry_count: None,
                error: None,
            },
            SuspendReason::ListeningForEvent { event_filter } => SuspensionMeta {
                reason: "listen".to_string(),
                until_ms: None,
                service: None,
                payload: None,
                event_filter: Some(event_filter.clone()),
                expected_signal: None,
                retry_count: None,
                error: None,
            },
            SuspendReason::WaitingForSignal { expected_signal } => SuspensionMeta {
                reason: "signal".to_string(),
                until_ms: None,
                service: None,
                payload: None,
                event_filter: None,
                expected_signal: Some(expected_signal.clone()),
                retry_count: None,
                error: None,
            },
            SuspendReason::RunSubflow {
                workflow,
                input,
                parent_instance_id: _,
            } => SuspensionMeta {
                reason: "run".to_string(),
                until_ms: None,
                service: Some(format!(
                    "{}::{}@{}",
                    workflow.namespace, workflow.name, workflow.version
                )),
                payload: input.clone(),
                event_filter: None,
                expected_signal: None,
                retry_count: None,
                error: None,
            },
        }
    }
}

// ─── 位置信息（Span） ───

/// 源码位置信息，用于错误报告
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub line: usize,
    pub column: usize,
    pub offset: usize,
}

impl Span {
    pub const fn new(line: usize, column: usize, offset: usize) -> Self {
        Self {
            line,
            column,
            offset,
        }
    }
}

// ─── 检验错误 ───

/// 校验错误类型
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationErrorKind {
    /// 未知任务类型
    UnknownTaskType,
    /// 缺少必填字段
    MissingRequiredField(String),
    /// JSON 语法错误
    SyntaxError(String),
    /// 任务引用不存在（switch transition target 未定义）
    UnknownTaskReference(String),
    /// 函数引用不存在（call function 未在 use.functions 中定义）
    UnknownFunctionReference(String),
    /// 循环依赖（switch 形成环）
    CyclicDependency(Vec<String>),
    /// 表达式语法错误
    InvalidExpression(String),
    /// 类型不匹配
    TypeMismatch(String),
    /// 重复任务名
    DuplicateTaskName(String),
}

/// 校验错误 —— 携带位置信息
#[derive(Debug, Clone, PartialEq)]
pub struct ValidationError {
    pub span: Option<Span>,
    pub kind: ValidationErrorKind,
    pub message: String,
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(span) = self.span {
            write!(
                f,
                "line {}, col {}: {:?}: {}",
                span.line, span.column, self.kind, self.message
            )
        } else {
            write!(f, "{:?}: {}", self.kind, self.message)
        }
    }
}

// ─── WorkflowInstance 构造器 ───

impl WorkflowInstance {
    /// 创建新的工作流实例（标准相位：创建即 `Pending`，start 驱动后进入 `Running`）
    pub fn new(
        definition: &WorkflowDefinition,
        input: Value,
        now_ms: i64,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            definition_ns: definition.document.namespace.clone(),
            definition_name: definition.document.name.clone(),
            definition_version: definition.document.version.clone(),
            status: InstanceStatus::Pending,
            context: input,
            task_stack: Vec::new(),
            current_task_index: 0,
            created_at: now_ms,
            updated_at: now_ms,
            output: None,
            fault: None,
            suspension_meta: None,
        }
    }
}

// ─── 测试 ───

#[cfg(test)]
mod tests {
    use super::*;

    // ─── 序列化往返测试 ───

    #[test]
    fn test_workflow_instance_new() {
        let def = WorkflowDefinition {
            id: None,
            document: Document {
                dsl: "1.0.0".to_string(),
                namespace: "test".to_string(),
                name: "minimal".to_string(),
                version: "1.0".to_string(),
                title: None,
                summary: None,
                tags: None,
            },
            do_tasks: vec![],
            input: None,
            output: None,
            timeout: None,
            use_components: None,
            schedule: Default::default(),
            auth: Default::default(),
            secrets: Default::default(),
            constants: Default::default(),
            task_meta: Default::default(),
        raw_yaml: None,
        };

        let inst = WorkflowInstance::new(&def, serde_json::json!({"key": "value"}), 1000);
        assert_eq!(inst.status, InstanceStatus::Pending);
        assert_eq!(inst.definition_name, "minimal");
        assert_eq!(inst.current_task_index, 0);
        assert_eq!(inst.context, serde_json::json!({"key": "value"}));
        assert!(inst.task_stack.is_empty());
        assert_eq!(inst.created_at, 1000);
    }

    #[test]
    fn test_instance_status_is_terminal() {
        assert!(!InstanceStatus::Running.is_terminal());
        assert!(!InstanceStatus::Suspended.is_terminal());
        assert!(InstanceStatus::Completed.is_terminal());
        assert!(InstanceStatus::Failed.is_terminal());
        assert!(InstanceStatus::Cancelled.is_terminal());
    }

    #[test]
    fn test_instance_status_is_resumable() {
        assert!(!InstanceStatus::Running.is_resumable());
        assert!(InstanceStatus::Suspended.is_resumable());
        assert!(!InstanceStatus::Completed.is_resumable());
        assert!(!InstanceStatus::Failed.is_resumable());
        assert!(!InstanceStatus::Cancelled.is_resumable());
    }

    #[test]
    fn test_suspend_reason_to_meta_wait() {
        let reason = SuspendReason::WaitingForDuration { until_ms: 5000 };
        let meta = reason.to_meta();
        assert_eq!(meta.reason, "wait");
        assert_eq!(meta.until_ms, Some(5000));
    }

    #[test]
    fn test_suspend_reason_to_meta_call() {
        let reason = SuspendReason::ExternalCall {
            service: "http".to_string(),
            with: Some(serde_json::json!({"method": "POST"})),
            input: serde_json::json!({"data": 1}),
        };
        let meta = reason.to_meta();
        assert_eq!(meta.reason, "call");
        assert_eq!(meta.service.as_deref(), Some("http"));
    }

    #[test]
    fn test_suspend_reason_to_meta_signal() {
        let reason = SuspendReason::WaitingForSignal {
            expected_signal: "approval".to_string(),
        };
        let meta = reason.to_meta();
        assert_eq!(meta.reason, "signal");
        assert_eq!(meta.expected_signal.as_deref(), Some("approval"));
    }

    // ─── 序列化/反序列化测试 ───

    #[test]
    fn test_document_serde_roundtrip() {
        let doc = Document {
            dsl: "1.0.0".to_string(),
            namespace: "icps".to_string(),
            name: "approval".to_string(),
            version: "1.0".to_string(),
            title: Some("审批流".to_string()),
            summary: None,
            tags: None,
        };
        let json = serde_json::to_string(&doc).unwrap();
        let parsed: Document = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, doc);
    }

    #[test]
    fn test_call_task_serde_roundtrip() {
        let task = Task::Call(CallTask {
            call: CallType::Http,
            with: Some(serde_json::json!({
                "method": "POST",
                "endpoint": "https://example.com/api"
            })),
        });

        let named = NamedTask {
            name: "callService".to_string(),
            task,
        };

        let json = serde_json::to_string(&named).unwrap();
        let parsed: NamedTask = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.name, "callService");
        match &parsed.task {
            Task::Call(c) => {
                assert_eq!(c.call, CallType::Http);
                assert!(c.with.is_some());
            }
            _ => panic!("expected Call task"),
        }
    }

    #[test]
    fn test_switch_task_serde_roundtrip() {
        let switch = SwitchTask {
            conditions: vec![SwitchCondition {
                condition: Some("${ .amount > 10000 }".to_string()),
                transition: "seniorApproval".to_string(),
            }],
            default_condition: Some(SwitchCondition {
                condition: None,
                transition: "directorApproval".to_string(),
            }),
        };

        let named = NamedTask {
            name: "checkAmount".to_string(),
            task: Task::Switch(switch),
        };

        let json = serde_json::to_string(&named).unwrap();
        let parsed: NamedTask = serde_json::from_str(&json).unwrap();
        match &parsed.task {
            Task::Switch(s) => {
                assert_eq!(s.conditions.len(), 1);
                assert_eq!(s.conditions[0].transition, "seniorApproval");
                assert!(s.default_condition.is_some());
            }
            _ => panic!("expected Switch task"),
        }
    }

    #[test]
    fn test_workflow_definition_serde_roundtrip() {
        let def = WorkflowDefinition {
            id: None,
            document: Document {
                dsl: "1.0.0".to_string(),
                namespace: "test".to_string(),
                name: "example".to_string(),
                version: "1.0".to_string(),
                title: None,
                summary: None,
                tags: None,
            },
            do_tasks: vec![
                NamedTask {
                    name: "step1".to_string(),
                    task: Task::Call(CallTask {
                        call: CallType::Http,
                        with: None,
                    }),
                },
                NamedTask {
                    name: "step2".to_string(),
                    task: Task::Wait(WaitTask {
                        wait: "PT1H".to_string(),
                    }),
                },
            ],
            input: None,
            output: None,
            timeout: None,
            use_components: None,
            schedule: Default::default(),
            auth: Default::default(),
            secrets: Default::default(),
            constants: Default::default(),
            task_meta: Default::default(),
        raw_yaml: None,
        };

        let json = serde_json::to_string_pretty(&def).unwrap();
        let parsed: WorkflowDefinition = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.document.name, "example");
        assert_eq!(parsed.do_tasks.len(), 2);
    }

    #[test]
    fn test_fork_task_structure() {
        let fork = ForkTask {
            branches: vec![
                ForkBranch {
                    name: "branch1".to_string(),
                    tasks: vec![NamedTask {
                        name: "task1".to_string(),
                        task: Task::Call(CallTask {
                            call: CallType::Http,
                            with: None,
                        }),
                    }],
                },
                ForkBranch {
                    name: "branch2".to_string(),
                    tasks: vec![NamedTask {
                        name: "task2".to_string(),
                        task: Task::Call(CallTask {
                            call: CallType::Http,
                            with: None,
                        }),
                    }],
                },
            ],
            compete: Some(false),
        };
        assert_eq!(fork.branches.len(), 2);
        assert_eq!(fork.compete, Some(false));
    }

    #[test]
    fn test_try_catch_task_structure() {
        let try_catch = TryCatchTask {
            r#try: vec![NamedTask {
                name: "riskyCall".to_string(),
                task: Task::Call(CallTask {
                    call: CallType::Http,
                    with: None,
                }),
            }],
            catch: vec![CatchClause {
                errors: Some(vec!["HTTPError".to_string()]),
                tasks: vec![NamedTask {
                    name: "compensate".to_string(),
                    task: Task::Call(CallTask {
                        call: CallType::Http,
                        with: None,
                    }),
                }],
            }],
        };
        assert_eq!(try_catch.r#try.len(), 1);
        assert_eq!(try_catch.catch.len(), 1);
        assert_eq!(try_catch.catch[0].errors.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn test_validation_error_display() {
        let err = ValidationError {
            span: Some(Span::new(10, 5, 150)),
            kind: ValidationErrorKind::UnknownTaskType,
            message: "unknown task type 'foo'".to_string(),
        };
        let display = format!("{}", err);
        assert!(display.contains("line 10"));
        assert!(display.contains("col 5"));
        assert!(display.contains("UnknownTaskType"));
        assert!(display.contains("unknown task type"));
    }

    #[test]
    fn test_all_task_types_serde() {
        // 验证所有任务类型都能正确序列化/反序列化
        let tasks: Vec<Task> = vec![
            Task::Call(CallTask { call: CallType::Http, with: None }),
            Task::Do(DoTask { tasks: vec![] }),
            Task::Switch(SwitchTask { conditions: vec![], default_condition: None }),
            Task::Fork(ForkTask { branches: vec![], compete: None }),
            Task::ForEach(ForEachTask { input: "${ .items }".into(), iteration: "item".into(), tasks: vec![] }),
            Task::Wait(WaitTask { wait: "PT5S".into() }),
            Task::Listen(ListenTask { listen: EventFilter { event_type: None, source: None, subject: None } }),
            Task::Emit(EmitTask { emit: EmitEvent { event_type: "done".into(), source: None, data: None } }),
            Task::Set(SetTask { variable: "x".into(), value: "${ .a + .b }".into()}),
            Task::Raise(RaiseTask { raise: ErrorDef { r#type: "HTTPError".into(), title: "HTTP 500".into(), status: None, detail: None } }),
            Task::TryCatch(TryCatchTask { r#try: vec![], catch: vec![] }),
            Task::Run(RunTask { workflow: WorkflowRef { namespace: "ns".into(), name: "sub".into(), version: "1".into() }, input: None }),
        ];

        for task in &tasks {
            let named = NamedTask { name: "test".to_string(), task: task.clone() };
            let json = serde_json::to_string(&named).unwrap();
            let parsed: NamedTask = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed.name, "test");
        }
    }
}
