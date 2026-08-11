// coord-core/workflow: Serverless Workflow 引擎核心模块
//
// 基于 CNCF Serverless Workflow 1.0 DSL 规范，提供：
// - model:   领域模型（12种任务类型、实例、状态机）
// - parser:  DSL 两阶段解析器（Phase 1: 语法解析, Phase 2: 语义校验）
// - validate: 语义校验器
// - expression: jq 表达式引擎
// - ports:   端口 trait 抽象层（Clock, Store, Dispatcher 等）
// - tasks:   任务执行模块（每种任务类型独立子模块，纯函数）
// - engine:  WorkflowExecutor（纯状态机，无 I/O，委托 tasks/）
// - runtime: WorkflowRuntime（异步驱动循环）

pub mod expression;
pub mod errors;
pub mod jsonschema;
pub mod cron;
pub mod model;
pub mod parser;
pub mod ports;
pub mod raft_store;
pub mod retry;
pub mod sw;
pub mod validate;
pub mod tasks;
pub mod engine;
pub mod runtime;
