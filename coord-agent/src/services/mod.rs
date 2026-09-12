// coord-agent: 可插拔服务 — 模块声明
//
// 每个高级基础服务为一个独立模块，实现 `BaseService` trait（契约见 `service.rs`）。
// 服务由 `PluginManager` 统一托管：注册进插件表（内建插件）、由其驱动生命周期、
// 并由 `Plugin::grpc_service()` 暴露 gRPC 面。

pub mod cache;
pub mod circuit_breaker;
pub mod config_center;
pub mod event_notification;
pub mod grpc_handlers;
pub mod idgen;
pub mod leader_election;
pub mod lock;
pub mod mq;
pub mod mq_event_provider;
pub mod opa;
pub mod policy;
pub mod rate_limiter;
pub mod registry;
pub mod replication;
pub mod scheduler;
pub mod transit;
pub mod workflow;
pub mod workflow_scheduler;
pub mod workflow_store;
