//! Concrete port adapters for WorkflowRuntime used in coord-server.
//!
//! ## ⚠️ 开发预览 / Development Preview
//!
//! 当前 v2 工作流运行时使用内存存储（[`MemoryWorkflowStore`]）和占位调度器
//! （[`NoOpTaskDispatcher`]），**不可用于生产环境**：
//!
//! - `MemoryWorkflowStore`：实例状态在重启后丢失。
//!   生产环境需实现 `RaftWorkflowStore`（通过 Raft 日志持久化）。
//! - `NoOpTaskDispatcher`：所有 `call` 任务均会返回错误。
//!   生产环境需实现真实的 HTTP/gRPC 调度器。
//!
//! 详见 `doc/adr/adr-001-workflow-migration.md`（ADR-001）。

use async_trait::async_trait;
use coord_core::clock::SystemClock;
use coord_core::workflow::expression::JqEvaluator;
use coord_core::workflow::model::{CloudEvent, EventFilter};
use coord_core::workflow::ports::{EventBus, EventError, TaskDispatcher, TaskError};
use coord_core::workflow::runtime::WorkflowRuntime;
use coord_core::workflow::store::MemoryWorkflowStore;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::broadcast;

// ─── NoOpTaskDispatcher ───────────────────────────────────────────────────────

/// Stub dispatcher that returns an error for all calls.
/// Replace with a real HTTP/gRPC dispatcher in production.
pub struct NoOpTaskDispatcher;

#[async_trait]
impl TaskDispatcher for NoOpTaskDispatcher {
    async fn dispatch(
        &self,
        service: &str,
        _with: &Value,
        _input: Value,
    ) -> Result<Value, TaskError> {
        Err(TaskError::new(format!(
            "NoOpTaskDispatcher: cannot dispatch to '{}'; register a real dispatcher",
            service
        )))
    }
}

// ─── BroadcastEventBus ────────────────────────────────────────────────────────

/// Simple in-process event bus backed by tokio broadcast channels.
pub struct BroadcastEventBus {
    tx: broadcast::Sender<CloudEvent>,
}

impl BroadcastEventBus {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        BroadcastEventBus { tx }
    }
}

#[async_trait]
impl EventBus for BroadcastEventBus {
    async fn emit(&self, event: &CloudEvent) -> Result<(), EventError> {
        // Ignore send errors when there are no subscribers
        let _ = self.tx.send(event.clone());
        Ok(())
    }

    async fn listen(
        &self,
        filter: &EventFilter,
        timeout_ms: Option<u64>,
    ) -> Result<CloudEvent, EventError> {
        let mut rx = self.tx.subscribe();
        let timeout = timeout_ms.map(std::time::Duration::from_millis);

        let wait = async {
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        if event_matches(&event, filter) {
                            return Ok(event);
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => {
                        return Err(EventError("event bus closed".to_string()));
                    }
                }
            }
        };

        match timeout {
            None => wait.await,
            Some(dur) => tokio::time::timeout(dur, wait)
                .await
                .map_err(|_| EventError("listen timeout".to_string()))?,
        }
    }
}

fn event_matches(event: &CloudEvent, filter: &EventFilter) -> bool {
    if let Some(t) = &filter.r#type
        && event.r#type != *t
    {
        return false;
    }
    if let Some(s) = &filter.source
        && event.source != *s
    {
        return false;
    }
    true
}

// ─── CoordWorkflowRuntime type alias ─────────────────────────────────────────

pub type CoordWorkflowRuntime = WorkflowRuntime<
    SystemClock,
    JqEvaluator,
    NoOpTaskDispatcher,
    BroadcastEventBus,
    MemoryWorkflowStore,
>;

/// Factory: build the default (development-preview) workflow runtime.
///
/// **⚠️ 非生产就绪**：使用 `MemoryWorkflowStore` 和 `NoOpTaskDispatcher`。
/// 生产环境需替换为 Raft-backed 存储和真实 dispatcher（见 ADR-001）。
pub fn new_coord_workflow_runtime() -> CoordWorkflowRuntime {
    CoordWorkflowRuntime::new(
        Arc::new(SystemClock),
        Arc::new(JqEvaluator),
        Arc::new(NoOpTaskDispatcher),
        Arc::new(BroadcastEventBus::new(256)),
        Arc::new(MemoryWorkflowStore::new()),
    )
}
