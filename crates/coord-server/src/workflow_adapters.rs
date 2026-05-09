//! Concrete port adapters for WorkflowRuntime used in coord-server.
//!
//! The workflow runtime is configured in durable mode. Blocking tasks (`wait`,
//! `call`, `listen`) persist suspension metadata through the Raft-backed
//! `workflow` replicated module; external completion is driven through
//! `ResumeWorkflow`.

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

/// Dispatcher used by durable workflow mode.
///
/// In durable mode `call` tasks suspend before dispatching. External workers
/// observe the persisted suspension and complete it through `ResumeWorkflow`.
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
    if let Some(correlation_id) = &filter.correlation_id {
        let Some(data) = &event.data else {
            return false;
        };
        if data
            .get("correlation_id")
            .and_then(|value| value.as_str())
            .map(|value| value != correlation_id)
            .unwrap_or(true)
        {
            return false;
        }
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

/// Factory: build the coord workflow runtime over a replicated workflow store.
pub fn new_coord_workflow_runtime(store: Arc<MemoryWorkflowStore>) -> CoordWorkflowRuntime {
    CoordWorkflowRuntime::new_durable(
        Arc::new(SystemClock),
        Arc::new(JqEvaluator),
        Arc::new(NoOpTaskDispatcher),
        Arc::new(BroadcastEventBus::new(256)),
        store,
    )
}
