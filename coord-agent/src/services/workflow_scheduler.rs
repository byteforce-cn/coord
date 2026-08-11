// coord-agent/services/workflow_scheduler.rs
// 工作流调度器 —— 标准 §Scheduling（schedule.every / cron / after / on）
//
// - `every`: 周期执行（ISO 8601 间隔）
// - `cron`:  CRON 表达式（coord-core::workflow::cron）
// - `after`: 完成后延迟重启
// - `on`:    事件触发（订阅事件 → 启动新实例）
//
// 后台 tick 循环扫描已部署定义（schedule 非空），到期触发 `WorkflowEngineService.start_instance`。
// 事件触发（on）由调用方将事件投递到 `handle_event`。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde_json::Value;

use coord_core::workflow::model::InstanceStatus;

use super::workflow::phase4::WorkflowEngineService;

/// 调度器触发事件（on 模式）
#[derive(Debug, Clone)]
pub struct WorkflowEvent {
    pub event_type: String,
    pub source: Option<String>,
    pub data: Value,
}

/// 工作流调度器
pub struct WorkflowScheduler {
    engine: Arc<WorkflowEngineService>,
    /// definition_id → 下次触发时间（Unix ms）
    next_fires: Mutex<HashMap<String, i64>>,
    /// definition_id → 上次完成时间（Unix ms），after 模式
    last_completed: Mutex<HashMap<String, i64>>,
}

impl WorkflowScheduler {
    /// 创建调度器
    pub fn new(engine: Arc<WorkflowEngineService>) -> Self {
        Self {
            engine,
            next_fires: Mutex::new(HashMap::new()),
            last_completed: Mutex::new(HashMap::new()),
        }
    }

    /// 启动后台调度循环（间隔 1s）
    pub fn spawn(self: &Arc<Self>) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                this.tick().await;
            }
        });
    }

    /// 单次调度检查
    async fn tick(&self) {
        let now_ms = now_ms();
        let defs = match self.engine.list_definitions("", usize::MAX, None).await {
            Ok(d) => d,
            Err(_) => return,
        };

        for def in &defs {
            if def.schedule.is_empty() {
                continue;
            }
            let id = match &def.id {
                Some(i) => i.clone(),
                None => continue,
            };

            // every：周期执行
            if let Some(every) = &def.schedule.every {
                if let Some(interval_ms) =
                    coord_core::workflow::engine::parse_iso8601_duration_ms(every)
                {
                    self.maybe_fire_interval(&id, interval_ms, now_ms).await;
                }
            }

            // cron：CRON 表达式
            if let Some(cron_expr) = &def.schedule.cron {
                if let Ok(schedule) = coord_core::workflow::cron::parse_cron(cron_expr) {
                    self.maybe_fire_cron(&id, &schedule, now_ms).await;
                }
            }

            // after：完成后延迟重启
            if let Some(after) = &def.schedule.after {
                if let Some(delay_ms) =
                    coord_core::workflow::engine::parse_iso8601_duration_ms(after)
                {
                    self.maybe_fire_after(&id, delay_ms, now_ms).await;
                }
            }
        }
    }

    /// every 模式：到期触发，并推进下次触发时间
    async fn maybe_fire_interval(&self, id: &str, interval_ms: i64, now_ms: i64) {
        let should_fire = {
            let mut fires = self.next_fires.lock().unwrap();
            match fires.get(id).copied() {
                Some(n) => now_ms >= n,
                None => {
                    // 首次见到：立即排程，下次触发 = now + interval
                    fires.insert(id.to_string(), now_ms + interval_ms);
                    false
                }
            }
        };
        if should_fire {
            self.engine.start_instance(id, Value::Object(Default::default())).await.ok();
            self.next_fires
                .lock()
                .unwrap()
                .insert(id.to_string(), now_ms + interval_ms);
        }
    }

    /// cron 模式：到期触发，并推进到下一次匹配
    async fn maybe_fire_cron(
        &self,
        id: &str,
        schedule: &coord_core::workflow::cron::CronSchedule,
        now_ms: i64,
    ) {
        let now_secs = now_ms / 1000;
        let due = {
            let fires = self.next_fires.lock().unwrap();
            fires.get(id).copied().map(|n| now_ms >= n).unwrap_or(false)
        };
        if due {
            self.engine.start_instance(id, Value::Object(Default::default())).await.ok();
        }
        // 推进到下一次匹配
        let mut fires = self.next_fires.lock().unwrap();
        match fires.get(id).copied() {
            Some(_) => {
                if let Some(next_secs) =
                    coord_core::workflow::cron::next_fire(schedule, now_secs)
                {
                    fires.insert(id.to_string(), next_secs * 1000);
                } else {
                    fires.remove(id);
                }
            }
            None => {
                if let Some(next_secs) = coord_core::workflow::cron::next_fire(schedule, now_secs) {
                    fires.insert(id.to_string(), next_secs * 1000);
                }
            }
        }
    }

    /// after 模式：监控已完成实例，完成后延迟重启
    async fn maybe_fire_after(&self, id: &str, delay_ms: i64, now_ms: i64) {
        // 更新本定义最近一次完成时间
        if let Ok(def) = self.engine.get_definition(id).await {
            if let Some(def) = def {
                let instances = self
                    .engine
                    .list_instances(None, Some(&def.document.name), usize::MAX, None)
                    .await
                    .unwrap_or_default();
                let latest_completed = instances
                    .iter()
                    .filter(|i| i.status == InstanceStatus::Completed)
                    .map(|i| i.updated_at)
                    .max();
                if let Some(t) = latest_completed {
                    self.last_completed.lock().unwrap().insert(id.to_string(), t);
                }
            }
        }

        let last = self.last_completed.lock().unwrap().get(id).copied();
        let should_fire = {
            let fires = self.next_fires.lock().unwrap();
            match last {
                // 已有完成记录：完成后延迟重启
                Some(t) => {
                    let next = fires.get(id).copied().unwrap_or(t + delay_ms);
                    now_ms >= next
                }
                None => true, // 尚无完成记录：立即启动一次
            }
        };
        if should_fire {
            self.engine.start_instance(id, Value::Object(Default::default())).await.ok();
            self.next_fires
                .lock()
                .unwrap()
                .insert(id.to_string(), now_ms + delay_ms);
        }
    }

    /// 事件触发（on 模式）：匹配 schedule.on 的定义 → 启动新实例
    pub async fn handle_event(&self, event: &WorkflowEvent) {
        let defs = match self.engine.list_definitions("", usize::MAX, None).await {
            Ok(d) => d,
            Err(_) => return,
        };
        for def in &defs {
            let Some(id) = &def.id else { continue };
            let Some(on) = &def.schedule.on else { continue };
            // 事件类型匹配（过滤器 event_type/source/subject）
            let type_match = on
                .event_type
                .as_ref()
                .map(|et| et == &event.event_type)
                .unwrap_or(true);
            let source_match = on
                .source
                .as_ref()
                .map(|s| event.source.as_ref().map(|es| es == s).unwrap_or(false))
                .unwrap_or(true);
            if type_match && source_match {
                // 标准 §Scheduling：事件触发输入为事件数组
                let input = serde_json::json!([{
                    "type": event.event_type,
                    "source": event.source,
                    "data": event.data,
                }]);
                self.engine.start_instance(id, input).await.ok();
            }
        }
    }
}

/// 当前 Unix 毫秒
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use coord_core::workflow::model::{Document, NamedTask, SetTask, Task, TaskMeta};

    fn make_engine() -> Arc<WorkflowEngineService> {
        Arc::new(WorkflowEngineService::new_for_test())
    }

    fn make_scheduled_def_yaml(schedule_block: &str) -> String {
        format!(
            r#"
document:
  dsl: "1.0.0"
  namespace: test
  name: scheduled-wf
  version: "1.0"
{schedule_block}
do:
  - step:
      set:
        variable: v
        value: "1"
"#
        )
    }

    #[tokio::test]
    async fn test_every_schedule_fires_instances() {
        let engine = make_engine();
        let def_id = engine
            .deploy_definition("test", &make_scheduled_def_yaml("schedule:\n  every: \"PT0.05S\""))
            .await
            .unwrap();
        let scheduler = Arc::new(WorkflowScheduler::new(Arc::clone(&engine)));

        // 手动触发多次 tick 模拟时间推进
        scheduler.tick().await; // 首次：仅排程
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        scheduler.tick().await; // 到期 → 触发
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        scheduler.tick().await; // 再次触发

        let instances = engine
            .list_instances(None, Some("scheduled-wf"), usize::MAX, None)
            .await
            .unwrap();
        // 首次 tick 排程 + 2 次触发 → ≥2 个实例
        assert!(instances.len() >= 2, "expected ≥2 instances, got {}", instances.len());
        assert!(instances.iter().any(|i| i.definition_name == "scheduled-wf"));
        let _ = def_id;
    }

    #[tokio::test]
    async fn test_event_schedule_on_starts_instance() {
        let engine = make_engine();
        let _def_id = engine
            .deploy_definition(
                "test",
                &make_scheduled_def_yaml(
                    "schedule:\n  on:\n    type: \"order.created\"\n    source: \"/orders\"",
                ),
            )
            .await
            .unwrap();
        let scheduler = Arc::new(WorkflowScheduler::new(Arc::clone(&engine)));

        // 匹配事件 → 启动实例
        scheduler
            .handle_event(&WorkflowEvent {
                event_type: "order.created".into(),
                source: Some("/orders".into()),
                data: serde_json::json!({"orderId": "O-1"}),
            })
            .await;

        let instances = engine
            .list_instances(None, Some("scheduled-wf"), usize::MAX, None)
            .await
            .unwrap();
        assert_eq!(instances.len(), 1);
        // 事件触发输入为事件数组
        assert_eq!(instances[0].context[0]["type"], "order.created");

        // 不匹配事件 → 不启动
        scheduler
            .handle_event(&WorkflowEvent {
                event_type: "other.event".into(),
                source: None,
                data: serde_json::json!({}),
            })
            .await;
        let instances = engine
            .list_instances(None, Some("scheduled-wf"), usize::MAX, None)
            .await
            .unwrap();
        assert_eq!(instances.len(), 1);
    }

    #[test]
    fn test_task_meta_construct() {
        // 确保 TaskMeta 可构造（编译期检查）
        let _ = TaskMeta {
            if_condition: None,
            input: None,
            output: None,
            export: None,
            retry: None,
            timeout: None,
        };
        let _ = NamedTask {
            name: "x".into(),
            task: Task::Set(SetTask {
                variable: "v".into(),
                value: "1".into(),
            }),
        };
        let _ = Document {
            dsl: "1.0.0".into(),
            namespace: "test".into(),
            name: "w".into(),
            version: "1.0".into(),
            title: None,
            summary: None,
            tags: None,
        };
    }
}
