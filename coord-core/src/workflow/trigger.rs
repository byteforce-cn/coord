// coord-core/workflow/trigger.rs
//
// MQ 触发器声明（ADR-0016 D12）—— 定义里「消息 → 实例」的绑定。
//
// 形状固定为顶层 `trigger` 块（与 `schedule` 并列），两条 DSL 解析路径（CNCF SW /
// 遗留 `document/do`）都从**已解析的顶层 value** 取，避免两套 parser 各自复制一份语义。
//
// 边界：这里只做声明与纯判定（形状校验、过滤匹配、组名/幂等键派生）；消费与 Start 是
// Agent 侧分发器（`coord-agent/src/services/workflow_trigger.rs`）的事。

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::model::EventFilter;

/// `max_attempts` 缺省值（ADR-0016 D12：连续失败到上限移 DLQ 并 Ack）。
pub const DEFAULT_MAX_ATTEMPTS: u32 = 3;

/// `max_attempts` 上限：上限只为把「无限重试」挡在部署期，不是吞吐调参。
pub const MAX_ATTEMPTS_LIMIT: u32 = 32;

/// 毒消息进 DLQ 的稳定原因锚点（日志/测试/runbook 逐字断言）。
pub const DLQ_REASON_MAX_ATTEMPTS: &str = "workflow_trigger_max_attempts";

/// 消费组名前缀；完整组名 = `wf-trigger:<definition_id>`。
///
/// 组名按 `definition_id` 派生（ADR-0016 D12）：同一定义的多个 Agent 因此落在同一个组，
/// 一个分区只归一个成员；重部署产生新 id ⇒ 新组，不会继承旧定义已提交的位点。
pub const GROUP_PREFIX: &str = "wf-trigger:";

/// 顶层 `trigger` 块。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct TriggerConfig {
    /// 订阅的 topic 名（必填；topic 必须已存在，触发器不自动建）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    /// 消息过滤（与 `schedule.on` 同形状）：`ce-type`/`ce-source`/`ce-subject`。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<EventFilter>,
    /// 连续失败上限；缺省 [`DEFAULT_MAX_ATTEMPTS`]。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<u32>,
}

impl TriggerConfig {
    /// 无 trigger 声明（既有定义）。
    pub fn is_empty(&self) -> bool {
        self.topic.is_none() && self.filter.is_none() && self.max_attempts.is_none()
    }

    /// 从**顶层** YAML/JSON value 取 `trigger`（缺省 = 空配置；形状不对 = `Err`）。
    ///
    /// 在 Deploy/Rollback 里对已解析的 `value` 调用（两种 DSL 格式共用一条路径）。
    pub fn from_top_level(value: &serde_json::Value) -> Result<Self, String> {
        match value.get("trigger") {
            None | Some(serde_json::Value::Null) => Ok(Self::default()),
            Some(trigger) => {
                serde_json::from_value(trigger.clone()).map_err(|err| format!("trigger: {err}"))
            }
        }
    }

    /// 部署期形状校验（失败即拒绝部署，不落库）。
    pub fn validate(&self) -> Result<(), String> {
        if self.is_empty() {
            return Ok(());
        }
        let topic = self
            .topic
            .as_deref()
            .map(str::trim)
            .filter(|topic| !topic.is_empty())
            .ok_or_else(|| "trigger.topic must not be empty".to_string())?;
        if topic.len() > 255 {
            return Err(format!(
                "trigger.topic must be 1..=255 bytes (got {})",
                topic.len()
            ));
        }
        if let Some(max_attempts) = self.max_attempts {
            if !(1..=MAX_ATTEMPTS_LIMIT).contains(&max_attempts) {
                return Err(format!(
                    "trigger.max_attempts must be 1..={MAX_ATTEMPTS_LIMIT} (got {max_attempts})"
                ));
            }
        }
        Ok(())
    }

    /// 生效的失败上限（未声明 = [`DEFAULT_MAX_ATTEMPTS`]）。
    pub fn effective_max_attempts(&self) -> u32 {
        self.max_attempts.unwrap_or(DEFAULT_MAX_ATTEMPTS)
    }

    /// 消费组名（`wf-trigger:<definition_id>`）。
    pub fn group_name(&self, definition_id: &str) -> String {
        format!("{GROUP_PREFIX}{definition_id}")
    }

    /// 消息是否命中过滤（空 filter / 字段全空 = 全命中）。
    ///
    /// 字段映射与 CloudEvents 头一致：`event_type`/`event_types` ↔ `ce-type`，
    /// `source` ↔ `ce-source`，`subject` ↔ `ce-subject`。未声明的维度不约束。
    pub fn matches(&self, headers: &BTreeMap<String, String>) -> bool {
        let Some(filter) = &self.filter else {
            return true;
        };
        let event_type = headers.get("ce-type").map(String::as_str);
        if !filter.event_types.is_empty() {
            if !event_type
                .map(|kind| filter.event_types.iter().any(|accepted| accepted == kind))
                .unwrap_or(false)
            {
                return false;
            }
        } else if let Some(expected) = &filter.event_type {
            if event_type != Some(expected.as_str()) {
                return false;
            }
        }
        if let Some(expected) = &filter.source {
            if headers.get("ce-source").map(String::as_str) != Some(expected.as_str()) {
                return false;
            }
        }
        if let Some(expected) = &filter.subject {
            if headers.get("ce-subject").map(String::as_str) != Some(expected.as_str()) {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    fn yaml(text: &str) -> serde_json::Value {
        serde_yaml::from_str(text).expect("yaml")
    }

    #[test]
    fn trigger_parses_the_top_level_block_and_applies_defaults() {
        let value = yaml(
            "trigger:\n  topic: orders\n  filter:\n    event_type: order.created\n  max_attempts: 5\n",
        );
        let trigger = TriggerConfig::from_top_level(&value).expect("parse");
        assert_eq!(trigger.topic.as_deref(), Some("orders"));
        assert_eq!(trigger.effective_max_attempts(), 5);
        trigger.validate().expect("valid");
        assert!(trigger.matches(&headers(&[("ce-type", "order.created")])));
        assert!(!trigger.matches(&headers(&[("ce-type", "order.cancelled")])));
        assert!(!trigger.matches(&headers(&[])));
        assert_eq!(trigger.group_name("default-abc"), "wf-trigger:default-abc");
    }

    #[test]
    fn a_missing_trigger_block_is_the_empty_config() {
        let trigger = TriggerConfig::from_top_level(&yaml("document:\n  dsl: 1.0.0\n"))
            .expect("absent trigger is fine");
        assert!(trigger.is_empty());
        trigger.validate().expect("empty trigger is valid");
        assert!(trigger.matches(&headers(&[("ce-type", "anything")])));
    }

    #[test]
    fn a_malformed_trigger_block_is_rejected_with_the_trigger_prefix() {
        let err = TriggerConfig::from_top_level(&yaml("trigger: 42\n")).expect_err("must fail");
        assert!(err.starts_with("trigger:"), "unexpected: {err}");
    }

    #[test]
    fn trigger_without_max_attempts_defaults_to_three_and_rejects_out_of_range() {
        let trigger = TriggerConfig {
            topic: Some("orders".into()),
            ..Default::default()
        };
        assert_eq!(trigger.effective_max_attempts(), DEFAULT_MAX_ATTEMPTS);
        trigger.validate().expect("defaults are valid");
        for bad in [0, MAX_ATTEMPTS_LIMIT + 1] {
            let trigger = TriggerConfig {
                topic: Some("orders".into()),
                max_attempts: Some(bad),
                ..Default::default()
            };
            assert!(trigger.validate().is_err(), "{bad} must be rejected");
        }
    }

    #[test]
    fn an_empty_or_oversized_topic_is_rejected() {
        for bad in ["", "   ", &"x".repeat(256)] {
            let trigger = TriggerConfig {
                topic: Some(bad.to_string()),
                ..Default::default()
            };
            assert!(trigger.validate().is_err(), "{bad:?} must be rejected");
        }
    }

    #[test]
    fn event_types_lists_match_any_listed_type() {
        let value = yaml("trigger:\n  topic: orders\n  filter:\n    event_types: [a, b]\n");
        let trigger = TriggerConfig::from_top_level(&value).expect("parse");
        assert!(trigger.matches(&headers(&[("ce-type", "a")])));
        assert!(trigger.matches(&headers(&[("ce-type", "b")])));
        assert!(!trigger.matches(&headers(&[("ce-type", "c")])));
    }

    #[test]
    fn source_and_subject_filters_are_conjunctive() {
        let value =
            yaml("trigger:\n  topic: orders\n  filter:\n    source: erp\n    subject: /orders/1\n");
        let trigger = TriggerConfig::from_top_level(&value).expect("parse");
        assert!(trigger.matches(&headers(&[
            ("ce-source", "erp"),
            ("ce-subject", "/orders/1")
        ])));
        assert!(!trigger.matches(&headers(&[("ce-source", "erp")])));
        assert!(!trigger.matches(&headers(&[
            ("ce-source", "erp"),
            ("ce-subject", "/orders/2")
        ])));
    }
}
