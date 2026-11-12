//! MQ v2 服务壳与能力门禁（ADR-0016 M0-4）。
//!
//! 语义单一归属：`docs/adr/0016-mq-v2.md`；gate 证据与工作项状态见
//! `docs/production/ops/mq-v2-gate.md`。本模块只回答两个问题：
//!
//! 1. `[mq_v2].enabled` 是否为真（ADR-0001/ADR-0016 D11 默认关闭）；
//! 2. 某个能力面是否已经交付（M1+ 分批；未交付 = `FAILED_PRECONDITION`）。
//!
//! 服务**始终注册**（reflection 可见）：关闭时每个 RPC 返回
//! `FAILED_PRECONDITION`，客户端能把"没开"和"网络故障"区分开。关闭态不得切保留
//! 区间、不起分片/后台任务、不改 v1 MQ 的 wire/磁盘/默认行为（ADR-0001）。

use std::collections::BTreeSet;

mod services;

use coord_core::error_code::{attach, CoordErrorCode};
use tonic::{Code, Status};

/// MQ v2 能力面（按契约的能力门禁分组，与 `coord:mq:*` 一一对应）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MqV2Capability {
    /// topic 生命周期/管理面（`coord:mq:manage`）
    Topic,
    /// 写路径（`coord:mq:publish`）
    Publish,
    /// 读路径与消费组（`coord:mq:consume`）
    Consume,
    /// best-effort 推送（`coord:mq:subscribe`）
    Subscribe,
    /// 死信（`coord:mq:dlq`）
    Dlq,
}

impl MqV2Capability {
    /// 契约包名（用于日志/错误消息，不参与请求路由）。
    pub fn package(self) -> &'static str {
        "coord.mq.v2"
    }

    /// 人类可读的能力面（错误消息用；稳定锚点见 `mq-v2-gate.md`）。
    pub fn label(self) -> &'static str {
        match self {
            Self::Topic => "topic management",
            Self::Publish => "publish",
            Self::Consume => "poll/ack + consumer groups",
            Self::Subscribe => "subscribe",
            Self::Dlq => "dead letter",
        }
    }
}

/// 运行时配置（由 `coord/src/main.rs` 从 `[mq_v2]` 段映射）。
///
/// 默认值与边界在 `coord/src/config.rs`（启动校验的单一归属）；本结构不做二级
/// 默认值，避免"配置文件里没有、代码里又有另一套默认"的双口径。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqV2RuntimeConfig {
    pub log_shards: u32,
    pub group_shards: u32,
    pub max_partitions_per_shard: u32,
    pub default_retention_secs: u64,
    pub max_topic_bytes: u64,
    pub max_message_bytes: u32,
    pub segment_bytes: u64,
    pub poll_max_bytes: u32,
    pub poll_max_messages: u32,
    pub long_poll_max_ms: u32,
    pub group_session_timeout_ms: u32,
}

impl Default for MqV2RuntimeConfig {
    fn default() -> Self {
        // 与 `coord::config::MqV2Config::default()` 逐字一致（设计 §5.9）。
        Self {
            log_shards: 8,
            group_shards: 4,
            max_partitions_per_shard: 1024,
            default_retention_secs: 604_800,
            max_topic_bytes: 1024 * 1024 * 1024,
            max_message_bytes: 1024 * 1024,
            segment_bytes: 64 * 1024 * 1024,
            poll_max_bytes: 4 * 1024 * 1024,
            poll_max_messages: 1_000,
            long_poll_max_ms: 30_000,
            group_session_timeout_ms: 30_000,
        }
    }
}

/// MQ v2 运行时状态：开关 + 已交付能力集合 + 配置快照。
#[derive(Debug, Clone)]
pub struct MqV2State {
    enabled: bool,
    implemented: BTreeSet<MqV2Capability>,
    config: MqV2RuntimeConfig,
}

impl Default for MqV2State {
    fn default() -> Self {
        Self::disabled()
    }
}

impl MqV2State {
    /// 关闭态（默认；不注册任何 v2 行为）。
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            implemented: BTreeSet::new(),
            config: MqV2RuntimeConfig::default(),
        }
    }

    /// 按配置构造；能力集合由装配方通过 [`Self::with_implemented`] 声明。
    pub fn new(enabled: bool, config: MqV2RuntimeConfig) -> Self {
        Self {
            enabled,
            implemented: BTreeSet::new(),
            config,
        }
    }

    /// 声明已交付的能力（供后续 gate 与测试使用）。
    pub fn with_implemented(
        mut self,
        capabilities: impl IntoIterator<Item = MqV2Capability>,
    ) -> Self {
        self.implemented = capabilities.into_iter().collect();
        self
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn config(&self) -> &MqV2RuntimeConfig {
        &self.config
    }

    pub fn is_implemented(&self, capability: MqV2Capability) -> bool {
        self.implemented.contains(&capability)
    }

    /// 门禁：关闭 → `FAILED_PRECONDITION`；开启但能力未交付 → 同样
    /// `FAILED_PRECONDITION`（消息点名能力，便于运维判断是"没开"还是"还没到
    /// 那个 gate"）。不用 `UNIMPLEMENTED`：那会让"集群没开 MQ v2"看起来像
    /// "客户端走错地址"。用户级拒绝不得变成 `INTERNAL`/`UNAVAILABLE`。
    pub fn gate(&self, capability: MqV2Capability) -> Result<(), Status> {
        if !self.enabled {
            return Err(attach(
                Status::new(
                    Code::FailedPrecondition,
                    "mq_v2 disabled: [mq_v2].enabled = false (the coord.mq.v2 service \
                     shell is registered but every RPC is gated; ADR-0016 D11)",
                ),
                CoordErrorCode::FailedPrecondition,
            ));
        }
        if !self.is_implemented(capability) {
            return Err(attach(
                Status::new(
                    Code::FailedPrecondition,
                    format!(
                        "mq_v2 enabled but {} ({}) is not implemented yet \
                         (MQ v2 gate pending; see docs/production/ops/mq-v2-gate.md)",
                        capability.label(),
                        capability.package()
                    ),
                ),
                CoordErrorCode::FailedPrecondition,
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use coord_core::error_code::error_code_of;

    #[test]
    fn default_state_is_disabled() {
        let state = MqV2State::default();
        assert!(!state.is_enabled());
        assert_eq!(state.config(), &MqV2RuntimeConfig::default());
        for cap in [
            MqV2Capability::Topic,
            MqV2Capability::Publish,
            MqV2Capability::Consume,
            MqV2Capability::Subscribe,
            MqV2Capability::Dlq,
        ] {
            let status = state.gate(cap).expect_err("disabled must reject");
            assert_eq!(status.code(), Code::FailedPrecondition);
            assert!(
                status.message().contains("mq_v2 disabled"),
                "{:?}",
                status.message()
            );
            assert_eq!(
                error_code_of(&status).as_deref(),
                Some("FAILED_PRECONDITION")
            );
        }
    }

    #[test]
    fn enabled_without_delivered_capability_still_rejects() {
        let state = MqV2State::new(true, MqV2RuntimeConfig::default());
        let status = state
            .gate(MqV2Capability::Publish)
            .expect_err("M0 has no implemented capability");
        assert_eq!(status.code(), Code::FailedPrecondition);
        assert!(
            status.message().contains("coord.mq.v2"),
            "错误必须点名契约包：{:?}",
            status.message()
        );
        assert!(
            status.message().contains("not implemented"),
            "{:?}",
            status.message()
        );
        assert_eq!(
            error_code_of(&status).as_deref(),
            Some("FAILED_PRECONDITION")
        );
    }

    #[test]
    fn delivered_capability_passes_gate() {
        let state = MqV2State::new(true, MqV2RuntimeConfig::default())
            .with_implemented([MqV2Capability::Topic]);
        assert!(state.gate(MqV2Capability::Topic).is_ok());
        assert!(state.gate(MqV2Capability::Consume).is_err());
    }
}
