//! KV-T v2 服务壳与能力门禁（ADR-0015 G0）。
//!
//! 语义单一归属：`docs/adr/0015-kv-t.md`。本模块只回答两个问题：
//!
//! 1. `[kv_t].enabled` 是否为真（ADR-0001 默认关闭）；
//! 2. 某个 v2 能力是否已经交付（G0–G6 分批；未交付 = `FAILED_PRECONDITION`）。
//!
//! v2 服务**始终注册**（reflection 可见）；`implemented` 只包含已交付 gate 的
//! 能力（当前为 G1 的 `Tso`、`Kv` 读与单 Region 写，G2 的 `Txn` 读阶段，G3 的
//! `Txn` Commit）。未交付的 handler 返回 `UNIMPLEMENTED`，未开启/未交付的能力
//! 返回 `FAILED_PRECONDITION`。后续 gate 在自己的 issue 里把对应能力加入
//! `implemented` 并实现 handler；不得用"未实现就返回 UNIMPLEMENTED"绕过门禁
//! （那会让调用方无法区分"关闭"与"写错地址"）。

use std::collections::BTreeSet;

pub mod commit_log_shards;
pub mod commit_watermark;
pub mod decision_apply;
pub(crate) mod lease;
pub mod safe_ts;
pub mod sequencer;
mod services;
pub(crate) mod txn;
pub mod watch;

use coord_core::error_code::{attach, CoordErrorCode};
use tonic::{Code, Status};

/// v2 能力面（与 `coord.<domain>.v2` 包一一对应）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum KvTCapability {
    /// `coord.tso.v2`（TSO 批次/单值）
    Tso,
    /// `coord.kv.v2`
    Kv,
    /// `coord.txn.v2`
    Txn,
    /// `coord.watch.v2`
    Watch,
    /// `coord.lease.v2`
    Lease,
    /// `coord.storage.v2`
    Storage,
}

impl KvTCapability {
    /// 契约包名（用于日志/错误消息，不参与请求路由）。
    pub fn package(self) -> &'static str {
        match self {
            Self::Tso => "coord.tso.v2",
            Self::Kv => "coord.kv.v2",
            Self::Txn => "coord.txn.v2",
            Self::Watch => "coord.watch.v2",
            Self::Lease => "coord.lease.v2",
            Self::Storage => "coord.storage.v2",
        }
    }
}

/// 运行时配置（由 `coord/src/main.rs` 从 `[kv_t]` 段映射）。
///
/// 字段语义与默认值在 ADR-0015 D9/D12；本结构不做二级默认值，避免出现
/// "配置文件里没有、代码里又有另一套默认"的双口径。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvTRuntimeConfig {
    pub tso_batch_size: u32,
    pub commit_sequencer_batch: u32,
    pub commit_log_shard_records: u64,
    pub commit_log_shard_secs: u64,
    pub shard_seal_timeout_secs: u64,
    pub watch_retention: u64,
    pub max_txn_bytes: u64,
    pub max_read_set: u32,
    pub max_range: u32,
    pub read_ts_wait_ms: u64,
    pub txn_ttl_secs: u64,
}

impl Default for KvTRuntimeConfig {
    fn default() -> Self {
        // 与 `coord::config::KvTConfig::default()` 逐字一致（ADR-0015 D9）。
        Self {
            tso_batch_size: 1024,
            commit_sequencer_batch: 256,
            commit_log_shard_records: 1_000_000,
            commit_log_shard_secs: 3_600,
            shard_seal_timeout_secs: 300,
            watch_retention: 86_400,
            max_txn_bytes: 4 * 1024 * 1024,
            max_read_set: 4_096,
            max_range: 10_000,
            read_ts_wait_ms: 2_000,
            txn_ttl_secs: 60,
        }
    }
}

/// KV-T 运行时状态：开关 + 已交付能力集合 + 配置快照。
#[derive(Debug, Clone)]
pub struct KvTState {
    enabled: bool,
    implemented: BTreeSet<KvTCapability>,
    config: KvTRuntimeConfig,
}

impl Default for KvTState {
    fn default() -> Self {
        Self::disabled()
    }
}

impl KvTState {
    /// 关闭态（v1 默认路径；不注册任何 v2 行为）。
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            implemented: BTreeSet::new(),
            config: KvTRuntimeConfig::default(),
        }
    }

    /// 按配置构造；能力集合由装配方通过 [`Self::with_implemented`] 声明。
    pub fn new(enabled: bool, config: KvTRuntimeConfig) -> Self {
        Self {
            enabled,
            implemented: BTreeSet::new(),
            config,
        }
    }

    /// 声明已交付的能力（供后续 gate 与测试使用）。
    pub fn with_implemented(
        mut self,
        capabilities: impl IntoIterator<Item = KvTCapability>,
    ) -> Self {
        self.implemented = capabilities.into_iter().collect();
        self
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn config(&self) -> &KvTRuntimeConfig {
        &self.config
    }

    pub fn is_implemented(&self, capability: KvTCapability) -> bool {
        self.implemented.contains(&capability)
    }

    /// 门禁：关闭 → `FAILED_PRECONDITION`；开启但能力未交付 → 同样
    /// `FAILED_PRECONDITION`（消息点名能力，便于运维判断是"没开"还是"还没到
    /// 那个 gate"）。用户级拒绝不得变成 `INTERNAL`/`UNAVAILABLE`。
    pub fn gate(&self, capability: KvTCapability) -> Result<(), Status> {
        if !self.enabled {
            return Err(attach(
                Status::new(
                    Code::FailedPrecondition,
                    "kv_t disabled: [kv_t].enabled=false (KV-T is default-off; see ADR-0015)",
                ),
                CoordErrorCode::FailedPrecondition,
            ));
        }
        if !self.is_implemented(capability) {
            return Err(attach(
                Status::new(
                    Code::FailedPrecondition,
                    format!(
                        "kv_t enabled but {} is not implemented yet \
                         (KV-T gate pending; see ADR-0015 D10)",
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
        let state = KvTState::default();
        assert!(!state.is_enabled());
        assert_eq!(state.config(), &KvTRuntimeConfig::default());
        for cap in [
            KvTCapability::Tso,
            KvTCapability::Kv,
            KvTCapability::Txn,
            KvTCapability::Watch,
            KvTCapability::Lease,
            KvTCapability::Storage,
        ] {
            let status = state.gate(cap).expect_err("disabled must reject");
            assert_eq!(status.code(), Code::FailedPrecondition);
            assert!(
                status.message().contains("[kv_t].enabled=false"),
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
        let state = KvTState::new(true, KvTRuntimeConfig::default());
        let status = state
            .gate(KvTCapability::Kv)
            .expect_err("G0 has no implemented capability");
        assert_eq!(status.code(), Code::FailedPrecondition);
        assert!(
            status.message().contains("coord.kv.v2"),
            "错误必须点名能力：{:?}",
            status.message()
        );
        assert!(
            status.message().contains("not implemented"),
            "{:?}",
            status.message()
        );
    }

    #[test]
    fn delivered_capability_passes_gate() {
        let state =
            KvTState::new(true, KvTRuntimeConfig::default()).with_implemented([KvTCapability::Kv]);
        assert!(state.gate(KvTCapability::Kv).is_ok());
        assert!(state.gate(KvTCapability::Txn).is_err());
    }
}
