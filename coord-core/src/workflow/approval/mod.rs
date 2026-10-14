//! v2 审批模型（中国式审批：会签 / 或签 / 票签 / 一票否决 / 加签 / 减签 / 驳回至节点 /
//! 转办 / 委派 / 阅签 / 撤回 / 终止）。
//!
//! 与 ADR-0013 的对应：
//! - **D2 五层状态模型**：`Instance`（实例）/ `Branch`（节点实例）/ `Task`（席位）/
//!   `Round`（轮次）/ `VoteRecord`（投票记录，内嵌于 `Task`），外加 task > branch >
//!   instance 三级变量作用域；
//! - **D8 存储布局**：v2 实体走独立 KV 前缀，与 v1 的实例扫描面互不可见。
//!
//! 语义来源：当前对外口径收敛在 `docs/production/workflow-semantics.md`；字段清单与逐条
//! 状态转换表的冻结证据在 `spike/2026-10/spike-1-state-model/`（只读）。
//!
//! 本模块默认关闭、尚未接线：能力面（proto/RPC）与事件契约的固化在生产验收流程内完成。

pub mod model;
