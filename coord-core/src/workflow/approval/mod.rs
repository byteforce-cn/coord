//! v2 审批模型（中国式审批：会签 / 或签 / 票签 / 一票否决 / 加签 / 减签 / 驳回至节点 /
//! 转办 / 委派 / 阅签 / 撤回 / 终止）。
//!
//! # 模块图
//!
//! - [`model`]：五层实体与枚举 + [`model::ApprovalError`]（纯数据，不读时钟、不做 I/O）；
//! - [`engine`]：命令层状态机与效果流（不变量与判定顺序见其模块 rustdoc）；
//! - [`keys`]：v4 实例键布局 `/_workflow/v4/instances/{instance_id}`；
//! - [`events`]：事件类型草案目录与出站记录构造（记录写 v3 outbox 设施）；
//! - [`store`]：存储端口 [`store::ApprovalStore`] + 内存实现 [`store::MemoryApprovalStore`]
//!   （仅测试用；生产实现是 `coord-agent` 的 `KvApprovalStore`）。
//!
//! # 与 ADR-0013 的对应
//!
//! - **D2 五层状态模型**：[`model::Instance`]（实例）/ [`model::Branch`]（节点实例）/
//!   [`model::Task`]（席位）/ [`model::Round`]（轮次）/ [`model::VoteRecord`]（投票记录，
//!   内嵌于 [`model::Task`]），外加 task > branch > instance 三级变量作用域；
//! - **D3 写路径**：唯一写路径带 CAS，没有无守卫写入形态；
//! - **D8 存储布局**：v2 实体走独立 KV 前缀，与 v1 的实例扫描面互不可见。
//!
//! # 语义来源
//!
//! 当前对外口径见 `docs/production/workflow-semantics.md` 的「v2 审批模型与存储」小节与
//! `docs/adr/0013-workflow-approval-v2.md`；字段清单与逐条状态转换表的冻结证据在
//! `spike/2026-10/spike-1-state-model/`（只读）。
//!
//! 本模块默认关闭、尚未接线：能力面（proto/RPC）、授权与事件契约固化归 M2-C。

pub mod engine;
pub mod events;
pub mod keys;
pub mod model;
pub mod store;
