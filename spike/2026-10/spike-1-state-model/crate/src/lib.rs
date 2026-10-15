//! SPIKE 2026-10 · Spike 1 —— 审批工作流 v2 内存状态模型。
//!
//! 决策证据，不并入 main；设计口径见 `../docs/MODEL.md`。
//! 纯内存、无 I/O：时间戳由调用方注入，命令带 `expected_revision`
//! 以便把「单写者 CAS」语义在模型层就固定下来（Spike 2 接 KV Txn 时复用）。

pub mod engine;
pub mod model;

pub use engine::{Engine, MAX_JUMPS};
pub use model::*;
