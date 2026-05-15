//! Application service layer.
//!
//! 本模块从 `coord-core` 重新导出所有应用层 facade，保持与旧导入路径
//! （`crate::application::*_app::*`）的兼容性。
//!
//! 核心 facade 实现驻留于 `coord-core::application`，公开仓库的 transport
//! handler 通过此模块引用，无需感知 facade 的物理位置。

pub use coord_core::application::config_app;
pub use coord_core::application::idgen_app;
pub use coord_core::application::lock_app;
pub use coord_core::application::pki_app;
pub use coord_core::application::policy_app;
pub use coord_core::application::registry_app;
pub use coord_core::application::security_app;
pub use coord_core::application::transit_app;
pub use coord_core::application::workflow_app;
