//! Client（gossip 代理）运行模式 — Phase 4D stub。
//!
//! 该模式将在 Phase 4D 实现，当前仅占位报错，以便二进制能正常编译。

use crate::cli::ClientArgs;

/// Entry point for `coord client`.
///
/// # Errors
/// 当前总是返回错误，Phase 4D 实现后移除此说明。
pub(crate) async fn run(_args: ClientArgs) -> anyhow::Result<()> {
    anyhow::bail!(
        "`coord client` mode is not yet implemented (planned for Phase 4D). \
         Use `coord server` or `coord dev` to start a Raft server node."
    )
}
