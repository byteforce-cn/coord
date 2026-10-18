//! 全局 TSO 分配器（region 0 system raft；ADR-0015 D2/D12.4，#136）。
//!
//! 只有 region 0 leader 能在本地 `client_write`；follower 返回 `NOT_LEADER`
//! （跨节点转发是后续工作项）。`GetTs` 目前按 `Allocate(1)` 实现；#137 再接入
//! 进程内批次缓存与指标。

use coord_core::error::{Error, Result};
use coord_core::types::NodeID;

use super::type_config::{Command, Response, TsoOp};
use super::CoordRaft;

/// 单个 TSO 批次（左闭右开：`start_ts` 可用，`end_ts` 不可用）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TsoBatch {
    pub start_ts: u64,
    pub end_ts: u64,
}

/// 批次请求归一：`0` 视为 1；超过 `max`（`[kv_t].tso_batch_size`，
/// D9 上限 65536）显式拒绝，不静默截断。
pub fn normalize_batch_size(requested: u32, max: u32) -> Result<u32> {
    let size = requested.max(1);
    if size > max {
        return Err(Error::InvalidArgument(format!(
            "tso batch_size {size} exceeds max {max}"
        )));
    }
    Ok(size)
}

/// region 0 leader 的 TSO 分配器。
///
/// `CoordRaft` 的 `Clone` 是 Arc bump；分配器可随请求临时构造。高水位不在
/// 内存：每次分配都经 raft apply 读/写 `/_sys/tso/high_water`，重启/换 leader
/// 自然接续。
#[derive(Clone)]
pub struct TsoAllocator {
    raft: CoordRaft,
    node_id: NodeID,
}

impl TsoAllocator {
    pub fn new(raft: CoordRaft, node_id: NodeID) -> Self {
        Self { raft, node_id }
    }

    /// 申请一个 `[start_ts, end_ts)` 批次。
    ///
    /// 非 leader：`NotLeader`（调用方应刷新 leader 后重试）；选举窗口：
    /// `ClusterUnavailable`。提交流失败后复核 leader，避免把 leadership 迁移
    /// 误报成存储错误。
    pub async fn allocate(&self, batch_size: u32) -> Result<TsoBatch> {
        let batch_size = batch_size.max(1);
        match self.raft.current_leader().await {
            Some(leader) if leader == self.node_id => {}
            Some(_) => return Err(Error::NotLeader { leader_addr: None }),
            None => {
                return Err(Error::ClusterUnavailable(
                    "region 0 leader unknown (election window)".to_string(),
                ))
            }
        }

        let resp = match self
            .raft
            .client_write(Command::Tso(TsoOp::Allocate { batch_size }))
            .await
        {
            Ok(resp) => resp,
            Err(e) => {
                // 提交流失败可能是 leadership 迁移：先刷新 leader 视图再分类。
                return match self.raft.current_leader().await {
                    Some(leader) if leader != self.node_id => {
                        Err(Error::NotLeader { leader_addr: None })
                    }
                    _ => Err(Error::ClusterUnavailable(format!(
                        "region0 raft tso propose failed: {e}"
                    ))),
                };
            }
        };

        match resp.response() {
            Response::Tso { start_ts, end_ts } => Ok(TsoBatch {
                start_ts: *start_ts,
                end_ts: *end_ts,
            }),
            other => Err(Error::Internal(format!(
                "unexpected region0 tso propose response: {other:?}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_batch_size_bounds() {
        assert_eq!(normalize_batch_size(0, 1024).unwrap(), 1);
        assert_eq!(normalize_batch_size(1024, 1024).unwrap(), 1024);
        assert!(matches!(
            normalize_batch_size(1025, 1024),
            Err(coord_core::error::Error::InvalidArgument(_))
        ));
    }
}
