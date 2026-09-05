// region 0 system raft 治理能力面（R-MR-08 / D1-a，P2）
//
// D1-a（docs/coord-multi-raft-production-plan-2026-09-05.md §4.5）把 PD operator
// 队列经 **region 0 system raft** 承载：全节点复制同一份全序队列，调度只发生在
// region 0 leader 节点，执行由「目标 Region 的当前 leader」节点认领（apply CAS
// 防双认领）。本文件定义 pd 模块依赖的 raft 端口：
//
//   - `SystemRaftHandle` trait：`current_leader` / `propose_pd` / `pd_queue`。
//     定义在 raft 层（raft 自我描述能力；`openraft::` 交互收敛在本目录内——
//     P1-06 类型隔离），`pd` 模块的 `PlacementDriver`/`OperatorExecutor` 只经
//     trait object 使用本端口，不接触任何 openraft 类型（与 `RegionRaftHandle`
//     同模式，见 `raft/region_runtime.rs`）。
//
// 真实实现 `CoordSystemRaftHandle` 包装 region 0 `CoordRaft` 与其 MVCC
// （main.rs：region 0 raft = 节点级单 Raft，`CoordNode.raft`；multi_raft 模式
// 下它同时是 system raft——鉴权/会话/迁移标记/PD 队列等 `/_sys/*`、`/_pd/*`
// 系统数据的承载 raft）。

use std::sync::Arc;

use async_trait::async_trait;
use coord_core::error::{Error, Result};
use coord_core::types::NodeID;

use crate::raft::type_config::{Command, PdOp, PdQueueEntry, Response};
use crate::raft::CoordRaft;
use crate::storage::mvcc::MvccStorage;
use crate::storage::redb_backend::RedbBackend;

/// region 0（system raft）PD 治理能力面（D1-a P2）
///
/// 语义约定（与 `pd/` 模块对齐，见 §4.5 数据模型/命令集）：
/// - `current_leader`：调度收敛闸（operator 生成只发生在 region 0 leader
///   所在节点）；选举窗口 = None；
/// - `propose_pd`：把 `PdOp`（Enqueue/Claim/Complete/Requeue）写入 region 0
///   日志并等待 apply；返回日志 index（revision）——Enqueue 时即队列条目的
///   `op_id`。非 leader 调用返回 Err；
/// - `pd_queue`：读 region 0 全局队列快照（op_id 升序）。本节点 region 0
///   MVCC = raft apply 的本地视图，跨节点经日志复制收敛（认领用 apply CAS，
///   读视图滞后不产生双执行）。
#[async_trait]
pub trait SystemRaftHandle: Send + Sync {
    /// 当前 region 0 leader（选举窗口/未知 = None）
    async fn current_leader(&self) -> Option<NodeID>;

    /// 提出一条 PD 队列命令到 region 0 raft，返回日志 index（revision）。
    async fn propose_pd(&self, op: PdOp) -> Result<u64>;

    /// 读取 region 0 全局 PD 队列快照（op_id 升序）。
    fn pd_queue(&self) -> Result<Vec<PdQueueEntry>>;
}

/// 真实 region 0 raft 的 PD 治理句柄（D1-a P2）
///
/// 包装节点级单 Raft（`CoordRaft`）与其共享 MVCC，把 `client_write(Command::Pd)`
/// / `pd_queue_entries` 收敛到 `SystemRaftHandle` 端口；错误映射为
/// `coord_core::error::Error`。
pub struct CoordSystemRaftHandle {
    raft: CoordRaft,
    mvcc: Arc<MvccStorage<RedbBackend>>,
}

impl CoordSystemRaftHandle {
    /// 从 region 0 raft + 其 MVCC 构建（`CoordRaft` Clone 为 Arc bump，廉价）
    pub fn new(raft: CoordRaft, mvcc: Arc<MvccStorage<RedbBackend>>) -> Self {
        Self { raft, mvcc }
    }
}

#[async_trait]
impl SystemRaftHandle for CoordSystemRaftHandle {
    async fn current_leader(&self) -> Option<NodeID> {
        self.raft.current_leader().await
    }

    async fn propose_pd(&self, op: PdOp) -> Result<u64> {
        let cmd = Command::Pd(op);
        let resp = self
            .raft
            .client_write(cmd)
            .await
            .map_err(|e| Error::Internal(format!("region0 raft pd propose failed: {e}")))?;
        match resp.response() {
            Response::Put { revision } => Ok(*revision),
            other => Err(Error::Internal(format!(
                "unexpected region0 pd propose response: {other:?}"
            ))),
        }
    }

    fn pd_queue(&self) -> Result<Vec<PdQueueEntry>> {
        self.mvcc
            .pd_queue_entries()
            .map_err(|e| Error::Internal(format!("read region0 pd queue: {e}")))
    }
}
