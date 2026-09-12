// region 0 system raft 治理能力面
//
// PD operator
// 队列经 **region 0 system raft** 承载：全节点复制同一份全序队列，调度只发生在
// region 0 leader 节点，执行由「目标 Region 的当前 leader」节点认领（apply CAS
// 防双认领）。本文件定义 pd 模块依赖的 raft 端口：
//
//   - `SystemRaftHandle` trait：`current_leader` / `propose_pd` / `pd_queue`。
//     定义在 raft 层（raft 自我描述能力；`openraft::` 交互收敛在本目录内——
//     openraft 类型隔离），`pd` 模块的 `PlacementDriver`/`OperatorExecutor` 只经
//     trait object 使用本端口，不接触任何 openraft 类型（与 `RegionRaftHandle`
//     同模式，见 `raft/region_runtime.rs`）。
//
// 真实实现 `CoordSystemRaftHandle` 包装 region 0 `CoordRaft` 与其 MVCC
// （main.rs：region 0 raft = 节点级单 Raft，`CoordNode.raft`；multi_raft 模式
// 下它同时是 system raft——鉴权/会话/迁移标记/PD 队列等 `/_sys/*`、`/_pd/*`
// 系统数据的承载 raft）。

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use coord_core::error::{Error, Result};
use coord_core::types::NodeID;

use crate::raft::network::RaftNetworkFactoryImpl;
use crate::raft::type_config::{Command, PdOp, PdQueueEntry, Response};
use crate::raft::CoordRaft;
use crate::storage::mvcc::MvccStorage;
use crate::storage::redb_backend::RedbBackend;

/// region 0（system raft）PD 治理能力面
///
/// 语义约定（与 `pd/` 模块对齐）：
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

/// 真实 region 0 raft 的 PD 治理句柄
///
/// 包装节点级单 Raft（`CoordRaft`）与其共享 MVCC，把 `client_write(Command::Pd)`
/// / `pd_queue_entries` 收敛到 `SystemRaftHandle` 端口；错误映射为
/// `coord_core::error::Error`。
///
/// openraft `client_write`
/// 只有 raft **leader** 能本地提出，follower 返回 `ForwardToLeader`。PD 执行器在
/// 「目标 Region 的当前 leader」节点认领 operator，该节点**未必是 region 0
/// leader**——若不转发，operator 会永远 Pending（调度器持续生成、执行器无法
/// 认领）。`with_forwarder` 装配节点间 `SubmitPdOp` RPC 后，
/// `propose_pd` 在本地非 leader 时把命令转发到 region 0 leader 节点提出
/// （apply CAS 语义不变；幂等/去重仍由 apply 层保证）。
pub struct CoordSystemRaftHandle {
    raft: CoordRaft,
    mvcc: Arc<MvccStorage<RedbBackend>>,
    /// 本节点 ID（region 0 leader 判定）
    node_id: NodeID,
    /// P5：region 0 leader 转发客户端（经 raft 节点间 gRPC；None = 单节点/
    /// 测试装配——本地提出即可）
    forwarder: Option<RaftNetworkFactoryImpl>,
}

impl CoordSystemRaftHandle {
    /// 从 region 0 raft + 其 MVCC 构建（`CoordRaft` Clone 为 Arc bump，廉价）。
    /// 单节点/测试装配用（本地提出）；生产装配另经 `with_forwarder` 挂转发。
    pub fn new(raft: CoordRaft, mvcc: Arc<MvccStorage<RedbBackend>>) -> Self {
        Self {
            raft,
            mvcc,
            node_id: 0,
            forwarder: None,
        }
    }

    /// 装配 region 0 leader 转发（生产接线，main.rs：executor 在非 region 0
    /// leader 节点认领 operator 时经节点间 RPC 转发提出，见模块文档 P5）。
    pub fn with_forwarder(mut self, node_id: NodeID, factory: RaftNetworkFactoryImpl) -> Self {
        self.node_id = node_id;
        self.forwarder = Some(factory);
        self
    }
}

#[async_trait]
impl SystemRaftHandle for CoordSystemRaftHandle {
    async fn current_leader(&self) -> Option<NodeID> {
        self.raft.current_leader().await
    }

    async fn propose_pd(&self, op: PdOp) -> Result<u64> {
        // 本节点是 region 0 leader → 本地 client_write（历史路径）
        if self.raft.current_leader().await == Some(self.node_id) {
            return self.propose_local(op).await;
        }
        // 非 leader：跟随 openraft ForwardToLeader 语义把命令**转发到 region 0
        // leader 节点**经 SubmitPdOp RPC 提出（P5；apply CAS 语义不变）。
        let Some(factory) = self.forwarder.as_ref() else {
            // 无转发装配（单节点测试/纯本地装配）：退回本地提出（单节点恒
            // leader；非 leader 时错误上抛由调用方/调度器跳过处理）
            return self.propose_local(op).await;
        };
        let mut last_err = "region 0 leader unknown (election window)".to_string();
        for _attempt in 0..5 {
            // 每次重试前刷新 leader 视图（选举/转移后 leader 可能已变化）
            match self.raft.current_leader().await {
                Some(l) if l == self.node_id => {
                    return self.propose_local(op.clone()).await;
                }
                Some(l) => match factory.submit_pd_op(l, op.clone()).await {
                    Ok(idx) => return Ok(idx),
                    Err(e) => {
                        tracing::warn!("PD: forward pd op to region 0 leader node {l} failed: {e}");
                        last_err = e.to_string();
                    }
                },
                None => {
                    tracing::warn!("PD: region 0 leader unknown; pd propose forward deferred");
                }
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        Err(Error::Internal(format!(
            "region0 raft pd propose (leader forward) failed after retries: {last_err}"
        )))
    }

    fn pd_queue(&self) -> Result<Vec<PdQueueEntry>> {
        self.mvcc
            .pd_queue_entries()
            .map_err(|e| Error::Internal(format!("read region0 pd queue: {e}")))
    }
}

impl CoordSystemRaftHandle {
    /// 本地 client_write（本节点 = region 0 leader 时调用）。
    async fn propose_local(&self, op: PdOp) -> Result<u64> {
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
}
