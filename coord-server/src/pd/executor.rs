// PD Operator 执行器
//
// 把调度器产生的 Operator 映射为对**真实 Region Raft** 的成员变更：
//   - AddPeer       → `add_learner(node, raft_addr)`（blocking 追平复制）→
//                     `promote_to_voter`（晋升 Voter）
//   - RemovePeer    → `remove_voter`（change_membership RemoveVoters）
//   - TransferLeader→ `transfer_leader(to)` 并发起后**轮询等待目标真正成为
//                     leader**（openraft 的 transfer_leader 只发起、异步完成）
//   - SplitRegion / MergeRegion → v1 明确不支持（依赖写路径一致性，延后）
//
// 成功执行 AddPeer/RemovePeer 后同步 `PdMetaStore` 的 RegionMeta.peers
// （conf_ver 递增，写穿落盘）——调度器以 meta_store 为副本数/均衡决策的真源，
// 不同步会导致同一 operator 被反复生成。
//
// 设计约束：
// - pd 模块不直接依赖 raft/openraft 类型（类型隔离：`openraft::`/`openraft_multi::`
//   路径只允许出现在 `raft/` 模块内）。对 Region raft 的全部交互经
//   `RegionRaftHandle` trait 抽象（定义于 `raft/region_runtime.rs`，由 raft 层
//   自我描述能力），执行器只经 trait object 使用。
// - **Leader 守卫**：只有 Region leader 才能发起 change_membership /
//   transfer_leader（openraft 拒绝非 leader 调用）。
// - 幂等：AddPeer 目标已是 Voter / RemovePeer 目标不在成员表 → 视为已达成
//   成功（不重复触发 raft 成员变更，也不把重试当失败）。
//
// 队列来源（退役 legacy 本地队列路径后唯一模式）：
// - **全局队列模式**（driver attach region 0 system raft，生产接线）：operator
//   队列经 region 0 raft 承载（`/_pd/ops/*`）。执行器从全局队列只认领「目标
//   Region 的当前 leader == 本节点」的 Pending 条目（本节点 raft 视角），
//   `PdOp::Claim` 经 apply CAS 防双认领——每 operator 恰好被其 Region leader
//   节点执行一次，无 follower Failed(not leader) 噪音、无重复 operator。认领
//   后执行（`execute` 保留 Leader 守卫：认领后 leader 漂移 → Failed，自愈——
//   调度器后续重新生成）。未装配 system raft 的装配（纯元数据类测试）无可
//   认领条目，`execute_one` 返回 None。
//
// 接线增强：
// - AddPeer 的**成员真源 = raft 已提交成员**（`current_members`），不再依赖
//   meta.peers 里的 learner 记录：目标已是 raft voter → 幂等成功；已是 raft
//   learner（openraft remove_voter 会把被移除 voter 降为 learner）→ 跳过
//   add_learner 仅 promote（重加自愈路径）；否则 add_learner → promote。
// - 调度器生成的 AddPeer 常以 `node_id=0` 占位（目标由 PD 选择）。接线层注入
//   `AddPeerTargetResolver` 后，执行器在执行前把占位目标解析为具体节点；无
//   可用目标时把 operator 留/放回 Pending 稍后重试，不记 Failed（避免
//   ReplicaChecker 驱动的无谓 churn）。

use std::sync::Arc;
use std::time::Duration;

use coord_core::types::{NodeID, Peer, PeerRole, RegionId, RegionMeta};
use tokio::time::MissedTickBehavior;

use super::{now_unix_secs, op_summary, Operator, PlacementDriver};
use crate::raft::region_runtime::RegionRaftHandle;
use crate::raft::system_raft::SystemRaftHandle;
use crate::raft::type_config::{IntentPhase, PdIntent, PdOp};
use crate::storage::region_table::{plan_merge, plan_split, RegionTable, SplitMergePlan};

/// Region raft 解析器：region_id → 本节点上该 Region 的 raft 句柄
///
/// 由接线层提供（main.rs 把 RegionManager 的 runtime 映射为 handle；
/// 测试/本迭代集成测试直接构造）。
pub type RegionRaftResolver = dyn Fn(RegionId) -> Option<Arc<dyn RegionRaftHandle>> + Send + Sync;

/// AddPeer（node_id=0 占位）目标解析器：region_id → (目标 node_id, raft_addr)
///
/// 由接线层注入（EmbeddedPd：从已注册集群节点中选在线且非该 Region
/// voter 的节点）；返回 None = 当前无可用目标（执行器把 operator 放回队列
/// 稍后重试）。
pub type AddPeerTargetResolver = dyn Fn(RegionId) -> Option<(NodeID, String)> + Send + Sync;

/// Split/Merge 的数据搬迁钩子（#163/#164 提供真实实现）。
///
/// 契约：**幂等、可重入**。执行器在 cutover 前调用一次；进程重启后按 intent
/// 重新进入时会再次调用同一计划。未装配钩子时执行器拒绝切表（fail-closed），
/// 绝不在数据未搬迁时改写权威表。
#[async_trait::async_trait]
pub trait SplitMergeMigrator: Send + Sync {
    async fn migrate(&self, plan: &SplitMergePlan) -> std::result::Result<(), String>;
}

/// Operator 执行器：消费 `PlacementDriver` 队列中的 operator 并应用到
/// 本节点可执行的 Region raft。
pub struct OperatorExecutor {
    /// 所属 PD（持有 meta_store 与 operator 队列）
    pd: Arc<PlacementDriver>,
    /// 本节点 ID（Leader 守卫）
    node_id: NodeID,
    /// TransferLeader 发起后等待新 leader 真正上线的超时
    transfer_timeout: Duration,
    /// AddPeer 占位目标解析器（None = 不解析：node_id=0 的 AddPeer 直接失败）
    add_peer_resolver: Option<Arc<AddPeerTargetResolver>>,
    /// Split/Merge 数据搬迁钩子（None = 未装配，split/merge fail-closed）
    migrator: Option<Arc<dyn SplitMergeMigrator>>,
    /// cutover 提出后等待本节点 region 0 视图可见的时长
    cutover_timeout: Duration,
}

impl OperatorExecutor {
    /// 创建执行器（`node_id` = 本节点；仅当本节点是目标 Region leader 时
    /// operator 才会真正执行）
    pub fn new(pd: Arc<PlacementDriver>, node_id: NodeID) -> Self {
        Self {
            pd,
            node_id,
            transfer_timeout: Duration::from_secs(30),
            add_peer_resolver: None,
            migrator: None,
            cutover_timeout: Duration::from_secs(5),
        }
    }

    /// 装配 Split/Merge 数据搬迁钩子（未装配时 split/merge fail-closed）。
    pub fn with_migrator(mut self, migrator: Arc<dyn SplitMergeMigrator>) -> Self {
        self.migrator = Some(migrator);
        self
    }

    /// 配置 cutover 结果等待超时（测试用）
    pub fn with_cutover_timeout(mut self, timeout: Duration) -> Self {
        self.cutover_timeout = timeout;
        self
    }

    /// 配置 TransferLeader 等待超时（测试用）
    pub fn with_transfer_timeout(mut self, timeout: Duration) -> Self {
        self.transfer_timeout = timeout;
        self
    }

    /// 注入 AddPeer 占位目标解析器（接线层）
    pub fn with_add_peer_resolver(mut self, resolver: Arc<AddPeerTargetResolver>) -> Self {
        self.add_peer_resolver = Some(resolver);
        self
    }

    /// 从 region 0 全局队列认领并执行一拍（P4b 退役本地队列路径后唯一模式；
    /// 无 region 0 system raft 的装配无可认领条目，返回 None）。
    ///
    /// 全局队列模式（driver 已 attach region 0 system raft，生产接线）：从
    /// region 0 全局队列认领「目标 Region 的当前 leader == 本节点」的 Pending
    /// 条目，经 `PdOp::Claim`（apply CAS 防双认领）→ 执行 → `PdOp::Complete`。
    /// 结果写入队列（Success/Failed）；无 Pending operator（或本 tick 无可认领
    /// 条目）时返回 None。AddPeer（node_id=0）先经目标解析器解析为具体节点。
    pub async fn execute_one(&self, resolve: &RegionRaftResolver) -> Option<Operator> {
        let system = self.pd.system_raft()?;
        self.execute_one_raft(system.as_ref(), resolve).await
    }

    /// 全局队列模式执行一拍。
    ///
    /// 从 region 0 全局队列（本节点 MVCC 视图，op_id 升序）扫描 Pending 条目，
    /// 认领并执行**恰好一个**「目标 Region 的当前 leader == 本节点」的 operator：
    ///
    /// 1. 目标 Region 不在本节点 / 本节点非其 leader → 他节点认领，跳过；
    /// 2. AddPeer 占位（node_id=0）目标暂不可解析 → 保持 Pending，跳过
    ///    （不产生 Requeue 日志噪音；目标可用后本节点或他节点自然认领）；
    /// 3. `PdOp::Claim`（apply CAS 防双认领）→ propose 返回后重读队列确认
    ///    `Running(claimed_by=本节点)`；竞态失败（他节点已认领）→ 跳过；
    /// 4. 执行（复用 `execute` 的 Leader 守卫——认领后 leader 若已漂移则
    ///    Failed(not leader)，自愈）；
    /// 5. `PdOp::Complete{success, error}` + 审计（success/failed 同口径）。
    ///
    /// 返回已处理 operator（可观测性）；无可认领条目返回 None。
    async fn execute_one_raft(
        &self,
        system: &dyn SystemRaftHandle,
        resolve: &RegionRaftResolver,
    ) -> Option<Operator> {
        // 1. 读全局队列（本节点 region 0 MVCC = raft apply 后本地视图）
        let queue = match system.pd_queue() {
            Ok(q) => q,
            Err(e) => {
                tracing::warn!("PD executor: read global pd queue failed: {e}");
                return None;
            }
        };

        // 2. 扫描 Pending 条目
        for entry in &queue {
            if !entry.is_pending() {
                continue;
            }
            let original = entry.op.clone();
            let op = match &original {
                Operator::AddPeer {
                    region_id,
                    node_id: 0,
                    ..
                } => match self.add_peer_resolver.as_ref().and_then(|r| r(*region_id)) {
                    Some((target, addr)) => Operator::AddPeer {
                        region_id: *region_id,
                        node_id: target,
                        raft_addr: addr,
                    },
                    None => {
                        tracing::debug!(
                            "PD executor: add-peer region {region_id} placeholder \
                             unresolvable; leave pending (global queue)"
                        );
                        continue;
                    }
                },
                _ => original.clone(),
            };

            // 仅认领「本节点是该 Region 的当前 leader」的条目
            let Some(raft) = resolve(op.region_id()) else {
                continue; // 目标 Region 不在本节点 → 他节点执行
            };
            if raft.current_leader().await != Some(self.node_id) {
                continue; // 本节点非该 Region leader → 他节点认领
            }

            // 3. Claim（apply CAS；client_write 等待 apply 后才返回）。
            //    P3：认领墙钟（claimed_at_unix）由认领者 propose 前填——Running
            //    超时重认领（requeue）判定依赖它，apply 期不读墙钟。
            let op_id = entry.op_id;
            if let Err(e) = system
                .propose_pd(PdOp::Claim {
                    op_id,
                    node_id: self.node_id,
                    claimed_at_unix: now_unix_secs(),
                })
                .await
            {
                tracing::warn!("PD executor: claim op {op_id} via region 0 failed: {e}");
                continue;
            }
            // 确认认领归属：并发的他节点认领若先 apply，本认领为 no-op
            match system.pd_queue() {
                Ok(after) => match after.into_iter().find(|e| e.op_id == op_id) {
                    Some(e) if e.is_running() && e.claimed_by == self.node_id => {}
                    _ => {
                        tracing::debug!(
                            "PD executor: lost claim race on op {op_id}; skip (claimed elsewhere)"
                        );
                        continue;
                    }
                },
                Err(e) => {
                    tracing::warn!("PD executor: read global queue after claim failed: {e}");
                    continue;
                }
            }
            // 认领成功审计（与 enqueue(pending)/complete(success|failed)
            // 同口径，拼出 enqueue→claim→complete 全生命周期追踪）
            self.pd
                .record_operator_event(&original, "claimed", &op_summary(&original));

            // 4. 执行 + 5. Complete
            let outcome = match &op {
                Operator::SplitRegion { .. } | Operator::MergeRegion { .. } => {
                    self.execute_split_merge(op_id, &op).await
                }
                _ => match resolve(op.region_id()) {
                    Some(raft) => self.execute(&op, raft).await,
                    None => Err(format!(
                        "region {} runtime not present on node {}",
                        op.region_id(),
                        self.node_id
                    )),
                },
            };
            let (success, msg) = match outcome {
                Ok(()) => (true, String::new()),
                Err(m) => (false, m),
            };
            if let Err(e) = system
                .propose_pd(PdOp::Complete {
                    op_id,
                    node_id: self.node_id,
                    success,
                    error: msg.clone(),
                })
                .await
            {
                tracing::warn!("PD executor: complete op {op_id} via region 0 failed: {e}");
            }
            // 审计（与本地 complete_operator 同口径；记录在原始占位条目语义上）
            if success {
                self.pd
                    .record_operator_event(&original, "success", &op_summary(&original));
            } else {
                self.pd.record_operator_event(
                    &original,
                    "failed",
                    &format!("{}; error: {}", op_summary(&original), msg),
                );
            }
            return Some(op);
        }
        None
    }

    /// 后台执行循环（与调度循环并行）
    ///
    /// 每个 interval 至多执行队列中一个 operator；收到 PD 关闭信号后优雅退出。
    pub fn start_executor_loop(
        self: &Arc<Self>,
        resolve: Arc<RegionRaftResolver>,
        interval: Duration,
    ) -> tokio::task::JoinHandle<()> {
        let ex = Arc::clone(self);
        let mut shutdown_rx = ex.pd.shutdown_rx.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Some(op) = ex.execute_one(&*resolve).await {
                            tracing::debug!("PD executor: processed {:?}", op.name());
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        tracing::info!("PD executor loop: shutdown signal received");
                        break;
                    }
                }
            }
        })
    }

    /// 执行单个 Operator（不经队列；由调用方/execute_one 持有结果）
    ///
    /// 返回 `Err(msg)` 时调用方负责把 operator 标记为 Failed（含理由）。
    pub async fn execute(
        &self,
        op: &Operator,
        raft: Arc<dyn RegionRaftHandle>,
    ) -> std::result::Result<(), String> {
        // Leader 守卫：只有 Region leader 能发起成员变更/转移。不是 leader /
        // 选举窗口（leader 未知）都不可执行——真实部署下该 operator 由
        // Region leader 所在节点的 PD 执行。
        match raft.current_leader().await {
            Some(l) if l == self.node_id => {}
            Some(l) => {
                return Err(format!(
                    "node {} is not leader of region {} (leader={}); operator not executable here",
                    self.node_id,
                    op.region_id(),
                    l
                ));
            }
            None => {
                return Err(format!(
                    "region {} has no leader (election window); operator not executable",
                    op.region_id()
                ));
            }
        }

        match op {
            Operator::AddPeer {
                region_id,
                node_id,
                raft_addr,
            } => {
                self.exec_add_peer(*region_id, *node_id, raft_addr, raft.as_ref())
                    .await
            }
            Operator::RemovePeer { region_id, node_id } => {
                self.exec_remove_peer(*region_id, *node_id, raft.as_ref())
                    .await
            }
            Operator::TransferLeader { region_id, to_node } => {
                self.exec_transfer_leader(*region_id, *to_node, raft.as_ref())
                    .await
            }
            Operator::SplitRegion { .. } | Operator::MergeRegion { .. } => Err(format!(
                "{} requires a claimed queue operator (journal op id)",
                op.name()
            )),
        }
    }

    // ──── Split / Merge（#162）────

    /// 执行一个已认领的 split/merge operator。
    ///
    /// 顺序：读权威表 → （无 intent 时）计划 + 落 intent → 数据搬迁钩子 →
    /// `PdOp::Cutover`（region 0 同事务原子改表 + 推进 intent 阶段）→ 等本节点
    /// 视图看到结果 → 对账 `PdMetaStore` → 清 intent。
    ///
    /// 任何一步失败都返回 `Err`（调用方 `Complete{success=false}`）；表只在
    /// cutover 里前进，失败路径不会留下半套拓扑。重启恢复复用同一入口：
    /// intent 已在则沿用原计划，不重算边界。
    async fn execute_split_merge(
        &self,
        op_id: u64,
        op: &Operator,
    ) -> std::result::Result<(), String> {
        let system = self.pd.system_raft().ok_or_else(|| {
            "split/merge requires the region 0 system raft (v1 static topology)".to_string()
        })?;
        let table = system
            .region_table()
            .map_err(|e| format!("read authoritative region table: {e}"))?;
        if table.version == 0 {
            return Err(
                "region table is not initialized; split/merge is disabled (v1 static topology)"
                    .to_string(),
            );
        }

        let mut intent = match system
            .pd_intent(op_id)
            .map_err(|e| format!("read split/merge intent {op_id}: {e}"))?
        {
            Some(intent) => intent,
            None => {
                // 先确认搬迁钩子已装配，再产生任何副作用。
                if self.migrator.is_none() {
                    return Err(
                        "split/merge data migration not wired (#163); refusing to cut over"
                            .to_string(),
                    );
                }
                let plan = match op {
                    Operator::SplitRegion {
                        region_id,
                        split_key,
                        new_region_id,
                    } => plan_split(&table, *region_id, split_key, *new_region_id),
                    Operator::MergeRegion { left, right } => plan_merge(&table, *left, *right),
                    other => return Err(format!("{} is not a split/merge operator", other.name())),
                }
                .map_err(|e| format!("split/merge plan rejected: {e}"))?;
                let intent = PdIntent {
                    op_id,
                    owner: self.node_id,
                    plan,
                    phase: IntentPhase::Prepared,
                };
                system
                    .propose_pd(PdOp::RecordIntent {
                        intent: intent.clone(),
                    })
                    .await
                    .map_err(|e| format!("record split/merge intent {op_id}: {e}"))?;
                intent
            }
        };

        loop {
            match intent.phase.clone() {
                IntentPhase::Applied { .. } => {
                    self.finalize_split_merge(system.as_ref(), op_id).await?;
                    return Ok(());
                }
                IntentPhase::Rejected { reason } => {
                    if let Err(e) = system.propose_pd(PdOp::ClearIntent { op_id }).await {
                        tracing::warn!("PD split/merge: clear rejected intent {op_id}: {e}");
                    }
                    return Err(format!("split/merge cutover rejected: {reason}"));
                }
                IntentPhase::Prepared => {
                    let migrator = self.migrator.as_ref().ok_or_else(|| {
                        "split/merge data migration not wired (#163); refusing to cut over"
                            .to_string()
                    })?;
                    migrator.migrate(&intent.plan).await?;
                    system
                        .propose_pd(PdOp::Cutover { op_id })
                        .await
                        .map_err(|e| format!("propose split/merge cutover {op_id}: {e}"))?;
                    intent = self.wait_cutover(system.as_ref(), op_id).await?;
                }
            }
        }
    }

    /// 轮询本节点 region 0 视图，直到 intent 离开 `Prepared`。
    ///
    /// `propose_pd` 在 region 0 leader 上 apply 后才返回；本节点是 follower 时
    /// 本地视图可能滞后。超时返回错误但**保留 intent**：下一次恢复/GC 会按
    /// 已落地的阶段继续，不会重复切表。
    async fn wait_cutover(
        &self,
        system: &dyn SystemRaftHandle,
        op_id: u64,
    ) -> std::result::Result<PdIntent, String> {
        let deadline = tokio::time::Instant::now() + self.cutover_timeout;
        loop {
            match system.pd_intent(op_id) {
                Ok(Some(intent)) if intent.phase != IntentPhase::Prepared => return Ok(intent),
                Ok(_) => {}
                Err(e) => return Err(format!("read split/merge intent {op_id}: {e}")),
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!(
                    "split/merge cutover {op_id} outcome not visible on this node; intent retained"
                ));
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// cutover 已生效后的收尾：单向对账 `PdMetaStore` + 清理 intent。
    async fn finalize_split_merge(
        &self,
        system: &dyn SystemRaftHandle,
        op_id: u64,
    ) -> std::result::Result<(), String> {
        let table = system
            .region_table()
            .map_err(|e| format!("read authoritative region table: {e}"))?;
        self.reconcile_meta_store(&table)?;
        system
            .propose_pd(PdOp::ClearIntent { op_id })
            .await
            .map_err(|e| format!("clear split/merge intent {op_id}: {e}"))?;
        Ok(())
    }

    /// 把权威 Region 表单向同步到 `PdMetaStore`（调度器的读视图）。
    ///
    /// 表是唯一真源：表里没有的 Region 从 meta store 删除，字段不同的覆盖。
    /// 空表（version 0）= v1 静态拓扑，no-op。
    fn reconcile_meta_store(&self, table: &RegionTable) -> std::result::Result<(), String> {
        if table.version == 0 {
            return Ok(());
        }
        let store = self.pd.meta_store();
        for meta in &table.regions {
            match store.get_region(meta.region_id) {
                Some(existing) if existing == *meta => {}
                Some(_) => store
                    .update_region(meta.clone())
                    .map_err(|e| format!("pd meta store update region {}: {e}", meta.region_id))?,
                None => store
                    .create_region(meta.clone())
                    .map_err(|e| format!("pd meta store create region {}: {e}", meta.region_id))?,
            }
        }
        for existing in store.list_regions() {
            if table.get(existing.region_id).is_none() {
                match store.delete_region(existing.region_id) {
                    Ok(()) => {}
                    Err(coord_core::error::Error::RegionNotFound { .. }) => {}
                    Err(e) => {
                        return Err(format!(
                            "pd meta store delete region {}: {e}",
                            existing.region_id
                        ))
                    }
                }
            }
        }
        Ok(())
    }

    // ──── 各 operator 的具体执行 ────

    async fn exec_add_peer(
        &self,
        region_id: RegionId,
        peer_id: NodeID,
        raft_addr: &str,
        raft: &dyn RegionRaftHandle,
    ) -> std::result::Result<(), String> {
        if peer_id == 0 {
            return Err(
                "add-peer: target node not resolved (node_id=0; scheduler deferred node pick)"
                    .to_string(),
            );
        }
        if raft_addr.is_empty() {
            return Err(format!(
                "add-peer: raft_addr empty for node {peer_id} (region {region_id})"
            ));
        }

        // 成员真源 = raft 已提交成员（current_members），不依赖
        // meta.peers 里的 learner 记录（remove_voter 后 meta 会移除该节点，
        // 但 raft 中它仍以 learner 存在——重加应走 promote-only 路径）。
        let raft_members = raft
            .current_members()
            .await
            .map_err(|e| format!("add-peer: read region {region_id} membership: {e}"))?;
        let existing_raft = raft_members.iter().find(|p| p.node_id == peer_id).cloned();
        match existing_raft {
            // 已是 raft Voter：幂等成功（不重复触发 raft 成员变更）
            Some(p) if p.role == PeerRole::Voter => return Ok(()),
            // 已是 raft Learner：跳过 add_learner，仅晋升；地址不一致则拒绝
            // （状态漂移——learner 记录中地址未知时无法比较，放行）
            Some(p)
                if p.role == PeerRole::Learner
                    && !p.raft_addr.is_empty()
                    && p.raft_addr != raft_addr =>
            {
                return Err(format!(
                    "add-peer: node {peer_id} already learner at {} but requested {raft_addr}",
                    p.raft_addr
                ));
            }
            _ => {}
        }

        let is_raft_learner = matches!(
            existing_raft.as_ref().map(|p| p.role),
            Some(PeerRole::Learner)
        );
        if !is_raft_learner {
            raft.add_learner(peer_id, raft_addr)
                .await
                .map_err(|e| format!("add_learner node {peer_id} (region {region_id}): {e}"))?;
        }
        raft.promote_to_voter(peer_id)
            .await
            .map_err(|e| format!("promote node {peer_id} to voter (region {region_id}): {e}"))?;

        // 同步 PD 元数据（调度真源）：写穿落盘 + conf_ver 递增
        let mut meta = self.region_meta_of(region_id)?;
        meta.peers.retain(|p| p.node_id != peer_id);
        meta.peers.push(Peer {
            node_id: peer_id,
            raft_addr: raft_addr.to_string(),
            role: PeerRole::Voter,
        });
        meta.epoch.conf_ver += 1;
        self.pd
            .meta_store()
            .update_region(meta)
            .map_err(|e| format!("add-peer: persist region {region_id} meta: {e}"))?;

        tracing::info!("PD: add-peer region {region_id} node {peer_id} -> voter (conf_ver bumped)");
        Ok(())
    }

    async fn exec_remove_peer(
        &self,
        region_id: RegionId,
        peer_id: NodeID,
        raft: &dyn RegionRaftHandle,
    ) -> std::result::Result<(), String> {
        let mut meta = self.region_meta_of(region_id)?;
        let peer = meta.peers.iter().find(|p| p.node_id == peer_id).cloned();
        match peer {
            // 不在成员表：幂等成功
            None => return Ok(()),
            // 只处理 Voter（移除 Learner 不是 RemovePeer 的语义）
            Some(p) if p.role == PeerRole::Learner => {
                return Err(format!(
                    "remove-peer: node {peer_id} is a learner of region {region_id}, not a voter"
                ));
            }
            _ => {}
        }

        raft.remove_voter(peer_id)
            .await
            .map_err(|e| format!("remove voter {peer_id} (region {region_id}): {e}"))?;

        meta.peers.retain(|p| p.node_id != peer_id);
        meta.epoch.conf_ver += 1;
        self.pd
            .meta_store()
            .update_region(meta)
            .map_err(|e| format!("remove-peer: persist region {region_id} meta: {e}"))?;

        tracing::info!("PD: remove-peer region {region_id} node {peer_id} (conf_ver bumped)");
        Ok(())
    }

    async fn exec_transfer_leader(
        &self,
        region_id: RegionId,
        to_node: NodeID,
        raft: &dyn RegionRaftHandle,
    ) -> std::result::Result<(), String> {
        let meta = self.region_meta_of(region_id)?;
        let is_voter = meta
            .peers
            .iter()
            .any(|p| p.node_id == to_node && p.role == PeerRole::Voter);
        if !is_voter {
            return Err(format!(
                "transfer-leader: node {to_node} is not a voter of region {region_id}"
            ));
        }

        if raft.current_leader().await == Some(to_node) {
            return Ok(());
        }

        raft.transfer_leader(to_node)
            .await
            .map_err(|e| format!("transfer leader to {to_node} (region {region_id}): {e}"))?;

        // openraft transfer_leader 只发起转移：轮询等待目标真正成为 leader
        let deadline = tokio::time::Instant::now() + self.transfer_timeout;
        while tokio::time::Instant::now() < deadline {
            if raft.current_leader().await == Some(to_node) {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(format!(
            "transfer-leader: region {region_id} leader did not move to {to_node} within {}s",
            self.transfer_timeout.as_secs()
        ))
    }

    // ──── 元数据辅助 ────

    fn region_meta_of(&self, region_id: RegionId) -> std::result::Result<RegionMeta, String> {
        self.pd
            .meta_store()
            .get_region(region_id)
            .ok_or_else(|| format!("region {region_id} not found in PD meta store"))
    }
}

// ──── 单元测试（Fake raft 替身；真实 raft 见 tests/pd_operator_executor_test.rs）────

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use coord_core::types::{RegionEpoch, RegionMeta};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::sync::watch;

    use crate::pd::operator::OperatorStatus;
    use crate::raft::system_raft::SystemRaftHandle;
    use crate::raft::type_config::{IntentPhase, PdIntent, PdOp, PdQueueEntry};

    /// 可控 raft 替身：记录成员变更调用、可切换 leader 视图、维护成员表
    struct FakeRaft {
        /// 当前 leader（executor 节点视角）
        leader: Mutex<Option<NodeID>>,
        /// raft 已提交成员表（current_members 真源；add/promote/remove 更新）
        members: Mutex<Vec<Peer>>,
        add_learner_calls: Mutex<Vec<(NodeID, String)>>,
        promote_calls: Mutex<Vec<NodeID>>,
        remove_calls: Mutex<Vec<NodeID>>,
        transfer_calls: Mutex<Vec<NodeID>>,
        /// transfer_leader 被调用后自动切换 leader（模拟转移成功）
        auto_switch: AtomicBool,
    }

    impl FakeRaft {
        fn new(leader: NodeID, members: Vec<Peer>) -> Self {
            Self {
                leader: Mutex::new(Some(leader)),
                members: Mutex::new(members),
                add_learner_calls: Mutex::new(Vec::new()),
                promote_calls: Mutex::new(Vec::new()),
                remove_calls: Mutex::new(Vec::new()),
                transfer_calls: Mutex::new(Vec::new()),
                auto_switch: AtomicBool::new(true),
            }
        }

        fn set_auto_switch(&self, on: bool) {
            self.auto_switch.store(on, Ordering::SeqCst);
        }

        fn add_learner_count(&self) -> usize {
            self.add_learner_calls.lock().unwrap().len()
        }

        fn promote_calls(&self) -> Vec<NodeID> {
            self.promote_calls.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl RegionRaftHandle for FakeRaft {
        async fn current_leader(&self) -> Option<NodeID> {
            self.leader.lock().unwrap().clone()
        }

        async fn current_members(&self) -> coord_core::error::Result<Vec<Peer>> {
            Ok(self.members.lock().unwrap().clone())
        }

        async fn add_learner(
            &self,
            node_id: NodeID,
            raft_addr: &str,
        ) -> coord_core::error::Result<()> {
            self.add_learner_calls
                .lock()
                .unwrap()
                .push((node_id, raft_addr.to_string()));
            // 已存在则仅更新地址；否则以 learner 加入成员表
            let mut members = self.members.lock().unwrap();
            match members.iter_mut().find(|p| p.node_id == node_id) {
                Some(p) => {
                    p.raft_addr = raft_addr.to_string();
                    p.role = PeerRole::Learner;
                }
                None => members.push(Peer {
                    node_id,
                    raft_addr: raft_addr.to_string(),
                    role: PeerRole::Learner,
                }),
            }
            Ok(())
        }

        async fn promote_to_voter(&self, node_id: NodeID) -> coord_core::error::Result<()> {
            self.promote_calls.lock().unwrap().push(node_id);
            let mut members = self.members.lock().unwrap();
            if let Some(p) = members.iter_mut().find(|p| p.node_id == node_id) {
                p.role = PeerRole::Voter;
            }
            Ok(())
        }

        async fn remove_voter(&self, node_id: NodeID) -> coord_core::error::Result<()> {
            self.remove_calls.lock().unwrap().push(node_id);
            // openraft 语义：被移除 voter 降级为 learner（仍收复制不投票）
            let mut members = self.members.lock().unwrap();
            if let Some(p) = members.iter_mut().find(|p| p.node_id == node_id) {
                p.role = PeerRole::Learner;
            }
            Ok(())
        }

        async fn transfer_leader(&self, to: NodeID) -> coord_core::error::Result<()> {
            self.transfer_calls.lock().unwrap().push(to);
            if self.auto_switch.load(Ordering::SeqCst) {
                *self.leader.lock().unwrap() = Some(to);
            }
            Ok(())
        }
    }

    // ──── 测试基建 ────

    /// 构造 executor：node 1 为 Region leader 的测试 PD + 单 Region
    fn make_executor_pd(
        node_id: NodeID,
        peers: Vec<Peer>,
    ) -> (Arc<PlacementDriver>, OperatorExecutor, watch::Sender<bool>) {
        let config = super::super::types::PdConfig::default();
        let meta_store = Arc::new(super::super::meta_store::PdMetaStore::new());
        let region = RegionMeta {
            region_id: 1,
            start_key: vec![],
            end_key: vec![],
            epoch: RegionEpoch::initial(),
            peers,
            approximate_size: 0,
            approximate_keys: 0,
        };
        meta_store.create_region(region).unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let pd = Arc::new(PlacementDriver::new(
            config,
            meta_store,
            shutdown_rx,
            node_id,
        ));
        let ex = OperatorExecutor::new(Arc::clone(&pd), node_id);
        (pd, ex, shutdown_tx)
    }

    fn peer_of(node_id: NodeID, role: PeerRole) -> Peer {
        Peer {
            node_id,
            raft_addr: format!("node{node_id}:50052"),
            role,
        }
    }

    fn voter(id: NodeID) -> Peer {
        peer_of(id, PeerRole::Voter)
    }

    /// Peer 无 PartialEq：按 (node_id, role) 判定
    fn has_peer(peers: &[Peer], node_id: NodeID, role: PeerRole) -> bool {
        peers.iter().any(|p| p.node_id == node_id && p.role == role)
    }

    /// 返回 (pd, executor[node1], fake raft leader=node1，成员表=meta peers)
    fn leader_fake(peers: Vec<Peer>) -> (Arc<PlacementDriver>, OperatorExecutor, Arc<FakeRaft>) {
        let (pd, ex, _tx) = make_executor_pd(1, peers);
        let members = pd.meta_store().get_region(1).unwrap().peers.clone();
        let fake = Arc::new(FakeRaft::new(1, members));
        (pd, ex, fake)
    }

    fn peers_of_meta(pd: &PlacementDriver) -> Vec<Peer> {
        pd.meta_store().get_region(1).unwrap().peers
    }

    fn conf_ver(pd: &PlacementDriver) -> u64 {
        pd.meta_store().get_region(1).unwrap().epoch.conf_ver
    }

    // ──── AddPeer ────

    #[tokio::test]
    async fn test_add_peer_learn_promote_and_update_meta() {
        let (pd, ex, fake) = leader_fake(vec![voter(1)]);
        let op = Operator::AddPeer {
            region_id: 1,
            node_id: 2,
            raft_addr: "node2:50052".into(),
        };
        let raft: Arc<dyn RegionRaftHandle> = fake.clone();
        ex.execute(&op, raft).await.expect("add-peer succeeds");

        // raft 侧：先 add_learner 再 promote
        assert_eq!(fake.add_learner_count(), 1);
        assert_eq!(fake.promote_calls(), vec![2]);
        // PD 元数据：peers 含新 voter、conf_ver 递增
        let peers = peers_of_meta(&pd);
        assert!(has_peer(&peers, 2, PeerRole::Voter), "peers: {peers:?}");
        assert_eq!(peers.len(), 2);
        assert_eq!(conf_ver(&pd), 2);
    }

    #[tokio::test]
    async fn test_add_peer_already_voter_is_idempotent_noop() {
        let (pd, ex, fake) = leader_fake(vec![voter(1), voter(2)]);
        let op = Operator::AddPeer {
            region_id: 1,
            node_id: 2,
            raft_addr: "node2:50052".into(),
        };
        let raft: Arc<dyn RegionRaftHandle> = fake.clone();
        ex.execute(&op, raft).await.expect("no-op success");

        assert_eq!(
            fake.add_learner_count(),
            0,
            "no raft change for existing voter"
        );
        assert!(fake.promote_calls().is_empty());
        assert_eq!(conf_ver(&pd), 1, "meta unchanged");
        assert_eq!(peers_of_meta(&pd).len(), 2);
    }

    #[tokio::test]
    async fn test_add_peer_existing_learner_skips_add_learner() {
        let (pd, ex, fake) = leader_fake(vec![voter(1), peer_of(2, PeerRole::Learner)]);
        let op = Operator::AddPeer {
            region_id: 1,
            node_id: 2,
            raft_addr: "node2:50052".into(),
        };
        let raft: Arc<dyn RegionRaftHandle> = fake.clone();
        ex.execute(&op, raft).await.expect("promote learner");

        assert_eq!(fake.add_learner_count(), 0, "learner already added");
        assert_eq!(fake.promote_calls(), vec![2]);
        assert_eq!(conf_ver(&pd), 2);
        assert!(has_peer(&peers_of_meta(&pd), 2, PeerRole::Voter));
    }

    #[tokio::test]
    async fn test_add_peer_unresolved_node_fails() {
        let (_pd, ex, fake) = leader_fake(vec![voter(1)]);
        let op = Operator::AddPeer {
            region_id: 1,
            node_id: 0,
            raft_addr: String::new(),
        };
        let raft: Arc<dyn RegionRaftHandle> = fake.clone();
        let err = ex.execute(&op, raft).await.expect_err("node_id=0 rejected");
        assert!(err.contains("not resolved"), "err: {err}");
        assert_eq!(fake.add_learner_count(), 0);
    }

    // ──── RemovePeer ────

    #[tokio::test]
    async fn test_remove_peer_removes_voter_and_updates_meta() {
        let (pd, ex, fake) = leader_fake(vec![voter(1), voter(2), voter(3)]);
        let op = Operator::RemovePeer {
            region_id: 1,
            node_id: 2,
        };
        let raft: Arc<dyn RegionRaftHandle> = fake.clone();
        ex.execute(&op, raft).await.expect("remove-peer succeeds");

        assert_eq!(*fake.remove_calls.lock().unwrap(), vec![2]);
        let peers = peers_of_meta(&pd);
        assert!(!peers.iter().any(|p| p.node_id == 2), "peers: {peers:?}");
        assert_eq!(peers.len(), 2);
        assert_eq!(conf_ver(&pd), 2);
    }

    #[tokio::test]
    async fn test_remove_peer_absent_is_idempotent_noop() {
        let (_pd, ex, fake) = leader_fake(vec![voter(1)]);
        let op = Operator::RemovePeer {
            region_id: 1,
            node_id: 99,
        };
        let raft: Arc<dyn RegionRaftHandle> = fake.clone();
        ex.execute(&op, raft).await.expect("no-op success");
        assert_eq!(fake.remove_calls.lock().unwrap().len(), 0);
    }

    // ──── TransferLeader ────

    #[tokio::test]
    async fn test_transfer_leader_waits_for_switch() {
        let (_pd, ex, fake) = leader_fake(vec![voter(1), voter(2)]);
        let op = Operator::TransferLeader {
            region_id: 1,
            to_node: 2,
        };
        let raft: Arc<dyn RegionRaftHandle> = fake.clone();
        ex.execute(&op, raft).await.expect("transfer succeeds");
        assert_eq!(*fake.transfer_calls.lock().unwrap(), vec![2]);
    }

    #[tokio::test]
    async fn test_transfer_leader_already_target_is_noop() {
        let (_pd, ex, _fake) = make_executor_pd(2, vec![voter(1), voter(2)]);
        let fake = Arc::new(FakeRaft::new(2, vec![voter(1), voter(2)])); // leader 已是 node2（executor 节点 2）
        let op = Operator::TransferLeader {
            region_id: 1,
            to_node: 2,
        };
        let raft: Arc<dyn RegionRaftHandle> = fake.clone();
        ex.execute(&op, raft).await.expect("already leader");
        assert_eq!(fake.transfer_calls.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_transfer_leader_timeout_if_no_switch() {
        let (_pd, ex, fake) = leader_fake(vec![voter(1), voter(2)]);
        fake.set_auto_switch(false);
        let ex = ex.with_transfer_timeout(Duration::from_millis(350));
        let op = Operator::TransferLeader {
            region_id: 1,
            to_node: 2,
        };
        let raft: Arc<dyn RegionRaftHandle> = fake.clone();
        let err = ex.execute(&op, raft).await.expect_err("timeout");
        assert!(err.contains("did not move to 2"), "err: {err}");
    }

    #[tokio::test]
    async fn test_transfer_leader_to_non_voter_fails() {
        let (_pd, ex, fake) = leader_fake(vec![voter(1), voter(2)]);
        let op = Operator::TransferLeader {
            region_id: 1,
            to_node: 3, // 不是 voter
        };
        let raft: Arc<dyn RegionRaftHandle> = fake.clone();
        let err = ex.execute(&op, raft).await.expect_err("non-voter rejected");
        assert!(err.contains("not a voter"), "err: {err}");
        assert_eq!(fake.transfer_calls.lock().unwrap().len(), 0);
    }

    // ──── 守卫 ────

    #[tokio::test]
    async fn test_execute_on_non_leader_fails() {
        // executor 节点 1，但 region leader 已是 node2 → 守卫拒绝执行
        let (_pd, ex, _fake) = make_executor_pd(1, vec![voter(1), voter(2)]);
        let fake = Arc::new(FakeRaft::new(2, vec![voter(1), voter(2)]));
        let op = Operator::AddPeer {
            region_id: 1,
            node_id: 3,
            raft_addr: "node3:50052".into(),
        };
        let raft: Arc<dyn RegionRaftHandle> = fake.clone();
        let err = ex
            .execute(&op, raft)
            .await
            .expect_err("not leader rejected");
        assert!(err.contains("not leader"), "err: {err}");
    }

    #[tokio::test]
    async fn test_direct_execute_rejects_split_without_queue_op_id() {
        let (_pd, ex, fake) = leader_fake(vec![voter(1)]);
        let op = Operator::SplitRegion {
            region_id: 1,
            split_key: b"m".to_vec(),
            new_region_id: 2,
        };
        let raft: Arc<dyn RegionRaftHandle> = fake.clone();
        let err = ex.execute(&op, raft).await.expect_err("split needs op id");
        assert!(
            err.contains("requires a claimed queue operator"),
            "err: {err}"
        );
    }

    // ──── 队列驱动（execute_one / 执行循环）────

    #[tokio::test]
    async fn test_execute_one_pulls_queue_and_completes_success() {
        let (pd, ex, fake) = leader_fake(vec![voter(1)]);
        // P4b：条目经全局队列（region 0 raft 承载）；认领→执行→完成
        let system = Arc::new(FakeSystemRaft::new(Some(1)));
        system.seed_pending(
            5,
            Operator::AddPeer {
                region_id: 1,
                node_id: 2,
                raft_addr: "node2:50052".into(),
            },
            1,
        );
        pd.attach_system_raft(system.clone());

        let fake2: Arc<dyn RegionRaftHandle> = fake.clone();
        let resolve: Arc<RegionRaftResolver> = Arc::new(move |_rid| Some(fake2.clone()));
        let op = ex.execute_one(&*resolve).await.expect("one operator");
        assert_eq!(op.name(), "add-peer");
        let e = system.queue_entry(5).expect("entry present");
        assert!(
            matches!(e.status, super::super::operator::OperatorStatus::Success),
            "queue entry must be Success: {e:?}"
        );
        // meta 已同步
        assert!(has_peer(&peers_of_meta(&pd), 2, PeerRole::Voter));
    }

    #[tokio::test]
    async fn test_execute_one_completes_failed_when_region_missing() {
        let (pd, ex, _fake) = make_executor_pd(1, vec![voter(1)]);
        let system = Arc::new(FakeSystemRaft::new(Some(1)));
        system.seed_pending(
            6,
            Operator::TransferLeader {
                region_id: 99, // meta_store 无此 region
                to_node: 2,
            },
            1,
        );
        pd.attach_system_raft(system.clone());

        let resolve: Arc<RegionRaftResolver> = Arc::new(|_rid| {
            Some(Arc::new(FakeRaft::new(1, vec![voter(1)])) as Arc<dyn RegionRaftHandle>)
        });
        let op = ex.execute_one(&*resolve).await.expect("one operator");
        assert_eq!(op.name(), "transfer-leader");
        let e = system.queue_entry(6).expect("entry present");
        assert!(
            matches!(e.status, super::super::operator::OperatorStatus::Failed(_)),
            "queue entry must be Failed: {e:?}"
        );
    }

    #[tokio::test]
    async fn test_execute_one_resolves_add_peer_placeholder_target() {
        // 调度器以 node_id=0 占位的 AddPeer 经目标解析器落地为具体节点
        let (pd, ex, fake) = leader_fake(vec![voter(1)]);
        let target: Arc<AddPeerTargetResolver> =
            Arc::new(|_rid| Some((2, "node2:50052".to_string())));
        let ex = ex.with_add_peer_resolver(target);
        let original = Operator::AddPeer {
            region_id: 1,
            node_id: 0, // 占位：由解析器选择目标
            raft_addr: String::new(),
        };
        let system = Arc::new(FakeSystemRaft::new(Some(1)));
        system.seed_pending(7, original.clone(), 1);
        pd.attach_system_raft(system.clone());

        let fake2: Arc<dyn RegionRaftHandle> = fake.clone();
        let resolve: Arc<RegionRaftResolver> = Arc::new(move |_rid| Some(fake2.clone()));
        let op = ex.execute_one(&*resolve).await.expect("resolved add-peer");
        assert_eq!(op.name(), "add-peer");
        // 完成状态记在原始占位条目（node_id=0）上
        let e = system.queue_entry(7).expect("entry present");
        assert!(
            matches!(e.status, super::super::operator::OperatorStatus::Success),
            "entry must reach Success: {e:?}"
        );
        assert_eq!(fake.add_learner_count(), 1);
        assert_eq!(fake.promote_calls(), vec![2]);
        assert!(has_peer(&peers_of_meta(&pd), 2, PeerRole::Voter));
        // 返回的是解析后的具体目标版本（可观察性）
        assert!(matches!(op, Operator::AddPeer { node_id: 2, .. }));
    }

    #[tokio::test]
    async fn test_execute_one_unresolvable_add_peer_requeues() {
        // 占位 AddPeer 无可选目标 → 保持 Pending 稍后重试，不记 Failed
        let (pd, ex, _fake) = leader_fake(vec![voter(1)]);
        let none: Arc<AddPeerTargetResolver> = Arc::new(|_rid| None);
        let ex = ex.with_add_peer_resolver(none);
        let op = Operator::AddPeer {
            region_id: 1,
            node_id: 0,
            raft_addr: String::new(),
        };
        let system = Arc::new(FakeSystemRaft::new(Some(1)));
        system.seed_pending(8, op.clone(), 1);
        pd.attach_system_raft(system.clone());

        let fake2: Arc<dyn RegionRaftHandle> = Arc::new(FakeRaft::new(1, vec![voter(1)]));
        let resolve: Arc<RegionRaftResolver> = Arc::new(move |_rid| Some(fake2.clone()));
        let ran = ex.execute_one(&*resolve).await;
        assert!(ran.is_none(), "unresolvable add-peer must not run");
        let e = system.queue_entry(8).expect("entry present");
        assert!(
            matches!(e.status, super::super::operator::OperatorStatus::Pending),
            "unresolvable add-peer must stay pending (not failed): {e:?}"
        );
    }

    // ──── 全局队列模式（region 0 raft 承载）────

    /// 可编程 region 0 system raft 替身：与 `apply_pd_op` 同构的 CAS 模拟 +
    /// 记录 propose 命令（供断言 Claim/Complete 序列）。
    struct FakeSystemRaft {
        leader: Mutex<Option<NodeID>>,
        queue: Mutex<Vec<PdQueueEntry>>,
        proposed: Mutex<Vec<PdOp>>,
        table: Mutex<crate::storage::region_table::RegionTable>,
        intents: Mutex<Vec<PdIntent>>,
        next_op_id: std::sync::atomic::AtomicU64,
    }

    impl FakeSystemRaft {
        fn new(leader: Option<NodeID>) -> Self {
            Self {
                leader: Mutex::new(leader),
                queue: Mutex::new(Vec::new()),
                proposed: Mutex::new(Vec::new()),
                table: Mutex::new(crate::storage::region_table::RegionTable::default()),
                intents: Mutex::new(Vec::new()),
                next_op_id: std::sync::atomic::AtomicU64::new(1),
            }
        }

        /// 预置一条 Pending 队列条目（op_id 由调用方指定）
        fn seed_pending(&self, op_id: u64, op: Operator, requester: NodeID) {
            self.queue.lock().unwrap().push(PdQueueEntry::new_pending(
                op_id,
                op,
                requester,
                1_700_000_000,
            ));
        }

        /// 预置权威表（executor 的单测不需要真实 raft）。
        fn seed_table(&self, table: crate::storage::region_table::RegionTable) {
            *self.table.lock().unwrap() = table;
        }

        fn table(&self) -> crate::storage::region_table::RegionTable {
            self.table.lock().unwrap().clone()
        }

        fn intents(&self) -> Vec<PdIntent> {
            self.intents.lock().unwrap().clone()
        }

        fn proposed_ops(&self) -> Vec<PdOp> {
            self.proposed.lock().unwrap().clone()
        }

        fn queue_entry(&self, op_id: u64) -> Option<PdQueueEntry> {
            self.queue
                .lock()
                .unwrap()
                .iter()
                .find(|e| e.op_id == op_id)
                .cloned()
        }
    }

    #[async_trait]
    impl SystemRaftHandle for FakeSystemRaft {
        async fn current_leader(&self) -> Option<NodeID> {
            *self.leader.lock().unwrap()
        }

        async fn propose_pd(&self, op: PdOp) -> coord_core::error::Result<u64> {
            self.proposed.lock().unwrap().push(op.clone());
            let mut queue = self.queue.lock().unwrap();
            match &op {
                PdOp::Enqueue {
                    op,
                    requester,
                    proposed_at_unix,
                } => {
                    let dup = queue
                        .iter()
                        .any(|e| (e.is_pending() || e.is_running()) && &e.op == op);
                    if !dup {
                        let id = self
                            .next_op_id
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        queue.push(PdQueueEntry::new_pending(
                            id,
                            op.clone(),
                            *requester,
                            *proposed_at_unix,
                        ));
                    }
                    Ok(1)
                }
                PdOp::Claim {
                    op_id,
                    node_id,
                    claimed_at_unix,
                } => {
                    if let Some(e) = queue.iter_mut().find(|e| e.op_id == *op_id) {
                        e.try_claim(*node_id, *claimed_at_unix);
                    }
                    Ok(1)
                }
                PdOp::Complete {
                    op_id,
                    node_id,
                    success,
                    error,
                } => {
                    if let Some(e) = queue.iter_mut().find(|e| e.op_id == *op_id) {
                        e.try_complete(*node_id, *success, error);
                    }
                    Ok(1)
                }
                PdOp::Requeue { op_id } => {
                    if let Some(e) = queue.iter_mut().find(|e| e.op_id == *op_id) {
                        e.try_requeue();
                    }
                    Ok(1)
                }
                PdOp::RecordIntent { intent } => {
                    let mut intents = self.intents.lock().unwrap();
                    if !intents.iter().any(|i| i.op_id == intent.op_id) {
                        intents.push(intent.clone());
                    }
                    Ok(1)
                }
                PdOp::Cutover { op_id } => {
                    let mut intents = self.intents.lock().unwrap();
                    if let Some(intent) = intents.iter_mut().find(|i| i.op_id == *op_id) {
                        if intent.phase == IntentPhase::Prepared {
                            let mut table = self.table.lock().unwrap();
                            intent.phase = if table.version != intent.plan.expected_table_version {
                                IntentPhase::Rejected {
                                    reason: format!(
                                        "region table moved to version {} (plan expected {})",
                                        table.version, intent.plan.expected_table_version
                                    ),
                                }
                            } else {
                                match table.apply(&intent.plan.changes) {
                                    Ok(_) => IntentPhase::Applied {
                                        table_version: table.version,
                                    },
                                    Err(err) => IntentPhase::Rejected {
                                        reason: err.to_string(),
                                    },
                                }
                            };
                        }
                    }
                    Ok(1)
                }
                PdOp::ClearIntent { op_id } => {
                    self.intents.lock().unwrap().retain(|i| i.op_id != *op_id);
                    Ok(1)
                }
            }
        }

        fn pd_queue(&self) -> coord_core::error::Result<Vec<PdQueueEntry>> {
            Ok(self.queue.lock().unwrap().clone())
        }

        fn region_table(
            &self,
        ) -> coord_core::error::Result<crate::storage::region_table::RegionTable> {
            Ok(self.table.lock().unwrap().clone())
        }

        fn pd_intents(&self) -> coord_core::error::Result<Vec<PdIntent>> {
            Ok(self.intents.lock().unwrap().clone())
        }

        fn pd_intent(&self, op_id: u64) -> coord_core::error::Result<Option<PdIntent>> {
            Ok(self
                .intents
                .lock()
                .unwrap()
                .iter()
                .find(|i| i.op_id == op_id)
                .cloned())
        }
    }

    /// 全局队列模式：本节点是 Region leader → 认领（Claim CAS）→ 执行 →
    /// 完成（Complete success）；队列终态 Success、meta 同步。
    #[tokio::test]
    async fn test_raft_mode_execute_claims_and_completes_when_region_leader() {
        let (pd, ex, fake) = leader_fake(vec![voter(1)]);
        let op = Operator::AddPeer {
            region_id: 1,
            node_id: 2,
            raft_addr: "node2:50052".into(),
        };
        // region 0 leader = 本节点（1）；预置一条 Pending AddPeer（op_id=5）
        let system = Arc::new(FakeSystemRaft::new(Some(1)));
        system.seed_pending(5, op.clone(), 1);
        pd.attach_system_raft(system.clone());

        let fake2: Arc<dyn RegionRaftHandle> = fake.clone();
        let resolve: Arc<RegionRaftResolver> = Arc::new(move |_rid| Some(fake2.clone()));
        let ran = ex.execute_one(&*resolve).await;
        assert!(
            ran.is_some(),
            "raft-mode executor should process leader-owned op"
        );

        // 执行落在真实 Region raft 替身
        assert_eq!(fake.add_learner_count(), 1);
        assert_eq!(fake.promote_calls(), vec![2]);
        assert!(has_peer(&peers_of_meta(&pd), 2, PeerRole::Voter));

        // propose 序列：先 Claim（认领墙钟已填）后 Complete(success)
        let proposed = system.proposed_ops();
        assert_eq!(proposed.len(), 2, "proposed: {proposed:?}");
        assert!(matches!(
            &proposed[0],
            PdOp::Claim {
                op_id: 5,
                node_id: 1,
                ..
            }
        ));
        if let PdOp::Claim {
            claimed_at_unix, ..
        } = &proposed[0]
        {
            assert!(
                *claimed_at_unix > 0,
                "认领者须填认领墙钟（P3 超时重认领依据）"
            );
        }
        assert!(matches!(
            &proposed[1],
            PdOp::Complete {
                op_id: 5,
                node_id: 1,
                success: true,
                ..
            }
        ));
        // 队列终态 Success
        let e = system.queue_entry(5).expect("entry present");
        assert!(
            matches!(e.status, super::super::operator::OperatorStatus::Success),
            "entry must reach Success: {e:?}"
        );
    }

    /// 全局队列模式：条目目标 Region 的 leader 不是本节点 → 不认领（留给
    /// 他节点），条目保持 Pending、无 propose。
    #[tokio::test]
    async fn test_raft_mode_execute_skips_op_when_not_region_leader() {
        // executor 节点 1，但 Region leader = node2
        let (_pd, ex, _fake) = make_executor_pd(1, vec![voter(1), voter(2)]);
        let op = Operator::AddPeer {
            region_id: 1,
            node_id: 3,
            raft_addr: "node3:50052".into(),
        };
        let system = Arc::new(FakeSystemRaft::new(Some(1))); // region 0 leader = 本节点
        system.seed_pending(7, op.clone(), 2);
        _pd.attach_system_raft(system.clone());

        let region_fake = Arc::new(FakeRaft::new(2, vec![voter(1), voter(2)])); // region leader=2
        let f2: Arc<dyn RegionRaftHandle> = region_fake.clone();
        let resolve: Arc<RegionRaftResolver> = Arc::new(move |_rid| Some(f2.clone()));

        let ran = ex.execute_one(&*resolve).await;
        assert!(ran.is_none(), "op led by node2 must not run on node1");
        assert!(
            system.proposed_ops().is_empty(),
            "no claim should be proposed for non-leader op"
        );
        let e = system.queue_entry(7).expect("entry present");
        assert!(e.is_pending(), "entry must stay Pending: {e:?}");
        assert_eq!(region_fake.add_learner_count(), 0);
    }

    /// 全局队列模式：认领竞态失败（他节点已认领为 Running）→ 执行器跳过、
    /// 不执行、不 Complete。
    #[tokio::test]
    async fn test_raft_mode_execute_skips_already_running_entry() {
        let (_pd, ex, _fake) = make_executor_pd(1, vec![voter(1)]);
        let op = Operator::RemovePeer {
            region_id: 1,
            node_id: 3,
        };
        let system = Arc::new(FakeSystemRaft::new(Some(1)));
        // 预置一条已由 node2 认领的 Running 条目（认领竞态失败场景）
        system.queue.lock().unwrap().push(PdQueueEntry {
            op_id: 9,
            op: op.clone(),
            status: super::super::operator::OperatorStatus::Running,
            requester: 2,
            claimed_by: 2,
            proposed_at_unix: 1_700_000_000,
            claimed_at_unix: 1_700_000_000,
            error: String::new(),
        });
        _pd.attach_system_raft(system.clone());

        let f2: Arc<dyn RegionRaftHandle> = Arc::new(FakeRaft::new(1, vec![voter(1)]));
        let resolve: Arc<RegionRaftResolver> = Arc::new(move |_rid| Some(f2.clone()));

        let ran = ex.execute_one(&*resolve).await;
        assert!(ran.is_none(), "already-running op must not be re-executed");
        assert!(
            system.proposed_ops().is_empty(),
            "no claim/complete for running entry claimed elsewhere"
        );
        let e = system.queue_entry(9).expect("entry present");
        assert_eq!(e.claimed_by, 2, "claim ownership unchanged");
    }

    #[tokio::test]
    async fn test_executor_loop_executes_and_stops_on_shutdown() {
        let (pd, ex, shutdown_tx) = make_executor_pd(1, vec![voter(1)]);
        // P4b：条目经全局队列（region 0 raft 承载）入队
        let system = Arc::new(FakeSystemRaft::new(Some(1)));
        system.seed_pending(
            1,
            Operator::AddPeer {
                region_id: 1,
                node_id: 2,
                raft_addr: "node2:50052".into(),
            },
            1,
        );
        pd.attach_system_raft(system.clone());

        let ex = Arc::new(ex);
        let fake = Arc::new(FakeRaft::new(1, vec![voter(1)]));
        let fake2: Arc<dyn RegionRaftHandle> = fake.clone();
        let resolve: Arc<RegionRaftResolver> = Arc::new(move |_rid| Some(fake2.clone()));
        let handle = ex.start_executor_loop(resolve, Duration::from_millis(50));

        // 等待 operator 被执行完成（全局队列条目终态 Success）
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let done = system
                .queue_entry(1)
                .map(|e| matches!(e.status, super::super::operator::OperatorStatus::Success))
                .unwrap_or(false);
            if done {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "executor loop never executed operator"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(fake.add_learner_count(), 1);

        // 优雅停止
        shutdown_tx.send(true).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(3), handle)
            .await
            .expect("executor loop should exit on shutdown");
    }

    // ──── Split / Merge（#162）────

    fn region_meta(id: RegionId, start: &[u8], end: &[u8]) -> RegionMeta {
        RegionMeta {
            region_id: id,
            start_key: start.to_vec(),
            end_key: end.to_vec(),
            epoch: RegionEpoch::initial(),
            peers: vec![voter(1)],
            approximate_size: 0,
            approximate_keys: 0,
        }
    }

    fn single_region_table() -> crate::storage::region_table::RegionTable {
        let mut table = crate::storage::region_table::RegionTable::default();
        table.init(vec![region_meta(1, b"", b"")]).unwrap();
        table
    }

    /// 迁移钩子替身：记录计划；可注入一次失败。
    struct FakeMigrator {
        calls: Mutex<Vec<crate::storage::region_table::SplitMergePlan>>,
        fail: AtomicBool,
    }

    impl FakeMigrator {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                calls: Mutex::new(Vec::new()),
                fail: AtomicBool::new(false),
            })
        }

        fn calls(&self) -> Vec<crate::storage::region_table::SplitMergePlan> {
            self.calls.lock().unwrap().clone()
        }

        fn fail_next(&self) {
            self.fail.store(true, Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl SplitMergeMigrator for FakeMigrator {
        async fn migrate(
            &self,
            plan: &crate::storage::region_table::SplitMergePlan,
        ) -> std::result::Result<(), String> {
            self.calls.lock().unwrap().push(plan.clone());
            if self.fail.load(Ordering::SeqCst) {
                Err("migration failed".to_string())
            } else {
                Ok(())
            }
        }
    }

    fn split_executor(
        table: crate::storage::region_table::RegionTable,
        migrator: Option<Arc<FakeMigrator>>,
    ) -> (
        Arc<PlacementDriver>,
        OperatorExecutor,
        Arc<FakeSystemRaft>,
        Arc<FakeRaft>,
    ) {
        let (pd, ex, fake) = leader_fake(vec![voter(1)]);
        let system = Arc::new(FakeSystemRaft::new(Some(1)));
        system.seed_table(table);
        pd.attach_system_raft(system.clone());
        let ex = match migrator {
            Some(m) => ex.with_migrator(m),
            None => ex,
        };
        (pd, ex, system, fake)
    }

    fn resolver_for(fake: &Arc<FakeRaft>) -> Arc<RegionRaftResolver> {
        let raft: Arc<dyn RegionRaftHandle> = fake.clone();
        Arc::new(move |_rid| Some(raft.clone()))
    }

    #[tokio::test]
    async fn test_split_operator_plans_intent_migrates_and_cuts_over() {
        let migrator = FakeMigrator::new();
        let (pd, ex, system, fake) = split_executor(single_region_table(), Some(migrator.clone()));
        system.seed_pending(
            5,
            Operator::SplitRegion {
                region_id: 1,
                split_key: b"m".to_vec(),
                new_region_id: 2,
            },
            1,
        );
        let resolve = resolver_for(&fake);

        let op = ex.execute_one(&*resolve).await.expect("operator executed");
        assert_eq!(op.name(), "split-region");

        let calls = migrator.calls();
        assert_eq!(calls.len(), 1, "migration runs once before cutover");
        assert_eq!(calls[0].expected_table_version, 1);
        assert_eq!(calls[0].changes.len(), 2);

        let table = system.table();
        assert_eq!(table.version, 2);
        assert_eq!(table.get(1).unwrap().end_key, b"m");
        assert_eq!(table.get(2).unwrap().start_key, b"m");
        assert!(system.intents().is_empty(), "intent cleared after success");

        let entry = system.queue_entry(5).expect("entry present");
        assert_eq!(entry.status, OperatorStatus::Success);

        let parent = pd.meta_store().get_region(1).expect("parent");
        assert_eq!(parent.end_key, b"m");
        let child = pd.meta_store().get_region(2).expect("child reconciled");
        assert_eq!(child.start_key, b"m");

        // 重复执行：队列里没有 Pending 条目，不得再触发迁移。
        assert!(ex.execute_one(&*resolve).await.is_none());
        assert_eq!(migrator.calls().len(), 1);
    }

    #[tokio::test]
    async fn test_split_without_migrator_fails_closed_before_side_effects() {
        let (pd, ex, system, fake) = split_executor(single_region_table(), None);
        system.seed_pending(
            6,
            Operator::SplitRegion {
                region_id: 1,
                split_key: b"m".to_vec(),
                new_region_id: 2,
            },
            1,
        );
        let resolve = resolver_for(&fake);

        ex.execute_one(&*resolve).await.expect("operator processed");
        assert_eq!(system.table().version, 1, "table untouched");
        assert!(system.intents().is_empty(), "no intent written");
        assert!(pd.meta_store().get_region(2).is_none());
        let entry = system.queue_entry(6).expect("entry present");
        match entry.status {
            OperatorStatus::Failed(reason) => {
                assert!(reason.contains("not wired"), "reason: {reason}")
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_merge_operator_cuts_over_and_reconciles_meta_store() {
        let mut table = crate::storage::region_table::RegionTable::default();
        table
            .init(vec![region_meta(1, b"", b"m"), region_meta(2, b"m", b"")])
            .unwrap();
        let migrator = FakeMigrator::new();
        let (pd, ex, system, fake) = split_executor(table, Some(migrator.clone()));
        pd.meta_store()
            .create_region(region_meta(2, b"m", b""))
            .unwrap();
        system.seed_pending(7, Operator::MergeRegion { left: 1, right: 2 }, 1);
        let resolve = resolver_for(&fake);

        ex.execute_one(&*resolve).await.expect("operator executed");
        assert_eq!(migrator.calls().len(), 1);
        let table = system.table();
        assert_eq!(table.version, 2);
        assert_eq!(table.get(1).unwrap().end_key, b"");
        assert!(table.get(2).is_none());
        let parent = pd.meta_store().get_region(1).unwrap();
        assert_eq!(parent.end_key, b"");
        assert!(
            pd.meta_store().get_region(2).is_none(),
            "retired region removed from the meta store"
        );
        assert!(system.intents().is_empty());
    }

    #[tokio::test]
    async fn test_split_migration_failure_keeps_table_and_intent_prepared() {
        let migrator = FakeMigrator::new();
        migrator.fail_next();
        let (_pd, ex, system, fake) = split_executor(single_region_table(), Some(migrator.clone()));
        system.seed_pending(
            9,
            Operator::SplitRegion {
                region_id: 1,
                split_key: b"m".to_vec(),
                new_region_id: 2,
            },
            1,
        );
        let resolve = resolver_for(&fake);
        ex.execute_one(&*resolve).await.expect("operator processed");

        assert_eq!(system.table().version, 1, "no cutover on migration failure");
        let intents = system.intents();
        assert_eq!(intents.len(), 1, "intent retained for recovery/GC");
        assert_eq!(intents[0].phase, IntentPhase::Prepared);
        match system.queue_entry(9).expect("entry").status {
            OperatorStatus::Failed(reason) => {
                assert!(reason.contains("migration failed"), "reason: {reason}")
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_split_cutover_rejected_when_table_version_moves() {
        use crate::storage::region_table::{plan_split, RegionChange};
        use coord_core::types::RegionEpoch;

        let migrator = FakeMigrator::new();
        let (pd, ex, system, fake) = split_executor(single_region_table(), Some(migrator.clone()));
        let plan = plan_split(&system.table(), 1, b"m", 2).unwrap();
        system
            .propose_pd(PdOp::RecordIntent {
                intent: PdIntent {
                    op_id: 8,
                    owner: 1,
                    plan,
                    phase: IntentPhase::Prepared,
                },
            })
            .await
            .unwrap();
        // 表在计划后前进：cutover 必须整批拒绝。
        let mut bumped = region_meta(1, b"", b"");
        bumped.epoch = RegionEpoch {
            conf_ver: 1,
            version: 2,
        };
        let mut table = system.table();
        table
            .apply(&[RegionChange::Update { meta: bumped }])
            .unwrap();
        system.seed_table(table);
        system.seed_pending(
            8,
            Operator::SplitRegion {
                region_id: 1,
                split_key: b"m".to_vec(),
                new_region_id: 2,
            },
            1,
        );
        let resolve = resolver_for(&fake);
        ex.execute_one(&*resolve).await.expect("operator processed");

        let entry = system.queue_entry(8).expect("entry present");
        match entry.status {
            OperatorStatus::Failed(reason) => {
                assert!(reason.contains("rejected"), "reason: {reason}")
            }
            other => panic!("expected Failed, got {other:?}"),
        }
        assert_eq!(system.table().version, 2, "rejected cutover keeps table");
        assert!(system.table().get(2).is_none());
        assert!(system.intents().is_empty(), "rejected intent is cleared");
        assert_eq!(migrator.calls().len(), 1);
        assert!(pd.meta_store().get_region(2).is_none());
    }
}
