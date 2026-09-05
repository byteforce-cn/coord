// PD Operator 执行器（Phase 3 T3.3）
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
// - pd 模块不直接依赖 raft/openraft 类型（P1-06：`openraft::`/`openraft_multi::`
//   路径只允许出现在 `raft/` 模块内）。对 Region raft 的全部交互经
//   `RegionRaftHandle` trait 抽象（定义于 `raft/region_runtime.rs`，由 raft 层
//   自我描述能力），执行器只经 trait object 使用。
// - **Leader 守卫**：只有 Region leader 才能发起 change_membership /
//   transfer_leader（openraft 拒绝非 leader 调用）。执行器运行在 Region leader
//   所在节点的 PD 上；本节点不是该 Region leader 时 operator 记 Failed——
//   跨节点 operator 转移/去重依赖 PD 元数据与命令经 region 0 system raft
//   复制（T3.4 接线层决策，本层不实现）。
// - 幂等：AddPeer 目标已是 Voter / RemovePeer 目标不在成员表 → 视为已达成
//   成功（不重复触发 raft 成员变更，也不把重试当失败）。

use std::sync::Arc;
use std::time::Duration;

use coord_core::types::{NodeID, Peer, PeerRole, RegionId, RegionMeta};
use tokio::time::MissedTickBehavior;

use super::{Operator, PlacementDriver};
use crate::raft::region_runtime::RegionRaftHandle;

/// Region raft 解析器：region_id → 本节点上该 Region 的 raft 句柄
///
/// 由接线层提供（T3.4：main.rs 把 RegionManager 的 runtime 映射为 handle；
/// 测试/本迭代集成测试直接构造）。
pub type RegionRaftResolver = dyn Fn(RegionId) -> Option<Arc<dyn RegionRaftHandle>> + Send + Sync;

/// T3.3 Operator 执行器：消费 `PlacementDriver` 队列中的 operator 并应用到
/// 本节点可执行的 Region raft。
pub struct OperatorExecutor {
    /// 所属 PD（持有 meta_store 与 operator 队列）
    pd: Arc<PlacementDriver>,
    /// 本节点 ID（Leader 守卫）
    node_id: NodeID,
    /// TransferLeader 发起后等待新 leader 真正上线的超时
    transfer_timeout: Duration,
}

impl OperatorExecutor {
    /// 创建执行器（`node_id` = 本节点；仅当本节点是目标 Region leader 时
    /// operator 才会真正执行）
    pub fn new(pd: Arc<PlacementDriver>, node_id: NodeID) -> Self {
        Self {
            pd,
            node_id,
            transfer_timeout: Duration::from_secs(30),
        }
    }

    /// 配置 TransferLeader 等待超时（测试用）
    pub fn with_transfer_timeout(mut self, timeout: Duration) -> Self {
        self.transfer_timeout = timeout;
        self
    }

    /// 弹出队列中下一个 Pending operator 并尝试在本节点执行
    ///
    /// 结果经 `complete_operator` 写入队列（Success/Failed）；无 Pending
    /// operator 时返回 None。
    pub async fn execute_one(&self, resolve: &RegionRaftResolver) -> Option<Operator> {
        let op = self.pd.take_next_operator()?;
        let outcome = match resolve(op.region_id()) {
            Some(raft) => self.execute(&op, raft).await,
            None => Err(format!(
                "region {} runtime not present on node {}",
                op.region_id(),
                self.node_id
            )),
        };
        match outcome {
            Ok(()) => self.pd.complete_operator(&op, true, None),
            Err(msg) => self.pd.complete_operator(&op, false, Some(msg)),
        }
        Some(op)
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
        // Region leader 所在节点的 PD 执行（见模块文档 T3.4 边界）。
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
            } => self
                .exec_add_peer(*region_id, *node_id, raft_addr, raft.as_ref())
                .await,
            Operator::RemovePeer {
                region_id,
                node_id,
            } => self.exec_remove_peer(*region_id, *node_id, raft.as_ref()).await,
            Operator::TransferLeader {
                region_id,
                to_node,
            } => self
                .exec_transfer_leader(*region_id, *to_node, raft.as_ref())
                .await,
            Operator::SplitRegion { .. } | Operator::MergeRegion { .. } => Err(format!(
                "operator {} not supported in v1 (split/merge deferred to write-path consistency work)",
                op.name()
            )),
        }
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

        let mut meta = self.region_meta_of(region_id)?;
        let existing = meta.peers.iter().find(|p| p.node_id == peer_id).cloned();
        match existing {
            // 已是 Voter：幂等成功（不重复触发 raft 成员变更）
            Some(p) if p.role == PeerRole::Voter => return Ok(()),
            // 已是 Learner：跳过 add_learner，仅晋升；地址不一致则拒绝（状态漂移）
            Some(p) if p.role == PeerRole::Learner && p.raft_addr != raft_addr => {
                return Err(format!(
                    "add-peer: node {peer_id} already learner at {} but requested {raft_addr}",
                    p.raft_addr
                ));
            }
            _ => {}
        }

        let is_learner = matches!(existing.as_ref().map(|p| p.role), Some(PeerRole::Learner));
        if !is_learner {
            raft.add_learner(peer_id, raft_addr)
                .await
                .map_err(|e| format!("add_learner node {peer_id} (region {region_id}): {e}"))?;
        }
        raft.promote_to_voter(peer_id)
            .await
            .map_err(|e| format!("promote node {peer_id} to voter (region {region_id}): {e}"))?;

        // 同步 PD 元数据（调度真源）：写穿落盘 + conf_ver 递增
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
    use coord_core::types::{RegionEpoch, RegionMeta};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;
    use tokio::sync::watch;

    /// 可控 raft 替身：记录成员变更调用、可切换 leader 视图
    struct FakeRaft {
        /// 当前 leader（executor 节点视角）
        leader: Mutex<Option<NodeID>>,
        add_learner_calls: Mutex<Vec<(NodeID, String)>>,
        promote_calls: Mutex<Vec<NodeID>>,
        remove_calls: Mutex<Vec<NodeID>>,
        transfer_calls: Mutex<Vec<NodeID>>,
        /// transfer_leader 被调用后自动切换 leader（模拟转移成功）
        auto_switch: AtomicBool,
    }

    impl FakeRaft {
        fn new(leader: NodeID) -> Self {
            Self {
                leader: Mutex::new(Some(leader)),
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

        async fn add_learner(
            &self,
            node_id: NodeID,
            raft_addr: &str,
        ) -> coord_core::error::Result<()> {
            self.add_learner_calls
                .lock()
                .unwrap()
                .push((node_id, raft_addr.to_string()));
            Ok(())
        }

        async fn promote_to_voter(&self, node_id: NodeID) -> coord_core::error::Result<()> {
            self.promote_calls.lock().unwrap().push(node_id);
            Ok(())
        }

        async fn remove_voter(&self, node_id: NodeID) -> coord_core::error::Result<()> {
            self.remove_calls.lock().unwrap().push(node_id);
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
        let pd = Arc::new(PlacementDriver::new(config, meta_store, shutdown_rx));
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

    /// 返回 (pd, executor[node1], fake raft leader=node1)
    fn leader_fake(peers: Vec<Peer>) -> (Arc<PlacementDriver>, OperatorExecutor, Arc<FakeRaft>) {
        let (pd, ex, _tx) = make_executor_pd(1, peers);
        let fake = Arc::new(FakeRaft::new(1));
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
        let fake = Arc::new(FakeRaft::new(2)); // leader 已是 node2（executor 节点 2）
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
        let fake = Arc::new(FakeRaft::new(2));
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
    async fn test_execute_split_region_not_supported() {
        let (_pd, ex, fake) = leader_fake(vec![voter(1)]);
        let op = Operator::SplitRegion {
            region_id: 1,
            split_key: b"m".to_vec(),
            new_region_id: 2,
        };
        let raft: Arc<dyn RegionRaftHandle> = fake.clone();
        let err = ex.execute(&op, raft).await.expect_err("split unsupported");
        assert!(err.contains("not supported"), "err: {err}");
    }

    // ──── 队列驱动（execute_one / 执行循环）────

    #[tokio::test]
    async fn test_execute_one_pulls_queue_and_completes_success() {
        let (pd, ex, fake) = leader_fake(vec![voter(1)]);
        pd.enqueue_operator(Operator::AddPeer {
            region_id: 1,
            node_id: 2,
            raft_addr: "node2:50052".into(),
        });

        let fake2: Arc<dyn RegionRaftHandle> = fake.clone();
        let resolve: Arc<RegionRaftResolver> = Arc::new(move |_rid| Some(fake2.clone()));
        let op = ex.execute_one(&*resolve).await.expect("one operator");
        assert_eq!(op.name(), "add-peer");
        assert!(matches!(
            pd.operator_status(&op),
            Some(super::super::operator::OperatorStatus::Success)
        ));
        // meta 已同步
        assert!(has_peer(&peers_of_meta(&pd), 2, PeerRole::Voter));
    }

    #[tokio::test]
    async fn test_execute_one_completes_failed_when_region_missing() {
        let (pd, ex, _fake) = make_executor_pd(1, vec![voter(1)]);
        pd.enqueue_operator(Operator::TransferLeader {
            region_id: 99, // meta_store 无此 region
            to_node: 2,
        });
        let resolve: Arc<RegionRaftResolver> =
            Arc::new(|_rid| Some(Arc::new(FakeRaft::new(1)) as Arc<dyn RegionRaftHandle>));
        let op = ex.execute_one(&*resolve).await.expect("one operator");
        let st = pd.operator_status(&op).unwrap();
        assert!(
            matches!(st, super::super::operator::OperatorStatus::Failed(_)),
            "status: {st:?}"
        );
    }

    #[tokio::test]
    async fn test_executor_loop_executes_and_stops_on_shutdown() {
        let (pd, ex, shutdown_tx) = make_executor_pd(1, vec![voter(1)]);
        pd.enqueue_operator(Operator::AddPeer {
            region_id: 1,
            node_id: 2,
            raft_addr: "node2:50052".into(),
        });

        let ex = Arc::new(ex);
        let fake = Arc::new(FakeRaft::new(1));
        let fake2: Arc<dyn RegionRaftHandle> = fake.clone();
        let resolve: Arc<RegionRaftResolver> = Arc::new(move |_rid| Some(fake2.clone()));
        let handle = ex.start_executor_loop(resolve, Duration::from_millis(50));

        // 等待 operator 被执行完成
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let stats = pd.operator_stats();
            if stats.success >= 1 {
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
}
