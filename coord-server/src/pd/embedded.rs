// PD 内嵌接线层
//
// 把 PD 组件（PdMetaStore / PlacementDriver / OperatorExecutor）与生产 Region
// 装配（RegionManager + per-region RegionRuntime）接起来，作为 main.rs 的
// 装配单元（`[multi_raft].enabled=true` 且 `[multi_raft.pd].enabled=true` 时
// 构建；测试套件同路径复用）：
//
//   1. **元数据落盘与播种**：`PdMetaStore::open(data_dir)`（`<data_dir>/pd/
//      pd-meta.db`，重启恢复）并按配置 Region 表（`RegionSeed`）播种——v1
//      静态：key range 以配置为真源，持久 peers/epoch 跨重启保留（operator
//      变更不因重启回退）；
//   2. **节点心跳**：注册 `cluster.initial_nodes` 全部成员并周期性刷新
//      （NodeState），调度器据此判在线/离线；
//   3. **Region 心跳上报**：每个 Region 周期性上报 leader / size / keys——
//      喂给 PD 实时调度数据面（leader 均衡、split/merge 阈值、离线清理）；
//   4. **PD 元数据对账（跨节点一致性）**：以 **raft 已提交 voter 集**为真源
//      把本节点 PdMetaStore 与 RegionHandle.meta 的 peers 收敛到 raft 成员。
//      成员变更经 raft 日志复制到所有节点，故「各节点 PD meta 与 raft 一致」
//      即达成 PD meta 的跨节点一致（执行器在 leader 上写穿、follower 经对账
//      收敛）；conf_ver 为本地变更计数器（信息性；中间态被跳过时可能滞后，
//      不影响调度——调度只看 peers/leader）；
//   5. **调度 + 执行循环**：scheduler loop 产生 operator，executor loop 应用
//      到目标 Region raft（Leader 守卫）；AddPeer 占位目标（node_id=0）经
//      注册节点表解析（在线且非该 Region voter 者优先），无可用目标放回队列
//      稍后重试（不记 Failed，避免 churn）。
//
// 模块边界：本层是装配/编排代码，接触 RegionRuntime 等 raft 层具体类型
// （决策逻辑仍经 trait 抽象，见 `executor.rs` 模块文档）；不引用任何
// `openraft::`/`openraft_multi::` 路径（类型隔离由 `raft/` 层收敛）。
//
// operator 跨节点去重经 region 0 system raft 承载（分阶段落地）。基座 =
// `Command::Pd`/`PdQueueEntry`/`apply_pd_op`；再为本层接线——`EmbeddedPd::start`
// 新增 `system_raft` 参数：main.rs 把 region 0 raft 句柄传入（全局队列模式：
// 调度收敛 region 0 leader、执行器从全局队列按 Region leader 认领）；此后
// system raft 为必填（legacy 本地队列路径退役，不再有 None/本地模式装配）。

use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use coord_core::storage::StorageBackend;
use coord_core::types::{NodeID, Peer, PeerRole, RegionEpoch, RegionId, RegionMeta};
use parking_lot::Mutex;
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;

use super::executor::{AddPeerTargetResolver, RegionRaftResolver};
use super::meta_store::PdMetaStore;
use super::types::NodeState;
use super::{OperatorExecutor, PdConfig, PlacementDriver};
use crate::raft::region::{RegionManager, RegionSeed};
use crate::raft::region_runtime::{CoordRegionRaftHandle, RegionRaftHandle, RegionRuntime};
use crate::raft::system_raft::SystemRaftHandle;

/// 集群节点信息（节点心跳注册 + AddPeer 目标地址解析用）
#[derive(Debug, Clone)]
pub struct NodeInfo {
    /// 节点 ID
    pub node_id: NodeID,
    /// Raft 通信地址
    pub raft_addr: String,
    /// gRPC 服务地址
    pub grpc_addr: String,
}

/// 内嵌 PD：单进程内 PlacementDriver 与其数据面/执行面的完整接线
pub struct EmbeddedPd {
    /// 本节点 ID（operator Leader 守卫）
    node_id: NodeID,
    /// PlacementDriver（调度决策）
    pub driver: Arc<PlacementDriver>,
    /// PD 元数据存储（持久化）
    pub meta_store: Arc<PdMetaStore>,
    /// 优雅关闭信号发送端
    shutdown_tx: watch::Sender<bool>,
    /// 后台循环句柄（scheduler / executor / heartbeat）
    handles: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl EmbeddedPd {
    /// 启动内嵌 PD。
    ///
    /// - `pd_config`：PD 运行时配置（间隔/阈值/副本目标等）
    /// - `node_id`：本节点 ID
    /// - `data_dir`：节点数据根目录（`<data_dir>/pd/pd-meta.db` 落盘）
    /// - `region_manager`：已装配的 RegionManager（心跳/对账/执行对象）
    /// - `seeds`：配置 Region 表（v1 静态；key range 真源，用于播种/对账）
    /// - `nodes`：集群全部成员（raft/grpc 地址；节点心跳 + AddPeer 目标池）
    /// - `system_raft`：region 0 system raft 治理句柄（
    ///   **必填**——退役 legacy 本地队列路径后，operator 队列恒经 region 0
    ///   raft 承载：调度收敛 region 0 leader + 执行器全局队列认领）。main.rs
    ///   传 `CoordSystemRaftHandle`（region 0 raft + MVCC）；测试装配需自行
    ///   提供真实单节点 region 0 raft 或替身。
    ///
    /// 注：参数为装配期配置项，逐一命名比打包 struct 更可读（调用点单份）。
    #[allow(clippy::too_many_arguments)]
    pub async fn start(
        pd_config: PdConfig,
        node_id: NodeID,
        data_dir: &Path,
        region_manager: &Arc<RegionManager>,
        seeds: &[RegionSeed],
        nodes: Vec<NodeInfo>,
        heartbeat_interval: Duration,
        system_raft: Arc<dyn SystemRaftHandle>,
    ) -> Result<Arc<Self>, coord_core::error::Error> {
        // 1. 元数据落盘 + 播种（key range 以配置为真源；持久 peers/epoch 保留）
        let meta_store = Arc::new(PdMetaStore::open(data_dir)?);
        let cluster_peers: Vec<Peer> = nodes
            .iter()
            .map(|n| Peer {
                node_id: n.node_id,
                raft_addr: n.raft_addr.clone(),
                role: PeerRole::Voter,
            })
            .collect();
        seed_regions(&meta_store, seeds, &cluster_peers)?;

        // 2. PlacementDriver + 节点心跳注册（调度器从首拍起即可见在线节点）
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let hb_shutdown_rx = shutdown_rx.clone();
        let driver = Arc::new(PlacementDriver::new(
            pd_config,
            Arc::clone(&meta_store),
            shutdown_rx,
            node_id,
        ));
        // 装配 region 0 system raft 治理句柄（全局
        // 队列模式——operator 队列唯一承载；必填）
        driver.attach_system_raft(system_raft);
        for n in &nodes {
            driver.handle_node_heartbeat(NodeState::new(
                n.node_id,
                n.raft_addr.clone(),
                n.grpc_addr.clone(),
            ));
        }

        // 3. 执行器：Leader 守卫 + AddPeer 占位目标解析
        let executor = Arc::new(
            OperatorExecutor::new(Arc::clone(&driver), node_id)
                .with_add_peer_resolver(add_peer_resolver(Arc::clone(&driver), nodes.clone())),
        );

        let region_ids: Vec<RegionId> = seeds.iter().map(|s| s.region_id).collect();

        let region_manager = Arc::clone(region_manager);

        // 4. 调度循环
        let scheduler_handle = driver.start_scheduler_loop();

        // 5. 执行循环（RegionManager runtime → RegionRaftHandle）
        let mgr_for_resolve = Arc::clone(&region_manager);
        let resolve: Arc<RegionRaftResolver> = Arc::new(move |rid| {
            mgr_for_resolve.runtime(rid).map(|rt| {
                let h: Arc<dyn RegionRaftHandle> =
                    Arc::new(CoordRegionRaftHandle::from_runtime(&rt));
                h
            })
        });
        let executor_handle = executor.start_executor_loop(resolve, heartbeat_interval);

        // 6. Region/节点心跳 + 成员对账循环
        let hb_handle = tokio::spawn(heartbeat_loop(
            Arc::clone(&driver),
            Arc::clone(&region_manager),
            region_ids.clone(),
            nodes.clone(),
            heartbeat_interval,
            hb_shutdown_rx,
        ));

        let embedded = Arc::new(Self {
            node_id,
            driver: Arc::clone(&driver),
            meta_store,
            shutdown_tx,
            handles: Mutex::new(vec![scheduler_handle, executor_handle, hb_handle]),
        });

        // executor / region_manager / nodes / region_ids / heartbeat_interval
        // 已由各循环任务持 Arc 克隆；这里不再存储（避免冗余字段）。
        drop(executor);
        drop(region_manager);

        tracing::info!(
            "Embedded PD started on node {node_id}: {} region(s), {} node(s), \
             heartbeat_interval={:?}",
            region_ids.len(),
            nodes.len(),
            heartbeat_interval
        );
        Ok(embedded)
    }

    /// 本节点 ID
    pub fn node_id(&self) -> NodeID {
        self.node_id
    }

    /// 心跳上报的 Region ID 列表
    pub fn region_ids(&self) -> Vec<RegionId> {
        self.driver
            .meta_store()
            .list_regions()
            .into_iter()
            .map(|m| m.region_id)
            .collect()
    }

    /// 集群节点列表
    pub fn nodes(&self) -> Vec<NodeInfo> {
        self.driver
            .list_nodes()
            .into_iter()
            .map(|n| NodeInfo {
                node_id: n.node_id,
                raft_addr: n.raft_addr,
                grpc_addr: n.grpc_addr,
            })
            .collect()
    }

    /// 优雅停止：发关闭信号并等待全部后台循环退出（幂等）。
    pub async fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
        let handles: Vec<_> = self.handles.lock().drain(..).collect();
        for h in handles {
            let _ = tokio::time::timeout(Duration::from_secs(5), h).await;
        }
        tracing::info!("Embedded PD on node {} shut down", self.node_id);
    }
}

/// 播种/对账配置 Region 表到持久化 meta store。
///
/// v1 静态 region 表：对每个 seed——已存在则仅修正 key range（配置为真源；
/// peers/epoch 保留，operator 变更跨重启不回退）；不存在则创建（peers =
/// 集群成员全部 voter、epoch 初始）。
fn seed_regions(
    meta_store: &Arc<PdMetaStore>,
    seeds: &[RegionSeed],
    cluster_peers: &[Peer],
) -> Result<(), coord_core::error::Error> {
    for seed in seeds {
        match meta_store.get_region(seed.region_id) {
            None => {
                let meta = RegionMeta {
                    region_id: seed.region_id,
                    start_key: seed.start_key.clone(),
                    end_key: seed.end_key.clone(),
                    epoch: RegionEpoch::initial(),
                    peers: cluster_peers.to_vec(),
                    approximate_size: 0,
                    approximate_keys: 0,
                };
                meta_store.create_region(meta)?;
                tracing::info!(
                    "PD: seeded region {} from config (range {:?}..{:?})",
                    seed.region_id,
                    seed.start_key,
                    seed.end_key
                );
            }
            Some(existing) => {
                if existing.start_key != seed.start_key || existing.end_key != seed.end_key {
                    // v1 无 split/merge；配置漂移属配置错误，修正并告警
                    tracing::warn!(
                        "PD: region {} key range drift vs config ({:?}..{:?} != {:?}..{:?}); \
                         config wins in v1",
                        seed.region_id,
                        existing.start_key,
                        existing.end_key,
                        seed.start_key,
                        seed.end_key
                    );
                    let mut meta = existing;
                    meta.start_key = seed.start_key.clone();
                    meta.end_key = seed.end_key.clone();
                    meta_store.update_region(meta)?;
                }
            }
        }
    }
    Ok(())
}

/// AddPeer 占位目标解析器：从注册集群节点中选在线且非该 Region voter 的节点。
///
/// 倾向副本数最少的节点（v1 静态成员；被 remove 的 voter 在 raft 中仍是
/// learner，会作为候选被选中→promote-only 重加）。返回 None = 无可用目标
/// （执行器放回队列稍后重试）。
fn add_peer_resolver(
    driver: Arc<PlacementDriver>,
    nodes: Vec<NodeInfo>,
) -> Arc<AddPeerTargetResolver> {
    Arc::new(move |region_id: RegionId| -> Option<(NodeID, String)> {
        let meta = driver.meta_store().get_region(region_id)?;
        let voters: BTreeSet<NodeID> = meta
            .peers
            .iter()
            .filter(|p| p.role == PeerRole::Voter)
            .map(|p| p.node_id)
            .collect();

        // 每节点当前 voter 副本数（跨 region 计）→ 均衡倾向
        let mut replica_counts: HashMap<NodeID, usize> = HashMap::new();
        for m in driver.meta_store().list_regions() {
            for p in &m.peers {
                if p.role == PeerRole::Voter {
                    *replica_counts.entry(p.node_id).or_insert(0) += 1;
                }
            }
        }

        let mut candidates: Vec<&NodeInfo> = nodes
            .iter()
            .filter(|n| !voters.contains(&n.node_id))
            .collect();
        candidates.sort_by_key(|n| {
            (
                replica_counts.get(&n.node_id).copied().unwrap_or(0),
                n.node_id,
            )
        });
        candidates
            .into_iter()
            .find(|n| {
                driver
                    .get_node_state(n.node_id)
                    .map(|s| s.online)
                    .unwrap_or(false)
            })
            .map(|n| (n.node_id, n.raft_addr.clone()))
    })
}

/// Region/节点心跳 + 成员对账循环（后台任务）。
///
/// 每拍：
/// - 刷新全部节点心跳（保持在线视图）；
/// - 每个 Region：上报 leader / size / keys（喂 PD 实时调度数据面）；
/// - 每个 Region：以 raft 已提交 voter 集对账 PdMetaStore 与 RegionHandle.meta
///   （跨节点 PD meta 一致性，见模块文档）。
async fn heartbeat_loop(
    driver: Arc<PlacementDriver>,
    region_manager: Arc<RegionManager>,
    region_ids: Vec<RegionId>,
    nodes: Vec<NodeInfo>,
    interval: Duration,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = shutdown_rx.changed() => {
                tracing::info!("PD heartbeat loop: shutdown signal received");
                break;
            }
        }

        // 节点心跳刷新
        for n in &nodes {
            driver.handle_node_heartbeat(NodeState::new(
                n.node_id,
                n.raft_addr.clone(),
                n.grpc_addr.clone(),
            ));
        }

        // Region 心跳上报 + 成员对账
        for region_id in &region_ids {
            let Some(rt) = region_manager.runtime(*region_id) else {
                continue;
            };
            let handle = CoordRegionRaftHandle::from_runtime(&rt);
            let leader = handle.current_leader().await;

            // size/keys：redb 文件 stat + 表 len；storage_bytes：本 Region
            // chunk 存储（coord.storage 数据面，`objects/` 目录）用量——若对象
            // 存储未启用则 0。移到阻塞池，避免阻塞 worker。
            let mvcc = Arc::clone(&rt.mvcc);
            let chunk_store = rt.chunk_store.clone();
            let (size, keys, storage_bytes) = match tokio::task::spawn_blocking(move || {
                let b = mvcc.backend();
                let size = b.disk_size_bytes();
                let keys = b.key_count();
                let storage = match &chunk_store {
                    Some(store) => store.usage_bytes(),
                    None => Ok(0),
                };
                (size, keys, storage)
            })
            .await
            {
                Ok((Ok(size), Ok(keys), Ok(storage))) => (size, keys, storage),
                Ok(_) | Err(_) => {
                    tracing::warn!("PD heartbeat: region {region_id} stats unavailable");
                    (0, 0, 0)
                }
            };

            if let Err(e) = driver.handle_region_heartbeat(
                *region_id,
                size,
                keys,
                storage_bytes,
                leader.unwrap_or(0),
            ) {
                tracing::warn!("PD heartbeat region {region_id}: {e}");
            }

            reconcile_region_members(&driver, &rt, &handle).await;
        }
    }
}

/// 把本节点 PD 元数据与 RegionHandle.meta 的 voter 集收敛到 raft 已提交成员。
///
/// 成员变更经 raft 日志复制到全部节点——对账使各节点 PD meta 与 raft 一致
/// （即 PD meta 的跨节点一致）。无已提交成员（初始化前）或 raft 与存储一致时
/// 为空操作。
async fn reconcile_region_members(
    driver: &PlacementDriver,
    rt: &Arc<RegionRuntime>,
    handle: &dyn RegionRaftHandle,
) {
    let members = match handle.current_members().await {
        Ok(m) if !m.is_empty() => m,
        _ => return, // 无已提交成员（集群初始化 / follower 未追平）→ 跳过
    };
    let region_id = rt.region_id;

    let Some(stored) = driver.meta_store().get_region(region_id) else {
        return;
    };

    let mut stored_voters: Vec<NodeID> = stored
        .peers
        .iter()
        .filter(|p| p.role == PeerRole::Voter)
        .map(|p| p.node_id)
        .collect();
    stored_voters.sort_unstable();
    let raft_voters: Vec<NodeID> = members
        .iter()
        .filter(|p| p.role == PeerRole::Voter)
        .map(|p| p.node_id)
        .collect();

    if stored_voters == raft_voters {
        return;
    }

    // raft 为准：写穿 meta_store（调度真源）+ 同步 RegionHandle.meta
    let voter_peers: Vec<Peer> = members
        .iter()
        .filter(|p| p.role == PeerRole::Voter)
        .cloned()
        .collect();
    let mut meta = stored.clone();
    meta.peers = voter_peers;
    meta.epoch.conf_ver += 1;
    if let Err(e) = driver.meta_store().update_region(meta.clone()) {
        tracing::warn!("PD: reconcile region {region_id} meta persist failed: {e}");
        return;
    }
    {
        let mut hmeta = rt.handle.meta.write();
        hmeta.peers = meta.peers.clone();
        hmeta.epoch.conf_ver = meta.epoch.conf_ver;
    }
    tracing::info!(
        "PD: region {region_id} meta reconciled to raft voters {raft_voters:?} \
         (conf_ver -> {})",
        meta.epoch.conf_ver
    );
}

// ============================================================================
// 单元测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_region_seed_creates_and_preserves() {
        let dir = tempfile::tempdir().unwrap();
        let cluster: Vec<Peer> = (1..=3)
            .map(|id| Peer {
                node_id: id,
                raft_addr: format!("127.0.0.1:5{:04}", id),
                role: PeerRole::Voter,
            })
            .collect();
        let seeds = vec![RegionSeed {
            region_id: 1,
            start_key: vec![],
            end_key: vec![],
        }];

        // 首次：创建 + 集群 voter；随后 operator 变更（peers 收缩 {1,2}）
        let persisted = {
            let store = Arc::new(PdMetaStore::open(dir.path()).unwrap());
            seed_regions(&store, &seeds, &cluster).unwrap();
            let mut meta = store.get_region(1).unwrap();
            assert_eq!(meta.peers.len(), 3);
            meta.peers = cluster[..2].to_vec();
            meta.epoch.conf_ver += 1;
            store.update_region(meta.clone()).unwrap();
            meta
        };
        assert_eq!(persisted.peers.len(), 2);

        // 重启播种保留（store 已释放，可重开同一 redb 文件）
        let store2 = Arc::new(PdMetaStore::open(dir.path()).unwrap());
        seed_regions(&store2, &seeds, &cluster).unwrap();
        let after = store2.get_region(1).unwrap();
        assert_eq!(after.peers.len(), 2, "persisted peers must survive re-seed");
        assert_eq!(after.epoch.conf_ver, 2);
    }

    #[test]
    fn test_region_seed_fixes_key_range_drift() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(PdMetaStore::open(dir.path()).unwrap());
        let cluster: Vec<Peer> = vec![Peer {
            node_id: 1,
            raft_addr: "127.0.0.1:5001".into(),
            role: PeerRole::Voter,
        }];
        let mut meta = RegionMeta {
            region_id: 1,
            start_key: b"a".to_vec(),
            end_key: vec![],
            epoch: RegionEpoch::initial(),
            peers: cluster.clone(),
            approximate_size: 0,
            approximate_keys: 0,
        };
        store.create_region(meta.clone()).unwrap();

        // 配置说 region 1 应覆盖 [∅,∅)——播种修正 range、保留 peers
        let seeds = vec![RegionSeed {
            region_id: 1,
            start_key: vec![],
            end_key: vec![],
        }];
        seed_regions(&store, &seeds, &cluster).unwrap();
        meta = store.get_region(1).unwrap();
        assert!(meta.start_key.is_empty());
        assert!(meta.end_key.is_empty());
        assert_eq!(meta.peers.len(), 1);
    }
}
