// Region 运行时装配
//
// 生产形态：单进程（coord server）承载多个 Region Raft 组。本模块把
// 「每 Region 的存储（MvccStorage / LogStore / SnapshotTracker）→ per-region
// Raft 实例 → 网络注册（RaftRpcService.set_region_raft）→ 路由注册
// （RegionManager）」的装配封装为可复用 API，供 main.rs 与集成测试共用。
//
// 存储隔离策略（目录级隔离，而非把 `/r/{region_id:016x}/...`
// 前缀写进共享 redb Key）：
//   1. redb 为单写者模型：若 N 个 Region 共享同一 Database，各 Region Raft 组
//      并发 append/apply 会产生交叠写事务，需全局串行化（WriteBatcher 仅覆盖
//      log append 路径，apply 路径无等价物），改造面与回归风险大；
//   2. region 0 = 既有单 Raft 生产数据（`store.db` 平铺 Key），前缀化会改变磁盘
//      布局，破坏既有备份/快照/回滚（multi_raft 关闭时需字节级退化）；
//   3. 每 Region 独立 DB 文件天然满足「日志/状态机按 Region 隔离」，且无需在
//      MVCC/LogStore 全链路透传 region_id。
// 目录约定（与 coord_core::region 前缀文档一一对应，物理实现等价物）：
//   region 0   → `<data_dir>`（legacy 单 Raft 布局，字节级不变）
//   region ≥1  → `<data_dir>/regions/region-{region_id:016x}/`

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use coord_core::error::{Error, Result};
use coord_core::storage::StorageBackend;
use coord_core::types::{NodeID, Peer, PeerRole, RegionId, RegionMeta, StorageConfig};

use crate::raft::log_store::LogStore;
use crate::raft::network::{RaftNetworkFactoryImpl, RaftRpcService, RegionRaftNetworkFactory};
use crate::raft::region::RegionHandle;
use crate::raft::state_machine::StateMachineStore;
use crate::raft::type_config::{Command, Response};
use crate::raft::{new_basic_node, new_raft, CoordRaft, RaftConfig, RaftNode, WatchReceiver};
use crate::storage::compaction::CompactProposer;
use crate::storage::mvcc::{MvccStorage, RevisionMode};
use crate::storage::object_store::{ChunkStore, ObjectStoreCtx};
use crate::storage::redb_backend::RedbBackend;
use crate::storage::snapshot::SnapshotTracker;
use crate::watch::WatchDispatcher;

/// 节点数据目录 → Region 存储数据目录
///
/// - region 0：节点根目录（legacy 单 Raft 布局：`store.db` / `raft-log/` / `snapshots/`）
/// - region ≥1：`<base>/regions/region-{region_id:016x}/`（目录级前缀隔离，
///   与 coord_core::region 的 key 前缀 `/r/{region_id:016x}/` 一一对应）
pub fn region_data_dir(base_data_dir: &Path, region_id: RegionId) -> PathBuf {
    if region_id == 0 {
        base_data_dir.to_path_buf()
    } else {
        base_data_dir
            .join("regions")
            .join(format!("region-{region_id:016x}"))
    }
}

/// 单个 Region 的装配描述
#[derive(Clone)]
pub struct RegionRuntimeSpec {
    /// Region 元数据（id / key range / epoch / peers）
    pub meta: RegionMeta,
    /// Region 存储数据目录（由 `region_data_dir` 计算；无需预先创建）
    pub data_dir: PathBuf,
    /// Raft 心跳/选举等运行参数（R-RFT-19）
    pub raft_config: Arc<RaftConfig>,
    /// 对象存储启用配置（`Some` = 启用：本 Region 数据目录下创建 chunk 文件
    /// 存储并挂到状态机/运行时；`None` = 未启用，布局不变）。
    pub object_store: Option<Arc<ObjectStoreCtx>>,
    /// revision 语义：v1 = `LogIndex`；`[kv_t].enabled=true` 的 v2 集群 = `CommitTs`。
    pub revision_mode: RevisionMode,
}

/// 单节点内某个 Region 的运行时句柄
///
/// 持有装配后的 Raft 实例与业务存储；`handle` 与 RegionManager 路由表共享
/// （同一 Arc<RegionHandle>，role/epoch 状态一致）。
pub struct RegionRuntime {
    /// Region ID
    pub region_id: RegionId,
    /// 路由/元数据句柄（与 RegionManager 路由表共享同一实例）
    pub handle: Arc<RegionHandle>,
    /// Raft 实例（client_write / read_index 入口）
    pub raft: CoordRaft,
    /// 该 Region 的业务存储（MVCC，目录隔离）
    pub mvcc: Arc<MvccStorage<RedbBackend>>,
    /// 该 Region 的本地 Raft Log 句柄（读路径一致性校验用，防陈旧读；
    /// 与 `main.rs` 单 Raft 的 `node_raft_log` 同理——KV 读屏障的
    /// 幻影态终检需按 Region 访问各自日志，LogStore 为 Clone 廉价句柄）
    pub raft_log_store: LogStore,
    /// 该 Region 的数据目录
    pub data_dir: PathBuf,
    /// 快照跟踪器（与 LogStore/StateMachineStore 共享）
    pub tracker: Arc<SnapshotTracker>,
    /// 该 Region 的 Watch 事件分发器（per-Region revision 语义；
    /// 与状态机共享——apply 时把本 Region 的变更事件 dispatch 到这里，订阅者只
    /// 见本 Region 的 key；region 0 单 Raft 路径不用此字段，沿用节点级 dispatcher）
    pub watch_dispatcher: Arc<WatchDispatcher>,
    /// 对象存储 chunk 文件存储（本 Region 数据目录 `objects/` 下；对象服务按
    /// 对象 manifest key 路由到 Region 后从这里读写 chunk；None = 未启用）
    pub chunk_store: Option<Arc<ChunkStore>>,
}

impl RegionRuntime {
    /// Region ID
    pub fn region_id(&self) -> RegionId {
        self.region_id
    }

    /// 路由句柄
    pub fn handle(&self) -> Arc<RegionHandle> {
        Arc::clone(&self.handle)
    }

    /// Region 元数据快照
    pub fn meta(&self) -> RegionMeta {
        self.handle.meta.read().clone()
    }
}

/// 装配一个 Region 的运行时（存储 + Raft + 网络注册）
///
/// 由 `RegionManager::spawn_region` 调用（负责路由注册）；也可独立使用
/// （调用方自行 register_region）。`handle` 必须已由调用方建好（与路由表共享）。
///
/// - `initialize=true`：本节点为 bootstrap，用 region meta 的 voter peers 初始化
///   成员（仅集群首个节点）；其余节点传 `false`，靠复制追赶。
pub async fn spawn_region_runtime(
    node_id: NodeID,
    shared_factory: &RaftNetworkFactoryImpl,
    rpc: &RaftRpcService,
    spec: RegionRuntimeSpec,
    handle: Arc<RegionHandle>,
    initialize: bool,
) -> Result<Arc<RegionRuntime>> {
    let region_id = spec.meta.region_id;
    let RegionRuntimeSpec {
        meta,
        data_dir,
        raft_config,
        object_store,
        revision_mode,
    } = spec;

    std::fs::create_dir_all(&data_dir).map_err(|e| {
        Error::Storage(format!(
            "create region {region_id} data dir {}: {e}",
            data_dir.display()
        ))
    })?;

    // 对象存储 chunk 文件存储（惰性建目录；关闭时不产生任何布局变化）
    let chunk_store = match object_store {
        Some(ctx) => Some(ChunkStore::new(
            &data_dir,
            Arc::clone(&ctx.limits),
            ctx.encryption_root_key_hex.as_deref(),
        )?),
        None => None,
    };

    // 业务存储：store.db（KV/元数据/changelog）+ raft-log/log.db + snapshots/
    let storage_config = StorageConfig::default();
    let backend = RedbBackend::open(&data_dir, &storage_config)
        .map_err(|e| Error::Storage(format!("open region {region_id} store: {e}")))?;
    let mvcc = Arc::new(
        MvccStorage::new_with_revision_mode(backend, revision_mode)
            .map_err(|e| Error::Storage(format!("create region {region_id} mvcc: {e}")))?,
    );
    let tracker = Arc::new(SnapshotTracker::default());
    let mut log_store = LogStore::new(&data_dir)
        .await
        .map_err(|e| Error::Storage(format!("open region {region_id} raft log: {e}")))?
        .with_snapshot_tracker(Arc::clone(&tracker));
    let mut sm_store = StateMachineStore::new(
        Arc::clone(&mvcc),
        data_dir.join("snapshots"),
        Arc::clone(&tracker),
    );

    // 每 Region 独立 WatchDispatcher（per-Region revision 语义）。
    // 必须在 new_raft 之前挂到状态机——StateMachineStore 被移入 raft 后不可再取回。
    let watch_dispatcher = Arc::new(WatchDispatcher::start());
    sm_store.set_watch_dispatcher(Arc::clone(&watch_dispatcher));

    // 对象存储：chunk 文件存储同样必须在 new_raft 之前挂到状态机（apply 路径用）
    if let Some(store) = &chunk_store {
        sm_store.set_object_chunk_store(Some(Arc::clone(store)));
    }

    // 读屏障幻影态终检需要访问本地日志；克隆一份 LogStore 句柄给
    // RegionRuntime（与 main.rs 单 Raft 的 node_raft_log 同理），再移入 raft。
    let raft_log_store = log_store.clone();

    // 重启引导（与 main.rs 单 Raft 的守卫同理）：只对**没有任何持久化 raft
    // 状态**的组 initialize。`is_initialized()` 只看 committed 行，会把
    // 「initialize 过、但 quorum 未上线因而从未提交」的空组误判成新组，重启时
    // 重复 initialize 被 openraft 拒绝并留下持有 store 锁的 core（同进程重试撞
    // `Database already open`）。判据见 `LogStore::has_prior_state`。
    let has_prior_state = log_store
        .has_prior_state()
        .await
        .map_err(|e| Error::Storage(format!("region {region_id}: read raft state: {e}")))?;

    // per-region 网络门面（共享节点连接池 + 绑定 region_id）
    let region_factory = RegionRaftNetworkFactory::new(shared_factory.clone(), region_id);
    let raft = new_raft(node_id, raft_config, region_factory, log_store, sm_store)
        .await
        .map_err(|e| Error::Internal(format!("create region {region_id} raft: {e}")))?;

    // 成员初始化（仅 bootstrap 节点，且仅当本 Region 没有任何 raft 状态）
    if initialize && !has_prior_state {
        let members: BTreeMap<u64, RaftNode> = meta
            .voter_peers()
            .map(|p| (p.node_id, new_basic_node(&p.raft_addr)))
            .collect();
        if members.is_empty() {
            // 失败前必须释放 store 锁：否则调用方重试同一目录会撞 `Database
            // already open`（本函数是装配入口，重试由上层按拍执行）。
            let _ = raft.shutdown().await;
            return Err(Error::InvalidArgument(format!(
                "region {region_id} has no voter peers; cannot initialize"
            )));
        }
        if let Err(err) = raft.initialize(members).await {
            // 对端已建组/并发初始化赢了：按「等 leader 复制」继续，不是错误。
            if matches!(
                err,
                openraft::errors::RaftError::APIError(
                    openraft::error::InitializeError::NotAllowed(_)
                )
            ) {
                tracing::debug!(
                    region_id,
                    "region group was initialized elsewhere before this node; waiting for replication"
                );
            } else {
                let _ = raft.shutdown().await;
                return Err(Error::Internal(format!(
                    "initialize region {region_id}: {err}"
                )));
            }
        }
    }

    // 网络注册（RaftRpcService 按 region_id 解复用）
    rpc.set_region_raft(region_id, raft.clone());

    Ok(Arc::new(RegionRuntime {
        region_id,
        handle,
        raft,
        mvcc,
        raft_log_store,
        data_dir,
        tracker,
        watch_dispatcher,
        chunk_store,
    }))
}

// ============================================================================
// Region raft 成员变更能力面（PD Operator 执行器依赖端口）
// ============================================================================

/// Region raft 成员变更能力面
///
/// 定义在 raft 层（raft 自我描述能力；全部 openraft 交互收敛在本目录内——
/// openraft 类型隔离），pd 模块的 `OperatorExecutor` 只经 trait object 使用本端口，
/// 不接触任何 openraft 类型。接线层/集成测试基于真实 `CoordRaft`
/// 实现（`CoordRegionRaftHandle`），单元测试可用替身。
///
/// 语义约定（与执行器对齐，见 `pd/executor.rs` 模块文档）：
/// - `add_learner`：将节点作为 Learner 加入并**等待复制追平**（blocking=true，
///   对齐单 Raft `join`/`member_add` 的成员变更语义）；
/// - `promote_to_voter`：Learner → Voter（`change_membership(AddVoterIds)`）；
/// - `remove_voter`：移除 Voter（`change_membership(RemoveVoters)`）；
/// - `transfer_leader`：发起 leader 转移（异步完成，调用方轮询 `current_leader`）。
#[async_trait]
pub trait RegionRaftHandle: Send + Sync {
    /// 当前 leader（选举窗口/未知 = None）
    async fn current_leader(&self) -> Option<NodeID>;

    /// 当前**已提交/已 apply** 的成员表（对账用）
    ///
    /// 返回 raft 状态机当前 membership 的全部节点（voter 优先、按 node_id
    /// 稳定排序），`raft_addr` 取成员节点表中自带的地址。成员经 raft 日志复制，
    /// 是所有节点 PD 元数据收敛的对账真源；尚未有已提交成员（集群初始化 /
    /// follower 未追平）时返回空 Vec（调用方应跳过对账，不得据此清空元数据）。
    async fn current_members(&self) -> Result<Vec<Peer>>;

    /// 添加 Learner（blocking：等待复制追平后返回）
    async fn add_learner(&self, node_id: NodeID, raft_addr: &str) -> Result<()>;

    /// 晋升为 Voter
    async fn promote_to_voter(&self, node_id: NodeID) -> Result<()>;

    /// 移除 Voter
    async fn remove_voter(&self, node_id: NodeID) -> Result<()>;

    /// 发起 Leader 转移（异步完成）
    async fn transfer_leader(&self, to: NodeID) -> Result<()>;
}

/// 真实 Region raft 的成员变更句柄
///
/// 包装某 Region 的 `CoordRaft`，把 openraft 成员变更 API 收敛到
/// `RegionRaftHandle` 端口；错误映射为 `coord_core::error::Error`。
pub struct CoordRegionRaftHandle {
    raft: CoordRaft,
}

impl CoordRegionRaftHandle {
    /// 从 Region raft 实例构建
    pub fn new(raft: CoordRaft) -> Self {
        Self { raft }
    }

    /// 从 Region 运行时构建（`CoordRaft` Clone 为 Arc bump，廉价）
    pub fn from_runtime(rt: &RegionRuntime) -> Self {
        Self {
            raft: rt.raft.clone(),
        }
    }
}

#[async_trait]
impl RegionRaftHandle for CoordRegionRaftHandle {
    async fn current_leader(&self) -> Option<NodeID> {
        self.raft.current_leader().await
    }

    async fn current_members(&self) -> Result<Vec<Peer>> {
        let m = self.raft.metrics().borrow_watched().clone();
        let membership = &m.membership_config;
        // 无已提交成员（初始化前）→ 空表，调用方跳过对账
        let voter_ids: std::collections::BTreeSet<u64> = membership.voter_ids().collect();
        if voter_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut peers: Vec<Peer> = membership
            .nodes()
            .map(|(id, node)| Peer {
                node_id: *id,
                raft_addr: node.addr.clone(),
                role: if voter_ids.contains(id) {
                    PeerRole::Voter
                } else {
                    PeerRole::Learner
                },
            })
            .collect();
        // voter 优先、按 node_id 稳定排序（对账用确定性顺序）
        peers.sort_by_key(|p| (if p.role == PeerRole::Voter { 0 } else { 1 }, p.node_id));
        Ok(peers)
    }

    async fn add_learner(&self, node_id: NodeID, raft_addr: &str) -> Result<()> {
        let node = new_basic_node(raft_addr);
        self.raft
            .add_learner(node_id, node, true)
            .await
            .map_err(|e| Error::Internal(format!("region raft add_learner node {node_id}: {e}")))?;
        Ok(())
    }

    async fn promote_to_voter(&self, node_id: NodeID) -> Result<()> {
        let mut ids = BTreeSet::new();
        ids.insert(node_id);
        self.raft
            .change_membership(crate::raft::add_voter_ids(ids), true)
            .await
            .map_err(|e| {
                Error::Internal(format!("region raft promote node {node_id} to voter: {e}"))
            })?;
        Ok(())
    }

    async fn remove_voter(&self, node_id: NodeID) -> Result<()> {
        let mut ids = BTreeSet::new();
        ids.insert(node_id);
        self.raft
            .change_membership(crate::raft::remove_voter_ids(ids), true)
            .await
            .map_err(|e| Error::Internal(format!("region raft remove voter {node_id}: {e}")))?;
        Ok(())
    }

    async fn transfer_leader(&self, to: NodeID) -> Result<()> {
        // alpha.34：transfer_leader 在 Trigger 上（Raft::trigger() 门面）
        self.raft
            .trigger()
            .transfer_leader(to)
            .await
            .map_err(|e| Error::Internal(format!("region raft transfer leader to {to}: {e}")))?;
        Ok(())
    }
}

// ============================================================================
// per-Region Compact 提案器
// ============================================================================

/// per-Region Compaction 提案器
///
/// 把 `Command::Compact{revision}` 提到该 Region 的 raft（节点一致 apply，
/// 与单 Raft 的 `CoordNode` CompactProposer 同语义）。leader-only：
/// `can_propose` = 本节点是该 Region raft 的当前 leader。提案成功后推进
/// `RegionHandle::compaction_watermark`（水位必须真实接线推进）。
///
/// 由接线层（main.rs / 集成测试）为每个 RegionRuntime 构造，
/// 供 `CompactionManager::start(region_mvcc, cfg, Some(proposer), metrics)` 使用。
pub struct RegionCompactProposer {
    node_id: NodeID,
    raft: CoordRaft,
    handle: Arc<RegionHandle>,
}

impl RegionCompactProposer {
    /// 从 RegionRuntime 构造（捕获 node_id、raft 与路由句柄）
    pub fn from_runtime(node_id: NodeID, rt: &RegionRuntime) -> Self {
        Self {
            node_id,
            raft: rt.raft.clone(),
            handle: Arc::clone(&rt.handle),
        }
    }
}

#[async_trait]
impl CompactProposer for RegionCompactProposer {
    async fn can_propose(&self) -> bool {
        self.raft.current_leader().await == Some(self.node_id)
    }

    async fn propose(&self, revision: u64) -> std::result::Result<u64, String> {
        let resp = self
            .raft
            .client_write(Command::Compact { revision })
            .await
            .map_err(|e| format!("region raft compact propose failed: {e}"))?;
        match resp.response() {
            Response::Compact { compacted_revision } => {
                // 推进 per-Region compact 水位（必须真实接线，不得是预留空转）
                self.handle
                    .compaction_watermark
                    .store(*compacted_revision, std::sync::atomic::Ordering::Relaxed);
                Ok(*compacted_revision)
            }
            other => Err(format!("unexpected compact response: {other:?}")),
        }
    }
}

// ============================================================================
// split/merge 分页导入端口
// ============================================================================

/// 经目标 Region raft 导入一页 split/merge 搬迁数据。
///
/// 只把 `Command::ImportRange` / `Command::ImportRangePreserveHistory` /
/// `Command::ClearImportRange` 提案给目标 Region 的 raft leader：导入必须
/// 成为日志条目，副本/快照/压缩才对同一状态机收敛。目标组的 leader 可能不在
/// 本节点（预创建时成员是完整副本集、选举任意）——`client_write` 只在 leader
/// 本地可提案，所以装配了 forwarder 时把命令经 `SubmitRegionCommand` 提到
/// leader 节点；未装配（单节点/测试）时直接本地提案。
pub struct CoordRangeImportTarget {
    raft: CoordRaft,
    /// 目标 Region ID（跨节点转发 `SubmitRegionCommand` 的解复用键）。
    region_id: RegionId,
    /// 目标组副本地址：新组的成员未必在启动期静态表里，转发前灌进工厂。
    peers: Vec<(NodeID, String)>,
    /// 节点间 raft RPC 工厂 + 本节点 ID；`None` = 单节点/测试装配。
    forwarder: Option<RegionCommandForwarder>,
    /// 等待目标组选出 leader 的上界（0 = 不等待，直接提案）。
    ///
    /// split 目标由 lifecycle 异步预创建：组刚装配时还没有 leader，`client_write`
    /// 会立刻失败。生产接线给出有界等待；建组失败/永不出 leader 仍会失败
    /// （intent 保留重试）。
    leader_wait: Duration,
}

/// 数据 Region 内部命令的跨节点提案器。
#[derive(Clone)]
pub struct RegionCommandForwarder {
    /// 本节点 ID（leader 在本节点时走本地 `client_write`）。
    pub node_id: NodeID,
    pub factory: RaftNetworkFactoryImpl,
}

/// 转发提案的重试预算/间隔。
///
/// 换届窗口里 leader 视图会短暂滞后（选举超时通常 ≥ 1s），所以按**截止时间**
/// 重试而不是固定次数；导入/清理命令 apply 幂等，重放安全。
const FORWARD_RETRY_BUDGET: Duration = Duration::from_secs(10);
const FORWARD_RETRY_POLL: Duration = Duration::from_millis(150);

impl CoordRangeImportTarget {
    /// 从 Region raft 实例构建
    pub fn from_raft(raft: CoordRaft) -> Self {
        Self {
            raft,
            region_id: 0,
            peers: Vec::new(),
            forwarder: None,
            leader_wait: Duration::ZERO,
        }
    }

    /// 从 Region 运行时构建（`CoordRaft` Clone 为 Arc bump，廉价）
    pub fn from_runtime(rt: &RegionRuntime) -> Self {
        let meta = rt.meta();
        Self {
            raft: rt.raft.clone(),
            region_id: meta.region_id,
            peers: meta
                .peers
                .iter()
                .map(|peer| (peer.node_id, peer.raft_addr.clone()))
                .collect(),
            forwarder: None,
            leader_wait: Duration::ZERO,
        }
    }

    /// 目标 Region ID（`from_raft` 构不到元数据时由接线层补）。
    pub fn with_region_id(mut self, region_id: RegionId) -> Self {
        self.region_id = region_id;
        self
    }

    /// 装配跨节点提案（生产接线：目标 leader 可能不在本节点）。
    pub fn with_forwarder(mut self, node_id: NodeID, factory: RaftNetworkFactoryImpl) -> Self {
        self.forwarder = Some(RegionCommandForwarder { node_id, factory });
        self
    }

    /// 提案前等待 leader 选举的上界。
    pub fn with_leader_wait(mut self, wait: Duration) -> Self {
        self.leader_wait = wait;
        self
    }

    /// 把一条内部命令提到目标 Region 的 leader 上提出并等 apply。
    ///
    /// 本节点是 leader → 本地 `client_write`；否则经 `SubmitRegionCommand`
    /// 转发到 leader 节点。换届窗口有界重试（每次刷新 leader 视图）；重试耗尽
    /// 则整次迁移失败、intent 保留重试。
    async fn propose(&self, command: Command) -> std::result::Result<Response, String> {
        self.wait_for_leader().await?;
        let Some(forwarder) = self.forwarder.as_ref() else {
            return self.propose_local(command).await;
        };
        // 新组的成员地址来自权威表（可能不在启动期静态表里）。
        for (node_id, addr) in &self.peers {
            if !addr.is_empty() {
                forwarder.factory.register_node(*node_id, addr.clone());
            }
        }
        let mut last_err = "target region raft has no leader".to_string();
        let deadline = tokio::time::Instant::now() + FORWARD_RETRY_BUDGET;
        while tokio::time::Instant::now() < deadline {
            match self.raft.current_leader().await {
                Some(leader) if leader == forwarder.node_id => {
                    return self.propose_local(command).await;
                }
                Some(leader) => {
                    match forwarder
                        .factory
                        .submit_region_command(leader, self.region_id, command.clone())
                        .await
                    {
                        Ok(response) => return Ok(response),
                        Err(e) => {
                            tracing::warn!(
                                region_id = self.region_id,
                                leader,
                                error = %e,
                                "forwarding region command to the target leader failed; retrying"
                            );
                            last_err = e;
                        }
                    }
                }
                None => {
                    tracing::warn!(
                        region_id = self.region_id,
                        "target region has no leader; retrying the forwarded command"
                    );
                }
            }
            tokio::time::sleep(FORWARD_RETRY_POLL).await;
        }
        Err(format!(
            "target region {} command could not be proposed within {FORWARD_RETRY_BUDGET:?}: \
             {last_err}",
            self.region_id
        ))
    }

    /// 本地提案（本节点是 leader，或无转发装配的单节点模式）。
    async fn propose_local(&self, command: Command) -> std::result::Result<Response, String> {
        let resp = self
            .raft
            .client_write(command)
            .await
            .map_err(|e| format!("target region raft propose failed: {e}"))?;
        Ok(resp.response().clone())
    }

    /// 等本组选出 leader（有界）。返回 `None` = 已就绪。
    async fn wait_for_leader(&self) -> std::result::Result<(), String> {
        if self.leader_wait.is_zero() {
            return Ok(());
        }
        let started = tokio::time::Instant::now();
        while self.raft.current_leader().await.is_none() {
            if started.elapsed() >= self.leader_wait {
                return Err(format!(
                    "target region raft has no leader after {:?}",
                    self.leader_wait
                ));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok(())
    }
}

#[async_trait]
impl crate::split_merge::RangeImportTarget for CoordRangeImportTarget {
    async fn reset_range(&self, start: &[u8], end: &[u8]) -> std::result::Result<(), String> {
        let resp = self
            .propose(Command::ClearImportRange {
                start_key: start.to_vec(),
                end_key: end.to_vec(),
            })
            .await?;
        match resp {
            Response::ImportRangeCleared { .. } => Ok(()),
            other => Err(format!(
                "target region raft returned unexpected response: {other:?}"
            )),
        }
    }

    async fn import_range(
        &self,
        start: &[u8],
        end: &[u8],
        rows: Vec<crate::storage::mvcc::VersionedKv>,
        watermark: u64,
        seal_prior_history: bool,
        done: bool,
    ) -> std::result::Result<u64, String> {
        let start_key = start.to_vec();
        let end_key = end.to_vec();
        let command = if seal_prior_history {
            Command::ImportRange {
                start_key,
                end_key,
                rows,
                watermark,
                done,
            }
        } else {
            Command::ImportRangePreserveHistory {
                start_key,
                end_key,
                rows,
                watermark,
                done,
            }
        };
        let resp = self
            .propose(command)
            .await
            .map_err(|e| format!("target region raft import: {e}"))?;
        match resp {
            // 回显的 watermark 必须与本页一致：不一致说明目标 apply 的实现
            // 钉在了别的 revision 上，继续翻页会把两代快照拼进同一个 Region。
            Response::ImportRange {
                imported,
                watermark: echoed,
            } if echoed == watermark => Ok(imported),
            Response::ImportRange {
                watermark: echoed, ..
            } => Err(format!(
                "target region raft import echoed watermark {echoed}, expected {watermark}"
            )),
            other => Err(format!(
                "target region raft returned unexpected response: {other:?}"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_region_data_dir_layout() {
        let base = Path::new("/data/coord");

        // region 0 = legacy 根目录（字节级不变）
        assert_eq!(region_data_dir(base, 0), PathBuf::from("/data/coord"));

        // region ≥1 = <base>/regions/region-{region_id:016x}
        assert_eq!(
            region_data_dir(base, 1),
            PathBuf::from("/data/coord/regions/region-0000000000000001")
        );
        assert_eq!(
            region_data_dir(base, 0xABCD),
            PathBuf::from("/data/coord/regions/region-000000000000abcd")
        );

        // region_id 与 coord_core::region 前缀格式一致（16 位十六进制，小写零填充）
        let hex = format!("{:016x}", 0xABCD);
        let dir = region_data_dir(base, 0xABCD);
        assert!(dir.ends_with(format!("region-{hex}")));
    }
}
