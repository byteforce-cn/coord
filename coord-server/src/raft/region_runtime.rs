// Region 运行时装配（T2.3 生产接线）
//
// 生产形态：单进程（coord server）承载多个 Region Raft 组。本模块把
// 「每 Region 的存储（MvccStorage / LogStore / SnapshotTracker）→ per-region
// Raft 实例 → 网络注册（RaftRpcService.set_region_raft）→ 路由注册
// （RegionManager）」的装配封装为可复用 API，供 main.rs（region 配置落地后，
// 见 T3.4）与集成测试共用。
//
// 存储隔离策略（T2.3 决策：目录级隔离，而非把 `/r/{region_id:016x}/...`
// 前缀写进共享 redb Key）：
//   1. redb 为单写者模型：若 N 个 Region 共享同一 Database，各 Region Raft 组
//      并发 append/apply 会产生交叠写事务，需全局串行化（WriteBatcher 仅覆盖
//      log append 路径，apply 路径无等价物），改造面与回归风险大；
//   2. region 0 = 既有单 Raft 生产数据（`store.db` 平铺 Key），前缀化会改变磁盘
//      布局，破坏既有备份/快照/回滚（T2.6：multi_raft 关闭时需字节级退化）；
//   3. 每 Region 独立 DB 文件天然满足「日志/状态机按 Region 隔离」，且无需在
//      MVCC/LogStore 全链路透传 region_id。
// 目录约定（与 coord_core::region 前缀文档一一对应，物理实现等价物）：
//   region 0   → `<data_dir>`（legacy 单 Raft 布局，字节级不变）
//   region ≥1  → `<data_dir>/regions/region-{region_id:016x}/`

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use coord_core::error::{Error, Result};
use coord_core::storage::StorageBackend;
use coord_core::types::{NodeID, RegionId, RegionMeta, StorageConfig};

use crate::raft::log_store::LogStore;
use crate::raft::network::{RaftNetworkFactoryImpl, RaftRpcService, RegionRaftNetworkFactory};
use crate::raft::region::RegionHandle;
use crate::raft::state_machine::StateMachineStore;
use crate::raft::{new_basic_node, new_raft, CoordRaft, RaftConfig, RaftNode};
use crate::storage::mvcc::MvccStorage;
use crate::storage::redb_backend::RedbBackend;
use crate::storage::snapshot::SnapshotTracker;

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
pub struct RegionRuntimeSpec {
    /// Region 元数据（id / key range / epoch / peers）
    pub meta: RegionMeta,
    /// Region 存储数据目录（由 `region_data_dir` 计算；无需预先创建）
    pub data_dir: PathBuf,
    /// Raft 心跳/选举等运行参数（R-RFT-19）
    pub raft_config: Arc<RaftConfig>,
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
    /// 与 `main.rs` 单 Raft 的 `node_raft_log` 同理——T2.4 中 KV 读屏障的
    /// 幻影态终检需按 Region 访问各自日志，LogStore 为 Clone 廉价句柄）
    pub raft_log_store: LogStore,
    /// 该 Region 的数据目录
    pub data_dir: PathBuf,
    /// 快照跟踪器（与 LogStore/StateMachineStore 共享）
    pub tracker: Arc<SnapshotTracker>,
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
    } = spec;

    std::fs::create_dir_all(&data_dir).map_err(|e| {
        Error::Storage(format!(
            "create region {region_id} data dir {}: {e}",
            data_dir.display()
        ))
    })?;

    // 业务存储：store.db（KV/元数据/changelog）+ raft-log/log.db + snapshots/
    let storage_config = StorageConfig::default();
    let backend = RedbBackend::open(&data_dir, &storage_config).map_err(|e| {
        Error::Storage(format!("open region {region_id} store: {e}"))
    })?;
    let mvcc = Arc::new(MvccStorage::new(backend).map_err(|e| {
        Error::Storage(format!("create region {region_id} mvcc: {e}"))
    })?);
    let tracker = Arc::new(SnapshotTracker::default());
    let log_store = LogStore::new(&data_dir)
        .await
        .map_err(|e| Error::Storage(format!("open region {region_id} raft log: {e}")))?
        .with_snapshot_tracker(Arc::clone(&tracker));
    let sm_store = StateMachineStore::new(
        Arc::clone(&mvcc),
        data_dir.join("snapshots"),
        Arc::clone(&tracker),
    );

    // T2.4：读屏障幻影态终检需要访问本地日志；克隆一份 LogStore 句柄给
    // RegionRuntime（与 main.rs 单 Raft 的 node_raft_log 同理），再移入 raft。
    let raft_log_store = log_store.clone();

    // 重启引导（与 main.rs 单 Raft 的 already_initialized 守卫同理）：bootstrap
    // 节点重启时 Region 日志已持久化，重复 initialize 会报错——仅在日志未初始化
    // 时才需要 initialize（首次启动）；非首次启动靠既有日志 + leader 复制收敛。
    let already_initialized = log_store.is_initialized().map_err(|e| {
        Error::Storage(format!("region {region_id}: read initialized state: {e}"))
    })?;

    // T2.2：per-region 网络门面（共享节点连接池 + 绑定 region_id）
    let region_factory = RegionRaftNetworkFactory::new(shared_factory.clone(), region_id);
    let raft = new_raft(node_id, raft_config, region_factory, log_store, sm_store)
        .await
        .map_err(|e| Error::Internal(format!("create region {region_id} raft: {e}")))?;

    // 成员初始化（仅 bootstrap 节点，且仅当本 Region 日志尚未初始化）
    if initialize && !already_initialized {
        let members: BTreeMap<u64, RaftNode> = meta
            .voter_peers()
            .map(|p| (p.node_id, new_basic_node(&p.raft_addr)))
            .collect();
        if members.is_empty() {
            return Err(Error::InvalidArgument(format!(
                "region {region_id} has no voter peers; cannot initialize"
            )));
        }
        raft.initialize(members).await.map_err(|e| {
            Error::Internal(format!("initialize region {region_id}: {e}"))
        })?;
    }

    // T2.5：网络注册（RaftRpcService 按 region_id 解复用）
    rpc.set_region_raft(region_id, raft.clone());

    Ok(Arc::new(RegionRuntime {
        region_id,
        handle,
        raft,
        mvcc,
        raft_log_store,
        data_dir,
        tracker,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_region_data_dir_layout() {
        let base = Path::new("/data/coord");

        // region 0 = legacy 根目录（字节级不变）
        assert_eq!(
            region_data_dir(base, 0),
            PathBuf::from("/data/coord")
        );

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
