// Legacy 单 Raft → Multi-Raft 数据迁移（R-MR-07 / T5.15/T5.16）
//
// 目标：存量单 Raft（multi_raft.enabled=false，用户 KV 全在 region 0 根目录
// store）升级到 Multi-Raft（用户 KV 按 key range 落入各数据 Region）时，把
// region 0 根 store 中的 **用户 KV** 无损搬入各 Region raft，并留下迁移标记，
// 使 fail-closed 启动闸（T5.16）放行。
//
// 一致性前提（本模块的架构约束，2026-09-05 设计决策）：
// 本仓 revision ≡ raft log index（D-A2），MVCC 状态机是 raft 日志的确定性
// apply 结果。**离线直接注入 store 会破坏 日志↔状态机 等价**（新 follower /
// 快照 / 压缩 / per-Region watch 全部以日志为准，会与直写数据分叉）。
// 因此迁移**必须经 raft 日志复制**：数据写入目标 Region raft 的
// `Command::Put`（幂等覆盖），由 raft 复制到全部副本节点——日志与状态机
// 同步收敛，天然满足快照/复制/压缩一致性。
//
// 执行模型（每节点本地自主 + raft 幂等，无需跨节点 RPC）：
//   1. 每个 Region 恰有一个 leader；各节点只导入**自己当前是 leader** 的
//      Region（非 leader 节点跳过，靠 raft 复制获得数据）。leader 变更/重启
//      重导由 Put 幂等性兜底（同 key 同值覆盖，无副作用）。
//   2. 迁移源 = 本节点 region 0 根 store 的**活用户 KV**（`/_sys/*`、
//      `/_lease/*` 系统前缀除外——它们属 region 0 system raft，原样保留）。
//   3. 本地完备性闸：本节点**全部** Region store 均含期望 key 集后才算完成
//      （follower 侧 Region 靠复制追平）。根 raft leader 只在其本地完备后写
//      迁移标记——标记经 region 0 raft 复制（各节点一致、持久），后续启动
//      据此跳过迁移。任一 Region 数据未达 quorum → 标记永不被写 → 集群卡在
//      迁移态（fail-safe，不会出现"假完成"）。
//   4. 迁移**只读源、不删源数据**（T2.6 回滚 = 关闭 multi_raft 用 region 0
//      原数据字节级恢复）；迁移后的新写入只进 Region store（回滚丢增量，文档化）。
//
// 边界（v1，见 docs/multi-raft-limits.md）：迁移在停机窗口启动时执行（boot
// 阻塞至标记写入）；加密启用 + multi_raft 的组合不在 v1 范围（Region store
// 与 root store 共享 Barrier 密钥时本流程天然对称，未单独验证）。

use std::collections::BTreeMap;
use std::time::Duration;

use coord_core::error::Result;
use coord_core::types::NodeID;

use crate::raft::region::{RegionManager, RegionSeed};
use crate::raft::type_config::Command;
use crate::raft::{CoordRaft, RegionRuntime};
use crate::storage::mvcc::MvccStorage;
use crate::storage::redb_backend::RedbBackend;

/// 迁移标记 key（写入 region 0 system raft；存在 = 本节点已完成/已确认迁移）。
///
/// 放 `/_sys/migration/` 前缀下（system raft 管辖）；经 raft Put 复制到全部
/// 节点根 store，各节点一致且持久。
pub const MIGRATION_MARKER_KEY: &[u8] = b"/_sys/migration/legacy-v1";

/// 迁移标记值 = 完成迁移的节点 + unix 秒（审计用，内容不参与语义判断）
const MARKER_PREFIX: &[u8] = b"coord-legacy-migration ";

/// 系统保留前缀（region 0 system raft 数据，不迁入数据 Region）
fn is_system_key(key: &[u8]) -> bool {
    key.starts_with(b"/_sys/") || key.starts_with(b"/_lease/")
}

// ──── fail-closed 启动闸原语（T5.16）────

/// region 0 根 store 是否存在**待迁移的 legacy 用户 KV**（活 key；`/_sys/*`、
/// `/_lease/*` 系统数据不计）。marker 自身属 `/_sys/`，不会误判。
pub fn has_legacy_user_data(mvcc: &MvccStorage<RedbBackend>) -> Result<bool> {
    let live = mvcc.range(b"", usize::MAX)?;
    Ok(live.iter().any(|(k, _)| !is_system_key(k)))
}

/// 迁移标记是否已存在于 region 0 根 store
pub fn has_migration_marker(mvcc: &MvccStorage<RedbBackend>) -> Result<bool> {
    Ok(mvcc.get(MIGRATION_MARKER_KEY)?.is_some())
}

/// T5.16：fail-closed 启动闸决策。
///
/// 返回 `Some(原因)` = 应**拒绝启动**（multi_raft 开启但根 store 尚有未迁移
/// 的 legacy 用户 KV）；`None` = 放行。
///
/// - `has_user_data`：根 store 存在待迁移用户 KV（`has_legacy_user_data`）
/// - `migrated`：迁移标记已存在（此前完成过迁移）
/// - `migrate`：本次启动执行迁移（`[multi_raft].legacy_migration = true`）
/// - `allow_unmigrated`：强制放行（`[multi_raft].allow_unmigrated = true`，
///   救援用；不迁移直接以 region 0 现有数据 + 空 Region 启动，数据面由运维负责）
pub fn boot_gate_decision(
    has_user_data: bool,
    migrated: bool,
    migrate: bool,
    allow_unmigrated: bool,
) -> Option<String> {
    if !has_user_data || migrated {
        return None; // 无待迁移数据 / 已迁移 → 放行
    }
    if migrate || allow_unmigrated {
        return None; // 本次迁移 / 显式救援放行
    }
    Some(
        "root store contains unmigrated legacy user keys while multi_raft is enabled; \
         run migration first (set [multi_raft].legacy_migration = true and restart), \
         or rescue with [multi_raft].allow_unmigrated = true (data in regions will be empty)"
            .to_string(),
    )
}

// ──── 迁移执行 ────

/// key 是否属于 [start, end)（空 end = 无上界；空 start = keyspace 起点）
fn key_in_range(key: &[u8], start: &[u8], end: &[u8]) -> bool {
    key >= start && (end.is_empty() || key < end)
}

/// 扫描 region 0 根 store 的活用户 KV，按 seed key range 分组。
///
/// 返回 (region_id → 该 Region 期望的 key 集)；任何 key 不被 region 表覆盖
/// 即返回 Err（配置校验已保证平铺，此处防御）。
fn scan_legacy_by_region(
    root: &MvccStorage<RedbBackend>,
    seeds: &[RegionSeed],
) -> Result<BTreeMap<u64, BTreeMap<Vec<u8>, Vec<u8>>>> {
    let live = root.range(b"", usize::MAX)?;
    let mut by_region: BTreeMap<u64, BTreeMap<Vec<u8>, Vec<u8>>> = BTreeMap::new();
    for (key, value) in live {
        if is_system_key(&key) {
            continue; // 系统数据留在 region 0
        }
        let rid = seeds
            .iter()
            .find(|s| key_in_range(&key, &s.start_key, &s.end_key))
            .map(|s| s.region_id)
            .ok_or_else(|| {
                coord_core::error::Error::Internal(format!(
                    "migration: legacy key {:?} not covered by region table",
                    String::from_utf8_lossy(&key)
                ))
            })?;
        by_region.entry(rid).or_default().insert(key, value);
    }
    Ok(by_region)
}

/// 把某 Region 的期望 key 集经 raft `Command::Put` 导入（仅 leader 执行）。
///
/// - 本节点不是该 Region leader → 跳过（leader 所在节点负责导入，本节点靠复制）；
/// - 导入中途 leader 变更（client_write ForwardToLeader）→ 中止剩余（新 leader
///   幂等重导），由外层完备性闸兜底。
/// 返回实际写入条数（跳过/中止场景由完备性闸收敛，不报错）。
async fn import_region_keys(
    rt: &RegionRuntime,
    node_id: NodeID,
    keys: &BTreeMap<Vec<u8>, Vec<u8>>,
) -> Result<usize> {
    if rt.raft.current_leader().await != Some(node_id) {
        tracing::debug!(
            "migration: node {node_id} not leader of region {}; skip import (replicate)",
            rt.region_id
        );
        return Ok(0);
    }

    let mut written = 0usize;
    for (key, value) in keys {
        let cmd = Command::Put {
            key: key.clone(),
            value: value.clone(),
            lease_id: None, // 迁移不携带 legacy 租约绑定（v1 边界：见 limits）
        };
        match rt.raft.client_write(cmd).await {
            Ok(_) => written += 1,
            Err(e) => {
                // leader 已切换等暂时性错误：中止，交由完备性闸确认/重导
                tracing::warn!(
                    "migration: region {} put {:?} failed (leader moved?): {}",
                    rt.region_id,
                    String::from_utf8_lossy(key),
                    e
                );
                break;
            }
        }
    }
    tracing::info!(
        "migration: node {node_id} imported {written}/{} keys into region {}",
        keys.len(),
        rt.region_id
    );
    Ok(written)
}

/// 等待本节点全部 Region store 达到期望 key 集（本地完备性闸）。
///
/// follower 侧 Region 靠 raft 复制追平（由各自 leader 导入）；全部就绪后才
/// 允许根 raft leader 写迁移标记（fail-safe：任一 Region 未达 quorum → 超时
/// 失败，不产生"假完成"标记）。
async fn wait_local_completeness(
    manager: &RegionManager,
    expected: &BTreeMap<u64, BTreeMap<Vec<u8>, Vec<u8>>>,
    timeout: Duration,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let mut complete = true;
        for (rid, want) in expected {
            let Some(rt) = manager.runtime(*rid) else {
                complete = false;
                break;
            };
            let mut missing = 0usize;
            for key in want.keys() {
                if rt.mvcc.get(key).map_err(|e| {
                    coord_core::error::Error::Storage(format!(
                        "migration completeness check region {rid}: {e}"
                    ))
                })? == None
                {
                    missing += 1;
                }
            }
            if missing > 0 {
                complete = false;
                tracing::trace!(
                    "migration: region {rid} local store missing {missing} keys (catching up)"
                );
            }
        }
        if complete {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            let per: Vec<String> = expected
                .iter()
                .map(|(rid, want)| {
                    let have = manager
                        .runtime(*rid)
                        .map(|rt| {
                            rt.mvcc
                                .range(b"", usize::MAX)
                                .map(|kvs| kvs.len())
                                .unwrap_or(0)
                        })
                        .unwrap_or(0);
                    format!("region {rid}: have {have} / want {}", want.len())
                })
                .collect();
            return Err(coord_core::error::Error::Internal(format!(
                "migration: local completeness timeout after {:?} ({}); \
                 some region did not receive all keys — cluster may be \
                 under-replicated or a region leader is down",
                timeout,
                per.join(", ")
            )));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// 执行 boot 迁移（幂等；已标记则跳过）。
///
/// = [`import_legacy_to_regions`]（数据搬移 + 本地完备性）+
/// [`write_migration_marker`]（region 0 raft 标记）。
///
/// 返回 `true` = 本次迁移完成（标记已写入/确认）；`false` = 此前已迁移（跳过）。
/// 任一失败返回 Err（boot 应失败退出，fail-closed）。
pub async fn migrate_legacy_to_regions(
    node_id: NodeID,
    root: &MvccStorage<RedbBackend>,
    root_raft: &CoordRaft,
    manager: &RegionManager,
    seeds: &[RegionSeed],
) -> Result<bool> {
    // 1. 已迁移（标记存在）→ 跳过（幂等；重启后不再重复执行）
    if has_migration_marker(root)? {
        tracing::info!("migration: marker present; already migrated (skip)");
        return Ok(false);
    }

    import_legacy_to_regions(node_id, root, manager, seeds).await?;
    write_migration_marker(node_id, root, root_raft).await?;
    Ok(true)
}

/// 数据搬移 + 本地完备性（[`migrate_legacy_to_regions`] 前半段；无 marker 依赖，
/// 便于测试/分步执行）。
///
/// 各节点只导入自己当前是 leader 的 Region（Put 幂等兜底 leader 变更/重启）；
/// 完成后本节点全部 Region store 均含期望 key 集。
pub async fn import_legacy_to_regions(
    node_id: NodeID,
    root: &MvccStorage<RedbBackend>,
    manager: &RegionManager,
    seeds: &[RegionSeed],
) -> Result<()> {
    // 1. 扫描源数据并分组
    let by_region = scan_legacy_by_region(root, seeds)?;
    let total_keys: usize = by_region.values().map(|m| m.len()).sum();
    if total_keys == 0 {
        tracing::info!("migration: no legacy user keys to migrate");
    } else {
        tracing::info!(
            "migration: {total_keys} legacy user key(s) across {} region(s)",
            by_region.len()
        );
    }

    // 2. 每个 Region：等待 leader 出现；本节点是 leader 则导入（Put 幂等）
    for (rid, keys) in &by_region {
        let Some(rt) = manager.runtime(*rid) else {
            return Err(coord_core::error::Error::Internal(format!(
                "migration: region {rid} runtime not assembled on node {node_id}"
            )));
        };
        // 等待该 Region 选出 leader（迁移依赖 raft 网络就绪 + quorum）
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            match rt.raft.current_leader().await {
                Some(l) if l == node_id => {
                    import_region_keys(&rt, node_id, keys).await?;
                    break;
                }
                Some(_) => {
                    // 其他节点是 leader：它负责导入，本节点靠复制
                    tracing::debug!(
                        "migration: region {} led by another node; rely on replication",
                        *rid
                    );
                    break;
                }
                None => {
                    if tokio::time::Instant::now() >= deadline {
                        return Err(coord_core::error::Error::Internal(format!(
                            "migration: region {rid} has no leader after 30s; \
                             cannot import (is the full cluster booting?)"
                        )));
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    // 3. 本地完备性：全部 Region store 含期望 key 集（含 follower 侧复制追平）
    wait_local_completeness(manager, &by_region, Duration::from_secs(180)).await?;
    Ok(())
}

/// 写入/确认迁移标记（[`migrate_legacy_to_regions`] 后半段）。
///
/// 仅 region 0（system raft）leader 写入（marker 经 raft 复制到全部节点根
/// store）；其余节点轮询等待标记复制到位。幂等：标记已存在即确认返回。
pub async fn write_migration_marker(
    node_id: NodeID,
    root: &MvccStorage<RedbBackend>,
    root_raft: &CoordRaft,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        if has_migration_marker(root)? {
            tracing::info!("migration: migration marker confirmed on node {node_id}");
            return Ok(());
        }
        if root_raft.current_leader().await == Some(node_id) {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let mut value = MARKER_PREFIX.to_vec();
            value.extend_from_slice(format!("node={node_id} ts={now}").as_bytes());
            root_raft
                .client_write(Command::Put {
                    key: MIGRATION_MARKER_KEY.to_vec(),
                    value,
                    lease_id: None,
                })
                .await
                .map_err(|e| {
                    coord_core::error::Error::Internal(format!(
                        "migration: write marker via region-0 raft failed: {e}"
                    ))
                })?;
            tracing::info!(
                "migration: node {node_id} wrote migration marker via region-0 raft"
            );
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(coord_core::error::Error::Internal(
                "migration: marker not confirmed within 60s (region-0 raft no leader?)"
                    .to_string(),
            ));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
