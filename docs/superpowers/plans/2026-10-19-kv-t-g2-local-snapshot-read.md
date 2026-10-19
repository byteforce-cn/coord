# KV-T G2（#188）任意节点跨 Region Range（本地快照读）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让任意持有命中 Region 本地副本的节点都能服务 `coord.kv.v2.Range` 的跨 Region 快照读（不再要求请求节点是各 Region 的 leader），同时保持 `read_ts` 一致性与「不返回部分结果」的失败语义；v1 行为不变。

**Architecture:** v2 Range 是快照读：请求级 `wait_for_safe_ts` 已保证 `read_ts <= 本节点 safe_ts`，而 `safe_ts = min(本地各数据 Region 的 applied revision)`，因此读取时每个分段都满足 `read_ts <= 本地 applied`；压缩水位随 applied revision 前进，历史可回放。分段读取因此**移除 ReadIndex 屏障**，改为每段复核 `read_ts <= current_revision`（不足 → `UNAVAILABLE + SAFE_TS_LAG`，可重试）。这是 issue #188 的方案 B（本地快照读），无需新增节点间 RPC/鉴权面。

**Tech Stack:** Rust、openraft/redb、tonic/prost、coord-proto v2、futures。

**Spec:** `docs/adr/0015-kv-t.md` D10（G2）/ D12.1 / D12.3 + issue #188；v2 线缆语义以 `coord-proto/src/proto/coord/kv/v2/kv.proto` 头注为准（与 `apis/contracts/proto/...` 逐字节同步）。

## Global Constraints

- `[kv_t].enabled=false`（默认）时 v1 行为完全不变；v1 跨 Region Range 继续 `INVALID_ARGUMENT`（`guard_range_in_target` 不动）。
- `Range.read_ts=0` → 请求节点本地 `safe_ts`；显式 `read_ts > safe_ts` 沿用 #142 的有界等待，超时 `UNAVAILABLE + SAFE_TS_LAG`。
- 一个请求内所有分段共享同一个 `read_ts`；响应回显该值。
- 每个分段读取前复核 `read_ts <= target.mvcc.current_revision()`；不满足 → 整请求 `UNAVAILABLE + SAFE_TS_LAG`，**不得**把当前态当成未来 revision 的快照返回。
- 任一分段路由/存储错误 → 整个请求失败，**不得**返回已读到的前缀结果。
- 不新增配置项、不新增内部 RPC、不新增 `TODO`/`FIXME`；临时文件只放 `/tmp`；注释只写当前「为什么」。
- 提交前本机跑：`cargo fmt --all -- --check`、`cargo clippy --workspace --lib --bins -- -D warnings`、`bash scripts/check-panics.sh`、`cargo audit --no-fetch`、`cargo deny check`、`bash scripts/check-doc-refs.sh`、`cargo test --workspace --no-fail-fast`。

## Rulings

- **选方案 B（本地快照读），不选方案 A（跨节点读转发）**：D12.3 已定义 `read_ts <= safe_ts` 的可见性；`safe_ts` 是本地各 Region applied 的最小值，等价于「本地每个 Region 都已 apply 到 `read_ts`」；压缩水位 = current revision，所以历史在窗口内可回放。方案 A 需要新增内部 RPC、节点鉴权/白名单与 leader 定位，且不改善快照语义（`read_ts=0` 仍取请求侧 safe_ts）。
- **`read_ts=0` 的线性化边界**：它是「本节点 safe_ts 的快照」，可能落后于集群最新提交；跨节点不保证同一数值/单调。跨节点同一快照或 read-your-writes 必须使用显式 `read_ts`（D12.3）。ADR-0015 D10 的「当前限制」改写为该语义，consumer contract 与 proto 头注同步。
- **只移除 v2 Range 分段的屏障**：v1 读、v2 写、Lease/对象存储等路径继续使用 ReadIndex 线性一致屏障；`ensure_linearizable_on` / `ensure_linearizable_barrier` 不删除。
- **幻影态终检的取舍**：快照读不携带 leader 身份，不依赖 ReadIndex 的 applied/committed 终检；正确性由「applied revision 单调 + 分段复核」保证。发现 `read_ts > 本地 applied` 时必须报错，不允许静默读当前态。
- **epoch 校验不在本项**：split/merge 的 routing epoch/EPOCH_STALE 归 #161/#167；本项保持既有路由缺口 fail-closed（`RouteNotReady` → `UNAVAILABLE`）。
- **v1 静态复制前提**：每个节点持有全部数据 Region 副本时「任意节点可读」成立；副本缺失/路由缺口整请求失败（#159/#160 的动态分布落地后由 epoch/转发另行处理）。

## Review Focus

1. **本地快照充分性**：`read_ts=0` 解析为本地 safe_ts、显式 `read_ts` 先等待；分段复核覆盖 `read_ts == 0`（返回空）与 `read_ts == current`（实时读）两个边界。
2. **不返回部分结果**：缺失 Region、压缩越界、apply 不足都必须是整请求错误，不能返回其他分段。
3. **跨节点同一快照**：3 节点 × 3 Region、leader 分散时，显式 `read_ts` 在任一节点返回相同 key 集。
4. **v1 零变化**：v1 跨 Region Range 仍 `INVALID_ARGUMENT`；v1 读仍要求 leader。
5. **文档单一归属**：ADR/consumer contract/proto 头注口径一致，不复制实现细节。

---

### Task 1: 失败的 3 节点 × 3 Region 验收测试

**Files:**
- Create: `coord-server/tests/kv_v2_multi_node_range_test.rs`

**Interfaces:**
- Consumes: `RegionManager::spawn_region`、`CoordNode::set_kvt_state`、`KvV2::range`、`Command::PutV2`。
- Produces: 跨节点快照读的 RED 证据（修复前请求节点不是 leader 的 Region 分段返回 `UNAVAILABLE`）。

- [ ] **Step 1: 写测试**

```rust
//! KV-T G2（#188）跨 Region Range 的跨节点读（本地快照读）：
//!
//! - 3 节点 × 3 数据 Region，leader 分散（r1→n1、r2→n2、r3→n3）；
//! - 任一节点都能在显式 `read_ts` 上服务跨 Region 快照，不要求本节点是 leader；
//! - `read_ts=0` 取**本节点** safe_ts；
//! - 本地 applied 不足 → `SAFE_TS_LAG`；Region 运行时缺失 → 整请求失败，
//!   不返回部分分段结果。

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use coord_core::error_code::error_code_of;
use coord_core::storage::StorageBackend;
use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionId, RegionMeta, StorageConfig};
use coord_proto::kv::v2::kv_server::Kv as KvV2;
use coord_proto::kv::v2::{RangeRequest as RangeRequestV2, RangeResponse as RangeResponseV2};
use coord_server::kvt::{KvTCapability, KvTRuntimeConfig, KvTState};
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcServer, RaftRpcService};
use coord_server::raft::region::RegionManager;
use coord_server::raft::region_runtime::{region_data_dir, RegionRuntimeSpec};
use coord_server::raft::type_config::{Command, Response};
use coord_server::raft::{new_basic_node, RaftConfig, RaftNode, WatchReceiver};
use coord_server::server::CoordNode;
use coord_server::storage::mvcc::{MvccStorage, RevisionMode};
use coord_server::storage::redb_backend::RedbBackend;
use tempfile::TempDir;
use tonic::Code;

use crate::common::bind_listener;

const NODES: [u64; 3] = [1, 2, 3];
const REGIONS: [(RegionId, &[u8], &[u8]); 3] = [(1, b"", b"d"), (2, b"d", b"n"), (3, b"n", b"")];

struct NodeHost {
    node_id: u64,
    node: Arc<CoordNode>,
    manager: Arc<RegionManager>,
    _dir: TempDir,
    _factory: RaftNetworkFactoryImpl,
    _raft_handle: tokio::task::JoinHandle<()>,
}

struct Cluster {
    hosts: Vec<NodeHost>,
}

fn region_meta(
    region_id: RegionId,
    start: &[u8],
    end: &[u8],
    raft_addrs: &BTreeMap<u64, String>,
) -> RegionMeta {
    RegionMeta {
        region_id,
        start_key: start.to_vec(),
        end_key: end.to_vec(),
        epoch: RegionEpoch::initial(),
        peers: raft_addrs
            .iter()
            .map(|(node_id, raft_addr)| Peer {
                node_id: *node_id,
                raft_addr: raft_addr.clone(),
                role: PeerRole::Voter,
            })
            .collect(),
        approximate_size: 0,
        approximate_keys: 0,
    }
}

impl Cluster {
    async fn start() -> Self {
        // 监听器预绑定并由 serve_with_incoming 接管（见 common::bind_listener）。
        let mut raft_listeners = BTreeMap::new();
        let mut raft_addrs: BTreeMap<u64, String> = BTreeMap::new();
        for node_id in NODES {
            let (listener, addr) = bind_listener().await;
            raft_listeners.insert(node_id, listener);
            raft_addrs.insert(node_id, addr);
        }

        let mut hosts = Vec::new();
        for node_id in NODES {
            let dir = tempfile::tempdir().unwrap();
            let base = dir.path().to_path_buf();
            let factory = RaftNetworkFactoryImpl::new(node_id);
            for (id, addr) in &raft_addrs {
                factory.register_node(*id, addr.clone());
            }
            let rpc = RaftRpcService::new();
            let manager = Arc::new(RegionManager::new(node_id));
            for (region_id, start, end) in REGIONS {
                manager
                    .spawn_region(
                        &factory,
                        &rpc,
                        RegionRuntimeSpec {
                            meta: region_meta(region_id, start, end, &raft_addrs),
                            data_dir: region_data_dir(&base, region_id),
                            raft_config: Arc::new(RaftConfig {
                                heartbeat_interval: 100,
                                election_timeout_min: 300,
                                election_timeout_max: 600,
                                ..Default::default()
                            }),
                            object_store: None,
                            revision_mode: RevisionMode::CommitTs,
                        },
                        false,
                    )
                    .await
                    .expect("spawn data region");
            }

            // region 模式下 KV 全量走 manager 路由；scratch 只满足构造签名。
            let backend = RedbBackend::open(&base, &StorageConfig::default()).unwrap();
            let scratch = Arc::new(MvccStorage::new(backend).unwrap());
            let mut node = CoordNode::new(scratch);
            node.node_id = node_id;
            node.region_manager = Some(Arc::clone(&manager));
            node.set_kvt_state(
                KvTState::new(
                    true,
                    KvTRuntimeConfig {
                        max_range: 100,
                        read_ts_wait_ms: 50,
                        ..KvTRuntimeConfig::default()
                    },
                )
                .with_implemented([KvTCapability::Kv]),
            );
            let node = Arc::new(node);

            let listener = raft_listeners.remove(&node_id).expect("listener pre-bound");
            let svc = RaftRpcServer::new(rpc);
            let raft_handle = tokio::spawn(async move {
                let _ = tonic::transport::Server::builder()
                    .add_service(svc)
                    .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                    .await;
            });

            hosts.push(NodeHost {
                node_id,
                node,
                manager,
                _dir: dir,
                _factory: factory,
                _raft_handle: raft_handle,
            });
        }

        // 所有 raft 监听器就绪后再初始化成员（否则 initialize 等不到 quorum）。
        for addr in raft_addrs.values() {
            tokio::net::TcpStream::connect(addr)
                .await
                .expect("raft server ready");
        }
        let members: BTreeMap<u64, RaftNode> = raft_addrs
            .iter()
            .map(|(id, addr)| (*id, new_basic_node(addr)))
            .collect();
        for (region_id, _, _) in REGIONS {
            hosts[0]
                .manager
                .runtime(region_id)
                .unwrap()
                .raft
                .initialize(members.clone())
                .await
                .expect("initialize 3-voter region");
        }

        let cluster = Self { hosts };
        cluster.spread_leaders().await;
        cluster
    }

    fn host(&self, node_id: u64) -> &NodeHost {
        self.hosts
            .iter()
            .find(|h| h.node_id == node_id)
            .expect("node exists")
    }

    async fn leader_node(&self, region_id: RegionId) -> u64 {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            for host in &self.hosts {
                let rt = host.manager.runtime(region_id).expect("region runtime");
                let m = rt.raft.metrics();
                let m = m.borrow_watched();
                if m.last_quorum_acked.is_some() && m.current_leader == Some(host.node_id) {
                    return host.node_id;
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "region {region_id} has no leader"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// r1→n1、r2→n2、r3→n3，使任一请求节点都至少对一个命中 Region 是 follower。
    async fn spread_leaders(&self) {
        for (region_id, target) in [(1u64, 1u64), (2, 2), (3, 3)] {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
            loop {
                let leader = self.leader_node(region_id).await;
                if leader == target {
                    break;
                }
                self.host(leader)
                    .manager
                    .runtime(region_id)
                    .unwrap()
                    .raft
                    .trigger()
                    .transfer_leader(target)
                    .await
                    .expect("request leader transfer");
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "region {region_id} leadership did not move to node {target}"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    async fn put(&self, region_id: RegionId, key: &[u8], value: &[u8], commit_ts: u64) {
        let leader = self.leader_node(region_id).await;
        let rt = self.host(leader).manager.runtime(region_id).unwrap();
        let resp = rt
            .raft
            .client_write(Command::PutV2 {
                key: key.to_vec(),
                value: value.to_vec(),
                lease_id: None,
                commit_ts,
            })
            .await
            .expect("put v2");
        assert!(
            matches!(resp.response(), Response::Put { revision } if *revision == commit_ts),
            "region {region_id} put must apply at its commit_ts"
        );
    }

    async fn seed(&self) {
        // 每 Region 内 commit_ts 严格递增；min(400, 420, 400) = 400。
        self.put(1, b"a", b"a100", 100).await;
        self.put(1, b"b", b"b200", 200).await;
        self.put(1, b"a2", b"a240", 240).await;
        self.put(1, b"a3", b"a400", 400).await;
        self.put(2, b"d", b"d100", 100).await;
        self.put(2, b"e", b"e250", 250).await;
        self.put(2, b"e2", b"e420", 420).await;
        self.put(3, b"n", b"n100", 100).await;
        self.put(3, b"z", b"z400", 400).await;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let applied_everywhere = self.hosts.iter().all(|h| {
                REGIONS.iter().all(|(region_id, _, _)| {
                    h.manager.runtime(*region_id).unwrap().mvcc.current_revision() >= 400
                })
            });
            if applied_everywhere {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "v2 writes did not converge to all nodes"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        for host in &self.hosts {
            assert_eq!(host.node.safe_ts(), 400, "node {} safe_ts", host.node_id);
        }
    }

    async fn range(&self, node_id: u64, read_ts: i64) -> Result<RangeResponseV2, tonic::Status> {
        KvV2::range(
            &*self.host(node_id).node,
            tonic::Request::new(RangeRequestV2 {
                key: b"a".to_vec(),
                range_end: b"zzz".to_vec(),
                read_ts,
                ..Default::default()
            }),
        )
        .await
        .map(|resp| resp.into_inner())
    }
}

fn keys(resp: &RangeResponseV2) -> Vec<Vec<u8>> {
    resp.kvs.iter().map(|kv| kv.key.clone()).collect()
}

/// read_ts=400 时可见：r1 a/a2/a3/b，r2 d/e（e2@420 排除），r3 n/z。
fn expected_keys() -> Vec<Vec<u8>> {
    [b"a".as_slice(), b"a2", b"a3", b"b", b"d", b"e", b"n", b"z"]
        .iter()
        .map(|k| k.to_vec())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_any_node_serves_cross_region_range_snapshot() {
    let cluster = Cluster::start().await;
    cluster.seed().await;

    // leader 分散：请求节点对至少一个命中 Region 只是 follower。
    assert_eq!(cluster.leader_node(1).await, 1);
    assert_eq!(cluster.leader_node(2).await, 2);
    assert_eq!(cluster.leader_node(3).await, 3);

    for node_id in NODES {
        let resp = cluster
            .range(node_id, 400)
            .await
            .unwrap_or_else(|e| panic!("node {node_id} must serve the snapshot: {e:?}"));
        assert_eq!(resp.read_ts, 400);
        assert_eq!(keys(&resp), expected_keys(), "node {node_id} explicit read_ts");

        // read_ts=0 → 本节点 safe_ts（三节点已收敛，同为 400）。
        let resp0 = cluster.range(node_id, 0).await.unwrap();
        assert_eq!(
            resp0.read_ts, 400,
            "node {node_id} read_ts=0 must echo local safe_ts"
        );
        assert_eq!(keys(&resp0), expected_keys(), "node {node_id} read_ts=0");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_range_fails_closed_when_local_snapshot_is_missing() {
    let cluster = Cluster::start().await;
    cluster.seed().await;

    // read_ts 超过本地 safe_ts：等待超时 → SAFE_TS_LAG（可重试），无部分结果。
    let err = cluster
        .range(2, 10_000)
        .await
        .expect_err("above safe_ts must fail");
    assert_eq!(err.code(), Code::Unavailable, "{err:?}");
    assert_eq!(error_code_of(&err).as_deref(), Some("SAFE_TS_LAG"));

    // 请求节点缺一个命中 Region 的运行时：整请求失败，不返回其他分段。
    cluster
        .host(3)
        .manager
        .remove_region_runtime(2)
        .unwrap();
    let err = cluster
        .range(3, 400)
        .await
        .expect_err("route gap must fail the whole request");
    assert_eq!(err.code(), Code::Unavailable, "{err:?}");

    // 其他节点不受影响：仍是同一快照。
    let resp = cluster.range(1, 400).await.unwrap();
    assert_eq!(resp.read_ts, 400);
    assert_eq!(keys(&resp), expected_keys());
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server --test kv_v2_multi_node_range_test -- --nocapture`
Expected: `test_any_node_serves_cross_region_range_snapshot` 失败（节点 2 对 Region 1 不是 leader → `UNAVAILABLE`）；`test_range_fails_closed_when_local_snapshot_is_missing` 的首个断言也可能先失败，说明当前实现把「本地 safe_ts 不足」伪装成其它错误——记录实际输出，作为 RED 证据。

- [ ] **Step 3: 提交 RED 测试**

```bash
git add coord-server/tests/kv_v2_multi_node_range_test.rs docs/superpowers/plans/2026-10-19-kv-t-g2-local-snapshot-read.md
git commit -m "test(kv-t): reproduce follower-less cross-region range gap (#188)"
```

---

### Task 2: 分段读取改为本地快照 + applied 复核

**Files:**
- Modify: `coord-server/src/kvt/services.rs`
- Test: `coord-server/tests/kv_v2_multi_node_range_test.rs`（Task 1）

**Interfaces:**
- Consumes: `KvRangeSegment { target: KvTarget { mvcc, region_id, .. }, .. }`、`CoordErrorCode::SafeTsLag`。
- Produces: `async fn read_kv_range_segment(seg: KvRangeSegment, read_ts: u64, scan_limit: usize) -> Result<Vec<kv_v2::KeyValue>, Status>`（去掉 `node` 参数）。

- [ ] **Step 1: 改实现**

`coord-server/src/kvt/services.rs`：

```rust
/// 读取一个 Region 分段在 `read_ts` 上的精确快照。
///
/// 快照读不需要 ReadIndex 屏障：请求级等待已保证本节点**所有**数据 Region 的
/// applied revision >= `read_ts`（`read_ts=0` 解析为本地 safe_ts，同样 <= 最小
/// applied），本地副本足以重建该快照；压缩水位随 applied revision 前进，
/// 历史在保留窗口内可回放。分段再复核一次 applied 水位：不足即
/// `SAFE_TS_LAG`（可重试），不得把「当前态」当成未来 revision 的快照返回。
async fn read_kv_range_segment(
    seg: KvRangeSegment,
    read_ts: u64,
    scan_limit: usize,
) -> Result<Vec<kv_v2::KeyValue>, Status> {
    let target = &seg.target;
    let current_now = target.mvcc.current_revision();
    if read_ts > current_now {
        return Err(attach(
            Status::unavailable(format!(
                "read_ts {read_ts} exceeds local applied revision {current_now} of region {}; \
                 retry later",
                target.region_id
            )),
            CoordErrorCode::SafeTsLag,
        ));
    }

    let rows = if read_ts == 0 {
        // 0 是“早于任何 commit”的合法快照（safe_ts 也可能为 0）；
        // changelog 的 0 水位会被当成已压缩，不能走历史读。
        Vec::new()
    } else if read_ts == current_now {
        let (rows, live_revision) = target
            .mvcc
            .range_in_with_metadata(&seg.start, &seg.end, scan_limit)
            .map_err(map_err)?;
        if live_revision > read_ts {
            target
                .mvcc
                .range_at_revision_with_metadata(&seg.start, &seg.end, scan_limit, read_ts)
                .map_err(v2_status)?
        } else {
            rows
        }
    } else {
        target
            .mvcc
            .range_at_revision_with_metadata(&seg.start, &seg.end, scan_limit, read_ts)
            .map_err(v2_status)?
    };

    Ok(rows
        .into_iter()
        .map(|(key, value, meta)| to_kv_v2(&key, &value, Some(&meta)))
        .collect())
}
```

调用点：

```rust
let reads = segments
    .into_iter()
    .map(|seg| read_kv_range_segment(seg, read_ts, scan_limit));
```

- [ ] **Step 2: 跑目标测试确认全绿**

Run: `cargo test -p coord-server --test kv_v2_multi_node_range_test -- --nocapture`
Expected: 2 tests pass。

- [ ] **Step 3: 跑既有 v2/G2 回归**

Run: `cargo test -p coord-server --test kv_v2_range_fanout_test --test kv_v2_single_region_test`
Run: `cargo test -p coord --test kv_t_g1_kv_rpc_test --test kv_t_g2_safe_ts_test`
Expected: 全绿（`Range(read_ts=0)` 的既有断言仍成立：单节点夹具 safe_ts 不变）。

- [ ] **Step 4: 提交实现**

```bash
git add coord-server/src/kvt/services.rs
git commit -m "fix(kv-t): serve cross-region range from local snapshots (#188)"
```

---

### Task 3: 契约与 ADR 口径（read_ts=0 的线性化边界）

**Files:**
- Modify: `docs/adr/0015-kv-t.md`（D10、D12.1）
- Modify: `docs/production/consumer-contract.md`
- Modify: `coord-proto/src/proto/coord/kv/v2/kv.proto`
- Modify: `apis/contracts/proto/coord/kv/v2/kv.proto`（与上者逐字节一致）
- Test: `bash scripts/check-doc-refs.sh`、`bash apis/contracts/scripts/check-wire-sync.sh`

- [ ] **Step 1: ADR-0015 D10 改写「当前限制」**

替换原「G2 跨 Region Range 的当前限制」段：

```markdown
- **G2 跨 Region Range 的跨节点语义**：v2 Range 是快照读，不要求请求节点是任何
  Region 的 leader。请求先把 `read_ts` 解析/等待到 `<= 本节点 safe_ts`（D12.1/D12.3），
  再在本地各 Region 副本的精确快照上读；每个分段复核 `read_ts <= 本地 applied`，
  不足时 `UNAVAILABLE + SAFE_TS_LAG`（可重试），整请求失败、不返回部分结果。
  前提是请求节点持有命中 Region 的本地副本（v1 静态复制下每个节点持有全部 Region）；
  副本缺失/路由缺口同样整请求失败。split/merge 的 routing epoch 校验归 #161/#167。
  `read_ts=0` 取**本节点** `safe_ts`，不是线性一致读；跨节点同一快照或
  read-your-writes 必须使用显式 `read_ts`。
```

- [ ] **Step 2: ADR-0015 D12.1 补充快照读边界**

把「快照读」一条改为：

```markdown
- **快照读**：`Range(read_ts=0)` 取**请求节点**当前的 `safe_ts`（本地副本的最小
  applied 水位）；显式 `read_ts` 用于跨 Region 一致性读，必须来自 TSO。`read_ts=0`
  的快照可能落后于集群最新提交，且跨节点不保证同一数值/单调；需要跨节点同一快照
  或 read-your-writes 的调用方必须使用显式 `read_ts`（D12.3）。
```

- [ ] **Step 3: consumer-contract 改写限制条目**

替换 `- **跨 Region Range（当前限制）** …` 为：

```markdown
- **跨 Region Range**：由请求节点的本地副本服务（v1 静态复制下每节点持有全部
  Region），不要求 leader；`read_ts <= 本地 applied` 由 safe_ts 等待 + 分段复核
  保证，不足 `UNAVAILABLE + SAFE_TS_LAG`（可重试）；整请求失败、不返回部分结果。
  `read_ts=0` 取**本节点** `safe_ts`，可能落后于集群最新提交、跨节点不保证同一
  数值/单调；跨节点同一快照或 read-your-writes 请用显式 `read_ts`。
```

- [ ] **Step 4: proto 头注（两个路径同步）**

读路径段落改为：

```proto
//   - `read_ts = 0` ⇒ 服务端取**本节点**当前 `safe_ts`（本地已 apply 水位），
//     保证多 key 原子可见；`read_ts > safe_ts` 时等待至多
//     `[kv_t].read_ts_wait_ms`，超时返回 `UNAVAILABLE` + `SAFE_TS_LAG`（可重试）；
//   - 读取由任意持有本地副本的节点服务，不要求 leader；快照在 `read_ts` 上一致，
//     `read_ts=0` 不保证线性一致（需要最新/跨节点同一快照请用显式 `read_ts`）；
```

复制到 `apis/contracts/proto/coord/kv/v2/kv.proto` 后逐字节比较：

```bash
cmp coord-proto/src/proto/coord/kv/v2/kv.proto apis/contracts/proto/coord/kv/v2/kv.proto
bash apis/contracts/scripts/check-wire-sync.sh
bash scripts/check-doc-refs.sh
```

- [ ] **Step 5: 提交文档**

```bash
git add docs/adr/0015-kv-t.md docs/production/consumer-contract.md coord-proto/src/proto/coord/kv/v2/kv.proto apis/contracts/proto/coord/kv/v2/kv.proto
git commit -m "docs(kv-t): define local-snapshot semantics for cross-region range (#188)"
```

---

### Task 4: 全量门禁与收口

- [ ] **Step 1: 本机门禁**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
bash scripts/check-panics.sh
cargo audit --no-fetch
cargo deny check
bash scripts/check-doc-refs.sh
cargo test --workspace --no-fail-fast
```

- [ ] **Step 2: 勾选计划并开 PR**

勾选本文件所有 `- [ ]`，推分支、`gh pr create --base main --fill`，等 `gh pr checks --watch` 全绿。

- [ ] **Step 3: 合并后关闭 #188**

`gh pr merge --delete-branch --merge`；在 #188 逐条贴证据（3 节点 leader 分散测试名、失败语义测试、ADR/contract 链接、门禁输出）。milestone 由 issue 关闭自动收敛。
