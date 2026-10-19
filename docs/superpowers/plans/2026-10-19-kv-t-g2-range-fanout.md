# KV-T G2 跨 Region Range 扇出 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 落地 issue #143——`coord.kv.v2.Range` 在区间跨多个 Region 时按**同一个 `read_ts`** 扇出读取、按 key 有序合并，`limit`/`count_only`/`keys_only` 以整个请求为口径；任一 Region 不可用则整个请求失败，不返回部分结果；v1 跨 Region 仍显式拒绝。

**Architecture:** `RegionManager::resolve_range` 把半开区间 `[start, end)` 按 Region 边界切成有序分段（首段从请求起点开始，末段到请求终点结束，末 Region 无上界时取请求终点）。`CoordNode::kv_range_targets` 把分段映射为 per-Region `KvTarget`（legacy 单 Raft 只有一个分段）。`Kv::range` 先解析一次全局 `read_ts`（`0` → `safe_ts`；显式值先等待全局水位），再用 `futures::future::try_join_all` 并行读各分段（每段独立 ReadIndex 屏障 + 该段的精确快照读），最后按分段顺序拼接/截断/计数。分段互不相交且有序，因此拼接结果天然有序；`try_join_all` 的首个错误直接返回，天然无部分结果。

**Tech Stack:** Rust、openraft/redb、tonic/prost、`futures::future::try_join_all`、coord-proto v2。

**Spec:** `docs/adr/0015-kv-t.md` D3/D7/D10/D12.3 + issue #143；v2 线缆语义以 `coord-proto/src/proto/coord/kv/v2/kv.proto` 头注为准。

## Global Constraints

- `[kv_t].enabled=false`（默认）时 v1 行为完全不变：`guard_range_in_target` 继续对 v1 跨 Region Range 返回 `INVALID_ARGUMENT`；本次只改 `coord.kv.v2` 的 handler。
- `coord.kv.v2.Range` 的 `read_ts=0` → 全局 `safe_ts`；显式 `read_ts > safe_ts` 沿用 #142 的有界等待，超时 `UNAVAILABLE + SAFE_TS_LAG`。
- 一个请求内所有分段共享**同一个** `read_ts`；响应 `read_ts` 回显该值（`read_ts=0 && safe_ts=0` 时回显 0）。
- 任一 Region 的路由/屏障/存储错误 → 整个请求失败（可重试错误保持可重试），**不得**返回已读到的前缀结果。
- 结果集上限沿用 ADR-0015 D12.2：`max_range` 默认 10000；超限 `INVALID_ARGUMENT` + `READ_SET_TOO_LARGE`（见 Rulings）。
- 不新增配置项；不新增 `TODO`/`FIXME`；临时文件只放 `/tmp`；注释只写当前「为什么」。
- 提交前本机跑：`cargo fmt --all -- --check`、`cargo clippy --workspace --lib --bins -- -D warnings`、`bash scripts/check-panics.sh`、`cargo audit --no-fetch`、`cargo deny check`、`bash scripts/check-doc-refs.sh`，以及 `cargo test --workspace --no-fail-fast`。

## Rulings

- **超限错误码用 `READ_SET_TOO_LARGE`，不是 `RESOURCE_EXHAUSTED`**：issue #143 正文提到 `RESOURCE_EXHAUSTED`，但 ADR-0015 D12.2、consumer contract 与 G1 已有测试都固定 `INVALID_ARGUMENT + READ_SET_TOO_LARGE`。`RESOURCE_EXHAUSTED` 在 consumer contract 里是「配额/背压，不重试」，会改变调用方重试矩阵。按 ADR 执行；若未来要改，是独立的契约变更。
- **`prev_kv` 不在 v2 Range 面**：`kv.v2.RangeRequest` 没有 `prev_kv` 字段（只有 `Delete` 有）；issue 的 `prev_kv` 一项对 Range 不适用，Delete-range 的既有路径本次不动。
- **分段不引入跨分段 epoch 校验**：split/merge 期间的 routing epoch/cutover 校验归 #161/#167；本计划不加入 epoch 重试（分段循环会重新读取 `key_index`/`meta`，缺口仍 fail-closed）。
- **扇出在请求节点本地执行**：每段 ReadIndex 屏障要求本节点是该 Region 的 leader；多节点 leader 分散时的跨节点读转发缺口跟踪于 #188，ADR-0015 D10 已登记该限制。
- **扇出并发上限 8**：`buffered(8)` 保持分段顺序；非 `count_only` 读在拿到全局前 `limit` 条后停止消费后续分段，`count_only` 逐段计数并超限即失败——避免 `Region 数 × limit` 的行缓冲。
- **`count_only` 每段最多读 `max_range + 1` 行**：并行分段无法共享剩余预算；`max_range` 默认 1 万、Region 数有限，最坏内存是 `段数 × (max_range+1)`。拼接后一旦总数超过 `max_range` 立即返回 `READ_SET_TOO_LARGE`。
- **`read_ts=0` 的段仍先过 ReadIndex 屏障**：保持 G1「空快照也确认目标 Region 身份」的既有行为，不因扇出改变错误面。
- **末 Region 无上界**：`RegionMeta.end_key` 为空表示 keyspace 末尾；该段的终点取请求终点，而不是无限扫描。
- **对象空间过滤在拼接后执行**：`/obj/` 可能落在单独 Region；先合并再过滤，保证 `count`/`limit` 的语义与返回的 KV 集合一致。

## Review Focus

1. **快照一致性**：领先 Region（`current_revision > read_ts`）必须走精确历史读，返回行的 `mod_revision <= read_ts`；不得把领先 Region 的新值混进同一快照。
2. **半开边界**：Region 起点 key 必须包含，Region 终点 key 必须排除；`range_end == key` 与空 `range_end` 都是单键读。
3. **全局 limit/count**：`limit` 是整请求上限，各分段自己的 limit 不得泄漏成「每段 N 条」；`count_only` 的 `count` 是全部命中数，超 `max_range` 错误而非静默截断。
4. **部分失败**：任一 Region 路由/屏障失败时整个请求失败；不得返回已成功分段的前缀结果。
5. **空与零快照**：`end < start` 返回空；`read_ts=0 && safe_ts=0` 返回 `read_ts=0` 的空结果；空结果也不得因某段错误而成功。
6. **v1 零变化**：v1 跨 Region Range 继续 `INVALID_ARGUMENT`；`RevisionMode::LogIndex` 路径不受影响。

---

### Task 1: `RegionManager::resolve_range`（有序分段）

**Files:**
- Modify: `coord-server/src/raft/region.rs`
- Test: `coord-server/tests/region_manager_test.rs`

**Interfaces:**
- Produces: `pub struct RegionRangeSegment { pub region_id: RegionId, pub start_key: Vec<u8>, pub end_key: Vec<u8> }`（`Debug + Clone + PartialEq + Eq`；半开 `[start_key, end_key)`）
- Produces: `RegionManager::resolve_range(&self, start: &[u8], end: &[u8]) -> Result<Vec<RegionRangeSegment>>`
  - `end` 必须非空（调用方把单键读换算为 `single_key_end`）；空 `end` → `Error::InvalidArgument`
  - `end <= start` → `Ok(vec![])`（空区间，不路由）
  - 首段 `start_key == start`；后续段首尾相接；末段 `end_key == end`
  - 表为空 → `Error::RouteNotReady`；`start` 不落在任何 Region → `Error::KeyNotInRegion`；相邻段之间有空隙/重叠 → `Error::RouteNotReady`（fail-closed，可重试）

- [ ] **Step 1: 写失败测试**（追加到 `coord-server/tests/region_manager_test.rs`）

```rust
use coord_core::error::Error;
use coord_server::raft::region::{RegionManager, RegionRangeSegment};

fn seg(region_id: RegionId, start: &[u8], end: &[u8]) -> RegionRangeSegment {
    RegionRangeSegment {
        region_id,
        start_key: start.to_vec(),
        end_key: end.to_vec(),
    }
}

fn tiled_manager() -> RegionManager {
    let rm = RegionManager::new(1);
    rm.register_region(make_test_meta(1, b"".to_vec(), b"d".to_vec())).unwrap();
    rm.register_region(make_test_meta(2, b"d".to_vec(), b"n".to_vec())).unwrap();
    rm.register_region(make_test_meta(3, b"n".to_vec(), b"".to_vec())).unwrap();
    rm
}

#[test]
fn test_resolve_range_splits_at_region_boundaries() {
    let rm = tiled_manager();
    assert_eq!(
        rm.resolve_range(b"a", b"z").unwrap(),
        vec![seg(1, b"a", b"d"), seg(2, b"d", b"n"), seg(3, b"n", b"z")]
    );
    assert_eq!(rm.resolve_range(b"d", b"n").unwrap(), vec![seg(2, b"d", b"n")]);
    assert_eq!(rm.resolve_range(b"", b"c").unwrap(), vec![seg(1, b"", b"c")]);
    // 无界末 Region：分段终点 = 请求终点。
    assert_eq!(rm.resolve_range(b"n", b"zzz").unwrap(), vec![seg(3, b"n", b"zzz")]);
    // 空区间不路由。
    assert!(rm.resolve_range(b"n", b"d").unwrap().is_empty());
    // 调用方必须先把单键换算成非空上界。
    assert!(matches!(
        rm.resolve_range(b"d", b""),
        Err(Error::InvalidArgument(_))
    ));
}

#[test]
fn test_resolve_range_rejects_gap_and_unroutable_start() {
    let gap = RegionManager::new(1);
    gap.register_region(make_test_meta(1, b"".to_vec(), b"d".to_vec())).unwrap();
    gap.register_region(make_test_meta(3, b"n".to_vec(), b"".to_vec())).unwrap();
    assert!(matches!(gap.resolve_range(b"a", b"z"), Err(Error::RouteNotReady)));

    let starts_at_d = RegionManager::new(1);
    starts_at_d
        .register_region(make_test_meta(2, b"d".to_vec(), b"".to_vec()))
        .unwrap();
    assert!(matches!(
        starts_at_d.resolve_range(b"a", b"z"),
        Err(Error::RouteNotReady)
    ));
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server --test region_manager_test resolve_range`
Expected: 编译失败（`RegionRangeSegment` / `resolve_range` 不存在）

- [ ] **Step 3: 实现**

在 `region.rs` 的 `RegionHandle` 之前或 `RegionManager` 附近定义 `RegionRangeSegment`；`impl RegionManager` 中新增：

```rust
pub fn resolve_range(&self, start: &[u8], end: &[u8]) -> Result<Vec<RegionRangeSegment>> {
    if end.is_empty() {
        return Err(Error::InvalidArgument(
            "range end must be non-empty (single-key reads use key + 0x00)".to_string(),
        ));
    }
    if end <= start {
        return Ok(Vec::new());
    }

    let mut segments = Vec::new();
    let mut cursor = start.to_vec();
    let mut region = self.route(&cursor)?;
    loop {
        let meta = region.meta.read().clone();
        let seg_end = if meta.end_key.is_empty() || end <= meta.end_key.as_slice() {
            end.to_vec()
        } else {
            meta.end_key.clone()
        };
        // 段必须前进；否则表已损坏（重叠/空 Region），不能静默跳 key。
        if seg_end <= cursor {
            return Err(Error::RouteNotReady);
        }
        segments.push(RegionRangeSegment {
            region_id: meta.region_id,
            start_key: cursor.clone(),
            end_key: seg_end.clone(),
        });
        if seg_end.as_slice() >= end {
            return Ok(segments);
        }
        cursor = seg_end;
        // 下一段必须从本段终点开始；缺口 = 路由表不可用（可重试），不得部分返回。
        let next_id = self.key_index.read().get(&cursor).copied();
        let next_id = next_id.ok_or(Error::RouteNotReady)?;
        region = self
            .get_region(next_id)
            .ok_or(Error::RegionNotFound { region_id: next_id })?;
        if !region.contains_key(&cursor) {
            return Err(Error::KeyNotInRegion {
                region_id: next_id,
            });
        }
    }
}
```

注意：`self.route(&cursor)` 已校验首段包含 `start`；循环内读取 `meta` 后立即释放锁（`clone`），随后只短暂持有 `key_index` 读锁，避免与注册/注销路径形成锁序反转。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server --test region_manager_test`
Expected: PASS（含既有用例）

- [ ] **Step 5: Commit**

```bash
git add coord-server/src/raft/region.rs coord-server/tests/region_manager_test.rs
git commit -m "feat(kv-t): resolve cross-region range segments (#143)"
```

### Task 2: v2 Range 跨 Region 扇出（同一 `read_ts` + 边界）

**Files:**
- Modify: `coord-server/src/server/mod.rs`
- Modify: `coord-server/src/kvt/services.rs`
- Test: `coord-server/tests/kv_v2_range_fanout_test.rs`（新建）

**Interfaces:**
- Consumes: `RegionManager::resolve_range`（Task 1）、`RegionRangeSegment`
- Produces: `pub(crate) struct KvRangeSegment { pub(crate) target: KvTarget, pub(crate) start: Vec<u8>, pub(crate) end: Vec<u8> }`
- Produces: `CoordNode::kv_range_targets(&self, start: &[u8], end: &[u8]) -> Result<Vec<KvRangeSegment>, Status>`
  - legacy（无 `region_manager`）→ 单个分段（`kv_target_for_key(start)`）
  - region 模式 → `resolve_range` + `manager.runtime(region_id)`；运行时缺失 → `NOT_FOUND`（`Error::RegionNotFound`，经 `map_err`）
- Produces: `async fn read_kv_range_segment(node: &CoordNode, seg: KvRangeSegment, read_ts: u64, scan_limit: usize) -> Result<Vec<kv_v2::KeyValue>, Status>`（`kvt/services.rs` 内自由函数；屏障 → 精确快照读 → v2 KeyValue）
- 处理器：`read_ts=0` 先解析为 `safe_ts` 并等待；单键（`range_end` 空或 `== key`）上界用 `single_key_end`；`end <= key` 且非单键 → 空响应

- [ ] **Step 1: 写失败测试**（新建 `coord-server/tests/kv_v2_range_fanout_test.rs`）

测试夹具参照 `coord/tests/kv_t_g2_safe_ts_test.rs` 的 `G2Node`：单节点、3 个数据 Region（`["", "d")` / `["d", "n")` / `["n", "")`）、`RevisionMode::CommitTs`、每个 Region 单 voter（进程内选主，不需要 raft gRPC server）；`CoordNode` 挂 `region_manager`，`KvTState::new(true, KvTRuntimeConfig { max_range, read_ts_wait_ms: 50, .. })` + `KvTCapability::Kv`。

```rust
struct FanoutNode { node: Arc<CoordNode>, manager: Arc<RegionManager>, _dir: TempDir, /* 保持工厂/目录存活 */ }

impl FanoutNode {
    async fn start(max_range: u32) -> Self;
    /// 直接向目标 Region 的 raft 提交 `Command::PutV2`（与 G2 夹具同路径）。
    async fn put(&self, key: &[u8], value: &[u8], commit_ts: u64);
    async fn range(
        &self,
        key: &[u8],
        end: &[u8],
        read_ts: i64,
        limit: i64,
        keys_only: bool,
        count_only: bool,
    ) -> Result<RangeResponseV2, Status>;
}

fn keys(resp: &RangeResponseV2) -> Vec<Vec<u8>> {
    resp.kvs.iter().map(|kv| kv.key.clone()).collect()
}

/// 8 个 key 跨 3 个 Region（r1: a,a2,a3,b；r2: d,e,e2；r3: n,z），
/// 并把三区 current 抬到 >= 400（safe_ts = 400）。
async fn standard_data() -> FanoutNode {
    let fx = FanoutNode::start(100).await;
    fx.put(b"a", b"a100", 100).await;
    fx.put(b"b", b"b200", 200).await;
    fx.put(b"a2", b"a260", 260).await;
    fx.put(b"a3", b"a400", 400).await;
    fx.put(b"d", b"d100", 100).await;
    fx.put(b"e", b"e250", 250).await;
    fx.put(b"e2", b"e420", 420).await;
    fx.put(b"n", b"n100", 100).await;
    fx.put(b"z", b"z400", 400).await;
    assert_eq!(fx.node.safe_ts(), 400);
    fx
}

#[tokio::test]
async fn test_range_reads_all_segments_at_one_read_ts() {
    let fx = FanoutNode::start(100).await;
    // r1: a@100, b@200；r2: d@100, e@250；r3: n@100, z@400
    fx.put(b"a", b"a100", 100).await;
    fx.put(b"b", b"b200", 200).await;
    fx.put(b"d", b"d100", 100).await;
    fx.put(b"e", b"e250", 250).await;
    fx.put(b"n", b"n100", 100).await;
    fx.put(b"z", b"z400", 400).await;

    // safe_ts = min(200, 250, 400) = 200：领先 Region 只暴露 <= 200 的历史。
    let r = fx.range(b"a", b"zzz", 0, 0, false, false).await.unwrap();
    assert_eq!(r.read_ts, 200);
    assert_eq!(keys(&r), vec![b"a".to_vec(), b"b".to_vec(), b"d".to_vec(), b"n".to_vec()]);
    assert!(r.kvs.iter().all(|kv| kv.mod_revision <= 200));

    // 显式 read_ts <= safe_ts：三区同一快照，不等待。
    let r = fx.range(b"a", b"zzz", 100, 0, false, false).await.unwrap();
    assert_eq!(r.read_ts, 100);
    assert_eq!(keys(&r), vec![b"a".to_vec(), b"d".to_vec(), b"n".to_vec()]);

    // 抬 r1 到 260 → safe_ts = 250，e@250 进入快照。
    fx.put(b"a2", b"a260", 260).await;
    let r = fx.range(b"a", b"zzz", 0, 0, false, false).await.unwrap();
    assert_eq!(r.read_ts, 250);
    assert_eq!(
        keys(&r),
        vec![b"a".to_vec(), b"b".to_vec(), b"d".to_vec(), b"e".to_vec(), b"n".to_vec()]
    );

    // 抬三区到 400+ → 全量快照（e2@420 仍排除）。
    fx.put(b"a3", b"a400", 400).await;
    fx.put(b"e2", b"e420", 420).await;
    let r = fx.range(b"a", b"zzz", 0, 0, false, false).await.unwrap();
    assert_eq!(r.read_ts, 400);
    assert_eq!(
        keys(&r),
        vec![
            b"a".to_vec(), b"a2".to_vec(), b"a3".to_vec(), b"b".to_vec(),
            b"d".to_vec(), b"e".to_vec(), b"n".to_vec(), b"z".to_vec(),
        ]
    );
}

#[tokio::test]
async fn test_range_is_half_open_at_region_boundaries_and_single_key() {
    let fx = standard_data().await;
    let r = fx.range(b"d", b"n", 400, 0, false, false).await.unwrap();
    assert_eq!(keys(&r), vec![b"d".to_vec(), b"e".to_vec()], "start 包含、end 排除");

    let r = fx.range(b"d", b"", 400, 0, false, false).await.unwrap();
    assert_eq!(keys(&r), vec![b"d".to_vec()], "空 range_end = 单键");

    let r = fx.range(b"n", b"d", 400, 0, false, false).await.unwrap();
    assert!(r.kvs.is_empty());
    assert_eq!(r.read_ts, 400);
}

#[tokio::test]
async fn test_range_zero_snapshot_is_empty_across_regions() {
    let fx = FanoutNode::start(100).await;
    let r = fx.range(b"a", b"zzz", 0, 0, false, false).await.unwrap();
    assert_eq!(r.read_ts, 0, "safe_ts=0 时快照 revision 为 0");
    assert!(r.kvs.is_empty());
    assert_eq!(r.count, 0);
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server --test kv_v2_range_fanout_test`
Expected: FAIL（`cross-region range is not supported`，`INVALID_ARGUMENT`）——首轮在 `test_range_reads_all_segments_at_one_read_ts` 失败。

- [ ] **Step 3: 实现**

1. `server/mod.rs`：把 `kv_target_for_key` 的 region 分支抽成 `fn kv_target_from_runtime(rt: &crate::raft::region_runtime::RegionRuntime) -> KvTarget`（字段构造保持现状：`Arc::clone(&rt.mvcc)`、`Some(Arc::new(rt.raft.clone()))`、`Some(rt.raft_log_store.clone())`、`Some(rt.handle())`），`kv_target_for_key` 复用它；新增 `KvRangeSegment` 与 `kv_range_targets`：

```rust
pub(crate) fn kv_range_targets(
    &self,
    start: &[u8],
    end: &[u8],
) -> Result<Vec<KvRangeSegment>, tonic::Status> {
    let Some(manager) = &self.region_manager else {
        return Ok(vec![KvRangeSegment {
            target: self.kv_target_for_key(start)?,
            start: start.to_vec(),
            end: end.to_vec(),
        }]);
    };

    manager
        .resolve_range(start, end)
        .map_err(map_err)?
        .into_iter()
        .map(|seg| {
            let rt = manager.runtime(seg.region_id).ok_or_else(|| {
                map_err(coord_core::error::Error::RegionNotFound {
                    region_id: seg.region_id,
                })
            })?;
            Ok(KvRangeSegment {
                target: kv_target_from_runtime(&rt),
                start: seg.start_key,
                end: seg.end_key,
            })
        })
        .collect()
}
```

2. `kvt/services.rs`：新增 `read_kv_range_segment`（把现有 `range` 里的屏障 + 快照选择逻辑原样搬入，只是把 key/end/limit 换成分段参数）：

```rust
async fn read_kv_range_segment(
    node: &CoordNode,
    seg: KvRangeSegment,
    read_ts: u64,
    scan_limit: usize,
) -> Result<Vec<kv_v2::KeyValue>, Status> {
    let target = &seg.target;
    node.ensure_linearizable_on(
        target.raft.as_deref(),
        target.raft_log_store.as_ref(),
        target.region_id,
    )
    .await?;

    let current_now = target.mvcc.current_revision();
    let rows = if read_ts == 0 {
        // 0 早于任何 commit；changelog 的 0 水位会被当成已压缩。
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

3. 重写 `Kv::range` 的读取段（`read_ts` 校验与 `limit` 解析保持原样）：

```rust
let single_key = RangeSemantics::of(&req.key, &req.range_end).is_single_key();
let end = if single_key {
    single_key_end(&req.key)
} else {
    req.range_end.clone()
};

let safe_ts = self.safe_ts();
let read_ts = if req.read_ts == 0 {
    safe_ts
} else {
    req.read_ts as u64
};
if read_ts > safe_ts {
    wait_for_safe_ts(self, read_ts, self.kvt().config().read_ts_wait_ms).await?;
}

// 空区间（非单键且 end <= key）：不路由、不读，但仍回显已解析的 read_ts。
if !single_key && end <= req.key {
    return Ok(Response::new(kv_v2::RangeResponse {
        kvs: Vec::new(),
        count: 0,
        read_ts: read_ts as i64,
    }));
}

let scan_limit = if req.count_only {
    if max_range == 0 { 0 } else { max_range.saturating_add(1) }
} else {
    limit
};
let segments = self.kv_range_targets(&req.key, &end)?;
let reads = segments
    .into_iter()
    .map(|seg| read_kv_range_segment(self, seg, read_ts, scan_limit));
let parts = futures::future::try_join_all(reads).await?;
let mut kvs: Vec<kv_v2::KeyValue> = parts.into_iter().flatten().collect();

let count = kvs.len() as i64;
Ok(Response::new(kv_v2::RangeResponse {
    kvs,
    count,
    read_ts: read_ts as i64,
}))
```

本任务只保证「同一 `read_ts` + 有序拼接 + 边界/空快照」；`limit`/`count_only`/`keys_only`/对象过滤的合并口径在 Task 3 完成（因此 `count` 暂时等于已读行数）。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server --test kv_v2_range_fanout_test && cargo test -p coord-server --test kv_v2_single_region_test`
Expected: PASS（新用例 + G1 单 Region 回归）

- [ ] **Step 5: Commit**

```bash
git add coord-server/src/server/mod.rs coord-server/src/kvt/services.rs coord-server/tests/kv_v2_range_fanout_test.rs
git commit -m "feat(kv-t): fan out v2 range across regions at one read_ts (#143)"
```

### Task 3: 合并口径（全局 limit / count_only / keys_only / 对象过滤）

**Files:**
- Modify: `coord-server/src/kvt/services.rs`
- Test: `coord-server/tests/kv_v2_range_fanout_test.rs`

**Interfaces:**
- Consumes: Task 2 的 `kv_range_targets` / `read_kv_range_segment`
- 行为固定：`limit` 是整请求上限（按 key 序取前 N）；`count_only` 的 `count` 为全部命中数，`max_range > 0 && count > max_range` → `INVALID_ARGUMENT + READ_SET_TOO_LARGE`；`keys_only` 清空 value；`object_enabled()` 时过滤 `/obj/` key

- [ ] **Step 1: 写失败测试**（追加到 `coord-server/tests/kv_v2_range_fanout_test.rs`）

```rust
#[tokio::test]
async fn test_range_limit_count_and_keys_only_are_global() {
    // 8 个 key（见 Task 2 的 standard_data），safe_ts = 400。
    let fx = standard_data().await;
    let r = fx.range(b"a", b"zzz", 400, 3, false, false).await.unwrap();
    assert_eq!(keys(&r), vec![b"a".to_vec(), b"a2".to_vec(), b"a3".to_vec()]);
    assert_eq!(r.count, 3, "limit 是整请求上限，不是每段上限");

    let r = fx.range(b"a", b"zzz", 400, 0, true, false).await.unwrap();
    assert_eq!(r.kvs.len(), 8);
    assert!(r.kvs.iter().all(|kv| kv.value.is_empty()));

    let r = fx.range(b"a", b"zzz", 400, 0, false, true).await.unwrap();
    assert_eq!(r.count, 8);
    assert!(r.kvs.is_empty());
}

#[tokio::test]
async fn test_range_count_only_caps_across_segments() {
    // 独立夹具：max_range = 4，5 个 key 分布在 3 个 Region。
    let fx = FanoutNode::start(4).await;
    // r1: a@100,b@100；r2: d@100,e@100；r3: n@100
    let err = fx.range(b"a", b"zzz", 0, 0, false, true).await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert_eq!(error_code_of(&err).as_deref(), Some("READ_SET_TOO_LARGE"));

    let err = fx.range(b"a", b"zzz", 0, 5, false, false).await.unwrap_err();
    assert_eq!(error_code_of(&err).as_deref(), Some("READ_SET_TOO_LARGE"));

    let r = fx.range(b"a", b"zzz", 0, 4, false, false).await.unwrap();
    assert_eq!(r.kvs.len(), 4);
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server --test kv_v2_range_fanout_test test_range_limit_count_and_keys_only_are_global test_range_count_only_caps_across_segments`
Expected: FAIL（`limit=3` 返回 6 条/`count` 不是 8；count_only 返回 5 条或错误码不是 `READ_SET_TOO_LARGE`）

- [ ] **Step 3: 实现**

在 Task 2 的拼接之后、返回之前补上：

```rust
kvs.dedup_by(|a, b| a.key == b.key);
if self.object_enabled() {
    kvs.retain(|kv| !crate::storage::object_store::key_in_object_space(&kv.key));
}

if req.count_only {
    if max_range > 0 && kvs.len() > max_range {
        return Err(attach(
            Status::invalid_argument(format!(
                "range scan exceeds max {max_range}; narrow the range"
            )),
            CoordErrorCode::ReadSetTooLarge,
        ));
    }
} else if limit > 0 && kvs.len() > limit {
    kvs.truncate(limit);
}

let count = kvs.len() as i64;
if req.keys_only {
    for kv in &mut kvs {
        kv.value.clear();
    }
}
if req.count_only {
    kvs.clear();
}
```

注意：`limit=0` 表示「不限制」（配置 `max_range=0` 时的既有语义），不得 `truncate(0)`。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server --test kv_v2_range_fanout_test`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add coord-server/src/kvt/services.rs coord-server/tests/kv_v2_range_fanout_test.rs
git commit -m "feat(kv-t): enforce global range limits and counts across regions (#143)"
```

### Task 4: 失败语义、v1 回归与全量门禁

**Files:**
- Test: `coord-server/tests/kv_v2_range_fanout_test.rs`
- Verify: 全仓门禁

**Interfaces:**
- Consumes: Task 2/3 的 handler 行为
- 行为固定：分段解析/读取失败 → 整个请求错误（无部分结果）；v1 跨 Region Range 保持 `INVALID_ARGUMENT`

- [ ] **Step 1: 写测试**

```rust
#[tokio::test]
async fn test_range_fails_closed_when_a_region_is_missing() {
    let fx = standard_data().await;
    fx.manager.remove_region_runtime(2).unwrap();
    let err = fx.range(b"a", b"zzz", 400, 0, false, false).await.unwrap_err();
    assert_eq!(err.code(), Code::Unavailable, "缺口必须整请求失败（可重试），不能返回前缀结果");
}

#[tokio::test]
async fn test_v1_cross_region_range_is_still_rejected() {
    let fx = standard_data().await;
    let err = coord_proto::kv::kv_server::Kv::range(
        &*fx.node,
        tonic::Request::new(coord_proto::kv::RangeRequest {
            key: b"a".to_vec(),
            range_end: b"zzz".to_vec(),
            ..Default::default()
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
}
```

- [ ] **Step 2: 跑测试**

Run: `cargo test -p coord-server --test kv_v2_range_fanout_test test_range_fails_closed_when_a_region_is_missing test_v1_cross_region_range_is_still_rejected`
Expected: PASS。说明：`v1` 用例在 Task 2 之前就已通过（它只钉住既有行为），无法制造失败首跑；在 ledger 记 `Ruling`。`fails_closed` 用例依赖 `resolve_range` 的缺口拒绝（Task 1），实现后应通过。

- [ ] **Step 3: 跑相关套件**

Run:
```bash
cargo test -p coord-server --test kv_v2_range_fanout_test
cargo test -p coord-server --test region_manager_test
cargo test -p coord-server --test kv_v2_single_region_test
cargo test -p coord-server --test region_kv_routing_test
cargo test -p coord --test kv_t_g1_kv_rpc_test
cargo test -p coord --test kv_t_g2_safe_ts_test
```
Expected: 全部 PASS（`region_kv_routing_test` 的 v1 跨区拒绝不变）

- [ ] **Step 4: 跑仓库门禁**

Run:
```bash
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
bash scripts/check-panics.sh
cargo audit --no-fetch
cargo deny check
bash scripts/check-doc-refs.sh
cargo test --workspace --no-fail-fast -- --test-threads=4
```
Expected: 全部通过。已知的既有并行 flake（`approval_rpc_test` / `pd_operator_executor_test` 等在满载下）单独重跑确认；不得跳过任一检查。

- [ ] **Step 5: Commit**

```bash
git add coord-server/tests/kv_v2_range_fanout_test.rs
git commit -m "test(kv-t): cover fan-out failure and v1 range regression (#143)"
```
