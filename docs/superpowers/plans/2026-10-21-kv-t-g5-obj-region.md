# KV-T G5 (#165) `/obj/` Fixed Region Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把保留命名空间 `[/obj/, /obj0)` 在权威 Region 表种子期切成**固定独立 Region**，并让 PD 调度器永不产生命中它的 split/merge operator——对象 manifest 与 chunk 因此不参与任意 KV split/merge。

**Architecture:** 切分是纯计算（`storage::region_table::carve_object_region`），在 `coord/src/main.rs` 由 `[multi_raft].initial_regions` 派生启动 Region 种子时调用一次（`[kv_t].enabled && [object_storage].enabled`）；派生出的种子同时喂给运行时装配、PD meta store 与 region 0 表种子，因此三处天然一致。调度器与 executor 的计划校验各自 fail-closed：前者不产生 operator，后者拒绝执行。

**Tech Stack:** Rust / openraft / redb / tokio（无新依赖）。

**Spec:** `docs/adr/0015-kv-t.md` D6（`/obj/` 例外）、D9（固定口径表）、D11.9；issue #165（`gh issue view 165`）；边界 `docs/production/ops/boundaries.md` B-KVT-3 / B-RG-1。

## Global Constraints

- `[kv_t].enabled=false`（默认）时不得改变 v1 行为（ADR-0001）：不切分、不写表、v1 静态 `initial_regions` 拓扑字节级不变。
- `[object_storage].enabled=false` 时不切分：`/obj/` 只是普通前缀（现有 `guard_object_write_key` 亦只在对象存储启用时生效）。
- 保留空间口径单点定义：`storage::object_store::{OBJECT_PREFIX, key_in_object_space, range_touches_object_space}`；不得在别处硬编码 `/obj0`。
- 表级非法变更降级为 `Rejected`，**不得**让 raft apply 返回 `Err`（D11.5）；本切片只读表。
- 注释只写当前「为什么」；不写日期/变更史；不新增 `TODO`/`FIXME`。
- 临时文件只放 `/tmp/coord-*`；提交前跑 AGENTS.md 门禁（fmt / clippy / check-panics / audit / deny / check-doc-refs）。

## Rulings（本计划对 spec 的落地裁定）

1. **固定 Region 在表种子期切分，不做在线迁移**：v2 是 greenfield（B-KVT-2），表种子是唯一拓扑入口；已初始化的表不会被 `Init` 重写（`RegionTable::init` 幂等 no-op），因此「已有表但缺固定对象 Region」按启动错误拒绝，绝不静默改写权威表。
2. **对象 Region id 由种子最大 id + 1 分配（确定性）**：所有节点用同一份配置算出同一个 id；PD 的 `allocate_region_id` 以 meta store 最大 id + 1 递增，种子先落 PD，故后续 split 不会撞 id。
3. **切分保留父 Region 的 id 与 epoch**：左段沿用父 id，右段拿新 id；三段的 `epoch`/`peers` 继承父 Region。种子期 epoch 是初始值，不推进 version（没有发生过 split/merge）。
4. **merge/split 守卫维持现有判据**（merge 拒绝形态恰好是 `[/obj/, /obj0)` 的一侧；split 拒绝落在保留空间的 `split_key`）：切分后区间平铺保证「吸收固定对象 Region」必然被 merge 守卫命中；对象存储关闭时普通 KV 区间跨过 `/obj/` 仍是合法形态（既有用例 `plan_merge(&plain, 1, 2).is_ok()` 明确要求），不能无故锁死。守卫无法知道对象存储是否启用，按表形态判定是唯一自洽口径。
5. **调度器与 executor 双重 fail-closed**：SplitChecker/MergeChecker 不产生 operator（避免 Failed 噪音与 churn），`plan_split`/`plan_merge` 仍拒绝（任何人绕过调度器也不能执行）。

## File Structure

- Modify: `coord-server/src/storage/region_table.rs` — `is_object_region` 转 pub、新增 `carve_object_region`；单测同处。
- Modify: `coord-server/src/pd/scheduler.rs` — `SplitChecker::{check,schedule}`、`MergeChecker::check` 跳过保留空间；单测同处。
- Modify: `coord/src/main.rs` — 启动期按配置切分 `region_seeds`，种子后校验固定对象 Region 存在（fail-closed）。
- Modify: `config.example.toml` — `[object_storage]`/`[kv_t]` 注释登记「kvt + 对象存储会把 `/obj/` 切成固定 Region」。
- Test: `coord-server/tests/kv_t_g5_obj_region_test.rs` — 真实 region 0 raft + 生命周期：manifest 路由进固定 Region、读写不受影响、split/merge 计划被拒。
- Modify: `docs/production/ops/boundaries.md` — B-KVT-3 / B-RG-1 现状。
- Modify: `docs/adr/0015-kv-t.md` — D6 `/obj/` 落地说明、D9 固定口径行。

## 非本切片（#165 保持 open 的部分）

- 对象容量/扩展管理（ADR 明确不在承诺内）：固定 Region 不因 chunk 增长被 split，容量规划走运维/后续设计。
- v2 `coord.storage.v2` 数据面（#166）：本切片只保证 manifest key 的 Region 归属与固定区间。
- 已初始化表（缺固定对象 Region）的在线修补：按 Ruling 1 拒绝启动，不做静默拓扑变更。

## Review Focus

- 用户 key 恰好等于 `[/obj/, /obj0)` 边界（`/obj/`、`/obj0`）时的归属：`/obj0` 必须在**右段**，`/obj/` 必须在对象 Region；两者都不能落进错误 Region。
- 对象存储关闭（`[object_storage].enabled=false`）时 `/obj/` 仍是普通前缀：不切分、`SplitChecker` 不得误跳过跨越它的普通 Region、merge 守卫不得锁死。
- 固定对象 Region 在任何阈值（size/keys，含 chunk 字节）下都不得产生 SplitRegion/MergeRegion operator。
- 已初始化表缺固定对象 Region 时启动必须失败，且失败前不写表、不装配多余 Region。
- 种子切分不得改变既有节点的 v1 拓扑（`[kv_t].enabled=false` 时 `initial_regions` 原样生效）。

---

### Task 1: `carve_object_region`

**Files:**
- Modify: `coord-server/src/storage/region_table.rs`
- Test: 同文件 `#[cfg(test)] mod tests`

**Interfaces:**
- Consumes: `coord_core::types::{RegionEpoch, RegionId, RegionMeta}`；`crate::storage::object_store::{key_in_object_space, range_touches_object_space, OBJECT_PREFIX}`。
- Produces:
  - `pub fn is_object_region(meta: &RegionMeta) -> bool`
  - `pub fn carve_object_region(regions: &mut Vec<RegionMeta>, first_new_id: RegionId) -> Result<RegionId>` — 返回固定对象 Region 的 id；`regions` 保持按 `start_key` 升序。

- [ ] **Step 1: 写失败测试**

```rust
fn meta(id: RegionId, start: &[u8], end: &[u8]) -> RegionMeta { /* 现有 helper 复用 */ }

#[test]
fn carve_splits_the_seed_at_the_object_space() {
    let mut regions = vec![meta(1, b"", b"")];
    let object = carve_object_region(&mut regions, 2).unwrap();
    assert_eq!(object, 2);
    assert_eq!(
        regions
            .iter()
            .map(|m| (m.region_id, m.start_key.clone(), m.end_key.clone()))
            .collect::<Vec<_>>(),
        vec![
            (1, b"".to_vec(), b"/obj/".to_vec()),
            (2, b"/obj/".to_vec(), b"/obj0".to_vec()),
            (3, b"/obj0".to_vec(), b"".to_vec()),
        ]
    );
    // 三段仍按表不变量平铺
    RegionTable { version: 1, regions: regions.clone() }.validate().unwrap();
    assert!(is_object_region(&regions[1]));
}

#[test]
fn carve_is_idempotent_when_the_object_region_exists() {
    let mut regions = vec![
        meta(1, b"", b"/obj/"),
        meta(2, b"/obj/", b"/obj0"),
        meta(3, b"/obj0", b""),
    ];
    let before = regions.clone();
    assert_eq!(carve_object_region(&mut regions, 9).unwrap(), 2);
    assert_eq!(regions, before, "已有固定对象 Region 时不重写 id/epoch");
}

#[test]
fn carve_rejects_seeds_that_do_not_cover_the_object_space() {
    let mut regions = vec![meta(1, b"", b"/obj/m"), meta(2, b"/obj/m", b"")];
    let err = carve_object_region(&mut regions, 3).unwrap_err();
    assert!(err.to_string().contains("/obj/"), "err: {err}");
}

#[test]
fn carve_rejects_id_collision_or_zero() {
    let mut regions = vec![meta(1, b"", b"")];
    assert!(carve_object_region(&mut regions, 1).is_err(), "id 复用");
    assert!(carve_object_region(&mut regions, 0).is_err(), "0 保留");
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server --lib storage::region_table::tests::carve 2>&1 | tail -20`
Expected: 编译失败 `cannot find function carve_object_region`（或 `is_object_region` 不可见）。

- [ ] **Step 3: 实现**

- `is_object_region` 改为 `pub`（保持现有实现与文档注释）。
- `carve_object_region` 语义（纯计算，不读时钟/IO）：
  1. `first_new_id == 0` 或 `<= regions 最大 id` → `Error::InvalidArgument`；
  2. 已存在 `is_object_region` 的区间 → 返回该 id（幂等，不动任何字段）；
  3. 找唯一覆盖整个保留空间的 Region：`start <= b"/obj/"` 且（`end.is_empty()` 或 `end >= b"/obj0"`）；找不到 → `Error::InvalidArgument`（消息含 `[/obj/, /obj0)`）；
  4. 用父 Region 的 `epoch`/`peers` 生成 `object`（id = `first_new_id`，range = `[/obj/, /obj0)`）与需要时的 `right`（id = `first_new_id + 1`，range = `[/obj0, parent.end)`），`approximate_size/keys = 0`；
  5. 父 Region 收缩为左段（`end = b"/obj/"`，id/epoch/peers 不变，`approximate_size/keys` 保留）；
  6. 空段（`start == end`）不插入；重排 `regions` 按 `start_key` 升序；返回对象 Region id。
- 不改 `plan_merge`/`plan_split` 判据：切分后表平铺保证「吸收固定对象 Region」必然被既有 merge 守卫命中；对象存储关闭时跨 `/obj/` 的普通区间仍可 merge/split（既有用例锁定该行为）。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server --lib storage::region_table 2>&1 | tail -20`
Expected: 全部 PASS（含既有 `plan_merge`/`plan_split`/表不变量用例）。

- [ ] **Step 5: 提交**

```bash
git add coord-server/src/storage/region_table.rs
git commit -m "feat(kv-t): carve [/obj/, /obj0) as a fixed Region interval"
```

---

### Task 2: PD 调度器跳过固定对象 Region

**Files:**
- Modify: `coord-server/src/pd/scheduler.rs`
- Test: 同文件 `#[cfg(test)] mod tests`

**Interfaces:**
- Consumes: `crate::storage::region_table::is_object_region`、`crate::storage::object_store::{key_in_object_space, range_touches_object_space}`。
- Produces: `SplitChecker::check` / `SplitChecker::schedule` / `MergeChecker::check` 对保留空间的行为（无新公开符号）。

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn test_obj_region_not_split() {
    let cfg = PdConfig::default();
    let checker = SplitChecker::new(&cfg);
    let object = RegionMeta { /* id 2, [/obj/, /obj0), size = usize::MAX/2, keys = u64::MAX */ };
    assert!(checker.check(&object, b"/obj/m".to_vec()).is_none());
    let ctx = ScheduleContext::with_regions(vec![object]);
    assert!(checker.schedule(&ctx).is_empty(), "固定对象 Region 永不 split");
}

#[test]
fn test_split_checker_skips_object_space_split_key() {
    let cfg = PdConfig::default();
    let checker = SplitChecker::new(&cfg);
    // 遗留表：Region 跨越保留空间，采样点落在 /obj/ 内
    let region = RegionMeta { /* id 1, [b"", b""), size 超阈值 */ };
    assert!(checker.check(&region, b"/obj/m/k".to_vec()).is_none());
    // 同一 Region 的非保留空间 split_key 仍可 split
    assert!(matches!(checker.check(&region, b"k".to_vec()), Some(Operator::SplitRegion { .. })));
}

#[test]
fn test_merge_checker_skips_object_region() {
    let cfg = PdConfig::default();
    let checker = MergeChecker::new(&cfg);
    let left = small(b"", b"/obj/");
    let object = small(b"/obj/", b"/obj0");
    assert!(checker.check(&left, &object).is_none());
    // 遗留跨越区间的相邻对也不合并
    let straddle = small(b"", b"/obj/x");
    let right = small(b"/obj/x", b"");
    assert!(checker.check(&straddle, &right).is_none());
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server --lib pd::scheduler::tests::test_obj_region_not_split 2>&1 | tail -20`
Expected: FAIL —— 对象 Region 仍产生 `SplitRegion`（`check` 返回 `Some`）。

- [ ] **Step 3: 实现**

- `SplitChecker::check`：`if is_object_region(region) || key_in_object_space(&split_key) { return None; }`（在阈值判断之前；跳过原因是保留空间，与阈值无关）。
- `SplitChecker::schedule`：算出 `split_key` 后同样判定，命中则 `continue`（不产生 operator，也不退化成别的 key——换 key 是静默改计划）。
- `MergeChecker::check`：`if is_object_region(left) || is_object_region(right) || range_touches_object_space(&left.start_key, &right.end_key) { return None; }`。
- 注释写「为什么」：对象 manifest/chunk 不随 KV split/merge 迁移（D6/D11.9）；调度器不产生 operator 是为了不让 executor 反复 Failed。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server --lib pd::scheduler 2>&1 | tail -20`
Expected: 全部 PASS（含既有 split/merge/hotspot 用例）。

- [ ] **Step 5: 提交**

```bash
git add coord-server/src/pd/scheduler.rs
git commit -m "feat(kv-t): keep PD schedulers off the fixed /obj/ region"
```

---

### Task 3: 启动接线——种子期切分与 fail-closed 校验

**Files:**
- Modify: `coord/src/main.rs`（`region_seeds` 派生后、region 表种子校验后）
- Modify: `config.example.toml`

**Interfaces:**
- Consumes: `coord_server::storage::region_table::carve_object_region`、`is_object_region`。
- Produces: 无新公开符号；`[kv_t].enabled && [object_storage].enabled` 时 `region_seeds` 含固定对象 Region。

- [ ] **Step 1: 接线（先写实现，Task 4 的集成测试是它的验证）**

在 `region_peers` 计算之后：

```rust
let mut region_seeds = region_seeds;
if cfg.kv_t.enabled && cfg.object_storage.enabled {
    let mut metas: Vec<RegionMeta> = region_seeds
        .iter()
        .map(|seed| RegionMeta {
            region_id: seed.region_id,
            start_key: seed.start_key.clone(),
            end_key: seed.end_key.clone(),
            epoch: RegionEpoch::initial(),
            peers: region_peers.clone(),
            approximate_size: 0,
            approximate_keys: 0,
        })
        .collect();
    let first_new_id = metas.iter().map(|m| m.region_id).max().unwrap_or(0) + 1;
    let object_region = coord_server::storage::region_table::carve_object_region(
        &mut metas,
        first_new_id,
    )
    .map_err(|e| format!("carve [/obj/, /obj0) out of multi_raft.initial_regions: {e}"))?;
    tracing::info!(object_region, "fixed /obj/ region carved from initial_regions");
    region_seeds = metas
        .into_iter()
        .map(|m| RegionSeed {
            region_id: m.region_id,
            start_key: m.start_key,
            end_key: m.end_key,
        })
        .collect();
}
```

在 6.6a 的 `seed_region_table(...)` + `refresh_region_table()` 之后加校验（同一 `if cfg.kv_t.enabled` 分支内）：

```rust
if cfg.object_storage.enabled {
    let table = node.storage.region_table().map_err(|e| format!("read region table: {e}"))?;
    if !table.regions.iter().any(coord_server::storage::region_table::is_object_region) {
        return Err("region table is already initialized without the fixed /obj/ region; \
                    enabling [object_storage] on an existing [kv_t] cluster is not supported \
                    (no silent topology rewrite)"
            .into());
    }
}
```

- `config.example.toml`：在 `[object_storage]` 段头注与 `[kv_t]` 段头注各加一行——「与 `[kv_t].enabled=true` 同时启用时，启动期把 `[/obj/, /obj0)` 从 `initial_regions` 切成独立固定 Region（不参与 split/merge）；已初始化的表缺该区间会被拒绝启动」。

- [ ] **Step 2: 编译与既有测试**

Run: `cargo build -p coord 2>&1 | tail -20 && cargo test -p coord --lib config 2>&1 | tail -20`
Expected: 编译通过；config 用例 PASS（切分只在运行期装配，不改变配置校验语义）。

- [ ] **Step 3: 提交**

```bash
git add coord/src/main.rs config.example.toml
git commit -m "feat(kv-t): seed the fixed /obj/ region when object storage is on"
```

---

### Task 4: 集成验收——manifest 路由进固定 Region

**Files:**
- Create: `coord-server/tests/kv_t_g5_obj_region_test.rs`
- Test: 同文件

**Interfaces:**
- Consumes: `raft::region::{spawn_configured_regions, RegionSeed}`、`raft::region_lifecycle::{RegionLifecycle, RegionLifecycleSpec}`、`storage::region_table::{carve_object_region, is_object_region, plan_split, plan_merge, RegionChange}`、`storage::object_store::manifest_key`。
- Produces: 无（验收套件）。

- [ ] **Step 1: 写失败测试**

用例（沿用 `kv_t_g5_region_lifecycle_test.rs` 的 harness 形态：单节点 region 0 system raft + `RegionLifecycle`）：

```rust
#[tokio::test(flavor = "multi_thread")]
async fn test_obj_region_not_split() {
    let harness = Harness::boot().await;   // 种子 = carve([b"", b""], 2) → 3 个 Region
    // 1. 三段落表：[/obj/, /obj0) 是独立 Region 2
    let table = harness.init_table().await;
    assert!(is_object_region(table.get(OBJECT_REGION).unwrap()));
    // 2. manifest key 路由进对象 Region，读写闭环
    let key = manifest_key(b"bucket", b"object");
    let runtime = harness.wait_region_leader(OBJECT_REGION).await;
    assert_eq!(harness.manager.route_runtime(&key).unwrap().region_id(), OBJECT_REGION);
    harness.put_v2(&runtime, &key, b"manifest-body", 10).await;
    assert_eq!(runtime.mvcc.get(&key).unwrap().as_deref(), Some(&b"manifest-body"[..]));
    // 3. 普通 KV key 不进对象 Region
    let plain = b"/kv/user-key";
    assert_ne!(harness.manager.route_runtime(plain).unwrap().region_id(), OBJECT_REGION);
    // 4. 固定区间不参与 split/merge
    assert!(plan_split(&table, OBJECT_REGION, b"/obj/m", 9).is_err());
    assert!(plan_merge(&table, LEFT, OBJECT_REGION).is_err());
    assert!(plan_merge(&table, OBJECT_REGION, RIGHT).is_err());
    // 5. 边界归属：/obj/ 属对象 Region，/obj0 属右段
    assert_eq!(harness.manager.route_runtime(b"/obj/").unwrap().region_id(), OBJECT_REGION);
    assert_eq!(harness.manager.route_runtime(b"/obj0").unwrap().region_id(), RIGHT);
}
```

- [ ] **Step 2: 跑测试确认失败（先跑，确认 harness 缺实现/断言未过）**

Run: `cargo test -p coord-server --test kv_t_g5_obj_region_test 2>&1 | tail -20`
Expected: FAIL —— harness 与用例断言尚未满足（例如对象 Region 未装配 / 路由到父 Region）。

- [ ] **Step 3: 实现 harness**

- `region(id, start, end)`、`raft_config()`、`bind_listener()` 沿用 lifecycle 套件；
- `boot()`：region 0 system raft → `region_seeds = carve([b"", b""], 2)` → `spawn_configured_regions`（`RevisionMode::CommitTs`、`object_store: None`）→ `RegionLifecycle` 接管 `ensure_region`；
- `init_table()`：`Command::RegionMeta(RegionMetaOp::Init { regions })` 经 region 0 raft 提交，返回应用后的 `RegionTable`；
- `put_v2()`：`Command::PutV2 { commit_ts }` + 等 leader 重试（复制 lifecycle 套件写法）。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server --test kv_t_g5_obj_region_test 2>&1 | tail -30`
Expected: PASS 1 test。

- [ ] **Step 5: 提交**

```bash
git add coord-server/tests/kv_t_g5_obj_region_test.rs
git commit -m "test(kv-t): prove the fixed /obj/ region owns object manifests"
```

---

### Task 5: 文档、门禁与 issue 证据

**Files:**
- Modify: `docs/production/ops/boundaries.md`（B-KVT-3、B-RG-1）
- Modify: `docs/adr/0015-kv-t.md`（D6 `/obj/` 落地行、D9 固定口径行）

- [ ] **Step 1: 文档按当前态改写**

- B-KVT-3：`[kv_t].enabled && [object_storage].enabled` 时启动期把 `[/obj/, /obj0)` 从 `initial_regions` 切成固定 Region（id = 种子最大 id + 1），SplitChecker/MergeChecker 跳过，`plan_split`/`plan_merge` 仍拒绝；chunk 随 Region 目录存放、不迁移；容量/扩展不在 ADR 承诺内；已初始化表缺该区间 → 启动拒绝（不静默改表）。判据加上 `coord-server/tests/kv_t_g5_obj_region_test.rs`、`pd::scheduler::tests::test_obj_region_not_split`、`storage::region_table::tests::carve_*`。
- B-RG-1：把「`/obj/` 固定区间细节……（#165 未交付）」改为「`/obj/` 固定区间由 #165 在表种子期切分；真实 split/merge 数据搬迁仍未交付（#163/#164）」。
- ADR-0015 D6 `/obj/` 行与 D9 表行：补一句「固定 Region 在表种子期切出；SplitChecker/MergeChecker 跳过」。

- [ ] **Step 2: 文档引用检查**

Run: `bash scripts/check-doc-refs.sh`
Expected: `dangling=0`（或脚本输出无悬空路径）。

- [ ] **Step 3: 全量门禁**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
bash scripts/check-panics.sh
cargo audit
cargo deny check
cargo test -p coord-server --lib 2>&1 | tail -5
cargo test -p coord-server --test kv_t_g5_obj_region_test --test kv_t_g5_region_lifecycle_test \
  --test kv_t_g5_region_table_test --test kv_t_g5_split_merge_test 2>&1 | tail -10
```

Expected: 全部通过（clippy 无 warning；audit/deny 无新增 advisory/license 问题）。

- [ ] **Step 4: 提交并开 PR**

```bash
git add docs/production/ops/boundaries.md docs/adr/0015-kv-t.md
git commit -m "docs(kv-t): record the fixed /obj/ region (#165)"
git push -u origin feat/kv-t-g5-obj-region
gh pr create --base main --title "feat(kv-t): carve [/obj/, /obj0) as a fixed Region (#165)" \
  --body-file /tmp/coord-pr/body.md
gh pr checks --watch
gh pr merge --merge --delete-branch
```
