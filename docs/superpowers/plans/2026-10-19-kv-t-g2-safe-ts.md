# KV-T G2 全局 SafeTs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 落地 issue #142——`safe_ts` 成为「所有数据 Region 已 apply 的 commit_ts 水位最小值」，并通过 region 0 发布 + 各 Region raft fence 持续推进；v2 Range 的 `read_ts=0` 与显式 `read_ts` 等待都以它为准，compaction 水位不再越过它。

**Architecture:** region 0 leader 周期性把 TSO 高水位（下一未发放值）作为 **fence** 经 region 0 raft 发布（`/_sys/kv_t/safe_ts_fence`，节点收敛；随 `/_sys/*` 快照恢复）。每个节点对本节点为 leader 的数据 Region 提案 `AdvanceSafeTs{fence}`：只把 `META_CURRENT_REVISION` 抬到 fence（不写数据/changelog），因此“fence 之后不可能再出现 `commit_ts <= fence` 的成功写入”。`safe_ts = min(各数据 Region current_revision)`（legacy = region 0 storage 的 current_revision）；`GetTs` 从 published fence 之上取值，避免发放必被拒的旧值。

**Tech Stack:** Rust、openraft/redb、tonic/prost、coord-proto v2。

**Spec:** `docs/adr/0015-kv-t.md` D3/D11/D12.3/D12.4 + issue #142；V2 读语义以 `apis/contracts/proto/coord/kv/v2/kv.proto` 头注为准。

## Global Constraints

- `[kv_t].enabled=false`（默认）时 v1 行为、revision（= applied index）、compaction、服务注册不变（ADR-0001）。
- `safe_ts` 绝不越过任一数据 Region 已 apply 的水位；用户级拒绝不得让 raft apply 返回 `Err`（D11.5）。
- `commit_ts` 全局唯一单调；fence 只抬水位，不能让同 key `mod_revision` 倒退（D11.2）。
- 不新增 TODO/FIXME；临时文件只在 `/tmp`；注释只写当前「为什么」；文档引用 0 悬空。
- 提交前本地跑：`cargo fmt --all -- --check`、`cargo clippy --workspace --lib --bins -- -D warnings`、`bash scripts/check-panics.sh`、`cargo test --workspace --no-fail-fast`、`cargo audit --no-fetch`、`cargo deny check`、`bash scripts/check-doc-refs.sh`。

## Rulings

- **fence 值是 TSO 分配边界，不是某个 key 的 commit_ts**：`META_CURRENT_REVISION` 在 v2 下是“已 apply 水位”，允许被 safe-point fence 抬到未发放的边界值；不会产生该 revision 的 key/changelog，历史读落在无写入窗口时取实时状态。`commit_ts` 的唯一性/单调仍由 TSO 保证。
- **为什么不会“越过未 apply”**：fence 后的写入若 `commit_ts <= fence` 会被 `accept_commit_ts` 拒绝；若写入先于 fence 进日志则先 apply，fence 只做 `max`。因此 `safe_ts = min(current)` 达到 F 时，所有 Region 对 `<=F` 的成功写入都已完成。
- **in-flight CommitDecision 归 #148**：G2 没有 sequencer，直写只有在 apply 后才算成功；因此 fence 不会越过已决策未 apply 的多 key 提交。#148 必须在 fence 候选上叠 `min(in_flight commit_ts)`，本次在 `advance_once` 里留出单一候选计算点。
- **节点本地聚合而非 region 0 全局值**：v1 静态复制保证每个节点持有全部 Region 副本，`min(本地副本 current_revision)` 是全局 safe_ts 的下界（滞后只会更保守）；#159 动态 Region 表落地后必须改为 region 0 汇总。
- **fence 只在 Region leader 上提案**：follower 的 current 由 raft apply 收敛；无 leader 的 Region 会钉住 safe_ts，直到重新选出 leader（保守，不静默跳过）。
- **leader 切换**：fence 与 current 都在持久化状态机里（region 0 fence + per-Region META），不依赖 leader 内存；真实 kill/failover 组合验证归 #168/#175。
- **空闲 Region 不永久钉住**：每轮 fence 会把它抬到 published fence；`GetTs` 跳过 `<= fence` 的缓存值（TSO 允许空洞）。
- **compaction 水位 = safe_ts**（ADR D3 / #139 的过渡口径在本次收口）：cutoff = `min(safe_ts, current) - retention`，绝不删除 `>= safe_ts` 的历史。
- **不新增配置项**：advance 周期用常量 `1s`（100k revision retention ≈ 27.8h，覆盖 Watch 24h 默认；真正 time-based retention 归 #157）。

## Review Focus

1. **拒绝承诺**：fence 发布后，`commit_ts <= min(current)` 的 Put/Delete 必须 `COMMIT_REJECTED`，且 raft apply 不返回 `Err`。
2. **`read_ts=0` 不越过安全点**：header 必须等于 `safe_ts`，返回行的 `mod_revision <= safe_ts`；落后 Region 存在时不得返回领先 Region 的新值。
3. **显式 `read_ts` 有界等待**：`read_ts > safe_ts` 时等待到全局水位（不是只看目标 Region），超时 `UNAVAILABLE + SAFE_TS_LAG`。
4. **GetTs 不发放陈旧值**：返回值必须 `> published fence`（跳过的值允许成为空洞）。
5. **v1 零影响**：`[kv_t].enabled=false` 时无 advancer、无 fence、compaction 仍按 applied index；`RevisionMode::LogIndex` 的写入顺序不变。
6. **重放/重启安全**：两个新命令按 log index 幂等（重复 apply 不重复抬水位/不报错）；重启后 fence 与 current 从持久化状态恢复。
7. **空闲 Region**：一轮 advance 后，即使 Region 没有任何数据，也不得让 safe_ts 永久停在 0。
8. **region 0 不参与 min**：system store 的 revision 不因 v2 数据写入推进，不能被算进 safe_ts。

---

### Task 1: 水位原语与 raft 命令

**Files:**
- Modify: `coord-server/src/storage/mvcc.rs`
- Modify: `coord-server/src/raft/type_config.rs`
- Modify: `coord-server/src/raft/state_machine.rs`

**Interfaces:**
- Produces: `MvccStorage::advance_commit_ts_watermark(&self, commit_ts: Revision, applied: AppliedLogId) -> Result<Revision>`
- Produces: `MvccStorage::set_safe_ts_fence(&self, fence_ts: Revision, applied: AppliedLogId) -> Result<Revision>`
- Produces: `MvccStorage::safe_ts_fence(&self) -> Result<Revision>`（缺失 = 0）
- Produces: `Command::PublishSafeTs { fence_ts: u64 }` / `Command::AdvanceSafeTs { commit_ts: u64 }`
- Produces: `Response::SafeTsPublished { fence_ts: u64 }` / `Response::SafeTsAdvanced { revision: u64 }`

- [ ] **Step 1: 写失败测试**（`storage/mvcc.rs` tests）

```rust
#[test]
fn test_advance_commit_ts_watermark_is_monotonic_persistent_and_replay_safe() {
    // put(k, v, 100)；advance(150, applied=1) → current=150，k 仍可读
    // changelog 不含 150；advance(120, applied=2) → 仍是 150（不倒退）
    // 重开同一 backend → current=150；put_at_commit_ts(149) → CommitRejected
}

#[test]
fn test_safe_ts_fence_is_monotonic_and_persistent() {
    // fence 缺失 = 0；set(900, applied=1) → 900
    // set(800, applied=2) → 仍是 900；重开 → 900
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server --lib storage::mvcc::tests::test_advance_commit_ts_watermark storage::mvcc::tests::test_safe_ts_fence_is_monotonic`
Expected: FAIL（方法不存在）

- [ ] **Step 3: 实现**

- 新增 `const SAFE_TS_FENCE: &[u8] = b"/_sys/kv_t/safe_ts_fence";`（放 `/_sys/` 以随快照整域恢复，避免恢复后已有 fence 回退）。
- `advance_commit_ts_watermark`：单写事务内 `note_log_index_revision(tx, applied.index)`（LogIndex 下保持旧语义）、把 `META_CURRENT_REVISION` 抬到 `max(current, commit_ts)`、写 `META_LAST_APPLIED`；返回 effective。写事务前若 `applied.index <= 已持久化 META_LAST_APPLIED.index` 则直接返回 `current_revision()`（重放幂等）。
- `set_safe_ts_fence`：同样的 applied-index 幂等守卫；事务内把 `SAFE_TS_FENCE` 抬到 `max(prev, fence_ts)`，写 applied；返回 effective。
- **不写 changelog**：fence 不是 commit，不能产生空 ChangeEvent（否则 CommitTs 模式下 log-index 键可能与某个合法 `commit_ts` 撞键，把正常写误判为 `CommitRejected`）。幂等只依赖 `META_LAST_APPLIED`；两者都不上 Watch。
- `type_config.rs`：两个 Command 变体**末尾追加**，Display 加分支；两个 Response 变体末尾追加，Display 加分支。
- `state_machine.rs::apply`：`PublishSafeTs` → `sm.set_safe_ts_fence(...)`；`AdvanceSafeTs` → `sm.advance_commit_ts_watermark(...)`；都返回 `(response, None)`。
- 测试补齐 apply 路径（`state_machine.rs` tests）：apply 后 current/fence 生效；同一 applied 重放不报错、不重复抬水位。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server --lib storage::mvcc::tests && cargo test -p coord-server --lib raft::state_machine::tests`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add coord-server/src/storage/mvcc.rs coord-server/src/raft/type_config.rs coord-server/src/raft/state_machine.rs
git commit -m "feat(kv-t): add safe-ts fence and applied-watermark commands (#142)"
```

### Task 2: safe_ts 观测、指标与多 Region 测试夹具

**Files:**
- Modify: `coord-server/src/raft/region.rs`
- Modify: `coord-server/src/server/mod.rs`
- Modify: `coord-server/src/metrics.rs`
- Test: `coord/tests/kv_t_g2_safe_ts_test.rs`（新建）

**Interfaces:**
- Produces: `RegionManager::list_runtimes(&self) -> Vec<Arc<RegionRuntime>>`
- Produces: `CoordNode::safe_ts(&self) -> u64`（计算 + 更新指标；legacy = `storage.current_revision()`）
- Produces: `CoordNode::safe_ts_fence(&self) -> u64`（region 0 已发布 fence；legacy = 0）
- Produces: `Metrics::{set_kv_t_safe_ts, set_kv_t_safe_ts_fence, inc_kv_v2_safe_ts_wait}`
- Produces: `RegionMetrics::{applied_revision, apply_lag}` 与 Prometheus 行 `coord_region_applied_revision` / `coord_region_apply_lag`

- [ ] **Step 1: 写失败测试**（`coord/tests/kv_t_g2_safe_ts_test.rs`）

```rust
// 夹具：单节点 region 0 raft + 2 个数据 Region（["", "m") / ["m", "")）；
// 全部单 voter、initialize=true；等待三个 raft 选出 leader。
async fn start_g2_node() -> G2Node { ... }

#[tokio::test]
async fn test_safe_ts_is_min_region_revision_and_metrics() {
    // region1 PutV2 apple@100；region2 PutV2 melon@60
    // node.safe_ts() == 60
    // Prometheus 文本含 coord_kv_t_safe_ts 60
    //   coord_region_applied_revision{region_id="1"} 100
    //   coord_region_applied_revision{region_id="2"} 60
    //   coord_region_apply_lag{region_id="1"} 40
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord --test kv_t_g2_safe_ts_test test_safe_ts_is_min`
Expected: FAIL（`CoordNode::safe_ts` 不存在 / 文件不存在）

- [ ] **Step 3: 实现**

- `RegionManager::list_runtimes`：`runtimes.read().values().cloned().collect()`（按 BTreeMap 顺序）。
- `CoordNode::safe_ts`：`region_manager` 为 None → `storage.current_revision()`；Some → `list_runtimes()` 的 `mvcc.current_revision()` 最小值（空列表 = 0，fail-closed）。同时计算 max，更新 `safe_ts` gauge、每个 Region 的 `applied_revision` 与 `apply_lag = max - applied`（`metrics.get_or_create_region_metrics(id)`）。
- `CoordNode::safe_ts_fence`：`storage.safe_ts_fence().unwrap_or(0)`（region 0），同时更新 fence gauge。
- `metrics.rs`：`MetricsInner` 新增 `kv_t_safe_ts`、`kv_t_safe_ts_fence`（gauge）与 `kv_v2_safe_ts_wait_total`、`kv_v2_safe_ts_wait_millis_total`（counter），加 setter/inc 方法与 Prometheus 行；`RegionMetrics` 新增 `applied_revision`、`apply_lag` 与 render 行。
- 夹具建 region 0：`RedbBackend::open` + `MvccStorage::new_with_revision_mode(CommitTs)` + `LogStore` + `StateMachineStore` + `new_raft` + `initialize`（单 voter），不启 gRPC server（单 voter 无需网络）。
- 数据 Region：`RegionManager::spawn_region`，各自 `region_data_dir`，`revision_mode = CommitTs`，peers = node 1。
- `node.region_manager = Some(manager)`；`node.set_kvt_state(KvTState::new(true, config).with_implemented([Kv, Tso]))`；`node.set_metrics`。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord --test kv_t_g2_safe_ts_test test_safe_ts_is_min`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add coord-server/src/raft/region.rs coord-server/src/server/mod.rs coord-server/src/metrics.rs coord/tests/kv_t_g2_safe_ts_test.rs
git commit -m "feat(kv-t): observe global safe-ts and region apply lag (#142)"
```

### Task 3: TSO 发放不越过 published fence

**Files:**
- Modify: `coord-server/src/raft/tso.rs`
- Modify: `coord-server/src/kvt/services.rs`
- Test: `coord/tests/kv_t_g2_safe_ts_test.rs`

**Interfaces:**
- Produces: `TsoClient::get_ts_above(&self, floor: u64) -> Result<u64>`
- `TsoClient::get_ts()` 保持签名，内部委托 `get_ts_above(0)`。
- `coord.tso.v2.GetTs` 使用 `node.safe_ts_fence()` 作为 floor。

- [ ] **Step 1: 写失败测试**

```rust
// coord-server/src/raft/tso.rs tests
#[tokio::test]
async fn test_get_ts_above_skips_values_at_or_below_floor() {
    // FakeSource 批次 [1, 9)：get_ts_above(5) → 6；cached_range().0 == 7
    // floor=20：连续 refill 直到 > 20，返回值 > 20 且只增
}

// coord/tests/kv_t_g2_safe_ts_test.rs
#[tokio::test]
async fn test_get_ts_stays_above_published_fence() {
    // node.storage.set_safe_ts_fence(1025, applied)；node.set_tso_client(FakeTso::new())
    // node.get_ts(...) → ts > 1025
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server --lib raft::tso::tests::test_get_ts_above && cargo test -p coord --test kv_t_g2_safe_ts_test test_get_ts_stays_above`
Expected: FAIL（方法不存在 / ts <= fence）

- [ ] **Step 3: 实现**

- `TsoClient::get_ts_above(floor)`：leadership 检查与 `get_ts` 相同；loop 中在 `needs_refill` 前先判断 `cache.next <= floor`：
  - 若 `cache.end > floor`：`cache.next = floor + 1`（更新 remaining 指标），返回下一个值；
  - 若 `floor >= cache.end`：`discard_batch()` 后 `refill()`，继续。
- `claim` 逻辑复用；跳过值只减 remaining，不回退 end。
- `GetTs` handler：`client.get_ts_above(self.safe_ts_fence()).await`；错误映射不变。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server --lib raft::tso::tests && cargo test -p coord --test kv_t_g2_safe_ts_test test_get_ts_stays_above`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add coord-server/src/raft/tso.rs coord-server/src/kvt/services.rs coord/tests/kv_t_g2_safe_ts_test.rs
git commit -m "feat(kv-t): keep TSO issuance above the published safe-ts fence (#142)"
```

### Task 4: SafeTs advancer（region 0 发布 + 各 Region fence）

**Files:**
- Modify: `coord-server/src/kvt/mod.rs`
- Create: `coord-server/src/kvt/safe_ts.rs`
- Modify: `coord/src/main.rs`
- Test: `coord/tests/kv_t_g2_safe_ts_test.rs`

**Interfaces:**
- Produces: `coord_server::kvt::safe_ts::advance_once(node: &CoordNode) -> Result<usize, String>`（返回本轮 fence 的 Region 数）
- Produces: `coord_server::kvt::safe_ts::start_advancer(node: Arc<CoordNode>, interval: Duration) -> JoinHandle<()>`

- [ ] **Step 1: 写失败测试**（`coord/tests/kv_t_g2_safe_ts_test.rs`）

```rust
#[tokio::test]
async fn test_advance_once_publishes_fence_and_fences_all_regions() {
    // node.storage.apply_tso_allocate(1024, 1, standalone(1)) → high_water=1025
    // advance_once → published fence=1025；两个 Region current=1025
    // node.safe_ts()==1025；两个 Region changelog 无 1025 记录
}

#[tokio::test]
async fn test_idle_regions_do_not_pin_safe_ts() {
    // 不写任何数据；一轮 advance → safe_ts == high_water > 0
}

#[tokio::test]
async fn test_fence_rejects_stale_commit_ts_without_raft_error() {
    // advance_once(fence=F)；直接 client_write(PutV2 commit_ts=F) → Response::CommitRejected
    // raft client_write 仍 Ok（不是 Err）；之后 PutV2(F+1) 成功
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord --test kv_t_g2_safe_ts_test test_advance_once`
Expected: FAIL（模块不存在）

- [ ] **Step 3: 实现**

`advance_once`：
1. 若 `node.raft` 存在且 `current_leader().await == Some(node.node_id)`：读 `node.storage.tso_high_water()`；若大于 `node.storage.safe_ts_fence()`，`client_write(Command::PublishSafeTs { fence_ts })` 并校验响应；失败只返回 `Err(String)`（调用方记录 warning）。
2. 读 `node.safe_ts_fence()`；遍历 `node.region_manager.list_runtimes()`：
   - 跳过 `raft.metrics().borrow_watched().current_leader != Some(node.node_id)` 的 Region；
   - `fence > rt.mvcc.current_revision()` 时 `rt.raft.client_write(Command::AdvanceSafeTs { commit_ts: fence })`，校验 `Response::SafeTsAdvanced`；计数 +1；
   - 单个 Region 失败不中断其他 Region（收集首个错误，全部尝试完再返回）。
3. 结束时调用 `node.safe_ts()` 刷新指标。
4. 注释写清 why：fence 是 TSO 分配边界、必须由 leader 提案、`#148` 会在此叠 in-flight floor。

`start_advancer`：`interval` ticker + `MissedTickBehavior::Skip`；每轮 `advance_once`，失败 `tracing::warn!`；任务随进程生命周期。

`main.rs`：在 node 变成 `Arc` 之后、compaction 启动附近，当 `cfg.kv_t.enabled && node.region_manager.is_some() && node.raft.is_some()` 时 `let _safe_ts_advancer = start_advancer(Arc::clone(&node), Duration::from_secs(1));`。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord --test kv_t_g2_safe_ts_test test_advance_once test_idle_regions test_fence_rejects`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add coord-server/src/kvt/mod.rs coord-server/src/kvt/safe_ts.rs coord/src/main.rs coord/tests/kv_t_g2_safe_ts_test.rs
git commit -m "feat(kv-t): advance safe-ts via region-0 fence publication (#142)"
```

### Task 5: v2 Range 以全局 safe_ts 读/等待

**Files:**
- Modify: `coord-server/src/kvt/services.rs`
- Test: `coord/tests/kv_t_g2_safe_ts_test.rs`
- Regression: `coord/tests/kv_t_g1_kv_rpc_test.rs`

**Interfaces:**
- Consumes: `CoordNode::safe_ts()`、Task 1/4 的 fence；`KvTRuntimeConfig::read_ts_wait_ms`。
- Produces: `Range` 的 `read_ts=0` → `safe_ts`；显式 `read_ts > safe_ts` 等待 `safe_ts >= read_ts`。

- [ ] **Step 1: 写失败测试**

```rust
#[tokio::test]
async fn test_range_read_ts_zero_uses_global_safe_ts() {
    // region1 @2000，region2 @1500；publish+h fence 到 1800
    // range(region1, read_ts=0) → read_ts=1800，看不到 2000 的写入
    // 再 fence 到 2200 → range(read_ts=0) 看到 2000 的写入，header=2200
}

#[tokio::test]
async fn test_explicit_read_ts_waits_for_lagging_region_then_safe_ts_lag() {
    // safe_ts=1800；range(read_ts=2000, wait_ms=50) → UNAVAILABLE + SAFE_TS_LAG
    // 指标 coord_kv_v2_safe_ts_wait_total 与 coord_kv_v2_safe_ts_lag_total 增加
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord --test kv_t_g2_safe_ts_test test_range_read_ts_zero test_explicit_read_ts`
Expected: FAIL（仍用目标 Region current，read_ts=0 返回 2000 或读到 2000 写入）

- [ ] **Step 3: 实现**

- 删除/替换 `wait_for_revision` 为 `wait_for_safe_ts(node, read_ts, wait_ms)`：每 10ms 调 `node.safe_ts()`，直到 `>= read_ts`；超时递增 `kv_v2_safe_ts_lag_total` 并返回 `UNAVAILABLE + SAFE_TS_LAG`；等待过（至少一轮）时记录 `inc_kv_v2_safe_ts_wait(elapsed_ms)`。
- `range` handler：`let safe_ts = self.safe_ts();`；`requested = if req.read_ts == 0 { safe_ts } else { req.read_ts as u64 }`；`requested > safe_ts` → wait。
- 读视图：`read_ts == current_now` → 实时；否则历史。**去掉 `req.read_ts == 0` 的特判**：并发提交导致 `live_revision > read_ts` 时，read_ts=0 也回退到历史视图；header 始终 = `read_ts`。
- 注释说明：`safe_ts` 是 min，所以 `read_ts <= safe_ts <= target.current_revision`，历史读不会要求未来 revision。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord --test kv_t_g2_safe_ts_test && cargo test -p coord --test kv_t_g1_kv_rpc_test`
Expected: PASS（G1 单 Region 行为不变）

- [ ] **Step 5: Commit**

```bash
git add coord-server/src/kvt/services.rs coord/tests/kv_t_g2_safe_ts_test.rs
git commit -m "feat(kv-t): read v2 ranges at the global safe-ts (#142)"
```

### Task 6: compaction 水位改为 safe_ts

**Files:**
- Modify: `coord-server/src/storage/compaction.rs`
- Modify: `coord-server/src/server/mod.rs`
- Modify: `coord/src/main.rs`

**Interfaces:**
- Produces: `pub trait SafeTsSource: Send + Sync { fn safe_ts(&self) -> u64; }`
- Produces: `CompactionManager::start_with_safe_ts(storage, config, proposer, metrics, safe_ts: Option<Arc<dyn SafeTsSource>>) -> Self`
- 既有 `start` / `run_cycle` 保持签名（内部传 `None`），测试与 v1 不变。
- `impl SafeTsSource for CoordNode`（server/mod.rs）。

- [ ] **Step 1: 写失败测试**（`storage/compaction.rs` tests）

```rust
#[tokio::test]
async fn test_auto_cycle_cutoff_is_clamped_to_safe_ts() {
    // storage current >= 60；retention=10；fake SafeTsSource = 40
    // run_cycle_with_safe_ts → 提案 revision = 30（不是 current-10）
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server --lib storage::compaction::tests::test_auto_cycle_cutoff_is_clamped`
Expected: FAIL（函数不存在）

- [ ] **Step 3: 实现**

- `run_cycle_with_safe_ts`：`base = safe_ts.map(|s| s.safe_ts().min(storage.current_revision())).unwrap_or_else(|| storage.current_revision())`；cutoff = `base - retention`。
- `start` 委托 `start_with_safe_ts(..., None)`；`start_with_safe_ts` 把 source 克隆进后台任务。
- `main.rs`：root `CompactionManager` 与 per-Region managers 都改用 `start_with_safe_ts(..., Some(Arc::clone(&node) as Arc<dyn SafeTsSource>))`。legacy 模式下 `CoordNode::safe_ts() == storage.current_revision()`，行为不变。
- 注释：`[kv_t].enabled=false` 时 source 仍等于 current，不改变 v1。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server --lib storage::compaction::tests`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add coord-server/src/storage/compaction.rs coord-server/src/server/mod.rs coord/src/main.rs
git commit -m "feat(kv-t): compact at safe-ts instead of current revision (#142)"
```

### Task 7: 全量门禁与文档引用

```bash
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
bash scripts/check-panics.sh
cargo test --workspace --no-fail-fast
cargo audit --no-fetch
cargo deny check
bash scripts/check-doc-refs.sh
```

修复到全绿后提交（如有格式化/文档改动）：

```bash
git add -A
git commit -m "chore(kv-t): satisfy gates for global safe-ts (#142)"
```

## Execution

- 分支：`feat/kv-t-g2-safe-ts`；PR 关闭 #142；合并后关闭 milestone「KV-T G2 跨区读与 SafeTs」中 #142 对应项（#143/#144/#145 另行推进）。
- issue 评论证据：本计划路径、关键测试名、门禁命令结果。
- 非范围：跨区 Range 扇出（#143）、Txn 读集（#144）、v2 Lease（#145）、Commit Sequencer/in-flight（#148）、动态 Region 表（#159）、Watch 保留（#157）、真实 failover 组合（#168/#175）。
