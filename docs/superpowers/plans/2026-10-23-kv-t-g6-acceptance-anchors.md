# KV-T G6 (#168) ADR acceptance anchors Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** ADR-0015 的验收锚点清单每一项都能指向一个可运行的测试（名字与 ADR 一致或由 ADR 逐项登记），并加一条 greenfield v2 集群（multi-raft + `[kv_t]`）的 TSO → 跨 Region Txn → Watch → Split → Merge 全链路 e2e；ADR 的「验收锚点（待实现）」改为当前事实。

**Architecture:** 不新增语义、不为测试重写装配：全链路测试复用 `coord/tests/support/kv_t_watch_node.rs`（真实 region 0 + 两个数据 Region raft、FakeTso、Commit Sequencer、Commit Log shard、Watch Coordinator）；缺口的两个锚点（no-double-serve、revision monotonic）放在已有 split/merge 夹具旁边。其余锚点按实际测试名登记到 ADR，避免复制同一断言的第二份实现。

**Tech Stack:** Rust / tokio / tonic / raft。

**Spec:** issue #168；ADR-0015 D11「验收锚点」与 D10 G6。

## Global Constraints

- `[kv_t].enabled=false`（v1）行为不变；新测试只装配显式 v2 状态。
- 不新增 wire 字段/错误码/语义；只验证 ADR 已冻结的语义。
- 测试名是契约：ADR 列出的锚点名与测试函数名必须逐字一致（重命名测试时同步 ADR）。
- 失败要能定位 gate/issue：每个新测试的文件头注明对应锚点与 D 条目。
- 注释只写当前「为什么」；不写日期/变更史；不新增 `TODO`/`FIXME`。
- 临时文件只放 `/tmp/coord-*`；门禁同 AGENTS.md。

## Rulings

1. **登记实际测试名，不复制断言。** `test_tso_client_never_reuses_batch` 已由 `test_tso_client_batches_and_never_reuses_across_failover` 覆盖；ADR 改为登记后者，而不是再加一个同名包装（包装会让两处断言漂移）。
2. **sequencer 锚点直接重命名。** `test_sequencer_conflict_order_and_retry` 的断言就是「冲突事务按序裁决 + 重试」，重命名为 ADR 名 `test_commit_sequencer_orders_conflicting_txns`。
3. **no-double-serve 用入口 epoch 判定。** 以真实的 split cutover 后表版本为准：旧版本请求 `EPOCH_STALE`（写/读都拒），当前版本只落新 Region，旧 Region 保留 cutover 前那行、不接收新写。
4. **revision monotonic 由现有 split/merge 搬迁用例承担。** `test_split_migrator_moves_live_rows_and_pins_revision`（源不动、目标钉在导入水位、后续决策推进）与 `test_merge_keeps_left_history_and_seals_absorbed_range`（水位不回退、历史窗口）已是该锚点的判据，ADR 逐项登记两个名字。
5. **全链路测试不引入多进程。** 多节点真实进程矩阵属于 #175（混沌/故障注入）；本切片用真实 raft 组的进程内 v2 集群覆盖 ADR 锚点的语义链路，CI 可重复。

## File Structure

- Create: `coord/tests/kv_t_g6_full_chain_test.rs` — 全链路 e2e（`test_full_chain_tso_txn_watch_split_merge`）。
- Modify: `coord/tests/kv_t_g5_continuity_test.rs` — 新增 `test_split_merge_no_double_serve`（真实 Region + 表版本 guard）。
- Modify: `coord-server/tests/kv_t_g3_sequencer_test.rs` — 重命名锚点测试。
- Modify: `docs/adr/0015-kv-t.md` — 验收锚点改为实际测试名 + 文件；补全链路锚点。
- Modify: `docs/superpowers/plans/...`（本文件）。

## Tasks

### Task 1: 全链路 e2e

**Files:**
- Create: `coord/tests/kv_t_g6_full_chain_test.rs`

**Interfaces:**
- Consumes: `WatchNode`（`start/open_first_shard/put_v2/manager/tso_client/commit_pairs/commit_puts/begin/replay_decision_on_region/apply_region_table_and_spawn/retire_region/settle`）、`WatchCoordinator`、`apply_pending_decisions`。
- Produces: `test_full_chain_tso_txn_watch_split_merge`。

- [ ] **Step 1: Write the failing test**

断言链（一次跑通）：
1. `tso_client().get_ts()` 两次严格递增；`put_v2(key, ts)` 后 `get_kv_metadata(key).mod_revision == ts`。
2. `commit_pairs([("bb", "left"), ("zz", "right")])`：两个 Region 各一键、同一 `commit_ts`；Watch 从 `SEED_TS+1` 起只看到一个事件、`changes` 恰好两个 key。
3. split 表 `[..,m)` / `[m,n)` / `[n,..)`（版本 2）+ spawn：子 Region 决策 + 父 Region 迟到重放；Watch 只出现一次该 `commit_ts`。
4. merge 回 `[..,m)` / `[m,..)`（版本 3）+ retire 子 Region：再提交一个事务；Watch 事件序列 = `[split_ts, merge_ts]`，cursor 连续。
5. `current_revision()` 在 split/merge 前后不倒退；merge 后新值可读。

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p coord --test kv_t_g6_full_chain_test`
Expected: FAIL（测试文件不存在 → 编译失败）

- [ ] **Step 3: Implement the test**

按 `coord/tests/kv_t_g5_continuity_test.rs` 的装配顺序（split 后先 `apply_pending_decisions` 再提交，重放后再次 apply + `settle`；merge 后 retire 再提交）。

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p coord --test kv_t_g6_full_chain_test`
Expected: PASS（1 passed）

- [ ] **Step 5: Sensitivities**

- 注释掉 split 后的 `replay_decision_on_region` → 重放去重断言不生效（事件数仍对，但该断言失效）：改为把 replay 的 `commit_ts` 改大 → Watch 事件序列断红。
- 把 merge 后提交的 key 改成仍落在已 retire 的 Region → 路由/读断言红。

- [ ] **Step 6: Commit**

```bash
git add coord/tests/kv_t_g6_full_chain_test.rs
git commit -m "test(kv-t): add the greenfield v2 full-chain acceptance test"
```

### Task 2: `test_split_merge_no_double_serve`

**Files:**
- Modify: `coord/tests/kv_t_g5_continuity_test.rs`

**Interfaces:**
- Consumes: `WatchNode`、`coord_proto::kv::v2::kv_server::Kv`、`guard_routing_epoch` 的 metadata 名 `coord-routing-epoch`。
- Produces: `test_split_merge_no_double_serve`。

- [ ] **Step 1: Write the failing test**

1. `WatchNode::start()` + `apply_region_table_and_spawn(initial_table(1))`（区域 1 `[..,m)`、2 `[m,..)`）。
2. 用 epoch `1` + TSO 时间戳写 `nn`（落 Region 2）；断言成功。
3. 切到 split 表版本 2（`1=[..,m)`、`2=[m,n)`、`3=[n,..)`）。
4. epoch `1` 的 `Put(nn)` 与 `Range(nn)` → `ABORTED` + `EPOCH_STALE`，trailer `coord-routing-epoch == 2`。
5. epoch `2` 的 `Put(nn, "new")` → 成功；Region 3 读到 `new`，Region 2 仍是 cutover 前的 `old`（不双写）。

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p coord --test kv_t_g5_continuity_test test_split_merge_no_double_serve`
Expected: FAIL（测试不存在）

- [ ] **Step 3: Implement the test**

`with_epoch` 辅助直接往 `tonic::Request` 插 metadata；`assert_epoch_stale` 断言 code/trailer（不复用其他文件，保持测试自足）。

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p coord --test kv_t_g5_continuity_test`
Expected: PASS（2 passed）

- [ ] **Step 5: Commit**

```bash
git add coord/tests/kv_t_g5_continuity_test.rs
git commit -m "test(kv-t): assert split cutover serves exactly one table version"
```

### Task 3: anchor names in ADR + sequencer rename

**Files:**
- Modify: `coord-server/tests/kv_t_g3_sequencer_test.rs`
- Modify: `docs/adr/0015-kv-t.md`

- [ ] **Step 1: Rename the sequencer anchor**

`test_sequencer_conflict_order_and_retry` → `test_commit_sequencer_orders_conflicting_txns`；运行 `cargo test -p coord-server --test kv_t_g3_sequencer_test`（7 passed）。

- [ ] **Step 2: Rewrite the ADR acceptance list**

把「验收锚点（待实现）」改为「验收锚点」，逐项写实际测试名 + 文件；补充：
- `test_full_chain_tso_txn_watch_split_merge`（`coord/tests/kv_t_g6_full_chain_test.rs`）
- `test_split_merge_no_double_serve`（`coord/tests/kv_t_g5_continuity_test.rs`）
- TSO client 锚点登记 `test_tso_client_batches_and_never_reuses_across_failover`（`coord/tests/cluster_test.rs`）与 `test_tso_restart_does_not_reuse_batch`（`coord-server/src/storage/mvcc.rs`）
- revision 单调登记 split/merge 搬迁两个用例（见 Rulings 4）

- [ ] **Step 3: Gates**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
bash scripts/check-panics.sh
bash scripts/check-doc-refs.sh
cargo test -p coord --test kv_t_g6_full_chain_test
cargo test -p coord --test kv_t_g5_continuity_test
cargo test -p coord-server --test kv_t_g3_sequencer_test
cargo audit --no-fetch
cargo deny --offline check
```

- [ ] **Step 4: Delivery**

PR → CI 全绿 → merge → 删分支；#168 回填证据并关闭（G6 其余项 #169/#170/#171/#172/#174/#175/#176/#177 仍 open）。

## Review Focus

- 全链路测试是否**真的**经过 TSO 分配（而不是直接写死 `commit_ts`），且 `mod_revision == commit_ts`？
- 跨 Region 事务在 Watch 里是否仍是一批（`changes.len() == 2`）而不是拆成两条事件？
- split 后父 Region 的迟到重放是否只产生一个 Watch 事件（不丢、不重）？
- no-double-serve 断言是否覆盖「旧 epoch 读」与「旧 epoch 写」两侧，且新写没有同时落到旧 Region？
- 拓扑变化前后 `current_revision` 是否单调（ADR D11.2）？
