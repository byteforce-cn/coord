# KV-T G6 (#173) v1 freeze Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `[kv_t].enabled=true` 的集群不服务 v1 数据面：`coord.kv` / `coord.txn` / `coord.watch` / `coord.lease` / `coord.storage` 的每个 RPC 在 handler 入口返回 `FAILED_PRECONDITION`（带稳定文案与 `x-coord-error-code`），health 对这些服务报 `NOT_SERVING`；`[kv_t].enabled=false` 的 v1 行为不变。

**Architecture:** 冻结判定收敛到 `KvTState::v1_gate()`（与 v2 `gate()` 同一状态对象，单一事实源），五个 v1 服务实现只在入口调用它；不包装 service、不改 wire、不改 v1 代码路径。`coord.storage` 在 v2 模式仍不注册（#166 已有）；health 侧把 v1 KV/Txn/Lease/Watch 从 "ready 即 SERVING" 改为 "ready 且 v1 模式才 SERVING"。

**Tech Stack:** Rust / tonic / tokio。

**Spec:** issue #173（v1 冻结与服务面收敛）；ADR-0015 D7/D13；`docs/production/ops/boundaries.md` B-KVT-2。

## Global Constraints

- `[kv_t].enabled=false`（默认）时 v1 行为、磁盘布局、服务注册、health 语义全部不变（ADR-0001）。
- v1 冻结是**策略拒绝**：用 `FAILED_PRECONDITION` + `CoordErrorCode::FailedPrecondition`，不用 `UNIMPLEMENTED`（走错地址）或 `UNAVAILABLE`（可重试）。文案是稳定诊断锚点，测试断言逐字保留。
- 冻结必须先于 `ensure_writable()` / leader 检查 / 鉴权后的 handler 逻辑：v2 集群的 v1 拒绝不应随磁盘水位或 raft 状态变化。
- 不新增 wire 字段/错误码；不改 v2 handler。
- 不新增 v1→v2 迁移/facade；`legacy_migration` 互斥已有配置校验，本切片不改。
- 注释只写当前「为什么」；不写日期/变更史；不新增 `TODO`/`FIXME`。
- 临时文件只放 `/tmp/coord-*`；提交前跑 AGENTS.md 门禁（fmt / clippy / check-panics / audit / deny / check-doc-refs）。

## Rulings

1. **v1 gate 用 enabled，不看 `implemented`。** 能力集合描述 v2 交付到哪个 gate；v1 冻结从第一个 v2 集群（G1 起）就成立，不能因为某个 v2 能力未交付而回退成 v1。
2. **`Storage` v1 也 gate。** 生产装配在 v2 模式不注册它（B-KVT-11(d)），但 handler 是公共实现（测试/嵌入方可能注册）；冻结语义不能只靠装配线。
3. **health 不是承重墙。** 冻结由 RPC 判定；health 只避免编排/负载均衡把流量继续送到 v1 服务。`Maintenance/Auth/CapabilityRegistry` 不受影响。
4. **文案固定为 `v1 data plane is frozen on a v2 cluster`。** 后半句点名 `coord.<domain>.v2` 与 ADR-0015 D7/D13，便于运维区分「没开 v2」与「v1 被冻结」。

## File Structure

- Modify: `coord-server/src/kvt/mod.rs` — `KvTState::v1_gate()` + 单测（关闭放行 / 开启拒绝且 trailer 正确）。
- Modify: `coord-server/src/server/mod.rs` — v1 `Kv::{put,range,delete}`、`Txn::txn`、`Lease::{lease_grant,lease_revoke,lease_keep_alive}`、`Watch::watch` 入口调用。
- Modify: `coord-server/src/server/object_storage.rs` — v1 `Storage::{put,get,stat,delete}` 入口调用。
- Modify: `coord/src/main.rs` — health：v2 模式不再把 v1 KV/Txn/Lease/Watch 置为 SERVING。
- Create: `coord/tests/kv_t_g6_v1_freeze_test.rs` — 真实 gRPC：v2 模式五个 v1 面全拒（含流式 RPC），v1 模式回归。
- Modify: `docs/production/ops/boundaries.md` — B-KVT-2 判据补本次测试与 health 口径。
- Modify: `docs/production/consumer-contract.md` — §1 冻结行补「错误码 + health」的可观察行为。
- Modify: `docs/adr/0015-kv-t.md` — 无（D7/D13 已冻结语义，不复制实现细节）。

## Tasks

### Task 1: `KvTState::v1_gate()`

**Files:**
- Modify: `coord-server/src/kvt/mod.rs`
- Test: 同文件 `mod tests`

**Interfaces:**
- Produces: `pub fn v1_gate(&self) -> Result<(), Status>`

- [ ] **Step 1: Write the failing test**

`v1_gate_is_open_when_disabled`: `KvTState::disabled().v1_gate().is_ok()`。
`v1_gate_rejects_on_a_v2_cluster`: `KvTState::new(true, KvTRuntimeConfig::default()).v1_gate()` 返回 `Code::FailedPrecondition`，message 含 `v1 data plane is frozen on a v2 cluster` 与 `coord.<domain>.v2`，`error_code_of(&status) == Some("FAILED_PRECONDITION")`；同一状态 `gate(KvTCapability::Kv)` 仍按能力集合拒绝（两条闸互不替代）。

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p coord-server kvt::tests::v1_gate -- --nocapture`
Expected: FAIL（`v1_gate` 不存在）

- [ ] **Step 3: Implement `v1_gate`**

在 `impl KvTState` 内、`gate` 旁：`enabled` 为假直接 `Ok(())`；否则 `attach(Status::new(Code::FailedPrecondition, "v1 data plane is frozen on a v2 cluster: use coord.<domain>.v2 (ADR-0015 D7/D13)"), CoordErrorCode::FailedPrecondition)`。

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p coord-server kvt::tests::v1_gate`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add coord-server/src/kvt/mod.rs
git commit -m "feat(kv-t): gate the v1 data plane on v2 clusters"
```

### Task 2: v1 KV / Txn / Lease / Watch handlers

**Files:**
- Modify: `coord-server/src/server/mod.rs`
- Test: `coord/tests/kv_t_g6_v1_freeze_test.rs`

**Interfaces:**
- Consumes: `KvTState::v1_gate()`（Task 1）
- Produces: 每个 v1 handler 入口 `self.kvt().v1_gate()?;`

- [ ] **Step 1: Write the failing test**

`coord/tests/kv_t_g6_v1_freeze_test.rs`，复用 `kv_t_g0_test.rs` 的进程内装配（`CoordNode` + `MvccStorage` + `WatchDispatcher` + `KvTState::new(true, ..)`），额外注册 v1 `TxnServer` / `LeaseServer` / `WatchServer`：

- `test_v2_cluster_rejects_v1_kv`：`KvClient::{put, range, delete}` 三例 → `FailedPrecondition` + 文案 + trailer。
- `test_v2_cluster_rejects_v1_txn_lease_watch`：`TxnClient::txn`、`LeaseClient::lease_grant/revoke`、`LeaseClient::lease_keep_alive`（发一帧 `LeaseKeepAliveRequest{ id: 1 }`）、`WatchClient::watch`（发一帧 `WatchRequest::Create`，断言首个返回错误）。
- `test_v1_cluster_still_serves_v1`：`false` 状态 Put/Range 成功（回归，一条即可）。

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p coord --test kv_t_g6_v1_freeze_test -- --nocapture`
Expected: FAIL（v1 Put 成功而不是被拒）

- [ ] **Step 3: Add `self.kvt().v1_gate()?;` at the top of each handler**

顺序固定为函数第一行（先于 `ensure_writable`/首帧读取），覆盖 `Kv::put/range/delete`、`Txn::txn`、`Lease::lease_grant/lease_revoke/lease_keep_alive`、`Watch::watch`。

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p coord --test kv_t_g6_v1_freeze_test`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add coord-server/src/server/mod.rs coord/tests/kv_t_g6_v1_freeze_test.rs
git commit -m "test(kv-t): freeze v1 kv/txn/lease/watch on v2 clusters"
```

### Task 3: v1 Storage handlers

**Files:**
- Modify: `coord-server/src/server/object_storage.rs`
- Test: `coord/tests/kv_t_g6_v1_freeze_test.rs`（`#[path = "support/kv_t_storage_node.rs"] mod storage_node;`）

**Interfaces:**
- Consumes: `KvTState::v1_gate()`；`StorageNode::start()`（已装配 `kv_t.enabled=true` + object limits）

- [ ] **Step 1: Write the failing test**

`test_v2_cluster_rejects_v1_storage`：起一个 `StorageServer`（v1）绑定到 `StorageNode.node`，调用 `Stat(Bucket=b, ObjectId=missing)`：
- 装 v2 集群（`v1_gate`）后 → `FailedPrecondition` + 文案；
- 断言错误不是 `NOT_FOUND`（证明 gate 先于对象查找）。

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p coord --test kv_t_g6_v1_freeze_test test_v2_cluster_rejects_v1_storage -- --nocapture`
Expected: FAIL（返回 `NOT_FOUND`/成功）

- [ ] **Step 3: Gate `Storage::{put,get,stat,delete}`**

`self.kvt().v1_gate()?;` 放在每个方法第一行（`put` 的流首帧之前）。

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p coord --test kv_t_g6_v1_freeze_test`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add coord-server/src/server/object_storage.rs coord/tests/kv_t_g6_v1_freeze_test.rs
git commit -m "fix(kv-t): reject the v1 storage face on v2 clusters"
```

### Task 4: health face

**Files:**
- Modify: `coord/src/main.rs`

- [ ] **Step 1: Implement**

在 health 循环 spawn 前捕获 `let kv_t_enabled = cfg.kv_t.enabled;`；`ready` 分支里只有在 `!kv_t_enabled` 时才把 v1 `Kv/Txn/Lease/Watch` 置 `SERVING`（初始已是 `NOT_SERVING`，`else` 分支不变）。`Maintenance/Auth/CapabilityRegistry` 与 `Storage` 的既有条件不动。

- [ ] **Step 2: Verify compile**

Run: `cargo build -p coord --bin coord`
Expected: 编译通过

- [ ] **Step 3: Commit**

```bash
git add coord/src/main.rs
git commit -m "fix(kv-t): keep the v1 data plane NOT_SERVING on v2 clusters"
```

### Task 5: docs + gates

**Files:**
- Modify: `docs/production/ops/boundaries.md`（B-KVT-2）
- Modify: `docs/production/consumer-contract.md`（§1 冻结行）

- [ ] **Step 1: Docs**

B-KVT-2 的「判据」补：`coord/tests/kv_t_g6_v1_freeze_test.rs`（五个 v1 面全拒 + v1 回归）；说明 health 在 v2 模式对 v1 数据面报 `NOT_SERVING`。consumer-contract §1 冻结行补：每个 v1 RPC 返回 `FAILED_PRECONDITION`（文案含 `v1 data plane is frozen on a v2 cluster`），health 为 `NOT_SERVING`；`coord.storage` v1 不注册。

- [ ] **Step 2: Gates**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
bash scripts/check-panics.sh
bash scripts/check-doc-refs.sh
cargo test -p coord --test kv_t_g6_v1_freeze_test
cargo test -p coord-server kvt::tests
cargo audit --no-fetch
cargo deny --offline check
```

- [ ] **Step 3: Delivery**

PR → CI 全绿 → merge → 删分支；#173 回填（v1 冻结面已落地；备份/恢复 #169、Agent #170、workflow #171、SDK #172、混沌/性能/ops/签署仍 open）。

## Review Focus

- v2 集群上 v1 `Put` 是否会因 `ensure_writable` 或幂等缓存先于冻结返回别的错误？（要求冻结第一行）
- 流式 v1 `Watch`/`LeaseKeepAlive` 是否在**读首帧前**拒绝，避免客户端以为订阅已建立？
- `[kv_t].enabled=false` 的 v1 Put/Range/Txn/Lease/Watch/Storage 是否字节级不变（回归测试）？
- 错误是否带 `x-coord-error-code: FAILED_PRECONDITION`（结构化判别，不解析文案）？
- health 是否只影响 v1 数据面，`Maintenance/Auth/CapabilityRegistry` 仍随 raft ready 切换？
