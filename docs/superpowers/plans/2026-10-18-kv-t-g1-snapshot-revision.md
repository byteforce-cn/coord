# KV-T G1 Snapshot 携带 current revision Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 落地 issue #140——快照携带 `current_revision`（TSO 高水位/批状态已随 `/_sys/tso/*` 域携带），装快照/重启后 revision 不倒退、TSO 批不复用。

**Architecture:** `SnapshotData` 升版到 v6，新增 `current_revision` 字段；导出在**同一读事务**内读取 `META_CURRENT_REVISION`（既有 v1 库回退 applied index），导入在同一写事务内恢复。TSO 高水位与 `TSO_LAST_BATCH` 继续由 `sys_entries`（`/_sys/*`）承载——不新增第二份 TSO 状态（单一事实源）。旧 v5 字节按仓库既有策略显式拒绝（无迁移阶梯）。

**Tech Stack:** Rust、redb、postcard 信封 V2。

**Spec:** `docs/adr/0015-kv-t.md` D3/D11.2/3 + issue #140；依赖 #138 的 `META_CURRENT_REVISION`。

## Global Constraints

- `[kv_t].enabled=false`（默认）时 v1 快照行为不变：导出恢复 `current_revision` 后仍等于 applied index。
- 装快照后 `current_revision` 不倒退；新 TSO 批的 `start_ts` 必须高于快照携带的高水位。
- 不新增 TODO/FIXME；临时文件只在 `/tmp`；文档引用 0 悬空。
- 提交前本地跑：`cargo fmt --all -- --check`、`cargo clippy --workspace --lib --bins -- -D warnings`、`bash scripts/check-panics.sh`、`cargo test`、`cargo audit`、`cargo deny check`、`bash scripts/check-doc-refs.sh`。

## Rulings

- **TSO 不重复入 payload**：`TSO_HIGH_WATER` / `TSO_LAST_BATCH` 位于 `/_sys/tso/*`，已由 `SnapshotData.sys_entries` 携带（#136 测试覆盖）。再加 `tso_high_water` 字段会形成第二份状态；本项只在注释与测试中固定该契约。
- **v5 → v6 显式拒绝**：仓库既有策略是「快照无迁移阶梯，版本不符显式报错」。快照 v6 仅被 v6 读取；升级=重建 v2 集群/重新导出。

## Review Focus

1. **不倒退**：v2 快照导入后 `current_revision()` 等于导出值；紧接着的小 `commit_ts` 被 `CommitRejected`。
2. **TSO 不重用**：导入后 `tso_high_water()` 等于导出值，下一批 `start_ts` 不低于它。
3. **v1 不变**：`LogIndex` 模式下导出的 `current_revision` 等于 applied index；导入后 `current_revision()` 与旧行为一致。

---

### Task 1: Snapshot v6 + `current_revision`

**Files:**
- Modify: `coord-server/src/storage/snapshot.rs`

**Interfaces:**
- Produces: `SnapshotData.current_revision: u64`；`CURRENT_VERSION = 6`。

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn test_snapshot_preserves_tso_and_revision() {
    // CommitTs store：PutV2(commit_ts=1000) + TSO Allocate(8) → export
    // → import 到新 CommitTs store
    // 断言 current_revision()==1000、tso_high_water()==9；
    // 下一次 TSO Allocate 的 start_ts >= 9；PutV2(999) 被 CommitRejected。
}

#[test]
fn test_current_revision_persists_across_reopen() {
    // export → import → drop → 以 CommitTs 模式重开：current_revision 不倒退，
    // commit_ts == current 被拒绝、+1 被接受。
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server --lib storage::snapshot::tests::test_snapshot_preserves_tso_and_revision storage::snapshot::tests::test_current_revision_persists_across_reopen`
Expected: FAIL（字段/行为不存在）

- [ ] **Step 3: 实现**

- `SnapshotData` 增加 `current_revision: u64`；`new()` 置 0；`CURRENT_VERSION = 6`，注释说明 TSO 状态在 `sys_entries`。
- 导出：单读事务内读 `META_CURRENT_REVISION`；缺失时 `LogIndex` 模式回退 applied index、`CommitTs` 模式 0。
- 导入：同一写事务恢复 `META_CURRENT_REVISION`（0 → remove）。
- 更新版本相关注释/测试（v5 → v6）。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server --lib storage::snapshot`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add coord-server/src/storage/snapshot.rs
git commit -m "feat(kv-t): carry current revision in snapshots (#140)"
```

### Task 2: 全量门禁

```bash
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
bash scripts/check-panics.sh
cargo test --workspace --no-fail-fast
cargo audit --no-fetch
cargo deny check
bash scripts/check-doc-refs.sh
```

## Execution

- 分支：`feat/kv-t-g1-snapshot`；PR 关闭 #140。
- 非范围：备份编排/恢复演练（#169）、Commit Log / ValidationIndex 快照（#157/#147）。
