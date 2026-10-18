# KV-T G1 Compaction 水位改为 current revision Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 落地 issue #139——compact 水位从 raft `applied.index` 改为 TSO current revision，禁止 log index 旁路钳制；不删除 current revision 之后的 changelog。

**Architecture:** `apply_compact` 的生效水位 = `min(command.revision, MvccStorage::current_revision())`；v1 下 `current_revision` 仍等于 applied index（行为不变），v2 下等于最大 `commit_ts`。`Command::Compact` 的响应在 apply 前用同一公式计算，避免用 apply 后的 revision 反推。SafeTs（#142）落地前的过渡口径是 current revision；#142 收紧后只改水位来源，不改本项删除语义。

**Spec:** `docs/adr/0015-kv-t.md` D3/D9 + issue #139；依赖 #138 的 `META_CURRENT_REVISION`。

## Global Constraints

- `[kv_t].enabled=false` 时 v1 行为不变（`current_revision == applied.index`，clamp 结果一致）。
- 不得用 `applied.index` 继续做 revision 钳制；未来 revision 仍由 RPC 层 `INVALID_ARGUMENT` 拒绝，apply 侧只做不得失败的防御性钳制。
- apply 期确定性（不读墙钟/随机数）；`revision <= compacted_revision` 幂等 no-op。
- 不新增 TODO/FIXME；临时文件只在 `/tmp`。

## Review Focus

1. **不误删**：`revision` 大于 current revision 时，生效水位只到 current（不是 log index），不删 future 数据。
2. **v2 不钳回 log index**：commit_ts=1000、log index=6 时 compact(1000) 的 compacted/response 都是 1000。
3. **边界**：compact(1000) 后 changelog(1000) 仍在、changelog(900) 被删；`target <= compacted` 的历史读仍 `RevisionCompacted`。

## Task 1: `apply_compact` + Compact 响应

**Files:** `coord-server/src/storage/mvcc.rs`、`coord-server/src/raft/state_machine.rs`

- [ ] **Step 1: 写失败测试**

```rust
// mvcc.rs（CommitTs store）
#[test]
fn test_compact_watermark_uses_current_revision_not_applied_index() {
    // PutV2(1000) @ applied 3；apply_compact(1000, applied 5)
    // 旧实现 min(1000, 5)=5；断言 compacted_revision()==1000。
}
#[test]
fn test_compact_future_revision_clamped_to_current_revision() {
    // current=1000；apply_compact(5000, applied 5) → compacted==1000（不是 5）。
}
// state_machine.rs
#[test]
fn test_compact_response_uses_current_revision_not_log_index() {
    // CommitTs store，execute_command(Compact{1000}, log_index=6) → Response::Compact{1000}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server --lib test_compact_watermark_uses_current_revision test_compact_future_revision test_compact_response_uses_current_revision`
Expected: FAIL（当前实现 clamp 到 applied.index）

- [ ] **Step 3: 实现**

- `apply_compact`：`effective = revision.min(self.current_revision())`；更新注释（D3：不得用 applied.index 钳制）。
- `execute_command(Command::Compact)`：apply 前 `let effective = (*revision).min(sm.current_revision());`，apply 后返回 `Response::Compact { compacted_revision: effective }`（不再用 `applied.index`）。
- 其余删除/幂等语义不变。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server --lib storage::mvcc::tests::test_compact storage::mvcc::tests::test_apply_compact raft::state_machine::tests::test_compact`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add coord-server/src/storage/mvcc.rs coord-server/src/raft/state_machine.rs
git commit -m "feat(kv-t): compact at current revision instead of applied index (#139)"
```

## Task 2: 全量门禁

`cargo fmt --all -- --check`、`cargo clippy --workspace --lib --bins -- -D warnings`、`bash scripts/check-panics.sh`、`cargo test --workspace --no-fail-fast`、`cargo audit --no-fetch`、`cargo deny check`、`bash scripts/check-doc-refs.sh`。

## Execution

- 分支：`feat/kv-t-g1-compaction`；PR 关闭 #139。
- 非范围：SafeTs 计算（#142）、Commit Log GC（#157）、Raft 日志回收（ADR-0004）。
