# KV-T G1 Revision 与 raft log index 解耦 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 落地 issue #138——把 MVCC revision 从 per-Region raft log index 解耦：持久化 `META_CURRENT_REVISION`，v2 写命令按显式 `commit_ts` 应用，重放幂等、回退显式拒绝（`CommitRejected`），`[kv_t].enabled=false`（默认）行为字节级不变。

**Architecture:** `MvccStorage` 增加 `RevisionMode`：`LogIndex`（v1 默认）与 `CommitTs`（`[kv_t].enabled=true` 时由启动装配注入）。`LogIndex` 模式下每个 apply 写事务继续把 revision 记为 log index（与现状完全一致）；`CommitTs` 模式下 log-index 记录只推进 `META_LAST_APPLIED`、不污染 revision，只有显式 `commit_ts` 的写路径校验 `commit_ts > current_revision`、写 changelog、并持久化 revision。v2 写命令（`PutV2` / `DeleteV2` / `DeleteRangeV2`）携带 `commit_ts`；用户级拒绝返回 `Response::CommitRejected`，不让 raft apply 返回 `Err`（D11.5）。

**Tech Stack:** Rust、openraft、redb、postcard 信封 V2。

**Spec:** `docs/adr/0015-kv-t.md` D3/D11.1/2/4/6 + issue #138；语义字段以 `apis/contracts/proto/coord/kv/v2/kv.proto` 头注为准。

## Global Constraints

- `[kv_t].enabled=false`（默认）不改变 v1 行为：revision 仍等于 log index，磁盘布局与响应字节级不变（ADR-0001）。
- `commit_ts` 全局唯一、单调；同一 `commit_ts` 重放不得重复写 changelog / 重复推进 revision（D11.6）。
- `commit_ts <= current_revision` 且未命中已有 revision ⇒ `CommitRejected`（D11.4）；**不静默写**。
- 用户级拒绝不得让 Raft apply 返回 `Err`（D11.5）；被拒绝的命令仍推进 `META_LAST_APPLIED`（否则重启会无限重放）。
- 不新增 TODO/FIXME；临时文件只在 `/tmp`；注释只写当前「为什么」；文档引用 0 悬空。
- 提交前本地跑：`cargo fmt --all -- --check`、`cargo clippy --workspace --lib --bins -- -D warnings`、`bash scripts/check-panics.sh`、`cargo test`、`cargo audit`、`cargo deny check`、`bash scripts/check-doc-refs.sh`。

## Rulings

- **issue 文本笔误**：issue #138 写「`commit_ts > current_revision` 或已命中已有 revision → `CommitRejected`」，与 ADR-0015 D11.4 及 `kv.proto` 头注（`commit_ts <= current_revision` 且未命中已有写入才拒绝）矛盾。按 ADR/契约实现；本文与 PR 记录该裁定。
- **系统 key 的边界**：v2 集群的 data Region 是 region ≥ 1（region 0 保留给 system raft，`[multi_raft].initial_regions.id > 0`），region 0 不承载用户 KV。`CommitTs` 模式下 log-index 记录（auth/lease/PD/TSO）不再推进 revision；把 lease/auth/PD/object storage 写入迁到 `commit_ts` 属于各自 gate（#145/#166），本项不提前改语义。v1 数据面在 v2 集群被 #173 冻结；本项不把 v1 写命令纳入 `commit_ts` 空间。
- **snapshot**：`install_snapshot` 后 `META_CURRENT_REVISION` 的携带/恢复属于 #140；本项只保证进程崩溃/重启（同一 store）后 revision 不倒退。

## Review Focus

1. **v1 不变**：`RevisionMode::LogIndex` 下 `current_revision()`、changelog、header revision、compact 行为与改动前一致。
2. **重放**：同一 `commit_ts` 二次 apply 返回 replayed，不新增 changelog、不改变 `current_revision`。
3. **拒绝**：`commit_ts <= current_revision` 且未命中 ⇒ `CommitRejected`，数据不落盘、`META_LAST_APPLIED` 前进、raft apply 不返回 `Err`。
4. **不钳制**：`current_revision` 取 `commit_ts` 而不是 `applied.index`；`commit_ts` 可大于或小于 log index。
5. **系统记录不污染**：`CommitTs` 模式下 TSO/PD/Auth/Lease/Compact 的 log-index 写入不推进 `current_revision`。

---

### Task 1: `CommitRejected` 错误与 gRPC 映射

**Files:**
- Modify: `coord-core/src/error.rs`
- Modify: `coord-server/src/server/mod.rs`（`map_core_error` + tests）

**Interfaces:**
- Produces: `Error::CommitRejected { commit_ts: u64, current_revision: u64 }`；gRPC `FAILED_PRECONDITION` + trailer `COMMIT_REJECTED`。

- [ ] **Step 1: 写失败测试**（`coord-server/src/server/mod.rs` tests）

```rust
#[test]
fn test_commit_rejected_maps_to_failed_precondition_with_code() {
    let err = coord_core::error::Error::CommitRejected {
        commit_ts: 7,
        current_revision: 9,
    };
    let status = map_err(err);
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert_eq!(error_code_of(&status).as_deref(), Some("COMMIT_REJECTED"));
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server --lib test_commit_rejected_maps`
Expected: FAIL（`Error::CommitRejected` 不存在）

- [ ] **Step 3: 实现**

- `coord-core/src/error.rs` 增加变体与 `#[error(...)]` 文案（含 `commit_ts` / `current_revision`）。
- `map_core_error` 映射为 `Status::failed_precondition` 并 `attach(status, CoordErrorCode::CommitRejected)`。
- 修复因新变体引起的穷举 match 编译错误。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server --lib test_commit_rejected_maps`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add coord-core/src/error.rs coord-server/src/server/mod.rs
git commit -m "feat(kv-t): add CommitRejected error and gRPC mapping (#138)"
```

### Task 2: `META_CURRENT_REVISION` 与 commit_ts 写路径

**Files:**
- Modify: `coord-server/src/storage/mvcc.rs`（常量、`RevisionMode`、`MvccStorage` 字段、`current_revision`、写路径、测试）

**Interfaces:**
- Produces:
  - `pub const META_CURRENT_REVISION: &[u8] = b"/_meta/current_revision";`（8BE）
  - `pub enum RevisionMode { LogIndex, CommitTs }`
  - `MvccStorage::new_with_revision_mode(backend, mode) -> Result<Self>`
  - `MvccStorage::revision_mode(&self) -> RevisionMode`
  - `pub fn put_at_commit_ts(&self, key, value, lease_id, commit_ts, applied) -> Result<ApplyOutcome>`
  - `pub fn delete_at_commit_ts(&self, key, commit_ts, applied) -> Result<ApplyOutcome>`
  - `pub fn delete_range_at_commit_ts(&self, start, range_end, commit_ts, applied) -> Result<DeleteRangeOutcome>`

- [ ] **Step 1: 写失败测试**（`storage/mvcc.rs` tests）

```rust
fn create_commit_ts_storage() -> (TempDir, MvccStorage<RedbBackend>) { /* new_with_revision_mode(CommitTs) */ }

#[test]
fn test_current_revision_uses_meta_not_log_index() {
    let (_dir, storage) = create_commit_ts_storage();
    storage.put_at_commit_ts(b"k", b"v", None, 100, applied_for(7)).unwrap();
    assert_eq!(storage.current_revision(), 100);
    assert_eq!(storage.get_applied_log_id().unwrap().unwrap().index, 7);
}

#[test]
fn test_put_at_rejects_revision_regression() {
    let (_dir, storage) = create_commit_ts_storage();
    storage.put_at_commit_ts(b"k", b"v", None, 100, applied_for(7)).unwrap();
    let err = storage.put_at_commit_ts(b"k2", b"v", None, 99, applied_for(8)).unwrap_err();
    assert!(matches!(err, Error::CommitRejected { commit_ts: 99, current_revision: 100 }));
    assert_eq!(storage.get(b"k2").unwrap(), None, "拒绝不得落盘");
    assert_eq!(storage.current_revision(), 100);
}

#[test]
fn test_commit_ts_not_clamped_by_applied_index() {
    let (_dir, storage) = create_commit_ts_storage();
    storage.put_at_commit_ts(b"k", b"v", None, 1_000_000, applied_for(3)).unwrap();
    assert_eq!(storage.current_revision(), 1_000_000);
    assert_eq!(storage.get_kv_metadata(b"k").unwrap().unwrap().mod_revision, 1_000_000);
}

#[test]
fn test_commit_ts_replay_is_idempotent() {
    let (_dir, storage) = create_commit_ts_storage();
    let first = storage.put_at_commit_ts(b"k", b"v1", None, 100, applied_for(7)).unwrap();
    let replay = storage.put_at_commit_ts(b"k", b"v2", None, 100, applied_for(7)).unwrap();
    assert!(!first.replayed && replay.replayed);
    assert_eq!(storage.get(b"k").unwrap().as_deref(), Some(&b"v1"[..]));
    assert_eq!(storage.get_kv_metadata(b"k").unwrap().unwrap().version, 1);
}

#[test]
fn test_commit_ts_survives_reopen() {
    let (dir, storage) = create_commit_ts_storage();
    storage.put_at_commit_ts(b"k", b"v", None, 100, applied_for(7)).unwrap();
    drop(storage);
    let reopened = MvccStorage::new_with_revision_mode(
        RedbBackend::open(dir.path(), &StorageConfig::default()).unwrap(),
        RevisionMode::CommitTs,
    ).unwrap();
    assert_eq!(reopened.current_revision(), 100);
    let err = reopened.put_at_commit_ts(b"k2", b"v", None, 99, applied_for(8)).unwrap_err();
    assert!(matches!(err, Error::CommitRejected { .. }));
}
```

  - 负控制（保持 v1）：`test_log_index_mode_revision_matches_applied_index`——`put_at_revision(..., 7, applied_for(7))` 后 `current_revision()==7`，再用 revision 6 写得到 `CommitRejected`。

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server --lib storage::mvcc::tests::test_current_revision_uses_meta_not_log_index storage::mvcc::tests::test_put_at_rejects_revision_regression`
Expected: FAIL（`put_at_commit_ts` / `new_with_revision_mode` 不存在）

- [ ] **Step 3: 实现**

- 常量 + `RevisionMode`（`Debug, Clone, Copy, PartialEq, Eq`）；`MvccStorage` 增加 `revision_mode`，`new()` 委托 `LogIndex`。
- `current_revision()`：读 `META_CURRENT_REVISION`（8BE）；缺失时 `LogIndex` 回退 `META_LAST_APPLIED.index`、`CommitTs` 返回 0。
- `set_last_applied`：`LogIndex` 模式下同事务写 `META_CURRENT_REVISION = applied.index`；`CommitTs` 不动 revision。
- 每个 log-index 写事务（put/delete/delete_range/txn/lease/delete_keys/auth/pd/tso/compact/persist_applied）在 `LogIndex` 模式下同事务写 `META_CURRENT_REVISION = revision`（compact 用 `applied.index`）；`CommitTs` 模式下不写。
- `put_at_commit_ts` / `delete_at_commit_ts` / `delete_range_at_commit_ts`：
  - `commit_ts == 0` → `Error::InvalidArgument`。
  - changelog 已有该 revision → `replayed`（不写、不推进 revision）。
  - 写事务内读 `current_revision`，`commit_ts <= current` → `Err(Error::CommitRejected { .. })`。
  - 否则写 KV/元数据/changelog（revision=`commit_ts`）+ `META_LAST_APPLIED` + `META_CURRENT_REVISION = commit_ts`。
- `create_revision` / `mod_revision` 走既有 `KvMetadata` 逻辑（传入 `commit_ts`）。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server --lib storage::mvcc::tests::test_`
Expected: PASS（含既有 4000 行测试）

- [ ] **Step 5: Commit**

```bash
git add coord-server/src/storage/mvcc.rs
git commit -m "feat(kv-t): persist current revision and apply writes at commit_ts (#138)"
```

### Task 3: v2 写命令与应用语义

**Files:**
- Modify: `coord-server/src/raft/type_config.rs`（`Command` / `Response` 末尾追加）
- Modify: `coord-server/src/raft/state_machine.rs`（`execute_command` + tests）

**Interfaces:**
- Produces:
  - `Command::PutV2 { key, value, lease_id, commit_ts }`
  - `Command::DeleteV2 { key, commit_ts }`
  - `Command::DeleteRangeV2 { key, range_end, commit_ts }`
  - `Response::CommitRejected { commit_ts, current_revision }`
- Consumes: Task 2 的 commit_ts 写方法

- [ ] **Step 1: 写失败测试**（`raft/state_machine.rs` tests）

```rust
#[test]
fn test_apply_v2_put_uses_commit_ts_not_log_index() {
    let (_tmp, store) = setup_store_with_mode(RevisionMode::CommitTs);
    let applied = AppliedLogId { term: 1, node_id: 1, index: 5 };
    let (resp, event) = store.execute_command(
        store.state_machine.as_ref(),
        &Command::PutV2 { key: b"k".to_vec(), value: b"v".to_vec(), lease_id: None, commit_ts: 1_000 },
        5,
        applied,
    ).unwrap();
    assert!(matches!(resp, Response::Put { revision: 1_000 }));
    assert!(event.is_some());
    assert_eq!(store.state_machine.current_revision(), 1_000);
}

#[test]
fn test_apply_v2_regressing_commit_ts_is_rejected_without_apply_error() {
    let (_tmp, store) = setup_store_with_mode(RevisionMode::CommitTs);
    /* 先 1000，再 999 */
    let (resp, event) = store.execute_command(/* PutV2 999 */).unwrap();
    assert!(matches!(resp, Response::CommitRejected { commit_ts: 999, current_revision: 1000 }));
    assert!(event.is_none());
    assert_eq!(store.state_machine.current_revision(), 1000);
    assert_eq!(store.state_machine.get_applied_log_id().unwrap().unwrap().index, 6);
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server --lib raft::state_machine::tests::test_apply_v2`
Expected: FAIL（`PutV2` 不存在）

- [ ] **Step 3: 实现**

- `Command` / `Response` 变体**末尾追加**（变体索引兼容）并补 `Display`。
- `execute_command`：
  - `PutV2` → `put_at_commit_ts`，成功返回 `Response::Put { revision: commit_ts }`，事件 revision = `commit_ts`。
  - `DeleteV2` / `DeleteRangeV2` 同理（事件类型 Delete）。
  - `Err(Error::CommitRejected { .. })` → `sm.set_last_applied(applied)` 后返回 `(Response::CommitRejected { .. }, None)`；**不得**返回 `Err`。
- 其余命令保持 `revision ≡ log index`。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server --lib raft::state_machine::tests::test_apply_v2`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add coord-server/src/raft/type_config.rs coord-server/src/raft/state_machine.rs
git commit -m "feat(kv-t): apply v2 writes at explicit commit_ts (#138)"
```

### Task 4: 启动装配（kv_t → CommitTs 模式）+ 全量门禁

**Files:**
- Modify: `coord/src/main.rs`（root `MvccStorage` / `spawn_configured_regions` 调用）
- Modify: `coord-server/src/raft/region.rs`（`spawn_configured_regions` 参数）
- Modify: `coord-server/src/raft/region_runtime.rs`（`RegionRuntimeSpec.revision_mode`）
- Modify: 构造 `RegionRuntimeSpec` 的测试（`coord-server/tests/region_*`、`pd_operator_executor_test`、`migration_test`）

**Interfaces:**
- Consumes: Task 2 的 `RevisionMode` / `new_with_revision_mode`

- [ ] **Step 1: 写失败测试**（`coord/tests/kv_t_g1_revision_test.rs`）

```rust
#[tokio::test]
async fn test_v2_region_uses_commit_ts_revision() {
    // 装配 `RegionRuntimeSpec { revision_mode: CommitTs, .. }` 的单节点 Region，
    // 经 raft apply 一条 `Command::PutV2 { commit_ts: 1_000, .. }`，
    // 断言该 Region 的 `mvcc.current_revision() == 1_000`（不是 log index）。
}
```

  - 测试装配复用 `coord-server/tests/region_snapshot_test.rs` 的单 Region helper；若成本过高，退化为 `RegionRuntimeSpec` 字段的编译期断言 + Task 2 的 MVCC 测试，并在 PR 说明理由。

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord --test kv_t_g1_revision_test`
Expected: FAIL（字段/模式不存在）

- [ ] **Step 3: 实现**

- `RegionRuntimeSpec` 增加 `revision_mode: RevisionMode`；`spawn_region_runtime` 用它构造 `MvccStorage::new_with_revision_mode`。
- `spawn_configured_regions` 增加参数并透传；`main.rs` 传 `if cfg.kv_t.enabled { CommitTs } else { LogIndex }`。
- `run_server` 的根 `MvccStorage` 同样按 `cfg.kv_t.enabled` 选择模式。
- 更新既有 `RegionRuntimeSpec` 构造点（默认 `LogIndex`）。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord --test kv_t_g1_revision_test && cargo test -p coord-server --lib`
Expected: PASS

- [ ] **Step 5: 全量门禁 + Commit**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
bash scripts/check-panics.sh
cargo test -p coord-server --lib
cargo test -p coord --test cluster_test
bash scripts/check-doc-refs.sh
git add coord/src/main.rs coord-server/src/raft/region.rs coord-server/src/raft/region_runtime.rs coord/tests/kv_t_g1_revision_test.rs
git commit -m "feat(kv-t): wire commit-ts revision mode into v2 startup (#138)"
```

## Execution

- 分支：`feat/kv-t-g1-revision`；PR 关闭 #138，正文按验收项逐条附证据（测试名/命令/裁定）。
- 非范围（留给后续工作项）：compact 水位（#139）、snapshot 携带 revision（#140）、v2 KV RPC（#141）、lease/auth/PD/object storage 的 commit_ts 化（#145/#166/#171）。
