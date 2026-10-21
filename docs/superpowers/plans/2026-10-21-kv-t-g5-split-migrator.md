# KV-T G5 (#163) Split Migrator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把「导出原语 + `Command::ImportRange`」接成真实可执行的 split 搬迁器：先 drain 目标区间，再按固定 revision 分页导出源 Region 的 live 行，逐页经**目标 Region raft** 导入，最后把新 Region 的水位钉在导入点。

**Architecture:** 新增 `coord-server/src/split_merge.rs`，用两个窄端口把数据面与 raft/会话表解耦：`RangeImportTarget`（经目标 Region raft 导入一页）与 `SplitDrain`（区间 drain）。`RangeImportMigrator` 实现既有的 `SplitMergeMigrator` 契约（幂等、可重入），只处理 Split；Merge 仍 fail-closed（#164）。导入目标是 `CoordRangeImportTarget`，包装真实 `CoordRaft` 提案 `Command::ImportRange`。

**Tech Stack:** Rust / openraft / redb / tokio / async-trait。

**Spec:** `docs/adr/0015-kv-t.md` D6/D11.2、issue #163；上一刀 `docs/superpowers/plans/2026-10-21-kv-t-g5-split-range-import.md`（导出/导入原语）；`#162` 的 `SplitMergeMigrator` 钩子契约；`#150` 的 drain 原语（`TxnSessions::drain` / `DrainGuard::wait`）。

## Global Constraints

- `[kv_t].enabled=false`（默认）时不得改变 v1 行为（ADR-0001）：搬迁器只在 `RevisionMode::CommitTs` 的目标上可用（`apply_range_import` 的既有守卫）；v1 无目标 Region raft 时自然 fail-closed。
- 已 commit 的 key 不丢、不双写、`mod_revision` 不倒退（D11.2/8）。
- 搬迁器必须**幂等、可重入**：进程在任意页后重启，重放同一页返回 0、不覆盖目标更新值、不让 revision 倒退。
- drain 未完成（超时）不得开始导出；导出期间源 revision 前进必须整次失败（不做「拼接快照」）。
- 用户 key 不得是内部前缀（`/_`）；导入行必须落在计划子区间内。
- 注释只写当前「为什么」；不写日期/变更史；不新增 `TODO`/`FIXME`。
- 临时文件只放 `/tmp/coord-*`；提交前跑 AGENTS.md 门禁（fmt / clippy / check-panics / audit / deny / check-doc-refs）。

## Rulings（本计划对 spec 的落地裁定）

1. **drain 先于导出，且只 drain 子区间**：split 只搬 `[split_key, parent.end_key)`；drain 该区间即冻结「即将易主」的读集，父区间左半不受影响。
2. **revision 只在第一页钉一次**：`watermark` = 第一页导出的源 revision；后续页用 `expected_revision` 校验源未前进。任何漂移 → 整次 `migrate` 返回 `Err`，executor 保留 `Prepared` intent 并重试；已导入的页因 `mod_revision` 守卫而幂等。
3. **空区间也要发一页 `done`**：没有 live 行时仍提案一页空 `ImportRange`，把目标 `current_revision`/`compacted_revision` 抬到 `watermark.max(1)`（`apply_range_import` 要求 watermark > 0；源 revision 0 的空白 Region 用 1）。
4. **不做源侧删除**：cutover 后父 Region 的表区间收缩，落在 `[split_key, end)` 的旧行不再被路由到；物理回收不属于本切片。搬迁只读源。
5. **Merge 保持 fail-closed**：`SplitMergeKind::Merge` → `Err`，绝不拿 split 路径糊 merge（#164 自有语义）。
6. **目标 Region 必须已装配**：本切片不预创建 pending Region（表里还没有新 Region，reconcile 不会 spawn）。resolver 取不到目标 raft 时返回可读错误并保留 intent，由后续切片的 pending 预创建接线消除。
7. **不做生产装配**：`EmbeddedPd::start`/`main.rs` 本切片不注入真实搬迁器；仍按现状 fail-closed。装配需要目标 Region 预创建（上一裁定）与会话表的 `Arc` 传递，属同一后续切片。

## File Structure

- Create: `coord-server/src/split_merge.rs` — `SplitDrain`、`RangeImportTarget`、`RegionMvccResolver`、`RegionImportTargetResolver`、`RangeImportMigrator` + 单元测试。
- Modify: `coord-server/src/lib.rs` — `pub mod split_merge;`。
- Modify: `coord-server/src/raft/region_runtime.rs` — `CoordRangeImportTarget`（`Command::ImportRange` 经 `client_write`）。
- Modify: `coord-server/src/kvt/txn.rs` — `impl SplitDrain for TxnSessions` + 单测。
- Create: `coord-server/tests/kv_t_g5_split_migrator_test.rs` — 真实目标 raft + 真实源存储的集成测试。
- Modify: `docs/production/ops/boundaries.md` — B-RG-1 现状行。

## Review Focus

- drain 超时后是否**绝不**导出？目标：`Err`，且不给目标 raft 提案任何页。
- 第一页与后续页的 `watermark` 是否恒等？任何页用「当前 revision」重取会让 `mod_revision > watermark` 的行被目标拒绝（partial import），必须整次失败重来。
- 空子区间 / 全 tombstone 子区间是否仍推进目标水位，而不是留下 `current_revision=0` 的新 Region？
- 重复执行（executor 重试/重启）是否把同 key 同值重放当成功、且不让 `version`/`mod_revision` 变化？
- 目标已有更新值（迁移期间新决策落到目标）时，旧页是否跳过而不是覆盖？

---

### Task 1: 目标 Region 的导入端口（真实 raft）

**Files:**
- Modify: `coord-server/src/raft/region_runtime.rs`
- Test: `coord-server/tests/kv_t_g5_split_migrator_test.rs`（新建）

**Interfaces:**
- `pub trait RangeImportTarget`（定义于 Task 2 的 `split_merge.rs`；本任务先建测试所需的形状）
  - `async fn import_range(&self, start: &[u8], end: &[u8], rows: Vec<VersionedKv>, watermark: Revision, done: bool) -> std::result::Result<u64, String>`
- `pub struct CoordRangeImportTarget` in `raft/region_runtime.rs`
  - `pub fn from_raft(raft: CoordRaft) -> Self`
  - `pub fn from_runtime(rt: &RegionRuntime) -> Self`
- 集成测试辅助：`boot_target_region() -> (TempDir, Arc<MvccStorage<RedbBackend>>, CoordRegionRaftHandle, CoordRangeImportTarget)`

- [ ] **Step 1: 写失败测试（harness + 首条断言）**

`coord-server/tests/kv_t_g5_split_migrator_test.rs`（复制 `kv_t_g5_split_merge_test.rs` 的单节点 raft 启动方式；`mod common;` 提供 `bind_listener`）：

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_import_range_crosses_the_target_region_raft() {
    let (_dir, mvcc, target) = boot_target_region().await;
    let rows = vec![(
        b"m".to_vec(),
        b"1".to_vec(),
        KvMetadata { version: 1, create_revision: 10, mod_revision: 10, lease_id: 7, deleted: false },
    )];
    let imported = target
        .import_range(b"m", b"", rows, 10, true)
        .await
        .expect("import through the target raft");
    assert_eq!(imported, 1);
    assert_eq!(mvcc.get(b"m").unwrap().as_deref(), Some(&b"1"[..]));
    assert_eq!(mvcc.current_revision(), 10);
    assert_eq!(mvcc.compacted_revision().unwrap(), 10);
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p coord-server --test kv_t_g5_split_migrator_test test_import_range_crosses_the_target_region_raft`
Expected: FAIL（`RangeImportTarget` / `CoordRangeImportTarget` 不存在）

- [ ] **Step 3: 实现端口的最小骨架与真实 raft 包装**

`split_merge.rs` 只先声明 trait（实现留给 Task 2 的搬迁器；`RangeImportTarget` 由 `CoordRangeImportTarget` 实现）：

- `CoordRangeImportTarget::import_range`：`self.raft.client_write(Command::ImportRange { .. }).await`，错误映射为 `format!("target region raft import: {e}")`；响应必须是 `Response::ImportRange { imported, watermark: echoed }`，`echoed != watermark` → `Err`；返回 `imported`。

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test -p coord-server --test kv_t_g5_split_migrator_test test_import_range_crosses_the_target_region_raft`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
cargo test -p coord-server --test kv_t_g5_split_migrator_test
cargo fmt --all -- --check
git add coord-server/src/split_merge.rs coord-server/src/lib.rs \
  coord-server/src/raft/region_runtime.rs coord-server/tests/kv_t_g5_split_migrator_test.rs
git commit -m "feat(kv-t): import split range pages through the target region raft"
```

### Task 2: `RangeImportMigrator`（drain → 固定 revision 分页 → 导入）

**Files:**
- Modify: `coord-server/src/split_merge.rs`
- Test: `coord-server/tests/kv_t_g5_split_migrator_test.rs`

**Interfaces:**
- `pub type RegionMvccResolver = dyn Fn(RegionId) -> Option<Arc<MvccStorage<RedbBackend>>> + Send + Sync;`
- `pub type RegionImportTargetResolver = dyn Fn(RegionId) -> Option<Arc<dyn RangeImportTarget>> + Send + Sync;`
- `pub trait SplitDrain { async fn drain(&self, start: &[u8], end: &[u8], timeout: Duration) -> std::result::Result<(), String>; }`
- `pub struct RangeImportMigrator`
  - `pub fn new(source: Arc<RegionMvccResolver>, targets: Arc<RegionImportTargetResolver>, drain: Arc<dyn SplitDrain>) -> Self`
  - `pub fn with_page_size(self, page_size: usize) -> Self`（默认 1024；0 = 不分页）
  - `pub fn with_drain_timeout(self, timeout: Duration) -> Self`（默认 30s）
- `impl SplitMergeMigrator for RangeImportMigrator`

测试替身（同文件）：`RecordingDrain { calls: Mutex<Vec<(Vec<u8>, Vec<u8>)>> }`；`DriftTarget { inner: Arc<CoordRangeImportTarget>, source: Arc<MvccStorage<RedbBackend>>, written: AtomicBool }`（首次 `import_range` 前先给源写一行新值，验证第二页导出失败）。

- [ ] **Step 1: 写失败测试**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_split_migrator_moves_live_rows_and_pins_revision() {
    let (_src_dir, source) = source_region_with_rows().await;   // a=10, m=11(lease 7), z=12
    let (_dst_dir, target_mvcc, target) = boot_target_region().await;
    let drain = RecordingDrain::new();
    let migrator = migrator(&source, &target, drain.clone()).with_page_size(1);

    migrator.migrate(&split_plan()).await.expect("split migration");

    assert_eq!(drain.calls(), vec![(b"m".to_vec(), b"".to_vec())], "drain 只覆盖子区间");
    assert_eq!(target_mvcc.get(b"m").unwrap().as_deref(), Some(&b"1"[..]));
    assert_eq!(target_mvcc.get(b"z").unwrap().as_deref(), Some(&b"3"[..]));
    assert!(target_mvcc.get(b"a").unwrap().is_none(), "父区间左半不搬迁");
    let meta = target_mvcc.get_kv_metadata(b"m").unwrap().unwrap();
    assert_eq!((meta.mod_revision, meta.lease_id), (11, 7));
    assert_eq!(target_mvcc.current_revision(), 12);
    assert_eq!(target_mvcc.compacted_revision().unwrap(), 12);
    assert_eq!(source.current_revision(), 12, "源只读、revision 不变");
}
```

再加三条：`test_split_migrator_is_idempotent_across_restarts`（跑两遍，行值/元数据不变、目标 revision 不退）、`test_split_migrator_fails_on_source_drift`（`DriftTarget` 在首屏后写源 → `Err` 含 `source revision advanced`，目标只有第一页）、`test_split_migrator_rejects_merge_until_164`。

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p coord-server --test kv_t_g5_split_migrator_test split_migrator`
Expected: FAIL（`RangeImportMigrator` 不存在）

- [ ] **Step 3: 实现 `RangeImportMigrator::migrate`**

实现要点（顺序即契约）：

1. `plan.kind != Split` → `Err("merge data migration is not implemented (#164); refusing to cut over")`。
2. 从 `plan.changes` 找 `RegionChange::Add { meta }`（`meta.region_id == plan.target_region`）作为子区间；缺失 → `Err`。
3. `source = (self.source)(plan.source_region)`；`target = (self.targets)(child.region_id)`；缺失的错误消息必须点明「runtime not assembled」。
4. `self.drain.drain(&child.start_key, &child.end_key, self.drain_timeout).await?`。
5. 分页循环：首页 `expected_revision=None`，用返回的 `page.revision` 钉 `watermark`；后续页 `expected_revision=Some(watermark)`，`after=page.next_after`。每页调用 `target.import_range(&child.start_key, &child.end_key, page.rows, watermark.max(1), page.next_after.is_none())`；`next_after == None` 结束。
6. `tracing::info!(region = child.region_id, watermark, "split range import complete")`。

- [ ] **Step 4: 运行确认通过**

Run: `cargo test -p coord-server --test kv_t_g5_split_migrator_test`
Expected: PASS（5 条）

- [ ] **Step 5: 提交**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
git add coord-server/src/split_merge.rs coord-server/tests/kv_t_g5_split_migrator_test.rs
git commit -m "feat(kv-t): drain and page-import split ranges at a pinned revision"
```

### Task 3: 会话表的 drain 端口实现

**Files:**
- Modify: `coord-server/src/kvt/txn.rs`
- Test: `coord-server/src/kvt/txn.rs`（同模块 `#[cfg(test)]`）

**Interfaces:**
- `impl SplitDrain for TxnSessions`：`ReadRange::new(start,end)` + `DrainGuard::wait(timeout)`；`TimedOut { active }` → `Err("split drain timed out with {active} active transaction(s)")`。

- [ ] **Step 1: 写失败测试**

```rust
#[tokio::test]
async fn split_drain_times_out_with_intersecting_session() {
    let sessions = TxnSessions::new();
    let id = sessions.begin(1, Duration::from_secs(30));
    sessions.record_read(&id, ReadIntent::Point(b"m"), 10).unwrap();
    let err = SplitDrain::drain(&sessions, b"m", b"", Duration::from_millis(20))
        .await
        .expect_err("active session must block the drain");
    assert!(err.contains("active transaction"), "{err}");
    sessions.abort(&id);
    SplitDrain::drain(&sessions, b"m", b"", Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn split_drain_passes_without_intersecting_session() {
    let sessions = TxnSessions::new();
    let id = sessions.begin(1, Duration::from_secs(30));
    sessions.record_read(&id, ReadIntent::Point(b"a"), 10).unwrap();
    SplitDrain::drain(&sessions, b"m", b"", Duration::from_millis(20)).await.unwrap();
}
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p coord-server --lib kvt::txn::tests::split_drain`
Expected: FAIL（`SplitDrain` 未为 `TxnSessions` 实现）

- [ ] **Step 3: 实现并导入 trait**

`use crate::split_merge::SplitDrain;` + `#[async_trait::async_trait] impl SplitDrain for TxnSessions { .. }`。

- [ ] **Step 4: 运行确认通过**

Run: `cargo test -p coord-server --lib kvt::txn::tests::split_drain`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add coord-server/src/kvt/txn.rs
git commit -m "feat(kv-t): expose the session table as the split drain port"
```

### Task 4: 边界文档与门禁

**Files:**
- Modify: `docs/production/ops/boundaries.md`（B-RG-1 现状行）

- [ ] **Step 1: 更新 B-RG-1**

把「split 数据搬迁已具备 live range 快照导出与 `Command::ImportRange` 的目标 Region raft 导入原语 …」扩为：真实搬迁器 `RangeImportMigrator`（drain 子区间 → 固定 revision 分页导出 → 逐页经目标 Region raft 导入、水位钉在导入点，幂等可重入）已交付并有集成测试；**目标 pending Region 预创建、生产装配（drain 句柄与搬迁器注入）仍未交付（#163）**，merge 归 #164。事实锚点补 `coord-server/src/split_merge.rs`、`coord-server/tests/kv_t_g5_split_migrator_test.rs`。

- [ ] **Step 2: 跑文档引用检查与全门禁**

```bash
bash scripts/check-doc-refs.sh
cargo test -p coord-server
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
bash scripts/check-panics.sh
cargo audit
cargo deny check
```

- [ ] **Step 3: 提交、开 PR、issue 进度评论**

```bash
git add docs/production/ops/boundaries.md
git commit -m "docs(kv-t): record the split migrator status (#163)"
git push -u origin feat/kv-t-g5-split-migrator
gh pr create --base main --fill
gh issue comment 163 --body "Split 搬迁器（drain → 固定 revision 分页导出 → 目标 raft 分页导入）已交付；剩余：目标 pending Region 预创建、生产装配。"
```
