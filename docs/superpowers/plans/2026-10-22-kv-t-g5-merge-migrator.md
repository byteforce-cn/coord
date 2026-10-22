# KV-T G5 (#164) Merge Migrator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 merge 从 fail-closed 变成真实数据搬迁：drain 合并后的整段区间 → 以右 Region 的固定 revision 分页导出 live 行 → 经左（接收方）Region raft 分页导入 → cutover 后只剩左 Region，且左 Region 自己的历史/压缩水位不被抬高。

**Architecture:** 复用 #163 的 `RangeImportMigrator`、`RangeImportTarget`、`SplitDrain` 端口；新增 `RegionRangeResolver`（从本地已应用权威表取被吸收 Region 的 key 区间）。导入端口增加显式 `seal_prior_history` 标志：split 目标（全新 Region）在 `done` 页把历史水位钉到 `watermark`；merge 接收方已有自己的历史，只抬高 `current_revision`，绝不动 `compacted_revision`。raft 命令用独立变体 `ImportRangePreserveHistory` 承载 merge 语义，保持旧 `ImportRange` 日志条目可解码。

**Tech Stack:** Rust / openraft / redb / tokio / async-trait。

**Spec:** issue #164、ADR-0015 D6/D11.2/8；上一刀 `docs/superpowers/plans/2026-10-21-kv-t-g5-split-migrator.md`（split 搬迁器与导入端口）、`2026-10-21-kv-t-g5-split-production-wiring.md`（生产装配）。

## Global Constraints

- `[kv_t].enabled=false`（默认）时不得改变 v1 行为（ADR-0001）：搬迁器只在 `RevisionMode::CommitTs` 的目标上可用（`apply_range_import` 的既有守卫）。
- 已 commit 的 key 不丢、不双写、`mod_revision` 不倒退；左 Region 的 revision 在 merge 后不得低于任一前身。
- 搬迁器必须**幂等、可重入**：进程在任意页后重启，重放同一页返回 0、不覆盖目标更新值。
- drain 未完成（超时）不得开始导出；导出期间右 Region revision 前进必须整次失败。
- 用户 key 不得是内部前缀（`/_`）；导入行必须落在计划/被吸收区间内。
- 注释只写当前「为什么」；不写日期/变更史；不新增 `TODO`/`FIXME`。
- 临时文件只放 `/tmp/coord-*`；提交前跑 AGENTS.md 门禁（fmt / clippy / check-panics / audit / deny / check-doc-refs）。

## Rulings（本计划对 spec 的落地裁定）

1. **被吸收区间从本地权威表解析，不写进 `SplitMergePlan`**：`SplitMergePlan` 经 bincode 存进 region 0 intent journal，追加字段会让既有 intent 无法解码。新增 `RegionRangeResolver`（生产 = `RegionManager::get_region` 的 meta），在 `Prepared` 阶段按当前表解析右 Region；表里没有右 Region、或区间端点与合并计划的 `Update` 不一致 → 整次失败（intent 保留重试）。
2. **drain 覆盖合并后的整段 `[merged.start, merged.end)`**：左、右两侧的读集都必须冻结；drain 失败/超时不得导出一页。
3. **左 revision 单调、历史不压缩**：`done` 页把左 `current_revision` 抬到 `max(left_current, right_watermark)`；`compacted_revision` 只在 `seal_prior_history=true`（split 新目标）时推进。
4. **行元数据原样搬迁**：`create_revision`/`mod_revision`/`version`/`lease_id` 保留，tombstone 不导出（不复活删除）；页级按 `mod_revision` 幂等。
5. **不做右 Region 目录回收**：cutover 后由 table 驱动的 `RegionLifecycle` retire 右 Region（停机、目录保留）；物理删除不属于本切片。
6. **跨协调节点 drain 与 cutover 连续性归 #167**：本切片只保证本节点协调的会话被 drain、搬迁数据与 revision 单调。
7. **不改 `SplitMergePlan`/`RegionChange` 的 wire 形状**：merge 所需的右 Region id 已在 `RegionChange::Remove` 中；区间通过 resolver 取得。
8. **不改 `Command::ImportRange` 的形状**：merge 用末尾追加的 `ImportRangePreserveHistory` 变体；给既有变体加字段会让升级前写入的 raft 日志不可解码。

## File Structure

- Modify: `coord-server/src/storage/mvcc.rs` — `apply_range_import` 增加 `seal_prior_history`，历史压缩只在显式请求时发生。
- Modify: `coord-server/src/raft/type_config.rs` — 末尾追加 `Command::ImportRangePreserveHistory`（旧 `ImportRange` 保持不变）。
- Modify: `coord-server/src/raft/state_machine.rs` — 透传 `seal_prior_history`。
- Modify: `coord-server/src/raft/region_runtime.rs` — `RangeImportTarget::import_range` 增加参数并经 raft 提案。
- Modify: `coord-server/src/split_merge.rs` — `RegionRangeResolver`、merge 分支、merge 判定/区间校验。
- Modify: `coord/src/main.rs` — 生产装配注入 range resolver。
- Create: `coord-server/tests/kv_t_g5_merge_migrator_test.rs` — 真实左 Region raft + 真实右 Region 存储。
- Modify: `coord-server/tests/kv_t_g5_split_migrator_test.rs` — 端口新参数、删除「merge 未实现」用例。
- Modify: `coord-server/tests/kv_t_g5_region_lifecycle_test.rs` — 构造器新参数。
- Modify: `docs/production/ops/boundaries.md` — B-RG-1、B-KVT-10 现状行。

## Review Focus

- merge 后左 Region 的 `compacted_revision` 是否保持原值（尤其是左 revision 为 0 的空 Region）？
- drain 失败/超时是否**绝不**导出、绝不提案导入？
- 右 Region 不在本地表/区间不一致时是否 fail-closed，而不是按猜测的区间导数据？
- 重复执行（executor 重试/重启）是否同值重放成功、版本不变、revision 不倒退？
- 右 Region 的 revision 高于左 Region 时，左 revision 是否抬到右水位且未来 commit_ts 不会落在已导入行之下？

---

### Task 1: 导入端口显式区分 split/merge 的历史水位

**Files:**
- Modify: `coord-server/src/storage/mvcc.rs`
- Modify: `coord-server/src/raft/type_config.rs`
- Modify: `coord-server/src/raft/state_machine.rs`
- Modify: `coord-server/src/raft/region_runtime.rs`
- Modify: `coord-server/src/split_merge.rs`
- Test: `coord-server/src/storage/mvcc.rs`（单元测试）
- Test: `coord-server/src/raft/state_machine.rs`（命令幂等测试）

**Interfaces:**
- `MvccStorage::apply_range_import(&self, start, end, rows, watermark, seal_prior_history: bool, done, applied) -> Result<u64>`
- `Command::ImportRange { start_key, end_key, rows, watermark, done }`（split，行为不变）
- `Command::ImportRangePreserveHistory { start_key, end_key, rows, watermark, done }`（merge，不压缩接收方历史）
- `RangeImportTarget::import_range(&self, start, end, rows, watermark, seal_prior_history: bool, done)`

- [ ] **Step 1: 写失败测试（merge 不压缩历史）**

在 `mvcc.rs` 的 tests 里新增：

```rust
#[test]
fn test_apply_range_import_merge_keeps_target_history() {
    let (_target_dir, target) = create_commit_ts_storage();
    // 空左 Region（current_revision = 0）也存在自己的历史窗口：merge 不得
    // 因为导入右半而把左半历史标成不可用。
    let rows = vec![(b"m".to_vec(), b"1".to_vec(), KvMetadata { version: 1, create_revision: 11, mod_revision: 11, lease_id: 0, deleted: false })];
    target
        .apply_range_import(b"m", b"", &rows, 14, false, true, applied_for(1))
        .expect("merge import");
    assert_eq!(target.current_revision(), 14);
    assert_eq!(target.compacted_revision().unwrap(), 0, "merge 不压缩接收方历史");
    assert!(target.range_at_revision(b"", b"", 10, 0).is_ok(), "旧 read_ts 仍可读");
}
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p coord-server --lib storage::mvcc::tests::test_apply_range_import_merge_keeps_target_history`
Expected: FAIL（参数个数不匹配）

- [ ] **Step 3: 实现显式标志**

`apply_range_import` 签名增加 `seal_prior_history: bool`；`done` 分支改为 `if done && seal_prior_history { ... }`，注释改成「split 的新目标没有可保留的历史；merge 接收方有」。`Command::ImportRange` 保持原形状，末尾追加 `ImportRangePreserveHistory`；状态机按变体传 `true`/`false`，`CoordRangeImportTarget` 按端口参数选变体。

- [ ] **Step 4: 更新既有测试并运行**

既有 mvcc/state_machine 测试的调用补 `true`/`false`；`test_apply_import_range_command_is_idempotent_and_preserves_commit_ts` 的 Command 字面量补字段。

Run: `cargo test -p coord-server --lib storage::mvcc::tests::test_apply_range_import && cargo test -p coord-server --lib raft::state_machine::tests::test_apply_import_range_command`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add -A
git commit -m "feat(kv-t): let range imports state whether they seal prior history"
```

### Task 2: `RangeImportMigrator` 执行 merge 搬迁

**Files:**
- Modify: `coord-server/src/split_merge.rs`
- Create: `coord-server/tests/kv_t_g5_merge_migrator_test.rs`
- Modify: `coord-server/tests/kv_t_g5_split_migrator_test.rs`

**Interfaces:**
- `pub type RegionRangeResolver = dyn Fn(RegionId) -> Option<(Vec<u8>, Vec<u8>)> + Send + Sync;`
- `RangeImportMigrator::new(source, targets, ranges, drain)`
- merge 计划校验：`Update` 必须是被吸收方（`plan.source_region`）的新 meta，`Remove` 恰好一个右 Region；`right.start <= right.end` 且 `right.end == merged.end`；否则 `Err`。

- [ ] **Step 1: 写失败测试（真实左 raft 导入右半）**

在 `kv_t_g5_merge_migrator_test.rs` 建夹具：左 Region raft（`boot_left_region`，同 split 测试的 `boot_target_region`，初始化并等 leader）+ 右 Region 存储（直接 `put_at_commit_ts` 造 `m=11(lease 7)`、`z=12`、`n` 13→删除 14）。左存储先写 `a`（rev 20）。表 = `[{1, "", "m"}, {2, "m", ""}]`，`plan_merge`。

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_merge_migrator_moves_right_rows_into_left_and_keeps_history() {
    // ... 装配 ...
    migrator().migrate(&plan).await.expect("merge migration");

    assert_eq!(drain.calls(), vec![(b"".to_vec(), b"".to_vec())], "drain 覆盖合并后的整段");
    assert_eq!(left_mvcc.get(b"a").unwrap().as_deref(), Some(&b"left"[..]));
    assert_eq!(left_mvcc.get(b"m").unwrap().as_deref(), Some(&b"1"[..]));
    assert_eq!(left_mvcc.get(b"z").unwrap().as_deref(), Some(&b"3"[..]));
    assert!(left_mvcc.get(b"n").unwrap().is_none(), "tombstone 不得复活");
    let meta = left_mvcc.get_kv_metadata(b"m").unwrap().unwrap();
    assert_eq!((meta.mod_revision, meta.lease_id), (11, 7));
    assert_eq!(left_mvcc.current_revision(), 20, "较新的左 revision 不回退");
    assert_eq!(left_mvcc.compacted_revision().unwrap(), 0);
    assert_eq!(right_mvcc.current_revision(), 14, "源只读");
}
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p coord-server --test kv_t_g5_merge_migrator_test test_merge_migrator_moves`
Expected: FAIL（`merge data migration is not implemented`）

- [ ] **Step 3: 实现 merge 分支**

在 `split_merge.rs`：

- 增加 `RegionRangeResolver` 与字段；`new` 多一个参数。
- `migrate` 按 `plan.kind` 分派：split 走现有逻辑；merge：
  1. 从 `changes` 取 `Update`（region = `plan.source_region`）与唯一 `Remove`（右 Region）。
  2. 校验右区间：`right.start <= right.end`、`right.end == merged.end_key`、`merged.start_key <= right.start_key`；否则 `Err`。
  3. `drain.drain(merged.start_key, merged.end_key, timeout)`。
  4. 源 = `(self.source)(right)`（错误信息说明 merge donor 未装配）；目标 = `resolve_target(left)`。
  5. 分页导出一律带 `expected_revision`；导入传 `seal_prior_history=false`。
- 抽出 split/merge 共用的分页导入 helper（`import_pages(source, target, start, end, seal)`），避免两套分页逻辑。

- [ ] **Step 4: 运行确认通过**

Run: `cargo test -p coord-server --test kv_t_g5_merge_migrator_test`
Expected: PASS

- [ ] **Step 5: 补幂等/空左 revision 用例**

- `test_merge_migrator_is_idempotent_across_restarts`：跑两遍，`m.version`/`mod_revision` 不变。
- `test_merge_into_empty_left_raises_revision_but_keeps_history`：左空（revision 0）、右 revision 30 → 左 `current_revision=30`、`compacted_revision=0`、`range_at_revision(.., 0)` 可读。
- `test_merge_drain_failure_imports_nothing`：drain 返回 Err → 左看不到任何右行。
- `test_merge_rejects_missing_or_mismatched_donor_range`：resolver 返回 None / end 与 merged 不一致 → `Err` 且无导入。

- [ ] **Step 6: 更新 split 测试**

`kv_t_g5_split_migrator_test.rs`：构造器补 range resolver；删除 `test_split_migrator_rejects_merge_until_164`（merge 已有专属测试）。

Run: `cargo test -p coord-server --test kv_t_g5_split_migrator_test`
Expected: PASS

- [ ] **Step 7: 提交**

```bash
git add -A
git commit -m "feat(kv-t): migrate adjacent region data on merge"
```

### Task 3: 生产装配、文档与门禁

**Files:**
- Modify: `coord/src/main.rs`
- Modify: `coord-server/tests/kv_t_g5_region_lifecycle_test.rs`
- Modify: `coord-server/src/pd/executor.rs`（fail-closed 文案去掉已关闭的 `#163`）
- Modify: `docs/production/ops/boundaries.md`

- [ ] **Step 1: 生产装配**

`range_import_migrator` 内新增 range resolver：`manager.get_region(id)` → `handle.meta.read()` 的 `(start_key, end_key)`；`main.rs` 的调用点无需改参数（仍传 manager/lifecycle/drain）。

Run: `cargo build -p coord`
Expected: PASS

- [ ] **Step 2: 更新既有构造器调用**

`kv_t_g5_region_lifecycle_test.rs` 与 split 测试里的 `RangeImportMigrator::new` 补 `ranges`。

Run: `cargo test -p coord-server --test kv_t_g5_region_lifecycle_test test_lifecycle_migrates_split_range_through_production_resolvers`
Expected: PASS

- [ ] **Step 3: 更新边界文档**

`docs/production/ops/boundaries.md`：
- B-RG-1：merge 数据搬迁已交付（drain 合并整段 → 固定 revision 分页导出右 Region → 经左 Region raft 导入；左历史不变、revision 取两侧最大）；剩余跨协调节点 drain/cutover 连续性 #167；右 Region 目录回收不在切片内。
- B-KVT-10：把「merge 数据搬迁（#164）…仍未交付」改为已交付；保留 #167 未交付。

Run: `bash scripts/check-doc-refs.sh`
Expected: 悬空 = 0

- [ ] **Step 4: 全量门禁**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
bash scripts/check-panics.sh
cargo test -p coord-server
cargo audit
cargo deny check
```

Expected: 全部 PASS（audit/deny 无外网时按既有约定用缓存 DB，PR CI 复核）。

- [ ] **Step 5: 提交、开 PR、issue 进度评论**

```bash
git add -A
git commit -m "docs(kv-t): record merge migration delivery (#164)"
git push -u origin feat/kv-t-g5-merge-migrator
gh pr create --base main --fill
gh issue comment 164 --body "Merge 数据搬迁已交付：drain 合并整段 → 右 Region 固定 revision 分页导出 → 经左 Region raft 分页导入；左 revision 取两侧最大、历史不压缩。剩余：跨协调节点 drain 与 cutover 连续性（#167）。"
```

按 AGENTS.md：等 `gh pr checks --watch` 全绿后 `gh pr merge --merge --delete-branch`。
