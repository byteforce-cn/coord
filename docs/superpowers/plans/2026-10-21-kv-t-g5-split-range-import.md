# KV-T G5 (#163) Split Range Import Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 提供 split 数据搬迁的第一个可验证切片：从源 Region 导出一致的 live KV 快照页，并通过**目标 Region 自己的 raft 日志**导入——保留 `KvMetadata`（`create_revision`/`mod_revision`/`version`/`lease_id`）、推进目标 revision 水位、把 split 之前的区域历史标记为已压缩。

**Architecture:** 导出在源 `MvccStorage` 的单个读事务里完成（revision 与行同快照，分页时用 `expected_revision` 做漂移检测）。导入走新的 `Command::ImportRange`：命令进目标 Region 的 raft 日志，apply 在单个 redb 写事务里写 KV/元数据/changelog 水位；所有副本得到相同结果，不破坏「状态机 = 日志的确定性 apply」这一既有约束。目标 Region 在导入完成前不得服务（由后续切片的 pending 状态接线），本切片先交付数据搬运原语。

**Tech Stack:** Rust / openraft / redb / postcard / tokio。

**Spec:** `docs/adr/0015-kv-t.md` D6（任意 key 在线 Split/Merge）、D11.2（`mod_revision` 单调）、D11.5（用户级拒绝不 fail apply）；issue #163；`#162` 的 `SplitMergeMigrator` 钩子契约（幂等、可重入）。

## Global Constraints

- `[kv_t].enabled=false`（默认）时不得改变 v1 行为（ADR-0001）：`ImportRange` 只在 `RevisionMode::CommitTs` 下可用；v1 收到该命令必须 fail-closed。
- 已 commit 的 key 不丢、不双写、`mod_revision` 不倒退（D11.2/8）。
- 导入页幂等、可重入：进程在任意页后重启，重放同一页不得重复写或覆盖更新的目标值。
- 用户 key 不得是内部前缀（`/_`）；导入行必须落在计划区间内。
- 注释只写当前「为什么」；不写日期/变更史；不新增 `TODO`/`FIXME`。
- 临时文件只放 `/tmp/coord-*`；提交前跑 AGENTS.md 门禁（fmt / clippy / check-panics / audit / deny / check-doc-refs）。

## Rulings（本计划对 spec 的落地裁定）

1. **只搬 live 行，不搬旧 changelog**：split 后的新 Region 把 `compacted_revision` 抬到导入水位 `watermark`，早于它的历史读返回 `RevisionCompacted`——明确不可用，绝不返回残缺历史。watch 依赖全局 Commit Log（#153/#156），不依赖 Region changelog。
2. **导入必须经目标 Region raft**：直接写目标 `store.db` 会破坏日志↔状态机等价（新 follower/快照/压缩会分叉）。`Command::ImportRange` 是唯一的导入入口。
3. **分页用 `expected_revision` fail-closed**：源在导出期间前进（未 drain 完成）时导出立即失败，而不是把不同 revision 的行拼进一个快照。执行器后续必须在 freeze 后才开始导出。
4. **导入不产生 Watch 事件**：变更已经存在于全局 Commit Log；再发一次会让 watch 重复。
5. **目标已有更新值时保留目标**：重入/并发决策落到目标后，旧页不得把 `mod_revision` 拉回退；仅当目标 `mod_revision` 严格更旧时写入。

## File Structure

- Modify: `coord-server/src/storage/mvcc.rs` — `RangeSnapshotPage` / `export_range_page` / `apply_range_import` + 单元测试。
- Modify: `coord-server/src/raft/type_config.rs` — `Command::ImportRange`、`Response::ImportRange`、`Display`。
- Modify: `coord-server/src/raft/state_machine.rs` — `execute_command` 分支 + apply 测试。
- Modify: `docs/production/ops/boundaries.md` — B-RG-1 记录 split 数据面当前进度。

## 非本切片（#163 保持 open）

- 源侧 freeze/drain 与 cutover 顺序、目标 pending（不服务）状态、executor 的 `SplitMergeMigrator` 真实接线。
- Merge 数据搬迁（#164）。
- 分页执行器的重试/退避与目录回收。

## Review Focus

- 同一 `commit_ts` 的多个 key 是否仍共享一个 revision？本切片按行保留各自 `mod_revision`，不合成事务事件；导入后的历史读水位必须让旧历史明确不可用。
- `lease_id` 是否随行搬迁？revoke 依赖新 Region 的 `KvMetadata.lease_id`。
- 分页边界：`after` 为最后一个已返回 key；空页、tombstone、区间端点不得死循环或漏 key。
- 重入：目标已应用更高 `commit_ts` 的决策时，旧页不得覆盖或让 revision 倒退。
- v1 集群收到 `ImportRange` 必须拒绝（不能把 log index 当 commit_ts）。

---

### Task 1: 源 Region 的 live range 快照页

**Files:**
- Modify: `coord-server/src/storage/mvcc.rs`
- Test: `coord-server/src/storage/mvcc.rs`（`#[cfg(test)] mod tests` 追加）

**Interfaces:**
- Consumes: `MvccStorage::{current_revision, backend}`, `StorageBackend::read`, `ReadTx::iter_range`, `encode_kv_meta_key`, `KvMetadata::from_bytes`, `decrypt_value`。
- Produces:
  - `pub struct RangeSnapshotPage { pub rows: Vec<VersionedKv>, pub next_after: Option<Vec<u8>>, pub revision: Revision }`
  - `pub fn export_range_page(&self, start: &[u8], end: &[u8], after: Option<&[u8]>, limit: usize, expected_revision: Option<Revision>) -> Result<RangeSnapshotPage>`
  - `limit == 0` = 不限；`next_after` 为下一页的 `after`，`None` = 已到区间末尾。

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn test_export_range_page_preserves_metadata_and_skips_tombstones() {
    let (_dir, storage) = create_commit_ts_storage();
    storage.put_at_commit_ts(b"a", b"1", Some(7), 10, applied_for(1)).unwrap();
    storage.put_at_commit_ts(b"b", b"2", None, 11, applied_for(2)).unwrap();
    storage.put_at_commit_ts(b"c", b"3", None, 12, applied_for(3)).unwrap();
    storage.delete_at_commit_ts(b"b", 13, applied_for(4)).unwrap();

    let page = storage
        .export_range_page(b"a", b"d", None, 0, Some(13))
        .expect("export range");
    assert_eq!(page.revision, 13);
    assert_eq!(page.next_after, None);
    let keys: Vec<&[u8]> = page.rows.iter().map(|(k, _, _)| k.as_slice()).collect();
    assert_eq!(keys, vec![b"a".as_slice(), b"c".as_slice()]);
    assert_eq!(page.rows[0].2.mod_revision, 10);
    assert_eq!(page.rows[0].2.lease_id, 7);
    assert_eq!(page.rows[1].2.mod_revision, 12);
}

#[test]
fn test_export_range_page_paginates_and_rejects_revision_drift() {
    let (_dir, storage) = create_commit_ts_storage();
    for (i, key) in [b"a".as_slice(), b"b".as_slice(), b"c".as_slice()].into_iter().enumerate() {
        storage.put_at_commit_ts(key, b"v", None, 10 + i as u64, applied_for(i as u64 + 1)).unwrap();
    }
    let first = storage.export_range_page(b"a", b"z", None, 1, None).unwrap();
    assert_eq!(first.rows.len(), 1);
    assert_eq!(first.next_after.as_deref(), Some(&b"a"[..]));
    let second = storage
        .export_range_page(b"a", b"z", first.next_after.as_deref(), 1, Some(first.revision))
        .unwrap();
    assert_eq!(second.rows[0].0, b"b");
    storage.put_at_commit_ts(b"d", b"v", None, 20, applied_for(9)).unwrap();
    let err = storage
        .export_range_page(b"a", b"z", second.next_after.as_deref(), 1, Some(first.revision))
        .expect_err("source advanced during export");
    assert!(matches!(err, Error::FailedPrecondition(_)));
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p coord-server --lib storage::mvcc::tests::test_export_range_page -- --nocapture`
Expected: FAIL（`export_range_page` 不存在）

- [ ] **Step 3: 实现 `RangeSnapshotPage` 与 `export_range_page`**

实现要点：
- 一个读事务内读取 `current_revision_in_tx` 与元数据行；`expected_revision` 不一致返回 `Error::FailedPrecondition`。
- 扫描 `TABLE_KV_META` 的 `[KV_META_PREFIX+start, KV_META_PREFIX+end)`；`after` 以 `after + [0]` 作为严格后继起点。
- 跳过 `deleted` 行；每条 live 行在同一事务内读 `TABLE_KV` 并 `decrypt_value`；值缺失按 `DataCorruption` 拒绝。
- 达到 `limit > 0` 后停止，把最后一个**已返回** key 写入 `next_after`；未达上限时 `next_after = None`。

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test -p coord-server --lib storage::mvcc::tests::test_export_range_page`
Expected: PASS（2 tests）

- [ ] **Step 5: Commit**

```bash
git add coord-server/src/storage/mvcc.rs
git commit -m "feat(kv-t): export live range snapshots for split migration"
```

---

### Task 2: 目标 Region 的幂等 range 导入

**Files:**
- Modify: `coord-server/src/storage/mvcc.rs`
- Test: `coord-server/src/storage/mvcc.rs`（`#[cfg(test)] mod tests` 追加）

**Interfaces:**
- Consumes: Task 1 的 `VersionedKv`、`KvMetadata`、`AppliedLogId`、`META_CURRENT_REVISION`、`META_COMPACT_REVISION`、`META_LAST_APPLIED`、`encrypt_value`。
- Produces:
  - `pub fn apply_range_import(&self, start: &[u8], end: &[u8], rows: &[VersionedKv], watermark: Revision, done: bool, applied: AppliedLogId) -> Result<u64>`
  - 返回本次**新写入**的行数；重放/目标已更新时为 0。

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn test_apply_range_import_preserves_metadata_and_compacts_prior_history() {
    let (source_dir, source) = create_commit_ts_storage();
    source.put_at_commit_ts(b"a", b"1", Some(9), 10, applied_for(1)).unwrap();
    source.put_at_commit_ts(b"b", b"2", None, 11, applied_for(2)).unwrap();
    let page = source.export_range_page(b"a", b"z", None, 0, Some(11)).unwrap();

    let (_target_dir, target) = create_commit_ts_storage();
    let imported = target
        .apply_range_import(b"a", b"z", &page.rows, page.revision, true, applied_for(5))
        .expect("import");
    assert_eq!(imported, 2);
    assert_eq!(target.get(b"a").unwrap().as_deref(), Some(&b"1"[..]));
    let meta = target.get_kv_metadata(b"a").unwrap().unwrap();
    assert_eq!((meta.mod_revision, meta.lease_id), (10, 9));
    assert_eq!(target.current_revision(), 11);
    assert_eq!(target.compacted_revision().unwrap(), 11);
    assert!(matches!(
        target.range_at_revision(b"a", b"z", 0, 5),
        Err(Error::RevisionCompacted { .. })
    ));

    let replay = target
        .apply_range_import(b"a", b"z", &page.rows, page.revision, true, applied_for(6))
        .expect("replay");
    assert_eq!(replay, 0, "同一页重放不得重复写入");
    let _ = source_dir;
}

#[test]
fn test_apply_range_import_keeps_newer_target_values() {
    let (_target_dir, target) = create_commit_ts_storage();
    target.put_at_commit_ts(b"a", b"new", None, 20, applied_for(1)).unwrap();
    let rows = vec![(
        b"a".to_vec(),
        b"old".to_vec(),
        KvMetadata { version: 1, create_revision: 10, mod_revision: 10, lease_id: 0, deleted: false },
    )];
    let imported = target
        .apply_range_import(b"a", b"z", &rows, 15, true, applied_for(2))
        .expect("import older page");
    assert_eq!(imported, 0);
    assert_eq!(target.get(b"a").unwrap().as_deref(), Some(&b"new"[..]));
    assert_eq!(target.current_revision(), 20, "导入不得让 revision 倒退");
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p coord-server --lib storage::mvcc::tests::test_apply_range_import`
Expected: FAIL（`apply_range_import` 不存在）

- [ ] **Step 3: 实现 `apply_range_import`**

实现要点：
- 校验：`watermark > 0`；每行 `key` 在 `[start,end)` 且不以 `/_` 开头；`meta.deleted == false`；`0 < meta.mod_revision <= watermark`；`0 < meta.create_revision <= meta.mod_revision`；页内 key 去重。
- 单个写事务：读目标现有元数据；目标 `mod_revision` 严格更大 → 跳过；相等 → 解密比较值，不同返回 `DataCorruption`；目标更旧 → 写 `encrypt_value` 与完整 `KvMetadata`。
- `done` 时把 `META_CURRENT_REVISION` 抬到 `max(current, watermark)`，把 `META_COMPACT_REVISION` 抬到 `min(max(current_after, watermark), watermark)`（即 `watermark`，且不高于 current）；无论 `done` 与否都在同一事务持久化 `META_LAST_APPLIED`。
- `RevisionMode::LogIndex` 下返回 `Error::FailedPrecondition`（v1 不接受该原语）。

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test -p coord-server --lib storage::mvcc::tests::test_apply_range_import`
Expected: PASS（2 tests）

- [ ] **Step 5: Commit**

```bash
git add coord-server/src/storage/mvcc.rs
git commit -m "feat(kv-t): apply idempotent range imports at commit_ts"
```

---

### Task 3: `Command::ImportRange` 通过目标 Region raft 应用

**Files:**
- Modify: `coord-server/src/raft/type_config.rs`
- Modify: `coord-server/src/raft/state_machine.rs`
- Test: `coord-server/src/raft/state_machine.rs`（`#[cfg(test)] mod tests` 追加）

**Interfaces:**
- Consumes: Task 2 的 `MvccStorage::apply_range_import`；`Command`/`Response` 的 postcard 信封。
- Produces:
  - `Command::ImportRange { start_key: Vec<u8>, end_key: Vec<u8>, rows: Vec<(Vec<u8>, Vec<u8>, KvMetadata)>, watermark: u64, done: bool }`
  - `Response::ImportRange { imported: u64, watermark: u64 }`
  - `KvMetadata` 增加 `Serialize, Deserialize`（字段顺序即 postcard 格式，保持现有字段顺序）。

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn test_apply_import_range_command_is_idempotent_and_preserves_commit_ts() {
    let (_tmp, store) = setup_store_with_mode(RevisionMode::CommitTs);
    let rows = vec![(
        b"k".to_vec(),
        b"v".to_vec(),
        KvMetadata {
            version: 3,
            create_revision: 7,
            mod_revision: 42,
            lease_id: 0,
            deleted: false,
        },
    )];
    let cmd = Command::ImportRange {
        start_key: b"a".to_vec(),
        end_key: b"z".to_vec(),
        rows: rows.clone(),
        watermark: 42,
        done: true,
    };
    let (resp, event) = store
        .execute_command(store.state_machine.as_ref(), &cmd, 5, applied_at(5))
        .expect("import apply");
    assert!(event.is_none(), "import 不得重复发 watch 事件");
    match resp {
        Response::ImportRange { imported, watermark } => {
            assert_eq!((imported, watermark), (1, 42));
        }
        other => panic!("unexpected response: {other:?}"),
    }
    assert_eq!(store.state_machine.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
    assert_eq!(store.state_machine.current_revision(), 42);
    let meta = store.state_machine.get_kv_metadata(b"k").unwrap().unwrap();
    assert_eq!((meta.create_revision, meta.mod_revision, meta.version), (7, 42, 3));

    let (replay, _) = store
        .execute_command(store.state_machine.as_ref(), &cmd, 6, applied_at(6))
        .expect("replay import");
    match replay {
        Response::ImportRange { imported, .. } => assert_eq!(imported, 0),
        other => panic!("unexpected response: {other:?}"),
    }
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p coord-server --lib raft::state_machine::tests::test_apply_import_range_command`
Expected: FAIL（`Command::ImportRange` 变体不存在）

- [ ] **Step 3: 实现命令、响应与 apply 分支**

实现要点：
- `Command` / `Response` 的新变体**追加在枚举末尾**（旧日志 postcard 兼容）。
- `Response::Display` 增加 `ImportRange(imported=.., watermark=..)`。
- `execute_command` 分支：`sm.apply_range_import(start_key, end_key, rows, watermark, done, applied)` → `Response::ImportRange`，返回事件 `None`；错误经 `io_err`。
- `[kv_t].enabled=false`（`RevisionMode::LogIndex`）时 `apply_range_import` 返回 `FailedPrecondition`，apply 不会静默按 log index 处理。

- [ ] **Step 4: 运行测试确认通过**

Run: `cargo test -p coord-server --lib raft::state_machine::tests::test_apply_import_range_command`
Expected: PASS

- [ ] **Step 5: 更新边界文档**

`docs/production/ops/boundaries.md` 的 B-RG-1 现状行补一句：split 数据搬迁已具备 live range 快照导出与 `ImportRange` raft 导入原语；freeze/cutover/pending 接线与 merge 仍未交付（#163/#164）。

- [ ] **Step 6: 跑门禁并提交**

```bash
cargo test -p coord-server --lib storage::mvcc::tests::test_export_range_page storage::mvcc::tests::test_apply_range_import
cargo test -p coord-server --lib raft::state_machine::tests
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
bash scripts/check-panics.sh
bash scripts/check-doc-refs.sh
git add coord-server/src/storage/mvcc.rs coord-server/src/raft/type_config.rs \
  coord-server/src/raft/state_machine.rs docs/production/ops/boundaries.md
git commit -m "feat(kv-t): import split range snapshots through the target raft"
```
