# KV-T G6 Backup/Restore v2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the v2 backup/restore slice of #169: state-machine snapshots carry the Commit Log table, and `coord backup save/restore` performs a verified offline full-node backup that preserves TSO, revision, Commit Log shard state, and watch history.

**Architecture:** Two independent changes. (1) `SnapshotData` (v7) exports/imports `TABLE_COMMIT_LOG` so Raft snapshot installs and per-region snapshots keep Watch history, and import refuses to lower `current_revision`. (2) A new offline backup command copies the whole node data directory (region 0 + data regions + Commit Log shard regions + Raft logs/snapshots) into a staged directory with a SHA-256 manifest, and restores it only after verifying every file; the swap is atomic per data directory.

**Tech Stack:** Rust 2021, `redb` (via `RedbBackend`), `postcard` envelope, `serde_json`, `sha2`, `clap`, `tempfile` in tests.

**Spec:** `docs/adr/0015-kv-t.md` (D3/D5/D7/D9/D11), issue #169, and `docs/production/ops/runbook.md` §3.

## Global Constraints

- `[kv_t].enabled=false` must stay byte-for-byte v1: new code paths only run when explicitly invoked, except `import_snapshot_data`'s revision guard (an invariant of D11.2 that also protects v1).
- Snapshot format is fail-closed: only `CURRENT_VERSION` is accepted; old version bytes are rejected (no migration ladder).
- User-level conflicts/rejections must not make Raft apply return `Err` (D11.5). The new backup CLI is offline and not on the apply path.
- `commit_ts`/TSO never reuse; restored TSO high water must be `>=` the manifest value.
- Documentation has one owner per fact: runbook for the procedure, `boundaries.md` for the limits, ADR for the anchor list.
- Before PR: `cargo fmt --all -- --check`, `cargo clippy --workspace --lib --bins -- -D warnings`, `bash scripts/check-panics.sh`, `cargo audit`, `cargo deny check`, and `bash scripts/check-doc-refs.sh`.

## Review Focus

- A shard region that installs a Raft snapshot from a lagging leader: after install it must still return every record that the shard table/watermark says is readable (otherwise Watch silently skips history).
- Restoring a backup into a data directory that still has data: must fail closed unless `--replace`; the old directory must not be destroyed before verification.
- Backup taken while the server is running: opening a region store with `RedbBackend` must fail on the live redb lock; a partial copy must never be accepted as a backup.
- A backup whose file set is incomplete (killed mid-copy) or tampered: restore must reject it before touching the target.
- An older snapshot imported over a newer store: `current_revision` must not go backwards (D11.2).

---

### Task 1: Carry the Commit Log in `SnapshotData`

**Files:**
- Modify: `coord-server/src/storage/snapshot.rs` (`SnapshotData`, `new`, `CURRENT_VERSION`, `export_snapshot_data`, `import_snapshot_data`, tests)
- Test: `coord-server/src/storage/snapshot.rs::tests`

**Interfaces:**
- Produces: `SnapshotData.commit_log_entries: Vec<SnapshotRawEntry>`; `CURRENT_VERSION = 7`.
- Produces: `import_snapshot_data` returns `Error::Storage` when `data.current_revision < storage.current_revision()`.
- Consumes: `MvccStorage`/`TABLE_COMMIT_LOG` (already `pub(crate)`), `COMMIT_LOG_PREFIX`.

- [ ] **Step 1: Write the failing test**

Add to `mod tests`:

```rust
#[test]
fn test_snapshot_preserves_commit_log_records() {
    let (tmp, storage) = setup_storage();
    let record = crate::storage::commit_log::CommitRecord {
        shard_id: 1,
        commit_ts: 10,
        txn_id: b"txn-10".to_vec(),
        start_ts: 5,
        region_id: 7,
        txn_bytes: 1,
        changes: vec![crate::storage::commit_log::CommitChange {
            key: b"aa".to_vec(),
            value: Some(b"v".to_vec()),
            prev: None,
        }],
    };
    storage.append_commit_record(&record, 4 * 1024 * 1024).unwrap();

    let data = export_snapshot_data(&storage, 1, 1).unwrap();
    assert_eq!(data.commit_log_entries.len(), 1);

    // 目标已有旧记录：导入必须整体替换，不得留下快照之外的 shard 行。
    let (_tmp2, storage2) = setup_storage();
    storage2
        .append_commit_record(
            &crate::storage::commit_log::CommitRecord {
                commit_ts: 99,
                ..record.clone()
            },
            4 * 1024 * 1024,
        )
        .unwrap();
    import_snapshot_data(&storage2, &data).unwrap();

    assert_eq!(storage2.commit_records_at(1, 10).unwrap(), vec![record]);
    assert!(storage2.commit_records_at(1, 99).unwrap().is_empty());
}

#[test]
fn test_import_snapshot_rejects_revision_regression() {
    let (_tmp, storage) = setup_commit_ts_storage();
    storage
        .put_at_commit_ts(b"/k", b"v", None, 100, AppliedLogId::standalone(1))
        .unwrap();
    let data = export_snapshot_data(&storage, 1, 1).unwrap();

    let (_tmp2, newer) = setup_commit_ts_storage();
    newer
        .put_at_commit_ts(b"/k", b"v", None, 200, AppliedLogId::standalone(1))
        .unwrap();

    let err = import_snapshot_data(&newer, &data).expect_err("不得把 revision 装回去");
    assert!(matches!(err, Error::Storage(_)));
    assert_eq!(newer.current_revision(), 200);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p coord-server storage::snapshot::tests::test_snapshot_preserves_commit_log_records storage::snapshot::tests::test_import_snapshot_rejects_revision_regression`
Expected: compile error / missing field; then the revision test fails because import succeeds.

- [ ] **Step 3: Implement**

- Add `pub commit_log_entries: Vec<SnapshotRawEntry>` to `SnapshotData`; initialize in `new()`.
- Set `CURRENT_VERSION = 7`; update the version comment (v7: Commit Log table; v6: `current_revision`).
- In the single `backend.read` of `export_snapshot_data`, read `tx.iter_prefix(TABLE_COMMIT_LOG, crate::storage::commit_log::COMMIT_LOG_PREFIX)?` and extend the tuple; map rows to `SnapshotRawEntry`.
- In `import_snapshot_data`, before `backend.write`:
  ```rust
  let current = storage.current_revision();
  if data.current_revision < current {
      return Err(Error::Storage(format!(
          "snapshot revision {} regresses current revision {}",
          data.current_revision, current
      )));
  }
  ```
- Inside the write transaction, clear `TABLE_COMMIT_LOG` under `COMMIT_LOG_PREFIX` (same pattern as changelog), then insert every `commit_log_entries` row.
- Update the module header and the `SnapshotData` doc comments.

- [ ] **Step 4: Run the snapshot tests**

Run: `cargo test -p coord-server storage::snapshot::tests`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add coord-server/src/storage/snapshot.rs
git commit -m "fix(kv-t): carry the commit log in state-machine snapshots (#169)"
```

---

### Task 2: Offline full-node backup CLI

**Files:**
- Create: `coord/src/backup.rs`
- Modify: `coord/src/main.rs` (module declaration, `BackupCmd`, dispatch)
- Modify: `coord/Cargo.toml` (`sha2 = { workspace = true }`)
- Test: `coord/tests/backup_cli_test.rs`

**Interfaces:**
- Produces: `backup::save(data_dir: &Path, output: &Path) -> Result<BackupManifest, Box<dyn Error>>`
- Produces: `backup::restore(input: &Path, data_dir: &Path, replace: bool) -> Result<BackupManifest, Box<dyn Error>>`
- Produces on disk: `<output>/coord-backup.json` + `<output>/data/**`; manifest fields `format`, `version`, `coord_version`, `created_unix_ms`, `region0_tso_high_water`, `region0_current_revision`, `entries[]` (`path`, `size`, `sha256`).
- Consumes: `coord_server::raft::region_runtime::region_data_dir` only for documentation; the copy is whole-directory.

- [ ] **Step 1: Write the failing integration test**

Create `coord/tests/backup_cli_test.rs` with a helper that builds a fake v2 node data dir:

```rust
fn build_data_dir(root: &Path) -> (u64, u64) {
    // region 0
    let backend = RedbBackend::open(root, &StorageConfig::default()).unwrap();
    let region0 = MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs).unwrap();
    let tso = region0.apply_tso_allocate(64, 1, AppliedLogId::standalone(1)).unwrap();
    region0.apply_commit_log_shard_op(
        &CommitLogShardOp::Open { start_ts: 1, opened_at_ms: 0 },
        1,
        AppliedLogId::standalone(2),
    ).unwrap();
    region0.put_at_commit_ts(b"/sys/rev", b"v", None, 300, AppliedLogId::standalone(3)).unwrap();
    drop(region0);

    // data region 1
    let dir1 = region_data_dir(root, 1);
    let backend = RedbBackend::open(&dir1, &StorageConfig::default()).unwrap();
    let data = MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs).unwrap();
    data.put_at_commit_ts(b"aa", b"v1", None, 100, AppliedLogId::standalone(1)).unwrap();
    data.put_at_commit_ts(b"aa", b"v2", None, 200, AppliedLogId::standalone(2)).unwrap();
    drop(data);

    // commit-log shard region 1
    let shard_dir = region_data_dir(root, shard_region_id(1));
    let backend = RedbBackend::open(&shard_dir, &StorageConfig::default()).unwrap();
    let shard = MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs).unwrap();
    for ts in [100u64, 200] {
        shard.append_commit_record(&CommitRecord {
            shard_id: 1,
            commit_ts: ts,
            txn_id: format!("txn-{ts}").into_bytes(),
            start_ts: ts - 1,
            region_id: 1,
            txn_bytes: 2,
            changes: vec![CommitChange {
                key: b"aa".to_vec(),
                value: Some(if ts == 100 { b"v1".to_vec() } else { b"v2".to_vec() }),
                prev: None,
            }],
        }, 4 * 1024 * 1024).unwrap();
    }
    drop(shard);

    std::fs::create_dir_all(root.join("raft-log")).unwrap();
    std::fs::write(root.join("raft-log/log.db"), b"raft-log-sentinel").unwrap();
    std::fs::write(root.join("auth-root-key.bin"), b"key-sentinel").unwrap();
    (tso.end_ts, 300)
}
```

Test 1 (`test_backup_save_restore_preserves_v2_state`):
- Build source in `TempDir`.
- Run `coord backup save --data-dir <src> --output <bak>`; assert success and `bak/coord-backup.json` exists.
- Assert the manifest lists `data/raft-log/log.db`.
- Wipe `<src>`; run `coord backup restore --input <bak> --data-dir <src>`; assert success.
- Reopen region 0: `tso_high_water() == tso_end`, `current_revision() == 300`, `commit_log_shard_table().shards.len() == 1` and `end_ts == 0`; allocate again and assert `start_ts >= tso_end`.
- Reopen region 1: `get_at_revision(b"aa", 200) == Some(b"v2")`; write at 400 and read it back.
- Reopen shard: `commit_records_after(1, 100, 0)` returns ts 200 and `commit_records_at(1, 200)` still returns the record (watch history continuous).
- Assert `raft-log/log.db` and `auth-root-key.bin` bytes match.

Test 2 (`test_backup_restore_fails_closed_on_bad_input`):
- Corrupt one file listed in the manifest → `restore` exits non-zero and the target is unchanged.
- Delete `coord-backup.json` (simulates a kill mid-copy) → `restore` exits non-zero.
- Restore into a non-empty target without `--replace` → exits non-zero; with `--replace` → succeeds.

Test 3 (`test_backup_restore_rejects_format_mismatch`):
- Edit `coord-backup.json`'s `version` to `2` → restore exits non-zero.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p coord --test backup_cli_test`
Expected: clap error (`unrecognized subcommand 'backup'`) / non-zero status.

- [ ] **Step 3: Implement `coord/src/backup.rs` and wire the CLI**

Key decisions:
- `save`: refuse if `output` exists; refuse if `output` is inside `data_dir`; `data_dir` must contain `store.db`.
- Stage at `<output_parent>/.<output_name>.backup-tmp-<pid>`; copy recursively with `std::fs::read` chunking (64 KiB), `sha2::Sha256`, and `File::sync_all`; reject symlinks/special files; write manifest last; `rename(stage, output)`.
- `restore`: read/validate manifest (`format == "coord-backup"`, `version == 1`); reject unsafe paths (`..`, absolute), missing files, size/hash mismatch **before** writing; stage at `<data_parent>/.<data_name>.restore-tmp-<pid>`; copy verified files; open `stage/store.db` and require `tso_high_water >= manifest.region0_tso_high_water` and `current_revision >= manifest.region0_current_revision`; if `data_dir` exists and is non-empty, require `replace`, then `rename(data_dir, data_dir.pre-restore-<ms>)`; `rename(stage, data_dir)`.
- Print a one-line summary for save/restore.
- `coord_version` mismatch is a warning, not an error (format/version are hard checks).
- Clap: `Backup(BackupCmd)` with `Save { data_dir, output }`, `Restore { input, data_dir, #[arg(long)] replace }`; defaults `data_dir = /var/lib/coord`.

- [ ] **Step 4: Run the integration test**

Run: `cargo test -p coord --test backup_cli_test`
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add coord/src/backup.rs coord/src/main.rs coord/Cargo.toml coord/tests/backup_cli_test.rs
git commit -m "feat(coord): add offline v2 data-dir backup and restore (#169)"
```

### Task 3: Stabilize the watch cancel test (flake found by the full suite)

**Files:**
- Modify: `coord/tests/kv_t_g4_watch_rpc_test.rs` (`test_watch_rpc_ack_and_cancel`)

The contract is "Cancel 后不再产出事件", not "the stream closes immediately": a
watermark-only empty batch already queued before the cancel may still arrive.
The old assertion treated any frame as a failure and flakes under full-suite load.
Drain for the 700 ms window, allow empty frames/close, and fail on any `events`.

- [x] **Step 1: Replace the single-message assertion with a 700 ms drain**
- [x] **Step 2: Run the test five times; all green**
- [x] **Step 3: Commit** (`test(kv-t): tolerate watermark-only frames after watch cancel`)

---

### Task 4: Docs and issue close-out

**Files:**
- Modify: `docs/adr/0015-kv-t.md` (Snapshot anchors: add `test_snapshot_preserves_commit_log_records`; note backup CLI)
- Modify: `docs/production/ops/runbook.md` (§3.3 v2 offline full-node backup/restore; distinguish from Raft snapshot)
- Modify: `docs/production/ops/boundaries.md` (B-KVT-4: Commit Log rows travel in snapshots; new B-KVT-12 for offline whole-node backup limits)
- Modify: `docs/production/consumer-contract.md` ("Commit Log 随快照/备份恢复归 #169" → current behavior)
- Comment on #169 with evidence (tests/commands) and remaining residuals.

- [ ] **Step 1: Update the docs with exact commands and limits**
- [ ] **Step 2: Run `bash scripts/check-doc-refs.sh`; expect 0 dangling paths**
- [ ] **Step 3: Run the full gate** (fmt, clippy, check-panics, audit, deny) and the affected tests
- [ ] **Step 4: Commit and open the PR** (`gh pr create --base main --fill`, `gh pr checks --watch`, merge per coord convention `--merge`)

---

## Self-Review

- **Spec coverage:** #169 scope (region 0/TSO, region MVCC, Commit Log shards, validation index) is covered by the whole-directory copy; acceptance (restore test, no TSO reuse, watch continuity, format consistency) is covered by Tasks 1–2 tests; non-scope (v1→v2, cross-region/geo backup, Raft snapshot as backup) is documented in Task 3.
- **Step scan:** each test step carries concrete assertions; the implementation steps name exact signatures/paths; no "handle edge cases" placeholders.
- **Type consistency:** `commit_log_entries: Vec<SnapshotRawEntry>`, `BackupManifest` fields, and the `save`/`restore` signatures are used consistently across tasks.
- **Review Focus:** shard snapshot install (Task 1 test), restore over live/partial data (Task 2 tests), version/revision regressions (Tasks 1–2), live-server lock (Task 2 save path).
- **Residuals:** online consistent cluster-wide backup and cross-node restore are out of scope; the CLI is offline and per-node. The issue stays open until those are either delivered or explicitly declared non-goals.
