//! #169 v2 整节点离线备份/恢复 CLI 验收。
//!
//! 备份是**停机后对整个数据目录的原样复制**（与 `coord snapshot` 的状态机
//! 快照区分）：region 0（TSO/元数据/校验索引/shard 表）、数据 Region、Commit
//! Log shard 与 Raft log/snapshot 一起走。恢复后 TSO 不复用、revision 不倒退、
//! Watch 历史（Commit Log 行）连续。

use std::path::Path;
use std::process::{Command, Output};

use coord_core::storage::StorageBackend;
use coord_core::types::StorageConfig;
use coord_server::raft::region_runtime::region_data_dir;
use coord_server::raft::type_config::CommitLogShardOp;
use coord_server::storage::commit_log::{CommitChange, CommitRecord};
use coord_server::storage::commit_log_shard::shard_region_id;
use coord_server::storage::mvcc::{AppliedLogId, MvccStorage, RevisionMode};
use coord_server::storage::redb_backend::RedbBackend;

const TSO_BATCH: u32 = 64;
const REGION0_REVISION: u64 = 300;

fn open(dir: &Path) -> MvccStorage<RedbBackend> {
    std::fs::create_dir_all(dir).unwrap();
    let backend = RedbBackend::open(dir, &StorageConfig::default()).unwrap();
    MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs).unwrap()
}

fn commit_record(ts: u64, value: &[u8]) -> CommitRecord {
    CommitRecord {
        shard_id: 1,
        commit_ts: ts,
        txn_id: format!("txn-{ts}").into_bytes(),
        start_ts: ts - 1,
        region_id: 1,
        txn_bytes: value.len() as u32,
        changes: vec![CommitChange {
            key: b"aa".to_vec(),
            value: Some(value.to_vec()),
            prev: None,
        }],
    }
}

/// 造一个最小但完整的 v2 节点数据目录：region 0 + 数据 Region 1 + Commit
/// Log shard 1，并带上 Raft log 与根密钥哨兵文件。
fn build_data_dir(root: &Path) -> u64 {
    let region0 = open(root);
    let tso = region0
        .apply_tso_allocate(TSO_BATCH, 1, AppliedLogId::standalone(1))
        .unwrap();
    region0
        .apply_commit_log_shard_op(
            &CommitLogShardOp::Open {
                start_ts: 1,
                opened_at_ms: 0,
            },
            1,
            AppliedLogId::standalone(2),
        )
        .unwrap();
    region0
        .put_at_commit_ts(
            b"/sys/rev",
            b"v",
            None,
            REGION0_REVISION,
            AppliedLogId::standalone(3),
        )
        .unwrap();
    drop(region0);

    let data = open(&region_data_dir(root, 1));
    data.put_at_commit_ts(b"aa", b"v1", None, 100, AppliedLogId::standalone(1))
        .unwrap();
    data.put_at_commit_ts(b"aa", b"v2", None, 200, AppliedLogId::standalone(2))
        .unwrap();
    drop(data);

    let shard = open(&region_data_dir(root, shard_region_id(1)));
    for (ts, value) in [(100u64, b"v1".to_vec()), (200, b"v2".to_vec())] {
        shard
            .append_commit_record(&commit_record(ts, &value), 4 * 1024 * 1024)
            .unwrap();
    }
    drop(shard);

    std::fs::create_dir_all(root.join("raft-log")).unwrap();
    std::fs::write(root.join("raft-log/log.db"), b"raft-log-sentinel").unwrap();
    std::fs::write(root.join("auth-root-key.bin"), b"key-sentinel").unwrap();
    tso.end_ts
}

fn coord(args: &[&std::ffi::OsStr]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_coord"))
        .args(args)
        .output()
        .expect("run coord binary")
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

fn path_arg(path: &Path) -> &std::ffi::OsStr {
    path.as_os_str()
}

#[test]
fn test_backup_save_restore_preserves_v2_state() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("coord-data");
    std::fs::create_dir_all(&src).unwrap();
    let tso_end = build_data_dir(&src);
    let backup = tmp.path().join("nested/backups/backup");

    let out = coord(&[
        std::ffi::OsStr::new("backup"),
        std::ffi::OsStr::new("save"),
        std::ffi::OsStr::new("--data-dir"),
        path_arg(&src),
        std::ffi::OsStr::new("--output"),
        path_arg(&backup),
    ]);
    assert!(out.status.success(), "backup save failed: {}", stderr(&out));

    let manifest: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(backup.join("coord-backup.json")).unwrap())
            .unwrap();
    assert_eq!(manifest["format"], "coord-backup");
    assert_eq!(manifest["version"], 1);
    assert_eq!(manifest["region0_tso_high_water"], tso_end);
    assert_eq!(manifest["region0_current_revision"], REGION0_REVISION);
    assert!(
        manifest["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["path"] == "raft-log/log.db"),
        "Raft log 必须进备份清单"
    );

    // 整目录恢复（先删掉源目录，模拟从备份重建节点）。
    std::fs::remove_dir_all(&src).unwrap();
    let out = coord(&[
        std::ffi::OsStr::new("backup"),
        std::ffi::OsStr::new("restore"),
        std::ffi::OsStr::new("--input"),
        path_arg(&backup),
        std::ffi::OsStr::new("--data-dir"),
        path_arg(&src),
    ]);
    assert!(
        out.status.success(),
        "backup restore failed: {}",
        stderr(&out)
    );

    // region 0：TSO 高水位/revision/shard 表（未 seal）/校验索引材料一起恢复。
    let region0 = open(&src);
    assert_eq!(region0.tso_high_water().unwrap(), tso_end);
    assert_eq!(region0.current_revision(), REGION0_REVISION);
    let shards = region0.commit_log_shard_table().unwrap();
    assert_eq!(shards.shards.len(), 1);
    assert_eq!(shards.shards[0].shard_id, 1);
    assert_eq!(shards.shards[0].end_ts, 0, "未 seal shard 必须保持未定界");
    assert_eq!(shards.shards[0].sealed_at_ms, 0);
    let next = region0
        .apply_tso_allocate(8, 2, AppliedLogId::standalone(4))
        .unwrap();
    assert!(
        next.start_ts >= tso_end,
        "恢复后 TSO 不得复用：next=[{}, {})，恢复前 high water={}",
        next.start_ts,
        next.end_ts,
        tso_end
    );
    drop(region0);

    // 数据 Region：历史读/新写都正常，revision 不倒退。
    let data = open(&region_data_dir(&src, 1));
    assert_eq!(data.get(b"aa").unwrap(), Some(b"v2".to_vec()));
    assert_eq!(
        data.get_at_revision(b"aa", 200).unwrap(),
        Some(b"v2".to_vec())
    );
    data.put_at_commit_ts(b"aa", b"v3", None, 400, AppliedLogId::standalone(3))
        .unwrap();
    assert_eq!(data.get(b"aa").unwrap(), Some(b"v3".to_vec()));
    drop(data);

    // Commit Log：cursor 之后的历史连续（Watch resume 不出现洞）。
    let shard = open(&region_data_dir(&src, shard_region_id(1)));
    let after = shard.commit_records_after(1, 100, 0).unwrap();
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].commit_ts, 200);
    assert_eq!(
        shard.commit_records_at(1, 100).unwrap(),
        vec![commit_record(100, b"v1")]
    );
    drop(shard);

    // 非 store.db 文件也必须原样恢复（Raft log / 根密钥）。
    assert_eq!(
        std::fs::read(src.join("raft-log/log.db")).unwrap(),
        b"raft-log-sentinel"
    );
    assert_eq!(
        std::fs::read(src.join("auth-root-key.bin")).unwrap(),
        b"key-sentinel"
    );
}

#[test]
fn test_backup_restore_fails_closed() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("coord-data");
    std::fs::create_dir_all(&src).unwrap();
    build_data_dir(&src);

    // 服务器还开着（本进程持有 redb 锁）⇒ 备份必须拒绝。
    let live = open(&src);
    let live_backup = tmp.path().join("live-backup");
    let out = coord(&[
        std::ffi::OsStr::new("backup"),
        std::ffi::OsStr::new("save"),
        std::ffi::OsStr::new("--data-dir"),
        path_arg(&src),
        std::ffi::OsStr::new("--output"),
        path_arg(&live_backup),
    ]);
    assert!(
        !out.status.success(),
        "运行中的 data dir 不得被备份（redb 锁必须 fail-closed）"
    );
    assert!(!live_backup.exists());
    drop(live);

    let backup = tmp.path().join("backup");
    let out = coord(&[
        std::ffi::OsStr::new("backup"),
        std::ffi::OsStr::new("save"),
        std::ffi::OsStr::new("--data-dir"),
        path_arg(&src),
        std::ffi::OsStr::new("--output"),
        path_arg(&backup),
    ]);
    assert!(out.status.success(), "save: {}", stderr(&out));

    // 非空目标目录默认拒绝；--replace 才允许，并保留旧目录。
    let target = tmp.path().join("restore-target");
    std::fs::create_dir_all(&target).unwrap();
    std::fs::write(target.join("keep-me"), b"old").unwrap();
    let out = coord(&[
        std::ffi::OsStr::new("backup"),
        std::ffi::OsStr::new("restore"),
        std::ffi::OsStr::new("--input"),
        path_arg(&backup),
        std::ffi::OsStr::new("--data-dir"),
        path_arg(&target),
    ]);
    assert!(!out.status.success(), "非空目标目录必须要求 --replace");
    assert_eq!(std::fs::read(target.join("keep-me")).unwrap(), b"old");

    let out = coord(&[
        std::ffi::OsStr::new("backup"),
        std::ffi::OsStr::new("restore"),
        std::ffi::OsStr::new("--input"),
        path_arg(&backup),
        std::ffi::OsStr::new("--data-dir"),
        path_arg(&target),
        std::ffi::OsStr::new("--replace"),
    ]);
    assert!(out.status.success(), "replace restore: {}", stderr(&out));
    assert!(
        std::fs::read(target.join("keep-me")).is_err(),
        "旧目录内容不得进入恢复后的目标目录"
    );
    let preserved = std::fs::read_dir(tmp.path())
        .unwrap()
        .filter_map(Result::ok)
        .any(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("restore-target.pre-restore-")
        });
    assert!(preserved, "被替换的旧目录必须保留供回滚");

    // 清单缺失（模拟备份中途被杀）⇒ 恢复拒绝，且不创建目标。
    let no_manifest = tmp.path().join("no-manifest");
    copy_dir(&backup, &no_manifest);
    std::fs::remove_file(no_manifest.join("coord-backup.json")).unwrap();
    let target2 = tmp.path().join("target2");
    let out = coord(&[
        std::ffi::OsStr::new("backup"),
        std::ffi::OsStr::new("restore"),
        std::ffi::OsStr::new("--input"),
        path_arg(&no_manifest),
        std::ffi::OsStr::new("--data-dir"),
        path_arg(&target2),
    ]);
    assert!(!out.status.success());
    assert!(!target2.exists(), "校验失败不得创建半成品目标目录");

    // 文件被篡改（校验和不匹配）⇒ 恢复拒绝。
    let tampered = tmp.path().join("tampered");
    copy_dir(&backup, &tampered);
    std::fs::write(
        tampered.join("data").join("raft-log").join("log.db"),
        b"tampered",
    )
    .unwrap();
    let target3 = tmp.path().join("target3");
    let out = coord(&[
        std::ffi::OsStr::new("backup"),
        std::ffi::OsStr::new("restore"),
        std::ffi::OsStr::new("--input"),
        path_arg(&tampered),
        std::ffi::OsStr::new("--data-dir"),
        path_arg(&target3),
    ]);
    assert!(!out.status.success());
    assert!(!target3.exists());

    // 格式版本不认识 ⇒ 恢复拒绝。
    let wrong_version = tmp.path().join("wrong-version");
    copy_dir(&backup, &wrong_version);
    let manifest_path = wrong_version.join("coord-backup.json");
    let mut manifest: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&manifest_path).unwrap()).unwrap();
    manifest["version"] = 2.into();
    std::fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
    let target4 = tmp.path().join("target4");
    let out = coord(&[
        std::ffi::OsStr::new("backup"),
        std::ffi::OsStr::new("restore"),
        std::ffi::OsStr::new("--input"),
        path_arg(&wrong_version),
        std::ffi::OsStr::new("--data-dir"),
        path_arg(&target4),
    ]);
    assert!(!out.status.success());
    assert!(!target4.exists());
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if from.is_dir() {
            copy_dir(&from, &to);
        } else {
            std::fs::copy(&from, &to).unwrap();
        }
    }
}
