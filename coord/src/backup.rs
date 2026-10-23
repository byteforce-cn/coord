//! #169：v2 整节点离线备份/恢复。
//!
//! 为什么不是 `coord snapshot`：那是一次**状态机快照**（`SnapshotData`），不含
//! Raft log、成员关系与 per-Region 目录；把它当备份恢复会丢掉快照点之后的已
//! 提交日志，也无法跨节点重建（ADR-0004 的边界）。这里备份的是**停机后的整个
//! 数据目录原样复制**：region 0（TSO/元数据/校验索引/shard 表）、各数据 Region、
//! Commit Log shard、Raft log/snapshot 与根密钥一起走；恢复后节点以同一
//! node_id 继续。
//!
//! 一致性靠三点：
//! 1. 备份前用 `RedbBackend::open` 拿 region 0 的 redb 写锁——服务器还开着时
//!    必然失败（fail-closed），复制期间也一直持锁；
//! 2. 清单（`coord-backup.json`）逐个文件记 SHA-256；恢复时边复制进 staging
//!    边校验，任何缺失/篡改/未完成备份都在换目录前拒绝；
//! 3. 复核 staging 的 TSO/revision 后，才把目标目录原子换出。

use std::collections::BTreeSet;
use std::error::Error;
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use coord_core::storage::StorageBackend;
use coord_core::types::StorageConfig;
use coord_server::storage::mvcc::{MvccStorage, RevisionMode};
use coord_server::storage::redb_backend::RedbBackend;

pub const MANIFEST_FILE: &str = "coord-backup.json";

const DATA_DIR_NAME: &str = "data";
const FORMAT: &str = "coord-backup";
const FORMAT_VERSION: u32 = 1;
const COPY_BUFFER_BYTES: usize = 64 * 1024;

/// 备份清单（`<backup>/coord-backup.json`）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupManifest {
    /// 固定为 `coord-backup`，防止把别的 JSON 当备份。
    pub format: String,
    /// 清单格式版本（不是 coord 版本）。
    pub version: u32,
    /// 产出备份的 coord 版本（跨版本恢复只告警，不拒绝）。
    pub coord_version: String,
    pub created_unix_ms: u64,
    /// region 0 的 TSO 高水位（下一未发放值）。
    pub region0_tso_high_water: u64,
    /// region 0 的 current revision（v2 = 最大 commit_ts）。
    pub region0_current_revision: u64,
    /// `data/` 下的所有文件，按路径排序。
    pub entries: Vec<BackupEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupEntry {
    /// 相对 `<backup>/data/` 的路径（`/` 分隔，无 `..`）。
    pub path: String,
    pub size: u64,
    /// 小写 hex SHA-256。
    pub sha256: String,
}

/// 备份一个已停机的 coord 数据目录。
pub fn save(data_dir: &Path, output: &Path) -> Result<BackupManifest, Box<dyn Error>> {
    if !data_dir.is_dir() {
        return Err(format!("data dir {} is not a directory", data_dir.display()).into());
    }
    if !data_dir.join("store.db").is_file() {
        return Err(format!(
            "{} is not a coord data directory (store.db is missing)",
            data_dir.display()
        )
        .into());
    }
    if output.exists() {
        return Err(format!(
            "backup output {} already exists; refusing to overwrite",
            output.display()
        )
        .into());
    }
    ensure_disjoint(data_dir, output)?;

    let parent = parent_dir(output)?;
    fs::create_dir_all(&parent)?;
    let stage = stage_path(output, "backup")?;
    reset_dir(&stage)?;
    let data_root = stage.join(DATA_DIR_NAME);
    fs::create_dir_all(&data_root)?;

    let result = (|| -> Result<BackupManifest, Box<dyn Error>> {
        // 复制期间一直持有 region 0 的 redb 锁：服务器没停就开不到这里，
        // 开着时也无法在复制中途启动（写锁会冲突）。
        let region0 = open_region0(data_dir)?;
        let tso_high_water = region0.tso_high_water()?;
        let current_revision = region0.current_revision();
        copy_tree(data_dir, &data_root)?;
        drop(region0);

        let entries = hash_tree(&data_root)?;
        if entries.is_empty() {
            return Err("data dir contains no files; refusing an empty backup".into());
        }
        let manifest = BackupManifest {
            format: FORMAT.to_string(),
            version: FORMAT_VERSION,
            coord_version: env!("CARGO_PKG_VERSION").to_string(),
            created_unix_ms: unix_ms(),
            region0_tso_high_water: tso_high_water,
            region0_current_revision: current_revision,
            entries,
        };
        write_manifest(&stage, &manifest)?;
        Ok(manifest)
    })();

    match result {
        Ok(manifest) => {
            fs::rename(&stage, output).map_err(|e| {
                format!(
                    "publish backup {} -> {}: {e}",
                    stage.display(),
                    output.display()
                )
            })?;
            println!(
                "Backup saved: {} files, TSO high water {}, current revision {} → {}",
                manifest.entries.len(),
                manifest.region0_tso_high_water,
                manifest.region0_current_revision,
                output.display()
            );
            Ok(manifest)
        }
        Err(err) => {
            let _ = fs::remove_dir_all(&stage);
            Err(err)
        }
    }
}

/// 从整节点备份恢复一个已停机的数据目录。
///
/// 目标非空时必须显式 `replace`；旧目录会被改名保留（`*.pre-restore-<ms>`），
/// 不会在验证前被删除。
pub fn restore(
    input: &Path,
    data_dir: &Path,
    replace: bool,
) -> Result<BackupManifest, Box<dyn Error>> {
    if !input.is_dir() {
        return Err(format!("backup input {} is not a directory", input.display()).into());
    }
    ensure_disjoint(input, data_dir)?;
    let manifest = read_manifest(input)?;
    validate_manifest(input, &manifest)?;
    if manifest.coord_version != env!("CARGO_PKG_VERSION") {
        eprintln!(
            "warning: backup was written by coord {} (this binary is {}); \
             restore only checks the manifest format, not the binary ABI",
            manifest.coord_version,
            env!("CARGO_PKG_VERSION")
        );
    }

    if data_dir.exists() && !data_dir.is_dir() {
        return Err(format!("target {} is not a directory", data_dir.display()).into());
    }
    if data_dir.exists() && !replace && !dir_is_empty(data_dir)? {
        return Err(format!(
            "target {} is not empty; stop the node and pass --replace \
             (the old directory is preserved for rollback)",
            data_dir.display()
        )
        .into());
    }

    let parent = parent_dir(data_dir)?;
    fs::create_dir_all(&parent)?;
    let stage = stage_path(data_dir, "restore")?;
    reset_dir(&stage)?;

    let result = (|| -> Result<(), Box<dyn Error>> {
        verify_and_copy(input, &stage, &manifest)?;
        // 复核 staging 里的 region 0：清单与文件必须是同一次备份，且 TSO/revision
        // 不得低于清单（低 = 混入旧目录 / 被回拨 ⇒ 恢复后会重用时间戳）。
        let region0 = open_region0(&stage)?;
        let tso = region0.tso_high_water()?;
        let revision = region0.current_revision();
        drop(region0);
        if tso < manifest.region0_tso_high_water {
            return Err(format!(
                "backup TSO high water {} is lower than manifest {}",
                tso, manifest.region0_tso_high_water
            )
            .into());
        }
        if revision < manifest.region0_current_revision {
            return Err(format!(
                "backup current revision {} is lower than manifest {}",
                revision, manifest.region0_current_revision
            )
            .into());
        }

        let previous = if data_dir.exists() {
            let previous = previous_path(data_dir);
            fs::rename(data_dir, &previous).map_err(|e| {
                format!(
                    "move target {} aside to {}: {e}",
                    data_dir.display(),
                    previous.display()
                )
            })?;
            Some(previous)
        } else {
            None
        };
        if let Err(err) = fs::rename(&stage, data_dir) {
            if let Some(previous) = &previous {
                let _ = fs::rename(previous, data_dir);
            }
            return Err(format!(
                "publish restored data {} -> {}: {err}",
                stage.display(),
                data_dir.display()
            )
            .into());
        }
        if let Some(previous) = previous {
            println!(
                "Previous data directory preserved at {} (delete after verification)",
                previous.display()
            );
        }
        println!(
            "Backup restored: {} files, TSO high water {}, current revision {} → {}",
            manifest.entries.len(),
            manifest.region0_tso_high_water,
            manifest.region0_current_revision,
            data_dir.display()
        );
        Ok(())
    })();

    if result.is_err() {
        let _ = fs::remove_dir_all(&stage);
    }
    result.map(|()| manifest)
}

fn read_manifest(input: &Path) -> Result<BackupManifest, Box<dyn Error>> {
    let path = input.join(MANIFEST_FILE);
    let bytes =
        fs::read(&path).map_err(|e| format!("read backup manifest {}: {e}", path.display()))?;
    let manifest: BackupManifest = serde_json::from_slice(&bytes)
        .map_err(|e| format!("parse backup manifest {}: {e}", path.display()))?;
    if manifest.format != FORMAT {
        return Err(format!(
            "unexpected backup format {:?} (expected {FORMAT})",
            manifest.format
        )
        .into());
    }
    if manifest.version != FORMAT_VERSION {
        return Err(format!(
            "unsupported backup format version {} (expected {FORMAT_VERSION})",
            manifest.version
        )
        .into());
    }
    if manifest.entries.is_empty() {
        return Err("backup manifest lists no files".into());
    }
    if !manifest
        .entries
        .iter()
        .any(|entry| entry.path == "store.db")
    {
        return Err("backup manifest has no root store.db (region 0 is missing)".into());
    }
    Ok(manifest)
}

fn validate_manifest(input: &Path, manifest: &BackupManifest) -> Result<(), Box<dyn Error>> {
    let root = input.join(DATA_DIR_NAME);
    let mut seen = BTreeSet::new();
    for entry in &manifest.entries {
        let rel = safe_relative(&entry.path)?;
        if !seen.insert(rel.clone()) {
            return Err(format!("duplicate backup entry {:?}", entry.path).into());
        }
        let path = root.join(&rel);
        let meta = fs::symlink_metadata(&path)
            .map_err(|e| format!("backup file {}: {e}", path.display()))?;
        if !meta.file_type().is_file() {
            return Err(format!("backup entry {} is not a regular file", path.display()).into());
        }
        if meta.len() != entry.size {
            return Err(format!(
                "backup file {} size {} does not match manifest {}",
                path.display(),
                meta.len(),
                entry.size
            )
            .into());
        }
    }
    Ok(())
}

fn verify_and_copy(
    input: &Path,
    stage: &Path,
    manifest: &BackupManifest,
) -> Result<(), Box<dyn Error>> {
    let root = input.join(DATA_DIR_NAME);
    for entry in &manifest.entries {
        let rel = safe_relative(&entry.path)?;
        let from = root.join(&rel);
        let to = stage.join(&rel);
        if let Some(parent) = to.parent() {
            fs::create_dir_all(parent)?;
        }
        let meta = fs::metadata(&from)?;
        let mut src = File::open(&from)?;
        let mut dst = File::create(&to)?;
        let mut hasher = Sha256::new();
        let mut size = 0u64;
        let mut buf = vec![0u8; COPY_BUFFER_BYTES];
        loop {
            let read = src
                .read(&mut buf)
                .map_err(|e| format!("read backup file {}: {e}", from.display()))?;
            if read == 0 {
                break;
            }
            hasher.update(&buf[..read]);
            dst.write_all(&buf[..read])
                .map_err(|e| format!("write restored file {}: {e}", to.display()))?;
            size += read as u64;
        }
        dst.sync_all()?;
        if size != entry.size || hex::encode(hasher.finalize()) != entry.sha256 {
            return Err(format!(
                "backup file {} checksum mismatch; backup is incomplete or corrupted",
                from.display()
            )
            .into());
        }
        fs::set_permissions(&to, meta.permissions())?;
    }
    Ok(())
}

fn write_manifest(stage: &Path, manifest: &BackupManifest) -> Result<(), Box<dyn Error>> {
    let path = stage.join(MANIFEST_FILE);
    let mut file = File::create(&path)?;
    serde_json::to_writer_pretty(&mut file, manifest)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

fn open_region0(data_dir: &Path) -> Result<MvccStorage<RedbBackend>, Box<dyn Error>> {
    let backend = RedbBackend::open(data_dir, &StorageConfig::default()).map_err(|e| {
        format!(
            "open {} (is the server still running?): {e}",
            data_dir.display()
        )
    })?;
    Ok(MvccStorage::new_with_revision_mode(
        backend,
        RevisionMode::CommitTs,
    )?)
}

fn copy_tree(src: &Path, dst: &Path) -> Result<(), Box<dyn Error>> {
    fs::create_dir_all(dst)?;
    let mut children: Vec<PathBuf> = fs::read_dir(src)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<_, _>>()?;
    children.sort();
    for path in children {
        let meta = fs::symlink_metadata(&path)?;
        let file_type = meta.file_type();
        if file_type.is_symlink() {
            return Err(format!(
                "refusing to back up symlink {}; data dirs must contain regular files only",
                path.display()
            )
            .into());
        }
        let name = path
            .file_name()
            .ok_or_else(|| format!("path without file name: {}", path.display()))?;
        let target = dst.join(name);
        if file_type.is_dir() {
            copy_tree(&path, &target)?;
        } else if file_type.is_file() {
            let mut reader = File::open(&path)?;
            let mut writer = File::create(&target)?;
            io::copy(&mut reader, &mut writer)
                .map_err(|e| format!("copy {} → {}: {e}", path.display(), target.display()))?;
            writer.sync_all()?;
            fs::set_permissions(&target, meta.permissions())?;
        } else {
            return Err(format!("refusing to back up non-regular file {}", path.display()).into());
        }
    }
    Ok(())
}

fn hash_tree(root: &Path) -> Result<Vec<BackupEntry>, Box<dyn Error>> {
    let mut entries = Vec::new();
    hash_tree_into(root, root, &mut entries)?;
    entries.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(entries)
}

fn hash_tree_into(
    root: &Path,
    dir: &Path,
    entries: &mut Vec<BackupEntry>,
) -> Result<(), Box<dyn Error>> {
    let mut children: Vec<PathBuf> = fs::read_dir(dir)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<_, _>>()?;
    children.sort();
    for path in children {
        let meta = fs::symlink_metadata(&path)?;
        if meta.file_type().is_dir() {
            hash_tree_into(root, &path, entries)?;
        } else if meta.file_type().is_file() {
            let rel = path
                .strip_prefix(root)
                .map_err(|e| format!("{} is not under {}: {e}", path.display(), root.display()))?;
            let rel = path_to_manifest(rel)?;
            let (size, sha256) = hash_file(&path)?;
            entries.push(BackupEntry {
                path: rel,
                size,
                sha256,
            });
        } else {
            return Err(format!("backup contains non-regular file {}", path.display()).into());
        }
    }
    Ok(())
}

fn hash_file(path: &Path) -> Result<(u64, String), Box<dyn Error>> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; COPY_BUFFER_BYTES];
    let mut size = 0u64;
    loop {
        let read = file.read(&mut buf)?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
        size += read as u64;
    }
    Ok((size, hex::encode(hasher.finalize())))
}

fn path_to_manifest(path: &Path) -> Result<String, Box<dyn Error>> {
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => parts.push(
                part.to_str()
                    .ok_or_else(|| format!("non-UTF-8 backup path: {}", path.display()))?,
            ),
            _ => {
                return Err(
                    format!("unexpected backup path component in {}", path.display()).into(),
                )
            }
        }
    }
    Ok(parts.join("/"))
}

fn safe_relative(path: &str) -> Result<PathBuf, Box<dyn Error>> {
    if path.is_empty() || path.starts_with('/') || path.contains('\\') {
        return Err(format!("unsafe backup entry path {path:?}").into());
    }
    let mut out = PathBuf::new();
    for component in Path::new(path).components() {
        match component {
            Component::Normal(part) => out.push(part),
            _ => return Err(format!("unsafe backup entry path {path:?}").into()),
        }
    }
    if out.as_os_str().is_empty() {
        return Err(format!("unsafe backup entry path {path:?}").into());
    }
    Ok(out)
}

/// `a` 与 `b` 不得互相包含（复制/换目录会递归或覆盖）。
fn ensure_disjoint(a: &Path, b: &Path) -> Result<(), Box<dyn Error>> {
    let a = canonical_ish(a)?;
    let b = canonical_ish(b)?;
    if a.starts_with(&b) || b.starts_with(&a) {
        return Err(format!(
            "{} and {} must not contain each other",
            a.display(),
            b.display()
        )
        .into());
    }
    Ok(())
}

fn canonical_ish(path: &Path) -> Result<PathBuf, Box<dyn Error>> {
    // 目标/输出可能有多层父目录尚不存在（`coord backup` 会创建它们）：
    // 找到最近的已存在祖先并规范化，再把缺失的尾部原样接回去。
    let mut suffix: Vec<std::ffi::OsString> = Vec::new();
    let mut current = path.to_path_buf();
    loop {
        if current.exists() {
            let mut out = current.canonicalize()?;
            for part in suffix.iter().rev() {
                out.push(part);
            }
            return Ok(out);
        }
        let name = current
            .file_name()
            .ok_or_else(|| format!("path has no file name: {}", path.display()))?
            .to_os_string();
        suffix.push(name);
        current = current
            .parent()
            .ok_or_else(|| format!("path has no existing ancestor: {}", path.display()))?
            .to_path_buf();
    }
}

fn parent_dir(path: &Path) -> Result<PathBuf, Box<dyn Error>> {
    let parent = path
        .parent()
        .ok_or_else(|| format!("path has no parent: {}", path.display()))?;
    if parent.as_os_str().is_empty() {
        Ok(PathBuf::from("."))
    } else {
        Ok(parent.to_path_buf())
    }
}

fn stage_path(target: &Path, kind: &str) -> Result<PathBuf, Box<dyn Error>> {
    let parent = parent_dir(target)?;
    let name = target
        .file_name()
        .ok_or_else(|| format!("path has no file name: {}", target.display()))?
        .to_string_lossy();
    Ok(parent.join(format!(".{name}.{kind}-tmp-{}", std::process::id())))
}

fn previous_path(target: &Path) -> PathBuf {
    let parent = target.parent().unwrap_or_else(|| Path::new("."));
    let name = target
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "coord-data".to_string());
    let base = format!("{name}.pre-restore-{}", unix_ms());
    let mut candidate = parent.join(&base);
    let mut suffix = 0u32;
    while candidate.exists() {
        suffix += 1;
        candidate = parent.join(format!("{base}-{suffix}"));
    }
    candidate
}

fn reset_dir(path: &Path) -> io::Result<()> {
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    fs::create_dir_all(path)
}

fn dir_is_empty(path: &Path) -> io::Result<bool> {
    Ok(fs::read_dir(path)?.next().is_none())
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}
