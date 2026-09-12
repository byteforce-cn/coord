// CLI 凭据持久化 + 自动续期（批次 12 / 计划 §19「CLI 自动续期」收口）
//
// 背景：此前 CLI 凭据是**进程级一次性注入**（`--token` / `COORD_TOKEN`）——
// CCT 默认 15 分钟过期后，长时脚本的后续管理命令会突然 `permission denied`，
// 只能靠调用方手工「两段式」`auth login --print-refresh` → 定期 `auth refresh`。
//
// 本模块提供凭据文件（默认 `$XDG_CONFIG_HOME/coord/credentials.json`，权限 0600）：
// - `coord auth login` 落盘 CCT + refresh token + 到期时刻 + 目标地址；
// - 后续 CLI 命令（`CliConn::connect_authed`）在**临近/已过期**时用 refresh token
//   换新并**回写**（服务端保证单次使用 → 不回写会让文件立刻失效）；
// - `coord auth logout` 删除；`coord auth status` 查看状态。
//
// 安全：
// - unix 下文件权限 0600、目录 0700；
// - 凭据仅在**地址一致**时被使用（避免把 A 集群的凭据发到 B 集群）；
// - refresh 失败 → 命令**显式失败**（fail-closed），不静默使用过期凭据。

use std::path::{Path, PathBuf};

/// 续期提前量（秒）：与 coord-client 的后台续期循环（`spawn_session_refresher`）
/// 取同一常量，避免两处行为漂移。
pub const REFRESH_LEAD_SECS: i64 = coord_client::refresh::REFRESH_LEAD_SECS;

/// 落盘的 CLI 会话凭据。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StoredCredentials {
    /// 目标集群地址（`host:port`）。地址不匹配的文件**不会**被使用。
    pub addr: String,
    /// 用户名（诊断用；服务端不校验此字段）
    #[serde(default)]
    pub user: String,
    /// 受限 CCT
    pub cct: String,
    /// refresh token（单次使用；空 = 无自动续期能力）
    #[serde(default)]
    pub refresh_token: String,
    /// CCT 到期时刻（Unix 秒；0 = 未知）
    #[serde(default)]
    pub expires_at: i64,
}

impl StoredCredentials {
    /// 是否具备自动续期能力。
    pub fn can_refresh(&self) -> bool {
        !self.refresh_token.trim().is_empty()
    }
}

/// 当前 Unix 秒。
pub fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_secs()).unwrap_or(0))
        .unwrap_or(0)
}

/// 默认凭据路径：`$COORD_CREDENTIALS` > `$XDG_CONFIG_HOME/coord/credentials.json`
/// > `$HOME/.config/coord/credentials.json`。
///
/// 均不可用时回退到当前目录（不 panic —— CLI 必须在任何环境下可用）。
pub fn default_path() -> PathBuf {
    if let Ok(explicit) = std::env::var("COORD_CREDENTIALS") {
        if !explicit.trim().is_empty() {
            return PathBuf::from(explicit);
        }
    }
    let base = match std::env::var("XDG_CONFIG_HOME") {
        Ok(dir) if !dir.trim().is_empty() => PathBuf::from(dir),
        _ => match std::env::var("HOME") {
            Ok(home) if !home.trim().is_empty() => PathBuf::from(home).join(".config"),
            _ => PathBuf::from("."),
        },
    };
    base.join("coord").join("credentials.json")
}

/// 读取凭据（文件缺失 / 解析失败 → `None`；不打印错误，调用方按「无凭据」处理）。
pub fn load(path: &Path) -> Option<StoredCredentials> {
    let raw = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&raw).ok()
}

/// 落盘凭据（目录 0700 / 文件 0600；父目录自动创建）。
pub fn save(path: &Path, creds: &StoredCredentials) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("failed to create {}: {e}", parent.display()))?;
            restrict_permissions(parent, 0o700);
        }
    }
    let body = serde_json::to_vec_pretty(creds)
        .map_err(|e| format!("failed to encode credentials: {e}"))?;

    #[cfg(unix)]
    {
        use std::io::Write as _;
        use std::os::unix::fs::OpenOptionsExt as _;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)
            .map_err(|e| format!("failed to open {}: {e}", path.display()))?;
        file.write_all(&body)
            .map_err(|e| format!("failed to write {}: {e}", path.display()))?;
    }
    #[cfg(not(unix))]
    {
        std::fs::write(path, &body)
            .map_err(|e| format!("failed to write {}: {e}", path.display()))?;
    }
    // 已存在的文件不受 `mode()` 影响 → 显式收紧权限。
    restrict_permissions(path, 0o600);
    Ok(())
}

/// 删除凭据文件；返回是否真的删除了（`Ok(false)` = 本来就不存在）。
pub fn remove(path: &Path) -> Result<bool, String> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(format!("failed to remove {}: {e}", path.display())),
    }
}

/// 是否需要在本次使用前续期。
///
/// - `expires_at <= 0`（未知）→ `true`（保守：宁可换一次也不带着可能过期的 CCT 出站）；
/// - 剩余寿命 <= [`REFRESH_LEAD_SECS`] → `true`。
pub fn needs_refresh(expires_at: i64, now: i64) -> bool {
    if expires_at <= 0 {
        return true;
    }
    expires_at - now <= REFRESH_LEAD_SECS
}

#[cfg(unix)]
fn restrict_permissions(path: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt as _;
    if let Err(e) = std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)) {
        tracing::debug!("failed to set permissions on {}: {e}", path.display());
    }
}

#[cfg(not(unix))]
fn restrict_permissions(_path: &Path, _mode: u32) {}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> StoredCredentials {
        StoredCredentials {
            addr: "127.0.0.1:50051".into(),
            user: "root".into(),
            cct: "cct-abc".into(),
            refresh_token: "rt-abc".into(),
            expires_at: 4_000_000_000,
        }
    }

    #[test]
    fn save_load_roundtrip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("nested").join("credentials.json");
        assert!(load(&path).is_none(), "missing file → None");
        save(&path, &sample()).expect("save");
        assert_eq!(load(&path), Some(sample()));
        assert!(save(&path, &sample()).is_ok(), "overwrite must be allowed");
        assert!(remove(&path).expect("remove"));
        assert!(!remove(&path).expect("remove missing"), "idempotent remove");
        assert!(load(&path).is_none());
    }

    #[cfg(unix)]
    #[test]
    fn credentials_file_is_owner_only() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("credentials.json");
        save(&path, &sample()).expect("save");
        let mode = std::fs::metadata(&path).expect("stat").permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "credentials must be 0600, got {mode:o}");
        // 预先以宽权限存在的文件也会被收紧
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).expect("chmod");
        save(&path, &sample()).expect("re-save");
        let mode = std::fs::metadata(&path).expect("stat").permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "re-save must tighten permissions");
    }

    #[test]
    fn needs_refresh_window_and_unknown_expiry() {
        let now = 1_000_000;
        assert!(!needs_refresh(now + 900, now), "fresh CCT needs no refresh");
        assert!(
            needs_refresh(now + REFRESH_LEAD_SECS, now),
            "inside lead window must refresh"
        );
        assert!(needs_refresh(now - 5, now), "expired must refresh");
        assert!(needs_refresh(0, now), "unknown expiry must refresh");
    }

    #[test]
    fn default_path_prefers_env_override() {
        // 只验证解析顺序中的显式覆盖分支（避免与其它测试的 env 竞争）。
        let key = "COORD_CREDENTIALS";
        let prev = std::env::var(key).ok();
        std::env::set_var(key, "/tmp/coord-test-credentials.json");
        assert_eq!(
            default_path(),
            PathBuf::from("/tmp/coord-test-credentials.json")
        );
        match prev {
            Some(v) => std::env::set_var(key, v),
            None => std::env::remove_var(key),
        }
    }

    #[test]
    fn can_refresh_requires_non_empty_token() {
        let mut creds = sample();
        assert!(creds.can_refresh());
        creds.refresh_token = "   ".into();
        assert!(!creds.can_refresh());
    }
}
