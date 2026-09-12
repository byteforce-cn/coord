// 审计日志子系统
//
// v1 范围：
// - `AuditEvent`：actor / action / resource / result / detail / 时间戳；
// - `AuditLogger::record`：异步追加到 `<data_dir>/audit/audit-<date>.log`
//   （每行一条 JSON，行缓冲 + 可配置 flush 策略）；
// - 内存环形缓冲（最近 N 条）供 `recent()` 查询接口；
// - 挂载点：鉴权拒绝（interceptor fail-closed 路径）+ 认证成功/失败、
//   refresh 成功/失败（AuthService）。管理操作的调用方身份传递为 v2 backlog
//   （tower 层无对端地址/令牌主体，已文档化）。
//
// 存储与查询接口：`AuditStore` trait（`append`/`recent`），生产实现为
// `FileAuditStore`；查询接口 `AuditLogger::recent(limit)`。

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use coord_core::error::{Error, Result};

/// 审计结果分类
pub const RESULT_SUCCESS: &str = "success";
pub const RESULT_DENIED: &str = "denied";
pub const RESULT_FAILED: &str = "failed";

/// 一条审计事件
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuditEvent {
    /// 事件时间（unix 毫秒）
    pub ts_ms: u64,
    /// 行为主体（v1：用户名；鉴权拒绝路径为 "anonymous"）
    pub actor: String,
    /// 动作（如 "auth.authenticate" / RPC 方法路径）
    pub action: String,
    /// 资源（用户名 / 角色名 / 键前缀 / RPC 方法）
    pub resource: String,
    /// 结果：success / denied / failed
    pub result: String,
    /// 附加信息（拒绝原因等）
    pub detail: String,
}

impl AuditEvent {
    pub fn new(actor: &str, action: &str, resource: &str, result: &str, detail: &str) -> Self {
        Self {
            ts_ms: now_ms(),
            actor: actor.to_string(),
            action: action.to_string(),
            resource: resource.to_string(),
            result: result.to_string(),
            detail: detail.to_string(),
        }
    }
}

/// 审计存储接口（存储与查询接口）
pub trait AuditStore: Send + Sync {
    /// 追加一条事件（生产实现：追加到当日日志文件）
    fn append(&self, event: &AuditEvent) -> Result<()>;
    /// 查询最近 N 条（生产实现：文件尾部 + 内存环形缓冲）
    fn recent(&self, limit: usize) -> Vec<AuditEvent>;
}

/// 生产实现：文件追加（每日一个文件，一行一条 JSON）+ 内存环形缓冲
pub struct FileAuditStore {
    dir: PathBuf,
    ring: Mutex<std::collections::VecDeque<AuditEvent>>,
    ring_capacity: usize,
}

impl FileAuditStore {
    pub fn new(dir: PathBuf, ring_capacity: usize) -> Result<Self> {
        std::fs::create_dir_all(&dir)
            .map_err(|e| Error::Storage(format!("create audit dir: {e}")))?;
        Ok(Self {
            dir,
            ring: Mutex::new(std::collections::VecDeque::with_capacity(ring_capacity)),
            ring_capacity,
        })
    }

    fn today_file(&self) -> PathBuf {
        let secs = now_ms() / 1000;
        // 按 UTC 日期分文件（与日志时间戳同口径）
        let days = secs / 86400;
        let rem = secs % 86400;
        let (y, m, d) = civil_from_days(days as i64);
        let _ = rem;
        self.dir.join(format!("audit-{y:04}-{m:02}-{d:02}.log"))
    }
}

/// days → (year, month, day)（Howard Hinnant 算法简化版，UTC）
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

impl AuditStore for FileAuditStore {
    fn append(&self, event: &AuditEvent) -> Result<()> {
        let mut line = serde_json::to_string(event)
            .map_err(|e| Error::Internal(format!("serialize audit event: {e}")))?;
        line.push('\n');

        let path = self.today_file();
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| Error::Storage(format!("open audit file {}: {e}", path.display())))?;
        file.write_all(line.as_bytes())
            .map_err(|e| Error::Storage(format!("write audit file: {e}")))?;
        // 审计事件强制落盘（写成功即不丢；写失败由 `AuditLogger::record` 以
        // ERROR + 失败计数暴露——本函数**不**能保证"审计不丢事件"）
        file.sync_all()
            .map_err(|e| Error::Storage(format!("fsync audit file: {e}")))?;

        let mut ring = self.ring.lock();
        if ring.len() >= self.ring_capacity {
            ring.pop_front();
        }
        ring.push_back(event.clone());
        Ok(())
    }

    fn recent(&self, limit: usize) -> Vec<AuditEvent> {
        self.ring.lock().iter().rev().take(limit).cloned().collect()
    }
}

/// 审计日志门面：记录入口 + 查询接口。
#[derive(Clone)]
pub struct AuditLogger {
    store: Arc<dyn AuditStore>,
    /// 追加失败计数（第三轮 §4.2③：让"审计缺口"可观测，而不是只打一条 warn）
    append_failures: Arc<AtomicU64>,
}

impl AuditLogger {
    /// 构建日志器（store 为持久化后端）。
    pub fn new(store: Arc<dyn AuditStore>) -> Self {
        Self {
            store,
            append_failures: Arc::new(AtomicU64::new(0)),
        }
    }

    /// 生产构造：`<data_dir>/audit/` 文件后端 + 最近 1024 条环形缓冲。
    pub fn file_logger(data_dir: &Path) -> Result<Self> {
        let store = FileAuditStore::new(data_dir.join("audit"), 1024)?;
        Ok(Self::new(Arc::new(store)))
    }

    /// 记录一条审计事件。
    ///
    /// # 失败语义（第三轮 §4.2③明确化）
    ///
    /// 追加失败**不阻断主路径**（审计不得成为拒绝服务面），但绝不等于"审计不丢事件"：
    /// 磁盘满 / 权限变更 / IO 错误都会让事件**永久丢失**。因此：
    /// - 失败以 **ERROR** 级别记录（此前是 `warn!`，与"合规可查"的承诺不匹配）；
    /// - 失败计入 [`Self::append_failures`]，供运维监控"审计缺口"；
    /// - [`FileAuditStore::append`] 确实做了 `sync_all()`——但那只能保证"写成功时不丢"，
    ///   不能把"写失败"变成"不丢"。门面与实现的表述必须一致。
    pub fn record(&self, event: AuditEvent) {
        if let Err(e) = self.store.append(&event) {
            self.append_failures.fetch_add(1, Ordering::Relaxed);
            tracing::error!(
                "AUDIT APPEND FAILED (event lost): {e} — 审计链出现缺口，请立即检查磁盘/权限"
            );
        }
    }

    /// 累计的审计追加失败次数（进程生命周期内）。
    ///
    /// 非零即表示审计链**存在缺口**，不应被当成健康状态。
    pub fn append_failures(&self) -> u64 {
        self.append_failures.load(Ordering::Relaxed)
    }

    /// 便捷记录：actor/action/resource/result/detail。
    pub fn record_event(
        &self,
        actor: &str,
        action: &str,
        resource: &str,
        result: &str,
        detail: &str,
    ) {
        self.record(AuditEvent::new(actor, action, resource, result, detail));
    }

    /// 查询最近 N 条事件（新→旧）。
    pub fn recent(&self, limit: usize) -> Vec<AuditEvent> {
        self.store.recent(limit)
    }
}

/// 当前 unix 毫秒
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ──── 测试 ────

#[cfg(test)]
mod tests {
    use super::*;

    struct MemStore {
        events: Mutex<Vec<AuditEvent>>,
    }

    impl AuditStore for MemStore {
        fn append(&self, event: &AuditEvent) -> Result<()> {
            self.events.lock().push(event.clone());
            Ok(())
        }

        fn recent(&self, limit: usize) -> Vec<AuditEvent> {
            self.events
                .lock()
                .iter()
                .rev()
                .take(limit)
                .cloned()
                .collect()
        }
    }

    #[test]
    fn test_record_and_recent() {
        let logger = AuditLogger::new(Arc::new(MemStore {
            events: Mutex::new(Vec::new()),
        }));
        logger.record_event("alice", "auth.authenticate", "alice", RESULT_SUCCESS, "");
        logger.record_event(
            "anonymous",
            "/coord.kv.KV/Put",
            "data:kv:write",
            RESULT_DENIED,
            "missing token",
        );
        let recent = logger.recent(10);
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].result, RESULT_DENIED, "newest first");
        assert_eq!(recent[1].actor, "alice");
    }

    #[test]
    fn test_file_store_append_and_recent() {
        let tmpdir = tempfile::tempdir().unwrap();
        let store = FileAuditStore::new(tmpdir.path().to_path_buf(), 4).unwrap();
        let logger = AuditLogger::new(Arc::new(store));
        for i in 0..6 {
            logger.record_event(
                "alice",
                "auth.authenticate",
                &format!("user{i}"),
                RESULT_SUCCESS,
                "",
            );
        }
        // 环形缓冲容量 4：仅保留最近 4 条
        let recent = logger.recent(10);
        assert_eq!(recent.len(), 4);
        assert_eq!(recent[0].resource, "user5");

        // 文件存在且为 JSON 行
        let files: Vec<_> = std::fs::read_dir(tmpdir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(files.len(), 1);
        let content = std::fs::read_to_string(files[0].path()).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 6);
        let first: AuditEvent = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first.resource, "user0");
    }

    #[test]
    fn test_civil_from_days_epoch() {
        // 1970-01-01 = days 0
        let (y, m, d) = civil_from_days(0);
        assert_eq!((y, m, d), (1970, 1, 1));
        // 2026-08-23
        let (y, m, d) = civil_from_days(20688);
        assert_eq!((y, m, d), (2026, 8, 23));
    }
}
