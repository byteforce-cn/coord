// 磁盘水位检测（资源防护）
//
// 磁盘水位三级语义：
// - Ok：剩余空间 ≥ 15%，正常服务；
// - Warn：剩余空间 < 15%，WARN 日志 + 指标告警（仍可写）；
// - ReadOnly：剩余空间 < 5%，写请求返回 `RESOURCE_EXHAUSTED`（读仍可用）。
//
// 实现：cfg(unix) 经 `libc::statvfs` 读取 f_bavail/f_blocks（跨 macOS/Linux）；
// 非 unix 平台返回 None（调用方按 Ok 处理并记录一次日志）。

use std::path::Path;

/// 磁盘空间快照
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskSpace {
    /// 可用字节（非特权用户视角，f_bavail × f_frsize）
    pub available_bytes: u64,
    /// 文件系统总字节（f_blocks × f_frsize）
    pub total_bytes: u64,
}

impl DiskSpace {
    /// 可用比例（0.0 ~ 1.0）
    pub fn available_ratio(&self) -> f64 {
        if self.total_bytes == 0 {
            return 1.0;
        }
        self.available_bytes as f64 / self.total_bytes as f64
    }
}

/// 磁盘水位判定
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskWatermark {
    Ok,
    Warn,
    ReadOnly,
}

/// 告警阈值：可用比例 < 15%
pub const WARN_RATIO: f64 = 0.15;
/// 只读阈值：可用比例 < 5%
pub const READONLY_RATIO: f64 = 0.05;

/// 依据可用比例判定水位
pub fn classify(available_ratio: f64) -> DiskWatermark {
    classify_with(available_ratio, WARN_RATIO, READONLY_RATIO)
}

/// 依据可用比例与可热更新阈值判定水位（SIGHUP 安全子集）。
///
/// 阈值来自 `coord::config::ReloadableConfig`（`disk_warn_ratio`/`disk_readonly_ratio`），
/// 校验由配置层保证：`0 < readonly < warn < 1`。
pub fn classify_with(available_ratio: f64, warn_ratio: f64, readonly_ratio: f64) -> DiskWatermark {
    if available_ratio < readonly_ratio {
        DiskWatermark::ReadOnly
    } else if available_ratio < warn_ratio {
        DiskWatermark::Warn
    } else {
        DiskWatermark::Ok
    }
}

/// 查询 `path` 所在文件系统的可用/总空间。
///
/// cfg(unix)：`libc::statvfs`（macOS/Linux）；非 unix 返回 `None`
/// （调用方按 `Ok` 处理，磁盘防护在非 unix 平台为 no-op，已文档化）。
pub fn check_disk_space(path: &Path) -> Option<DiskSpace> {
    #[cfg(unix)]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let c_path = CString::new(path.as_os_str().as_bytes()).ok()?;
        let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
        // SAFETY: c_path 为合法 C 字符串，stat 为合法可变指针
        let ret = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };
        if ret != 0 {
            return None;
        }
        let frsize = stat.f_frsize as u64;
        Some(DiskSpace {
            available_bytes: (stat.f_bavail as u64).saturating_mul(frsize),
            total_bytes: (stat.f_blocks as u64).saturating_mul(frsize),
        })
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        None
    }
}

// ──── 测试 ────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_thresholds() {
        assert_eq!(classify(0.50), DiskWatermark::Ok);
        assert_eq!(classify(0.15), DiskWatermark::Ok, "boundary: >= 15% is Ok");
        assert_eq!(classify(0.149), DiskWatermark::Warn);
        assert_eq!(
            classify(0.05),
            DiskWatermark::Warn,
            "boundary: >= 5% is Warn"
        );
        assert_eq!(classify(0.049), DiskWatermark::ReadOnly);
    }

    #[test]
    fn test_classify_with_custom_thresholds() {
        // 动态阈值（SIGHUP 热更新安全子集）
        assert_eq!(classify_with(0.20, 0.25, 0.10), DiskWatermark::Warn);
        assert_eq!(classify_with(0.30, 0.25, 0.10), DiskWatermark::Ok);
        assert_eq!(classify_with(0.09, 0.25, 0.10), DiskWatermark::ReadOnly);
    }

    #[test]
    fn test_check_disk_space_on_data_dir() {
        // 任何存在的路径都应能查询（unix）；返回值合理
        let space = check_disk_space(std::path::Path::new("."));
        #[cfg(unix)]
        {
            let s = space.expect("statvfs should succeed on unix");
            assert!(s.total_bytes > 0);
            assert!(s.available_bytes <= s.total_bytes);
            assert!((0.0..=1.0).contains(&s.available_ratio()));
        }
        #[cfg(not(unix))]
        {
            assert!(space.is_none());
        }
    }
}
