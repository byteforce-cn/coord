// coord-agent: 配置文件热加载
//
// 提供 ConfigWatcher，支持：
// - 从 TOML 文件加载 AgentConfig
// - 手动或定期重新加载配置
// - 原子替换：读者始终看到一致的配置快照
//
// 参见 docs/client-agent-architecture.md §4.6。

use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::AgentConfig;

// ──── ConfigWatcher ────

/// 配置文件监视器
///
/// 持有 `Arc<RwLock<AgentConfig>>`，提供原子读取和重新加载。
/// 读取者调用 `current_config()` 获取配置快照（克隆）。
/// 重新加载时获取写锁，确保读取者不会看到部分更新的配置。
#[derive(Clone)]
pub struct ConfigWatcher {
    config: Arc<RwLock<AgentConfig>>,
    path: PathBuf,
}

impl ConfigWatcher {
    /// 创建 ConfigWatcher 并从指定文件加载初始配置
    ///
    /// # Errors
    /// 文件不存在、无法读取或 TOML 解析失败时返回错误。
    pub fn new(path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let config = AgentConfig::from_file(path)?;
        Ok(Self {
            config: Arc::new(RwLock::new(config)),
            path: path.to_path_buf(),
        })
    }

    /// 获取当前配置的快照（克隆）
    ///
    /// 获取读锁后克隆配置，返回独立的配置副本。
    /// 在重新加载期间，此调用会阻塞直到写锁释放。
    pub fn current_config(&self) -> AgentConfig {
        self.config.read().clone()
    }

    /// 从文件重新加载配置
    ///
    /// 获取写锁，读取文件并原子替换内部配置。
    /// 在重新加载期间，`current_config()` 调用者会阻塞。
    pub fn reload(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let new_config = AgentConfig::from_file(&self.path)?;
        let mut cfg = self.config.write();
        *cfg = new_config;
        Ok(())
    }

    /// 返回配置文件的路径
    pub fn path(&self) -> &Path {
        &self.path
    }
}
