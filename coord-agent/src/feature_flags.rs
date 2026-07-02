// coord-agent: 特性开关服务 (Feature Flags) — Phase H
//
// v8.2 §4.13: 特性开关 — 基于 KV 的布尔开关，支持百分比灰度。
//
// 核心机制：
// - 简单开关：boolean toggle
// - 百分比灰度：基于用户 ID hash 的一致性分桶
// - 上下文求值：支持用户级、租户级覆盖

use std::collections::HashMap;
use std::sync::Arc;
use std::hash::{Hash, Hasher};

use parking_lot::RwLock;

// ──── FlagConfig ────

/// 特性开关配置
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct FlagConfig {
    /// 开关默认 TTL（秒，目前为占位，未来可用于自动过期）
    #[serde(default = "default_flag_ttl")]
    pub default_ttl_secs: u64,
}

fn default_flag_ttl() -> u64 { 60 }

impl Default for FlagConfig {
    fn default() -> Self {
        Self { default_ttl_secs: 60 }
    }
}

// ──── FlagState ────

/// 开关状态
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlagState {
    /// 是否启用
    pub enabled: bool,
    /// 百分比（0-100），None 表示全量开关
    pub percentage: Option<u8>,
}

// ──── FlagEvalContext ────

/// 开关求值上下文
#[derive(Debug, Clone, Default)]
pub struct FlagEvalContext {
    /// 用户 ID（用于百分比分桶）
    pub user_id: Option<String>,
    /// 租户 ID（未来用于租户级覆盖）
    pub tenant_id: Option<String>,
}

// ──── FeatureFlagService ────

/// 特性开关服务
///
/// 线程安全的内存存储，支持：
/// - boolean toggle
/// - 百分比灰度（基于用户 ID hash 一致性分桶）
/// - 上下文求值
pub struct FeatureFlagService {
    #[allow(dead_code)]
    config: FlagConfig,
    flags: Arc<RwLock<HashMap<String, FlagState>>>,
}

impl FeatureFlagService {
    /// 创建特性开关服务
    pub fn new(config: FlagConfig) -> Self {
        Self {
            config,
            flags: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 设置全量开关
    pub fn set_flag(&self, key: &str, enabled: bool) -> Result<(), FlagError> {
        let mut flags = self.flags.write();
        flags.insert(
            key.to_string(),
            FlagState { enabled, percentage: None },
        );
        Ok(())
    }

    /// 设置百分比灰度开关
    ///
    /// `percentage` 范围 0-100。
    pub fn set_percentage_flag(&self, key: &str, enabled: bool, percentage: u8) -> Result<(), FlagError> {
        if percentage > 100 {
            return Err(FlagError::InvalidPercentage(percentage));
        }
        let mut flags = self.flags.write();
        flags.insert(
            key.to_string(),
            FlagState {
                enabled,
                percentage: Some(percentage),
            },
        );
        Ok(())
    }

    /// 检查开关是否启用（无上下文，仅全量开关有效）
    pub fn is_enabled(&self, key: &str) -> Result<bool, FlagError> {
        let flags = self.flags.read();
        match flags.get(key) {
            Some(state) => Ok(state.enabled && state.percentage.is_none()),
            None => Ok(false),
        }
    }

    /// 基于上下文求值开关
    ///
    /// 1. 若开关不存在 → false
    /// 2. 若为全量开关 → 返回 enabled
    /// 3. 若为百分比开关 → 基于 user_id hash 一致性分桶
    pub fn evaluate(&self, key: &str, ctx: &FlagEvalContext) -> Result<bool, FlagError> {
        let flags = self.flags.read();
        let state = match flags.get(key) {
            Some(s) => s,
            None => return Ok(false),
        };

        if !state.enabled {
            return Ok(false);
        }

        match state.percentage {
            None => Ok(true), // 全量开关
            Some(pct) => {
                let user_id = ctx.user_id.as_deref().unwrap_or("");
                let bucket = hash_user_to_bucket(user_id, key);
                Ok(bucket < pct)
            }
        }
    }

    /// 获取开关状态
    pub fn get_flag_state(&self, key: &str) -> Result<FlagState, FlagError> {
        let flags = self.flags.read();
        flags
            .get(key)
            .cloned()
            .ok_or(FlagError::NotFound(key.to_string()))
    }

    /// 列出所有开关
    pub fn list_flags(&self) -> Result<Vec<(String, FlagState)>, FlagError> {
        let flags = self.flags.read();
        Ok(flags.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
    }

    /// 删除开关
    pub fn delete_flag(&self, key: &str) -> Result<(), FlagError> {
        let mut flags = self.flags.write();
        flags.remove(key);
        Ok(())
    }
}

/// 基于用户 ID + flag key 的一致性哈希分桶（0-99）
fn hash_user_to_bucket(user_id: &str, flag_key: &str) -> u8 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    user_id.hash(&mut hasher);
    flag_key.hash(&mut hasher);
    let hash = hasher.finish();
    (hash % 100) as u8
}

// ──── FlagError ────

/// 特性开关错误
#[derive(Debug)]
pub enum FlagError {
    NotFound(String),
    InvalidPercentage(u8),
}

impl std::fmt::Display for FlagError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(key) => write!(f, "flag not found: {key}"),
            Self::InvalidPercentage(p) => write!(f, "invalid percentage: {p} (must be 0-100)"),
        }
    }
}

impl std::error::Error for FlagError {}

// ──── tests ────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_toggle() {
        let svc = FeatureFlagService::new(FlagConfig::default());
        svc.set_flag("test", true).unwrap();
        assert!(svc.is_enabled("test").unwrap());
        svc.set_flag("test", false).unwrap();
        assert!(!svc.is_enabled("test").unwrap());
    }

    #[test]
    fn test_percentage_rollout_consistency() {
        let svc = FeatureFlagService::new(FlagConfig::default());
        svc.set_percentage_flag("canary", true, 50).unwrap();

        let ctx = FlagEvalContext { user_id: Some("user-1".into()), ..Default::default() };
        let r1 = svc.evaluate("canary", &ctx).unwrap();
        let r2 = svc.evaluate("canary", &ctx).unwrap();
        assert_eq!(r1, r2);
    }

    #[test]
    fn test_invalid_percentage_rejected() {
        let svc = FeatureFlagService::new(FlagConfig::default());
        assert!(svc.set_percentage_flag("bad", true, 101).is_err());
    }
}
