// coord-agent: 特性开关服务 (Feature Flags)
//
// 特性开关 — 布尔开关，支持百分比灰度。
//
// 核心机制：
// - 简单开关：boolean toggle
// - 百分比灰度：基于用户 ID hash 的一致性分桶
// - 上下文求值：支持用户级、租户级覆盖
//
// ⚠️ 历史缺陷（本次整改，计划书 P0-5 / E2）：本服务此前把开关放在
// `Arc<RwLock<HashMap<String, FlagState>>>`（本文件原 `:71`）——
// **重启即丢**，且与其他“头部自称 KV / 实现为内存”的服务同族
// （计划书 N8 的第三类缺陷）。现已改为 [`FeatureFlagStore`] 支撑：
// 生产 = coord-server 共享 KV（见 `feature_flags_store.rs`）。
//
// ⚠️ 读路径**不做本地缓存**：wire 面只有只读 RPC（`IsEnabled` / `Evaluate`），
// 开关更新是带外发生的 ⇒ 无 Watch 失效的本地缓存会静默陈旧。正确性优先。

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::feature_flags_store::{FeatureFlagStore, FeatureFlagStoreError};

// ──── FlagConfig ────

/// 特性开关配置
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct FlagConfig {
    /// 开关默认 TTL（秒，目前为占位，未来可用于自动过期）
    #[serde(default = "default_flag_ttl")]
    pub default_ttl_secs: u64,
}

fn default_flag_ttl() -> u64 {
    60
}

impl Default for FlagConfig {
    fn default() -> Self {
        Self {
            default_ttl_secs: 60,
        }
    }
}

// ──── FlagState ────

/// 开关状态
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
/// 状态存放在 [`FeatureFlagStore`] 中（生产 = coord-server 共享 KV），支持：
/// - boolean toggle
/// - 百分比灰度（基于用户 ID hash 一致性分桶）
/// - 上下文求值
pub struct FeatureFlagService {
    /// 服务配置（`default_ttl_secs` 仍为**占位**，见 `FlagConfig` 字段注释）
    #[allow(dead_code)]
    config: FlagConfig,
    store: Arc<dyn FeatureFlagStore>,
}

impl FeatureFlagService {
    /// 创建特性开关服务（**内存后端**；生产装配见 [`Self::with_store`]）
    pub fn new(config: FlagConfig) -> Self {
        Self::with_store(
            config,
            Arc::new(crate::feature_flags_store::MemoryFeatureFlagStore::new()),
        )
    }

    /// 使用指定存储后端创建（**生产**：`KvFeatureFlagStore`）
    pub fn with_store(config: FlagConfig, store: Arc<dyn FeatureFlagStore>) -> Self {
        Self { config, store }
    }

    /// 设置全量开关
    pub async fn set_flag(&self, key: &str, enabled: bool) -> Result<(), FlagError> {
        self.store
            .put(
                key,
                &FlagState {
                    enabled,
                    percentage: None,
                },
            )
            .await
            .map_err(FlagError::Store)
    }

    /// 设置百分比灰度开关
    ///
    /// `percentage` 范围 0-100。
    pub async fn set_percentage_flag(
        &self,
        key: &str,
        enabled: bool,
        percentage: u8,
    ) -> Result<(), FlagError> {
        if percentage > 100 {
            return Err(FlagError::InvalidPercentage(percentage));
        }
        self.store
            .put(
                key,
                &FlagState {
                    enabled,
                    percentage: Some(percentage),
                },
            )
            .await
            .map_err(FlagError::Store)
    }

    /// 检查开关是否启用（无上下文，仅全量开关有效）
    pub async fn is_enabled(&self, key: &str) -> Result<bool, FlagError> {
        match self.store.get(key).await.map_err(FlagError::Store)? {
            Some(state) => Ok(state.enabled && state.percentage.is_none()),
            None => Ok(false),
        }
    }

    /// 基于上下文求值开关
    ///
    /// 1. 若开关不存在 → false
    /// 2. 若为全量开关 → 返回 enabled
    /// 3. 若为百分比开关 → 基于 user_id hash 一致性分桶
    pub async fn evaluate(&self, key: &str, ctx: &FlagEvalContext) -> Result<bool, FlagError> {
        let state = match self.store.get(key).await.map_err(FlagError::Store)? {
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
    pub async fn get_flag_state(&self, key: &str) -> Result<FlagState, FlagError> {
        self.store
            .get(key)
            .await
            .map_err(FlagError::Store)?
            .ok_or_else(|| FlagError::NotFound(key.to_string()))
    }

    /// 列出所有开关（按 key 升序）
    pub async fn list_flags(&self) -> Result<Vec<(String, FlagState)>, FlagError> {
        self.store.list().await.map_err(FlagError::Store)
    }

    /// 删除开关
    pub async fn delete_flag(&self, key: &str) -> Result<(), FlagError> {
        self.store.delete(key).await.map_err(FlagError::Store)
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
    /// 存储层错误（KV 不可用 / 序列化失败）
    Store(FeatureFlagStoreError),
}

impl std::fmt::Display for FlagError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(key) => write!(f, "flag not found: {key}"),
            Self::InvalidPercentage(p) => write!(f, "invalid percentage: {p} (must be 0-100)"),
            Self::Store(e) => write!(f, "feature flag store error: {e}"),
        }
    }
}

impl std::error::Error for FlagError {}

// ──── BaseService：插件生命周期 ────
//
// 开关状态存放在 [`FeatureFlagStore`]（生产 = coord-server KV），
// 构造即就绪；`start`/`stop` 为登记性动作。

#[async_trait::async_trait]
impl crate::service::BaseService for FeatureFlagService {
    fn name(&self) -> &'static str {
        "feature_flags"
    }

    async fn start(&self) -> crate::service::ServiceResult<()> {
        Ok(())
    }

    async fn stop(&self) -> crate::service::ServiceResult<()> {
        Ok(())
    }

    fn health_check(&self) -> bool {
        true
    }
}

// ──── tests ────

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_toggle() {
        let svc = FeatureFlagService::new(FlagConfig::default());
        svc.set_flag("test", true).await.unwrap();
        assert!(svc.is_enabled("test").await.unwrap());
        svc.set_flag("test", false).await.unwrap();
        assert!(!svc.is_enabled("test").await.unwrap());
    }

    #[tokio::test]
    async fn test_percentage_rollout_consistency() {
        let svc = FeatureFlagService::new(FlagConfig::default());
        svc.set_percentage_flag("canary", true, 50).await.unwrap();

        let ctx = FlagEvalContext {
            user_id: Some("user-1".into()),
            ..Default::default()
        };
        let r1 = svc.evaluate("canary", &ctx).await.unwrap();
        let r2 = svc.evaluate("canary", &ctx).await.unwrap();
        assert_eq!(r1, r2);
    }

    #[tokio::test]
    async fn test_invalid_percentage_rejected() {
        let svc = FeatureFlagService::new(FlagConfig::default());
        assert!(svc.set_percentage_flag("bad", true, 101).await.is_err());
    }

    #[tokio::test]
    async fn test_absent_flag_defaults_false_and_get_errors() {
        let svc = FeatureFlagService::new(FlagConfig::default());
        assert!(!svc.is_enabled("nope").await.unwrap());
        assert!(!svc
            .evaluate("nope", &FlagEvalContext::default())
            .await
            .unwrap());
        assert!(matches!(
            svc.get_flag_state("nope").await,
            Err(FlagError::NotFound(_))
        ));
    }

    /// 存续：换一个 service 实例、同一 store，状态必须仍在
    /// （KV 后端的跨进程等价性质由 store 层保证）
    #[tokio::test]
    async fn test_state_survives_store_sharing() {
        use crate::feature_flags_store::MemoryFeatureFlagStore;

        let store: Arc<dyn FeatureFlagStore> = Arc::new(MemoryFeatureFlagStore::new());
        {
            let svc = FeatureFlagService::with_store(FlagConfig::default(), Arc::clone(&store));
            svc.set_percentage_flag("canary", true, 25).await.unwrap();
        }
        let svc2 = FeatureFlagService::with_store(FlagConfig::default(), store);
        assert!(
            !svc2.is_enabled("canary").await.unwrap(),
            "百分比开关不是全量开关，is_enabled 应为 false"
        );
        assert_eq!(
            svc2.get_flag_state("canary").await.unwrap().percentage,
            Some(25),
            "重启后开关定义必须存续"
        );
    }
}
