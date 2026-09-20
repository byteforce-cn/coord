// 特性开关服务测试
//
// 特性开关 — 布尔开关 + 百分比灰度。
//
// ⚠️ 原为 RED 阶段草稿（注释称"FeatureFlagService 尚未定义"，且头部署名
// "基于 KV" 与实现矛盾 —— 实现是进程内 HashMap）。计划书 P0-5 / E2 整改后
// 状态真正落在 `FeatureFlagStore`（生产 = coord-server 共享 KV），全部方法异步。
// 语义断言**逐条保留**，并新增"存续"用例。

use std::sync::Arc;

use coord_agent::feature_flags::{FeatureFlagService, FlagConfig, FlagEvalContext};
use coord_agent::feature_flags_store::{FeatureFlagStore, MemoryFeatureFlagStore};

/// 验证 FlagConfig 默认值
#[test]
fn test_flag_config_defaults() {
    let config = FlagConfig::default();
    assert_eq!(config.default_ttl_secs, 60);
}

/// 验证基本开关操作
#[tokio::test]
async fn test_feature_flag_basic_toggle() {
    let svc = FeatureFlagService::new(FlagConfig::default());

    // 设置开关
    svc.set_flag("feature-x", true).await.expect("设置开关失败");
    assert!(svc.is_enabled("feature-x").await.expect("检查失败"));

    // 关闭开关
    svc.set_flag("feature-x", false)
        .await
        .expect("设置开关失败");
    assert!(!svc.is_enabled("feature-x").await.expect("检查失败"));
}

/// 验证不存在的开关默认返回 false
#[tokio::test]
async fn test_feature_flag_not_found_defaults_false() {
    let svc = FeatureFlagService::new(FlagConfig::default());
    assert!(!svc.is_enabled("nonexistent-flag").await.expect("检查失败"));
}

/// 验证带百分比灰度的开关
#[tokio::test]
async fn test_feature_flag_percentage_rollout() {
    let svc = FeatureFlagService::new(FlagConfig::default());

    // 设置 50% 灰度
    svc.set_percentage_flag("beta-feature", true, 50)
        .await
        .expect("设置失败");

    // 验证 flag 配置
    let state = svc.get_flag_state("beta-feature").await.expect("获取失败");
    assert!(state.enabled);
    assert_eq!(state.percentage, Some(50));
}

/// 验证基于上下文的求值（用户 ID hash）
#[tokio::test]
async fn test_feature_flag_context_evaluation() {
    let svc = FeatureFlagService::new(FlagConfig::default());
    svc.set_percentage_flag("canary", true, 50)
        .await
        .expect("设置失败");

    // 同一用户的多次求值应该一致
    let ctx1 = FlagEvalContext {
        user_id: Some("user-123".to_string()),
        ..Default::default()
    };
    let result1 = svc.evaluate("canary", &ctx1).await.expect("求值失败");
    let result2 = svc.evaluate("canary", &ctx1).await.expect("求值失败");
    assert_eq!(result1, result2, "同一用户应得到一致结果");

    // 全量开关对任何用户都返回 true
    svc.set_flag("global-on", true).await.expect("设置失败");
    let ctx_any = FlagEvalContext::default();
    assert!(svc.evaluate("global-on", &ctx_any).await.expect("求值失败"));
}

/// 验证开关列表
#[tokio::test]
async fn test_feature_flag_list() {
    let svc = FeatureFlagService::new(FlagConfig::default());
    svc.set_flag("flag-a", true).await.expect("设置失败");
    svc.set_flag("flag-b", false).await.expect("设置失败");
    svc.set_percentage_flag("flag-c", true, 25)
        .await
        .expect("设置失败");

    let flags = svc.list_flags().await.expect("列出失败");
    assert_eq!(flags.len(), 3);
    assert!(flags.iter().any(|(k, v)| k == "flag-a" && v.enabled));
    assert!(flags.iter().any(|(k, v)| k == "flag-b" && !v.enabled));
}

/// 验证删除开关
#[tokio::test]
async fn test_feature_flag_delete() {
    let svc = FeatureFlagService::new(FlagConfig::default());
    svc.set_flag("temp", true).await.expect("设置失败");
    svc.delete_flag("temp").await.expect("删除失败");
    assert!(!svc.is_enabled("temp").await.expect("检查失败"));
    // 幂等：重复删除不报错
    svc.delete_flag("temp").await.expect("重复删除应幂等");
}

/// 验证开关状态在服务实例重建后存续（P0-5 / E2 的验收核心）
///
/// 旧实现状态在进程内 `HashMap` ⇒ 换实例即全空。本用例用共享 store 表达
/// "重启存续"；生产后端为 coord-server KV（跨进程）。
#[tokio::test]
async fn test_feature_flag_state_survives_instance_recreation() {
    let store: Arc<dyn FeatureFlagStore> = Arc::new(MemoryFeatureFlagStore::new());

    {
        let svc = FeatureFlagService::with_store(FlagConfig::default(), Arc::clone(&store));
        svc.set_flag("persisted-on", true).await.expect("设置失败");
        svc.set_percentage_flag("persisted-pct", true, 30)
            .await
            .expect("设置失败");
    }

    // "重启"：新实例、同一 store
    let svc2 = FeatureFlagService::with_store(FlagConfig::default(), store);
    assert!(
        svc2.is_enabled("persisted-on").await.expect("检查失败"),
        "重启后全量开关必须存续"
    );
    assert_eq!(
        svc2.list_flags().await.expect("列出失败").len(),
        2,
        "重启后开关数量必须存续"
    );
    assert_eq!(
        svc2.get_flag_state("persisted-pct")
            .await
            .expect("获取失败")
            .percentage,
        Some(30),
        "重启后灰度百分比必须存续"
    );
}
