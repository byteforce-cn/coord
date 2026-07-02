// TDD: 特性开关服务测试 (Phase H — 待实施)
//
// v8.2 §4.13: 特性开关 — 基于 KV 的布尔开关，支持百分比灰度。
//
// RED stage: FeatureFlagService 尚未定义

use coord_agent::feature_flags::{FeatureFlagService, FlagConfig, FlagEvalContext, FlagState};

/// 验证 FlagConfig 默认值
#[test]
fn test_flag_config_defaults() {
    let config = FlagConfig::default();
    assert_eq!(config.default_ttl_secs, 60);
}

/// 验证基本开关操作
#[test]
fn test_feature_flag_basic_toggle() {
    let svc = FeatureFlagService::new(FlagConfig::default());

    // 设置开关
    svc.set_flag("feature-x", true).expect("设置开关失败");
    assert!(svc.is_enabled("feature-x").expect("检查失败"));

    // 关闭开关
    svc.set_flag("feature-x", false).expect("设置开关失败");
    assert!(!svc.is_enabled("feature-x").expect("检查失败"));
}

/// 验证不存在的开关默认返回 false
#[test]
fn test_feature_flag_not_found_defaults_false() {
    let svc = FeatureFlagService::new(FlagConfig::default());
    assert!(!svc.is_enabled("nonexistent-flag").expect("检查失败"));
}

/// 验证带百分比灰度的开关
#[test]
fn test_feature_flag_percentage_rollout() {
    let svc = FeatureFlagService::new(FlagConfig::default());

    // 设置 50% 灰度
    svc.set_percentage_flag("beta-feature", true, 50).expect("设置失败");

    // 验证 flag 配置
    let state = svc.get_flag_state("beta-feature").expect("获取失败");
    assert!(state.enabled);
    assert_eq!(state.percentage, Some(50));
}

/// 验证基于上下文的求值（用户 ID hash）
#[test]
fn test_feature_flag_context_evaluation() {
    let svc = FeatureFlagService::new(FlagConfig::default());
    svc.set_percentage_flag("canary", true, 50).expect("设置失败");

    // 同一用户的多次求值应该一致
    let ctx1 = FlagEvalContext { user_id: Some("user-123".to_string()), ..Default::default() };
    let result1 = svc.evaluate("canary", &ctx1).expect("求值失败");
    let result2 = svc.evaluate("canary", &ctx1).expect("求值失败");
    assert_eq!(result1, result2, "同一用户应得到一致结果");

    // 全量开关对任何用户都返回 true
    svc.set_flag("global-on", true).expect("设置失败");
    let ctx_any = FlagEvalContext::default();
    assert!(svc.evaluate("global-on", &ctx_any).expect("求值失败"));
}

/// 验证开关列表
#[test]
fn test_feature_flag_list() {
    let svc = FeatureFlagService::new(FlagConfig::default());
    svc.set_flag("flag-a", true).expect("设置失败");
    svc.set_flag("flag-b", false).expect("设置失败");
    svc.set_percentage_flag("flag-c", true, 25).expect("设置失败");

    let flags = svc.list_flags().expect("列出失败");
    assert_eq!(flags.len(), 3);
    assert!(flags.iter().any(|(k, v)| k == "flag-a" && v.enabled));
    assert!(flags.iter().any(|(k, v)| k == "flag-b" && !v.enabled));
}
