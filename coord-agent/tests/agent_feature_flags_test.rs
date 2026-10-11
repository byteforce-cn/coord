// 特性开关服务测试
//
// 特性开关 — 布尔开关 + 百分比灰度。
//
// ⚠️ 状态落在 `FeatureFlagStore`（生产 = coord-server 共享 KV），全部方法异步；
// 头部署名不得与实现矛盾（声明"基于 KV"就必须真的走 KV，而不是进程内 HashMap）。
// 语义断言**逐条保留**，并含"存续"用例。

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

/// 验证开关状态在服务实例重建后存续（验收核心）
///
/// 状态若放在进程内 `HashMap`，换实例即全空。本用例用共享 store 表达
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

// ═══════════════════════════════════════════════════════════════════
// gRPC 管理面（PutFlag / DeleteFlag / ListFlags，#52）
// ═══════════════════════════════════════════════════════════════════

use coord_proto::agent::feature_flags_server::FeatureFlags;
use coord_proto::agent::{
    FeatureFlagDeleteFlagRequest, FeatureFlagListFlagsRequest, FeatureFlagPutFlagRequest,
};
use tonic::{Code, Request};

/// PutFlag（全量/百分比）→ 求值立即生效；ListFlags 按键升序；DeleteFlag 幂等。
///
/// 同名方法消歧：`delete_flag` / `list_flags` 与服务的固有方法同名，
/// 方法解析不按实参排除固有方法 ⇒ 必须用 UFCS 调 trait 实现。
#[tokio::test]
async fn test_grpc_flags_management_roundtrip() {
    let svc = FeatureFlagService::new(FlagConfig::default());

    // 全量开关
    svc.put_flag(Request::new(FeatureFlagPutFlagRequest {
        flag_name: "beta".into(),
        enabled: true,
        percentage: None,
    }))
    .await
    .expect("put_flag 必须成功");
    assert!(svc.is_enabled("beta").await.unwrap());

    // 百分比灰度
    svc.put_flag(Request::new(FeatureFlagPutFlagRequest {
        flag_name: "canary".into(),
        enabled: true,
        percentage: Some(25),
    }))
    .await
    .expect("put percentage flag 必须成功");
    let state = svc.get_flag_state("canary").await.unwrap();
    assert_eq!(state.percentage, Some(25));

    // 越界 percentage ⇒ INVALID_ARGUMENT（fail-loud，不静默裁剪）
    let err = svc
        .put_flag(Request::new(FeatureFlagPutFlagRequest {
            flag_name: "bad".into(),
            enabled: true,
            percentage: Some(101),
        }))
        .await
        .expect_err("percentage > 100 必须拒绝");
    assert_eq!(err.code(), Code::InvalidArgument);

    // ListFlags：按键升序、字段对齐
    let resp = FeatureFlags::list_flags(&svc, Request::new(FeatureFlagListFlagsRequest {}))
        .await
        .expect("list_flags 必须成功")
        .into_inner();
    let names: Vec<&str> = resp.flags.iter().map(|f| f.flag_name.as_str()).collect();
    assert_eq!(names, vec!["beta", "canary"], "按键升序、结果稳定可比");
    let canary = resp
        .flags
        .iter()
        .find(|f| f.flag_name == "canary")
        .expect("canary 必须在列表中");
    assert!(canary.enabled);
    assert_eq!(canary.percentage, Some(25));

    // DeleteFlag：删除后求值 false；重复删除幂等
    FeatureFlags::delete_flag(
        &svc,
        Request::new(FeatureFlagDeleteFlagRequest {
            flag_name: "beta".into(),
        }),
    )
    .await
    .expect("delete_flag 必须成功");
    assert!(!svc.is_enabled("beta").await.unwrap());
    FeatureFlags::delete_flag(
        &svc,
        Request::new(FeatureFlagDeleteFlagRequest {
            flag_name: "beta".into(),
        }),
    )
    .await
    .expect("重复删除必须幂等");
    let resp = FeatureFlags::list_flags(&svc, Request::new(FeatureFlagListFlagsRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.flags.len(), 1, "删除后列表不再包含该开关");
}

/// 跨 agent 一致：两个服务实例共享同一 store（生产 = 共享 KV），一方写、另一方读。
///
/// 判据：写入**无本地缓存延迟**——写成功即可从另一实例求值一致（无需等待）。
#[tokio::test]
async fn test_grpc_flags_management_cross_instance_immediate() {
    let store = Arc::new(MemoryFeatureFlagStore::new());
    let a: Arc<dyn FeatureFlagStore> = store.clone();
    let b: Arc<dyn FeatureFlagStore> = store;
    let svc_a = FeatureFlagService::with_store(FlagConfig::default(), a);
    let svc_b = FeatureFlagService::with_store(FlagConfig::default(), b);

    svc_a
        .put_flag(Request::new(FeatureFlagPutFlagRequest {
            flag_name: "shared".into(),
            enabled: true,
            percentage: None,
        }))
        .await
        .expect("put_flag 必须成功");
    assert!(
        svc_b.is_enabled("shared").await.unwrap(),
        "写方立即生效、读方一致（读路径无本地缓存）"
    );
}
