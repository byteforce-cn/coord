// TDD: CloudEvents 1.0 合规测试 (Phase E — 待实施)
//
// v8.2 §5: 所有跨 Agent 边界事件强制遵循 CloudEvents 1.0。
// - specversion: "1.0"
// - type, source, id 必填
// - data, datacontenttype, subject, time 可选
//
// RED stage: CloudEvent 类型尚未定义在 event_notification 中。

use coord_agent::services::event_notification::CloudEvent;

/// 验证 CloudEvent 最小必填字段
#[test]
fn test_cloudevent_required_fields() {
    let event = CloudEvent::new(
        "cn.byteforce.order.created",
        "/coord-agent/order-service",
    );
    assert_eq!(event.specversion, "1.0");
    assert_eq!(event.event_type, "cn.byteforce.order.created");
    assert_eq!(event.source, "/coord-agent/order-service");
    assert!(!event.id.is_empty(), "id 应自动生成");
    assert!(event.time.is_some(), "time 应自动填充");
}

/// 验证 CloudEvent 序列化/反序列化（JSON）
#[test]
fn test_cloudevent_json_roundtrip() {
    let mut event = CloudEvent::new(
        "cn.byteforce.cache.updated",
        "/coord-agent/cache-service",
    );
    event.data = Some(b"hello cloud".to_vec());
    event.datacontenttype = Some("application/octet-stream".to_string());
    event.subject = Some("cache-key-001".to_string());

    let json = serde_json::to_string(&event).expect("序列化失败");
    let parsed: CloudEvent = serde_json::from_str(&json).expect("反序列化失败");

    assert_eq!(parsed.specversion, "1.0");
    assert_eq!(parsed.event_type, "cn.byteforce.cache.updated");
    assert_eq!(parsed.source, "/coord-agent/cache-service");
    assert_eq!(parsed.subject, Some("cache-key-001".to_string()));
    assert_eq!(parsed.datacontenttype, Some("application/octet-stream".to_string()));
}

/// 验证每个事件 ID 唯一
#[test]
fn test_cloudevent_unique_ids() {
    let e1 = CloudEvent::new("test.type", "/test");
    let e2 = CloudEvent::new("test.type", "/test");
    assert_ne!(e1.id, e2.id, "每个事件应有唯一 ID");
}

/// 验证 CloudEvent 符合规范格式（可以序列化为标准 JSON）
#[test]
fn test_cloudevent_spec_compliance() {
    let event = CloudEvent::new("com.example.test", "/example");

    // CloudEvents 1.0 要求 specversion 必须是 "1.0"
    assert_eq!(event.specversion, "1.0");

    // type 必须是反向 DNS 名称
    assert!(event.event_type.contains('.'), "type 应为反向 DNS 格式");

    // source 必须是 URI-reference
    assert!(event.source.starts_with('/'), "source 应为 URI-reference");

    // id 不能为空
    assert!(!event.id.is_empty());

    // time 若存在应为 RFC 3339
    if let Some(ref t) = event.time {
        assert!(t.contains('T') || t.contains('Z'), "time 应为 RFC 3339 格式");
    }
}
