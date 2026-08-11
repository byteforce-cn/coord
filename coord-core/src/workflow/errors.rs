// coord-core/workflow/errors.rs
// 标准错误模型 —— RFC 7807 Problem Details + Open Workflow DSL 标准错误类型
//
// 标准 §Error Handling 要求：
// - 错误必须符合 Problem Details（type/title/status/detail/instance）
// - 使用标准错误类型（timeout/communication/validation/expression/authorization/...）
//
// type 采用 URI 形式：`urn:io.serverlessworkflow:errors:{name}`，
// 与 CNCF Serverless Workflow `errors` 定义的 errorRef 命名空间一致。

pub use super::model::WorkflowFault;

/// 错误类型命名空间前缀
pub const ERROR_NS: &str = "urn:io.serverlessworkflow:errors";

/// 标准错误类型常量
pub mod kind {
    /// 超时（timeout）—— 408
    pub const TIMEOUT: &str = "timeout";
    /// 通信错误（communication）—— 502/503
    pub const COMMUNICATION: &str = "communication";
    /// 校验错误（validation）—— 400
    pub const VALIDATION: &str = "validation";
    /// 表达式错误（expression）—— 400
    pub const EXPRESSION: &str = "expression";
    /// 认证错误（authentication）—— 401
    pub const AUTHENTICATION: &str = "authentication";
    /// 授权错误（authorization）—— 403
    pub const AUTHORIZATION: &str = "authorization";
    /// 未找到（notfound）—— 404
    pub const NOT_FOUND: &str = "notfound";
    /// 状态冲突（conflict）—— 409
    pub const CONFLICT: &str = "conflict";
    /// 内部错误（internal）—— 500
    pub const INTERNAL: &str = "internal";
    /// 运行时错误（runtime）—— 500
    pub const RUNTIME: &str = "runtime";
}

/// 构造标准错误类型 URI
pub fn error_type(kind: &str) -> String {
    format!("{ERROR_NS}/{kind}")
}

impl WorkflowFault {
    /// 超时错误（408）—— 工作流/任务/事件超时
    pub fn timeout(title: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            r#type: error_type(kind::TIMEOUT),
            title: title.into(),
            status: 408,
            detail: detail.into(),
            instance: None,
        }
    }

    /// 通信错误（502）—— 外部调用失败
    pub fn communication(title: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            r#type: error_type(kind::COMMUNICATION),
            title: title.into(),
            status: 502,
            detail: detail.into(),
            instance: None,
        }
    }

    /// 校验错误（400）—— 输入/输出 schema 校验失败
    pub fn validation(title: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            r#type: error_type(kind::VALIDATION),
            title: title.into(),
            status: 400,
            detail: detail.into(),
            instance: None,
        }
    }

    /// 表达式错误（400）—— 表达式求值失败
    pub fn expression(title: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            r#type: error_type(kind::EXPRESSION),
            title: title.into(),
            status: 400,
            detail: detail.into(),
            instance: None,
        }
    }

    /// 认证错误（401）
    pub fn authentication(title: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            r#type: error_type(kind::AUTHENTICATION),
            title: title.into(),
            status: 401,
            detail: detail.into(),
            instance: None,
        }
    }

    /// 授权错误（403）—— 密钥缺失/越权
    pub fn authorization(title: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            r#type: error_type(kind::AUTHORIZATION),
            title: title.into(),
            status: 403,
            detail: detail.into(),
            instance: None,
        }
    }

    /// 未找到（404）
    pub fn not_found(title: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            r#type: error_type(kind::NOT_FOUND),
            title: title.into(),
            status: 404,
            detail: detail.into(),
            instance: None,
        }
    }

    /// 状态冲突（409）
    pub fn conflict(title: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            r#type: error_type(kind::CONFLICT),
            title: title.into(),
            status: 409,
            detail: detail.into(),
            instance: None,
        }
    }

    /// 内部错误（500）
    pub fn internal(title: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            r#type: error_type(kind::INTERNAL),
            title: title.into(),
            status: 500,
            detail: detail.into(),
            instance: None,
        }
    }

    /// 设置错误定位（RFC 7807 `instance`，JSON Pointer）
    pub fn with_instance(mut self, instance: impl Into<String>) -> Self {
        self.instance = Some(instance.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_standard_error_types_have_standard_status() {
        let t = WorkflowFault::timeout("x", "y");
        assert_eq!(t.status, 408);
        assert_eq!(t.r#type, error_type(kind::TIMEOUT));

        let v = WorkflowFault::validation("x", "y");
        assert_eq!(v.status, 400);
        assert_eq!(v.r#type, error_type(kind::VALIDATION));

        let c = WorkflowFault::communication("x", "y");
        assert_eq!(c.status, 502);
        assert_eq!(c.r#type, error_type(kind::COMMUNICATION));

        let a = WorkflowFault::authorization("x", "y");
        assert_eq!(a.status, 403);
        assert_eq!(a.r#type, error_type(kind::AUTHORIZATION));

        let n = WorkflowFault::not_found("x", "y");
        assert_eq!(n.status, 404);
        assert_eq!(n.r#type, error_type(kind::NOT_FOUND));
    }

    #[test]
    fn test_with_instance_sets_json_pointer() {
        let f = WorkflowFault::validation("bad input", "nope")
            .with_instance("/input/amount");
        assert_eq!(f.instance.as_deref(), Some("/input/amount"));
        assert!(serde_json::to_string(&f).unwrap().contains("\"instance\""));
    }

    #[test]
    fn test_fault_serializes_rfc7807_fields() {
        let f = WorkflowFault::timeout("workflow timeout", "exceeded 5m");
        let json = serde_json::to_value(&f).unwrap();
        assert!(json.get("type").is_some());
        assert!(json.get("title").is_some());
        assert!(json.get("status").is_some());
        assert!(json.get("detail").is_some());
        assert!(json.get("instance").is_none()); // optional
    }
}
