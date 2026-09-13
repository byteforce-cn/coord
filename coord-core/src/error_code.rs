//! 结构化错误码契约（Rust 服务端/agent ↔ Java SDK 共用）。
//!
//! 第四轮 §3.14.2：Java SDK 的 `ErrorMapper` 以 gRPC trailer `x-coord-error-code`
//! 为首选判据，但全仓 **Rust 侧 0 处**写入该 trailer（`grep -rn x-coord-error-code`
//! 只有 3 处 Java 命中）→ 首选分支是**死代码**，实际生效的只有一张 6→12 的有损
//! 状态码映射，后果是结构性错分类：
//!
//! - `NOT_FOUND` → 一律报成 `REGISTRY_SERVICE_NOT_FOUND`（KV 查不到 key、证书不存在、
//!   workflow 定义不存在、`LeaseRevoke` 不存在的租约……全部报成"注册中心服务不存在"）；
//! - `UNAVAILABLE`（含 **not-leader**）→ 一律报成 `AGENT_UNAVAILABLE`，而
//!   **SDK 的重试矩阵正是按这个码决策的**；
//! - `UNAUTHENTICATED`/`PERMISSION_DENIED`/`FAILED_PRECONDITION` → 全归 `INTERNAL`
//!   （不可重试），即"角色配错了"和"服务内部炸了"对调用方不可区分。
//!
//! 本模块是 wire 契约的**唯一定义**。`CoordErrorCode::as_str()` 的返回值必须与
//! `coord-java-sdk/src/main/java/cn/byteforce/coord/sdk/ErrorCode.java` 的 `protoName`
//! 逐字一致；两侧各有穷举测试，另有 `scripts/check-error-code-contract.sh` 做跨语言
//! 一致性校验（防止"两侧各有一份定义，慢慢漂移"——本仓库已被同类问题伤过）。
//!
//! # 为什么错误码单独枚举，而不是直接用 gRPC 状态码
//!
//! gRPC 状态码是**传输层**分类，粒度不足以做业务决策：`NOT_FOUND` 无法区分
//! "租约不存在"与"服务名没注册"；`UNAVAILABLE` 无法区分 "not leader（**应当重试到
//! leader**）" 与 "agent 挂了（应当等待/告警）"。状态码只能作为**兜底**（
//! [`default_for_code`]），不能作为主判据。

use tonic::{Code, Status};

/// gRPC trailer 键。
///
/// 与 Java `ErrorMapper.ERROR_CODE_KEY` 必须逐字一致（ASCII、小写）。
pub const ERROR_CODE_TRAILER: &str = "x-coord-error-code";

/// 结构化错误码（wire 值见 [`CoordErrorCode::as_str`]）。
///
/// **只允许追加**：改名或改字面量等于破坏已发布客户端（Java 侧 `fromProtoName`
/// 对未知名返回 `INTERNAL`，即静默降级为"不可重试的内部错误"）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CoordErrorCode {
    /// 未认证（缺/无效/过期凭据）。**不可重试**，应先取凭据。
    Unauthenticated,
    /// 已认证但无权（scope / capability 不足）。**不可重试**。
    PermissionDenied,
    /// 目标资源不存在。**不可重试**（对点查而言）。
    NotFound,
    /// 资源已存在（如注册实例重复注册）。
    AlreadyExists,
    /// 请求参数非法。**不可重试**。
    InvalidArgument,
    /// 前置条件不满足（如集群 sealed / revision 已 compact）。**不可重试**。
    FailedPrecondition,
    /// 取值越界（如 lease TTL 超范围）。
    OutOfRange,
    /// 资源耗尽（连接数、队列、批大小上限）。**可退避重试**。
    ResourceExhausted,
    /// 超时 / 期限已过。**可重试**。
    DeadlineExceeded,
    /// 集群/传输暂不可用（无 quorum、agent 不可达）。**可退避重试**。
    Unavailable,
    /// 当前节点不是 leader —— 与 `Unavailable` 分开，因为**正确的动作是重定向到
    /// leader 并重试**，而不是"等待 agent 恢复"。这是本轮修复的核心动机之一。
    NotLeader,
    /// 事务 CAS 比较失败（业务判定，非故障）。
    TxnCasFailed,
    /// 其他内部错误。**不可重试**。
    Internal,
}

impl CoordErrorCode {
    /// 全部码（穷举测试与跨语言一致性校验用）。
    pub const ALL: &'static [CoordErrorCode] = &[
        Self::Unauthenticated,
        Self::PermissionDenied,
        Self::NotFound,
        Self::AlreadyExists,
        Self::InvalidArgument,
        Self::FailedPrecondition,
        Self::OutOfRange,
        Self::ResourceExhausted,
        Self::DeadlineExceeded,
        Self::Unavailable,
        Self::NotLeader,
        Self::TxnCasFailed,
        Self::Internal,
    ];

    /// wire 值（= Java `ErrorCode.protoName`）。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unauthenticated => "UNAUTHENTICATED",
            Self::PermissionDenied => "PERMISSION_DENIED",
            Self::NotFound => "NOT_FOUND",
            Self::AlreadyExists => "ALREADY_EXISTS",
            Self::InvalidArgument => "INVALID_ARGUMENT",
            Self::FailedPrecondition => "FAILED_PRECONDITION",
            Self::OutOfRange => "OUT_OF_RANGE",
            Self::ResourceExhausted => "RESOURCE_EXHAUSTED",
            Self::DeadlineExceeded => "DEADLINE_EXCEEDED",
            Self::Unavailable => "UNAVAILABLE",
            Self::NotLeader => "NOT_LEADER",
            Self::TxnCasFailed => "TXN_CAS_FAILED",
            Self::Internal => "INTERNAL",
        }
    }
}

impl std::fmt::Display for CoordErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 由 gRPC 状态码推导**兜底**错误码。
///
/// 仅用于调用点没有给出更精确语义时（[`attach`] 会优先保留已有码）。
/// 注意 `UNAVAILABLE` 的兜底是 [`CoordErrorCode::Unavailable`] 而**不是**
/// `NotLeader`——not-leader 必须由知道该事实的调用点显式标注（`CoreError::NotLeader`
/// 分支），兜底不得猜测。
pub fn default_for_code(code: Code) -> CoordErrorCode {
    match code {
        Code::Ok => CoordErrorCode::Internal,
        Code::Cancelled => CoordErrorCode::Internal,
        Code::Unknown => CoordErrorCode::Internal,
        Code::InvalidArgument => CoordErrorCode::InvalidArgument,
        Code::DeadlineExceeded => CoordErrorCode::DeadlineExceeded,
        Code::NotFound => CoordErrorCode::NotFound,
        Code::AlreadyExists => CoordErrorCode::AlreadyExists,
        Code::PermissionDenied => CoordErrorCode::PermissionDenied,
        Code::ResourceExhausted => CoordErrorCode::ResourceExhausted,
        Code::FailedPrecondition => CoordErrorCode::FailedPrecondition,
        Code::Aborted => CoordErrorCode::TxnCasFailed,
        Code::OutOfRange => CoordErrorCode::OutOfRange,
        Code::Unimplemented => CoordErrorCode::Internal,
        Code::Internal => CoordErrorCode::Internal,
        Code::Unavailable => CoordErrorCode::Unavailable,
        Code::DataLoss => CoordErrorCode::Internal,
        Code::Unauthenticated => CoordErrorCode::Unauthenticated,
    }
}

/// 给 `Status` 附上结构化错误码，返回同一个 `Status`（便于 `return attach(..)`）。
///
/// **幂等且不覆盖**：调用点给出的精确码优先于任何后续兜底（例如
/// `attach(Status::unavailable(..), NotLeader)` 之后再被通用兜底处理，
/// 仍是 `NOT_LEADER`）。
///
/// 不 panic：trailer 值不合法（不可能，全部是 `[A-Z_]` 字面量）时静默跳过——
/// 错误码是**增强信息**，它的缺失不得把一次普通 RPC 失败升级成 panic。
pub fn attach(status: Status, code: CoordErrorCode) -> Status {
    attach_if_absent(status, code)
}

/// 与 [`attach`] 相同（名字直白地表达"已有码则不覆盖"）。
pub fn attach_if_absent(mut status: Status, code: CoordErrorCode) -> Status {
    if status.metadata().contains_key(ERROR_CODE_TRAILER) {
        return status;
    }
    if let Ok(value) = tonic::metadata::MetadataValue::try_from(code.as_str()) {
        status.metadata_mut().insert(ERROR_CODE_TRAILER, value);
    }
    status
}

/// 以状态码 + 兜底错误码构造 `Status`。
pub fn status(code: Code, message: impl Into<String>, error_code: CoordErrorCode) -> Status {
    attach(Status::new(code, message), error_code)
}

/// 从 `Status` 读回结构化错误码（测试与日志用）。
pub fn error_code_of(status: &Status) -> Option<String> {
    status
        .metadata()
        .get(ERROR_CODE_TRAILER)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// wire 值穷举：值必须是 `[A-Z_]`，唯一，且与 Java 侧 `ErrorCode.protoName`
    /// 逐字一致（一致性由 `scripts/check-error-code-contract.sh` 跨语言校验）。
    #[test]
    fn wire_names_are_unique_upper_snake() {
        let mut seen = std::collections::HashSet::new();
        for code in CoordErrorCode::ALL {
            let name = code.as_str();
            assert!(
                !name.is_empty() && name.bytes().all(|b| b.is_ascii_uppercase() || b == b'_'),
                "{name} 必须是 UPPER_SNAKE"
            );
            assert!(seen.insert(name), "wire 值重复：{name}");
        }
        assert_eq!(seen.len(), CoordErrorCode::ALL.len());
    }

    /// 本轮必须补齐的码（报告 §3.14.2 点名）：缺任何一个，Java 侧对应错误仍会被
    /// 误分类成 `INTERNAL`（不可重试）。
    #[test]
    fn report_named_codes_are_present() {
        for name in [
            "UNAUTHENTICATED",
            "PERMISSION_DENIED",
            "NOT_LEADER",
            "TXN_CAS_FAILED",
            "NOT_FOUND",
        ] {
            assert!(
                CoordErrorCode::ALL.iter().any(|c| c.as_str() == name),
                "缺少 wire 码 {name}（第四轮 §3.14.2 点名要求）"
            );
        }
    }

    #[test]
    fn attach_sets_trailer_and_is_idempotent() {
        let s = attach(Status::not_found("k"), CoordErrorCode::NotFound);
        assert_eq!(error_code_of(&s).as_deref(), Some("NOT_FOUND"));

        // 已有码不得被覆盖（精确码优先于兜底）
        let s2 = attach(s, CoordErrorCode::Internal);
        assert_eq!(error_code_of(&s2).as_deref(), Some("NOT_FOUND"));
    }

    /// 未附码的 `Status` 不得凭空多出 trailer（"有没有码"必须可区分）。
    #[test]
    fn bare_status_has_no_trailer() {
        let s = Status::internal("boom");
        assert_eq!(error_code_of(&s), None);
        assert!(!s.metadata().contains_key(ERROR_CODE_TRAILER));
    }

    /// `UNAVAILABLE` 的兜底**不得**猜成 `NOT_LEADER`：
    /// 猜错的代价是 SDK 把"agent 挂了"当"该重定向到 leader"。
    #[test]
    fn unavailable_fallback_is_not_not_leader() {
        assert_eq!(
            default_for_code(Code::Unavailable),
            CoordErrorCode::Unavailable
        );
        assert_ne!(
            default_for_code(Code::Unavailable),
            CoordErrorCode::NotLeader
        );
    }

    #[test]
    fn default_for_code_covers_every_grpc_code() {
        // 穷举 tonic::Code 的全部 17 个取值（新增取值时本测试因 match 不加 _ 而编译失败）
        for c in [
            Code::Ok,
            Code::Cancelled,
            Code::Unknown,
            Code::InvalidArgument,
            Code::DeadlineExceeded,
            Code::NotFound,
            Code::AlreadyExists,
            Code::PermissionDenied,
            Code::ResourceExhausted,
            Code::FailedPrecondition,
            Code::Aborted,
            Code::OutOfRange,
            Code::Unimplemented,
            Code::Internal,
            Code::Unavailable,
            Code::DataLoss,
            Code::Unauthenticated,
        ] {
            let ec = default_for_code(c);
            assert!(
                CoordErrorCode::ALL.contains(&ec),
                "{c:?} → {ec:?} 不在 ALL 中"
            );
        }
    }
}
