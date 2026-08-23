// Raft TypeConfig — 定义 Openraft 所需的全部关联类型
//
// 使用 declare_raft_types! 宏声明 Coord 的类型配置。

use serde::{Deserialize, Serialize};

use crate::txn::{TxnCompare, TxnOp, TxnOpResponse};

// ──── 应用层数据类型 ────

/// Lease 生命周期操作（P0-B：入 raft 日志，apply 持久化到 `/_lease/{id}`）
///
/// deadline_wall_ms 由 leader 在 propose 前计算（墙钟毫秒），保证 apply 确定性
/// （规格 A.4 约束 1：apply 返回值仅依赖日志内容）。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LeaseOp {
    /// 首次绑定/重置：id 由 LeaseManager 分配后随命令入日志
    Grant {
        id: i64,
        ttl: i64,
        deadline_wall_ms: i64,
    },
    /// 续约：推进 keepalive_revision 并更新 deadline
    KeepAlive { id: i64, deadline_wall_ms: i64 },
    /// 主动吊销/过期：按 `KvMetadata.lease_id` 索引删除绑定 Key
    Revoke { id: i64, delete_keys: bool },
}

/// 鉴权元数据操作（P0-C.2：入 raft 日志，apply 持久化到 `/_sys/auth/` 前缀）
///
/// 用户/角色/吊销登记全部经 raft 达成集群一致；`AuthManager` 内存表为 apply
/// 派生的缓存视图。密码哈希为 Argon2id PHC 字符串（P0-C.6）。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AuthOp {
    /// 创建用户（含 Argon2id 哈希与初始角色）
    UserAdd {
        name: String,
        hash: String,
        roles: Vec<String>,
    },
    /// 删除用户
    UserDelete { name: String },
    /// 修改用户密码
    UserSetPassword { name: String, hash: String },
    /// 用户授予角色
    UserGrantRole { name: String, role: String },
    /// 用户撤销角色
    UserRevokeRole { name: String, role: String },
    /// 创建角色
    RoleAdd { role: String },
    /// 删除角色
    RoleDelete { role: String },
    /// 角色授予 Key 前缀权限
    RoleGrantPermission {
        role: String,
        perm_type: u8,
        key: Vec<u8>,
        range_end: Vec<u8>,
    },
    /// 角色撤销 Key 前缀权限
    RoleRevokePermission {
        role: String,
        key: Vec<u8>,
        range_end: Vec<u8>,
    },
    /// 吊销登记（P0-C.5：写入 `/_sys/auth/revoked/{jti}`）
    RevokeJti { jti: String },
    /// P2-07：会话落盘（写入 `/_sys/auth/sessions/{hash_hex}`，重启不失效）
    IssueSession {
        /// token 的 SHA256 hex（存储键，不落明文 token）
        hash_hex: String,
        username: String,
        expires_at_unix: u64,
        is_refresh: bool,
    },
    /// P2-07：会话消费/删除（refresh 单次使用、登出、吊销同路径）
    ConsumeSession { hash_hex: String },
}

/// Raft 日志负载：客户端提交的状态机命令
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Command {
    /// 写入 Key-Value
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        /// 关联的 Lease ID（可选）
        lease_id: Option<i64>,
    },
    /// 删除 Key
    Delete { key: Vec<u8> },
    /// 原子条件事务
    Txn {
        /// 比较条件列表（AND 语义，全部满足才执行 success 分支）
        compares: Vec<TxnCompare>,
        /// 条件全部满足时执行的操作
        success_ops: Vec<TxnOp>,
        /// 任一条件不满足时执行的操作
        failure_ops: Vec<TxnOp>,
    },
    /// Lease 生命周期操作（P0-B）
    Lease(LeaseOp),
    /// 鉴权元数据操作（P0-C）
    Auth(AuthOp),
    /// 压缩历史（P1-01）：raft 下发 compact revision，节点一致删除
    /// revision 之前的 changelog/tombstone；apply 幂等（单调，低 revision 为 no-op）
    Compact { revision: u64 },
}

impl std::fmt::Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::Put { key, .. } => write!(f, "Put(key={})", String::from_utf8_lossy(key)),
            Command::Delete { key } => write!(f, "Delete(key={})", String::from_utf8_lossy(key)),
            Command::Txn { compares, .. } => {
                write!(f, "Txn(compares={})", compares.len())
            }
            Command::Lease(op) => write!(f, "Lease({op:?})"),
            Command::Auth(op) => write!(f, "Auth({op:?})"),
            Command::Compact { revision } => write!(f, "Compact(revision={revision})"),
        }
    }
}

/// 状态机对 Command 的响应
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Response {
    /// Put 操作结果
    Put {
        /// 分配的 Revision
        revision: u64,
    },
    /// Delete 操作结果
    Delete { revision: u64 },
    /// Txn 操作结果
    Txn {
        /// 条件是否全部满足
        succeeded: bool,
        /// 分配的 Revision
        revision: u64,
        /// 执行分支中每个操作的响应
        responses: Vec<TxnOpResponse>,
    },
    /// Lease 操作结果（P0-B）
    Lease { revision: u64 },
    /// Auth 操作结果（P0-C）
    Auth { revision: u64 },
    /// Compact 操作结果（P1-01）：实际生效的 compacted revision
    Compact { compacted_revision: u64 },
}

impl std::fmt::Display for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Response::Put { revision } => write!(f, "Put(rev={})", revision),
            Response::Delete { revision } => write!(f, "Delete(rev={})", revision),
            Response::Txn {
                succeeded,
                revision,
                ..
            } => write!(f, "Txn(succeeded={}, rev={})", succeeded, revision),
            Response::Lease { revision } => write!(f, "Lease(rev={})", revision),
            Response::Auth { revision } => write!(f, "Auth(rev={})", revision),
            Response::Compact { compacted_revision } => {
                write!(f, "Compact(rev={})", compacted_revision)
            }
        }
    }
}

// ──── TypeConfig 声明 ────

openraft::declare_raft_types!(
    /// Coord 的 Raft 类型配置
    pub TypeConfig:
        D = Command,
        R = Response,
        NodeId = u64,
        Node = openraft::impls::BasicNode,
);

// ──── 测试 ────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::txn::{CompareOp, CompareTarget, CompareValue, TxnCompare, TxnOp, TxnOpResponse};

    // ──── Command serde ────

    #[test]
    fn test_command_put_serde_roundtrip() {
        let cmd = Command::Put {
            key: b"hello".to_vec(),
            value: b"world".to_vec(),
            lease_id: Some(42),
        };
        let bytes = bincode::serialize(&cmd).unwrap();
        let decoded: Command = bincode::deserialize(&bytes).unwrap();
        match decoded {
            Command::Put {
                key,
                value,
                lease_id,
            } => {
                assert_eq!(key, b"hello");
                assert_eq!(value, b"world");
                assert_eq!(lease_id, Some(42));
            }
            _ => panic!("expected Put"),
        }
    }

    #[test]
    fn test_command_delete_serde_roundtrip() {
        let cmd = Command::Delete {
            key: b"bye".to_vec(),
        };
        let bytes = bincode::serialize(&cmd).unwrap();
        let decoded: Command = bincode::deserialize(&bytes).unwrap();
        match decoded {
            Command::Delete { key } => assert_eq!(key, b"bye"),
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn test_command_txn_serde_roundtrip() {
        let cmd = Command::Txn {
            compares: vec![TxnCompare {
                key: b"k".to_vec(),
                op: CompareOp::Equal,
                target: CompareTarget::Value,
                target_value: CompareValue::Value(b"v".to_vec()),
            }],
            success_ops: vec![TxnOp::Put {
                key: b"k".to_vec(),
                value: b"v2".to_vec(),
                lease_id: None,
            }],
            failure_ops: vec![TxnOp::Delete { key: b"k".to_vec() }],
        };
        let bytes = bincode::serialize(&cmd).unwrap();
        let decoded: Command = bincode::deserialize(&bytes).unwrap();
        match decoded {
            Command::Txn {
                compares,
                success_ops,
                failure_ops,
            } => {
                assert_eq!(compares.len(), 1);
                assert_eq!(success_ops.len(), 1);
                assert_eq!(failure_ops.len(), 1);
            }
            _ => panic!("expected Txn"),
        }
    }

    #[test]
    fn test_command_put_lease_none_serde() {
        let cmd = Command::Put {
            key: b"no-lease".to_vec(),
            value: b"val".to_vec(),
            lease_id: None,
        };
        let bytes = bincode::serialize(&cmd).unwrap();
        let decoded: Command = bincode::deserialize(&bytes).unwrap();
        match decoded {
            Command::Put { lease_id, .. } => assert_eq!(lease_id, None),
            _ => panic!("expected Put"),
        }
    }

    // ──── P1-01 Compact serde ────

    #[test]
    fn test_command_compact_serde_roundtrip() {
        let cmd = Command::Compact { revision: 123456 };
        let bytes = bincode::serialize(&cmd).unwrap();
        let decoded: Command = bincode::deserialize(&bytes).unwrap();
        match decoded {
            Command::Compact { revision } => assert_eq!(revision, 123456),
            _ => panic!("expected Compact"),
        }
    }

    #[test]
    fn test_response_compact_serde_roundtrip() {
        let resp = Response::Compact {
            compacted_revision: 777,
        };
        let bytes = bincode::serialize(&resp).unwrap();
        let decoded: Response = bincode::deserialize(&bytes).unwrap();
        match decoded {
            Response::Compact { compacted_revision } => assert_eq!(compacted_revision, 777),
            _ => panic!("expected Compact response"),
        }
    }

    // ──── Response serde ────

    #[test]
    fn test_response_put_serde_roundtrip() {
        let resp = Response::Put { revision: 12345 };
        let bytes = bincode::serialize(&resp).unwrap();
        let decoded: Response = bincode::deserialize(&bytes).unwrap();
        match decoded {
            Response::Put { revision } => assert_eq!(revision, 12345),
            _ => panic!("expected Put"),
        }
    }

    #[test]
    fn test_response_delete_serde_roundtrip() {
        let resp = Response::Delete { revision: 67890 };
        let bytes = bincode::serialize(&resp).unwrap();
        let decoded: Response = bincode::deserialize(&bytes).unwrap();
        match decoded {
            Response::Delete { revision } => assert_eq!(revision, 67890),
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn test_response_txn_serde_roundtrip() {
        let resp = Response::Txn {
            succeeded: true,
            revision: 100,
            responses: vec![
                TxnOpResponse::Put { revision: 100 },
                TxnOpResponse::Delete { revision: 101 },
            ],
        };
        let bytes = bincode::serialize(&resp).unwrap();
        let decoded: Response = bincode::deserialize(&bytes).unwrap();
        match decoded {
            Response::Txn {
                succeeded,
                revision,
                responses,
            } => {
                assert!(succeeded);
                assert_eq!(revision, 100);
                assert_eq!(responses.len(), 2);
            }
            _ => panic!("expected Txn"),
        }
    }

    // ──── Display ────

    #[test]
    fn test_command_display_put() {
        let cmd = Command::Put {
            key: b"mykey".to_vec(),
            value: b"myval".to_vec(),
            lease_id: None,
        };
        let s = format!("{cmd}");
        assert!(s.contains("Put"));
        assert!(s.contains("mykey"));
    }

    #[test]
    fn test_command_display_delete() {
        let cmd = Command::Delete {
            key: b"delkey".to_vec(),
        };
        let s = format!("{cmd}");
        assert!(s.contains("Delete"));
        assert!(s.contains("delkey"));
    }

    #[test]
    fn test_command_display_txn() {
        let cmd = Command::Txn {
            compares: vec![TxnCompare {
                key: b"x".to_vec(),
                op: CompareOp::Equal,
                target: CompareTarget::Value,
                target_value: CompareValue::Value(b"y".to_vec()),
            }],
            success_ops: vec![],
            failure_ops: vec![],
        };
        let s = format!("{cmd}");
        assert!(s.contains("Txn"));
        assert!(s.contains("compares=1"));
    }

    #[test]
    fn test_response_display_put() {
        let resp = Response::Put { revision: 5 };
        let s = format!("{resp}");
        assert!(s.contains("Put"));
        assert!(s.contains("5"));
    }

    #[test]
    fn test_response_display_delete() {
        let resp = Response::Delete { revision: 7 };
        let s = format!("{resp}");
        assert!(s.contains("Delete"));
        assert!(s.contains("7"));
    }

    #[test]
    fn test_response_display_txn() {
        let resp = Response::Txn {
            succeeded: false,
            revision: 9,
            responses: vec![],
        };
        let s = format!("{resp}");
        assert!(s.contains("Txn"));
        assert!(s.contains("false"));
    }
}
