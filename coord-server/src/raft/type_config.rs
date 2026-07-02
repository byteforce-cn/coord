// Raft TypeConfig — 定义 Openraft 所需的全部关联类型
//
// 使用 declare_raft_types! 宏声明 Coord 的类型配置。

use serde::{Deserialize, Serialize};

use crate::txn::{TxnCompare, TxnOp, TxnOpResponse};

// ──── 应用层数据类型 ────

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
    Delete {
        key: Vec<u8>,
    },
    /// 原子条件事务
    Txn {
        /// 比较条件列表（AND 语义，全部满足才执行 success 分支）
        compares: Vec<TxnCompare>,
        /// 条件全部满足时执行的操作
        success_ops: Vec<TxnOp>,
        /// 任一条件不满足时执行的操作
        failure_ops: Vec<TxnOp>,
    },
}

impl std::fmt::Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::Put { key, .. } => write!(f, "Put(key={})", String::from_utf8_lossy(key)),
            Command::Delete { key } => write!(f, "Delete(key={})", String::from_utf8_lossy(key)),
            Command::Txn { compares, .. } => {
                write!(f, "Txn(compares={})", compares.len())
            }
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
    Delete {
        revision: u64,
    },
    /// Txn 操作结果
    Txn {
        /// 条件是否全部满足
        succeeded: bool,
        /// 分配的 Revision
        revision: u64,
        /// 执行分支中每个操作的响应
        responses: Vec<TxnOpResponse>,
    },
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
    use crate::txn::{TxnCompare, TxnOp, TxnOpResponse, CompareOp, CompareTarget, CompareValue};

    // ──── Command serde ────

    #[test]
    fn test_command_put_serde_roundtrip() {
        let cmd = Command::Put { key: b"hello".to_vec(), value: b"world".to_vec(), lease_id: Some(42) };
        let bytes = bincode::serialize(&cmd).unwrap();
        let decoded: Command = bincode::deserialize(&bytes).unwrap();
        match decoded {
            Command::Put { key, value, lease_id } => {
                assert_eq!(key, b"hello");
                assert_eq!(value, b"world");
                assert_eq!(lease_id, Some(42));
            }
            _ => panic!("expected Put"),
        }
    }

    #[test]
    fn test_command_delete_serde_roundtrip() {
        let cmd = Command::Delete { key: b"bye".to_vec() };
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
            success_ops: vec![TxnOp::Put { key: b"k".to_vec(), value: b"v2".to_vec(), lease_id: None }],
            failure_ops: vec![TxnOp::Delete { key: b"k".to_vec() }],
        };
        let bytes = bincode::serialize(&cmd).unwrap();
        let decoded: Command = bincode::deserialize(&bytes).unwrap();
        match decoded {
            Command::Txn { compares, success_ops, failure_ops } => {
                assert_eq!(compares.len(), 1);
                assert_eq!(success_ops.len(), 1);
                assert_eq!(failure_ops.len(), 1);
            }
            _ => panic!("expected Txn"),
        }
    }

    #[test]
    fn test_command_put_lease_none_serde() {
        let cmd = Command::Put { key: b"no-lease".to_vec(), value: b"val".to_vec(), lease_id: None };
        let bytes = bincode::serialize(&cmd).unwrap();
        let decoded: Command = bincode::deserialize(&bytes).unwrap();
        match decoded {
            Command::Put { lease_id, .. } => assert_eq!(lease_id, None),
            _ => panic!("expected Put"),
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
            Response::Txn { succeeded, revision, responses } => {
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
        let cmd = Command::Put { key: b"mykey".to_vec(), value: b"myval".to_vec(), lease_id: None };
        let s = format!("{cmd}");
        assert!(s.contains("Put"));
        assert!(s.contains("mykey"));
    }

    #[test]
    fn test_command_display_delete() {
        let cmd = Command::Delete { key: b"delkey".to_vec() };
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
        let resp = Response::Txn { succeeded: false, revision: 9, responses: vec![] };
        let s = format!("{resp}");
        assert!(s.contains("Txn"));
        assert!(s.contains("false"));
    }
}
