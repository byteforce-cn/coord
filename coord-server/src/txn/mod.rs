// Txn 原子事务模块
//
// 提供 Compare-And-Swap 原子事务执行能力：
// - 条件比较（Compare）：支持 Value / Version / ModRevision / CreateRevision
// - 条件分支执行：全部条件满足执行 success 分支，否则执行 failure 分支
// - 原子性保证：所有比较和操作在单个写事务中完成，作为单条 Raft 日志条目提交
//
// Txn 是协调层核心原语之一，依赖 MVCC Storage 和 Raft 共识层。

use serde::{Deserialize, Serialize};

// ──── 比较类型 ────

/// 比较目标字段
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompareTarget {
    /// 比较 Key 的 version（被修改次数）
    Version,
    /// 比较 Key 的当前 value
    Value,
    /// 比较 Key 的 mod_revision（最后修改 Revision）
    ModRevision,
    /// 比较 Key 的 create_revision（创建 Revision）
    CreateRevision,
}

/// 比较运算符
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompareOp {
    Equal,
    Greater,
    Less,
    NotEqual,
}

/// 比较目标值
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CompareValue {
    Version(i64),
    Value(Vec<u8>),
    ModRevision(i64),
    CreateRevision(i64),
}

/// 单条比较条件
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TxnCompare {
    pub key: Vec<u8>,
    pub target: CompareTarget,
    pub op: CompareOp,
    pub target_value: CompareValue,
}

// ──── 操作类型 ────

/// Txn 内的单个操作
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TxnOp {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        lease_id: Option<i64>,
    },
    Delete {
        key: Vec<u8>,
    },
    Range {
        key: Vec<u8>,
        range_end: Vec<u8>,
        limit: i64,
    },
}

/// Txn 内单个操作的响应
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TxnOpResponse {
    Put {
        /// 本次 Txn 分配的 Revision（所有操作共享同一 Revision）
        revision: u64,
    },
    Delete {
        revision: u64,
    },
    Range {
        kvs: Vec<(Vec<u8>, Vec<u8>)>,
        count: i64,
        revision: u64,
    },
}

// ──── Txn 结果 ────

/// Txn 执行结果
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TxnResult {
    /// 所有比较条件是否全部满足
    pub succeeded: bool,
    /// 本次 Txn 分配的全局 Revision
    pub revision: u64,
    /// 执行分支中每个操作的响应（顺序与请求一致）
    pub responses: Vec<TxnOpResponse>,
}

// ──── 辅助方法 ────

impl CompareValue {
    /// 比较实际值与目标值，返回 true 表示满足运算符条件
    pub fn compare(&self, actual: &CompareValue, op: &CompareOp) -> bool {
        match op {
            CompareOp::Equal => self.eq(actual),
            CompareOp::NotEqual => !self.eq(actual),
            CompareOp::Greater => self.cmp_gt(actual),
            CompareOp::Less => self.cmp_lt(actual),
        }
    }

    fn eq(&self, other: &CompareValue) -> bool {
        match (self, other) {
            (CompareValue::Version(a), CompareValue::Version(b)) => a == b,
            (CompareValue::Value(a), CompareValue::Value(b)) => a == b,
            (CompareValue::ModRevision(a), CompareValue::ModRevision(b)) => a == b,
            (CompareValue::CreateRevision(a), CompareValue::CreateRevision(b)) => a == b,
            _ => false,
        }
    }

    fn cmp_gt(&self, other: &CompareValue) -> bool {
        match (self, other) {
            (CompareValue::Version(a), CompareValue::Version(b)) => a > b,
            (CompareValue::Value(a), CompareValue::Value(b)) => a.as_slice() > b.as_slice(),
            (CompareValue::ModRevision(a), CompareValue::ModRevision(b)) => a > b,
            (CompareValue::CreateRevision(a), CompareValue::CreateRevision(b)) => a > b,
            _ => false,
        }
    }

    fn cmp_lt(&self, other: &CompareValue) -> bool {
        match (self, other) {
            (CompareValue::Version(a), CompareValue::Version(b)) => a < b,
            (CompareValue::Value(a), CompareValue::Value(b)) => a.as_slice() < b.as_slice(),
            (CompareValue::ModRevision(a), CompareValue::ModRevision(b)) => a < b,
            (CompareValue::CreateRevision(a), CompareValue::CreateRevision(b)) => a < b,
            _ => false,
        }
    }
}

// ──── 测试 ────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compare_equal() {
        assert!(CompareValue::Version(5).compare(&CompareValue::Version(5), &CompareOp::Equal));
        assert!(!CompareValue::Version(5).compare(&CompareValue::Version(6), &CompareOp::Equal));

        assert!(CompareValue::Value(b"hello".to_vec())
            .compare(&CompareValue::Value(b"hello".to_vec()), &CompareOp::Equal));
    }

    #[test]
    fn test_compare_not_equal() {
        assert!(
            CompareValue::Version(5).compare(&CompareValue::Version(6), &CompareOp::NotEqual)
        );
        assert!(
            !CompareValue::Version(5).compare(&CompareValue::Version(5), &CompareOp::NotEqual)
        );
    }

    #[test]
    fn test_compare_greater() {
        assert!(
            CompareValue::Version(10).compare(&CompareValue::Version(5), &CompareOp::Greater)
        );
        assert!(
            !CompareValue::Version(5).compare(&CompareValue::Version(10), &CompareOp::Greater)
        );
    }

    #[test]
    fn test_compare_less() {
        assert!(CompareValue::Version(3).compare(&CompareValue::Version(5), &CompareOp::Less));
        assert!(!CompareValue::Version(5).compare(&CompareValue::Version(3), &CompareOp::Less));
    }
}
