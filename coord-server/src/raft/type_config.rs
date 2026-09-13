// Raft TypeConfig — 定义 Openraft 所需的全部关联类型
//
// 使用 declare_raft_types! 宏声明 Coord 的类型配置。

use serde::{Deserialize, Serialize};

use coord_core::error::{Error, Result};
use coord_core::types::NodeID;

use crate::pd::operator::{Operator, OperatorStatus};
use crate::txn::{TxnCompare, TxnOp, TxnOpResponse};

// ──── 应用层数据类型 ────

/// Lease 生命周期操作（入 raft 日志，apply 持久化到 `/_lease/{id}`）
///
/// deadline_wall_ms 由 leader 在 propose 前计算（墙钟毫秒），保证 apply 确定性
/// （约束 1：apply 返回值仅依赖日志内容）。
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

/// 鉴权元数据操作（入 raft 日志，apply 持久化到 `/_sys/auth/` 前缀）
///
/// 用户/角色/吊销登记全部经 raft 达成集群一致；`AuthManager` 内存表为 apply
/// 派生的缓存视图。密码哈希为 Argon2id PHC 字符串。
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
    /// 吊销登记（写入 `/_sys/auth/revoked/{jti}`）
    RevokeJti { jti: String },
    /// 会话落盘（写入 `/_sys/auth/sessions/{hash_hex}`，重启不失效）
    IssueSession {
        /// token 的 SHA256 hex（存储键，不落明文 token）
        hash_hex: String,
        username: String,
        expires_at_unix: u64,
        is_refresh: bool,
    },
    /// 会话消费/删除（refresh 单次使用、登出、吊销同路径）
    ConsumeSession { hash_hex: String },
    /// 角色授予能力（capability_id + scope；持久化到角色记录 `capability_grants`）
    ///
    /// 只允许末尾追加（bincode 变体索引 = 旧日志/快照升级兼容）。
    RoleGrantCapability {
        role: String,
        capability_id: String,
        scope: String,
    },
    /// 角色撤销能力（精确匹配 capability_id + scope）
    RoleRevokeCapability {
        role: String,
        capability_id: String,
        scope: String,
    },
    /// 动态签发 agent bootstrap 令牌（TTL + 一次性）
    ///
    /// 明文令牌**不入日志**：只落 SHA256 hex（`hash_hex`），
    /// 与 `IssueSession` 同口径。持久化到 `/_sys/auth/bootstrap/{id}`。
    IssueBootstrapToken {
        /// 令牌 ID（revoke / 列表用；非密文）
        id: String,
        /// 令牌的 SHA256 hex（存储键，不落明文）
        hash_hex: String,
        label: String,
        created_by: String,
        created_at_unix: u64,
        expires_at_unix: u64,
    },
    /// 消费 bootstrap 令牌（一次性；标记 consumed_at，保留记录供审计）
    ConsumeBootstrapToken { id: String, consumed_at_unix: u64 },
    /// 撤销（删除）bootstrap 令牌
    RevokeBootstrapToken { id: String },
    /// **批量**删除已过期会话（定期清理任务；键前缀 `/_sys/auth/sessions/`）
    ///
    /// 与逐条 `ConsumeSession` 语义等价（删内存视图 + 删持久化行），但一次扫描
    /// 只产生**一条** raft 条目。会话表随正常 refresh 流量持续增长（第四轮 §3.6 b：
    /// 10k 客户端 ≈ 96 万条/天，内存 150–200 MB/天且每条约一行 KV），而"过期只报错
    /// 不回收"——逐条提案会把清理本身变成 raft 日志洪泛，故用批量变体。
    ///
    /// 变体索引 17（**末尾追加**，旧日志/快照的既有索引不漂移）。
    ConsumeSessions { hash_hexes: Vec<String> },
}

// ──── PD 全局调度命令（/ 见 docs）────

/// PD 全局 operator 队列治理命令（经 **region 0 system raft** 承载）。
///
/// 设计要点：
/// - 本枚举及 `Command::Pd` 变体**只允许末尾追加**（bincode 变体索引 = 旧日志/快照
///   升级兼容）；
/// - 仅 region 0 raft 提出并 apply；data region raft 收到（不应发生）视为其各自
///   MVCC 上的无害记录（键前缀 `/_pd/` 不在业务 keyspace）；
/// - apply 期**不读墙钟/随机数**（确定性约束同约束 1）：`Enqueue` 携带
///   `proposed_at_unix`（由 proposer/leader 在 propose 前填，先例
///   `LeaseOp::Grant.deadline_wall_ms`）。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PdOp {
    /// 入队一个 operator。幂等：队列中已有**相同** Pending/Running operator 时
    /// no-op（全局去重命中点）；`op_id` = 本命令的日志 index（revision）。
    Enqueue {
        op: Operator,
        /// 提出方节点（审计/归属）
        requester: NodeID,
        /// 提出方墙钟 Unix 秒（proposer 填；仅信息性）
        proposed_at_unix: i64,
    },
    /// 认领执行（仅 Pending 生效；已被认领/Running → no-op，防双认领）
    ///
    /// `claimed_at_unix` 由认领者（执行器节点）在 propose 前填本节点墙钟
    /// （Running 超时重认领判定依赖的"Running 起始时间"——apply 期不读
    /// 墙钟的确定性约束同 `Enqueue.proposed_at_unix` 先例）。
    Claim {
        op_id: u64,
        node_id: NodeID,
        claimed_at_unix: i64,
    },
    /// 完成（仅 Running 且 `claimed_by == node_id` 生效；成功或失败携带原因）
    Complete {
        op_id: u64,
        node_id: NodeID,
        success: bool,
        error: String,
    },
    /// 重新入队（Running → Pending、清认领者；认领者失联/可重试时用）
    Requeue { op_id: u64 },
}

/// region 0 PD 队列条目（`/_pd/ops/{op_id:u64be}` 的 bincode 载荷）
///
/// `op_id` ≡ 入队日志 index（revision）：单调、唯一、全序、跨重放/重启稳定，
/// 无需独立计数器。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PdQueueEntry {
    /// 入队日志 index（revision）
    pub op_id: u64,
    /// 具体调度操作
    pub op: Operator,
    /// 生命周期（Pending → Running → Success/Failed；Running → Pending）
    pub status: OperatorStatus,
    /// 提出方节点（审计）
    pub requester: NodeID,
    /// 认领执行节点（0 = 未认领）
    pub claimed_by: NodeID,
    /// 提出方墙钟 Unix 秒（仅信息性，不参与决策）
    pub proposed_at_unix: i64,
    /// 认领方墙钟 Unix 秒（Running 超时重认领的依据；0 = 未认领）
    pub claimed_at_unix: i64,
    /// Failed 原因 / 审计信息
    pub error: String,
}

impl PdQueueEntry {
    /// 新建 Pending 条目（op_id = Enqueue 日志 index）
    pub fn new_pending(op_id: u64, op: Operator, requester: NodeID, proposed_at_unix: i64) -> Self {
        Self {
            op_id,
            op,
            status: OperatorStatus::Pending,
            requester,
            claimed_by: 0,
            proposed_at_unix,
            claimed_at_unix: 0,
            error: String::new(),
        }
    }

    /// 是否待执行
    pub fn is_pending(&self) -> bool {
        self.status == OperatorStatus::Pending
    }

    /// 是否执行中（已被某节点认领）
    pub fn is_running(&self) -> bool {
        self.status == OperatorStatus::Running
    }

    /// 是否终态（Success/Failed/Cancelled；裁剪对象）
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.status,
            OperatorStatus::Success | OperatorStatus::Failed(_) | OperatorStatus::Cancelled
        )
    }

    /// 认领：仅 Pending 生效（置 Running + claimed_by + 认领墙钟）。返回是否生效。
    pub fn try_claim(&mut self, node_id: NodeID, claimed_at_unix: i64) -> bool {
        if self.is_pending() {
            self.status = OperatorStatus::Running;
            self.claimed_by = node_id;
            self.claimed_at_unix = claimed_at_unix;
            true
        } else {
            false
        }
    }

    /// 完成：仅 Running 且认领者 == node_id 生效。返回是否生效。
    pub fn try_complete(&mut self, node_id: NodeID, success: bool, error: &str) -> bool {
        if self.is_running() && self.claimed_by == node_id {
            self.status = if success {
                OperatorStatus::Success
            } else {
                OperatorStatus::Failed(error.to_string())
            };
            self.error = if success {
                String::new()
            } else {
                error.to_string()
            };
            true
        } else {
            false
        }
    }

    /// 重新入队：仅 Running 生效（回 Pending、清认领者/认领墙钟与错误）。
    /// 返回是否生效。
    pub fn try_requeue(&mut self) -> bool {
        if self.is_running() {
            self.status = OperatorStatus::Pending;
            self.claimed_by = 0;
            self.claimed_at_unix = 0;
            self.error.clear();
            true
        } else {
            false
        }
    }

    /// Running 已持续时长（秒；未认领返回 0）。供 region 0 leader 判
    /// Running 超时重认领（`claimed_at_unix` 由 Claim 命令携带，见上）。
    pub fn running_for_secs(&self, now_unix: i64) -> i64 {
        if self.is_running() && self.claimed_at_unix > 0 {
            (now_unix - self.claimed_at_unix).max(0)
        } else {
            0
        }
    }

    /// bincode 序列化（redb 原始行）
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| Error::Internal(format!("encode pd queue entry: {e}")))
    }

    /// bincode 反序列化（损坏行返回 None，调用方跳过）
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        bincode::deserialize(bytes).ok()
    }
}

/// 对象存储数据面操作（docs/volume-object-storage.md 决策记录）
///
/// manifest 以**用户 KV** 形式存于保留前缀 `/obj/m/{bucket}/{object_id}`
/// （自动获得 /kv/ 的加密/快照/压缩/强一致语义）；本命令仅承载 chunk 数据面
/// （apply 时落 append-only chunk 文件，不进 MVCC、不入快照）与受控的
/// manifest 状态迁移（Begin/Chunk/Commit/Delete）。
///
/// 语义（apply 内确定性执行，`storage::object_store::apply_object_store_op`）：
/// - Begin：已存在（Committed/Creating）→ no-op 冲突；tombstone 后允许重建；
///   `total_size == 0` = **未知长度**（Commit 时按实际字节定长）；
/// - Chunk：按 seq 严格递增追加（并发/重试重叠 → no-op）；数据 ≤ chunk_size，
///   累计 ≤ Begin 声明 total_size（未知长度时以 `max_object_size` 为上限）；
///   文件先落盘、manifest 后提交（同 apply 串行）；
/// - Commit：声明长度须 size==total_size，未知长度则**以 size 定长**，均置 committed
///   （幂等）；不符/无数据 → no-op（GC 收尾）；
/// - Delete：KV tombstone + 同步删除 chunk 文件（幂等；no-op 也消耗 revision）。
///
/// **末尾追加**（bincode 变体索引兼容；勿插队）。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ObjectStoreOp {
    /// 开始上传：创建 Creating manifest（声明期望总字节数）。
    /// `total_size == 0` = **未知长度**（Commit 时按实际累计字节定长）；
    /// `> 0` = 声明长度（Commit 要求严格相等）。
    /// `started_at_unix` 由提议侧填墙钟（apply 不读墙钟，先例同 PdQueueEntry）。
    Begin {
        bucket: Vec<u8>,
        object_id: Vec<u8>,
        total_size: u64,
        started_at_unix: i64,
    },
    /// 追加一个 chunk：数据随 raft 日志复制；apply 落文件 + manifest 追加记录。
    /// `now_unix` 由提议侧填墙钟（GC 判 stale Creating）。
    Chunk {
        bucket: Vec<u8>,
        object_id: Vec<u8>,
        seq: u32,
        data: Vec<u8>,
        now_unix: i64,
    },
    /// 完成上传：校验字节数后置 committed
    Commit { bucket: Vec<u8>, object_id: Vec<u8> },
    /// 删除：KV tombstone + chunk 文件清理
    Delete { bucket: Vec<u8>, object_id: Vec<u8> },
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
    /// 范围删除（R-SVC-07）：单个 raft 命令原子删除 `[key, range_end)` 内所有 Key
    DeleteRange { key: Vec<u8>, range_end: Vec<u8> },
    /// 原子条件事务
    Txn {
        /// 比较条件列表（AND 语义，全部满足才执行 success 分支）
        compares: Vec<TxnCompare>,
        /// 条件全部满足时执行的操作
        success_ops: Vec<TxnOp>,
        /// 任一条件不满足时执行的操作
        failure_ops: Vec<TxnOp>,
    },
    /// Lease 生命周期操作
    Lease(LeaseOp),
    /// 鉴权元数据操作
    Auth(AuthOp),
    /// 压缩历史：raft 下发 compact revision，节点一致删除
    /// revision 之前的 changelog/tombstone；apply 幂等（单调，低 revision 为 no-op）
    Compact { revision: u64 },
    /// per-Region 删除绑定到某 Lease 的全部 Key
    ///
    /// Multi-Raft 模式下 Lease 记录在 region 0（全局租约表），但绑定 Key 落在
    /// 各业务 Region 的 MVCC。region 0 的 `LeaseOp::Revoke` apply 后，各 Region
    /// leader 经本命令在**各自 Region raft** 内按 `KvMetadata.lease_id` 索引
    /// 原子删除绑定 Key（apply 期扫描，避免 leader 侧扫描的 TOCTOU；幂等）。
    DeleteKeysByLease { lease_id: i64 },
    /// PD 全局调度命令（仅 region 0 raft 提出）
    ///
    /// **末尾追加**（变体索引兼容）；data region raft 收到视为无害记录。
    Pd(PdOp),
    /// 对象存储数据面命令（manifest 状态迁移 + chunk 文件副作用）。
    ///
    /// 按对象 manifest key 路由到所属 Region raft 提出；**末尾追加**。
    ObjectStore(ObjectStoreOp),
}

impl std::fmt::Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::Put { key, .. } => write!(f, "Put(key={})", String::from_utf8_lossy(key)),
            Command::Delete { key } => write!(f, "Delete(key={})", String::from_utf8_lossy(key)),
            Command::DeleteRange { key, range_end } => write!(
                f,
                "DeleteRange(key={}, range_end={})",
                String::from_utf8_lossy(key),
                String::from_utf8_lossy(range_end)
            ),
            Command::Txn { compares, .. } => {
                write!(f, "Txn(compares={})", compares.len())
            }
            Command::Lease(op) => write!(f, "Lease({op:?})"),
            Command::Auth(op) => write!(f, "Auth({op:?})"),
            Command::Compact { revision } => write!(f, "Compact(revision={revision})"),
            Command::DeleteKeysByLease { lease_id } => {
                write!(f, "DeleteKeysByLease(lease_id={lease_id})")
            }
            Command::Pd(op) => write!(f, "Pd({op:?})"),
            Command::ObjectStore(op) => write!(f, "ObjectStore({op:?})"),
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
    /// DeleteRange 操作结果（R-SVC-07）
    DeleteRange {
        revision: u64,
        deleted: u64,
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
    /// Lease 操作结果
    Lease {
        revision: u64,
    },
    /// Auth 操作结果
    Auth {
        revision: u64,
    },
    /// Compact 操作结果：实际生效的 compacted revision
    /// 对象存储数据面操作结果：op 是否达成语义期望
    /// （Begin 冲突/Chunk 顺序错乱/Commit 字节不符/Delete no-op → ok=false）
    ObjectStore {
        revision: u64,
        ok: bool,
    },
    Compact {
        compacted_revision: u64,
    },
}

impl std::fmt::Display for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Response::Put { revision } => write!(f, "Put(rev={})", revision),
            Response::Delete { revision } => write!(f, "Delete(rev={})", revision),
            Response::DeleteRange { revision, deleted } => {
                write!(f, "DeleteRange(rev={}, deleted={})", revision, deleted)
            }
            Response::Txn {
                succeeded,
                revision,
                ..
            } => write!(f, "Txn(succeeded={}, rev={})", succeeded, revision),
            Response::Lease { revision } => write!(f, "Lease(rev={})", revision),
            Response::Auth { revision } => write!(f, "Auth(rev={})", revision),
            Response::ObjectStore { revision, ok } => {
                write!(f, "ObjectStore(rev={}, ok={})", revision, ok)
            }
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

    // ──── Compact serde ────

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

    // ──── Command::Pd serde 往返 ────

    fn test_add_peer() -> crate::pd::operator::Operator {
        crate::pd::operator::Operator::AddPeer {
            region_id: 1,
            node_id: 3,
            raft_addr: "127.0.0.1:5003".into(),
        }
    }

    #[test]
    fn test_command_pd_enqueue_serde_roundtrip() {
        let cmd = Command::Pd(PdOp::Enqueue {
            op: test_add_peer(),
            requester: 2,
            proposed_at_unix: 1_700_000_000,
        });
        let bytes = bincode::serialize(&cmd).unwrap();
        let decoded: Command = bincode::deserialize(&bytes).unwrap();
        match decoded {
            Command::Pd(PdOp::Enqueue {
                op,
                requester,
                proposed_at_unix,
            }) => {
                assert_eq!(op.region_id(), 1);
                assert_eq!(requester, 2);
                assert_eq!(proposed_at_unix, 1_700_000_000);
            }
            _ => panic!("expected Pd(Enqueue)"),
        }
    }

    #[test]
    fn test_command_pd_claim_complete_requeue_serde_roundtrip() {
        let cmds = vec![
            Command::Pd(PdOp::Claim {
                op_id: 42,
                node_id: 7,
                claimed_at_unix: 1_700_000_100,
            }),
            Command::Pd(PdOp::Complete {
                op_id: 42,
                node_id: 7,
                success: false,
                error: "region 1 not leader".into(),
            }),
            Command::Pd(PdOp::Requeue { op_id: 42 }),
        ];
        for cmd in cmds {
            let bytes = bincode::serialize(&cmd).unwrap();
            let decoded: Command = bincode::deserialize(&bytes).unwrap();
            assert!(matches!(decoded, Command::Pd(_)), "roundtrip {:?}", cmd);
        }
    }

    #[test]
    fn test_pd_queue_entry_serde_roundtrip() {
        let entry = PdQueueEntry::new_pending(42, test_add_peer(), 1, 1_700_000_000);
        let bytes = entry.to_bytes().unwrap();
        let decoded = PdQueueEntry::from_bytes(&bytes).expect("decode");
        assert_eq!(decoded, entry);
        assert!(decoded.is_pending());
        assert_eq!(decoded.op_id, 42);

        // 认领后状态往返（Running + claimed_by + claimed_at）
        let mut claimed = entry;
        assert!(claimed.try_claim(7, 1_700_000_100));
        let bytes = claimed.to_bytes().unwrap();
        let decoded = PdQueueEntry::from_bytes(&bytes).expect("decode");
        assert!(decoded.is_running());
        assert_eq!(decoded.claimed_by, 7);
        assert_eq!(decoded.claimed_at_unix, 1_700_000_100);
        // Requeue 清认领墙钟
        let mut requeued = decoded;
        assert!(requeued.try_requeue());
        assert_eq!(requeued.claimed_at_unix, 0);
    }

    #[test]
    fn test_command_pd_display() {
        let cmd = Command::Pd(PdOp::Requeue { op_id: 9 });
        let s = format!("{cmd}");
        assert!(s.contains("Pd"));
        assert!(s.contains("Requeue"));
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
