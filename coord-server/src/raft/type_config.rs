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

/// v2 租约操作（仅 region 0 system raft；apply 持久化到 `/_leasev2/{id}`）。
///
/// 与 v1 [`LeaseOp`] 分离：v1 的 revision ≡ log index，v2 的数据删除必须使用
/// TSO 分配的 `commit_ts`，两者不能共用记录格式与清理路径。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LeaseV2Op {
    /// 发放租约：`lease_id`/`granted_ts` 由 leader 从 TSO 取得后入日志，
    /// apply 不重新分配（确定性）。
    Grant {
        lease_id: i64,
        ttl_seconds: i64,
        granted_ts: u64,
        deadline_wall_ms: i64,
        request_id: Vec<u8>,
    },
    /// 续约：只延长 deadline，不产生 commit_ts。
    KeepAlive {
        lease_id: i64,
        deadline_wall_ms: i64,
    },
    /// 吊销/到期：记录清理时间戳。已吊销记录收到更高时间戳时视为清理重试
    /// （`COMMIT_REJECTED` 后由 revoker 重新取 TSO）。
    Revoke {
        lease_id: i64,
        commit_ts: u64,
        /// 到期 worker 观测到的 deadline；apply 时若记录已被续期（deadline
        /// 更晚）则放弃本次到期清理。0 = 显式 revoke / 清理重试，不做该守卫。
        expected_deadline_wall_ms: i64,
        request_id: Vec<u8>,
    },
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
    /// 只允许末尾追加（变体索引 = 旧日志/快照升级兼容）。
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
    /// 只产生**一条** raft 条目。会话表随正常 refresh 流量持续增长（10k 客户端
    /// ≈ 96 万条/天，内存 150–200 MB/天且每条约一行 KV），而"过期只报错不回收"——
    /// 逐条提案会把清理本身变成 raft 日志洪泛，故用批量变体。
    ///
    /// 变体索引 17（**末尾追加**，旧日志/快照的既有索引不漂移）。
    ConsumeSessions { hash_hexes: Vec<String> },
}

// ──── PD 全局调度命令（/ 见 docs）────

/// PD 全局 operator 队列治理命令（经 **region 0 system raft** 承载）。
///
/// 设计要点：
/// - 本枚举及 `Command::Pd` 变体**只允许末尾追加**（变体索引 = 旧日志/快照
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

/// region 0 PD 队列条目（`/_pd/ops/{op_id:u64be}` 的信封 V2 载荷）
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

    /// 统一信封 V2 序列化（redb 原始行；读路径兼容旧格式）。
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        crate::storage::envelope::encode(self)
            .map_err(|e| Error::Internal(format!("encode pd queue entry: {e}")))
    }

    /// 反序列化（唯一格式：信封 V2-postcard；损坏/退役行返回 None，调用方跳过）。
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        crate::storage::envelope::decode(bytes).ok()
    }
}

/// 对象存储数据面操作（docs/production/volume-object-storage.md 决策记录）
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
/// **末尾追加**（变体索引 = 旧日志/快照兼容；勿插队）。
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

/// 全局时间戳（TSO）命令（仅 region 0 system raft 提出）。
///
/// `batch_size` 由 proposer 归一（0 → 1、上限校验）后入日志：apply 必须只依赖
/// 日志内容（确定性），不得读配置或墙钟。高水位持久化与幂等见
/// `MvccStorage::apply_tso_allocate`。**末尾追加变体**（旧日志/快照兼容）。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TsoOp {
    /// 分配 `[start_ts, start_ts + batch_size)`；只增不减、允许空洞、绝不复用
    Allocate { batch_size: u32 },
}

/// Commit Sequencer 事务写操作（point / range）。
///
/// `DeleteRange` 是末尾追加的变体：旧决策日志用 postcard 编码，给枚举末尾加
/// 变体不会破坏解码；不要调整既有变体顺序。区间写没有单一 key，调用方用
/// [`SequencerMutation::point_key`] 显式区分。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SequencerMutation {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        lease_id: Option<i64>,
    },
    Delete {
        key: Vec<u8>,
    },
    /// 删除 `[start, end)`；`end` 非空（RPC 层已校验）。
    DeleteRange {
        start: Vec<u8>,
        end: Vec<u8>,
    },
}

impl SequencerMutation {
    /// 点写的 key；区间写返回 `None`（没有单一归属 key）。
    pub fn point_key(&self) -> Option<&[u8]> {
        match self {
            Self::Put { key, .. } | Self::Delete { key } => Some(key),
            Self::DeleteRange { .. } => None,
        }
    }
}

/// 一个数据 Region 要应用的决策切片（#148）。
///
/// `mutations` 只包含属于目标 Region 的 key；没有本 Region key 的决策也保留
/// 空条目，让该 Region 的已应用游标随全局决策顺序前进（否则空闲 Region 会
/// 永久钉住 safe_ts）。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitDecisionApply {
    pub commit_ts: u64,
    pub mutations: Vec<SequencerMutation>,
}

/// 一条带 Commit Log 元数据的决策切片（#153）。
///
/// 与 [`CommitDecisionApply`] 分开而不是加字段：raft 日志是 postcard（非自描述），
/// 给既有结构体加字段会让旧日志解码失败；新变体末尾追加，旧日志照常重放。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitDecisionApplyLog {
    /// 记录归属的 Commit Log shard（#154 接管路由前恒为
    /// `commit_log::COMMIT_LOG_STAGING_SHARD_ID`）。
    pub shard_id: u64,
    pub region_id: u64,
    pub commit_ts: u64,
    pub txn_id: Vec<u8>,
    pub start_ts: u64,
    /// 只含本 Region 的 key（与 [`CommitDecisionApply`] 同一裁剪口径）。
    pub mutations: Vec<SequencerMutation>,
}

/// 一条待 Commit Sequencer 校验的事务。
///
/// `commit_ts` 由 region 0 leader 从 TSO 取号后写入日志；apply 只校验/落盘它，
/// 不再分配（确定性）。`read_set` 是去重后的 point key；range 谓词放在
/// [`SequencerTxnWithRanges`] 里，避免旧日志因结构体新增字段而无法解码。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequencerTxn {
    pub txn_id: Vec<u8>,
    pub start_ts: u64,
    pub commit_ts: u64,
    pub request_id: Vec<u8>,
    pub read_set: Vec<Vec<u8>>,
    pub mutations: Vec<SequencerMutation>,
}

/// 半开 range 读谓词 `[start, end)`；`end` 为空 = 无上界。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequencerReadRange {
    pub start: Vec<u8>,
    pub end: Vec<u8>,
}

/// 带 range 读谓词的候选事务（#149）。
///
/// 与 [`SequencerTxn`] 分开是为了让 `SequencerOp` 以**末尾追加变体**的方式升级：
/// raft 日志用 postcard（非自描述），给既有结构体加字段会让旧日志解码失败。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequencerTxnWithRanges {
    pub txn: SequencerTxn,
    pub read_ranges: Vec<SequencerReadRange>,
}

/// OCC 校验上限（#149）。
///
/// 由 leader 按 `[kv_t]` 配置写入命令；apply 只读日志里的值，保证各节点即使
/// 配置漂移也得到同一判定。`0` 沿用区间查询约定：不限制。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequencerValidationLimits {
    pub max_read_set: u32,
    pub max_range: u32,
}

/// 非法候选事务（用户级输入问题，不是 raft 错误）。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SequencerInvalid {
    /// `commit_ts <= start_ts`：不可能作为一次“读到 start_ts 快照后提交”的决策。
    CommitTsNotAboveStart,
    /// 空 `txn_id` 无法建立单次提交索引。
    MissingTxnId,
    /// point + range 读集条目超过 `max_read_set`（映射 `READ_SET_TOO_LARGE`）。
    ReadSetTooLarge,
    /// range 校验扫描的索引项超过 `max_range`；无法证明无冲突，必须 fail-closed。
    RangeScanExceeded,
}

/// apply 对单条事务的结果（用户级结果一律走响应，不让 apply 返回 `Err`）。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SequencerOutcome {
    /// 校验通过并写入决策。
    Committed { commit_ts: u64 },
    /// 读集在 `(start_ts, commit_ts]` 内被写命中；不写决策。
    Conflict {
        commit_ts: u64,
        conflict_key: Vec<u8>,
        conflict_ts: u64,
    },
    /// 同一事务/请求已决策：返回首次 `commit_ts`，不重复分配。
    Duplicate { commit_ts: u64 },
    /// `commit_ts` 不高于已决策水位：不写决策（D11.1 的最后防线）。
    OutOfOrder { commit_ts: u64, last_commit_ts: u64 },
    /// 输入非法；不写决策。
    Invalid {
        commit_ts: u64,
        reason: SequencerInvalid,
    },
}

/// Commit Sequencer 命令（仅 region 0 system raft 提出）。
///
/// **末尾追加变体**（旧日志/快照兼容）。批内顺序已由 leader 按 `commit_ts`
/// 递增排列；apply 按数组顺序校验。带 range 谓词的命令把上限一并写入日志，
/// apply 不读本地配置，因此判定在节点间确定。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SequencerOp {
    ValidateAndCommit {
        txns: Vec<SequencerTxn>,
    },
    ValidateAndCommitWithRanges {
        txns: Vec<SequencerTxnWithRanges>,
        limits: SequencerValidationLimits,
    },
}

/// Commit Log shard 元数据操作（仅 region 0 system raft 提出，#154）。
///
/// apply 只依赖日志内容：边界与时间戳都随命令携带，节点间判定确定。非法操作
/// 降级为 no-op（D11.5：用户级/元数据级拒绝不得让 raft apply 返回 `Err`）。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitLogShardOp {
    /// 打开新 shard：表空时用 `start_ts`，否则起点取前一 shard 的 `end_ts`。
    Open { start_ts: u64, opened_at_ms: u64 },
    /// 给最后一个未定界 shard 定界（不可回退）。
    SetEnd {
        shard_id: u64,
        end_ts: u64,
        now_ms: u64,
    },
    /// 标记已定界 shard 为 sealed（幂等）。
    Seal { shard_id: u64, now_ms: u64 },
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
    /// 全局 TSO 批次分配（仅 region 0 system raft 提出）。
    ///
    /// **末尾追加**；data region raft 不应收到（收到时按普通内部记录 apply，
    /// 不影响业务 keyspace）。
    Tso(TsoOp),
    /// v2 直写 Put：revision = 调用方 `commit_ts`（ADR-0015 D3/D11）。
    ///
    /// **末尾追加**；仅 `[kv_t].enabled=true` 的 v2 集群提出。
    PutV2 {
        key: Vec<u8>,
        value: Vec<u8>,
        lease_id: Option<i64>,
        commit_ts: u64,
    },
    /// v2 直写 Delete：revision = `commit_ts`。
    DeleteV2 { key: Vec<u8>, commit_ts: u64 },
    /// v2 直写范围删除：整个区间共享同一 `commit_ts`。
    DeleteRangeV2 {
        key: Vec<u8>,
        range_end: Vec<u8>,
        commit_ts: u64,
    },
    /// 把数据 Region 的已 apply 水位抬到安全点 fence（不写数据/changelog）。
    ///
    /// **末尾追加**；仅 `[kv_t].enabled=true` 的 v2 集群提出。之后
    /// `commit_ts <= commit_ts` 的写入会被 `CommitRejected`（safe-point 语义）。
    AdvanceSafeTs { commit_ts: u64 },
    /// 发布全局 safe-point fence（仅 region 0 system raft 提出）。
    ///
    /// **末尾追加**；`fence_ts` 是 TSO 分配边界，节点经 raft apply 收敛。
    PublishSafeTs { fence_ts: u64 },
    /// v2 租约生命周期（仅 region 0 system raft 提出）。
    ///
    /// **末尾追加**；租约表与业务 keyspace 分离，apply 不推进 revision。
    LeaseV2(LeaseV2Op),
    /// 数据 Region 按 `KvMetadata.lease_id` 在 `commit_ts` 上删除绑定 Key。
    ///
    /// **末尾追加**；只由 v2 Lease revoke/到期触发。
    DeleteKeysByLeaseV2 { lease_id: i64, commit_ts: u64 },
    /// Commit Sequencer 校验/决策（仅 region 0 system raft 提出）。
    ///
    /// **末尾追加**；决策写 `/_sys/seq/`（随 `/_sys/*` 快照域恢复）。
    Sequencer(SequencerOp),
    /// 数据 Region 应用一批已决策事务（#148；仅数据 Region raft 提出）。
    ///
    /// **末尾追加**；`mutations` 只含本 Region 的 key，空切片用于推进游标。
    ApplyCommitDecisions { decisions: Vec<CommitDecisionApply> },
    /// 数据 Region 应用一批已决策事务，并在同一 redb 事务里写 CommitRecord（#153）。
    ///
    /// **末尾追加**；`max_txn_bytes` 取提出方的 `[kv_t].max_txn_bytes`，让重放节点
    /// 用与写入方相同的写集上限校验记录（日志内容决定结果，与本地配置无关）。
    ApplyCommitDecisionsWithLog {
        max_txn_bytes: u64,
        decisions: Vec<CommitDecisionApplyLog>,
    },
    /// Commit Log shard 元数据（仅 region 0 system raft 提出，#154）。
    ///
    /// **末尾追加**；数据 Region 收到按无害记录 apply（不触碰业务 keyspace）。
    CommitLogShard(CommitLogShardOp),
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
            Command::Tso(op) => write!(f, "Tso({op:?})"),
            Command::PutV2 { key, commit_ts, .. } => write!(
                f,
                "PutV2(key={}, commit_ts={commit_ts})",
                String::from_utf8_lossy(key)
            ),
            Command::DeleteV2 { key, commit_ts } => write!(
                f,
                "DeleteV2(key={}, commit_ts={commit_ts})",
                String::from_utf8_lossy(key)
            ),
            Command::DeleteRangeV2 {
                key,
                range_end,
                commit_ts,
            } => write!(
                f,
                "DeleteRangeV2(key={}, range_end={}, commit_ts={commit_ts})",
                String::from_utf8_lossy(key),
                String::from_utf8_lossy(range_end)
            ),
            Command::AdvanceSafeTs { commit_ts } => {
                write!(f, "AdvanceSafeTs(commit_ts={commit_ts})")
            }
            Command::PublishSafeTs { fence_ts } => {
                write!(f, "PublishSafeTs(fence_ts={fence_ts})")
            }
            Command::LeaseV2(op) => write!(f, "LeaseV2({op:?})"),
            Command::DeleteKeysByLeaseV2 {
                lease_id,
                commit_ts,
            } => {
                write!(
                    f,
                    "DeleteKeysByLeaseV2(lease_id={lease_id}, commit_ts={commit_ts})"
                )
            }
            Command::Sequencer(SequencerOp::ValidateAndCommit { txns }) => {
                write!(f, "Sequencer(txns={})", txns.len())
            }
            Command::Sequencer(SequencerOp::ValidateAndCommitWithRanges { txns, .. }) => {
                write!(f, "SequencerWithRanges(txns={})", txns.len())
            }
            Command::ApplyCommitDecisions { decisions } => {
                write!(f, "ApplyCommitDecisions(decisions={})", decisions.len())
            }
            Command::ApplyCommitDecisionsWithLog { decisions, .. } => write!(
                f,
                "ApplyCommitDecisionsWithLog(decisions={})",
                decisions.len()
            ),
            Command::CommitLogShard(op) => write!(f, "CommitLogShard({op:?})"),
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
    /// TSO 批次分配结果（左闭右开）
    Tso {
        start_ts: u64,
        end_ts: u64,
    },
    /// v2 写被拒：`commit_ts <= current_revision` 且未命中已有写入
    /// （D11.4）。用户级拒绝不返回 apply `Err`（D11.5）。
    CommitRejected {
        commit_ts: u64,
        current_revision: u64,
    },
    /// 数据 Region 已 apply 水位抬升结果（#142）。
    SafeTsAdvanced {
        revision: u64,
    },
    /// region 0 安全点 fence 发布结果（#142；实际生效值）。
    SafeTsPublished {
        fence_ts: u64,
    },
    /// v2 租约发放结果：`created=false` = 显式 id 已被不同请求占用
    /// （handler 报 `ALREADY_EXISTS`）；`request_id` 命中既有记录时回放首次
    /// id/granted_ts 且 `created=true`。
    LeaseV2Grant {
        lease_id: i64,
        granted_ts: u64,
        ttl_seconds: i64,
        created: bool,
    },
    /// v2 续约结果：`ok=false` 表示记录不存在或已吊销（不写 Err）。
    LeaseV2KeepAlive {
        ok: bool,
    },
    /// v2 吊销结果：0 = 本次没有新决策（幂等/未知/已吊销）。
    LeaseV2Revoke {
        commit_ts: u64,
    },
    /// Commit Sequencer 批结果：与命令中的事务一一对应、顺序一致。
    Sequencer {
        outcomes: Vec<SequencerOutcome>,
    },
    /// 数据 Region 应用决策的进度（#148）：`through_ts` 之后仍有未应用决策时
    /// 说明卡在坏决策之前（游标未推进）。
    CommitDecisionsApplied {
        through_ts: u64,
        decisions: u64,
    },
    /// region 0 应用 shard 元数据操作后的整表视图（#154）。
    CommitLogShard {
        shards: Vec<crate::storage::commit_log_shard::CommitLogShardMeta>,
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
            Response::Tso { start_ts, end_ts } => {
                write!(f, "Tso([{start_ts}, {end_ts}))")
            }
            Response::CommitRejected {
                commit_ts,
                current_revision,
            } => write!(
                f,
                "CommitRejected(commit_ts={commit_ts}, current={current_revision})"
            ),
            Response::SafeTsAdvanced { revision } => {
                write!(f, "SafeTsAdvanced(revision={revision})")
            }
            Response::SafeTsPublished { fence_ts } => {
                write!(f, "SafeTsPublished(fence_ts={fence_ts})")
            }
            Response::LeaseV2Grant {
                lease_id,
                granted_ts,
                ttl_seconds,
                created,
            } => write!(
                f,
                "LeaseV2Grant(id={lease_id}, granted_ts={granted_ts}, ttl={ttl_seconds}, created={created})"
            ),
            Response::LeaseV2KeepAlive { ok } => write!(f, "LeaseV2KeepAlive(ok={ok})"),
            Response::LeaseV2Revoke { commit_ts } => {
                write!(f, "LeaseV2Revoke(commit_ts={commit_ts})")
            }
            Response::Sequencer { outcomes } => {
                write!(f, "Sequencer(outcomes={})", outcomes.len())
            }
            Response::CommitDecisionsApplied {
                through_ts,
                decisions,
            } => write!(
                f,
                "CommitDecisionsApplied(through={through_ts}, decisions={decisions})"
            ),
            Response::CommitLogShard { shards } => {
                write!(f, "CommitLogShard(shards={})", shards.len())
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
        let bytes = postcard::to_allocvec(&cmd).unwrap();
        let decoded: Command = postcard::from_bytes(&bytes).unwrap();
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
    fn test_command_sequencer_serde_roundtrip() {
        let cmd = Command::Sequencer(SequencerOp::ValidateAndCommit {
            txns: vec![SequencerTxn {
                txn_id: b"txn-1".to_vec(),
                start_ts: 7,
                commit_ts: 9,
                request_id: b"req-1".to_vec(),
                read_set: vec![b"reads".to_vec()],
                mutations: vec![
                    SequencerMutation::Put {
                        key: b"k".to_vec(),
                        value: b"v".to_vec(),
                        lease_id: Some(3),
                    },
                    SequencerMutation::Delete {
                        key: b"gone".to_vec(),
                    },
                ],
            }],
        });
        let bytes = postcard::to_allocvec(&cmd).unwrap();
        let decoded: Command = postcard::from_bytes(&bytes).unwrap();
        match decoded {
            Command::Sequencer(SequencerOp::ValidateAndCommit { txns }) => {
                assert_eq!(txns.len(), 1);
                assert_eq!(txns[0].txn_id, b"txn-1");
                assert_eq!(txns[0].commit_ts, 9);
                assert_eq!(txns[0].read_set, [b"reads".to_vec()]);
                assert_eq!(txns[0].mutations.len(), 2);
            }
            other => panic!("expected Sequencer, got {other:?}"),
        }
    }

    #[test]
    fn test_command_delete_serde_roundtrip() {
        let cmd = Command::Delete {
            key: b"bye".to_vec(),
        };
        let bytes = postcard::to_allocvec(&cmd).unwrap();
        let decoded: Command = postcard::from_bytes(&bytes).unwrap();
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
        let bytes = postcard::to_allocvec(&cmd).unwrap();
        let decoded: Command = postcard::from_bytes(&bytes).unwrap();
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
        let bytes = postcard::to_allocvec(&cmd).unwrap();
        let decoded: Command = postcard::from_bytes(&bytes).unwrap();
        match decoded {
            Command::Put { lease_id, .. } => assert_eq!(lease_id, None),
            _ => panic!("expected Put"),
        }
    }

    // ──── Compact serde ────

    #[test]
    fn test_command_compact_serde_roundtrip() {
        let cmd = Command::Compact { revision: 123456 };
        let bytes = postcard::to_allocvec(&cmd).unwrap();
        let decoded: Command = postcard::from_bytes(&bytes).unwrap();
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
        let bytes = postcard::to_allocvec(&cmd).unwrap();
        let decoded: Command = postcard::from_bytes(&bytes).unwrap();
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
            let bytes = postcard::to_allocvec(&cmd).unwrap();
            let decoded: Command = postcard::from_bytes(&bytes).unwrap();
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

    /// 唯一 V2 + 退役格式拒绝（P3：PD 队列条目）。
    /// 负控制：恢复 V1/无前缀读腿 ⇒ 退役断言红；放宽精确消费 ⇒
    /// 尾随/截断断言红；放宽版本检查 ⇒ 未知版本断言红。
    #[test]
    fn test_pd_queue_entry_v2_only_read_and_retired_rejected() {
        let entry = PdQueueEntry::new_pending(42, test_add_peer(), 1, 1_700_000_000);

        // 写路径断言：to_bytes 产物必须为 V2 前缀，且可回读
        let written = entry.to_bytes().unwrap();
        assert!(written.starts_with(&crate::storage::envelope::MAGIC));
        assert_eq!(
            written[crate::storage::envelope::MAGIC.len()],
            crate::storage::envelope::VERSION_V2
        );
        assert_eq!(PdQueueEntry::from_bytes(&written).as_ref(), Some(&entry));

        // 退役格式：V1 / 无前缀行 ⇒ None
        let mut v1 = Vec::new();
        v1.extend_from_slice(&crate::storage::envelope::MAGIC);
        v1.push(1);
        v1.extend_from_slice(&[0xAA, 0xBB]);
        assert!(PdQueueEntry::from_bytes(&v1).is_none());
        assert!(PdQueueEntry::from_bytes(&[0x05u8, 0, 0, 0, 0, 0, 0, 0]).is_none());

        // 篡改矩阵：未知版本 / 魔数 / 尾随 / 截断 ⇒ None
        let mut bad_version = written.clone();
        bad_version[crate::storage::envelope::MAGIC.len()] = 9;
        assert!(PdQueueEntry::from_bytes(&bad_version).is_none());
        let mut bad_magic = written.clone();
        bad_magic[0] = 0x03;
        assert!(PdQueueEntry::from_bytes(&bad_magic).is_none());
        let mut trailing = written.clone();
        trailing.extend_from_slice(&[0xDE, 0xAD]);
        assert!(PdQueueEntry::from_bytes(&trailing).is_none());
        assert!(PdQueueEntry::from_bytes(&written[..written.len() - 1]).is_none());
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
        let bytes = postcard::to_allocvec(&resp).unwrap();
        let decoded: Response = postcard::from_bytes(&bytes).unwrap();
        match decoded {
            Response::Compact { compacted_revision } => assert_eq!(compacted_revision, 777),
            _ => panic!("expected Compact response"),
        }
    }

    // ──── Response serde ────

    #[test]
    fn test_response_put_serde_roundtrip() {
        let resp = Response::Put { revision: 12345 };
        let bytes = postcard::to_allocvec(&resp).unwrap();
        let decoded: Response = postcard::from_bytes(&bytes).unwrap();
        match decoded {
            Response::Put { revision } => assert_eq!(revision, 12345),
            _ => panic!("expected Put"),
        }
    }

    #[test]
    fn test_response_delete_serde_roundtrip() {
        let resp = Response::Delete { revision: 67890 };
        let bytes = postcard::to_allocvec(&resp).unwrap();
        let decoded: Response = postcard::from_bytes(&bytes).unwrap();
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
        let bytes = postcard::to_allocvec(&resp).unwrap();
        let decoded: Response = postcard::from_bytes(&bytes).unwrap();
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
    #[test]
    fn test_command_apply_commit_decisions_with_log_serde_roundtrip() {
        let cmd = Command::ApplyCommitDecisionsWithLog {
            max_txn_bytes: 4 * 1024 * 1024,
            decisions: vec![CommitDecisionApplyLog {
                shard_id: 1,
                region_id: 7,
                commit_ts: 10,
                txn_id: b"txn-10".to_vec(),
                start_ts: 9,
                mutations: vec![SequencerMutation::Put {
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                    lease_id: None,
                }],
            }],
        };
        let bytes = postcard::to_allocvec(&cmd).unwrap();
        let decoded: Command = postcard::from_bytes(&bytes).unwrap();
        match decoded {
            Command::ApplyCommitDecisionsWithLog {
                max_txn_bytes,
                decisions,
            } => {
                assert_eq!(max_txn_bytes, 4 * 1024 * 1024);
                assert_eq!(decisions.len(), 1);
                assert_eq!(decisions[0].shard_id, 1);
                assert_eq!(decisions[0].region_id, 7);
                assert_eq!(decisions[0].txn_id, b"txn-10");
                assert_eq!(decisions[0].start_ts, 9);
            }
            other => panic!("expected ApplyCommitDecisionsWithLog, got {other:?}"),
        }
    }
}
