# KV-T G3（#151）v2 Txn Commit 与幂等重试 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `coord.txn.v2.Txn.Commit` 从 `UNIMPLEMENTED` 接到已交付的
Commit Sequencer：写集校验、读集快照、冲突/重复/非法结果的错误码映射、
`request_id` 幂等重放，并提供 Rust client 的 v2 事务包装。

**Architecture:** `Commit` 在协调节点上一次性「冻结读集 → 交给 sequencer →
按结果收口会话」：会话在提交开始时从 `Active` 变为 `Committing`（此后 `Read`
一律拒绝，防止快照后的读绕过 OCC），sequencer 返回 `Committed`/`Duplicate`
后变 `Committed` 并在 TTL 内回放同一个 `commit_ts`；`Conflict`/`Invalid`
释放回 `Active`（冲突会让客户端换 `start_ts` 重跑，旧会话不再可用；非法输入
允许同一会话改正后重试）；传输失败（超时/换 leader）也释放回 `Active`，重试
靠 sequencer 的 `txn_id`/`request_id` 幂等索引保证不产生第二个决策。

**Tech Stack:** Rust、tokio、tonic（错误码 trailer）、prost、parking_lot。

**Spec:** `docs/adr/0015-kv-t.md` D4/D9/D11/D12.2/D12.5 + issue #151；前置
#144（读集）、#146（sequencer）、#148（决策 apply）、#149（OCC）、#150（TTL）；
下游 #152（故障切换）、#172（SDK GA）。

## Global Constraints

- `[kv_t].enabled=false`（默认）时 `Commit` 仍由 `KvTState::gate` 拒绝
  `FAILED_PRECONDITION`；v1 行为字节级不变（ADR-0001）。
- `Commit` 在 CommitDecision 持久化后返回 `commit_ts`，**不等待**各 Region
  apply（D12.3）；`commit_ts` 只由 sequencer 分配，RPC 不接受调用方时间戳。
- 冲突 = `ABORTED` + `TXN_CONFLICT`，不写 `CommitDecision`；会话随后不可用
  （再提交按 `TXN_NOT_FOUND` 处理，客户端重跑整个事务）。
- 写集序列化上限 `[kv_t].max_txn_bytes`（默认 4MiB），超限 `INVALID_ARGUMENT`
  + `TXN_TOO_LARGE`；读集上限已在 `Read` 阶段拒绝（`READ_SET_TOO_LARGE`）。
- `Commit.request_id` 在会话 TTL 内幂等：同一 `txn_id` + `request_id` 重试
  返回首次 `commit_ts`，不重复提案；`request_id` 为空表示不做去重。
- 用户级结果（冲突/重复/非法）不得让 raft apply 返回 `Err`（D11.5）——
  sequencer 已保证，本项只做映射。
- 不新增 `TODO`/`FIXME`；注释只写当前「为什么」；临时文件只放 `/tmp`。

## Rulings

- **会话状态机而不是「提交即删会话」**：proto 承诺同一 `request_id` 重试返回
  首次 `commit_ts`；删会话只能返回 `TXN_NOT_FOUND`。`Committed` 状态在 TTL
  内保留 `request_id + commit_ts`，过期/reaper 语义不变。
- **提交开始时冻结读集**：`begin_commit` 在同一把表锁里把 `Active` 置为
  `Committing`，并返回读集快照；此后 `Read` 返回 `ABORTED` + `TXN_NOT_FOUND`
  （协议只定义了「会话不可用」这一种收口）。否则提交期间新记录的谓词不会进入
  OCC 校验，产生假阴性。
- **同一会话上并发的不同 `request_id` 拒绝**（`FAILED_PRECONDITION`）：两个
  不同写集不能同时声称提交成功；同一 `request_id` 的重试允许重新提案
  （sequencer 幂等）。
- **`Conflict`/`Invalid` 后会话回 `Active` 而不是删除**：`Read` 阶段已记录
  读集，客户端可以 `Abort`；但再 `Commit` 会重新校验并得到同样结果（除非
  改了写集）。`TXN_NOT_FOUND` 留给「会话丢失/过期」，不混用。
- **写集字节数复用 `CommitCandidate::wire_bytes()`**：与 sequencer 切批同一
  公式，避免 RPC 与内部上限两套算法。读集字节也计入（略保守，不会放行超限）。
- **`lease_id` 与 `coord.kv.v2.Put` 同规则**：非 0 时在提案前
  `require_live_lease`，死租约直接 `FAILED_PRECONDITION`。租约 key 归属由
  `KvMetadata.lease_id` 记录（`keys_by_lease` 可枚举），不在本项新增绑定表。
- **client 会话绑定协调节点**：`TxnV2Session` 复用 `begin` 选中的 channel，
  不做换节点重试（换节点必然 `TXN_NOT_FOUND`）；冲突由调用方换 `start_ts`
  重跑，wrapper 暴露 `TxnCommit::Conflict` 供上层决定重试。
- **`DELETE_RANGE` 本项 fail-closed**：`SequencerMutation` 只有 point 写（刻意
  不提供 range 字段），OCC 写索引按 key 枚举；区间删除无法在不枚举 key 的
  前提下让并发读集冲突。RPC 以 `UNIMPLEMENTED` 拒绝 `DELETE_RANGE`，不静默
  退化成单 key；range 写 + 区间写索引另立 issue（回填 #127），不在本项承诺。
- **`max_txn_bytes` 的判定用候选事务的 `wire_bytes()`**：与切批公式一致；
  超限后 `cancel_commit`，会话可 `Abort` 或改小写集重试。

## Review Focus

1. **快照后的读**：`Commit` 开始后再 `Read` 必须被拒绝，且读集快照包含此前
   所有已接受的读（含 range 谓词）。
2. **幂等重放**：响应丢失后同一 `txn_id` + `request_id` 再提交，返回同一
   `commit_ts`，决策日志/校验索引不出现第二条。
3. **冲突语义**：冲突不写决策、返回 `TXN_CONFLICT`；换新 `start_ts` 重跑后
   同一写集成功。
4. **失败释放**：写集超限、非法 mutation、sequencer 传输失败后会话不能卡在
   `Committing`（合法重试必须能继续）。
5. **上限与错误码**：`TXN_TOO_LARGE`/`READ_SET_TOO_LARGE` 的 gRPC code 与
   trailer 与 WHITEPAPER §6.1.1 一致；调用方只按码分支。
6. **关闭态**：`[kv_t].enabled=false` 时 Commit 仍 `FAILED_PRECONDITION`，
   不触碰 sequencer/会话表。
7. **记录可见性**：Commit 返回后不保证本节点立即可读；`read_ts >= commit_ts`
   在 apply 完成后可见，未 apply 时 `SAFE_TS_LAG`（D12.3）。

---

### Task 1: 会话提交状态机与读集快照

**Files:**
- Modify: `coord-server/src/kvt/txn.rs`

**Interfaces:**
- Produces:
  - `enum SessionError { NotFound, Closed, Committed, CommitInProgress, ReadSetLimit(..), Draining }`
  - `struct CommitInput { start_ts: u64, read_set: ReadSet, replay: Option<u64> }`
  - `TxnSessions::begin_commit(&self, id: &[u8], request_id: &[u8]) -> Result<CommitInput, SessionError>`
  - `TxnSessions::finish_commit(&self, id: &[u8], request_id: &[u8], commit_ts: u64)`
  - `TxnSessions::cancel_commit(&self, id: &[u8], request_id: &[u8])`
  - `ReadSet::into_parts(self) -> (BTreeSet<Vec<u8>>, Vec<ReadRange>)`
- Consumes: 既有 `TxnTable`/`sweep`/`ReadSet`。

- [ ] **Step 1: 写失败测试（`txn.rs` tests 模块）**

```rust
#[test]
fn commit_freezes_read_set_and_replays_same_request_id() {
    let sessions = TxnSessions::new();
    let id = sessions.begin(7, Duration::from_secs(30));
    sessions
        .record_read(&id, ReadIntent::Point(b"a"), 10)
        .unwrap();

    let first = sessions.begin_commit(&id, b"req-1").unwrap();
    assert_eq!(first.start_ts, 7);
    assert!(first.replay.is_none());
    assert_eq!(first.read_set.len(), 1);

    // 提交开始后读被拒绝，且不能把新谓词塞进已经冻结的读集。
    assert_eq!(
        sessions.record_read(&id, ReadIntent::Point(b"b"), 10),
        Err(SessionError::Closed)
    );
    // 同一 request_id 重试允许重新提案（sequencer 幂等）。
    assert!(sessions.begin_commit(&id, b"req-1").unwrap().replay.is_none());
    // 不同 request_id 并发提交被拒绝。
    assert_eq!(
        sessions.begin_commit(&id, b"req-2"),
        Err(SessionError::CommitInProgress)
    );

    sessions.finish_commit(&id, b"req-1", 100);
    assert_eq!(
        sessions.begin_commit(&id, b"req-1").unwrap().replay,
        Some(100)
    );
    assert_eq!(
        sessions.begin_commit(&id, b"req-2"),
        Err(SessionError::Committed)
    );
    // 已提交会话不接受新读，abort 也不得丢掉幂等记录。
    assert_eq!(
        sessions.record_read(&id, ReadIntent::Point(b"c"), 10),
        Err(SessionError::Closed)
    );
    sessions.abort(&id);
    assert_eq!(
        sessions.begin_commit(&id, b"req-1").unwrap().replay,
        Some(100)
    );
}

#[test]
fn cancel_commit_restores_active_and_abort_drops_it() {
    let sessions = TxnSessions::new();
    let id = sessions.begin(9, Duration::from_secs(30));
    sessions.begin_commit(&id, b"req").unwrap();
    sessions.cancel_commit(&id, b"req");

    assert!(sessions
        .record_read(&id, ReadIntent::Point(b"k"), 10)
        .is_ok());

    sessions.begin_commit(&id, b"req").unwrap();
    sessions.abort(&id);
    assert_eq!(
        sessions.begin_commit(&id, b"req"),
        Err(SessionError::NotFound)
    );
}
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p coord-server --lib kvt::txn::tests::commit_`
Expected: FAIL — `begin_commit`/`finish_commit`/`cancel_commit`/`SessionError::Closed` 不存在。

- [ ] **Step 3: 实现**

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
enum SessionState {
    Active,
    Committing { request_id: Vec<u8> },
    Committed { request_id: Vec<u8>, commit_ts: u64 },
}

struct TxnSession {
    start_ts: u64,
    expires_at: Instant,
    read_set: ReadSet,
    state: SessionState,
}

pub(crate) struct CommitInput {
    pub(crate) start_ts: u64,
    pub(crate) read_set: ReadSet,
    /// `Some` = 同一 `request_id` 已提交：直接回放，不再提案。
    pub(crate) replay: Option<u64>,
}
```

- `begin` 初始化 `state: SessionState::Active`。
- `record_read`：过期/缺失 → `NotFound`；`state != Active` → `Closed`；
  其余不变。
- `begin_commit`：

```rust
let now = Instant::now();
let mut table = self.table.lock();
if table.sessions.get(id).is_some_and(|s| s.expires_at <= now) {
    table.sessions.remove(id);
}
let session = table.sessions.get_mut(id).ok_or(SessionError::NotFound)?;
match &session.state {
    SessionState::Committed { request_id, commit_ts } if request_id == request_id_param => {
        Ok(CommitInput {
            start_ts: session.start_ts,
            read_set: session.read_set.clone(),
            replay: Some(*commit_ts),
        })
    }
    SessionState::Committed { .. } => Err(SessionError::Committed),
    SessionState::Committing { request_id: claimed } if claimed != request_id_param => {
        Err(SessionError::CommitInProgress)
    }
    SessionState::Committing { .. } => Ok(CommitInput {
        start_ts: session.start_ts,
        read_set: session.read_set.clone(),
        replay: None,
    }),
    SessionState::Active => {
        session.state = SessionState::Committing { request_id: request_id_param.to_vec() };
        Ok(CommitInput { start_ts: session.start_ts, read_set: session.read_set.clone(), replay: None })
    }
}
```

- `finish_commit`：仅在 `Committing` 且 `request_id` 匹配时置
  `Committed { request_id, commit_ts }`。
- `cancel_commit`：仅在 `Committing` 且 `request_id` 匹配时回到 `Active`。
- `abort`：只删除 `Active` / `Committing` 会话；`Committed` 保留（幂等回放）。
- `ReadSet::into_parts`：`points` 与 `ranges` + `empty_ranges` 合并后返回。

- [ ] **Step 4: 运行确认通过**

Run: `cargo test -p coord-server --lib kvt::txn::`
Expected: PASS（既有会话/drain 测试 + 新增 2 个）。

- [ ] **Step 5: 提交**

```bash
git add coord-server/src/kvt/txn.rs
git commit -m "feat(kv-t): freeze txn read set on commit and replay by request id (#151)"
```

---

### Task 2: `Commit` RPC 接线与错误码映射

**Files:**
- Modify: `coord-server/src/kvt/services.rs`
- Modify: `coord-server/src/kvt/mod.rs`（模块文档：Txn 全 RPC 已交付）

**Interfaces:**
- Consumes: Task 1 的 `begin_commit`/`finish_commit`/`cancel_commit`、
  `CommitSequencer::validate_and_commit(Vec<CommitCandidate>)`、
  `SequencerOutcome::{Committed,Duplicate,Conflict,Invalid,OutOfOrder}`。
- Produces: `Txn.Commit` 的可用实现；`fn validate_commit_mutations(&[txn_v2::Mutation]) -> Result<Vec<SequencerMutation>, Status>`；
  `txn_session_status` 新增 `Closed`/`Committed`/`CommitInProgress` 分支。

- [ ] **Step 1: 写失败测试（`services.rs` tests 模块）**

```rust
#[test]
fn commit_mutations_validate_ops_and_ranges() {
    use txn_v2::{mutation::Op, Mutation};

    let put = |op, key: &[u8], end: &[u8]| Mutation {
        op: op as i32,
        key: key.to_vec(),
        range_end: end.to_vec(),
        value: b"v".to_vec(),
        lease_id: 0,
    };
    assert!(validate_commit_mutations(&[put(Op::Put, b"k", b"")]).is_ok());
    assert!(validate_commit_mutations(&[put(Op::Delete, b"k", b"")]).is_ok());
    assert!(validate_commit_mutations(&[put(Op::DeleteRange, b"a", b"b")]).is_ok());

    for bad in [
        put(Op::Unspecified, b"k", b""),
        put(Op::Put, b"k", b"z"),          // PUT 必须是单 key
        put(Op::Delete, b"k", b"z"),       // DELETE 必须是单 key
        put(Op::DeleteRange, b"a", b""),   // 必须给上界
        put(Op::DeleteRange, b"b", b"a"),  // 空区间
    ] {
        let err = validate_commit_mutations(&[bad]).expect_err("invalid mutation");
        assert_eq!(err.code(), Code::InvalidArgument, "{err:?}");
        assert_eq!(error_code_of(&err).as_deref(), Some("INVALID_ARGUMENT"));
    }
}

#[test]
fn session_errors_map_to_protocol_codes() {
    let not_found = txn_session_status(SessionError::NotFound);
    assert_eq!(not_found.code(), Code::Aborted);
    assert_eq!(error_code_of(&not_found).as_deref(), Some("TXN_NOT_FOUND"));

    let closed = txn_session_status(SessionError::Closed);
    assert_eq!(closed.code(), Code::Aborted);
    assert_eq!(error_code_of(&closed).as_deref(), Some("TXN_NOT_FOUND"));

    for err in [SessionError::Committed, SessionError::CommitInProgress] {
        let status = txn_session_status(err);
        assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
        assert_eq!(
            error_code_of(&status).as_deref(),
            Some("FAILED_PRECONDITION")
        );
    }
}
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p coord-server --lib kvt::services::tests::commit_`
Expected: FAIL — `validate_commit_mutations`/新 `SessionError` 分支不存在。

- [ ] **Step 3: 实现**

`txn_session_status` 新增：

```rust
SessionError::Closed => attach(
    Status::aborted("transaction is committing or committed; start a new transaction"),
    CoordErrorCode::TxnNotFound,
),
SessionError::Committed => attach(
    Status::failed_precondition(
        "transaction already committed with a different request_id; start a new transaction",
    ),
    CoordErrorCode::FailedPrecondition,
),
SessionError::CommitInProgress => attach(
    Status::failed_precondition("transaction commit already in progress with another request_id"),
    CoordErrorCode::FailedPrecondition,
),
```

`validate_commit_mutations`（放在 `txn_session_status` 上方）：

```rust
fn validate_commit_mutations(
    mutations: &[txn_v2::Mutation],
) -> Result<Vec<SequencerMutation>, Status> {
    use txn_v2::mutation::Op;
    let mut out = Vec::with_capacity(mutations.len());
    for mutation in mutations {
        if mutation.lease_id < 0 {
            return Err(attach(Status::invalid_argument("lease_id must be >= 0"), CoordErrorCode::InvalidArgument));
        }
        out.push(match mutation.op() {
            Op::Put if mutation.range_end.is_empty() => SequencerMutation::Put {
                key: mutation.key.clone(),
                value: mutation.value.clone(),
                lease_id: (mutation.lease_id != 0).then_some(mutation.lease_id),
            },
            Op::Delete if mutation.range_end.is_empty() => SequencerMutation::Delete {
                key: mutation.key.clone(),
            },
            Op::DeleteRange => {
                // 区间写索引尚未交付（见 Rulings）：fail-closed，不静默退化成
                // 单 key 删除，也不接受会被忽略的 range_end。
                return Err(attach(
                    Status::unimplemented(
                        "txn DELETE_RANGE is not implemented; delete keys individually",
                    ),
                    CoordErrorCode::InvalidArgument,
                ));
            }
            Op::Put | Op::Delete | Op::Unspecified => {
                return Err(attach(
                    Status::invalid_argument("invalid mutation: check op, range_end and key order"),
                    CoordErrorCode::InvalidArgument,
                ))
            }
        });
    }
    Ok(out)
}
```

`commit` handler：

```rust
async fn commit(
    &self,
    request: Request<txn_v2::CommitRequest>,
) -> Result<Response<txn_v2::CommitResponse>, Status> {
    self.kvt().gate(KvTCapability::Txn)?;
    let req = request.into_inner();
    if req.txn_id.is_empty() {
        return Err(attach(
            Status::invalid_argument("txn_id must not be empty"),
            CoordErrorCode::InvalidArgument,
        ));
    }
    let mutations = validate_commit_mutations(&req.mutations)?;
    for mutation in &mutations {
        match mutation {
            SequencerMutation::Put { lease_id: Some(id), .. } => {
                lease_rpc::require_live_lease(self, *id)?;
            }
            _ => {}
        }
    }

    let input = self
        .txn_sessions()
        .begin_commit(&req.txn_id, &req.request_id)
        .map_err(txn_session_status)?;
    if let Some(commit_ts) = input.replay {
        return Ok(Response::new(txn_v2::CommitResponse {
            commit_ts: ts_to_i64(commit_ts)?,
        }));
    }

    let release = |status: Status| {
        self.txn_sessions()
            .cancel_commit(&req.txn_id, &req.request_id);
        status
    };
    let Some(sequencer) = self.commit_sequencer() else {
        return Err(release(attach(
            Status::failed_precondition(
                "txn Commit requires the region0 Commit Sequencer (local assembly missing)",
            ),
            CoordErrorCode::FailedPrecondition,
        )));
    };

    let (read_set, read_ranges) = input.read_set.into_parts();
    let candidate = CommitCandidate {
        txn_id: req.txn_id.clone(),
        start_ts: input.start_ts,
        request_id: req.request_id.clone(),
        read_set,
        read_ranges: read_ranges
            .into_iter()
            .map(|range| SequencerReadRange {
                start: range.start,
                end: range.end,
            })
            .collect(),
        mutations,
    };
    let limit = self.kvt().config().max_txn_bytes;
    if candidate.wire_bytes() as u64 > limit {
        return Err(release(attach(
            Status::invalid_argument(format!(
                "transaction write set exceeds max_txn_bytes ({limit}); split the transaction"
            )),
            CoordErrorCode::TxnTooLarge,
        )));
    }

    let outcome = match sequencer.validate_and_commit(vec![candidate]).await {
        Ok(outcomes) => outcomes.into_iter().next(),
        Err(err) => return Err(release(v2_status(err))),
    };
    match outcome {
        Some(SequencerOutcome::Committed { commit_ts })
        | Some(SequencerOutcome::Duplicate { commit_ts }) => {
            self.txn_sessions()
                .finish_commit(&req.txn_id, &req.request_id, commit_ts);
            Ok(Response::new(txn_v2::CommitResponse {
                commit_ts: ts_to_i64(commit_ts)?,
            }))
        }
        Some(SequencerOutcome::Conflict { commit_ts, .. }) => {
            let status = attach(
                Status::aborted(format!(
                    "transaction conflict at commit_ts {commit_ts}; restart with a newer start_ts"
                )),
                CoordErrorCode::TxnConflict,
            );
            Err(release(status))
        }
        Some(SequencerOutcome::Invalid { reason, .. }) => {
            let status = match reason {
                SequencerInvalid::ReadSetTooLarge | SequencerInvalid::RangeScanExceeded => attach(
                    Status::invalid_argument("transaction read set exceeds the server limit"),
                    CoordErrorCode::ReadSetTooLarge,
                ),
                other => attach(
                    Status::invalid_argument(format!("transaction rejected: {other:?}")),
                    CoordErrorCode::InvalidArgument,
                ),
            };
            Err(release(status))
        }
        Some(SequencerOutcome::OutOfOrder { commit_ts, last_commit_ts }) => Err(release(
            Status::internal(format!(
                "commit_ts {commit_ts} is not above the sequencer watermark {last_commit_ts}"
            )),
        )),
        None => Err(release(Status::internal(
            "sequencer returned no outcome for the transaction",
        ))),
    }
}
```

> 会话在 `Conflict`/`Invalid` 上回到 `Active`（`cancel_commit`），在
> `Committed`/`Duplicate` 上 `finish_commit`；传输错误同样 `cancel_commit`。

- [ ] **Step 4: 运行确认通过**

Run: `cargo test -p coord-server --lib kvt::`
Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add coord-server/src/kvt/services.rs coord-server/src/kvt/mod.rs
git commit -m "feat(kv-t): wire txn Commit to the commit sequencer (#151)"
```

---

### Task 3: e2e 验收（提交、冲突、幂等、上限）

**Files:**
- Create: `coord/tests/kv_t_g3_txn_commit_test.rs`

**Interfaces:**
- Consumes: Task 1/2 的 RPC、`decision_apply::apply_pending_decisions`、
  `CommitSequencer`（夹具复用 `coord/tests/kv_t_g3_commit_decision_test.rs` 的
  region 0 + 两个数据 Region 装配，`FakeTso` 高水位从 1000 起）。
- Produces: 验收测试，PR 描述与 issue 证据引用的测试名。

- [ ] **Step 1: 写失败测试**

夹具要点：

- `FakeTso.high_water = 1000`；两个 Region 各写一个 seed key（`PutV2` 直接
  提交到 region raft，`commit_ts = 1000`），使 `node.safe_ts() == 1000`。
- `begin(start_ts)` 直接调 `TxnV2::begin`（不经过 gRPC），`commit`/`read`/
  `abort` 同理；`commit_ts` 由 sequencer 从 TSO 连续取号。
- 读可见性：`apply_pending_decisions(&node, 16).await` 后再用
  `KvV2::range(read_ts = commit_ts)` 断言。

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_txn_commit_is_visible_and_replays_same_request_id() {
    let fx = TxnCommitNode::start().await;
    let a = fx.begin(1000).await;

    let read = fx.read(&a.txn_id, b"apple", b"", 0, false).await.unwrap();
    assert_eq!(read.kvs[0].value, b"seed");

    let commit_ts = fx.commit(&a.txn_id, &[(b"apple", b"v1")], b"req-1").await.unwrap();
    assert_eq!(commit_ts, 1001);
    // 同一 request_id 重放：同一 commit_ts，决策只有一条。
    assert_eq!(
        fx.commit(&a.txn_id, &[(b"apple", b"v1")], b"req-1").await.unwrap(),
        commit_ts
    );
    assert!(fx.node.storage.get_sequencer_decision(commit_ts).unwrap().is_some());

    // 不同 request_id = 已提交，不得静默接受另一份写集。
    let err = fx
        .commit(&a.txn_id, &[(b"apple", b"v2")], b"req-2")
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");

    // 提交后 Read 不再可用。
    let err = fx.read(&a.txn_id, b"apple", b"", 0, false).await.unwrap_err();
    assert_eq!(err.code(), Code::Aborted, "{err:?}");
    assert_eq!(error_code_of(&err).as_deref(), Some("TXN_NOT_FOUND"));

    apply_pending_decisions(&fx.node, 16).await.unwrap();
    assert_eq!(fx.node.safe_ts(), commit_ts);
    let range = fx.range(b"apple", b"", commit_ts).await;
    assert_eq!(range.kvs[0].value, b"v1");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_txn_conflict_then_retry_with_new_start_ts() {
    let fx = TxnCommitNode::start().await;
    let a = fx.begin(1000).await;
    fx.read(&a.txn_id, b"apple", b"", 0, false).await.unwrap();

    let b = fx.begin(1000).await;
    let b_ts = fx.commit(&b.txn_id, &[(b"apple", b"from-b")], b"req-b").await.unwrap();
    assert!(b_ts > 1000);

    let err = fx.commit(&a.txn_id, &[(b"apple", b"from-a")], b"req-a").await.unwrap_err();
    assert_eq!(err.code(), Code::Aborted, "{err:?}");
    assert_eq!(error_code_of(&err).as_deref(), Some("TXN_CONFLICT"));
    // 冲突不写决策：A 分配的 commit_ts 在日志里不存在。
    assert!(fx.node.storage.get_sequencer_decision(b_ts + 1).unwrap().is_none());
    // 会话已不可继续提交。
    let err = fx.commit(&a.txn_id, &[(b"apple", b"from-a")], b"req-a").await.unwrap_err();
    assert_eq!(error_code_of(&err).as_deref(), Some("TXN_NOT_FOUND"));

    // 应用 B 后，用 B 的 commit_ts 作为新 start_ts 重跑成功。
    apply_pending_decisions(&fx.node, 16).await.unwrap();
    let retry = fx.begin(b_ts).await;
    let read = fx.read(&retry.txn_id, b"apple", b"", 0, false).await.unwrap();
    assert_eq!(read.kvs[0].value, b"from-b");
    let ts = fx.commit(&retry.txn_id, &[(b"apple", b"from-a")], b"req-a2").await.unwrap();
    assert!(ts > b_ts);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_txn_commit_limits_and_invalid_mutations_keep_session_usable() {
    let fx = TxnCommitNode::start_with_max_txn_bytes(256).await;
    let txn = fx.begin(1000).await;

    let err = fx
        .commit(&txn.txn_id, &[(b"big", &[0u8; 512])], b"req-big")
        .await
        .unwrap_err();
    assert_eq!(error_code_of(&err).as_deref(), Some("TXN_TOO_LARGE"));

    let err = fx.commit_raw(&txn.txn_id, vec![Mutation { op: 0, ..Default::default() }], b"req-bad")
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err:?}");

    // 失败都释放了提交占用：同一会话可以继续读并成功提交。
    fx.read(&txn.txn_id, b"apple", b"", 0, false).await.unwrap();
    let ts = fx.commit(&txn.txn_id, &[(b"apple", b"ok")], b"req-ok").await.unwrap();
    assert!(ts > 1000);

    let err = fx.commit(&b"missing".to_vec(), &[], b"req-x").await.unwrap_err();
    assert_eq!(error_code_of(&err).as_deref(), Some("TXN_NOT_FOUND"));
}
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p coord --test kv_t_g3_txn_commit_test`
Expected: FAIL — `Commit` 返回 `UNIMPLEMENTED`（以及测试文件不存在）。

- [ ] **Step 3: 实现夹具并跑通**

- 夹具从 `kv_t_g3_commit_decision_test.rs` 复制 region 0 + 数据 Region 装配，
  增加 `set_commit_sequencer` 与 seed；`begin`/`read`/`commit`/`abort` 调
  `txn_v2::txn_server::Txn`。
- 断言错误码用 `coord_core::error_code::error_code_of`。
- `commit` 辅助函数把 `(key, value)` 转成 `Mutation { op: Put, .. }`。

- [ ] **Step 4: 运行确认通过**

Run: `cargo test -p coord --test kv_t_g3_txn_commit_test`
Expected: PASS 3/3。

- [ ] **Step 5: 提交**

```bash
git add coord/tests/kv_t_g3_txn_commit_test.rs
git commit -m "test(kv-t): cover txn commit visibility, conflict and replay (#151)"
```

---

### Task 4: Rust client v2 Txn 包装与文档收口

**Files:**
- Create: `coord-client/src/txn_v2.rs`
- Modify: `coord-client/src/lib.rs`、`coord-client/src/client.rs`
- Modify: `coord/src/main.rs`（启动日志：Commit 已可用）
- Modify: `docs/production/consumer-contract.md`、`docs/production/ops/boundaries.md`

**Interfaces:**
- Produces:
  - `Client::txn_v2(&self) -> TxnV2Client`
  - `TxnV2Client::begin(&self, start_ts: u64, ttl_ms: u32) -> Result<TxnV2Session>`
  - `TxnV2Session::{txn_id, start_ts, read, commit, abort}`
  - `enum TxnCommit { Committed { commit_ts: u64 }, Conflict }`
- Consumes: `Client::get_leader_channel/return_channel`、`coord_proto::txn::v2`、
  `error_code_of`。

- [ ] **Step 1: 写失败测试（`txn_v2.rs` tests 模块）**

```rust
#[test]
fn txn_status_maps_protocol_codes() {
    let conflict = attach(Status::aborted("conflict"), CoordErrorCode::TxnConflict);
    assert!(matches!(
        txn_commit_error(conflict).unwrap_err(),
        Error::TxnConflictStub
    ));
    let missing = attach(Status::aborted("missing"), CoordErrorCode::TxnNotFound);
    assert!(matches!(
        txn_error(missing),
        Error::NotFound { resource: "transaction", .. }
    ));
}
```
（实现时把 `TxnConflictStub` 换成实际决策：`commit` 把 `TXN_CONFLICT` 映射为
`Ok(TxnCommit::Conflict)`，因此测试直接断言 `is_txn_conflict(&status)` 与
`txn_error` 的 `NotFound` 映射。）

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p coord-client --lib txn_v2::`
Expected: FAIL — 模块/函数不存在。

- [ ] **Step 3: 实现**

```rust
pub struct TxnV2Client { client: Client }

impl TxnV2Client {
    pub async fn begin(&self, start_ts: u64, ttl_ms: u32) -> Result<TxnV2Session> {
        let (endpoint, channel) = self.client.get_leader_channel().await?;
        let mut stub = txn_v2::txn_client::TxnClient::new(channel.clone());
        let resp = stub
            .begin(tonic::Request::new(txn_v2::BeginRequest {
                start_ts: ts_i64("start_ts", start_ts)?,
                ttl_ms,
            }))
            .await
            .map_err(txn_error);
        match resp {
            Ok(resp) => {
                let inner = resp.into_inner();
                let start_ts = u64::try_from(inner.start_ts)
                    .map_err(|_| Error::Internal("negative start_ts in response".into()))?;
                Ok(TxnV2Session { client: self.client.clone(), endpoint, channel: Some(channel), txn_id: inner.txn_id, start_ts })
            }
            Err(err) => {
                self.client.return_channel(&endpoint, channel);
                Err(err)
            }
        }
    }
}
```

- `TxnV2Session::read`：同一个 pinned channel；`read_ts` 回显 `start_ts`。
- `TxnV2Session::commit`：`TXN_CONFLICT` → `Ok(TxnCommit::Conflict)`，其余
  `txn_error`；成功返回 `Committed { commit_ts }`。
- `TxnV2Session::abort`：幂等；`Drop` 把 channel 归还连接池。
- `txn_error(status)`：先用 `error_code_of` 分流 `TXN_NOT_FOUND` →
  `Error::NotFound { resource: "transaction", .. }`、`READ_SET_TOO_LARGE`/
  `TXN_TOO_LARGE` → `Error::InvalidArgument`，其余回落到 `from_status`。
- 文档注释写明：会话绑定协调节点，冲突后必须换新 `start_ts` 重跑
  （`TxnCommit::Conflict` 不自动重试，避免隐藏重复读）。

文档改动：

- `coord/src/main.rs` 启动 warning 改为：v2 TSO/KV/Txn（含 Commit）可用，
  Watch/Lease/Storage 仍 `UNIMPLEMENTED` 待各自 gate。
- `docs/production/consumer-contract.md` 事务 bullet 补：`Commit` 幂等
  （`request_id`）、4MiB 写集上限、冲突换 `start_ts` 重跑、提交后不等待 apply。
- `docs/production/ops/boundaries.md` B-KVT-9 的「盲写」表述改为：drain 只等待
  读集相交的会话；提交路径不因 drain 拒绝写，写侧路由由 #163 cutover 收口。

- [ ] **Step 4: 运行确认通过**

Run: `cargo test -p coord-client --lib txn_v2:: && bash scripts/check-doc-refs.sh`
Expected: PASS；文档引用悬空 = 0。

- [ ] **Step 5: 提交**

```bash
git add coord-client/src/txn_v2.rs coord-client/src/lib.rs coord-client/src/client.rs \
  coord/src/main.rs docs/production/consumer-contract.md docs/production/ops/boundaries.md
git commit -m "feat(client): add v2 txn session wrapper and document commit semantics (#151)"
```

---

### Task 5: 门禁、PR 与 issue 收口

- [ ] **Step 1: 本地门禁（AGENTS.md）**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
bash scripts/check-panics.sh
cargo test -p coord-server --lib kvt::
cargo test -p coord --test kv_t_g3_txn_commit_test
cargo test -p coord-client --lib txn_v2::
bash scripts/check-doc-refs.sh
```

- [ ] **Step 2: 推送并开 PR**

```bash
git push -u origin feat/kv-t-g3-txn-commit
gh pr create --base main --title "feat(kv-t): txn v2 Commit with idempotent retry (#151)" --body "..."
```

- [ ] **Step 3: CI 全绿后合并**

`gh pr checks --watch` → `gh pr merge --merge --delete-branch`（coord 用 `--merge`）。

- [ ] **Step 4: 关闭 #151 的证据评论**

评论列出：测试名（`test_txn_commit_is_visible_and_replays_same_request_id`、
`test_txn_conflict_then_retry_with_new_start_ts`、
`test_txn_commit_limits_and_invalid_mutations_keep_session_usable`）、
错误码映射（TXN_CONFLICT/TXN_NOT_FOUND/TXN_TOO_LARGE/READ_SET_TOO_LARGE）、
client wrapper 入口（`Client::txn_v2`）、以及未覆盖项（gRPC 全链路 client e2e
归 #172 SDK GA）。
