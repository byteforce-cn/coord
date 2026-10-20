# KV-T G4 CommitWatermark 与 Shard Seal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 发布一个可持久化、单调的 CommitWatermark（`min(in-flight commit_ts) - 1`，无 in-flight = max_committed），并用它把已完成的 Commit Log shard seal；停滞超过 5 分钟时告警并走既有封顶路径，不无限阻塞。

**Architecture:** region 0 system raft 仍是唯一权威：每轮 shard tick 由 region 0 leader 用本节点视图计算 watermark（决策日志的最后 `commit_ts` + 各数据 Region 的 apply 游标 + 各 shard 对每个数据 Region 的已 ship 水位），经 `Command::PublishCommitWatermark` 单调持久化到 region 0 MVCC；`seal` 的主路径改为 `watermark >= shard.end_ts`，seal 超时保留为停滞兜底。读取（#156）用 region 0 的已发布值；GC/保留窗口归 #157。

**Tech Stack:** Rust, openraft 0.10.0-alpha.34（经 `coord-server/src/raft` 隔离层）, redb, postcard, tokio。

**Spec:** `docs/adr/0015-kv-t.md` D5/D9/D11；issue #155（前置 #148、#154）。

## Global Constraints

- `[kv_t].enabled=false`（默认）时不得改变 v1 行为；新代码只在 v2 装配路径可达（ADR-0001）。
- 内部水位命令不得写 changelog、不得产生 Watch 事件；重放幂等（同一 applied 返回当前值）。
- `commit_ts` 全局唯一单调（D11.1）；watermark 只前进，绝不回退，重启后不得归零。
- 默认阈值逐字取自 D9：`shard_seal_timeout_secs = 300` 是停滞兜底；seal 主条件 `watermark >= shard.end_ts`。
- 用户级/元数据级拒绝不得让 Raft apply 返回 `Err`（D11.5）。
- 注释只写当前「为什么」（不变量/陷阱/契约）；不写日期、变更史或一次性任务号；不新增 TODO/FIXME。
- 临时文件只放 `/tmp`。
- 提交前本机必须跑通 `cargo fmt --all -- --check`、`cargo clippy --workspace --lib --bins -- -D warnings`、`bash scripts/check-panics.sh`、`cargo audit`、`cargo deny check`；涉及文档跑 `bash scripts/check-doc-refs.sh`。

## Review Focus

- **决策 ts 空洞**：watermark 不能只用 `min(last_applied)` 推，否则「区域 A 已 apply 20、区域 B 停在 10、决策只有 10/20」会把 watermark 卡在 9；必须按「每个 Region 的下一条待 apply 决策 ts」取最小，再减一。
- **跨 Region 乱序 ship**：shard 的 `cursor`（最大已追加 ts）不等于「前缀完整」；某个 Region 有未 ship 的 15 而另一个 Region 已 ship 100 时，watermark 必须停在 14。
- **停滞不可无限**：数据 Region 停止 ship 时 watermark 会钉住；超时后必须可观测（计数器 + warning）并 seal，且不得让 tick 报错卡住其它 shard。
- **seal 后迟到记录不丢**：超时 seal 的 shard 仍接受区间内的迟到记录（既不丢弃也不改写），否则 Watch 会缺事件。
- **重启/换 leader**：已发布 watermark 从 region 0 恢复后不得回退；新 leader 用更低本地视图只能「不推进」，不能把已发布值调低。

---

### Task 1: Region 0 持久化 CommitWatermark

**Files:**
- Modify: `coord-server/src/storage/mvcc.rs`（`SAFE_TS_FENCE` 附近加 key；`safe_ts_fence` 之后加读写 API；`mod tests` 加测试）

**Interfaces:**
- Produces:
  - `pub fn commit_watermark(&self) -> Result<u64>`（缺失 = 0）
  - `pub fn publish_commit_watermark(&self, watermark: u64, applied: AppliedLogId) -> Result<u64>`（只前进；同一 applied 重放返回当前值；不写 changelog）
- Consumes: `Self::applied_contains(tx, applied)`（同 `set_safe_ts_fence`）

- [x] **Step 1: 写失败测试**

在 `mvcc.rs` 的 `mod tests`（`test_safe_ts_fence_is_monotonic_and_persistent` 之后）加：

```rust
#[test]
fn test_commit_watermark_is_monotonic_persistent_and_replay_safe() {
    let (dir, storage) = create_storage();
    assert_eq!(storage.commit_watermark().unwrap(), 0);

    assert_eq!(
        storage
            .publish_commit_watermark(900, applied_for(1))
            .unwrap(),
        900
    );
    assert_eq!(
        storage
            .publish_commit_watermark(800, applied_for(2))
            .unwrap(),
        900,
        "watermark 只前进"
    );
    assert_eq!(storage.commit_watermark().unwrap(), 900);

    // 同一 applied 重放：返回当前值，不改写。
    assert_eq!(
        storage
            .publish_commit_watermark(950, applied_for(2))
            .unwrap(),
        900
    );

    drop(storage);
    let backend = RedbBackend::open(dir.path(), &StorageConfig::default()).unwrap();
    let reopened = MvccStorage::new(backend).unwrap();
    assert_eq!(reopened.commit_watermark().unwrap(), 900);
}
```

- [x] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server --lib test_commit_watermark_is_monotonic_persistent_and_replay_safe`
Expected: FAIL（`no method named commit_watermark`）

- [x] **Step 3: 实现存储 API**

在 `const SAFE_TS_FENCE` 旁加：

```rust
const COMMIT_WATERMARK: &[u8] = b"/_sys/commit_log/watermark";
```

在 `set_safe_ts_fence` 之后加（结构照抄 `set_safe_ts_fence` 的 applied 守卫与持久化）：

```rust
/// 读取已发布的 CommitWatermark（缺失 = 0）。
pub fn commit_watermark(&self) -> Result<u64> {
    self.backend.read(|tx| {
        Ok(tx
            .get(TABLE_KV, COMMIT_WATERMARK)?
            .and_then(|bytes| decode_u64(&bytes))
            .unwrap_or(0))
    })
}

/// 发布 CommitWatermark（region 0 system raft，#155）。
///
/// 只前进：watermark 是「<= 它的 commit_ts 不会再有新记录」的承诺，回退会让
/// Watch 重新暴露已跳过的时间段。同一 applied 重放返回当前值。
pub fn publish_commit_watermark(
    &self,
    watermark: u64,
    applied: AppliedLogId,
) -> Result<u64> {
    self.backend.write(|tx| {
        let prev = tx
            .get(TABLE_KV, COMMIT_WATERMARK)?
            .and_then(|bytes| decode_u64(&bytes))
            .unwrap_or(0);
        if Self::applied_contains(tx, applied)? {
            return Ok(prev);
        }
        let effective = prev.max(watermark);
        if effective > prev {
            tx.insert(TABLE_KV, COMMIT_WATERMARK, &effective.to_be_bytes())?;
        }
        tx.insert(TABLE_META, META_LAST_APPLIED, &applied.to_bytes())?;
        Ok(effective)
    })
}
```

- [x] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server --lib test_commit_watermark_is_monotonic_persistent_and_replay_safe`
Expected: PASS

- [x] **Step 5: 提交**

```bash
git add coord-server/src/storage/mvcc.rs
git commit -m "feat(kv-t): persist the commit watermark in region 0 (#155)"
```

---

### Task 2: Raft 命令 `PublishCommitWatermark`

**Files:**
- Modify: `coord-server/src/raft/type_config.rs`（`Command`/`Response` 枚举 + `Display`）
- Modify: `coord-server/src/raft/state_machine.rs`（apply 分支 + 测试）

**Interfaces:**
- Produces:
  - `Command::PublishCommitWatermark { watermark: u64 }`（末尾追加）
  - `Response::CommitWatermarkPublished { watermark: u64 }`（实际生效值）
- Consumes: Task 1 的 `MvccStorage::publish_commit_watermark`

- [x] **Step 1: 写失败测试**

在 `state_machine.rs` 的 `test_apply_safe_ts_fence_and_watermark` 之后加：

```rust
#[test]
fn test_apply_publish_commit_watermark_is_monotonic_and_eventless() {
    let (_tmp, store) = setup_store_with_mode(RevisionMode::CommitTs);

    let (resp, event) = store
        .execute_command(
            store.state_machine.as_ref(),
            &Command::PublishCommitWatermark { watermark: 2_000 },
            5,
            applied_at(5),
        )
        .expect("publish commit watermark apply");
    match resp {
        Response::CommitWatermarkPublished { watermark } => assert_eq!(watermark, 2_000),
        other => panic!("unexpected response: {other:?}"),
    }
    assert!(event.is_none(), "watermark 不得产生 Watch 事件");
    assert_eq!(store.state_machine.commit_watermark().unwrap(), 2_000);

    // 同一 applied 重放：不报错、不倒退。
    let (resp, _) = store
        .execute_command(
            store.state_machine.as_ref(),
            &Command::PublishCommitWatermark { watermark: 1_000 },
            5,
            applied_at(5),
        )
        .expect("publish replay must not fail raft");
    match resp {
        Response::CommitWatermarkPublished { watermark } => assert_eq!(watermark, 2_000),
        other => panic!("unexpected response: {other:?}"),
    }
}
```

- [x] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server --lib test_apply_publish_commit_watermark_is_monotonic_and_eventless`
Expected: FAIL（`no variant named PublishCommitWatermark`）

- [x] **Step 3: 加命令/响应 + apply 分支**

`type_config.rs`：在 `PublishSafeTs` 后**末尾追加**（不得改动已有变体的字段/顺序，postcard 日志不是自描述格式）：

```rust
/// 发布 CommitWatermark（仅 region 0 system raft 提出，#155）。
///
/// **末尾追加**；`watermark` 是「<= 它的 commit_ts 不会再有新 CommitRecord」的
/// 单调承诺，apply 不推进 revision、不写 changelog。
PublishCommitWatermark { watermark: u64 },
```

`Response` 在 `SafeTsPublished` 后加：

```rust
/// CommitWatermark 发布结果（#155；实际生效值）。
CommitWatermarkPublished {
    watermark: u64,
},
```

两处 `Display` 各加一行 `write!`（命令 `PublishCommitWatermark(watermark={watermark})`，响应 `CommitWatermarkPublished(watermark={watermark})`）。

`state_machine.rs` 的 `Command::PublishSafeTs` 分支之后加：

```rust
Command::PublishCommitWatermark { watermark } => {
    // #155：与 #142 的 fence 相同——内部水位不上 Watch，重放由
    // applied-index 守卫返回当前值。
    let effective = sm
        .publish_commit_watermark(*watermark, applied)
        .map_err(io_err)?;
    Ok((
        Response::CommitWatermarkPublished {
            watermark: effective,
        },
        None,
    ))
}
```

- [x] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server --lib test_apply_publish_commit_watermark_is_monotonic_and_eventless`
Expected: PASS

- [x] **Step 5: 提交**

```bash
git add coord-server/src/raft/type_config.rs coord-server/src/raft/state_machine.rs
git commit -m "feat(kv-t): publish the commit watermark through region 0 raft (#155)"
```

---

### Task 3: 纯函数 watermark 计算

**Files:**
- Create: `coord-server/src/kvt/commit_watermark.rs`
- Modify: `coord-server/src/kvt/mod.rs`（`pub mod commit_watermark;`）

**Interfaces:**
- Produces:
  - `pub struct CommitWatermarkState { watermark: u64, in_flight: u64, min_in_flight_ts: Option<u64> }`
  - `pub fn commit_watermark(decision_last_ts: u64, in_flight: &[u64]) -> CommitWatermarkState`
  - `pub fn pending_decision(last_applied_ts: u64, next_decision_ts: Option<u64>) -> Option<u64>`
  - `pub fn unshipped_record(start_ts: u64, end_ts: u64, next_outbox_ts: Option<u64>) -> Option<u64>`
- Consumes: 无（纯计算，不读时钟/IO）

- [x] **Step 1: 写失败测试**

新建 `commit_watermark.rs`（先只写模块文档 + 测试）：

```rust
//! CommitWatermark 计算（ADR-0015 D5/D9，#155）。
//!
//! D9 口径：`watermark = min(in-flight commit_ts) - 1`；无 in-flight 时等于
//! `max_committed_ts`。in-flight 有两类来源，均以「最早未完成 ts」表示：
//! 1. 已决策但尚未被某个数据 Region apply 的决策（apply 游标之后的下一条）；
//! 2. 已 apply（outbox 有记录）但尚未追加进 shard 组的记录。
//!
//! 这里只做纯计算；候选的收集（扫描游标/outbox）在协调器里完成。

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watermark_is_the_last_committed_ts_without_in_flight() {
        assert_eq!(commit_watermark(0, &[]).watermark, 0);
        let state = commit_watermark(10, &[]);
        assert_eq!(state.watermark, 10);
        assert_eq!(state.in_flight, 0);
        assert_eq!(state.min_in_flight_ts, None);
    }

    #[test]
    fn watermark_stops_below_the_earliest_in_flight_ts() {
        let state = commit_watermark(10, &[9, 7]);
        assert_eq!(state.watermark, 6);
        assert_eq!(state.in_flight, 2);
        assert_eq!(state.min_in_flight_ts, Some(7));
        assert_eq!(commit_watermark(10, &[1]).watermark, 0);
        assert_eq!(
            commit_watermark(10, &[11]).watermark,
            10,
            "候选不得超过已决策水位"
        );
    }

    #[test]
    fn pending_decision_starts_after_the_applied_cursor() {
        assert_eq!(pending_decision(5, Some(7)), Some(7));
        assert_eq!(
            pending_decision(7, Some(7)),
            None,
            "等于游标的决策已 apply"
        );
        assert_eq!(pending_decision(7, None), None);
        assert_eq!(pending_decision(0, Some(2)), Some(2));
    }

    #[test]
    fn unshipped_record_only_counts_inside_the_shard_range() {
        assert_eq!(unshipped_record(1, 10, Some(7)), Some(7));
        assert_eq!(
            unshipped_record(1, 10, Some(10)),
            None,
            "end 是半开区间"
        );
        assert_eq!(
            unshipped_record(5, 10, Some(4)),
            None,
            "扫描下界之下不再有记录"
        );
        assert_eq!(
            unshipped_record(1, 0, Some(7)),
            Some(7),
            "未定界 shard 接受任意更大 ts"
        );
        assert_eq!(unshipped_record(1, 10, None), None);
    }
}
```

- [x] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server --lib commit_watermark::`
Expected: FAIL（`cannot find function commit_watermark`）

- [x] **Step 3: 实现纯函数**

在测试模块之前加：

```rust
/// 一次 watermark 计算的结果（供 tick/指标/测试读取）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitWatermarkState {
    pub watermark: u64,
    pub in_flight: u64,
    pub min_in_flight_ts: Option<u64>,
}

/// D9：`min(in-flight) - 1`；无 in-flight = `decision_last_ts`。
///
/// 候选高于已决策水位时按已决策水位封顶（防御性：调用方的扫描游标不会产生
/// 这种输入，但封顶保证 watermark 不越过尚未存在的决策）。
pub fn commit_watermark(decision_last_ts: u64, in_flight: &[u64]) -> CommitWatermarkState {
    let min_in_flight = in_flight.iter().copied().min();
    let watermark = match min_in_flight {
        Some(ts) => ts.saturating_sub(1).min(decision_last_ts),
        None => decision_last_ts,
    };
    CommitWatermarkState {
        watermark,
        in_flight: in_flight.len() as u64,
        min_in_flight_ts: min_in_flight,
    }
}

/// 某数据 Region 已 apply 游标之后的下一条决策（无 = 该 Region 不 in-flight）。
pub fn pending_decision(last_applied_ts: u64, next_decision_ts: Option<u64>) -> Option<u64> {
    next_decision_ts.filter(|ts| *ts > last_applied_ts)
}

/// 某 shard 对某数据 Region 的下一条未 ship outbox 记录（区间外 = 该 Region
/// 在本 shard 已搬完）。
///
/// 扫描本身从 `max(shipped, start - 1)` 之后开始，因此返回的 `ts` 不会低于
/// `start_ts`；这里仍做半开区间校验，避免调用方误传全量扫描结果。
pub fn unshipped_record(start_ts: u64, end_ts: u64, next_outbox_ts: Option<u64>) -> Option<u64> {
    next_outbox_ts.filter(|ts| *ts >= start_ts && (end_ts == 0 || *ts < end_ts))
}
```

- [x] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server --lib commit_watermark::`
Expected: PASS（4 passed）

- [x] **Step 5: 提交**

```bash
git add coord-server/src/kvt/commit_watermark.rs coord-server/src/kvt/mod.rs
git commit -m "feat(kv-t): derive the commit watermark from in-flight work (#155)"
```

---

### Task 4: 协调器收集 in-flight 并发布 watermark

**Files:**
- Modify: `coord-server/src/kvt/commit_log_shards.rs`
- Test: 同文件 `mod tests`（`ShardFixture` 扩展 + 2 个测试）

**Interfaces:**
- Consumes: Task 1/2/3 的 API；`MvccStorage::{sequencer_last_commit_ts, next_sequencer_decision_ts, last_applied_decision_ts, commit_records_after, commit_log_shipped_through}`
- Produces:
  - `pub fn local_watermark(&self) -> Result<CommitWatermarkState, String>`
  - `pub fn published_watermark(&self) -> Result<u64>`
  - `pub async fn publish_watermark(&self, watermark: u64) -> Result<u64, String>`
  - `CommitLogShardTick` 增加 `watermark: u64, in_flight: u64`（`Default` 保持全 0）
  - `tick()` 顺序：装配 → ship → 计算 → （region 0 leader）发布 → 元数据推进

- [x] **Step 1: 写失败测试**

在 `commit_log_shards.rs` 的 `ShardFixture` 中加字段 `root_mvcc: Arc<MvccStorage<RedbBackend>>`（`start_with` 里 `Arc::clone(&root_mvcc)` 存一份），并加辅助：

```rust
/// 写 region 0 决策日志（真实 sequencer apply 路径，不做 outbox 伪造）。
fn write_decisions(&self, timestamps: &[u64]) {
    use crate::raft::type_config::{
        SequencerMutation, SequencerOp, SequencerTxn, SequencerTxnWithRanges,
    };
    let txns = timestamps
        .iter()
        .map(|ts| SequencerTxnWithRanges {
            txn: SequencerTxn {
                txn_id: format!("txn-{ts}").into_bytes(),
                start_ts: ts.saturating_sub(1),
                commit_ts: *ts,
                request_id: format!("req-{ts}").into_bytes(),
                read_set: Vec::new(),
                mutations: vec![SequencerMutation::Put {
                    key: format!("k{ts}").into_bytes(),
                    value: b"v".to_vec(),
                    lease_id: None,
                }],
            },
            read_ranges: Vec::new(),
        })
        .collect();
    self.root_mvcc
        .apply_sequencer_op(
            &SequencerOp::ValidateAndCommitWithRanges {
                txns,
                limits: crate::raft::type_config::SequencerValidationLimits {
                    max_read_set: 4_096,
                    max_range: 10_000,
                },
            },
            1,
            crate::storage::mvcc::AppliedLogId::standalone(1),
        )
        .unwrap();
}

/// 把决策 apply 进数据 Region：outbox 记录与 apply 游标在同一事务里前进。
fn apply_decisions(&self, timestamps: &[u64]) {
    use crate::raft::type_config::{CommitDecisionApplyLog, SequencerMutation};
    let decisions = timestamps
        .iter()
        .map(|ts| CommitDecisionApplyLog {
            shard_id: crate::storage::commit_log::COMMIT_LOG_STAGING_SHARD_ID,
            region_id: self.data.region_id(),
            commit_ts: *ts,
            txn_id: format!("txn-{ts}").into_bytes(),
            start_ts: ts.saturating_sub(1),
            mutations: vec![SequencerMutation::Put {
                key: format!("k{ts}").into_bytes(),
                value: b"v".to_vec(),
                lease_id: None,
            }],
        })
        .collect();
    self.data
        .mvcc
        .apply_commit_decisions_with_log(
            &decisions,
            1,
            crate::storage::mvcc::AppliedLogId::standalone(1),
            self.manager.config().max_txn_bytes,
        )
        .unwrap();
}
```

加测试：

```rust
#[tokio::test]
async fn watermark_never_passes_a_decision_the_data_region_has_not_applied() {
    let fx = ShardFixture::start().await;
    fx.manager.tick().await.unwrap(); // open shard 1

    fx.write_decisions(&[2, 3, 4]);
    let tick = fx.manager.tick().await.unwrap();
    assert_eq!(tick.watermark, 1, "决定 2 未 apply，watermark 停在 1");
    assert_eq!(tick.in_flight, 1);
    assert_eq!(fx.manager.published_watermark().unwrap(), 1);

    fx.apply_decisions(&[2]);
    let tick = fx.manager.tick().await.unwrap();
    assert_eq!(tick.watermark, 2, "apply 2 之后仍等 3");

    fx.apply_decisions(&[3, 4]);
    let tick = fx.manager.tick().await.unwrap();
    assert_eq!(tick.in_flight, 0);
    assert_eq!(tick.watermark, 4);
    assert_eq!(fx.manager.published_watermark().unwrap(), 4);
}

#[tokio::test]
async fn watermark_never_passes_an_applied_record_that_is_not_in_the_shard() {
    let fx = ShardFixture::start().await;
    fx.manager.tick().await.unwrap(); // open shard 1

    fx.write_decisions(&[2, 3]);
    fx.apply_decisions(&[2, 3]);
    // 不跑 tick：outbox 已有 2/3，但 shard 还没有记录。
    let state = fx.manager.local_watermark().unwrap();
    assert_eq!(state.min_in_flight_ts, Some(2));
    assert_eq!(state.watermark, 1);

    let tick = fx.manager.tick().await.unwrap();
    assert_eq!(tick.shipped, 2);
    assert_eq!(tick.watermark, 3);
}
```

- [x] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server --lib watermark_never_passes`
Expected: FAIL（`no method named local_watermark`）

- [x] **Step 3: 实现收集/发布 + tick 重排**

`commit_log_shards.rs`：

1. 顶部 `use crate::kvt::commit_watermark::{commit_watermark, pending_decision, unshipped_record, CommitWatermarkState};`、`use crate::raft::type_config::{Command, Response};`（若未导入）。
2. `CommitLogShardTick` 加字段 `pub watermark: u64, pub in_flight: u64`。
3. 加方法：

```rust
/// 本节点视图的 watermark（不发布）：决策日志最后 ts + 两类 in-flight 候选。
///
/// 只读本地副本：v1 静态复制下 region 0 leader 持有每个数据 Region 与每个
/// shard 的副本；本地 apply 落后只会让 watermark 更保守，不会越过未完成事务。
pub fn local_watermark(&self) -> Result<CommitWatermarkState, String> {
    let decision_last = self
        .mvcc
        .sequencer_last_commit_ts()
        .map_err(|err| format!("read sequencer last commit_ts: {err}"))?;
    let regions = self.manager.list_runtimes();
    let table = self.table().map_err(|err| format!("read commit log shard table: {err}"))?;
    let mut in_flight = Vec::new();

    for rt in &regions {
        let applied = rt
            .mvcc
            .last_applied_decision_ts()
            .map_err(|err| format!("region {} read decision cursor: {err}", rt.region_id()))?;
        let next = self
            .mvcc
            .next_sequencer_decision_ts(applied)
            .map_err(|err| format!("region 0 scan pending decisions: {err}"))?;
        in_flight.extend(pending_decision(applied, next));
    }

    for meta in &table.shards {
        let Some(shard_rt) = self.runtime(meta.shard_id) else {
            // 未装配的 shard 无法确认搬运进度：把水位钉在它起点之下（保守）。
            in_flight.push(meta.start_ts);
            continue;
        };
        for rt in &regions {
            let region_id = rt.region_id();
            let shipped = shard_rt
                .mvcc
                .commit_log_shipped_through(region_id)
                .map_err(|err| format!("read shipped watermark for region {region_id}: {err}"))?;
            let after = shipped.max(meta.start_ts.saturating_sub(1));
            let next = rt
                .mvcc
                .commit_records_after(
                    crate::storage::commit_log::COMMIT_LOG_STAGING_SHARD_ID,
                    after,
                    1,
                )
                .map_err(|err| format!("read outbox of region {region_id}: {err}"))?
                .first()
                .map(|record| record.commit_ts);
            in_flight.extend(unshipped_record(meta.start_ts, meta.end_ts, next));
        }
    }

    Ok(commit_watermark(decision_last, &in_flight))
}

/// 已发布的 watermark（region 0 视图；Watch 读取口径，#156）。
pub fn published_watermark(&self) -> Result<u64> {
    self
        .mvcc
        .commit_watermark()
        .map_err(|err| format!("read published commit watermark: {err}"))
}

/// 经 region 0 raft 发布 watermark（实际生效值；仅 region 0 leader 调用）。
pub async fn publish_watermark(&self, watermark: u64) -> Result<u64, String> {
    let response = self
        .raft
        .client_write(Command::PublishCommitWatermark { watermark })
        .await
        .map_err(|err| format!("publish commit watermark {watermark}: {err}"))?;
    match response.response() {
        Response::CommitWatermarkPublished { watermark } => Ok(*watermark),
        other => Err(format!(
            "unexpected PublishCommitWatermark response: {other:?}"
        )),
    }
}
```

4. `tick()` 重排为：装配全部 shard → 本地 shard leader 逐 shard ship → `local_watermark()` → region 0 leader 时 `if state.watermark > published { publish_watermark(state.watermark).await? }` → `advance_metadata(state.watermark)`；`tick.watermark/in_flight` 用计算结果。`advance_metadata` 暂时保留旧签名（Task 5 再改成用 watermark）。

5. 重排后 `tick_opens_shards_precreates_at_80_percent_and_seals_by_timeout` 的 80% 预创建发生在第 8 次 `ship_one` 的 tick 里（ship 先于元数据），显式 tick 的 `opened` 变成 0：把该测试的断言改为循环结束后 `table.shards.len() == 2`、`shards[1].start_ts == shards[0].end_ts`，seal 超时部分保持不变（加 `tick.stalls == 1` 留到 Task 5 一并断言）。

- [x] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server --lib watermark_never_passes`
Expected: PASS（2 passed）

- [x] **Step 5: 提交**

```bash
git add coord-server/src/kvt/commit_log_shards.rs
git commit -m "feat(kv-t): track in-flight decisions and records in the shard tick (#155)"
```

---

### Task 5: 按 watermark seal + 停滞兜底

**Files:**
- Modify: `coord-server/src/kvt/commit_log_shards.rs`

**Interfaces:**
- Consumes: Task 4 的 `local_watermark` / `published_watermark`
- Produces: `CommitLogShardTick` 增加 `stalls: u64`；`advance_metadata(&self, watermark: u64)`

- [x] **Step 1: 写失败测试**

在 `commit_log_shards.rs` 的 `mod tests` 加（并更新既有两条 seal 测试，见 Step 3）：

```rust
#[tokio::test]
async fn watermark_seals_a_completed_shard_without_waiting_for_the_timeout() {
    let fx = ShardFixture::start().await;
    fx.manager.tick().await.unwrap(); // open shard 1
    fx.write_decisions(&[2, 3, 4]);
    fx.apply_decisions(&[2, 3, 4]);
    fx.manager.tick().await.unwrap(); // ship 2/3/4, watermark = 4
    fx.manager
        .propose(CommitLogShardOp::SetEnd {
            shard_id: 1,
            end_ts: 4,
            now_ms: 1_000,
        })
        .await
        .unwrap();

    // 时钟不动：seal 只能来自 watermark，不是超时。
    let tick = fx.manager.tick().await.unwrap();
    assert_eq!(tick.stalls, 0);
    assert_eq!(tick.sealed, 1);
    let meta = fx.manager.table().unwrap().shards[0].clone();
    assert_eq!(meta.sealed_at_ms, 1_000, "tick 使用注入时钟");
    assert_eq!(
        fx.manager.table().unwrap().shards[0].end_ts,
        4,
        "seal 不改边界"
    );
}

#[tokio::test]
async fn stalled_watermark_is_observable_and_still_lands_late_records() {
    let fx = ShardFixture::start().await;
    fx.manager.tick().await.unwrap(); // open shard 1
    fx.write_decisions(&[2, 3]);
    fx.manager
        .propose(CommitLogShardOp::SetEnd {
            shard_id: 1,
            end_ts: 3,
            now_ms: 2_000,
        })
        .await
        .unwrap();

    // 决策 2 未 apply：watermark 停在 1，< end_ts。
    let tick = fx.manager.tick().await.unwrap();
    assert_eq!(tick.sealed, 0);
    assert_eq!(tick.stalls, 0);

    fx.set_now(2_000 + 300_000);
    let tick = fx.manager.tick().await.unwrap();
    assert_eq!(tick.stalls, 1, "超时后走兜底并计入停滞");
    assert_eq!(tick.sealed, 1);

    // seal 之后迟到的区间内记录仍必须落进 shard（不丢事件）。
    fx.apply_decisions(&[2]);
    let tick = fx.manager.tick().await.unwrap();
    assert_eq!(tick.shipped, 1);
    assert_eq!(fx.manager.records_at(1, 2).await.unwrap().len(), 1);

    // seal 后不再接受越界记录：边界 [1,3) 之外的 ts=3 被 shard 拒收。
    let response = fx
        .manager
        .runtime(1)
        .unwrap()
        .raft
        .client_write(Command::CommitLogAppend {
            shard_id: 1,
            start_ts: 1,
            end_ts: 3,
            max_txn_bytes: fx.manager.config().max_txn_bytes,
            records: vec![crate::storage::commit_log::CommitRecord {
                shard_id: 1,
                commit_ts: 3,
                txn_id: b"txn-3".to_vec(),
                start_ts: 2,
                region_id: fx.data.region_id(),
                txn_bytes: 16,
                changes: vec![crate::storage::commit_log::CommitChange {
                    key: b"k3".to_vec(),
                    value: Some(b"v".to_vec()),
                    prev: None,
                }],
            }],
        })
        .await
        .unwrap();
    match response.response() {
        Response::CommitLogAppend {
            inserted, rejected, ..
        } => {
            assert_eq!(*inserted, 0, "sealed shard 不写越界记录");
            assert_eq!(*rejected, 1);
        }
        other => panic!("unexpected response: {other:?}"),
    }
    assert!(fx.manager.records_at(1, 3).await.unwrap().is_empty());
}
```

- [x] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server --lib stalled_watermark_is_observable`
Expected: FAIL（`no field stalls`）

- [x] **Step 3: 实现 seal 主路径 + 更新既有断言**

1. `CommitLogShardTick` 加 `pub stalls: u64`。
2. `advance_metadata(&self, watermark: u64)`：
   - 容量路径：`full || precreate_due` 时仍 `SetEnd + Open`；**删除** `if full { seal }` 的立即 seal（`end = last_decision + 1` 时 `watermark <= last_decision < end`，按 D9 只能等新决策或超时）。
   - 重读表后遍历 `end_ts != 0 && sealed_at_ms == 0`：
     - `watermark >= end_ts` → propose Seal（正常路径，不计 stall）；
     - 否则若 `seal_timeout_due(ended_at_ms, now, cfg.shard_seal_timeout_secs)` → `tracing::warn!(shard_id, end_ts, watermark, "commit log shard seal stalled; sealing on timeout")` + propose Seal + `stalls += 1`。
   - 返回 `(opened, sealed, stalls)`；`tick()` 接住并填 `tick.stalls`。
3. 更新既有测试：
   - `tick_opens_shards_precreates_at_80_percent_and_seals_by_timeout`：seal 那一轮断言 `tick.stalls == 1`（无决策 → watermark 0）；预创建断言按 Task 4 的改动保留。
   - `tick_seals_immediately_when_the_record_capacity_is_reached` 改名为 `tick_bounds_a_full_shard_and_opens_the_next`：断言 `tick.opened == 1`、shard 1 有界、`tick.sealed == 0`（还没有新决策/超时），再用 `set_now(ended + timeout)` 跑一轮断言 `stalls == 1 && sealed == 1`。

- [x] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server --lib commit_log_shards::`
Expected: PASS（全部，含既有 shard 测试）

- [x] **Step 5: 提交**

```bash
git add coord-server/src/kvt/commit_log_shards.rs
git commit -m "feat(kv-t): seal commit log shards by watermark with a stall fallback (#155)"
```

---

### Task 6: 指标（watermark / in-flight / seal 延迟 / stall）

**Files:**
- Modify: `coord-server/src/metrics.rs`
- Modify: `coord-server/src/kvt/commit_log_shards.rs`（`start_shard_manager`）

**Interfaces:**
- Produces:
  - `pub fn set_kv_t_commit_watermark(&self, watermark: u64, in_flight: u64)`
  - `pub fn set_kv_t_commit_log_seal_lag(&self, seconds: u64)`
  - `pub fn inc_kv_t_commit_watermark_stall(&self)`
  - Prometheus：`coord_kv_t_commit_watermark`、`coord_kv_t_commit_in_flight`（gauge）、`coord_kv_t_commit_log_seal_lag_seconds`（gauge）、`coord_kv_t_commit_watermark_stalls_total`（counter）
- Consumes: `CommitLogShardTick { watermark, in_flight, stalls, .. }`

- [x] **Step 1: 写失败测试**

在 `metrics.rs` 的 `mod tests` 加：

```rust
#[test]
fn test_metrics_commit_watermark() {
    let m = Metrics::new();
    m.set_kv_t_commit_watermark(120, 3);
    m.set_kv_t_commit_log_seal_lag(42);
    m.inc_kv_t_commit_watermark_stall();
    m.inc_kv_t_commit_watermark_stall();

    let output = m.render_prometheus_text();
    assert!(output.contains("coord_kv_t_commit_watermark 120"));
    assert!(output.contains("coord_kv_t_commit_in_flight 3"));
    assert!(output.contains("coord_kv_t_commit_log_seal_lag_seconds 42"));
    assert!(output.contains("coord_kv_t_commit_watermark_stalls_total 2"));
}
```

- [x] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server --lib test_metrics_commit_watermark`
Expected: FAIL（`no method named set_kv_t_commit_watermark`）

- [x] **Step 3: 实现字段/方法/暴露 + 背景任务接线**

`metrics.rs`：结构体加 4 个 `AtomicU64`（`kv_t_commit_watermark`、`kv_t_commit_in_flight`、`kv_t_commit_log_seal_lag_seconds`、`kv_t_commit_watermark_stalls_total`），`new()` 初始化为 0，加上述 3 个 setter/inc 与 `render_prometheus_text()` 的 4 段（help/type/value，格式照 `coord_kv_t_commit_log_shards`）。

`commit_log_shards.rs`：`tick()` 计算 `seal_lag_secs`（有界未 seal 的最大 `(now - ended_at_ms)/1000`，无 = 0），填进 `CommitLogShardTick`；`start_shard_manager` 每轮：

```rust
metrics.set_kv_t_commit_watermark(tick.watermark, tick.in_flight);
metrics.set_kv_t_commit_log_seal_lag(tick.seal_lag_secs);
for _ in 0..tick.stalls {
    metrics.inc_kv_t_commit_watermark_stall();
}
```

- [x] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server --lib test_metrics_commit_watermark`
Expected: PASS

- [x] **Step 5: 提交**

```bash
git add coord-server/src/metrics.rs coord-server/src/kvt/commit_log_shards.rs
git commit -m "feat(kv-t): expose commit watermark and seal lag metrics (#155)"
```

---

### Task 7: 文档、门禁与交付

**Files:**
- Modify: `docs/production/consumer-contract.md`（Watch 段）
- Modify: 本计划（勾选 + 验证记录）

- [x] **Step 1: 更新 consumer-contract**

在 Watch 的 Commit Log 分片条目后补一段当前语义（不写任务号/日期）：

```
  CommitWatermark 是「不再出现更小 commit_ts 记录」的单调水位：无 in-flight 时等于
  最后决策的 `commit_ts`，否则为最早未 apply/未搬运的 `commit_ts - 1`。shard 只在
  `watermark >= end_ts` 时 seal；停滞超过 5 分钟按超时 seal 并计入
  `coord_kv_t_commit_watermark_stalls_total`，sealed shard 仍接受区间内迟到记录。
```

- [x] **Step 2: 跑门禁**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
bash scripts/check-panics.sh
cargo audit
cargo deny check
bash scripts/check-doc-refs.sh
cargo test -p coord-server --lib commit_watermark::
cargo test -p coord-server --lib commit_log_shards::
cargo test --workspace
```

Expected: 全部通过（`cargo audit` 允许已知的 1 条 allow 警告，与既有记录一致）。

- [x] **Step 3: 计划勾选与验证记录**

勾选本文件所有步骤，在文件末尾追加：

```markdown
## Verification

- `cargo fmt --all -- --check` / `cargo clippy --workspace --lib --bins -- -D warnings` / `bash scripts/check-panics.sh` / `cargo audit` / `cargo deny check` / `bash scripts/check-doc-refs.sh`：通过
- 单测：`commit_watermark::`、`commit_log_shards::`、`test_metrics_commit_watermark`、`test_apply_publish_commit_watermark_is_monotonic_and_eventless`、`test_commit_watermark_is_monotonic_persistent_and_replay_safe` 通过
- 全量：`cargo test --workspace` 通过
```

- [x] **Step 4: PR**

```bash
git add docs/production/consumer-contract.md docs/superpowers/plans/2026-10-20-kv-t-g4-commit-watermark.md
git commit -m "docs(kv-t): document commit watermark and shard seal semantics (#155)"
git push -u origin feat/kv-t-g4-commit-watermark
gh pr create --base main --fill
gh pr checks --watch
gh pr merge --merge --delete-branch
```

合并后用证据评论关闭 #155；milestone KV-T G4 的 open 数减一。

## Verification

- `cargo fmt --all -- --check` / `cargo clippy --workspace --lib --bins -- -D warnings` / `bash scripts/check-panics.sh` / `cargo audit` / `cargo deny check` / `bash scripts/check-doc-refs.sh`：通过
- 单测：`commit_watermark::`、`commit_log_shards::`、`test_metrics_commit_watermark`、`test_apply_publish_commit_watermark_is_monotonic_and_eventless`、`test_commit_watermark_is_monotonic_persistent_and_replay_safe` 通过
- 全量：`cargo test --workspace` 通过
