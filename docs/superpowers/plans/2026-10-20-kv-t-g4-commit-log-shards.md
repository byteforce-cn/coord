# KV-T G4 Commit Log Shard Raft 组 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 #153 写入数据 Region 的 CommitRecord outbox 接管为按 `commit_ts` 区间分片的独立 Raft 组，带 1M/1h seal、80% 预创建、seal 超时与多节点复制。

**Architecture:** region 0 system raft 是 shard 元数据（`[start_ts,end_ts)`、状态、时间戳）的唯一权威；每个 shard 一个独立 Raft 组（复用 `spawn_region_runtime` 的目录/日志/网络装配，region id 使用保留区间）。数据 Region apply 仍把记录写进本地 outbox（#153 的写前日志，保证「数据可见 ⇒ 记录已落盘」），各节点的 shard 协调器在本地 shard 副本为 leader 时把 outbox 记录幂等追加进 shard 组。Watch 读取（#156）以 shard 组为准；outbox 的清理归 #157。

**Tech Stack:** Rust, openraft 0.10.0-alpha.34（经 `coord-server/src/raft` 隔离层）, redb, postcard, zstd, tokio, tonic。

**Spec:** `docs/adr/0015-kv-t.md` D5/D9/D11；issue #154（前置 #153）。

## Global Constraints

- `[kv_t].enabled=false`（默认）时不得改变 v1 行为；新代码只在 v2 装配路径可达（ADR-0001）。
- 用户级拒绝/冲突不得让 Raft apply 返回 `Err`（D11.5）：越界记录、重复 seal 只计数不失败。
- `commit_ts` 全局唯一单调；shard 边界 `[start_ts,end_ts)` 半开、无重叠、无空洞（本计划以「start = 前一 shard 的 end」构造性保证）。
- 默认阈值逐字取自 D9：`commit_log_shard_records = 1_000_000`、`commit_log_shard_secs = 3600`、`shard_seal_timeout_secs = 300`；80% 预创建 = `count*5 >= records*4 || age*5 >= secs*4`。
- 注释只写当前「为什么」（不变量/陷阱/契约），不写变更史、日期或一次性任务号；不新增 TODO/FIXME。
- 提交前本机必须跑通 `cargo fmt --all -- --check`、`cargo clippy --workspace --lib --bins -- -D warnings`、`bash scripts/check-panics.sh`、`cargo audit`、`cargo deny check`；涉及文档跑 `bash scripts/check-doc-refs.sh`。
- 临时文件只放 `/tmp`。

## Review Focus

- 第一个 shard 的 `start_ts` 必须覆盖**建表之前**已提交的决策（取 1，而不是首个 commit_ts），否则更早的 outbox 记录永远没有归属 shard。
- SetEnd 与并发决策的边界：`end = max(shard 已 ship 的 ts, sequencer 最后决策 ts) + 1`，保证 `end` 之后的新决策只进新 shard，`end` 之前的只进旧 shard；任一方向都不能产生空洞。
- shard 组初始化只能发生一次：只有 region 0 leader 的节点用 `initialize=true`；重复初始化视为良性（记录日志，不 panic、不阻塞 tick）。
- 记录追加必须幂等：同一 `(shard, commit_ts, region_id)` 同内容 = Duplicate，不同内容 = 拒绝并保留原记录（不覆盖、不失败 apply）。
- shard leader 之外不写：shipper 只在本地 shard raft leader 时提出 append；leader 迁移后新 leader 从 shipped 水位续传，不丢不重。
- 记录的 outbox 扫描水位必须取「所有 shard 对该数据 Region 的已 ship 水位的最小值 - 1」，否则中断在跨 shard 批次中间会永久跳过边界 `commit_ts`。
- 重启：shard 元数据随 region 0 快照/日志恢复，shard 组按目录重开；不得重复初始化成员、不得回退边界。

---

### Task 1: Shard 表模型与阈值策略

**Files:**
- Create: `coord-server/src/storage/commit_log_shard.rs`
- Modify: `coord-server/src/storage/mod.rs`（`pub mod commit_log_shard;`）

**Interfaces:**
- Produces:
  - `pub struct CommitLogShardMeta { shard_id: u64, start_ts: u64, end_ts: u64, opened_at_ms: u64, ended_at_ms: u64, sealed_at_ms: u64 }`（`end_ts == 0` = 未定界；`ended_at_ms/sealed_at_ms == 0` = 未定界/未 seal）
  - `pub struct CommitLogShardTable { shards: Vec<CommitLogShardMeta> }`：`encode() -> Result<Vec<u8>>`、`decode(&[u8]) -> Result<Self>`、`covering(ts) -> Option<&CommitLogShardMeta>`、`latest() -> Option<&CommitLogShardMeta>`、`open(first_start_ts, now_ms) -> Result<bool>`、`set_end(shard_id, end_ts, now_ms) -> Result<bool>`、`seal(shard_id, now_ms) -> Result<bool>`、`validate() -> Result<()>`
  - `pub fn split_end_ts(ship_cursor_ts: u64, sequencer_last_ts: u64, start_ts: u64) -> u64`
  - `pub fn precreate_due(records: u64, age_ms: u64, shard_records: u64, shard_secs: u64) -> bool`
  - `pub fn seal_due(records: u64, age_ms: u64, shard_records: u64, shard_secs: u64) -> bool`
  - `pub fn seal_timeout_due(ended_at_ms: u64, now_ms: u64, timeout_secs: u64) -> bool`
  - `pub const COMMIT_LOG_SHARD_TABLE_KEY: &[u8]`
  - `pub const COMMIT_LOG_SHARD_REGION_ID_BASE: u64 = 0x4000_0000_0000_0000`
  - `pub fn shard_region_id(shard_id: u64) -> u64`

- [ ] **Step 1: 写失败测试**

在 `commit_log_shard.rs` 的 `#[cfg(test)] mod tests` 中：

```rust
#[test]
fn open_first_shard_starts_at_one_and_covers_history() {
    let mut t = CommitLogShardTable::default();
    assert!(t.open(1, 1_000).unwrap());
    assert!(t.covering(1).is_some(), "pre-existing commit_ts must be covered");
    assert!(t.covering(9_999).is_some(), "open-ended shard covers the future");
    assert_eq!(t.latest().unwrap().shard_id, 1);
    assert!(!t.open(1, 1_100).unwrap(), "second open is a no-op while unbounded");
}

#[test]
fn split_opens_next_shard_without_gap_or_overlap() {
    let mut t = CommitLogShardTable::default();
    t.open(1, 1_000).unwrap();
    assert!(t.set_end(1, 101, 2_000).unwrap());
    assert!(t.open(1, 2_100).unwrap());
    assert_eq!(t.shards.len(), 2);
    assert_eq!(t.shards[1].start_ts, 101);
    assert_eq!(t.covering(100).unwrap().shard_id, 1, "half-open: end belongs to next");
    assert_eq!(t.covering(101).unwrap().shard_id, 2);
    assert!(t.validate().is_ok());
}

#[test]
fn set_end_is_monotonic_and_requires_unbounded_latest() {
    let mut t = CommitLogShardTable::default();
    t.open(1, 1_000).unwrap();
    assert!(t.set_end(1, 50, 2_000).unwrap());
    assert!(!t.set_end(1, 40, 2_100).unwrap(), "must not lower end");
    assert!(t.set_end(9, 60, 2_200).is_err(), "unknown shard rejected");
}

#[test]
fn seal_requires_a_bounded_shard_and_is_idempotent() {
    let mut t = CommitLogShardTable::default();
    t.open(1, 1_000).unwrap();
    assert!(t.seal(1, 2_000).is_err(), "open-ended shard cannot be sealed");
    t.set_end(1, 100, 1_500).unwrap();
    assert!(t.seal(1, 2_000).unwrap());
    assert_eq!(t.shards[0].sealed_at_ms, 2_000);
    assert!(!t.seal(1, 2_100).unwrap(), "second seal is a no-op");
}

#[test]
fn policy_thresholds_follow_d9_percentages() {
    assert!(!precreate_due(799_999, 0, 1_000_000, 3_600));
    assert!(precreate_due(800_000, 0, 1_000_000, 3_600));
    assert!(precreate_due(0, 2_880_000, 1_000_000, 3_600)); // 80% of 1h
    assert!(!seal_due(999_999, 3_599_999, 1_000_000, 3_600));
    assert!(seal_due(1_000_000, 0, 1_000_000, 3_600));
    assert!(seal_due(0, 3_600_000, 1_000_000, 3_600));
    assert!(!seal_timeout_due(1_000, 1_299_999, 300));
    assert!(seal_timeout_due(1_000, 1_300_000, 300));
    assert_eq!(split_end_ts(0, 0, 7), 8);
    assert_eq!(split_end_ts(105, 109, 7), 110);
}

#[test]
fn encode_decode_roundtrip_and_rejects_bad_tables() {
    let mut t = CommitLogShardTable::default();
    t.open(1, 1_000).unwrap();
    t.set_end(1, 10, 1_500).unwrap();
    assert_eq!(CommitLogShardTable::decode(&t.encode().unwrap()).unwrap(), t);
    let mut bad = t.clone();
    bad.shards[0].start_ts = 0;
    assert!(bad.validate().is_err());
    let mut overlap = t.clone();
    overlap.shards.push(CommitLogShardMeta { shard_id: 2, start_ts: 5, ..overlap.shards[0].clone() });
    assert!(overlap.validate().is_err());
}

#[test]
fn shard_region_ids_live_in_the_reserved_namespace() {
    assert_eq!(shard_region_id(1), COMMIT_LOG_SHARD_REGION_ID_BASE | 1);
    assert!(shard_region_id(1) > u32::MAX as u64, "must not collide with data regions");
}
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p coord-server --lib commit_log_shard`
Expected: 编译失败（`CommitLogShardTable` 未定义）。

- [ ] **Step 3: 实现模型与策略**

- `open`：表空 → 新 shard `start_ts = first_start_ts`；非空且 latest 未定界 → `Ok(false)`；否则新 shard `start_ts = latest.end_ts`，`shard_id = latest.shard_id + 1`（不采信调用方的起始值，杜绝空洞）。
- `set_end`：只接受 latest 且 `end_ts == 0` 的 shard；`end_ts > start_ts`；同值重复 = `Ok(false)`，更小值 = `Ok(false)`，未知/非 latest = `Err(InvalidArgument)`。
- `seal`：`end_ts == 0` → `Err(InvalidArgument)`；已 seal = `Ok(false)`。
- `validate`：start_ts > 0、`end_ts == 0 || end_ts > start_ts`、相邻 `end == next.start`、最多一个未定界 shard（且必须是最后一个）。
- `split_end_ts`：`max(ship_cursor_ts, sequencer_last_ts, start_ts) + 1`（用 `saturating_add` 后保证 > start）。
- `covering`：`start <= ts && (end == 0 || ts < end)`；表按 shard_id 升序，二分或线性都行（表 ≤ 数百项）。

- [ ] **Step 4: 运行通过**

Run: `cargo test -p coord-server --lib commit_log_shard`
Expected: 上列测试全绿。

- [ ] **Step 5: 提交**

```bash
git add coord-server/src/storage/commit_log_shard.rs coord-server/src/storage/mod.rs
git commit -m "feat(kv-t): model commit log shard table and D9 seal policy (#154)"
```

---

### Task 2: Region 0 shard 元数据命令与持久化

**Files:**
- Modify: `coord-server/src/raft/type_config.rs`（`Command` 末尾追加变体、`Response` 追加、`Display`）
- Modify: `coord-server/src/raft/state_machine.rs`（apply 分派）
- Modify: `coord-server/src/storage/mvcc.rs`（`apply_commit_log_shard_op`、`commit_log_shard_table`）

**Interfaces:**
- Produces:
  - `pub enum CommitLogShardOp { Open { start_ts: u64, opened_at_ms: u64 }, SetEnd { shard_id: u64, end_ts: u64, now_ms: u64 }, Seal { shard_id: u64, now_ms: u64 } }`
  - `Command::CommitLogShard(CommitLogShardOp)`（末尾追加，仅 region 0 提出）
  - `Response::CommitLogShard { shards: Vec<CommitLogShardMeta> }`
  - `MvccStorage::apply_commit_log_shard_op(&self, op: &CommitLogShardOp, revision: Revision, applied: AppliedLogId) -> Result<Vec<CommitLogShardMeta>>`
  - `MvccStorage::commit_log_shard_table(&self) -> Result<CommitLogShardTable>`

- [ ] **Step 1: 写失败测试（`mvcc.rs` tests 模块）**

```rust
#[test]
fn commit_log_shard_ops_persist_and_replay_idempotently() {
    let (_dir, storage) = test_storage();
    let open = CommitLogShardOp::Open { start_ts: 1, opened_at_ms: 1_000 };
    let applied = applied(7);
    let shards = storage.apply_commit_log_shard_op(&open, 7, applied).unwrap();
    assert_eq!(shards.len(), 1);
    assert_eq!(shards[0].shard_id, 1);
    assert_eq!(shards[0].start_ts, 1);

    // 同一 revision 重放：不重复建表，也不报错（raft 重放/重启路径）。
    let replayed = storage.apply_commit_log_shard_op(&open, 7, applied).unwrap();
    assert_eq!(replayed, shards);

    let end = CommitLogShardOp::SetEnd { shard_id: 1, end_ts: 100, now_ms: 2_000 };
    storage.apply_commit_log_shard_op(&end, 8, applied(8)).unwrap();
    let seal = CommitLogShardOp::Seal { shard_id: 1, now_ms: 3_000 };
    let shards = storage.apply_commit_log_shard_op(&seal, 9, applied(9)).unwrap();
    assert_eq!(shards[0].end_ts, 100);
    assert_eq!(shards[0].sealed_at_ms, 3_000);
    assert_eq!(storage.commit_log_shard_table().unwrap().shards, shards);
}

#[test]
fn invalid_shard_ops_do_not_fail_apply() {
    let (_dir, storage) = test_storage();
    storage
        .apply_commit_log_shard_op(&CommitLogShardOp::Seal { shard_id: 9, now_ms: 1 }, 1, applied(1))
        .expect("unknown shard seal is a no-op, not an apply error");
    assert!(storage.commit_log_shard_table().unwrap().shards.is_empty());
}
```

（`test_storage()` / `applied(n)` 用该 tests 模块已有的临时 redb 夹具；没有就按现有模式补一个。）

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p coord-server --lib commit_log_shard_ops`
Expected: 编译失败（变体未定义）。

- [ ] **Step 3: 实现**

- `type_config.rs`：`CommitLogShardOp` 放 `SequencerOp` 附近；`Command::CommitLogShard(CommitLogShardOp)` 追加在 `ApplyCommitDecisionsWithLog` 之后；`Response::CommitLogShard { shards: Vec<CommitLogShardMeta> }`；`Display` 打印 `CommitLogShard(Open/SetEnd/Seal)` 摘要。
- `mvcc.rs`：常量 `COMMIT_LOG_SHARD_TABLE_KEY: &[u8] = b"/_sys/commit_log/shards"` 引用 Task 1 的定义；apply 在一条 redb 事务里：`note_log_index_revision` → 读表 → 应用 op（错误只 `tracing::warn!` 并保持原表，**不返回 Err**）→ 写回 postcard → `META_LAST_APPLIED`；同一 `revision` 已在 changelog → 直接读表返回（重放幂等）。无需 changelog 事件也可用 `changelog_contains_revision` 判定重放；照 `apply_pd_op` 的模式写一条 `encode_changelog_key(revision)` 记录。
- `state_machine.rs`：`Command::CommitLogShard(op) => { let shards = sm.apply_commit_log_shard_op(op, revision, applied).map_err(io_err)?; Ok((Response::CommitLogShard { shards }, None)) }`，不产生 Watch 事件。

- [ ] **Step 4: 运行通过**

Run: `cargo test -p coord-server --lib commit_log_shard`
Expected: Task 1 + Task 2 测试全绿。

- [ ] **Step 5: 提交**

```bash
git add coord-server/src/raft/type_config.rs coord-server/src/raft/state_machine.rs coord-server/src/storage/mvcc.rs
git commit -m "feat(kv-t): persist commit log shard metadata through region 0 raft (#154)"
```

---

### Task 3: Shard 组内的记录追加、游标与读取

**Files:**
- Modify: `coord-server/src/raft/type_config.rs`（`Command::CommitLogAppend`、`Response::CommitLogAppend`）
- Modify: `coord-server/src/raft/state_machine.rs`
- Modify: `coord-server/src/storage/mvcc.rs`
- Modify: `coord-server/src/storage/commit_log.rs`（`COMMIT_LOG_CURSOR_KEY`、`COMMIT_LOG_COUNT_KEY`、`COMMIT_LOG_SHIPPED_PREFIX`、`shipped_key(region_id)`）

**Interfaces:**
- Produces:
  - `Command::CommitLogAppend { shard_id: u64, start_ts: u64, end_ts: u64, max_txn_bytes: u64, records: Vec<CommitRecord> }`
  - `Response::CommitLogAppend { inserted: u64, duplicates: u64, rejected: u64, cursor_ts: u64 }`
  - `pub struct CommitLogAppendResult { inserted: u64, duplicates: u64, rejected: u64, cursor_ts: u64 }`
  - `MvccStorage::apply_commit_log_append(...) -> Result<CommitLogAppendResult>`
  - `MvccStorage::commit_log_cursor(&self) -> Result<u64>`
  - `MvccStorage::commit_log_record_count(&self) -> Result<u64>`
  - `MvccStorage::commit_log_shipped_through(&self, region_id: u64) -> Result<u64>`

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn commit_log_append_is_idempotent_and_rejects_out_of_range() {
    let (_dir, storage) = test_storage();
    let in_range = record_with_shard(10, 2, 101, b"k", b"v");
    let out_of_range = record_with_shard(200, 2, 101, b"k", b"v");
    let result = storage
        .apply_commit_log_append(2, 100, 200, &[in_range.clone(), out_of_range], 4 * 1024 * 1024, 1, applied(1))
        .unwrap();
    assert_eq!((result.inserted, result.rejected, result.cursor_ts), (1, 1, 10));
    assert_eq!(storage.commit_log_record_count().unwrap(), 1);
    assert_eq!(storage.commit_log_shipped_through(7).unwrap(), 10);

    let replay = storage
        .apply_commit_log_append(2, 100, 200, &[in_range], 4 * 1024 * 1024, 2, applied(2))
        .unwrap();
    assert_eq!((replay.inserted, replay.duplicates), (0, 1));
    assert_eq!(storage.commit_log_record_count().unwrap(), 1, "no double count");
    assert_eq!(storage.commit_log_cursor().unwrap(), 10);
}

#[test]
fn shipped_watermark_is_per_data_region_and_monotonic() {
    let (_dir, storage) = test_storage();
    storage.apply_commit_log_append(2, 100, 200, &[record_with_shard(10, 2, 101, b"k", b"v")], 4 << 20, 1, applied(1)).unwrap();
    storage.apply_commit_log_append(2, 100, 200, &[record_with_shard(9, 2, 102, b"k", b"v")], 4 << 20, 2, applied(2)).unwrap();
    assert_eq!(storage.commit_log_shipped_through(7).unwrap(), 10, "must not go backwards");
    assert_eq!(storage.commit_log_shipped_through(8).unwrap(), 9);
}
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p coord-server --lib commit_log_append`
Expected: 编译失败。

- [ ] **Step 3: 实现**

- apply：先判 `changelog_contains_revision(revision)`（重放只汇总当前游标返回 0/0/0 + cursor）。逐条校验 `record.shard_id == shard_id`、`start_ts <= commit_ts && (end_ts == 0 || commit_ts < end_ts)`；不合格 `rejected += 1` 并 `tracing::warn!`。合格走 `append_commit_record_in_tx`（同 txn），统计 Inserted/Duplicate，更新 `COMMIT_LOG_CURSOR_KEY`（max）、`COMMIT_LOG_COUNT_KEY`（inserted 累加）、每 Region 的 `COMMIT_LOG_SHIPPED_PREFIX/{region:u64be}`（max）。空事务不写 count/cursor（避免无意义写放大）。写 `META_LAST_APPLIED` 与 changelog 记录。
- 读取：`commit_log_cursor`/`commit_log_record_count` 读 `TABLE_KV` 的 u64 BE（缺失 = 0）；`commit_log_shipped_through` 读 `TABLE_KV`。
- `Display`：`CommitLogAppend(shard=..., records=...)`。

- [ ] **Step 4: 运行通过**

Run: `cargo test -p coord-server --lib commit_log`
Expected: 全绿。

- [ ] **Step 5: 提交**

```bash
git add coord-server/src/raft/type_config.rs coord-server/src/raft/state_machine.rs coord-server/src/storage/mvcc.rs coord-server/src/storage/commit_log.rs
git commit -m "feat(kv-t): append commit records in per-shard raft groups (#154)"
```

---

### Task 4: Shard 协调器（runtime 装配、表刷新、读取）

**Files:**
- Create: `coord-server/src/kvt/commit_log_shards.rs`
- Modify: `coord-server/src/kvt/mod.rs`（`pub mod commit_log_shards;`）

**Interfaces:**
- Consumes: Task 1 `CommitLogShardTable`、Task 2 `commit_log_shard_table()`、`RegionManager`、`spawn_region_runtime`。
- Produces:
  - `pub struct CommitLogShardManager { ... }`：`new(node_id, raft, mvcc, manager, factory, rpc, base_dir, raft_config, config) -> Self`、`with_clock(self, clock: Arc<dyn Fn() -> u64 + Send + Sync>) -> Self`
  - `pub fn table(&self) -> Result<CommitLogShardTable>`
  - `pub fn shard_for_ts(&self, ts: u64) -> Result<Option<CommitLogShardMeta>>`
  - `pub async fn ensure_runtime(&self, shard_id: u64) -> Result<Arc<RegionRuntime>, String>`
  - `pub async fn records_at(&self, shard_id, ts) -> Result<Vec<CommitRecord>>`、`records_after(&self, shard_id, after_ts, limit)`
  - `pub fn start_shard_manager(node: Arc<CoordNode>, interval: Duration) -> JoinHandle<()>`

- [ ] **Step 1: 写失败测试（`#[tokio::test]`，单节点真实 shard raft）**

```rust
#[tokio::test]
async fn shard_runtime_spawns_from_table_and_serves_reads() {
    let fx = ShardFixture::start().await; // 单节点 region 0 + 1 个数据 Region
    fx.open_shard(1, 1_000).await;        // 经 region 0 raft 落表
    let rt = fx.manager.ensure_runtime(1).await.unwrap();
    assert_eq!(rt.region_id(), shard_region_id(1));
    assert!(fx.manager.ensure_runtime(1).await.is_ok(), "second call is idempotent");
    let same = fx.manager.ensure_runtime(1).await.unwrap();
    assert_eq!(same.region_id(), rt.region_id());
    assert!(fx.manager.shard_for_ts(1).unwrap().is_some());
}
```

（`ShardFixture` 放该文件 `#[cfg(test)]`：region 0 用 `new_raft` + `initialize`，数据 Region 用 `RegionManager::spawn_region`，manager 用真实 `RaftNetworkFactoryImpl`/`RaftRpcService`。）

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p coord-server --lib shard_runtime_spawns`
Expected: 编译失败。

- [ ] **Step 3: 实现**

- `ensure_runtime`：已存在直接返回；否则 `table()` 找到 meta，构造 `RegionMeta { region_id: shard_region_id(id), start_key: synthetic(id), end_key: synthetic(id+1), epoch: RegionEpoch::initial(), peers: data region peers, ..}`，`RegionHandle::new(meta)`，`spawn_region_runtime(node_id, &factory, &rpc, RegionRuntimeSpec { data_dir: region_data_dir(base_dir, shard_region_id(id)), raft_config, object_store: None, revision_mode: RevisionMode::CommitTs }, handle, initialize)` 后登记。
- `initialize` = `raft.current_leader().await == Some(self.node_id)`；初始化失败且错误含 "already" → 只 `tracing::info!` 并继续（并发初始化良性）。
- `peers`：`manager.list_regions().first()` 的 `meta.peers`；为空时返回 `Err("commit log shard requires at least one assembled data region")`。
- `records_at/after`：先 `ensure_runtime`，再读该 runtime 的 `mvcc.commit_records_at/after(shard_id, ...)`。
- `table/shard_for_ts`：读 region 0 `mvcc.commit_log_shard_table()`。

- [ ] **Step 4: 运行通过**

Run: `cargo test -p coord-server --lib commit_log_shards`
Expected: 全绿。

- [ ] **Step 5: 提交**

```bash
git add coord-server/src/kvt/commit_log_shards.rs coord-server/src/kvt/mod.rs
git commit -m "feat(kv-t): assemble commit log shard raft runtimes (#154)"
```

---

### Task 5: 元数据推进（open/预创建/seal）与 outbox 搬运

**Files:**
- Modify: `coord-server/src/kvt/commit_log_shards.rs`
- Modify: `coord-server/src/kvt/mod.rs`（`KvTCapability::Watch` 不新增：Watch RPC 归 #158）
- Modify: `coord-server/src/metrics.rs`
- Modify: `coord-server/src/server/mod.rs`（`commit_log_shards` 字段 + setter/getter）
- Modify: `coord/src/main.rs`（构造 manager + 启动 ticker）

**Interfaces:**
- Produces:
  - `pub async fn tick(&self) -> Result<CommitLogShardTick, String>`，`pub struct CommitLogShardTick { pub opened: u64, pub sealed: u64, pub shipped: u64, pub rejected: u64 }`
  - `Metrics::{set_kv_t_commit_log_shards, inc_kv_t_commit_log_shard_sealed, add_kv_t_commit_log_records_shipped, add_kv_t_commit_log_records_rejected}`

- [ ] **Step 1: 写失败测试（纯策略 + tick 集成，同 Task 4 夹具）**

```rust
#[tokio::test]
async fn tick_opens_shards_precreates_at_80_percent_and_seals_by_timeout() {
    let fx = ShardFixture::start_with(10, 3_600, 300).await; // 10 records / 1h / 5m
    fx.now.store(1_000);
    fx.manager.tick().await.unwrap();
    assert_eq!(fx.manager.table().unwrap().shards.len(), 1, "first open");

    // 8/10 records = 80% → set end + pre-create next
    for ts in 1..=8 { fx.append_record(ts).await; }
    fx.now.store(2_000);
    fx.manager.tick().await.unwrap();
    let table = fx.manager.table().unwrap();
    assert_eq!(table.shards.len(), 2);
    assert!(table.shards[0].end_ts > 0);
    assert_eq!(table.shards[1].start_ts, table.shards[0].end_ts);

    // seal timeout 300s 后关闭旧 shard
    fx.now.store(2_000 + 300_000);
    fx.manager.tick().await.unwrap();
    let sealed = fx.manager.table().unwrap();
    assert!(sealed.shards[0].sealed_at_ms > 0);
    assert_eq!(sealed.shards[0].sealed_at_ms, 2_000 + 300_000);
}

#[tokio::test]
async fn ship_moves_outbox_rows_once_and_only_on_the_shard_leader() {
    let fx = ShardFixture::start().await;
    fx.manager.tick().await.unwrap();          // 建第一个 shard
    fx.apply_outbox_records(&[(1, b"a", b"1"), (2, b"b", b"2")]).await;
    let tick = fx.manager.tick().await.unwrap();
    assert!(tick.shipped >= 2);
    let shard = fx.manager.shard_for_ts(2).unwrap().unwrap().shard_id;
    let rows = fx.manager.records_after(shard, 0, 0).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].changes[0].key, b"a");

    let again = fx.manager.tick().await.unwrap();
    assert_eq!(again.shipped, 0, "outbox cursor prevents re-shipping");
    assert_eq!(fx.manager.records_after(shard, 0, 0).await.unwrap().len(), 2);
}
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p coord-server --lib commit_log_shard`
Expected: 编译失败（`tick`/`CommitLogShardTick` 未定义）。

- [ ] **Step 3: 实现**

`tick()`（每节点、周期调用；内部顺序固定）：
1. `table = commit_log_shard_table()`。
2. `ensure_runtime` 每个 shard；指标 `set_kv_t_commit_log_shards(len)`。
3. **仅 region 0 leader** 推进元数据：
   - 表空 → `CommitLogShardOp::Open { start_ts: 1, opened_at_ms: now }`。
   - latest 未定界且 `precreate_due(records, age, records_cap, secs_cap)` → `SetEnd { end_ts: split_end_ts(ship_cursor, sequencer_last_commit_ts, start) }`，`split_end_ts` 见 Task 1；紧接着 `Open { start_ts: end_ts, opened_at_ms: now }`（预创建下一 shard）。
   - `seal_due(...)` 或 `seal_timeout_due(ended_at_ms, now, timeout)` 且未 seal → `Seal { shard_id, now_ms: now }`；`sealed` 计数 + `inc_kv_t_commit_log_shard_sealed`。
   - 每次 propose 后重新读表（本地 apply 已完成）再继续，避免基于旧表连续提案。
4. **所有节点**搬运：对每个本地为 leader 的 shard（`rt.raft.current_leader().await == Some(node_id)`）：
   - 对每个数据 runtime `R`：`floor = min over shards of shard_mvcc.commit_log_shipped_through(R.region_id())`；`after = floor.saturating_sub(1)`；`rows = R.mvcc.commit_records_after(COMMIT_LOG_STAGING_SHARD_ID, after, 256)`。
   - 过滤 `row.commit_ts` 落在本 shard 区间（`start_ts <= ts && (end_ts == 0 || ts < end_ts)`），按 `max_txn_bytes` 与 8MiB/批切片，`Command::CommitLogAppend { shard_id, start_ts, end_ts, max_txn_bytes, records }` 提出；非 leader 直接跳过。
   - 响应统计：`inserted/duplicates/rejected` → `shipped += inserted + duplicates`；`rejected > 0` 时 `tracing::warn!` 并在下一 tick 用刷新后的表重试（记录留在 outbox，不丢）。
   - 读失败/提案失败：`tracing::warn!` 后继续其它 shard（单 shard 故障不阻塞其它 shard）。
5. 返回 `CommitLogShardTick`。

`CoordNode`：字段 `commit_log_shards: Option<Arc<CommitLogShardManager>>` + `set_commit_log_shards` / `commit_log_shards()`；`start_shard_manager` 循环 `tick`，错误只 warn。`main.rs`：在 region_manager 装配后、`Arc::new(node)` 前构造（需要 `raft`、`mvcc`、`region_shared_factory`、`raft_rpc`、`data_dir`、`kv_t` 配置、`raft_config`），仅 `cfg.kv_t.enabled && multi_raft.enabled` 时注入并启动；关闭时零装配。

- [ ] **Step 4: 运行通过**

Run: `cargo test -p coord-server --lib commit_log_shard`
Expected: 全绿（含 Task 4）。

- [ ] **Step 5: 提交**

```bash
git add coord-server/src/kvt/commit_log_shards.rs coord-server/src/kvt/mod.rs coord-server/src/metrics.rs coord-server/src/server/mod.rs coord/src/main.rs
git commit -m "feat(kv-t): pre-create, seal and ship commit log shards (#154)"
```

---

### Task 6: 单节点端到端与重启恢复验收

**Files:**
- Create: `coord/tests/kv_t_g4_commit_log_shards_test.rs`

**Interfaces:**
- Consumes: 既有 G3 夹具（region 0 + 2 数据 Region + 真实 sequencer/apply）+ `CommitLogShardManager::tick`。

- [ ] **Step 1: 写失败测试**

```rust
#[tokio::test]
async fn committed_txn_records_land_in_shard_groups_with_full_values() {
    let fx = ShardNode::start(/* shard_records */ 10).await;
    fx.manager.tick().await.unwrap();
    let ts = fx.commit_txn(b"k1", b"v1", b"k2", b"v2").await; // 走 sequencer + apply_pending_decisions
    fx.tick_until_shipped(2).await;
    let shard = fx.manager.shard_for_ts(ts).unwrap().unwrap().shard_id;
    let rows = fx.manager.records_at(shard, ts).await.unwrap();
    assert_eq!(rows.len(), 2, "one fragment per data region");
    assert_eq!(rows.iter().map(|r| r.changes[0].key.clone()).collect::<BTreeSet<_>>(), BTreeSet::from([b"k1".to_vec(), b"k2".to_vec()]));
    assert!(rows.iter().all(|r| r.commit_ts == ts && r.start_ts < ts));
}

#[tokio::test]
async fn shard_replica_survives_restart_without_duplicates_or_boundary_drift() {
    let fx = ShardNode::start(10).await;
    fx.manager.tick().await.unwrap();
    let ts = fx.commit_txn(b"a", b"1", b"b", b"2").await;
    fx.tick_until_shipped(2).await;
    let before = fx.manager.table().unwrap();
    let shard = before.shards[0].shard_id;
    let rows_before = fx.manager.records_after(shard, 0, 0).await.unwrap();

    drop(fx);                       // 关闭 runtime（redb 落盘）
    let fx = ShardNode::reopen(fx_dir).await; // 同一 data_dir 重开 region 0 + shard
    fx.manager.tick().await.unwrap();
    assert_eq!(fx.manager.table().unwrap(), before, "boundaries must not drift");
    let rows_after = fx.manager.records_after(shard, 0, 0).await.unwrap();
    assert_eq!(rows_after, rows_before, "exact rows, no duplicates");
    assert!(rows_after.iter().any(|r| r.commit_ts == ts));
}
```

（`ShardNode` 复制 `kv_t_g3_delete_range_test.rs` 的夹具，加 manager；`tick_until_shipped` 在 30s 内轮询 `tick()`。）

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p coord --test kv_t_g4_commit_log_shards_test`
Expected: 编译失败（测试文件不存在/manager 未接线）。

- [ ] **Step 3: 让测试通过**

只改实现（夹具 + 必要的 `pub` 可见性），不改断言；若发现 tick 语义与断言冲突，按 Review Focus 的顺序修正实现，并在 ledger 记 `Ruling:`。

- [ ] **Step 4: 运行通过**

Run: `cargo test -p coord --test kv_t_g4_commit_log_shards_test`
Expected: 2 passed。

- [ ] **Step 5: 提交**

```bash
git add coord/tests/kv_t_g4_commit_log_shards_test.rs
git commit -m "test(kv-t): end-to-end commit log shard shipping and restart recovery (#154)"
```

---

### Task 7: 多节点复制与 leader 迁移验收

**Files:**
- Create: `coord-server/tests/kv_t_g4_commit_log_shards_multinode_test.rs`

**Interfaces:**
- Consumes: `mod common`、`kv_t_g3_sequencer_failover_test.rs` 的 3 节点 in-process 夹具（region 0 raft + 数据 Region + gRPC listener）。

- [ ] **Step 1: 写失败测试**

```rust
#[tokio::test]
async fn shard_group_replicates_records_to_all_nodes_and_new_leader_continues() {
    let cluster = ShardCluster::start(3).await;               // 3 节点，region 0 + 数据 Region 1
    cluster.tick_until_shards(1).await;                        // 仅 region 0 leader 建表
    let ts = cluster.commit_and_ship(&[(b"a", b"1")]).await;   // shard leader 搬运
    cluster.wait_all_replicas(ts, 1).await;                    // 三个节点的 shard 副本都有记录

    let old_leader = cluster.shard_leader().await;
    cluster.shutdown_node(old_leader).await;                   // 关掉 shard leader 的 raft
    let new_leader = cluster.wait_shard_leader_excluding(old_leader).await;
    let ts2 = cluster.commit_and_ship(&[(b"b", b"2")]).await;  // 新 leader 续传
    cluster.wait_all_replicas(ts2, 2).await;
    assert_eq!(cluster.record_count(node_all).await, 2, "no duplicate after retries");
}

#[tokio::test]
async fn metadata_ops_only_follow_region_zero_leader() {
    let cluster = ShardCluster::start(3).await;
    cluster.tick_until_shards(1).await;
    let region0_leader = cluster.region0_leader().await;
    for id in 1..=3 {
        let local = cluster.manager(id).tick().await.unwrap();
        if id != region0_leader {
            assert_eq!(local.opened, 0, "followers never open shards");
        }
    }
    assert_eq!(cluster.manager(region0_leader).table().unwrap().shards.len(), 1);
}
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p coord-server --test kv_t_g4_commit_log_shards_multinode_test`
Expected: 编译失败。

- [ ] **Step 3: 让测试通过**

修正：shard 组初始化竞争（只有 region 0 leader 初始化；重复初始化降级为日志）、follower 的 runtime 目录隔离、shipper 只在本地 shard leader 写、记录去重。不改断言。

- [ ] **Step 4: 运行通过**

Run: `cargo test -p coord-server --test kv_t_g4_commit_log_shards_multinode_test -- --test-threads=1`
Expected: 2 passed。

- [ ] **Step 5: 提交**

```bash
git add coord-server/tests/kv_t_g4_commit_log_shards_multinode_test.rs
git commit -m "test(kv-t): commit log shard replication and leader handover (#154)"
```

---

### Task 8: 文档、门禁、PR

**Files:**
- Modify: `docs/production/consumer-contract.md`（Commit Log shard 语义：分片=存储与 watch 读取；watermark/seal 归 #155；HISTORY_UNAVAILABLE 归 #157）
- Modify: `docs/superpowers/plans/2026-10-20-kv-t-g4-commit-log-shards.md`（本文件，勾选完成项）

- [ ] **Step 1: 文档**

在 consumer-contract 的 Watch/Commit Log 小节补：shard 边界 `[start_ts,end_ts)`、region 0 权威、1M/1h/80%/5m、shard 组按 commit_ts 存储完整记录、读取接口 `records_at/after`。

- [ ] **Step 2: 门禁**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
bash scripts/check-panics.sh
cargo audit
cargo deny check
bash scripts/check-doc-refs.sh
cargo test --workspace
```

Expected: 全绿；`cargo audit` 仅既有已允许告警（与 main 相同）。

- [ ] **Step 3: 提交并开 PR**

```bash
git add docs/
git commit -m "docs(kv-t): document commit log shard boundaries and seal policy (#154)"
git push -u origin feat/kv-t-g4-commit-log-shards
gh pr create --base main --title "feat(kv-t): Commit Log shard raft 组 (#154)" --body "$(cat <<'EOF'
## 变更
- region 0 权威的 Commit Log shard 表：`[start_ts,end_ts)`、Open/SetEnd/Seal、无重叠/空洞
- 每个 shard 独立 Raft 组（独立目录/日志/状态机），按需装配、重启恢复
- 数据 Region outbox → shard 组的幂等搬运（仅 shard leader），per-Region shipped 水位
- D9 阈值：1M/1h seal、80% 预创建、seal 超时 5m；指标与日志可观测

## 验收
- 单节点 e2e：commit → outbox → shard 组，完整 key+value、重放幂等、重启不重复
- 多节点：3 副本收敛、shard leader 迁移后续传不丢不重

Closes #154
EOF
)"
gh pr checks --watch
```

- [ ] **Step 4: 合并**

```bash
gh pr merge --delete-branch --merge
git switch main && git pull
```

---

## Self-Review Notes

- Spec 覆盖：#154 的 shard 模型、raft 组、region 0 元数据、写读接口、leader 转发（以「仅 leader 写 + 副本收敛」实现）、创建/切换/边界/预创建/seal 超时、重启恢复、多节点。
- 明确非范围：#155（watermark 发布与基于 watermark 的 seal）、#157（保留窗口/GC/outbox 清理）、#156（Watch Coordinator 跨 shard 顺序读）、#169（备份携带 commit log）。
- 已知代价：outbox 在 shard 组接管后仍保留（写前日志），空间回收留给 #157；shipper 在 leader 迁移后可能重扫（幂等，不重复计数）。
