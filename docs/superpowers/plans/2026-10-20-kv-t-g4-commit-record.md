# KV-T G4（#153）CommitRecord 格式与写入实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** 为严格 Watch 定义可独立读出的 CommitRecord（完整 key+value、tombstone、prev 值、
zstd、4MiB 写集校验、shard 归属），并在 CommitDecision apply 的同一条 redb 事务里落盘，
做到「不丢、不重、可直接被 Watch 消费、不落明文敏感值」。

**Architecture:** CommitRecord 是**按 Region 的完整变更分片**：prev 值与区间删除的
tombstone 只有数据 Region 的 apply 才知道，因此记录在 `apply_commit_decisions` 的写事务内
构造并写入 `commit_log` 表（`(shard_id, commit_ts, region_id)` 键），值与 MVCC 落盘字节
完全一致（同一 Barrier 信封，保留 key_id）。同一 `commit_ts` 的多个 Region 分片由 #156
按 `commit_ts` 合并为一个原子 WatchEvent。格式自带魔数/版本/zstd 帧与解压上界，读侧不依赖
Region 历史。

**Tech Stack:** Rust、redb、postcard、serde、zstd（新增依赖）、sha2（既有）。

**Spec:** `docs/adr/0015-kv-t.md` D5/D9 + issue #153；前置 #131/#148；消费方 #154/#156；
总表 #127。

## Global Constraints

- `[kv_t].enabled=false`（默认）时不得产生任何 Commit Log 写入，v1 行为字节级不变
  （ADR-0001）。
- **Raft apply 不得因记录侧校验失败返回 `Err`**：用户级/记录级失败只记 error 并继续
  apply（数据可见性与 safe_ts 游标优先）；数据路径不得因 Commit Log 受阻。
- 4MiB 是**事务写集**上限（`txn.write set`，与 Commit 阶段 `max_txn_bytes` 同一口径），
  不是压缩后记录大小；prev 值不计入写集，但计入记录体积。
- 敏感值不得以明文进入 24h 保留的 Commit Log：记录中的 `value`/`prev.value` 必须是
  MVCC 落盘的同一份字节（Barrier envelope 已开启时即密文）。
- 格式版本只增不改；v2 only，不读 v1 changelog 作为替代。
- 不做 shard 管理/预创建/seal（#154）；不做 watermark（#155）；不做 Watch 输出（#156）。
- 不新增公开 RPC，不改 `coord.kv.v2`/`coord.txn.v2`/`coord.watch.v2` 契约字段。
- 不新增 `TODO`/`FIXME`；临时文件只放 `/tmp`；注释只写当前「为什么」。

## Rulings

- **记录按 Region 分片，而非每事务一条**：`prev_kv` 与区间删除的 tombstone 只能由数据
  Region 在 apply 时得到（sequencer 没有旧值）。每事务一条记录就必须回查 Region 或放弃
  `prev_kv`，两者都违反 D5「Watch 不依赖 Region 历史 / prev_kv」。原子性由 #156 按
  `commit_ts` 合并分片保证；成本：Watch 侧需要合并逻辑（#156 已列）。
- **记录与数据 apply 同一事务写入**：跨 raft group 的提交（#154 的 shard group）无法与
  Region apply 原子，本项先落在 Region 后端的 `commit_log` 表——它既是「不丢记录」的
  outbox，也是 #154 shard 状态机的存储原型。成本：若 #154 改为独立 shard group，需要
  一次搬移/重放（已在 #154 的写入接口范围内）。
- **新增 `Command::ApplyCommitDecisionsWithLog` 而不是给 `CommitDecisionApply` 加字段**：
  raft 日志是 postcard（非自描述），给既有结构体加字段会让旧日志解码失败；按仓内既有
  约定末尾追加变体，旧 `ApplyCommitDecisions` 保留解码/回归路径。成本：多一个变体与
  一层视图转换。
- **4MiB 校验在记录头存 `txn_bytes`（按决策的明文 mutation 计）**：apply 里仍拿得到
  mutation 明文；密文/prev 不计入，避免合法 4MiB 事务因加密信封而误拒。成本：记录头
  多 4 字节。
- **记录构建/编码失败只记 error、不阻断 apply**：与「Raft apply 不得返回 Err」一致；
  可重放性由快照/备份修复（#169）与后续容量工作（#174）承担。成本：极端缺陷下 Watch
  历史可能缺一条，但数据可见性不受影响。
- **staging shard=1**：shard 边界/路由是 #154 的范围；本项把 `shard_id` 放进记录与键，
  写入用常量 `COMMIT_LOG_STAGING_SHARD_ID = 1`，让 #154 换实现时不需要改记录格式。
- **`commit_log` 表不进 Region 快照**：快照只导出 `/_sys/*`（TABLE_KV）与 changelog；
  Commit Log 的随快照恢复归 #169。本项不伪装成已覆盖。
- **压缩用 `zstd` crate**：D9 固定 zstd；纯 Rust 的 ruzstd 压缩实现较新，记录是可回放
  数据而非一次性缓存，选参考实现（C 后端由 CI 的 cc 构建）。

## Review Focus

1. **明文泄漏**：开启 Barrier 时，`commit_log` 原始行不得包含用户 value/prev 明文
   （key 与 changelog 同口径为明文）。
2. **重放幂等**：同一决策批重放不得新增行、不得改写行；同键不同内容必须 fail-closed。
3. **Raft 活性**：编码/校验失败不得让 apply 返回 `Err`；数据仍可见、游标仍推进。
4. **tombstone 完整性**：`DELETE_RANGE` 的每个存活 key 都要有 tombstone + prev；已删除
   （`KvMetadata.deleted`）的 key 不得伪造 prev。
5. **解压上界**：解码不得按帧头声明无限分配（zip bomb）；尾随字节/未知版本显式拒绝。
6. **4MiB 口径**：按决策明文写集计，压缩后大小不参与；加密与 prev 不应导致误拒。
7. **关闭态**：`kv_t=false` 时不写 `commit_log`，v1 测试全绿。

---

### Task 1: 记录格式、zstd 编解码与大小校验

**Files:**
- Create: `coord-server/src/storage/commit_log.rs`
- Modify: `coord-server/src/storage/mod.rs`
- Modify: `Cargo.toml`（workspace `[workspace.dependencies]`）
- Modify: `coord-server/Cargo.toml`

**Interfaces:**
- Consumes: `crate::raft::type_config::SequencerMutation`（只用于 `txn_write_bytes`）。
- Produces:

```rust
pub const COMMIT_LOG_MAGIC: [u8; 4] = *b"CLR1";
pub const COMMIT_LOG_FORMAT_VERSION: u8 = 1;
pub const COMMIT_LOG_CODEC_ZSTD: u8 = 1;
pub const COMMIT_LOG_STAGING_SHARD_ID: u64 = 1;
/// 单帧解压上界：写集 4MiB + prev 值/信封开销的硬上界（防 zip bomb）。
pub const COMMIT_LOG_MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitRecord {
    pub shard_id: u64,
    pub commit_ts: u64,
    pub txn_id: Vec<u8>,
    pub start_ts: u64,
    pub region_id: u64,
    /// 决策明文写集字节数（不含 prev；与 Commit 阶段 max_txn_bytes 同口径）
    pub txn_bytes: u32,
    pub changes: Vec<CommitChange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitChange {
    pub key: Vec<u8>,
    /// `None` = tombstone（DELETE / DELETE_RANGE）
    pub value: Option<Vec<u8>>,
    /// 完整旧值（MVCC 落盘字节 + 元数据）；此前不存在/已删除时为 None
    pub prev: Option<CommitPrevValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitPrevValue {
    pub value: Vec<u8>,
    pub create_revision: i64,
    pub mod_revision: i64,
    pub version: i64,
    pub lease_id: i64,
}

impl CommitRecord {
    /// 决策明文的写集字节数：Put key+value+16、Delete key+8、DeleteRange start+end+32
    /// （与 `CommitCandidate::wire_bytes` 的 writes 部分同一口径）。
    pub fn txn_write_bytes(mutations: &[SequencerMutation]) -> u64;
    /// 帧布局：MAGIC(4) | VERSION(1) | CODEC(1) | uncompressed_len(4 BE)
    /// | compressed_len(4 BE) | zstd(postcard(record))。
    pub fn encode(&self, max_txn_bytes: u64) -> Result<Vec<u8>>;
    pub fn decode(bytes: &[u8]) -> Result<Self>;
}
```

- [x] **Step 1: 写失败测试（`commit_log.rs` 的 `#[cfg(test)] mod`）**

```rust
#[test] fn test_commit_record_roundtrip_full_values_and_tombstones() {
    // put c=value（prev=old + 元数据）、delete a（prev=old）、delete b（无 prev）
    // encode -> decode 与原记录逐字段相等；tombstone 的 value 必须是 None
}
#[test] fn test_commit_record_zstd_compresses_repetitive_values() {
    // 64KiB 全 'x' 的 value：encoded.len() < 4096；decode 仍得到原值
}
#[test] fn test_commit_record_rejects_txn_over_limit() {
    // txn_bytes = 4MiB + 1，encode(4MiB) => Err；恰好 4MiB => Ok
}
#[test] fn test_commit_record_decode_rejects_corruption() {
    // 坏魔数/version=2/截断/尾随字节 => Err
}
#[test] fn test_commit_record_decode_bounds_decompression() {
    // 把帧头 uncompressed_len 改成 64MiB+1 => Err（不尝试解压）
}
```

- [x] **Step 2: 跑 RED**

Run: `cargo test -p coord-server --lib commit_record -- --nocapture`
Expected: 编译失败（`commit_log` 模块与类型不存在）。

- [x] **Step 3: 加依赖并实现编码/解码**

```toml
# Cargo.toml [workspace.dependencies]
# Commit Log 记录压缩（ADR-0015 D9 固定 zstd；只用到 bulk compress/decompress）
zstd = { version = "0.14.3", default-features = false }
```

```toml
# coord-server/Cargo.toml [dependencies]
zstd = { workspace = true }
```

- `encode`：先校验 `shard_id > 0`、`commit_ts > 0`、`!txn_id.is_empty()`、
  `txn_bytes as u64 <= max_txn_bytes`；postcard 序列化（`postcard::to_allocvec`）后
  zstd 压缩（`zstd::bulk::compress(&payload, 3)`），帧头写实际长度。
- `decode`：逐字节校验帧头；`uncompressed_len > COMMIT_LOG_MAX_FRAME_BYTES` 直接拒绝；
  `compressed_len` 必须精确等于剩余长度（尾随字节拒绝）；用
  `zstd::bulk::decompress(&frame, uncompressed_len)` 并断言长度一致；postcard 精确消费
  （`postcard::take_from_bytes` + remainder 为空）。
- 所有错误用 `coord_core::error::Error::Storage`/`InvalidArgument`，消息含
  `commit record` 前缀，便于 grep。

- [x] **Step 4: 跑 GREEN**

Run: `cargo test -p coord-server --lib commit_record -- --nocapture`
Expected: 5 个测试 PASS。

- [x] **Step 5: Commit**

```bash
git add Cargo.toml Cargo.lock coord-server/Cargo.toml \
  coord-server/src/storage/commit_log.rs coord-server/src/storage/mod.rs
git commit -m "feat(kv-t): define CommitRecord format with zstd framing (#153)"
```

### Task 2: `commit_log` 表、幂等 append 与有序读

**Files:**
- Modify: `coord-server/src/storage/redb_backend.rs`（表注册）
- Modify: `coord-server/src/storage/mvcc.rs`（表常量、写入/读取方法、测试）
- Modify: `coord-server/src/storage/commit_log.rs`（键编码与 append/read 辅助）

**Interfaces:**
- Consumes: `ReadTx::iter_range_limited`、`WriteTx::{get,insert}`、`prefix_upper_bound`。
- Produces:

```rust
/// 键：/_sys/commit_log/{shard_id:u64be}/{commit_ts:u64be}/{region_id:u64be}
pub(crate) fn encode_commit_log_key(shard_id: u64, commit_ts: u64, region_id: u64) -> Vec<u8>;
pub const COMMIT_LOG_PREFIX: &[u8] = b"/_sys/commit_log/";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitLogAppend { Inserted, Duplicate }

pub(crate) fn append_commit_record_in_tx(
    tx: &mut dyn WriteTx, record: &CommitRecord, max_txn_bytes: u64,
) -> Result<CommitLogAppend>;

impl<B: StorageBackend> MvccStorage<B> {
    pub fn append_commit_record(&self, record: &CommitRecord, max_txn_bytes: u64)
        -> Result<CommitLogAppend>;
    /// 同一 commit_ts 的全部 Region 分片，按 (commit_ts, region_id) 升序。
    pub fn commit_records_at(&self, shard_id: u64, commit_ts: u64) -> Result<Vec<CommitRecord>>;
    /// `(shard_id, commit_ts > after_ts)` 之后最多 limit 行（limit=0 不限）。
    pub fn commit_records_after(&self, shard_id: u64, after_ts: u64, limit: usize)
        -> Result<Vec<CommitRecord>>;
}
```

- [x] **Step 1: 写失败测试**

```rust
#[test] fn test_commit_log_append_is_idempotent_and_rejects_conflicts() {
    // 同记录两次 => Inserted 然后 Duplicate，表内仍 1 行；
    // 同 (shard, ts, region) 不同 value => Err，原行不被覆盖
}
#[test] fn test_commit_log_reads_fragments_in_commit_ts_order() {
    // 写入 (10,r3)、(20,r2)、(10,r1)：
    // after(1,0,0) => [10/r1, 10/r3, 20/r2]；after(1,10,0) => [20/r2]；
    // at(1,10) => [10/r1, 10/r3]；limit=1 只返回第一行
}
#[test] fn test_commit_log_survives_reopen() {
    // append -> drop -> reopen -> commit_records_after 仍读到且内容相等
}
```

- [x] **Step 2: 跑 RED**

Run: `cargo test -p coord-server --lib commit_log_ -- --nocapture`
Expected: 编译失败（表常量/方法不存在）。

- [x] **Step 3: 实现表与读写**

- `redb_backend.rs`：`const TABLE_COMMIT_LOG` 加入 `resolve_table` 与初始化
  `open_table` 列表（与 `changelog`/`kv_meta` 同处）。
- `mvcc.rs`：`pub(crate) const TABLE_COMMIT_LOG: &str = "commit_log";`
- `append_commit_record_in_tx`：编码 → 取同键旧行；存在则解码比较（相等 →
  `Duplicate`；不等或旧行损坏 → `Err(Error::Storage("conflicting commit record ..."))`），
  不存在则 `insert`。
- 读取：`at` 用 `iter_prefix`（`prefix + shard + ts`），`after` 用
  `iter_range_limited(TABLE_COMMIT_LOG, prefix+shard+(after+1)+[0;8],
  prefix_upper_bound(prefix+shard), limit)`；逐行 `CommitRecord::decode`，坏行 fail-closed。
- 表内容不写 changelog、不推进 revision（与 `/_sys/` 内部记录同口径）。

- [x] **Step 4: 跑 GREEN**

Run: `cargo test -p coord-server --lib commit_log_ -- --nocapture`
Expected: 3 个测试 PASS。

- [x] **Step 5: Commit**

```bash
git add coord-server/src/storage/redb_backend.rs coord-server/src/storage/mvcc.rs \
  coord-server/src/storage/commit_log.rs
git commit -m "feat(kv-t): store CommitRecords with idempotent append and ordered reads (#153)"
```

### Task 3: apply 内构造记录（prev/加密/区间删除）与 applier 接线

**Files:**
- Modify: `coord-server/src/raft/type_config.rs`（末尾追加命令变体 + 结构）
- Modify: `coord-server/src/raft/state_machine.rs`（新变体分支）
- Modify: `coord-server/src/storage/mvcc.rs`（共享 apply 核心、prev 捕获、同事务写入）
- Modify: `coord-server/src/kvt/decision_apply.rs`（构造带元数据的批、发新变体）

**Interfaces:**
- Consumes: Task 1 的 `CommitRecord`/`CommitChange`/`CommitPrevValue`；Task 2 的
  `append_commit_record_in_tx`；既有 `KvMetadata`、`encrypt_value`、`encode_kv_key`。
- Produces:

```rust
// type_config.rs（**末尾追加**；旧 ApplyCommitDecisions 变体保留解码路径）
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitDecisionApplyLog {
    pub shard_id: u64,
    pub region_id: u64,
    pub commit_ts: u64,
    pub txn_id: Vec<u8>,
    pub start_ts: u64,
    pub mutations: Vec<SequencerMutation>,
}
Command::ApplyCommitDecisionsWithLog {
    max_txn_bytes: u64,
    decisions: Vec<CommitDecisionApplyLog>,
}

// mvcc.rs
pub fn apply_commit_decisions_with_log(
    &self,
    decisions: &[CommitDecisionApplyLog],
    revision: Revision,
    applied: AppliedLogId,
    max_txn_bytes: u64,
) -> Result<CommitDecisionApplyResult>;

// kvt/decision_apply.rs
fn build_apply_batch(...) -> Vec<CommitDecisionApplyLog>; // 保持现有切批/裁剪逻辑
```

- [x] **Step 1: 写失败测试（`mvcc.rs` + `type_config.rs`）**

```rust
#[test] fn test_apply_decision_with_log_writes_full_record_and_prev() {
    // 先用 put_at_commit_ts 在 ts=5 写 a=old、d=dv、e=ev；
    // 再 apply_commit_decisions_with_log(ts=10：Put c=cv、Delete a、DeleteRange d..f)：
    // at(1,10) 一条分片：c 有 value/无 prev；a tombstone + prev=old；d/e tombstone + prev；
    // 原始行（TABLE_COMMIT_LOG 首行字节）不含 b"cv"/b"old"，decode 后 barrier.decrypt 得明文；
    // 数据可见性与 plain apply 一致（a 删除、c=cv、d/e 删除）
}
#[test] fn test_apply_decision_with_log_replay_writes_record_once() {
    // 同一 (commit_ts, mutations, txn_id) 以 revision+1 再 apply 一次：
    // commit_log 行数不变、结果 decisions 计数正常、无 Err
}
#[test] fn test_apply_decision_with_log_skips_empty_fragment() {
    // mutations 为空（本 Region 无 key）=> 不写记录，游标仍推进
}
#[test] fn test_apply_decision_with_log_oversize_record_keeps_raft_alive() {
    // max_txn_bytes=1 + 2KiB value => apply Ok、数据可见、无记录（只记 error）
}
#[test] fn test_apply_commit_decisions_plain_path_writes_no_records() {
    // 旧 apply_commit_decisions 不产生 commit_log 行（v1/旧日志路径）
}
// type_config.rs
#[test] fn test_command_apply_commit_decisions_with_log_postcard_roundtrip() {
    // 新变体 postcard 往返；旧 ApplyCommitDecisions 仍可解码（末尾追加不漂移）
}
```

- [x] **Step 2: 跑 RED**

Run: `cargo test -p coord-server --lib apply_decision_with_log -- --nocapture`
Expected: 编译失败（新命令/方法不存在）。

- [x] **Step 3: 实现 apply 核心与 prev 捕获**

- 把现有 `apply_commit_decisions` 主体抽成私有
  `apply_commit_decisions_core(&self, views: &[DecisionView<'_>], revision, applied,
  max_txn_bytes)`；`DecisionView` 提供 `commit_ts()` / `mutations()` / `logged()`；
  旧方法传 `max_txn_bytes=0` 且 `logged=None`，新方法传实参。
- Put：在覆盖前读 `TABLE_KV` 旧密文 + `existing_meta`（`!deleted` 才算 prev），
  change = `value: Some(encrypted)`、`prev` 按需；Delete 同理（`value: None`）。
- DeleteRange：沿用现有 `iter_range` 枚举存活 key，为每个 key 读出旧密文与元数据，
  生成 tombstone + prev；`deleted` 的 key 不生成 prev。
- 事务末尾：若 `changes` 非空且 `logged().is_some()`，用
  `CommitRecord::txn_write_bytes(decision.mutations)` 填 `txn_bytes`，构造
  `CommitRecord { shard_id, region_id, commit_ts, txn_id, start_ts, .. }`，在**同一
  redb 写事务**里 `append_commit_record_in_tx`；`Err` 只 `tracing::error!` 不返回。
- `state_machine.rs` 新变体分支与旧分支同返回 `CommitDecisionsApplied`（含
  `through_ts`/`decisions`）；`Command` 的 `Display` 补一行。
- `kvt/decision_apply.rs`：`build_apply_batch` 填 `shard_id:
  COMMIT_LOG_STAGING_SHARD_ID`、`region_id: meta.region_id`、`txn_id`/`start_ts`；
  `client_write(Command::ApplyCommitDecisionsWithLog { max_txn_bytes, decisions })`。

- [x] **Step 4: 跑 GREEN**

Run: `cargo test -p coord-server --lib apply_decision_with_log -- --nocapture`
Run: `cargo test -p coord-server --lib test_command_apply_commit_decisions_with_log -- --nocapture`
Run: `cargo test -p coord-server --lib kvt::decision_apply -- --nocapture`
Expected: 全 PASS（含既有 `build_apply_batch_*` 回归）。

- [x] **Step 5: Commit**

```bash
git add coord-server/src/raft/type_config.rs coord-server/src/raft/state_machine.rs \
  coord-server/src/storage/mvcc.rs coord-server/src/kvt/decision_apply.rs
git commit -m "feat(kv-t): write CommitRecords from decision apply with prev values (#153)"
```

### Task 4: 文档、门禁与 PR

**Files:**
- Modify: `docs/production/consumer-contract.md`（§1 Watch 条目）

- [x] **Step 1: 文档补口径**

在 Watch 条目补：Commit Log 记录按 Region 分片、同一事务的分片按 `commit_ts` 合并为一个
原子事件；记录含完整 key/value 与 prev 值，zstd 压缩；写集 4MiB 上限在 Commit 阶段强制；
值与 MVCC 同信封（保留 `key_id`），DEK 轮换窗口必须 ≥ `watch_retention`（默认 24h，最大
7d）；Commit Log 随快照恢复归 #169。

- [x] **Step 2: 跑本机门禁**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
bash scripts/check-panics.sh
cargo audit
cargo deny check
bash scripts/check-doc-refs.sh
cargo test -p coord-server --lib commit_log -- --nocapture
cargo test -p coord-server --lib apply_decision -- --nocapture
cargo test -p coord-server --test kv_t_g3_commit_decision_test
```

- [x] **Step 3: PR**

- 分支 `feat/kv-t-g4-commit-record`（从最新 `origin/main`）。
- PR 正文：`Closes #153`；写清四个边界：分片按 Region / #154 接管 shard 存储 /
  #169 接管快照携带 / 失败只记 error 不阻断 raft；附写放大估算（记录 ≈ 写集 + prev，
  每 Region 分片有约数十字节元数据开销，zstd 3 级）。
- `gh pr checks --watch` 全绿后 `gh pr merge --merge --delete-branch`。

- [x] **Step 4: 关闭 #153 证据评论**

测试名/命令/结论；`kv_t=false` 无写入；明文检查与 DEK 轮换窗口；4MiB 口径。

## 验收矩阵

| 用例 | 断言 |
|:--|:--|
| 往返 | put/delete/tombstone/prev 全字段相等 |
| 压缩 | 64KiB 重复 value 编码 < 4KiB，解码一致 |
| 4MiB | `txn_bytes > max_txn_bytes` 拒绝；等于上限接受 |
| 损坏 | 坏魔数/未知版本/截断/尾随字节均 Err |
| 解压上界 | 帧头 > 64MiB 直接拒绝，不分配 |
| 幂等 | 同记录重复 append = Duplicate；同键不同内容 Err |
| 顺序 | `(commit_ts, region_id)` 升序；limit 有界 |
| apply 记录 | put 有值、delete/区间删 tombstone、prev 元数据正确 |
| 加密 | 原始行无明文；decode + Barrier 解密得原文；prev 同样 |
| 重放 | 同决策重放不新增/不改写记录 |
| 空分片 | 无本 Region key 不写记录，游标仍推进 |
| 超限 | 记录侧拒绝不阻断 apply，数据仍可见 |
| 旧路径 | 旧 `ApplyCommitDecisions` 不写记录且可解码 |
| 关闭态 | `kv_t=false` 无 `commit_log` 表写入，v1 测试全绿 |
| 快照边界 | 本项**不**声称 Commit Log 随快照恢复（#169） |

## 验证记录

- `cargo test -p coord-server --lib commit_record`：5 passed（往返、zstd 压缩比、
  4MiB 上下界、损坏帧、解压上界）。
- `cargo test -p coord-server --lib commit_log_`：3 passed（幂等/冲突、分片顺序与
  limit、重开恢复）。
- `cargo test -p coord-server --lib apply_decision_with_log`：4 passed（完整记录 +
  prev + 区间删除 tombstone + Barrier 明文检查、重放一次、空分片、超限不阻断）；
  另有 `apply_commit_decisions` 9 passed（含旧路径不写记录）。
- `cargo test -p coord-server --lib test_command_apply_commit_decisions_with_log`：1 passed。
- `cargo test -p coord-server --lib`：795 passed。
- `cargo test -p coord-server --test kv_t_g3_sequencer_test --test kv_t_g3_sequencer_failover_test`：
  4 + 7 passed。
- `cargo clippy -p coord-server --lib -- -D warnings`：0 warning。
- 写放大：记录 ≈ 写集 + prev（区间删除按存活 key 计）；每个 Region 分片额外
  `txn_id/start_ts/帧头` 约数十字节；zstd 只对明文/重复 key 有效，密文 value 近似
  不可压缩（与 MVCC 同信封的必然代价）。
- PR/CI/合并/issue 证据：见 SDD ledger。
