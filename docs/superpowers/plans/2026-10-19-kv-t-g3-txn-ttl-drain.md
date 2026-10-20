# KV-T G3（#150）事务 TTL/abort 清理与 Split Drain 契约 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 v2 事务会话在 TTL 到期后被后台回收，并为 G5 split/merge 提供有界、
可复用的 drain 原语：标记区间后拒绝命中该区间的新读，等待相交会话
commit/abort/TTL 到期，等待本身有超时上限。

**Architecture:** 会话表仍只活在协调节点内存（ADR-0015 D12.2）。`TxnSessions`
内部改为 `Arc<Mutex<TxnTable>>`（sessions + 当前 draining 区间），因此后台 reaper
与 `DrainGuard` 共享同一张表。`DrainGuard` 在 drop 时解除标记；`wait()` 轮询
（10ms）并在每次轮询前清掉过期会话。OCC 读阶段没有 intent，abort 只需丢弃会话；
读集之外的盲写由 #151/#163 在提交/路由层拒绝，不在本项。

**Tech Stack:** Rust、tokio（`time`/`spawn`）、parking_lot、tonic（错误映射）。

**Spec:** `docs/adr/0015-kv-t.md` D4/D6/D12.2 + issue #150；前置 #149；
消费方 #151（Commit 必须移除会话并检查写集）与 #163（split drain）。

## Global Constraints

- `[kv_t].enabled=false`（默认）时不注册 v2 RPC 行为、不启动任何后台任务，
  v1 行为字节级不变（ADR-0001）；reaper 只在 enabled 分支启动。
- 会话 TTL 从 `Begin` 起算，`Read` **不续期**（proto 无心跳/续期 RPC；#129 口径）。
- 到期/丢失会话的线上结果保持 `ABORTED` + `TXN_NOT_FOUND`，客户端重跑整个事务。
- drain 拒绝新读时用**可重试**错误（`UNAVAILABLE`），不是 `TXN_CONFLICT`、
  不是 `TXN_NOT_FOUND`：会话仍有效，退避后同一会话可继续读区间外的 key。
- drain 等待必须有界：`wait(timeout)` 最迟在 deadline 返回，不得无限阻塞；
  超时返回仍相交的会话数（诊断/调用方决定回滚）。
- 读阶段没有 intent/lock：`abort` 只删除会话（读集/临时状态随会话一起释放），
  不写 MVCC/决策；已决策事务的 apply 不因 abort 丢失（#148 路径）。
- 不新增 `TODO`/`FIXME`；临时文件只放 `/tmp`；注释只写当前「为什么」。

## Rulings

- **会话表用 `Arc<Mutex<TxnTable>>` 而不是给字段换类型**：后台 reaper 与 guard
  需要共享表，但 `CoordNode.txn_sessions` 保持 `TxnSessions`，避免改动所有调用点；
  clone 的是内部 `Arc`，不是会话。
- **drain 的相交判定覆盖 point 与 range，空谓词不算**：空谓词读不到任何 key，
  不应阻止 split；相邻但不重叠的区间（`[a,c)` vs `[c,e)`）也没有共同 key，不阻塞。
- **drain 在 `record_read` 里先于读集增长判定**：被拒绝的读不得进入读集
  （否则 drain 会因被拒绝的读而延长）。
- **drain 拒绝映射为 `UNAVAILABLE`**：split 尚未 cutover，`EPOCH_STALE` 留给
  #161 的 cutover 后路径；两者对客户端都是「退避重试」，但语义不同。
- **后台 reaper 周期由调用方传入**（main 取 `txn_ttl_secs / 4` 钳制到 1–30s）：
  周期是运行策略，不写进 `KvTRuntimeConfig`，避免新增一个没有 ADR 依据的配置项。
- **不做事件通知，轮询 10ms**：split 是低频运维动作；为 drain 引入 notify 会
  在每次 abort/commit 上付代价，收益不成比例。

## Review Focus

1. **TTL 不被读续期**：连续 Read 的长事务仍应在 `Begin + ttl` 到期；不得出现
   “读一次就续命”的隐式行为。
2. **无关会话不阻塞 drain**：drain `[c,e)` 时，只读 `[a,c)` 的会话必须立刻放行。
3. **空谓词/空区间**：`[z,a)` 空谓词、空 drain 区间不得触发 Draining、不得阻塞。
4. **guard 生命周期**：drop 后读立即恢复；两个相同区间的 drain 必须各自计数，
   drop 一个不能清掉另一个的标记。
5. **过期会话不永久阻塞**：TTL 到期后 `wait` 必须在下一次轮询清掉它并返回
   `Drained`；`wait` 超时必须有界并报告 active 数。
6. **关闭态零副作用**：reaper 只在 `[kv_t].enabled` 分支启动；v1 测试不变。

---

### Task 1: TTL 后台回收

**Files:**
- Modify: `coord-server/src/kvt/txn.rs`
- Modify: `coord-server/src/server/mod.rs`（`start_kvt_txn_reaper` 委托）
- Modify: `coord/src/main.rs`（enabled 分支启动）

**Interfaces:**
- Produces: `TxnSessions::purge_expired(&self) -> usize`、
  `TxnSessions::spawn_reaper(&self, interval: Duration) -> tokio::task::JoinHandle<()>`、
  `CoordNode::start_kvt_txn_reaper(self: &Arc<Self>, interval: Duration)`。
- Consumes: 既有 `TxnSessions::begin/abort`、`TxnSession.expires_at`。

- [ ] **Step 1: 写失败测试（`coord-server/src/kvt/txn.rs` 的 tests 模块）**

```rust
#[test]
fn purge_expired_removes_only_expired_sessions_and_reports_count() {
    let sessions = TxnSessions::new();
    let long = sessions.begin(7, Duration::from_secs(30));
    let short = sessions.begin(8, Duration::from_millis(1));
    std::thread::sleep(Duration::from_millis(5));

    assert_eq!(sessions.purge_expired(), 1);
    assert_eq!(sessions.len(), 1);
    assert!(sessions.read_set(&long).is_some());
    assert!(sessions.read_set(&short).is_none());
    // 幂等：没有新过期会话时返回 0。
    assert_eq!(sessions.purge_expired(), 0);
}

#[tokio::test]
async fn reaper_reclaims_expired_sessions() {
    let sessions = TxnSessions::new();
    let _reaper = sessions.spawn_reaper(Duration::from_millis(5));
    let _id = sessions.begin(9, Duration::from_millis(1));
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(sessions.len(), 0);
}
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p coord-server --lib kvt::txn::tests::purge_expired_removes_only_expired_sessions_and_reports_count`
Expected: FAIL — `purge_expired` 返回 `()`，且 `len`/`spawn_reaper` 不存在。

- [ ] **Step 3: 实现**

`coord-server/src/kvt/txn.rs`：

```rust
struct TxnTable {
    sessions: BTreeMap<Vec<u8>, TxnSession>,
    drains: Vec<ReadRange>,
}

pub(crate) struct TxnSessions {
    table: Arc<Mutex<TxnTable>>,
}

impl TxnSessions {
    pub(crate) fn new() -> Self { /* Arc::new(Mutex::new(TxnTable::default())) */ }
    pub(crate) fn purge_expired(&self) -> usize { /* lock + sweep */ }
    pub(crate) fn spawn_reaper(&self, interval: Duration) -> tokio::task::JoinHandle<()> { /* tokio::spawn tick loop */ }
    #[cfg(test)]
    fn len(&self) -> usize { /* table.sessions.len() */ }
}

fn sweep(table: &mut TxnTable, now: Instant) -> usize {
    let before = table.sessions.len();
    table.sessions.retain(|_, session| session.expires_at > now);
    before - table.sessions.len()
}
```

- `TxnTable` 加 `DrainGuard` 用的 `drains`（Task 2 使用；本任务可为空 Vec）。
- `begin` 在插入前调用 `sweep`（原 `purge_expired()`）。
- reaper 用 `tokio::time::interval(interval)` + `MissedTickBehavior::Delay`；
  每 tick 只做 `sweep`，不读配置、不碰 raft。
- `coord-server/src/server/mod.rs`：

```rust
impl CoordNode {
    /// 启动 v2 事务会话 TTL 回收（仅 `[kv_t].enabled` 时由 main 调用）。
    pub fn start_kvt_txn_reaper(self: &Arc<Self>, interval: std::time::Duration) {
        self.txn_sessions.spawn_reaper(interval);
    }
}
```

- `coord/src/main.rs` 在现有 `if cfg.kv_t.enabled && node.raft.is_some()` 分支
  （`start_kvt_lease_*` 旁）追加：

```rust
node.start_kvt_txn_reaper(std::time::Duration::from_secs(
    (cfg.kv_t.txn_ttl_secs / 4).clamp(1, 30),
));
```

- [ ] **Step 4: 运行确认通过**

Run: `cargo test -p coord-server --lib kvt::txn::`
Expected: PASS（既有会话测试 + 新增 2 个）。

- [ ] **Step 5: 提交**

```bash
git add coord-server/src/kvt/txn.rs coord-server/src/server/mod.rs coord/src/main.rs
git commit -m "feat(kv-t): reap expired txn sessions in background (#150)"
```

---

### Task 2: Split drain 原语、读准入与契约文档

**Files:**
- Modify: `coord-server/src/kvt/txn.rs`（`ReadRange::overlaps`、`ReadSet::intersects`、
  `ReadIntent::intersects`、`DrainGuard`/`DrainOutcome`、`SessionError::Draining`）
- Modify: `coord-server/src/kvt/services.rs`（`txn_session_status` 映射 + 单元测试）
- Modify: `docs/production/ops/boundaries.md`（B-KVT-9）
- Modify: `docs/production/consumer-contract.md`（事务 bullet）

**Interfaces:**
- Produces:
  - `ReadSet::intersects(&self, range: &ReadRange) -> bool`
  - `TxnSessions::drain(&self, range: ReadRange) -> DrainGuard`
  - `DrainGuard::wait(&self, timeout: Duration) -> DrainOutcome`
  - `enum DrainOutcome { Drained, TimedOut { active: usize } }`
  - `SessionError::Draining`
- Consumes: Task 1 的 `TxnTable`/`sweep`。

- [ ] **Step 1: 写失败测试（`txn.rs` tests 模块）**

```rust
#[test]
fn read_set_intersects_only_real_key_overlap() {
    let mut set = ReadSet::default();
    set.try_add_point(b"d", 10).unwrap();
    set.try_add_range(b"a", b"c", 10).unwrap();
    set.try_add_range(b"z", b"a", 10).unwrap(); // 空谓词

    assert!(set.intersects(&ReadRange::new(b"c", b"e"))); // d 命中
    assert!(set.intersects(&ReadRange::new(b"b", b"b1"))); // range 重叠
    assert!(!set.intersects(&ReadRange::new(b"c", b"d"))); // 相邻 [a,c) 无共同 key
    assert!(!set.intersects(&ReadRange::new(b"e", b"f")));
    assert!(!set.intersects(&ReadRange::new(b"z", b"a"))); // 空 drain 区间
}

#[tokio::test]
async fn drain_waits_for_intersecting_session_and_times_out() {
    let sessions = TxnSessions::new();
    let id = sessions.begin(1, Duration::from_secs(30));
    sessions
        .record_read(&id, ReadIntent::Point(b"d"), 10)
        .unwrap();
    let guard = sessions.drain(ReadRange::new(b"c", b"e"));

    let outcome = guard.wait(Duration::from_millis(30)).await;
    assert_eq!(outcome, DrainOutcome::TimedOut { active: 1 });

    sessions.abort(&id);
    assert_eq!(
        guard.wait(Duration::from_secs(1)).await,
        DrainOutcome::Drained
    );
}

#[tokio::test]
async fn drain_ignores_unrelated_and_expired_sessions() {
    let sessions = TxnSessions::new();
    let unrelated = sessions.begin(1, Duration::from_secs(30));
    sessions
        .record_read(&unrelated, ReadIntent::Range { start: b"a", end: b"c" }, 10)
        .unwrap();
    let expired = sessions.begin(1, Duration::from_millis(1));
    sessions
        .record_read(&expired, ReadIntent::Point(b"d"), 10)
        .unwrap();
    std::thread::sleep(Duration::from_millis(5));

    let guard = sessions.drain(ReadRange::new(b"c", b"e"));
    assert_eq!(
        guard.wait(Duration::from_millis(50)).await,
        DrainOutcome::Drained
    );
}

#[tokio::test]
async fn drain_rejects_new_reads_only_inside_range() {
    let sessions = TxnSessions::new();
    let id = sessions.begin(1, Duration::from_secs(30));
    let guard = sessions.drain(ReadRange::new(b"c", b"e"));

    assert_eq!(
        sessions.record_read(&id, ReadIntent::Point(b"d"), 10),
        Err(SessionError::Draining)
    );
    assert_eq!(
        sessions.record_read(&id, ReadIntent::Range { start: b"b", end: b"d" }, 10),
        Err(SessionError::Draining)
    );
    assert_eq!(
        sessions.record_read(&id, ReadIntent::Point(b"a"), 10),
        Ok(1)
    );
    // 被拒绝的读不进读集：读集仍只有 a。
    assert_eq!(sessions.read_set(&id).unwrap().len(), 1);

    drop(guard);
    assert_eq!(
        sessions.record_read(&id, ReadIntent::Point(b"d"), 10),
        Ok(1)
    );
}

#[tokio::test]
async fn nested_drains_release_independently() {
    let sessions = TxnSessions::new();
    let id = sessions.begin(1, Duration::from_secs(30));
    let first = sessions.drain(ReadRange::new(b"c", b"e"));
    let second = sessions.drain(ReadRange::new(b"c", b"e"));

    drop(first);
    assert_eq!(
        sessions.record_read(&id, ReadIntent::Point(b"d"), 10),
        Err(SessionError::Draining)
    );
    drop(second);
    assert_eq!(
        sessions.record_read(&id, ReadIntent::Point(b"d"), 10),
        Ok(1)
    );
}
```

`services.rs` 末尾新增单元测试模块：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use coord_core::error_code::error_code_of;

    #[test]
    fn draining_session_error_is_retryable_unavailable() {
        let status = txn_session_status(SessionError::Draining);
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(error_code_of(&status).as_deref(), Some("UNAVAILABLE"));
    }
}
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p coord-server --lib kvt::txn::tests::drain_rejects_new_reads_only_inside_range`
Expected: FAIL — `drain`/`DrainOutcome`/`Draining` 不存在。

- [ ] **Step 3: 实现**

```rust
impl ReadRange {
    /// 有共同 key（相邻区间不算重叠）。
    pub(crate) fn overlaps(&self, other: &Self) -> bool {
        !self.is_empty() && !other.is_empty()
            && self.start < other.end && other.start < self.end
    }
}

impl ReadSet {
    pub(crate) fn intersects(&self, range: &ReadRange) -> bool {
        if range.is_empty() { return false; }
        self.points.iter().any(|point| range.contains(point))
            || self.ranges.iter().any(|r| r.overlaps(range))
    }
}

impl ReadIntent<'_> {
    fn intersects(&self, range: &ReadRange) -> bool { /* Point → contains; Range → overlaps */ }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DrainOutcome {
    Drained,
    /// 截止时仍有会话读集与 drain 区间相交。
    TimedOut { active: usize },
}

pub(crate) struct DrainGuard {
    table: Arc<Mutex<TxnTable>>,
    range: ReadRange,
}

impl TxnSessions {
    pub(crate) fn drain(&self, range: ReadRange) -> DrainGuard { /* push range; clone Arc */ }
}

impl DrainGuard {
    pub(crate) async fn wait(&self, timeout: Duration) -> DrainOutcome {
        // loop: lock → sweep → count intersecting → 0 ⇒ Drained;
        //       deadline 已过 ⇒ TimedOut; 否则 sleep(min(10ms, 剩余))
    }
}

impl Drop for DrainGuard {
    fn drop(&mut self) { /* 移除一个相同 range 条目 */ }
}
```

- `record_read`：取到会话后、写读集前：
  `if table.drains.iter().any(|d| intent.intersects(d)) { return Err(SessionError::Draining); }`
  （过期会话仍先返回 `NotFound`；被拒绝的读不增长读集）。
- `SessionError` 增加 `Draining`；`txn_session_status` 映射：

```rust
SessionError::Draining => attach(
    Status::unavailable("key range is draining for split; retry with backoff"),
    CoordErrorCode::Unavailable,
),
```

- 模块文档补三句：TTL 不续期、abort 无 intent、drain 的等待/拒绝/边界
  （写集由 #151/#163 在提交/路由层检查）。

- `docs/production/ops/boundaries.md` 的 B-KVT 表追加一行：

```markdown
| B-KVT-9 | **事务 TTL 不续期；split drain 只覆盖读集**：TTL 从 `Begin` 起算，`Read` 不延长；到期回收后迟到请求 `ABORTED` + `TXN_NOT_FOUND`。drain 是协调节点本地原语：标记区间后拒绝与该区间相交的新读（`UNAVAILABLE`，退避重试），等待已有会话 commit/abort/TTL，等待有超时上限；OCC 读阶段没有 intent，abort 只丢弃会话。读集之外的盲写由 #151 提交路径与 #163 cutover/epoch 拒绝 | ADR-0015 D4/D6/D12.2；`coord-server/src/kvt/txn.rs`（`DrainGuard`/`SessionError::Draining`）、`coord-server/src/kvt/services.rs`（`txn_session_status`） | 复制会话/跨节点 drain（成本高且不改变原子性证明）；盲写保护在 #151/#163 落地前不承诺 |
```

- `docs/production/consumer-contract.md` 事务 bullet 补：
  「TTL 从 `Begin` 起算、不续期；split drain 期间命中被 drain 区间的 `Read`
  返回 `UNAVAILABLE`（退避后重试同一会话或重跑）。」

- [ ] **Step 4: 运行确认通过**

Run: `cargo test -p coord-server --lib kvt::txn:: && cargo test -p coord-server --lib kvt::services::tests`
Expected: PASS（Task 1 测试 + 新增 5 个 drain 测试 + 1 个映射测试）。

- [ ] **Step 5: 提交**

```bash
git add coord-server/src/kvt/txn.rs coord-server/src/kvt/services.rs \
  docs/production/ops/boundaries.md docs/production/consumer-contract.md
git commit -m "feat(kv-t): add bounded split drain for txn sessions (#150)"
```

---

### Task 3: 门禁、PR 与证据

**Files:** 无（只跑命令与 PR 描述）

- [ ] **Step 1: 勾选本计划并写验证记录**（测试名/命令/边界）
- [ ] **Step 2: 本机跑 CI 基础门禁**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
bash scripts/check-panics.sh
cargo audit
cargo deny check
cargo test -p coord-server --lib kvt::
cargo test -p coord --test kv_t_g3_commit_decision_test
cargo test -p coord-server --test kv_v2_txn_read_test
bash scripts/check-doc-refs.sh
```

- [ ] **Step 3: PR**

分支 `feat/kv-t-g3-txn-ttl-drain`，正文 `Closes #150`，写清：

- TTL 从 Begin 起算、Read 不续期；后台 reaper 周期由 main 传入。
- drain 是协调节点本地原语；拒绝新读 = `UNAVAILABLE`（不是冲突/会话丢失）；
  等待有 timeout，超时报告 active。
- 读阶段无 intent；abort 只丢弃会话；盲写保护归 #151/#163。
- 关闭态零后台任务。

`gh pr checks --watch` 全绿后 `gh pr merge --merge --delete-branch`。

- [ ] **Step 4: 关闭 #150 的证据评论**（测试名/命令/上限/与 #163 接口）

## 验收矩阵

| 用例 | 断言 |
|:--|:--|
| TTL 回收 | 过期会话被 sweep 删除并计数；未过期保留 |
| 后台 reaper | 无请求时过期会话也被回收 |
| 读不续期 | 连续 Read 不改变 `expires_at`（由既有 TTL 测试 + 无续期代码保证） |
| 无关会话 | 只读 `[a,c)` 的会话不阻塞 drain `[c,e)` |
| 空谓词/空区间 | 不触发 Draining、不阻塞 |
| 准入拒绝 | 相交读 ⇒ `Draining`（UNAVAILABLE）；被拒绝的读不进读集 |
| 有界等待 | 超时返回 `TimedOut { active }`；abort 后返回 `Drained` |
| 嵌套 drain | drop 一个 guard 不清除另一个标记 |
| 过期会话 | 到期后 drain 在下一次轮询放行 |
| 关闭态 | reaper 只在 enabled 分支启动；v1 测试不变 |

## 验证记录

- `cargo test -p coord-server --lib kvt::`：24 passed（既有 txn/lease/sequencer/safe_ts 回归 +
  新增 2 个 TTL 测试 + 5 个 drain 测试 + 1 个错误映射测试）。
- `cargo test -p coord-server --lib kvt::txn::`：14 passed（含 `purge_expired_removes_only_expired_sessions_and_reports_count`、
  `reaper_reclaims_expired_sessions`、`read_set_intersects_only_real_key_overlap`、
  `drain_waits_for_intersecting_session_and_times_out`、`drain_ignores_unrelated_and_expired_sessions`、
  `drain_rejects_new_reads_only_inside_range`、`nested_drains_release_independently`）。
- drain 轮询 10ms；`wait(30ms)` 在仍有相交会话时返回 `TimedOut { active: 1 }`，
  abort 后 `wait(1s)` 返回 `Drained`；TTL 到期会话在下一次轮询被清掉。
- 关闭态：reaper 只在 `cfg.kv_t.enabled && node.raft.is_some()` 分支启动。
- Ruling（执行中修正计划）：`drain_waits` 测试必须先读、后 `drain`；先 drain 后读会被正确拒绝
  （`Draining`），这是准入行为而非缺陷。
