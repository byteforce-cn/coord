# KV-T G3（#152）Sequencer 故障切换与重复决策 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** 用进程内 3 节点 region 0 raft 证明 Commit Sequencer 在 leader kill /
分区 / 重启后不丢已写决策、不产生第二个决策或重复 `commit_ts`，并让未完成
提案按 `txn_id`/`request_id` 重试后可继续；补齐批内部分失败的 apply 语义测试。

**Architecture:** sequencer 是 region 0 system raft 上的确定性状态机（#146），
`commit_ts` 来自同一 raft 组的 TSO（#136）——因此故障切换后新 leader 从已
持久化的 TSO 高水位 + `SEQUENCER_LAST_COMMIT_TS` 继续，旧 leader 未提交的
提案要么随日志保留（先于新提案 apply）、要么被截断（时间戳成洞、不复用）。
本项不改协议，新增 3 节点可 kill/restart/partition 的测试夹具，用真实
raft 复制验证这些不变量；仅当测试暴露缺口才改实现。

**Tech Stack:** Rust、openraft/redb、tokio、tonic（无新增依赖/RPC/配置项）。

**Spec:** `docs/adr/0015-kv-t.md` D2/D4/D9/D11/D12.2 + issue #152；前置
#136/#137（TSO）、#146（sequencer）、#148（决策 apply）；下游 #168（e2e）、
#175（进程级混沌）；总表 #127。

## Global Constraints

- `[kv_t].enabled=false`（默认）时 v1 行为字节级不变（ADR-0001）；本项只动
  测试、`RaftRpcService` 的 Clone 可见性与文档。
- `commit_ts` 全局唯一、严格单调、绝不复用（D11.1/D11.2）；故障留下的空洞
  合法（D2）。
- 同一 `txn_id` 永不产生第二个 `CommitDecision`；同一 `txn_id` + `request_id`
  的重试返回首次 `commit_ts`（D12.2）。
- 冲突/重复/乱序仍是 `SequencerOutcome`，不得让 raft apply 返回 `Err`（D11.5）。
- 不在本项承诺：进程级 SIGSTOP/真实网络分区（#175）、跨 Region e2e 可见性
  （#168）、多组 sequencer 分片（D4 不承诺）。
- 不新增 `TODO`/`FIXME`；注释只写当前「为什么」；临时文件只放 `/tmp`。

## Rulings

- **kill = `raft.shutdown()`，保留 gRPC listener。** 入站 raft RPC 在核心停止后
  返回错误，集群视角等价于节点失联；listener 不释放，重启时不需要重新绑定
  端口，避免 `coord-server/tests/common` 明确禁止的「释放端口后重绑」竞态。
- **重启 = 重开 redb + log store + 新 raft，替换 `RaftRpcService` 里的实例。**
  `RaftRpcService` 的字段全是 `Arc`，给它加 `#[derive(Clone)]` 让测试与服务任务
  共享同一张 region→raft 表；这是测试可见性改动，不改变生产行为。
- **pause 用 partition 覆盖。** 对 raft 来说「节点冻结」与「双向不可达」不可
  区分：partition 后旧 leader 拿不到 quorum、其余节点选新 leader，恢复后旧
  节点按更高 term 降级并追日志。进程级 SIGSTOP 证据归 #175。
- **分区用既有 `RaftNetworkFactoryImpl::block_node` 双向封锁**（每个节点封
  对方），不新造网络层；这是仓库既有的 partition-sim 钩子（见 `network.rs`）。
- **「无部分可见」在本项只断言决策级原子性**：冲突/非法不写决策；已提交事务
  的所有 mutation 在同一个 `SequencerDecision` 里。跨 Region 的数据可见性由
  safe_ts 收敛，e2e 归 #168。
- **批内部分失败不阻塞后续事务**：apply 逐条给出 outcome，冲突/乱序/非法
  只跳过自身；同一命令里后续合法事务仍提交，决策日志允许时间戳空洞。
- **重试辅助只吞可重试错误**：测试里对 `NotLeader`/`ClusterUnavailable` 做
  有界重试；`Internal`/存储错误直接失败，避免测试掩盖真实回归。

## Review Focus

1. **已决策不丢**：新 leader 上任后重试已提交事务必须返回首次 `commit_ts`，
   决策日志里该 `txn_id` 只有一条。
2. **未决策可重试**：kill 打断提案后，重试要么拿到首次决策（提案已提交），
   要么以更大 `commit_ts` 提交；两种路径都只能有一条决策。
3. **时间戳不复用**：故障切换前后所有决策的 `commit_ts` 互不相同且严格递增
   （按日志顺序），重试返回的旧值不参与新分配。
4. **批边界**：`max_batch_count=1` 的多候选提交被 kill 打断后，已提交批次
   幂等回放、未提交批次继续，不出现半批丢失或重复决策。
5. **恢复收敛**：kill 的节点重启后能应用故障期间的决策；分区节点愈合后
   不得用旧 term 覆盖新 leader 的日志。
6. **关闭态**：`[kv_t].enabled=false` 路径不因测试夹具改动而变化。

---

### Task 1: 批内部分失败与同批重复决策（apply 层）

**Files:**
- Modify: `coord-server/src/storage/mvcc.rs`（`#[cfg(test)] mod`）

**Interfaces:**
- Consumes: 既有 `seq_op`/`seq_txn` 测试辅助、`apply_sequencer_op`、
  `sequencer_decisions_after`、`sequencer_last_commit_ts`。
- Produces: 两个测试名（供 #152 证据引用）：
  `test_sequencer_batch_rejections_do_not_block_later_txns`、
  `test_sequencer_duplicate_in_same_batch_writes_one_decision`。

- [x] **Step 1: 写失败测试**

```rust
#[test]
fn test_sequencer_batch_rejections_do_not_block_later_txns() {
    // 同一个 ValidateAndCommit 里依次：
    //   t1 start=1 ts=10 writes b            -> Committed(10)
    //   t2 start=1 ts=9  writes x            -> OutOfOrder{9, last=10}
    //   t3 start=5 ts=12 reads b             -> Conflict{12, b, 10}
    //   t4 start=20 ts=20 writes c           -> Invalid(CommitTsNotAboveStart)
    //   t5 start=12 ts=13 writes d           -> Committed(13)
    // 断言：outcome 与输入一一对应；决策只有 10/13；
    // sequencer_decisions_after(0, 16) == [10, 13]（空洞不阻塞扫描）。
}

#[test]
fn test_sequencer_duplicate_in_same_batch_writes_one_decision() {
    // 同一条命令里 t1 出现两次（ts=10/11，同 txn_id + request_id）：
    //   -> [Committed(10), Duplicate(10)]
    // 断言 get_sequencer_decision(11) 为 None、last_commit_ts == 10、
    // 决策扫描只有一条。
}
```

- [x] **Step 2: 运行确认失败**

Run: `cargo test -p coord-server --lib test_sequencer_batch_rejections_do_not_block_later_txns test_sequencer_duplicate_in_same_batch`
Expected: FAIL（测试不存在）。若测试直接通过，说明该语义已被现有测试覆盖：
保留测试作为 #152 的锚点，并在提交信息里注明。

- [x] **Step 3: 实现**

只写测试；若断言暴露实现缺口，按 `systematic-debugging` 定位并修最小实现
（预期不需要：`sequencer_validate_in_tx` 已逐条返回 outcome）。

- [x] **Step 4: 运行确认通过**

Run: `cargo test -p coord-server --lib test_sequencer_batch_rejections_do_not_block_later_txns test_sequencer_duplicate_in_same_batch`
Expected: PASS（2 passed）。

- [x] **Step 5: 提交**

```bash
git add coord-server/src/storage/mvcc.rs
git commit -m "test(kv-t): pin sequencer batch rejection and duplicate semantics (#152)"
```

---

### Task 2: 3 节点故障切换夹具与本位重放

**Files:**
- Modify: `coord-server/src/raft/network.rs`（`RaftRpcService` 加 `#[derive(Clone)]`）
- Create: `coord-server/tests/kv_t_g3_sequencer_failover_test.rs`

**Interfaces:**
- Produces（测试文件内部）：
  - `struct FailoverCluster { _dir: TempDir, nodes: Vec<FailoverNode> }`
  - `struct FailoverNode { id: u64, dir: PathBuf, factory: RaftNetworkFactoryImpl,
    service: RaftRpcService, raft: Option<Arc<CoordRaft>>,
    mvcc: Option<Arc<MvccStorage<RedbBackend>>>, _server: JoinHandle<()> }`
  - `FailoverCluster::start() -> Self`（3 节点、共同 membership、每节点
    `store_dir = _dir/node-{id}`）
  - `FailoverCluster::leader(&self) -> u64`、`wait_leader_among(ids, timeout) -> u64`
  - `FailoverCluster::tso(&self, id) -> TsoClient`
  - `FailoverCluster::sequencer(&self, id) -> Arc<CommitSequencer>`（256/batch）
  - `FailoverCluster::sequencer_with_batch(&self, id, max_count) -> Arc<CommitSequencer>`
  - `failover_commit_until(seq, candidates, deadline)`：只重试
    `NotLeader`/`ClusterUnavailable`。
  - `decisions(&self, id) -> Vec<(u64, Vec<u8>)>`（`commit_ts, txn_id`）。
- Consumes: `common::bind_listener`、`RaftRpcService`、`TsoAllocator`/`TsoClient`、
  `CommitSequencer`/`CommitCandidate`、`RevisionMode::CommitTs`。

- [x] **Step 1: 写失败测试**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_sequencer_failover_replays_and_continues() {
    let mut cluster = FailoverCluster::start().await;
    let leader = cluster.leader().await;
    let start = cluster.tso(leader).get_ts().await.unwrap();
    let ts_a = committed_ts(
        failover_commit_until(
            cluster.sequencer(leader),
            vec![candidate(b"failover-a", start, b"req-a", &[], &[(b"a", b"1")])],
            DEADLINE,
        ).await,
    );

    cluster.kill(leader).await;
    let alive: Vec<u64> = cluster.ids().into_iter().filter(|id| *id != leader).collect();
    let new_leader = cluster.wait_leader_among(&alive, Duration::from_secs(10)).await;

    // 已决策不丢：同 txn_id + request_id 在新 leader 上返回首次值。
    let replay = failover_commit_until(
        cluster.sequencer(new_leader),
        vec![candidate(b"failover-a", start, b"req-a", &[], &[(b"a", b"1")])],
        DEADLINE,
    ).await;
    assert_eq!(replay, vec![SequencerOutcome::Duplicate { commit_ts: ts_a }]);

    // 继续服务：新事务拿到更大的 commit_ts。
    let start_b = cluster.tso(new_leader).get_ts().await.unwrap();
    let ts_b = committed_ts(
        failover_commit_until(
            cluster.sequencer(new_leader),
            vec![candidate(b"failover-b", start_b, b"req-b", &[], &[(b"b", b"2")])],
            DEADLINE,
        ).await,
    );
    assert!(ts_b > ts_a);

    for id in &alive {
        let mut decisions = cluster.decisions(*id);
        decisions.sort();
        assert_eq!(
            decisions,
            vec![(ts_a, b"failover-a".to_vec()), (ts_b, b"failover-b".to_vec())]
        );
    }
}
```

`FailoverCluster::start` 的装配（每节点同：redb → `MvccStorage::new_with_revision_mode(.., CommitTs)`
→ `LogStore` → `StateMachineStore` → `new_raft` → `RaftRpcService::set_raft` →
`serve_with_incoming`），3 节点共同 `initialize`，并等待三节点对 leader 达成一致。

- [x] **Step 2: 运行确认失败**

Run: `cargo test -p coord-server --test kv_t_g3_sequencer_failover_test test_sequencer_failover_replays_and_continues`
Expected: FAIL — 测试文件/夹具不存在。

- [x] **Step 3: 实现夹具与测试**

- `RaftRpcService` 加 `#[derive(Clone)]`（注释：字段均为 `Arc`，clone 共享
  region→raft 表；测试重启时替换实例而不重绑 listener）。
- 实现 `kill`：`service.remove_region_raft(0)` → `raft.shutdown().await` →
  取走并 drop raft/mvcc（`Option` 置 None）。**不 abort server**。
- 实现 `restart`：重开 redb/log store/state machine → `new_raft` →
  `service.set_raft`，恢复 `raft`/`mvcc`。
- `candidate(...)` 与 `committed_ts(...)` 复制 `kv_t_g3_sequencer_test.rs` 的
  同名辅助语义（该文件未导出，避免跨文件耦合）。

- [x] **Step 4: 运行确认通过**

Run: `cargo test -p coord-server --test kv_t_g3_sequencer_failover_test test_sequencer_failover_replays_and_continues`
Expected: PASS（1 passed）。

- [x] **Step 5: 提交**

```bash
git add coord-server/src/raft/network.rs coord-server/tests/kv_t_g3_sequencer_failover_test.rs
git commit -m "test(kv-t): prove sequencer replay after leader failover (#152)"
```

---

### Task 3: 批边界中断与节点重启恢复

**Files:**
- Modify: `coord-server/tests/kv_t_g3_sequencer_failover_test.rs`

**Interfaces:**
- Consumes: Task 2 的 `FailoverCluster::{kill,restart,sequencer_with_batch,decisions}`、
  `failover_commit_until`。
- Produces: 两个测试名（#152 证据）：
  `test_sequencer_failover_mid_batch_keeps_one_decision_per_txn`、
  `test_sequencer_restart_rejoins_and_replays`。

- [x] **Step 1: 写失败测试**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_sequencer_failover_mid_batch_keeps_one_decision_per_txn() {
    // max_batch_count=1：两个候选必然拆成两次 raft 提案。
    // spawn 提交后 10ms 内 kill leader，再在新 leader 上重试同一对候选。
    // 断言（无论 kill 落在第一批前后）：
    //   - 每个 txn_id 恰好一条决策；
    //   - 重试 outcome 的 commit_ts 与决策一致（Duplicate 用旧值）；
    //   - 决策 ts 两两不同、按扫描顺序严格递增。
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_sequencer_restart_rejoins_and_replays() {
    // 1) leader 提交 A；kill leader；新 leader 提交 B（ts_b > ts_a）。
    // 2) restart 被 kill 的节点；等待它应用 A/B 且视图收敛到新 leader。
    // 3) 在新 leader 上重试 A -> Duplicate(ts_a)，提交 C -> ts_c > ts_b；
    //    等待重启节点也应用 C。
    // 4) restart 节点的决策集合 == 存活节点 == {A,B,C}，无重复 ts。
}
```

- [x] **Step 2: 运行确认失败**

Run: `cargo test -p coord-server --test kv_t_g3_sequencer_failover_test test_sequencer_failover_mid_batch test_sequencer_restart_rejoins`
Expected: FAIL — 测试不存在。

- [x] **Step 3: 实现**

- `sequencer_with_batch` 把 `max_batch_count` 传 1、字节上限保留 1MiB。
- 用 `tokio::spawn` 跑提交，`tokio::time::sleep(10ms)` 后 `kill`；对
  spawned 结果做 `tokio::time::timeout(15s, ..)`，超时视为失败（propose
  timeout 是 10s，必须自行收敛）。
- `restart` 后按 `wait_until` 轮询 `decisions(id)` 与
  `raft.current_leader()`；重启节点作为 follower 收敛，不用它提案。

- [x] **Step 4: 运行确认通过**

Run: `cargo test -p coord-server --test kv_t_g3_sequencer_failover_test test_sequencer_failover_mid_batch test_sequencer_restart_rejoins`
Expected: PASS（2 passed）。

- [x] **Step 5: 提交**

```bash
git add coord-server/tests/kv_t_g3_sequencer_failover_test.rs
git commit -m "test(kv-t): cover mid-batch failover and node restart recovery (#152)"
```

---

### Task 4: 分区隔离、愈合与收敛

**Files:**
- Modify: `coord-server/tests/kv_t_g3_sequencer_failover_test.rs`
- Modify: `coord-server/src/raft/network.rs`（分区钩子：blocklist 不再在
  `new_client` 时固化成静态 `Blocked`，否则分区期间创建的 heartbeat worker /
  复制流在 unblock 后永远不可达）

**Interfaces:**
- Consumes: Task 2/3 夹具 + `RaftNetworkFactoryImpl::{block_node,unblock_node}`。
- Produces: `FailoverCluster::{partition,heal}` 与测试
  `test_sequencer_partitioned_leader_heals_without_duplicate_decisions`。

- [x] **Step 1: 写失败测试**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_sequencer_partitioned_leader_heals_without_duplicate_decisions() {
    // 1) leader 提交 A（ts_a）。
    // 2) partition(leader)：本节点屏蔽所有对端，所有对端屏蔽本节点。
    // 3) 其余两节点选出新 leader；提交 B -> ts_b > ts_a。
    // 4) 断言被隔离节点尚未看到 B（无部分可见）。
    // 5) heal；等待收敛到同一 leader 且被隔离节点应用 B。
    // 6) 在新 leader 上重试 B -> Duplicate(ts_b)；三节点决策集合一致。
}
```

- [x] **Step 2: 运行确认失败**

Run: `cargo test -p coord-server --test kv_t_g3_sequencer_failover_test test_sequencer_partitioned_leader`
Expected: FAIL — `partition`/`heal` 不存在。

- [x] **Step 3: 实现**

```rust
async fn partition(&self, id: u64) {
    for node in &self.nodes {
        if node.id == id {
            for peer in self.ids().filter(|p| *p != id) {
                node.factory.block_node(peer);
            }
        } else {
            node.factory.block_node(id);
        }
    }
}
async fn heal(&self, id: u64) { /* 对称 unblock */ }
```

- 等待「其余两节点」的 `current_leader` 一致且 != 被隔离节点；不依赖
  被隔离节点的自我视图（它可能仍认为自己是 leader）。
- heal 后等待三节点 `current_leader` 一致（允许再次选举）；重试走
  `failover_commit_until`，不假设还是同一个新 leader。

- [x] **Step 4: 运行确认通过**

Run: `cargo test -p coord-server --test kv_t_g3_sequencer_failover_test test_sequencer_partitioned_leader`
Expected: PASS（1 passed）。

- [x] **Step 5: 提交**

```bash
git add coord-server/tests/kv_t_g3_sequencer_failover_test.rs
git commit -m "test(kv-t): partition and heal a sequencer leader without duplicate commits (#152)"
```

---

### Task 5: 文档、门禁与 issue 收口

**Files:**
- Modify: `docs/production/consumer-contract.md`（事务 bullet）
- Modify: 本计划的验证记录/勾选

- [x] **Step 1: 更新消费方契约**

在事务 bullet 的幂等句后补一句（当前态，不写变更史）：

> 协调节点存活而 region 0 leader 切换时，同一 `txn_id` + `request_id`
> 重试返回首次 `commit_ts`；协调节点崩溃会丢会话（`TXN_NOT_FOUND`），
> 需重跑整个事务（D12.2）。

- [x] **Step 2: 本地门禁（AGENTS.md）**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
bash scripts/check-panics.sh
cargo audit
cargo deny check
cargo test -p coord-server --lib sequencer
cargo test -p coord-server --test kv_t_g3_sequencer_test
cargo test -p coord-server --test kv_t_g3_sequencer_failover_test
cargo test -p coord --test kv_t_g3_txn_commit_test
bash scripts/check-doc-refs.sh
```

Expected: 全绿；`check-doc-refs.sh` 悬空 = 0。失败按 `systematic-debugging`
定位，不跳过、不 `#[ignore]`。

- [x] **Step 3: 推送并开 PR**

```bash
git push -u origin feat/kv-t-g3-sequencer-failover
gh pr create --base main --fill
```

PR 描述写：测试名与断言、关闭 #152 的证据、未覆盖项（进程级 SIGSTOP/真实
网络分区归 #175；跨 Region 可见性 e2e 归 #168）。

- [x] **Step 4: CI 全绿后合并**

```bash
gh pr checks --watch
gh pr merge --merge --delete-branch
git switch main && git pull
```

- [x] **Step 5: 关闭 #152 的证据评论**

评论列出：新增测试名、`cargo test` 命令、三个不变量（无重复决策 /
无重复 commit_ts / 恢复后继续）、以及未覆盖项对应的 issue（#168/#175）。

---

## 验证记录

- `cargo test -p coord-server --lib test_sequencer_`：21 passed（含新增
  批内部分失败/同批重复两条锚点）。
- `cargo test -p coord-server --test kv_t_g3_sequencer_failover_test test_sequencer_`：
  4 passed（1.8–5.3s），连续 5 轮全绿。
- `cargo test -p coord-server --test kv_t_g3_sequencer_test`：7 passed。
- `cargo test -p coord-server --test replication_snapshot_f69_test`：1 passed
  （同一 partition 钩子的既有测试，验证修复未回归）。
- `cargo test -p coord-server --lib raft::network`：15 passed；
  `cargo test -p coord-server --test multi_raft_network_test`：2 passed。
- `cargo test -p coord --test kv_t_g3_txn_commit_test`：4 passed。
- `cargo fmt --all -- --check`、`cargo clippy --workspace --lib --bins -- -D warnings`、
  `bash scripts/check-panics.sh`、`cargo audit --no-fetch`、`cargo deny check` 全部通过。
- `bash scripts/check-doc-refs.sh`：本次改动无悬空引用；脚本仍报 3 条
  `docs/pki-assessment/` 的既有悬空引用——该目录被 `.gitignore:40 docs/*` 忽略，
  不在 git 树内（与 #146 记录一致）。

### 执行中的裁定

- **分区钩子修复**：`new_client` 在目标被 block 时不再返回静态
  `RaftNetwork::Blocked`，而是始终构造 `Real`，由每次 RPC 的动态 blocklist
  检查拒绝。理由：openraft 的 heartbeat worker / replication stream 会跨分区
  复用 network 对象，静态 `Blocked` 会把一次分区固化成永久不可达；`Blocked`
  只保留给「地址不可解析」的配置错误。生产路径不填 blocklist，行为不变。
- **重启测试的端口策略**：kill 只 `raft.shutdown()`、不 abort gRPC server；
  restart 用同一 listener/addr 重新注册 raft，避免 `coord-server/tests/common`
  禁止的「释放端口后重绑」竞态。

## 验收矩阵

| 用例 | 断言 |
|:--|:--|
| 批内冲突/乱序/非法 | 后续合法事务仍 Committed；被拒条目不写决策 |
| 同批重复 txn | `[Committed, Duplicate]`；决策 1 条；`last_commit_ts` 不回退 |
| kill leader 后重试 | 已决策事务 `Duplicate(首次 ts)`；新事务 ts 更大 |
| kill 打断多批提交 | 每个 txn 恰好 1 条决策；ts 唯一且递增 |
| 节点重启 | 追平故障期间决策；重试仍 `Duplicate` |
| 分区 leader | 其余节点继续提交；隔离节点愈合后收敛，无重复 ts |
| 关闭态 | 夹具不触碰 `[kv_t]` 装配路径；v1 测试全绿 |
