# KV-T G5 动态 Region 表 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 Region 元数据从静态配置升级为 region 0 raft 中的权威表：支持原子 add/update/remove、区间平铺与 epoch 单调校验、全节点收敛到 `RegionManager` 路由表，作为 split/merge（#162–#164）的元数据基座。

**Architecture:** `RegionTable`（`version` + 按 `start_key` 有序、平铺的 `Vec<RegionMeta>`）以单键 `/_sys/pd/regions` 存在 region 0 的 `MvccStorage`，与 Commit Log shard 表同构（region 0 apply、无 Watch 事件、replay 幂等）。变更经 `Command::RegionMeta(RegionMetaOp)` 入 region 0 raft：`Init` 只在空表时种入静态 `initial_regions`，`Apply` 在一个 raft 条目里原子应用 `Add/Update/Remove`（split = Update+Add，merge = Update+Remove）。每个节点本地 apply 后表即收敛；路由入口按表版本刷新 `RegionManager`（撤销旧 handle、注册新 handle、原地更新区间/epoch/peers）。运行时 spawn 与 split 执行仍归 #160/#162。

**Tech Stack:** Rust、openraft、redb（`MvccStorage`）、postcard、tokio。

**Spec:** `docs/adr/0015-kv-t.md` D6；issue #159（前置 #138 已合入）。

## Global Constraints

- `[kv_t].enabled=false`（默认）时 v1 行为不变（ADR-0001）：不种表、不刷新路由、region 0 不新增任何记录；`RegionManager` 的静态装配语义不变。
- 表不变量：按 `start_key` 升序、首区 `start_key` 为空、相邻 `end_key == 下一区 start_key`、末区 `end_key` 为空、`region_id` 唯一且 > 0、每区 `start < end`。任一变更违反 → **整批拒绝**（不部分应用）。
- epoch 单调：`Update` 的 `conf_ver`/`version` 分量不减且至少一项严格增大；members 变化必须增大 `conf_ver`，key range 变化必须增大 `version`。`Remove` 携带的 `expected_epoch` 必须等于当前 epoch（CAS）。
- 用户级/元数据级拒绝不得让 raft apply 返回 `Err`：`RegionMetaOutcome::{Applied, Rejected}` 随命令返回。
- 表版本（`RegionTable.version`）只在真正改变表时 +1；`Init` 在 `version > 0` 时是 no-op，便于重启/多节点重复种子。
- 临时文件只放 `/tmp`。注释只写当前“为什么”（不变量/陷阱/契约），不写日期、变更史或一次性任务号；不新增 TODO/FIXME。
- 提交前本机跑通：`cargo fmt --all -- --check`、`cargo clippy --workspace --lib --bins -- -D warnings`、`bash scripts/check-panics.sh`、`cargo audit`、`cargo deny check`；文档变更跑 `bash scripts/check-doc-refs.sh`。

## Ledger of decisions

- **Ruling:** 一次 raft 条目承载一批 `RegionChange`，而不是 add/update/remove 三个独立命令。split/merge 需要“旧区缩界 + 新区插入”原子生效；拆开会在 apply 之间制造合法窗口外的空洞，与“区间平铺”不变量冲突。代价：PD 执行器必须构造整批（#162 实现时按此口径）。
- **Ruling:** `Update` 只要求 epoch 分量不减 + 对应维度严格增大，不做 `expected_epoch` CAS。写入方（PD executor）在 #162 负责读-改-写；本层用单调性挡住回退/重放旧 epoch。代价：两个并发 stale update 都带更大 epoch 时后者可能覆盖前者——#162 的 cutover 还需 region 0 串行化，属于执行器职责。
- **Ruling:** 表放在 `coord-server/src/storage/region_table.rs`，与 `commit_log_shard.rs` 并列；`RegionMetaOp`/`RegionMetaOutcome` 放在 `raft/type_config.rs`（与 `CommitLogShardOp` 同惯例）。`RegionManager` 只依赖表值类型，不依赖 kvt 模块。
- **Ruling:** 路由刷新在 `kv_target_for_key` / `kv_range_targets` 入口按需执行（一次 redb 单键读），不做后台任务/额外 watch 通道。节点已通过 region 0 raft apply 持有同一张表；单键读的代价远小于请求本身的 read-index 屏障。读失败只 warn 并沿用旧表（不把健康节点的读路径变成故障点），旧表在 split 数据迁移（#160+）落地前仍是正确路由。
- **Ruling:** `RegionManager::apply_region_table` 保留本地近似统计（heartbeat 瞬态），只替换 range/epoch/peers；删除表中已不存在的 region 时同时丢弃其 runtime（避免“无 handle 有 runtime”的半状态；运行时 spawn 归 #160）。
- **Ruling:** 与 PD `PdMetaStore` 的对账不在本计划内：当前没有生产写入方（split/merge executor 是 #162），对账在 #162 接线时以本表为权威单向同步。issue #159 在本 PR 后仍保持 open，剩余项写进 issue 评论。

## Review Focus

- **多节点收敛**：3 节点 region 0 raft，leader 提交 `Init` + split 批之后，每个节点的 `region_table()` 与 `RegionManager` 路由必须一致；follower 不得自行提议/写入。
- **拒绝不卡 raft**：非法区间/epoch 回退/CAS 不符 → apply 返回 `Rejected` 且表版本不变；同一日志在所有节点得到同一结果（确定性）。
- **重放幂等**：同一批命令重放（快照/重启/重复 `Init`）不改变表内容、不重复 bump version。
- **默认关闭不变**：`[kv_t].enabled=false` 时 `kv_target_for_key` 不读表、region 0 无 `/_sys/pd/regions`、v1 静态 `initial_regions` 行为逐字不变。

---

### Task 1: `RegionTable` 类型、区间/epoch 校验与原子批应用

**Files:**
- Create: `coord-server/src/storage/region_table.rs`
- Modify: `coord-server/src/storage/mod.rs`（`pub mod region_table;`）
- Test: `coord-server/src/storage/region_table.rs`（`#[cfg(test)] mod tests`）

**Interfaces:**
- Produces:
  - `pub const REGION_TABLE_KEY: &[u8] = b"/_sys/pd/regions";`
  - `pub struct RegionTable { pub version: u64, pub regions: Vec<RegionMeta> }`（`Default`、`PartialEq`、`Serialize/Deserialize`）
  - `RegionTable::encode(&self) -> Result<Vec<u8>>` / `decode(&[u8]) -> Result<Self>`
  - `RegionTable::get(&self, region_id: RegionId) -> Option<&RegionMeta>`
  - `RegionTable::route(&self, key: &[u8]) -> Option<&RegionMeta>`
  - `RegionTable::validate(&self) -> Result<()>`
  - `RegionTable::init(&mut self, regions: Vec<RegionMeta>) -> Result<bool>`（`version > 0` → `Ok(false)`）
  - `RegionTable::apply(&mut self, changes: &[RegionChange]) -> Result<bool>`（整批校验；失败时 `self` 不变）
  - `pub enum RegionChange { Add { meta }, Update { meta }, Remove { region_id, expected_epoch } }`（serde，`PartialEq`）
- 语义：
  - `validate`：`version == 0` 时表必须为空；`version > 0` 时至少一区并满足 Global Constraints 的平铺规则；epoch 两个分量都 `>= 1`。
  - `init`：校验后用给定区间建表并把 `version` 置 1；已有表 → `false`。
  - `apply`：逐条应用后按 `start_key` 排序并 `validate`；`Add` 已存在且完全相同 → no-op，不同 → `InvalidArgument`；`Update` 不存在 → `InvalidArgument`；`Remove` 的 epoch 不匹配 → `InvalidArgument`；成功后 `version += 1`。

- [ ] **Step 1: 写失败测试**

在 `region_table.rs` 的 tests 写（`meta(id, start, end, conf_ver, version)` / `meta_v(id, start, end)` 辅助函数）：

```rust
#[test]
fn init_tiles_and_is_idempotent() {
    let mut table = RegionTable::default();
    assert!(table.init(vec![meta(1, b"", b"m"), meta(2, b"m", b"")]).unwrap());
    assert_eq!(table.version, 1);
    assert!(!table.init(vec![meta(9, b"", b"")]).unwrap());
    assert_eq!(table.regions.len(), 2);
}

#[test]
fn invalid_tiling_is_rejected_atomically() {
    let mut table = RegionTable::default();
    table.init(vec![meta(1, b"", b"")]).unwrap();
    let before = table.clone();
    assert!(table
        .apply(&[RegionChange::Add { meta: meta(2, b"zz", b"") }])
        .is_err());
    assert_eq!(table, before);
    assert!(table
        .apply(&[RegionChange::Add { meta: meta(3, b"", b"") }])
        .is_err());
}

#[test]
fn epoch_must_be_monotonic_per_dimension() {
    let mut table = RegionTable::default();
    table.init(vec![meta(1, b"", b"")]).unwrap();
    let mut same = meta(1, b"", b"");
    assert!(table.apply(&[RegionChange::Update { meta: same.clone() }]).is_err());
    same.epoch = RegionEpoch { conf_ver: 2, version: 2 };
    assert!(table.apply(&[RegionChange::Update { meta: same }]).unwrap());
}

#[test]
fn range_change_requires_version_bump_and_member_change_requires_conf_ver() {
    let mut table = RegionTable::default();
    table.init(vec![meta(1, b"", b"")]).unwrap();
    let mut range_only = meta(1, b"", b"k");
    range_only.epoch = RegionEpoch { conf_ver: 1, version: 2 };
    assert!(table.apply(&[RegionChange::Update { meta: range_only }]).unwrap());

    let mut peers_only = meta(1, b"", b"k");
    peers_only.peers.push(Peer { node_id: 2, raft_addr: "127.0.0.1:1".into(), role: PeerRole::Voter });
    peers_only.epoch = RegionEpoch { conf_ver: 2, version: 2 };
    assert!(table.apply(&[RegionChange::Update { meta: peers_only }]).is_err());
    let mut peers_ok = meta(1, b"", b"k");
    peers_ok.peers.push(Peer { node_id: 2, raft_addr: "127.0.0.1:1".into(), role: PeerRole::Voter });
    peers_ok.epoch = RegionEpoch { conf_ver: 2, version: 3 };
    assert!(table.apply(&[RegionChange::Update { meta: peers_ok }]).unwrap());
}

#[test]
fn split_batch_and_merge_batch_are_atomic() {
    let mut table = RegionTable::default();
    table.init(vec![meta(1, b"", b"")]).unwrap();
    let split = vec![
        RegionChange::Update { meta: meta_v(1, b"", b"m") },
        RegionChange::Add { meta: meta(2, b"m", b"") },
    ];
    assert!(table.apply(&split).unwrap());
    assert_eq!(table.route(b"a").unwrap().region_id, 1);
    assert_eq!(table.route(b"z").unwrap().region_id, 2);

    let merge = vec![
        RegionChange::Update { meta: meta_v(1, b"", b"") },
        RegionChange::Remove { region_id: 2, expected_epoch: RegionEpoch::initial() },
    ];
    assert!(table.apply(&merge).unwrap());
    assert_eq!(table.route(b"z").unwrap().region_id, 1);
}

#[test]
fn remove_requires_matching_epoch() {
    let mut table = RegionTable::default();
    table.init(vec![meta(1, b"", b"m"), meta(2, b"m", b"")]).unwrap();
    let stale = RegionEpoch { conf_ver: 2, version: 1 };
    assert!(table
        .apply(&[RegionChange::Remove { region_id: 2, expected_epoch: stale }])
        .is_err());
}

#[test]
fn decode_rejects_trailing_bytes_and_invalid_tables() {
    let mut table = RegionTable::default();
    table.init(vec![meta(1, b"", b"")]).unwrap();
    let mut bytes = table.encode().unwrap();
    bytes.push(0);
    assert!(RegionTable::decode(&bytes).is_err());
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server --lib storage::region_table`
Expected: 编译失败（模块/类型不存在）。

- [ ] **Step 3: 实现表与校验**

- `RegionTable { version: u64, regions: Vec<RegionMeta> }`；`encode` 用 `postcard::to_allocvec`，`decode` 用 `take_from_bytes` + 尾字节拒绝 + `validate`（照 `commit_log_shard.rs` 的写法）。
- 区间比较用 `Vec<u8>` 字典序；末区 `end_key.is_empty()` 视为无上界。
- `apply` 在克隆上修改，全部成功后 `version += 1`，失败返回 `Error::InvalidArgument` 且不动原表。
- 模块头注释写清“region 0 权威元数据/单键整表覆盖写/区间平铺由构造性校验保证”。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server --lib storage::region_table`
Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add coord-server/src/storage/region_table.rs coord-server/src/storage/mod.rs
git commit -m "feat(kv-t): add validated dynamic region table type"
```

---

### Task 2: region 0 raft 命令与状态机 apply

**Files:**
- Modify: `coord-server/src/raft/type_config.rs`（`Command::RegionMeta`、`Response::RegionMeta`、Display）
- Modify: `coord-server/src/raft/state_machine.rs`（apply 分支）
- Modify: `coord-server/src/storage/mvcc.rs`（`apply_region_meta_op` / `region_table`）
- Test: `coord-server/src/storage/mvcc.rs`（`#[cfg(test)]`，若既有测试模块；否则新建 `coord-server/tests/region_table_apply_test.rs`）

**Interfaces:**
- Produces:
  - `pub enum RegionMetaOp { Init { regions: Vec<RegionMeta> }, Apply { changes: Vec<RegionChange> } }`
  - `pub enum RegionMetaOutcome { Applied { version: u64 }, Rejected { reason: String } }`
  - `Command::RegionMeta(RegionMetaOp)`（枚举末尾追加）、`Response::RegionMeta { outcome: RegionMetaOutcome }`
  - `MvccStorage::apply_region_meta_op(&self, op: &RegionMetaOp, revision: Revision, applied: AppliedLogId) -> Result<RegionMetaOutcome>`
  - `MvccStorage::region_table(&self) -> Result<RegionTable>`
- 语义：`Init`/`Apply` 的内部错误（校验失败）降级为 `Rejected`（warn 日志，表不变）；存储层错误（redb/编码）仍 `Err`。无论 applied/rejected 都写 `META_LAST_APPLIED`；不写 changelog、不上 Watch。

- [ ] **Step 1: 写失败测试**

`MvccStorage` 测试模块（参照 `test_commit_log_shard_ops_persist_and_replay_idempotently`）：
```rust
#[test]
fn test_region_meta_ops_persist_and_reject_without_failing_apply() {
    let storage = /* temp redb MvccStorage，CommitTs 或 LogIndex 均可 */;
    let mut applied = applied_for(1);
    let init = RegionMetaOp::Init { regions: vec![meta(1, b"", b"")] };
    let out = storage.apply_region_meta_op(&init, 1, applied).unwrap();
    assert!(matches!(out, RegionMetaOutcome::Applied { version: 1 }));
    assert_eq!(storage.region_table().unwrap().regions.len(), 1);

    let bad = RegionMetaOp::Apply { changes: vec![RegionChange::Add { meta: meta(2, b"q", b"") }] };
    applied = applied_for(2);
    let out = storage.apply_region_meta_op(&bad, 2, applied).unwrap();
    assert!(matches!(out, RegionMetaOutcome::Rejected { .. }));
    assert_eq!(storage.region_table().unwrap().version, 1);

    // 重放同一条 Init：no-op，不 bump version
    let out = storage.apply_region_meta_op(&init, 1, applied_for(1)).unwrap();
    assert!(matches!(out, RegionMetaOutcome::Applied { version: 1 }));
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server --lib storage::region_meta`（或新测试目标）
Expected: 编译失败（`RegionMetaOp` 等不存在）。

- [ ] **Step 3: 实现类型、apply 与响应**

- `type_config.rs` 末尾追加 `Command::RegionMeta(RegionMetaOp)`；`RegionMetaOp`/`RegionChange` 用 serde；`Response::RegionMeta` + Display 一行。
- `mvcc.rs`：读 `TABLE_KV/REGION_TABLE_KEY` → `RegionTable::decode`/`default`；执行 `init`/`apply`；`Ok(true)` 时覆盖写；写 `META_LAST_APPLIED`；返回 outcome。`region_table()` 读缺失返回 `default`。
- `state_machine.rs` 分支：`Command::RegionMeta(op) => sm.apply_region_meta_op(op, revision, applied)` → `Ok((Response::RegionMeta { outcome }, None))`。
- 不要调用 `note_log_index_revision`（元数据表不推进业务 revision）。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server --lib storage::region_meta && cargo test -p coord-server --lib raft::type_config`
Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add coord-server/src/raft/type_config.rs coord-server/src/raft/state_machine.rs \
        coord-server/src/storage/mvcc.rs
git commit -m "feat(kv-t): apply region metadata ops through region 0 raft"
```

---

### Task 3: `RegionManager` 收敛与路由缓存失效

**Files:**
- Modify: `coord-server/src/raft/region.rs`（`RegionHandle::replace_meta`、`RegionManager::apply_region_table`/`table_version`）
- Test: `coord-server/tests/region_manager_test.rs`

**Interfaces:**
- Produces:
  - `RegionHandle::replace_meta(&self, meta: RegionMeta)`（原地更新 range/epoch/peers，保留本地 `approximate_*` 统计）
  - `RegionManager::apply_region_table(&self, table: &RegionTable) -> Result<bool>`（表版本 <= 当前 → `Ok(false)`；否则原子替换 `regions`/`key_index`，删除表中不存在的 handle/runtime，设置版本）
  - `RegionManager::table_version(&self) -> u64`
- 语义：已是存在的 handle 走 `replace_meta`（与 runtime 共享同一 `Arc<RegionHandle>`，路由读到的 meta 立即生效）；新 id 建 handle；不 spawn runtime。

- [ ] **Step 1: 写失败测试**

在 `region_manager_test.rs`：
```rust
#[test]
fn test_apply_region_table_adds_updates_and_removes() {
    let rm = RegionManager::new(1);
    rm.register_region(make_test_meta(1, b"".to_vec(), b"".to_vec())).unwrap();
    let mut table = RegionTable::default();
    table.init(vec![make_test_meta(1, b"".to_vec(), b"".to_vec())]).unwrap();
    assert!(rm.apply_region_table(&table).unwrap());
    assert_eq!(rm.table_version(), 1);

    let split = RegionTable {
        version: 2,
        regions: vec![
            make_test_meta(1, b"".to_vec(), b"m".to_vec()),
            make_test_meta(2, b"m".to_vec(), b"".to_vec()),
        ],
    };
    assert!(rm.apply_region_table(&split).unwrap());
    assert_eq!(rm.route(b"a").unwrap().region_id(), 1);
    assert_eq!(rm.route(b"z").unwrap().region_id(), 2);
    assert_eq!(rm.get_region(1).unwrap().meta.read().end_key, b"m".to_vec());

    let merged = RegionTable { version: 3, regions: vec![make_test_meta(1, b"".to_vec(), b"".to_vec())] };
    assert!(rm.apply_region_table(&merged).unwrap());
    assert!(rm.get_region(2).is_none());
    assert_eq!(rm.route(b"z").unwrap().region_id(), 1);
    assert!(!rm.apply_region_table(&merged).unwrap()); // 版本不前进 = no-op
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server --test region_manager_test`
Expected: 编译失败（`apply_region_table` 不存在）。

- [ ] **Step 3: 实现收敛**

注意锁顺序：先 `regions.write()` 再 `key_index.write()`（与既有路径一致），版本最后写 `AtomicU64`。`RegionHandle::replace_meta` 先把 `approximate_size/keys` 从旧 meta 带过来，再覆盖。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server --test region_manager_test`
Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add coord-server/src/raft/region.rs coord-server/tests/region_manager_test.rs
git commit -m "feat(kv-t): reconcile region manager with the authoritative table"
```

---

### Task 4: 启动种子、路由入口刷新与多节点收敛验收

**Files:**
- Modify: `coord-server/src/server/mod.rs`（`CoordNode::refresh_region_table`；`kv_target_for_key`/`kv_range_targets` 入口按 `kvt.is_enabled()` 刷新）
- Modify: `coord/src/main.rs`（`[kv_t].enabled` 时经 region 0 raft `Init` 种子，限时重试）
- Create: `coord-server/tests/kv_t_g5_region_table_test.rs`（3 节点 region 0 raft + RegionManager）

**Interfaces:**
- Produces:
  - `CoordNode::refresh_region_table(&self) -> Result<bool>`（无 manager / 表版本 0 / 读失败 → `Ok(false)`；读失败只 warn）
  - `main.rs` 内部 `seed_region_table(raft: &CoordRaft, regions: Vec<RegionMeta>)`（限时重试，`Init` 幂等）
- 语义：
  - `Init` 的 `RegionMeta` 由 `region_seeds` + `region_peers` 构造（全成员 voter peers，与静态装配一致）。
  - 路由刷新失败（redb 读错误）不阻断请求，沿用旧表。

- [ ] **Step 1: 写失败的多节点测试**

`coord-server/tests/kv_t_g5_region_table_test.rs`（照 `kv_t_g4_commit_log_shards_multinode_test.rs` 的 `boot` 简化：只建 region 0 raft + `RegionManager`，不建数据 Region）：
- 3 节点、各自 `RegionManager` 先注册 region 1（`""..""`）。
- 断言 region 0 leader 提交：
  1. `Init { regions: [region 1] }` → 三节点 `mvcc.region_table().version == 1`；
  2. split 批 `Update(region 1 -> "".."m", version=2) + Add(region 2 "m".."", initial)` → 三节点表一致、版本 2；
  3. 非法批 `Add(region 3 "zz".."")` → 三节点都得到 `Rejected`，版本仍 2。
- 每节点 `manager.apply_region_table(&node.mvcc.region_table().unwrap())`，断言 `route(b"a")==1`、`route(b"z")==2`。
- 用 `tokio::time::timeout(30s)` 轮询 follower 收敛（表版本）。

Run: `cargo test -p coord-server --test kv_t_g5_region_table_test`
Expected: FAIL（编译失败/命令未实现）。

- [ ] **Step 2: 实现刷新与种子**

- `CoordNode::refresh_region_table`：`self.region_manager.is_some()` 且 `self.kvt.is_enabled()` 才读 `self.storage.region_table()`；`manager.apply_region_table`。
- `kv_target_for_key` / `kv_range_targets` 开头调用 `self.refresh_region_table()`；错误 `tracing::warn!` 后继续。
- `main.rs`：region 0 raft 就绪之后、`CoordNode` 构建前后均可，gated `cfg.kv_t.enabled`；用与 root 用户持久化相同的“限时重试直到 leader”模式提交 `Init`，失败使启动失败（fail-closed：v2 集群没有 region 表无法 split）。
- `Init` 条目在非 bootstrap 节点也提交（no-op 幂等），避免“bootstrap 节点重启丢失种子”的特例。

Run: `cargo test -p coord-server --test kv_t_g5_region_table_test`
Expected: PASS。

- [ ] **Step 3: 跑相关回归**

Run: `cargo test -p coord-server --lib storage::region_table && cargo test -p coord-server --test region_manager_test && cargo test -p coord-server --lib raft::type_config`
Expected: PASS。

- [ ] **Step 4: 提交**

```bash
git add coord-server/src/server/mod.rs coord/src/main.rs \
        coord-server/tests/kv_t_g5_region_table_test.rs
git commit -m "feat(kv-t): seed and refresh the dynamic region table"
```

---

### Task 5: 文档、门禁与 PR

**Files:**
- Modify: `docs/adr/0015-kv-t.md`（D6 落地注记：表位置/命令/原子批/epoch 规则；不改语义）
- Modify: `docs/production/ops/boundaries.md`（新增 B-KVT-8：region 表权威、拒绝语义、默认关闭不变）
- Modify: `apis/contracts/STATUS.md`（仅当有对外可见状态行需要更新）

- [ ] **Step 1: 更新文档**

`boundaries.md` 写清：表是 region 0 权威元数据；非法变更整批拒绝且不失败 apply；路由刷新按表版本；`[kv_t].enabled=false` 时无表、无刷新。`0015-kv-t.md` D6 下补 3–5 行“当前实现”注记（不复制 ADR 语义）。

- [ ] **Step 2: 文档引用检查**

Run: `bash scripts/check-doc-refs.sh`
Expected: 悬空引用 0。

- [ ] **Step 3: 全量门禁**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
bash scripts/check-panics.sh
cargo audit
cargo deny check
cargo test -p coord-server --lib storage::region_table
cargo test -p coord-server --lib storage::region_meta
cargo test -p coord-server --test region_manager_test
cargo test -p coord-server --test kv_t_g5_region_table_test
```
Expected: 全绿。

- [ ] **Step 4: 提交文档**

```bash
git add docs/ apis/contracts/STATUS.md
git commit -m "docs(kv-t): document the authoritative region table"
```

- [ ] **Step 5: 开 PR（分支 → PR → CI 全绿 → merge → 删分支）**

```bash
git push -u origin feat/kv-t-g5-region-table
gh pr create --base main --fill
gh pr checks --watch
gh pr merge --merge --delete-branch
```

PR 描述逐条勾选 #159 中本 PR 覆盖的验收项（多节点一致、非法变更 fail-closed、epoch 单调、路由缓存失效），把未覆盖项（PD 对账、`/obj/` 固定区间、运行时 spawn）显式列为后续 issue 的工作；#159 保持 open 并在评论里记录剩余范围。
