# KV-T G5 (#166) v2 对象存储数据面 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `coord.storage.v2` 从 UNIMPLEMENTED 壳变成 v2 集群可用的对象数据面：manifest 行以全局 `commit_ts` 写进固定 `/obj/` Region 的 MVCC（可用 `read_ts` 做快照读），chunk 文件仍随 raft 日志复制到各副本的 Region 目录；流式 Put/Get、Stat/Delete、配额/水位/GC 保持 v1 语义（ADR-0015 D7 只换 revision/Region 口径）。

**Architecture:** 末尾追加 `Command::ObjectStoreV2 { op, commit_ts }`（旧 `Command::ObjectStore` 不变，升级前的 v1 raft 日志继续可解码）。apply 用 `put_at_commit_ts` / `delete_at_commit_ts`；commit_ts 冲突作为响应返回（`Response::CommitRejected`），绝不让 raft apply 返回 `Err`。handler 在 Commit Sequencer 的「取 TSO → 提案」锁内分配 `commit_ts` 并提案，被 safe-ts fence 越过的罕见拒绝换新 ts 有界重试。GC 的 stale-Creating 回收在 v2 下走同一条提案路径。

**Tech Stack:** Rust / tonic / openraft / redb / tokio / postcard。

**Spec:** issue #166、ADR-0015 D6/D7/D12、`apis/contracts/proto/coord/storage/v2/storage.proto` 头注、`docs/production/volume-object-storage.md`（v1 语义单一归属）。

## Global Constraints

- `[kv_t].enabled=false`（默认）时不得改变 v1 行为（ADR-0001）：`Command::ObjectStore`、`object_gc_loop` 的 v1 路径与 `coord.storage`（v1）字节级不变。
- v2 下 manifest 行的 changelog 键必须是 `commit_ts`：不得把 raft log index 当 revision 写进 v2 Region（会与 commit_ts 撞键，`read_ts` 读到错历史）。
- `commit_ts` 全局唯一单调、绝不复用（D11）；允许空洞。
- 用户级冲突/拒绝不得让 Raft apply 返回 `Err`（D11.6）；任何 apply 拒绝都必须落 applied 水位，重启不得无限重放。
- 用户 key 不得占用 `/obj/`（既有 `guard_object_write_key`）；manifest key `[/obj/m/, /obj/n)` 只由对象数据面写。
- 注释只写当前「为什么」；不写日期/变更史；不新增 `TODO`/`FIXME`。
- 临时文件只放 `/tmp/coord-*`；提交前跑 AGENTS.md 门禁（fmt / clippy `--lib --bins -D warnings` / check-panics / audit / deny / check-doc-refs）。

## Rulings（本计划对 spec 的落地裁定）

1. **一个对象 op = 一条 raft 日志 + 显式 commit_ts**：apply 在同一步里写 chunk 文件并写 manifest 行，保持 v1「文件 + manifest 同一条目」的原子性；不拆成「文件条目 + manifest PutV2 条目」（拆开会把部分上传暴露成两个 revision，重放语义和配额记账都要重做）。
2. **commit_ts 在 Sequencer 的 `lock_fence_publication` 内分配并提案**：safe_ts 的 fence 只能越过已提案的条目（续用 #142 的既有不变量）。跨节点仍存在「已分配未提案」被 fence 越过的窗口 ⇒ apply 返回 `CommitRejected`，handler 换新 ts 重试（有界 8 次），客户端看不到这次内部重试。
3. **`read_ts` 语义**：`0` = ReadIndex 线性一致 + 当前 manifest；`>0` = 等 `safe_ts` 后用 `get_at_revision` 读 manifest 快照。chunk 字节不进 MVCC：快照 manifest 指向的 chunk 文件已被后续 Delete 删除时返回 `OUT_OF_RANGE` + `HISTORY_UNAVAILABLE`（绝不回退成当前值）。
4. **缺对象的三条路径各不相同**（proto 头注）：`Stat` 返回 `exists=false` 的零 stat（`exists` 字段就是为此存在）；`Get` 返回 `NOT_FOUND`（流无法表达缺席）；`Delete` 返回 `NOT_FOUND`（调用方可视为已删除）。
5. **对象 manifest 不产生 Watch 事件**（v1 同口径，`/obj/` 订阅本身被拒）；manifest 的 changelog 行仍写入，供 `read_ts` 快照读使用。
6. **v2 集群不注册 v1 `coord.storage`**（B-KVT-2）：否则 v1 Put 会以 log index 写 v2 Region 的 manifest 行，直接破坏 revision 语义。apply 层再加一道守卫：v2 模式下遇到 `Command::ObjectStore` 不产生副作用（返回 `ok=false`），不 wedge raft。
7. **GC**：stale-Creating 回收在 v2 下与数据面共用提案路径（TSO + fence 锁 + 重试）；孤儿 chunk 文件回收与 v1 相同（只读 live manifest 集）。
8. **不做**：`/obj/` 的 split/merge 交互（#165 已保证固定区间）、对象容量扩展、跨地域、v1→v2 迁移。

## File Structure

- Modify: `coord-server/src/storage/object_store.rs` — `ObjectStoreOpV2`（`op` + `commit_ts`）与 `apply_object_store_op_v2`；v1 函数保持签名；内部按 revision 策略分支，拒绝返回 `ObjectApply::Rejected`。
- Modify: `coord-server/src/raft/type_config.rs` — 末尾追加 `Command::ObjectStoreV2`（Display/断言配套）。
- Modify: `coord-server/src/raft/state_machine.rs` — apply 分支；`CommitRejected` → `Response::CommitRejected` + `set_last_applied`。
- Modify: `coord-server/src/server/mod.rs` — `propose_object_op_v2`（fence 锁内取 ts → 提案 → 拒绝重试）、`object_snapshot_manifest` 读助手。
- Modify: `coord-server/src/kvt/services.rs` — `storage.v2` 的 Put/Get/Stat/Delete 实现（门禁 + epoch 守卫 + 配额/水位 + 流式）。
- Modify: `coord-server/src/server/object_storage.rs` — `ObjectGcTarget` 结构 + v2 stale-Creating 删除路径。
- Modify: `coord/src/main.rs` — `Storage` 能力登记、v1 storage 服务只在 `!kv_t` 注册、GC 装配。
- Create: `coord/tests/support/kv_t_storage_node.rs` — 单节点 v2 集群（region 0 + 固定 `/obj/` Region + FakeTso），供 e2e 用例复用。
- Create: `coord/tests/kv_t_g5_storage_v2_test.rs` — Put/Get/Stat/Delete、read_ts 快照、重启重放、`/obj/` 路由、配额/上限、v1 不注册。
- Modify: `docs/production/ops/boundaries.md` — B-KVT-3/B-KVT-11 现状行（对象数据面已交付 + 快照读边界）。
- Modify: `docs/adr/0015-kv-t.md` — D7 落地现状行（只补现状，不改语义）。
- Modify: `apis/contracts/STATUS.md` — Storage v2 行的实现锚点。

## Review Focus

- v1 关闭态是否字节级不变（`Command::ObjectStore` 路径、v1 服务注册、GC 行为）？
- v2 apply 是否**不会**把 log index 写进 changelog；`read_ts` 快照是否真的读到历史 manifest 而不是当前值？
- `CommitRejected` 是否走响应（不 wedge raft、落 applied），handler 是否换新 ts 重试而不是把错误抛给客户端？
- 上传中途失败/leader 变更是否与 v1 同语义（整体失败、残留 Creating 由 GC 回收），重试不会复活已删除对象？
- `/obj/` 之外的 key 是否仍写不进对象 Region；`/obj/` 内的用户 key 是否仍被 `INVALID_ARGUMENT` 拒绝？

---

### Task 1: v2 对象 op 的 apply 语义

**Files:**
- Modify: `coord-server/src/storage/object_store.rs`
- Modify: `coord-server/src/raft/type_config.rs`
- Modify: `coord-server/src/raft/state_machine.rs`
- Test: `coord-server/src/storage/object_store.rs`（单元测试）
- Test: `coord-server/src/raft/state_machine.rs`（命令幂等/拒绝）

**Interfaces:**
- `ObjectStoreOpV2 { op: ObjectStoreOp, commit_ts: u64 }`
- `apply_object_store_op_v2(sm, op_v2, applied, chunks) -> Result<ObjectApply>`
- `enum ObjectApply { Applied(bool), Rejected { commit_ts, current_revision } }`
- `Command::ObjectStoreV2(ObjectStoreOpV2)`
- `Response::ObjectStore { revision, ok }`（成功/no-op）与 `Response::CommitRejected { .. }`（拒绝）

- [ ] **Step 1: 写失败测试（v2 apply 用 commit_ts）**

在 `object_store.rs` 的 tests 里新增（复用既有 `test_sm()` 风格的 CommitTs 存储构造）：

```rust
#[test]
fn test_apply_object_store_op_v2_uses_commit_ts_as_revision() { /* Begin@1000 → manifest create_revision/last_revision == 1000, current_revision == 1000 */ }

#[test]
fn test_apply_object_store_op_v2_rejects_out_of_order_commit_ts() { /* Begin@1000 后 Begin(other)@999 → Rejected{current_revision:1000}，无行写入 */ }

#[test]
fn test_apply_object_store_op_v2_replay_is_noop() { /* 同 commit_ts 重放 → Applied(false)，不重复写 chunk/manifest */ }
```

- [ ] **Step 2: 跑红**

Run: `cargo test -p coord-server --lib storage::object_store::tests::test_apply_object_store_op_v2`
Expected: 编译失败（`apply_object_store_op_v2` 不存在）。

- [ ] **Step 3: 实现**

- `ObjectStoreOp` 保持原样；新增 `ObjectStoreOpV2`（derive Clone/Debug/Serialize/Deserialize，`commit_ts` 放 `op` 之后）。
- `apply_object_store_op` 改为内部 `apply_object_store_op_with(sm, op, revision, policy, applied, chunks)`：`RevisionPolicy::LogIndex` 时 `put_at_revision`/`delete_at_revision`，`CommitTs` 时 `put_at_commit_ts`/`delete_at_commit_ts`；`CommitRejected` 统一转成 `ObjectApply::Rejected`（不返回 `Err`）。`CommitTs` 下遇到 v1 命令由 state machine 直接返回 `ok=false`（见 Task 1 Step 4）。
- `Command::ObjectStoreV2` 追加在 `Command` 末尾；`Display` 增加一行。
- `state_machine.rs`：`Command::ObjectStoreV2 { op }` → `apply_object_store_op_v2(...)`；`Applied(ok)` → `Response::ObjectStore { revision: commit_ts, ok }`；`Rejected{..}` → `set_last_applied(applied)` 后 `Response::CommitRejected`。`Command::ObjectStore` 在 `sm.revision_mode() == CommitTs` 时记 warn 并返回 `ok=false`（推进 applied）。

- [ ] **Step 4: 跑绿 + 回归**

Run: `cargo test -p coord-server --lib storage::object_store::tests`、`cargo test -p coord-server --lib raft::state_machine::tests`
Expected: 新增 3 例 + 既有对象/状态机用例全绿（v1 行为不变）。

---

### Task 2: handler 的 commit_ts 分配与重试

**Files:**
- Modify: `coord-server/src/server/mod.rs`
- Test: `coord-server/src/server/mod.rs`（`mod tests` 内单测：拒绝重试与锁内提案）

**Interfaces:**
- `async fn propose_object_op_v2(&self, target: &ObjectTarget, op: ObjectStoreOp) -> Result<(u64, bool), tonic::Status>`（返回 `(commit_ts, ok)`）
- `fn object_fence_source(&self) -> Result<u64, tonic::Status>`（读 region 0 `safe_ts_fence`）
- `fn sequencer_proposal_gate(&self) -> Result<tokio::sync::MutexGuard<'_, ()>, tonic::Status>`（未装配 sequencer → `FAILED_PRECONDITION`）

- [ ] **Step 1: 写失败测试**

用 `tokio::sync::Mutex` 模拟 propose 门 + 假 TSO：断言 ① 两次并发 `propose_object_op_v2` 的提案顺序与 commit_ts 顺序一致；② `Response::CommitRejected` 时用新 ts 重试并在第 2 次成功；③ 重试次数耗尽返回 `ABORTED` + `CommitRejected`。

- [ ] **Step 2: 实现**

```rust
const OBJECT_COMMIT_ATTEMPTS: usize = 8;

async fn propose_object_op_v2(&self, target: &ObjectTarget, op: ObjectStoreOp) -> Result<(u64, bool), Status> {
    let sequencer = self.commit_sequencer().ok_or_else(|| failed_precondition("..."))?;
    let tso = self.tso_client().ok_or_else(|| failed_precondition("..."))?;
    let raft = target.raft.as_ref().ok_or_else(|| unavailable("object storage requires raft mode"))?;
    let fence_source = Arc::clone(&self.storage);
    for _ in 0..OBJECT_COMMIT_ATTEMPTS {
        let (commit_ts, resp) = {
            // 锁覆盖「取 TSO → 提案」：fence 发布共用同锁，不会越过未提案的 ts。
            let _gate = sequencer.lock_fence_publication().await;
            let fence = fence_source.safe_ts_fence().map_err(map_err)?;
            let commit_ts = tso.get_ts_above(fence).await.map_err(tso_status)?;
            let cmd = Command::ObjectStoreV2(ObjectStoreOpV2 { op: op.clone(), commit_ts });
            (commit_ts, self.client_write_with_timeout(raft, cmd, target.region_id).await?)
        };
        match resp.response() {
            Response::ObjectStore { ok, .. } => return Ok((commit_ts, *ok)),
            Response::CommitRejected { .. } => continue, // 换新 ts 重试（不向客户端暴露）
            other => return Err(Status::internal(format!("unexpected raft response for ObjectStoreV2: {other:?}"))),
        }
    }
    Err(attach(Status::aborted("object commit_ts rejected by open raft fence; retry"), CoordErrorCode::CommitRejected))
}
```

`CommitRejected` 的映射与 kv.v2 Put 同一个 `v2_status`（`services.rs` 内已有）。

- [ ] **Step 3: 跑绿**

Run: `cargo test -p coord-server --lib server::tests::object_commit`
Expected: 3 例绿。

---

### Task 3: `coord.storage.v2` 四个 handler

**Files:**
- Modify: `coord-server/src/kvt/services.rs`
- Test: `coord-server/src/kvt/services.rs`（入参校验单测）

**Interfaces:**
- 复用 v1 `server/object_storage.rs` 的 `validate_ref`、`obj_stat`、`read_manifest`、`read_manifest_chunk`。
- `Put`：与 v1 同一流程，但每步 `propose_object_op_v2`；响应 `commit_ts` = Commit 那一步的 ts。
- `Get`/`Stat`：`read_ts == 0` → ReadIndex；`read_ts > 0` → `wait_for_safe_ts` 后 `mvcc.get_at_revision(manifest_key, read_ts)`。

- [ ] **Step 1: 写失败测试（入参/门禁）**

`put` 首帧非 meta、`total_size` 非法、`read_ts < 0`、`read_ts > safe_ts` 超时、`Stat` 缺对象、`Delete` 缺对象、`/obj/` 用户 key 守卫。

- [ ] **Step 2: 实现**

- 每个 handler：`self.kvt().gate(KvTCapability::Storage)?` → `guard_routing_epoch(self, request.metadata())?` → `validate_ref` → `object_target_for`。
- `Put`：`ensure_writable` + 配额 admission + `Begin`（`ok=false` → `ALREADY_EXISTS`）→ 逐 chunk（`ok=false` → `ABORTED`）→ `Commit`（`ok=false` → `INVALID_ARGUMENT`）；中途流错误 → `ABORTED`（残留 Creating 由 GC 回收），与 v1 消息逐字一致。
- `Get`：首帧 `ObjectStat`（含 `commit_ts = manifest.last_revision`、`create_ts = manifest.create_revision`），随后逐 chunk 读文件；chunk 文件缺失 → `OUT_OF_RANGE` + `HISTORY_UNAVAILABLE`。
- `Stat`：缺对象 → `exists=false` 零 stat。
- `Delete`：缺对象 → `NOT_FOUND`；存在 → 提案 Delete（`commit_ts` = tombstone ts），`deleted = ok`。
- 能力登记：`KvTCapability::Storage` 加入 `main.rs` 的 `implemented` 列表（Task 5）。

- [ ] **Step 3: 跑绿**

Run: `cargo test -p coord-server --lib kvt::services`
Expected: 既有 v2 用例 + 新增校验用例全绿。

---

### Task 4: GC 与 v1 服务注册

**Files:**
- Modify: `coord-server/src/server/object_storage.rs`
- Modify: `coord/src/main.rs`

**Interfaces:**
- `pub struct ObjectGcTarget { node_id, region_id, raft, mvcc, chunk_store, limits, interval_secs, v2: Option<Arc<CoordNode>> }`
- `pub async fn object_gc_loop(target: ObjectGcTarget)`

- [ ] **Step 1: 写失败测试**

`object_gc_loop` 的 v2 分支：stale Creating（`last_write_at_unix` 超时）→ 经 `ObjectStoreV2::Delete` 落 tombstone（`current_revision` 前进、manifest 行消失），且第二次 tick 不再重复删除。

- [ ] **Step 2: 实现**

- `ObjectGcTarget` 取代 7 个位置参数；`v2` 为 `Some` 时删除走 `node.propose_object_op_v2`（失败只 warn，不退出循环），`None` 时保持 v1 `Command::ObjectStore`。
- `main.rs`：v1 `storage_svc` 只在 `cfg.object_storage.enabled && !cfg.kv_t.enabled` 注册（B-KVT-2）；GC 装配传入 `Some(Arc::clone(&node))` 当 `cfg.kv_t.enabled`。
- `Storage` 能力加入 `impl .with_implemented([...])`。

- [ ] **Step 3: 跑绿**

Run: `cargo test -p coord-server --lib server::object_storage`、`cargo test -p coord --lib`
Expected: 全绿；v1 GC 用例行为不变。

---

### Task 5: e2e 与文档

**Files:**
- Create: `coord/tests/support/kv_t_storage_node.rs`
- Create: `coord/tests/kv_t_g5_storage_v2_test.rs`
- Modify: `docs/production/ops/boundaries.md`、`docs/adr/0015-kv-t.md`、`apis/contracts/STATUS.md`

- [ ] **Step 1: 写 harness**

单节点：region 0（system raft）+ 种子 `[/obj/, /obj0)` 切出的固定对象 Region（`object_store: Some(ObjectStoreCtx)`）+ `RegionLifecycle`；`FakeTso` + `CommitSequencer`；`KvTState` 打开 `Storage`；`chunk_size` 调小（如 1KiB）便于多 chunk。

- [ ] **Step 2: 写 e2e 用例**

- `test_storage_v2_put_get_stat_delete_roundtrip`：未知长度 Put（3 chunk）→ Get 逐 chunk 字节一致 → Stat `committed/commit_ts` → Delete → Stat `exists=false`、Get/Delete `NOT_FOUND`。
- `test_storage_v2_put_is_idempotent_until_delete`：重复 Put 同对象 → `ALREADY_EXISTS`；Delete 后重建成功且 `commit_ts` 前进。
- `test_storage_v2_read_ts_snapshot_and_history_unavailable`：Put v1（ts=T1）→ Delete（T2）→ Put 新值（T3）；`Stat@T1` 仍见旧 commit_ts；`Get@T1`（chunk 已删）→ `OUT_OF_RANGE` + `HISTORY_UNAVAILABLE`。
- `test_storage_v2_manifest_stays_in_object_region`：manifest 行落在 `/obj/` Region，普通 KV Region 无该行；`/obj/` 用户 key Put 被拒。
- `test_storage_v2_disabled_and_capability_gate`：`[kv_t]` 关闭 / 未登记 Storage → `FAILED_PRECONDITION`。
- `test_storage_v2_quota_and_limits`：声明超 `max_object_size` → `INVALID_ARGUMENT`；超配额 → `RESOURCE_EXHAUSTED`。

- [ ] **Step 3: 跑绿 + 文档**

Run: `cargo test -p coord --test kv_t_g5_storage_v2_test`
Run: `bash scripts/check-doc-refs.sh`
Expected: 全绿、悬空引用 0。

---

## 验收（与 issue #166 对齐）

- [ ] v2 storage e2e（CRUD/重启/配额/边界）绿；与 v1 行为对照（除 revision/Region）。
- [ ] `/obj/` 固定区间一致：manifest 只在对象 Region，用户 key 不可占用。
- [ ] `[kv_t].enabled=false` 时 v1 数据面与 GC 行为不变。
- [ ] 门禁：fmt / clippy / check-panics / audit / deny / check-doc-refs 全绿。
