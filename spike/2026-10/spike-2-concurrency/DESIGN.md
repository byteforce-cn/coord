# Spike 2 · 并发控制方案（DESIGN）

> Last verified: 2026-10-13 ｜ 证据见 `README.md` 索引
> 现状锚点：`coord-core/src/workflow/ports.rs`（WorkflowStore）、
> `coord-core/src/workflow/runtime.rs`（drive）、
> `coord-agent/src/services/workflow_store.rs`（KvWorkflowStore）、
> `coord-agent/src/services/workflow_lease.rs`（KvInstanceLease）。

## 0. 目标与边界

- 目标：**一实例一驱动**（单写者）+ 状态写回防丢更新（CAS）+ 拒绝失去写权者的迟到写（fencing）。
- 边界：本方案为 **Spike 决策证据**，只落在 `spike/2026-10-spike2` 分支；
  不新增/修改对外 RPC 与 proto 字段。
- 前提事实（均已核实）：Lease 无 fencing token（`docs/production/consumer-contract.md` §3）；
  `save_instance_atomic`（ModRev CAS）此前**除 trait/实现外无调用点**；引擎关键字写回为普通 Put。

## 1. 状态写回的 CAS / Txn 形状

所有写回都归纳到一个入口：`WorkflowRuntime::persist_instance(store, inst, &mut rev, guard)`，
按「有租约 / 无租约但存储支持修订号 / 都不支持」三级降级：

1. **有租约（`guard = Some`）** → `LeaseGuard::fenced_save(inst, expected_mod_rev)`，
   即带 fencing 的 Txn（见 §3）。
2. **无租约但存储支持修订号**（`rev = Some`，来自 `load_instance_with_rev`）→
   `WorkflowStore::save_instance_atomic(inst, rev)`：
   `Txn(Compare(Target::ModRev, key=instance_key, target_value=ModRevision(rev)) → Put(instance_key))`。
3. **都不支持**（`rev = None` 且无租约，如 `MemoryWorkflowStore`）→ 普通 `save_instance`。

`KvWorkflowStore::save_instance_atomic` 的既有形状保持不变（`Target::ModRev` + `Put`）；
新增的只是**取期望修订号的线性一致读**：`WorkflowStore::load_instance_with_rev`。
默认实现返回 `Ok(None)`（= 该存储不提供修订号，调用方回退），因此
`MemoryWorkflowStore` 行为**逐字不变**。

**关于 `rev` 的刷新**：每次成功写回后，`persist_instance` 立即用
`load_instance_with_rev` 重新线性一致读回新 `mod_revision`；失败时不刷新，
drive 循环下一轮从头重读，避免用陈旧修订号反复 CAS。

**drive 循环的读取**：`drive` 每轮优先用 `load_instance_with_rev`（线性一致读）取
`(inst, rev)`；仅当存储不支持时才回退缓存优先的 `load_instance`（因此
`KvWorkflowStore` 的驱动读**绕过本地缓存**，见 §6 的缓存语义陷阱）。

**冲突处理（不静默吞掉）**：
- 首写（`Pending → Running`，实例「认领」）CAS/fencing 失败 ⇒ **退出 drive 循环**
  （判定失去写权）。
- 其余写回失败 ⇒ 计入 `WorkflowRuntime::cas_conflict_total()`（可观测信号）；drive
  下一轮重读重写。所有原 `let _ = store.save_instance(...)` 已替换为
  `let _ = self.persist_instance(...)`，冲突有计数出口。

## 2. 实例租约生命周期

端口（`coord-core`，对象安全，可注入）：

```rust
trait InstanceLease { async fn acquire(&self, id) -> Result<Option<Arc<dyn LeaseGuard>>, StoreError>; }
trait LeaseGuard {
    async fn renew(&self) -> Result<bool, StoreError>;   // false = 失去写权
    async fn release(&self);
    fn epoch(&self) -> i64;                               // 租约 authority 的单调 id
    fn renew_interval(&self) -> Duration;                 // 默认 1s
    async fn fenced_save(&self, inst, expected_mod_rev) -> Result<bool, StoreError>; // 默认 fail-closed
}
```

KV 实现（`coord-agent::services::workflow_lease::KvInstanceLease`）：

| 阶段 | 操作 |
|:--|:--|
| 获取 | 1) `LeaseGrant(ttl)`；2) `Txn(Compare(driver_key VERSION==0) → Put(driver_key=token, lease_id))`；失败方 `LeaseRevoke` 自己的空租约并返回 `None`；3) 线性一致读 driver_key 的 `mod_revision` 作为 `epoch` |
| 续租 | 后台任务每 `ttl/3`（下限 1s）`KeepAlive(lease_id)`；`ttl==0`/错误 ⇒ 停止续租 |
| 释放 | `LeaseRevoke(lease_id)` —— 服务端级联删除 driver_key（删除事件投递给 Watch） |
| 失去 | `KeepAlive` 返回 `ttl==0` 或错误；此时 `renew()==false`，drive 的 fencing 写会因 epoch 不匹配被拒 |

驱动接线（`WorkflowRuntime::spawn_drive`）：
1. 注入租约时先 `acquire`；
   - `Ok(None)`（他人持有）或 `Err`（后端不可用）⇒ **跳过驱动**、`lease_skipped_total += 1`、
     正常收尾（不计为 worker 故障）。**fail-closed**：绝不在无租约下驱动。
2. 取到 → 起后台续租任务 → `drive(..., Some(guard))`。
3. drive 结束 → abort 续租任务 → `release`。
4. 未注入租约 ⇒ 行为与现状一致（不去重）。

租约 key 布局：`/_workflow/v3/drivers/{instance_id}`（与实例 key 同 `/_workflow/v3/` 键空间，
但非 `instances/` 前缀，`KvWorkflowStore` 的 Watch 不会把它误当实例）。TTL 秒级，由实现方注入。

## 3. fencing 方案（Lease 无 fencing token 的处置）

**核心**：把**租约 key 的 `mod_revision` 当作 epoch**（不是把 token 存在自增计数里）。
fencing 写（`KvLeaseGuard::fenced_save`）在一个原子 Txn 内做**三重 compare**后 Put 实例状态：

1. `Compare(instance_key ModRev == expected_mod_rev)` —— 防丢更新；
2. `Compare(driver_key ModRev == epoch)` —— 租约未被撤销/重授（撤销→级联删→重建 ⇒ mod_revision 变化）；
3. `Compare(driver_key Value == token)` —— 防跨租约复用（lease id 可能被复用，随机 token 不会）。

任一不满足 ⇒ Txn `succeeded=false` ⇒ 旧驱动者的迟到写被拒（`fenced_write_rejects_stale_driver` 验证）。

**为什么不用 request_id 当 fencing**：`request_id` 去重是单节点/进程内/60s/不复制不持久
（kv.proto 明示），跨节点重试会重复生效，不具备单调 authority。

## 4. 失败模式与处理

| 失败模式 | 处理 |
|:--|:--|
| 两驱动者同时 `Pending → Running`（无租约） | CAS 恰一个成功；失败者退出 drive（`branch` 断点），不重复执行 |
| 两驱动者并发（有租约） | 租约互斥，失败者 acquire `None` → 跳过并计数 |
| 租约后端不可用（网络/错误） | **fail-closed 跳过驱动**（不计为 worker 故障），计 `lease_skipped_total` |
| 续租失败/租约过期 | 续租任务停止；旧驱动 fencing 写被拒；租约 key 过期级联删除，其他驱动者重新竞争 |
| 状态 CAS 冲突 | 计 `cas_conflict_total`，drive 下轮重读重写；首写冲突则退出 |
| 进程崩溃 | 租约 TTL 过期回收 key，其他实例可重新获取（恢复语义不在本 spike，见 Spike 3） |

## 5. 未接线项与残余风险（红线要求显式记录）

1. **引擎写路径未全覆盖**：只有 `drive` 循环的写回走 `persist_instance`（CAS/fencing）。
   `resume`、`resume_by_event`、`auto_resume`、`apply_timeout`、`fail_instance` 仍是普通 Put。
   其中 `auto_resume`/`resume_by_event` 已改为经 `spawn_drive`（受同一租约约束）；
   `resume`/`apply_timeout`/`fail_instance` 的前置写回**未**加 fencing
   —— 多 agent 并发 `resume` 时存在丢更新可能，属**已知未接线**。
2. **fencing 只保护状态写回，不保护外部副作用**：若驱动者在两次写之间已发出 `call`，
   撤销租约**无法撤回**该副作用。**唯一正确兜底是 StepToken**（见 `STEPTOKEN.md`），
   本 spike 只验证租约+fencing，未实现 StepToken 执行路径。
3. **`KvWorkflowStore` 缓存语义陷阱**：`save_instance`（普通 Put）成功后更新本地缓存；
   `save_instance_atomic` 仅在 CAS 成功时更新缓存（失败不污染缓存，符合要求）。
   但 fencing 写（`fenced_save`）**绕过缓存**，仅在 KV 生效 —— 若消费方未 `init()`
   （无 Watch 使缓存失效），其**缓存优先的 `load_instance` 会读到陈旧快照**。
   这是本 spike 实测发现（`await_terminal` 必须改用线性一致读）；驱动侧已用
   `load_instance_with_rev` 规避。**待办**：fenced 写后应同时失效/更新缓存，或统一
   让驱动读走线性一致读（当前已是后者）。
4. **`drive_instance` 为新公开入口**：supervisor 重驱动已存在实例时使用（受同一租约约束）。
   本 spike 用它触发两个并发驱动者；它不是对外 RPC。
5. **租约 TTL 秒级**：续租间隔 `ttl/3`；TTL 过小会导致频繁重新竞争，生产值待定。
6. **`epoch` 读取的窗口**：acquire 后单独一次线性读 driver_key 取 `mod_revision`。
   在「acquire 成功」到「读 epoch」之间若发生撤销（仅本进程持有，正常不发生），
   epoch 会读到新值；由于同时 compare `driver_key Value == token`，仍不会误放行。
