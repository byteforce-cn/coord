# ADR-0004: Raft 日志回收——快照锚定的保留窗口

- 状态：proposed
- 日期：2026-10-01
- 决策者：维护团队（设计评审记录见本 ADR 对应 PR）

## 背景

需要为 raft 日志钉住三个问题：**何时回收、回收到哪里、崩溃恢复与落后 follower 追赶靠什么保证**。当前实现没有把这些语义写进单一决策，文档与实现之间也存在失真，因此先以本 ADR 固化语义，再落实现（B-ST-1 / B-ST-2）。

### 上游契约（openraft =0.10.0-alpha.34，registry 源码实读）

- 不变量 `purged ≤ snapshot ≤ applied ≤ committed`：**已回收的日志必然被快照覆盖**（`src/docs/data/log_pointers.md`）。
- 自动回收只发生在新快照构建成功之后；回收点 = `snapshot_last_log_id.next_index() − max_in_snapshot_log_to_keep`（`src/engine/handler/log_handler/mod.rs`，`calc_purge_upto`）。
- 手动 `Raft::trigger().purge_log(upto)`：无快照直接拒绝（"no snapshot, cannot purge"）；有快照则把 `upto` 钳制到 `snapshot_last_log_id`；leader 上若有在途复制任务正在使用待删日志，会延迟到复制推进后再删（`src/engine/engine_impl.rs`，`trigger_purge_log`）。
- `SnapshotPolicy::Never` 下仍可手动 `trigger().snapshot()`（`src/config/config.rs`，`SnapshotPolicy` 字段文档）。
- 字段文档原文："Logs that are not in a snapshot will never be purged."；"The maximum number of logs to keep that are already included in **snapshot**."（`max_in_snapshot_log_to_keep`，默认 1000）。

**推论：「不依赖快照的回收」（按保留窗口直接截断日志、「只 compact 不 snapshot」）在本上游版本上不可实现**——它违反 `purged ≤ snapshot` 不变量：落后于回收点的 follower 将既无日志可追、又无快照可装，无法收敛。coord 侧两处机制都建立在该不变量上：`LogStore::purge` 的持久快照前置守卫（`durable_covers`），以及启动恢复检查（无快照覆盖已回收点时，从 MVCC 重建或等待 leader 下发 install-snapshot）。

### coord 现状与失真

- 缺省（`[raft].snapshot_logs_since_last` 未配置）：`None` → 保持 openraft 默认 `LogsSinceLast(5000)`，**自动快照开启、日志有界**（实验佐证：`RaftConfig::default()` + 空 tuning ⇒ `LogsSinceLast(5000)`）。
- `= 0`：`SnapshotPolicy::Never`，自动快照关闭 ⇒ 无新快照 ⇒ 回收条件恒不成立（B-ST-1）。启动 WARN、`config.example.toml`、`boundaries.md`、`runbook.md` 已按条件语义写明；README（英文 / 中文）将「禁用」写成默认状态，与实现不符，随实现 PR 更正。
- 「日志保留上界」目前**不可配置**：`max_in_snapshot_log_to_keep` 固定为上游默认 1000。
- `CompactionConfig::{raft_log_retention_entries, tombstone_retention_revisions}` 无读取点（B-ST-2）；tombstone 物理清理由 `apply_compact` 与 changelog 共用同一 compact 水位完成，无独立窗口。

## 决定

1. **回收语义一律快照锚定**：不实现、也不承诺「无快照回收」或「只 compact 不 snapshot」。任何日志删除都必须发生在覆盖该位置的持久快照落盘之后（上游不变量 + coord `LogStore::purge` 守卫双重成立）。
2. **两个正交开关，均为 `[raft]` 配置**：

   | 开关 | 语义 | 缺省 |
   |:--|:--|:--|
   | `snapshot_logs_since_last`（既有） | 快照节奏；`0` = 手动快照模式（`Never`）——自动快照与自动回收同时关闭，保留启动 WARN，标注为「显式选择：日志随写入单调增长」 | 上游默认（5000） |
   | `max_in_snapshot_log_to_keep`（新增，1:1 透传上游同名字段） | 保留窗口：快照点之后仍保留的（已入快照）日志条数；`0` = 允许回收紧贴快照点 | 1000 |

3. **有界性口径**：缺省配置下日志尾部条数上界 ≈ `max_in_snapshot_log_to_keep + snapshot_logs_since_last`（leader 在途复制与 `purge_batch_size` 允许滞后）。验收实验与负控制按此口径断言：受控写 N 倍条目 ⇒ 日志条数不随 N 线性增长；`snapshot_logs_since_last = 0` ⇒ 有界断言必红。
4. **B-ST-2 死配置版本化删除**：删除 `CompactionConfig::raft_log_retention_entries` 与 `tombstone_retention_revisions`（无 TOML 读取面、无调用点）；tombstone 清理维持与 changelog 共用单一 compact 水位，独立 tombstone 窗口不在本决策范围。
5. **手动触发留作后续**：`trigger().snapshot()` / `trigger().purge_log()` 的运维出口不在本期暴露；若后续开放，语义仍以快照为锚（purge 会被上游钳制到快照边界）。

## 后果

- 缺省即「有界」；`0` 模式 = 明确的无回收语义（需要时由运维显式选择，fail-loud WARN 已存在）。
- 文档四处（启动 WARN、`config.example.toml`、`boundaries.md`、`runbook.md`）就「两个正交开关 + 有界口径」同步；README 两个语言版本的失真表述一并更正（实现 PR）。
- 快照构建是回收的前置路径：快照构建失败（含 SM worker 故障）会同时阻塞回收——这是安全性要求，不是缺陷。
- `LogStore::purge` 守卫拒绝 = fail-closed：正常路径永不触发；一旦触发（顺序颠倒 / 回退），`io::Error` 经上游 `Fatal::StorageError` 通道使节点停止，避免出现「快照缺失 + 日志已删」的不可恢复状态。

## 参考

- 上游（openraft =0.10.0-alpha.34）：`src/docs/data/log_pointers.md`（不变量）；`src/engine/engine_impl.rs::trigger_purge_log`；`src/engine/handler/log_handler/mod.rs::calc_purge_upto`；`src/raft/trigger.rs::purge_log`；`src/config/config.rs`（`SnapshotPolicy` / `max_in_snapshot_log_to_keep` 字段文档）；`src/errors/fatal.rs`（`StorageError` → `Fatal`）。
- coord 代码：`coord-server/src/raft/mod.rs`（`apply_tuning`）；`coord-server/src/raft/log_store.rs`（`purge` 守卫）；`coord-server/src/storage/snapshot.rs`（`SnapshotTracker`）；`coord-server/src/storage/compaction.rs`（`CompactionConfig`）；`coord/src/main.rs`（启动 WARN 与恢复检查）。
- 测试锚点：`coord-server/tests/snapshot_visibility_test.rs`（快照构建可见性）；实现 PR 增补有界性受控实验与负控制测试。
- 边界：`docs/production/ops/boundaries.md` B-ST-1 / B-ST-2。
