# KV-T G6 — merge 导出水位放宽（#276）

**Goal:** 让 merge 在有直写、有并发写的 Region 上收敛，同时保留「导出水位以下不得有
未 apply 的决策」这条 fail-closed 判据。

**Spec:** `docs/adr/0015-kv-t.md` D6（G5 cutover 连续性）；issue #276；PR #273（split 侧同口径）。

## 现状（为什么永远失败）

`migrate_merge` 现在的四个硬条件把「直写」和「接收方并发写」都变成永久失败：

1. `donor_cursor == pin`（`pin = donor.current_revision()`）——直写只推进 revision、
   不产生决策，任何有直写的 donor 都永远不相等；
2. `receiver_cursor == receiver_revision`——同上；
3. `receiver_revision == pin`——两侧独立 apply/直写，水位几乎不可能相等；
4. `receiver_revision < pin` 直接失败——并发写下车轮永远对不齐。

## 判据推导（为什么可以放宽）

记 `C_d`/`C_r` 为 donor/absorbing 的决策游标（已 apply 决策的连续前缀），
`R_d`/`R_r` 为各自 `current_revision`。已知：

- 决策日志全局、**按 commit_ts 顺序发到每个 Region**（空切片也推进游标）；
- 直写受守卫约束：本 Region 的 `next_unapplied_decision_ts > commit_ts`
  （`guard_direct_write_commit_ts`），所以 `(C, R]` 内没有决策，只有直写 revision；
- 导入的 live 行带 `mod_revision`，目标只在 `mod_revision <= watermark` 时接受；
- merge 不抬接收方 revision/compact，只记 `[start,end)` 的导入历史下限；
- 决策 apply 对「`commit_ts <= current_revision` 且无 changelog 证据」fail-closed，
  所以 **不能让合并后的 Region 重放一条已被任一侧 apply 过的决策**（两侧的裁剪集不同，
  changelog 事件对不上）。

因此安全、可收敛的条件是：

1. **决策游标相等**：`C_d == C_r == C★`（有界等待；两侧都追同一条全局决策流，
   短暂不等很正常，等不到才失败）。等价于：
   - 双方对 `<= C★` 的决策都已有数据（donor 覆盖 donor 区间，receiver 覆盖自身区间）；
   - 之后的决策都未被任一侧 apply、且 `> C★`，合并后按新裁剪集重放。
2. `pin = R_d`（导出水位取 donor 当前 revision，含直写；导出期间 `expected_revision`
   校验保证水位不动）。
3. 导入完成后复查 `C_r` 仍为 `C★`、`R_d` 仍为 `pin`；否则本次迁移失败重试
   （`reset_range` 幂等，未 cutover，无副作用）。
4. 不再要求 `receiver_revision` 与 `pin` 相等：`R_r < pin` 时导入行
   `mod_revision > R_r`，在 revision 追上来之前对 `read_ts <= R_r` 不可见——这是 MVCC
   的正常可见性边界（不是丢数据）；`R_r > pin` 时导入行全部立即可见。

## 改动

- `coord-server/src/split_merge.rs::migrate_merge`：换成上面的判据；游标等待复用
  `decision_wait` 预算与 `DECISION_BARRIER_POLL`；错误文案保留
  `decision cursor` / `behind` 锚点（runbook/测试断言依赖）。
- `coord-server/tests/kv_t_g5_merge_migrator_test.rs`：
  - `test_merge_fails_closed_when_receiver_lags_donor`：仍失败（游标不等），断言改为
    cursor 口径；
  - `test_merge_fails_closed_when_donor_lags_the_receiver`：改为「游标相等、revision
    不等 ⇒ 允许」（旧断言基于 revision 相等，是 #276 要放宽的目标）；
  - 新增：donor 有直写（cursor < revision）⇒ 允许；receiver 有直写 ⇒ 允许；
    receiver 游标 < pin（重放窗口）⇒ 允许且导入后按 pin 读到 donor 行；
    receiver 游标在导入期间前进 ⇒ fail-closed 重试；
    donor 有未 apply 决策 ⇒ 仍失败。
- `docs/adr/0015-kv-t.md` D6：把「merge 两侧仍按原口径 … 另案」改成冻结后的判据。
- `docs/production/ops/kv-t-ga-gate.md` §3.5：补一条「merge 直写口径」缺陷与回归。

## 验收

- 上述新用例全绿；`kv_t_g5_split_merge_test` / `kv_t_g5_merge_migrator_test` 全量绿。
- `cargo fmt/clippy/check-panics/check-doc-refs/audit/deny` 绿。
