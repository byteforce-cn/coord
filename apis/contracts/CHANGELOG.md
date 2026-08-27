# 契约变更记录（CHANGELOG）

版本规则见 WHITEPAPER.md §4。契约版本独立于代码版本。

## [contracts/v1.0.0] — 2026-08-27（首次冻结）

基线代码：`82eb398`（main）。

**纳入（稳定承诺）**

- `coord.kv.KV`：`Put` / `Range` / `Delete`
- `coord.txn.Txn`：`Txn`
- `coord.lease.Lease`：`LeaseGrant` / `LeaseRevoke` / `LeaseKeepAlive`（双向流）
- `coord.watch.Watch`：`Watch`（双向流）
- `coord.maintenance.Maintenance`：仅 `Status`（`StatusResponse` 仅承诺 `revision` 字段，
  编号 2–5 reserved）
- `grpc.health.v1.Health`：`Check` / `Watch`

**随附承诺文本**

- 兼容性铁律与废弃策略（WHITEPAPER §3）
- 错误码契约（WHITEPAPER §5，以 `map_core_error` 代码为准；
  修正内部文档 NotLeader 映射漂移：实际为 `UNAVAILABLE` + `coord-leader-hint`）
- Metadata 约定（WHITEPAPER §6）
- 语义边界：Revision 单调递增但不承诺与 Raft Log Index 对齐（WHITEPAPER §7）

**明确排除（红区，不承诺）**

- Multi-Raft/PD（experimental，零生产路径引用）
- Cache/MQ ISR 复制（experimental，R-AGT-13）
- Seal/Unseal/静态加密（默认关闭，未生产验证）
- Workflow（生产路径内存态）、Agent 侧其余服务、`coord.agent.Replica`（内部）

**质量门禁**

- 新增 `.github/workflows/contract-check.yml`：buf lint + buf breaking + wire-sync 三卡口。

---

*后续条目模板：版本号 / 日期 / 变更类型（Patch|Minor|Major）/ 变更内容 / 影响范围（消费者数量）/ 回滚预案 / sunsetDate（如涉及废弃）*
