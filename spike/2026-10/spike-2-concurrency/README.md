# Spike 2 · 单写者并发演练（CAS + 实例租约 + StepToken）

> Owner: Spike 执行（AI 协作者）｜ Last verified: 2026-10-13
> 分支：`spike/2026-10-spike2`（不并入 main）｜ 手册对应：§4「Spike 2」
> 状态：**初版完成**（真实单节点 Raft 上 7/7 全绿，日志与时间线已存档）

本目录是《coord 工作流引擎 Spike 落地执行手册 v1.1》§4 Spike 2 的**决策证据**，
不是生产代码。所有结论挂可复现测试与日志，未接线处显式标注。

## 结论（对应手册 §4 三问）

| 问题 | 结论 | 证据 |
|:--|:--|:--|
| 1. KV Txn CAS 能否实现实例状态写入（替换普通 Put）？ | **可行**。`Compare(Target::ModRev)` + `Put` 的 Txn 语义成立；引擎 drive 的关键写回已走 CAS/fencing。 | `cas_two_writers_same_rev_one_wins`、`cas_stale_revision_rejected`、`engine_concurrent_drivers_*` |
| 2. coord Lease 能否实现实例租约（一实例一驱动）？无 fencing token 的缺口？ | **可行**。`LeaseGrant` + `Txn create-if-absent` 得到互斥租约，撤销/过期后可重新竞争。**Lease 无 fencing token** 的处置：用「**租约 key 的 mod_revision 作为 epoch** + 写回时**三重 compare**」实现 fencing（见 `DESIGN.md`）。残余风险：fencing 只能拒绝**状态写回**，无法撤回**已产生的外部副作用** → 需 StepToken 兜底。 | `lease_mutex_exclusive`、`lease_expiry_reacquire`、`fenced_write_rejects_stale_driver` |
| 3. 并发场景下能否做到无重复副作用？StepToken 用 CAS key 还是 `request_id`？ | **在单写者租约覆盖的写路径下成立**（单效果恰好一次；去掉租约即重复）。StepToken 必须用 **CAS key**：`request_id` 是**单节点 / 进程内 / 60s / 不复制不持久**（`apis/contracts/proto/coord/kv/kv.proto`），不可作为幂等基础。 | `engine_concurrent_drivers_single_effect`（count=1）对照 `engine_concurrent_drivers_duplicate_without_lease`（count=2）；方案见 `STEPTOKEN.md` |

**红线遵守**：本 spike **不**宣称「Lease 无 fencing 时安全」。fencing 方案（三重 compare）
已单独验证，但引擎写路径只覆盖 drive 循环；`resume`/`auto_resume`/`apply_timeout`/`fail_instance`
的写回仍是普通 Put —— 见 `DESIGN.md`「未接线项与残余风险」。

## 复现（一条命令）

```bash
bash spike/2026-10/spike-2-concurrency/scripts/repro.sh
```

等价手工命令（日志已存档于 `evidence/`）：

```bash
CARGO_BUILD_JOBS=3 cargo test -p coord --test spike2_concurrency -- --nocapture --test-threads=1
```

要求：单机（8C/31G 测试机）、`coord` 集成测试夹具 `coord/tests/common/mod.rs::start_test_server()`
（进程内真实单节点 Raft + KV/Txn/Lease/Watch gRPC）。

## 证据索引

| 文件 | 内容 |
|:--|:--|
| `evidence/test-run-20261013-115233.log` | 7/7 全绿测试日志（`scripts/repro.sh` 产出，含引擎级 TIMELINE 行） |
| `evidence/timeline-20261013-115233.jsonl` | 引擎级并发时间线（派发顺序/时刻）：单效果 1 次 vs 无租约 2 次 |
| `evidence/matrix.tsv` | 结果矩阵：场景 → 机制 → 期望 → 观测 |
| `coord/tests/spike2_concurrency.rs` | 全部 7 个可执行用例（真实 Raft 上的 CAS/Lease/fencing/引擎） |

## 文档

- `DESIGN.md`：并发控制方案（每个 save 点的 CAS/Txn 形状、租约生命周期、fencing、失败模式、未接线项）。
- `STEPTOKEN.md`：StepToken 方案（CAS key vs `request_id`、推荐 key 布局、去重时序与残余窗口）。

## 契约与代码改动（仅本 spike 分支）

- `coord-core`：`WorkflowStore::load_instance_with_rev`（默认 `None`，内存存储行为不变）；
  `InstanceLease` / `LeaseGuard` 端口；`WorkflowRuntime::with_instance_lease`（builder）
  + `lease_skipped_total`/`cas_conflict_total` 可观测信号；`drive` 关键写回走 CAS/fencing；
  新增公开入口 `WorkflowRuntime::drive_instance`（supervisor 重驱动）。
- `coord-agent`：`KvWorkflowStore::load_instance_with_rev`（线性一致读 mod_revision）；
  新增 `services/workflow_lease.rs`（`KvInstanceLease` + `KvLeaseGuard`，fencing 三重 compare）。
- `coord-client`：`KvClient::range_with_rev`（读取 `mod_revision`）。

不新增/修改任何对外 RPC 或 proto 字段。
