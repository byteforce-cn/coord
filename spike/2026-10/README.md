# coord 工作流引擎 Spike — 2026-10

> Owner: Spike 执行（AI 协作者）｜ Last verified: 2026-10-13
> 状态：进行中｜分支：`spike/2026-10-workflow-v2`（不并入 main）

本目录是 CTO 批准《coord 工作流引擎 Spike 落地执行手册 v1.1》的产出物集合。
所有内容为**决策证据**，不是生产代码，不承诺任何对外契约。

## 目录

| 目录 | 对应 Spike | 状态 |
|:--|:--|:--|
| `CHARTER-INPUTS.md` | 前置条件 3.1/3.2/3.3 的台账（含临时替代输入声明） | 已建立（3.2/3.3 未到位） |
| `NEXT-SPIKES-NOTES.md` | Spike 2/3/5 接续手册（第一步 + 锚点 + 红线） | 已建立 |
| `spike-1-state-model/` | Spike 1 状态模型 v2 内存验证 | ✅ 初版完成（10-13，16/16，可复现） |
| `spike-4-conformance/` | Spike 4 规范符合性 | ✅ 初版完成（10-13，24 语料，报告+原始数据） |
| `spike-2-concurrency/` | Spike 2 单写者并发演练 | ✅ 初版完成（10-13，7/7，CAS+租约+fencing，可复现） |
| `spike-3-recovery/` | Spike 3 Outbox + 恢复 | ✅ 初版完成（10-13；kill -9 三场景 PASS，机制测试 6/6，事件 schema 校验通过） |
| `spike-5-bench/` | Spike 5 基准测试 | ✅ 初版完成（10-13，5/5；安静机 canonical + 逐条原始数据；共享负载首跑入 aux，非目标硬件） |
| `spike-6-adr/` | Spike 6 形态决策 | ✅ ADR-0013（proposed）+ 判据对照表；Spike 1–5 证据齐备，H3（性能）待 10-18 阈值/目标硬件 |

## 事实源与边界

- 架构决策：`docs/adr/0013-workflow-approval-v2.md`（proposed，本分支落档）。
- 现状事实源：`apis/contracts/proto/coord/`、`coord-core/src/workflow/`、
  `docs/production/workflow-semantics.md`、`docs/production/ops/boundaries.md`、`docs/adr/`。
- 本目录只记录 Spike 的假设、验证与结论；与现状冲突处以现状事实源为准。
- 不修改 `main`；不新增对外 RPC/字段；Spike 工装只在 spike 分支登记
  （根工作区成员：`spike/2026-10/spike-4-conformance/harness`、
  `spike/2026-10/spike-3-recovery/harness`，main 上均不存在）。

## 复现方法（Spike 1）

```bash
cd spike/2026-10/spike-1-state-model/crate
cargo test 2>&1 | tee ../evidence/test-run-$(date +%Y%m%d-%H%M%S).log
```

每个 Spike 的 `README.md` 给出该项的最小复现步骤。

## 阶段二 M1 落地状态

> Owner: Spike 执行（AI 协作者）｜ Last verified: 2026-10-13
> M1 = 分支 `feat/workflow-v2-foundation`（基线 `spike/2026-10-workflow-v2@850989a`）；
> 判定在 HEAD 上全量执行。本节只记录落地判定，**不**改本目录的历史证据。

### 里程碑判据（判据 → 覆盖测试/证据 → 结论）

| 判据 | 覆盖测试/证据 | 结论 |
|:--|:--|:--|
| 并发驱动/控制面：恰一胜、败者零写入 | `workflow_foundation::concurrent_resume_exactly_one_takes_effect`；`spike2_concurrency::fenced_write_rejects_stale_driver`、`stale_driver_fenced_commit_rejects_state_and_events` | ✅ 通过 |
| 状态 + 伴生事件同 Txn | `spike3_durable_kv::state_and_multiple_events_commit_atomically`；`spike2_concurrency::fenced_commit_writes_state_and_events_in_one_txn` | ✅ 通过 |
| 外部副作用幂等键先登记后派发、跨重试不变 | `workflow_steptoken::register_is_create_if_absent_and_mark_done_is_visible`、`retry_reuses_same_idempotency_key` | ✅ 通过 |
| 崩溃恢复：wait/listen/retry + Running 重驱动 | `workflow_foundation::running_instance_is_redriven_after_restart`；`spike3_durable_kv::{wait_instance_recovers_after_engine_restart, listen_instance_resubscribes_after_engine_restart, outbox_events_pending_at_crash_are_delivered_after_restart}`；kill9 repro 三场景 | ✅ 通过 |
| 定义漂移 fail-closed | `workflow_foundation::resume_after_redeploy_same_version_fails_closed`、`stale_pin_running_instance_faults_on_restart_reconcile` | ✅ 通过 |
| 未实现构造 fail-closed | `spike4_conformance` 8/8（`run_script_container_shell_fail_closed`、`run_await_return_fail_closed`、`use_extensions_fail_closed`、`official_switch_then_fail_closed`、`matrix_gap_files_still_rejected` …） | ✅ 通过 |
| 对外契约零改动 | `git diff --stat spike/2026-10-workflow-v2..HEAD -- apis/` 空输出；`git diff --name-only a4e0e22..HEAD` 无 proto/RPC 文件 | ✅ 通过 |

### 验收证据（Task 11，HEAD `c24469c`）

- 静态检查：`cargo fmt --all -- --check` 通过；`cargo clippy --all-targets --all-features`
  通过（0 error；warning 均为基线既有、非 error）。
- 全量测试：`cargo test -p coord-core` 391 passed / 0 failed；`cargo test -p coord-agent`
  878 passed / 0 failed；覆盖测试套件 `--test-threads=1` 全通过 ——
  `spike2_concurrency` 9、`spike3_durable_kv` 17、`spike4_conformance` 8、
  `workflow_foundation` 5、`workflow_steptoken` 2。
- 文档引用：`bash scripts/check-doc-refs.sh` OK（悬空路径 0）。
- 抖动 soak：`listen_instance_resubscribes_after_engine_restart` 10/10 通过；
  `spike3_durable_kv` 全量复跑 3/3 通过（各 17/17）。
- 进程级 kill -9：`SPIKE3_SKIP_BUILD=1 bash spike/2026-10/spike-3-recovery/scripts/repro.sh`
  → wait/listen/retry 三场景 PASS，`outbox_pending=0`、`missing_event_ids=0`、
  `duplicate_event_ids=0`；`evidence/verdict.tsv` 的日志列为仓库相对路径。

### 与 Spike 3 文档的一处语义更正（ack-after-state）

M1 把 `MqEventProvider` 的消费语义改为 **ack-after-state**（commit `53ce23d`）：
`wait_for_event` 只返回投递信封、**不**提交游标，位点由调用方在实例状态写成功后经
`ack_event` 提交 ⇒ 崩溃窗口从「丢一次投递」变为「重复投递」，由消费方状态守卫吸收。
这修正了 `spike-3-recovery/RECOVERY.md` §5/§5.1/§7 记录的 R1（listen 崩溃窗口）；
该文档对应位置已就地标注（历史证据保留）。覆盖测试：
`workflow_foundation::listen_event_is_acked_after_state_commit`、
`coord-agent::services::mq_event_provider::delivery_is_acked_only_after_explicit_ack`。
