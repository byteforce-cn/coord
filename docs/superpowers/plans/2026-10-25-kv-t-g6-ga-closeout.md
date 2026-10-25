# KV-T G6 GA 收口 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 #169/#170/#171/#172/#174/#175 剩下的 GA 阻塞项收口，产出 #177 需要的可复查证据与 GA checklist。

**Architecture:** 三件事分开做、各自可独立合入：(1) 文档面新增 GA gate 清单与性能/容量报告（单一事实源，不复制 ADR 口径）；(2) 新增「v2 集群上 15 个内建服务」的真实进程冒烟测试，补上 #170 最后一块证据；(3) 新增 v2 故障注入场景（混合直写 + 事务 + kill/pause 决策面），补上 #175 的 v2 证据。备份/恢复侧只补「恢复后 Watch 连续 + 历史窗口边界」两条证据，不动已落地的备份实现。

**Tech Stack:** Rust 2021、tokio、tonic、coord-client v2、coord-server v2（multi-raft + `[kv_t]`）、bash 夹具脚本、Java SDK（仅文档引用）。

**Spec:** `docs/adr/0015-kv-t.md`（D3/D5/D6/D9/D10/D11/D13）、issue #169/#170/#171/#172/#174/#175/#177、`docs/production/ops/runbook.md` §3.3。

## Global Constraints

- `[kv_t].enabled=false`（默认）时不得改变 v1 行为（ADR-0001）；新增测试不得让默认路径多出 v2 代码执行。
- 不新增语义；测试只验证 ADR 已冻结的口径。发现语义缺口 ⇒ 停，不在测试里绕过去。
- 不把「未跑」写成「通过」；证据必须能在仓库里用命令复跑。
- 文档单一归属：GA 状态进 `docs/production/ops/kv-t-ga-gate.md`，性能数字进 `docs/production/ops/performance.md`，处置步骤留在 runbook，ADR 只保留锚点引用。
- 合入前门禁：`cargo fmt --all -- --check`、`cargo clippy --workspace --lib --bins -- -D warnings`、`bash scripts/check-panics.sh`、`cargo audit`、`cargo deny check`、`bash scripts/check-doc-refs.sh`。
- 临时文件只落 `/tmp`（`/tmp/coord-*`），运行产物不写仓库。

## Review Focus

- **v2 冒烟是否真的走 v2**：测试必须证明 Agent 内部服务经 `coord.*.v2` 落到 server（例如 idgen segment 号段、feature flag 跨 agent 可见），而不是本地内存/降级路径；无 server 连接时服务会静默降级，测试要挡住这种假绿。
- **kill/pause 后的不变量**：已提交事务最终可见、`safe_ts` 能追上、无 pending decision 被直写越过；断言必须来自可观测状态（KV 读、status RPC、日志锚点），不能只看「进程起来了」。
- **恢复后 Watch 连续**：恢复前写入的历史必须能从 cursor 读到；低于 `gc_before_ts` 的起点必须明确 `HISTORY_UNAVAILABLE`，不能返回空流。
- **测试夹具的端口/目录隔离**：进程级夹具必须端口冲突 fail-closed、每次运行清空数据目录，否则会把别的进程当自己的集群。
- **文档会不会漂**：GA 清单里的测试名必须与仓库中的真实测试名逐字一致；`check-doc-refs.sh` 只查路径，不查测试名，评审时要人眼核对。

---

### Task 1: GA gate 清单（#177 的验收面）

**Files:**
- Create: `docs/production/ops/kv-t-ga-gate.md`
- Modify: `docs/adr/0015-kv-t.md`（「验收锚点」末尾加一行指向清单，不复制内容）
- Modify: `docs/production/ops/runbook.md`（§0 通用前置加一句：GA 状态与 gate 证据看清单）

**Interfaces:**
- Produces: `kv-t-ga-gate.md` 的 gate 表（G0–G6）+ 证据列（测试名/命令/CI job）+ 未达标项列。
- Produces: 签字记录表（日期 / 结论 / 依据 commit / 未关闭项），GA 时填写；未填即「未签」。

- [ ] **Step 1: 盘点证据**（逐 gate 把测试名与命令对齐到真实文件）
- [ ] **Step 2: 写清单**（现状态，不写计划语气；每个未达标项写「停/回退」口径）
- [ ] **Step 3: 加两处引用**（ADR 锚点、runbook 前置）
- [ ] **Step 4: 跑 `bash scripts/check-doc-refs.sh`**，0 悬空
- [ ] **Step 5: 提交并开 PR**

### Task 2: v2 集群上的内建服务冒烟（#170）

**Files:**
- Create: `coord/tests/kv_t_g6_agent_services_v2_test.rs`
- Modify: `docs/production/ops/boundaries.md`（B-KVT-6 补一句：15 个内建服务在 v2 冒烟的证据测试名）
- Modify: `docs/adr/0015-kv-t.md`（验收锚点新增该测试名）

**Interfaces:**
- Consumes: `coord server` 二进制（`CARGO_BIN_EXE_coord`）与 v2 配置（multi-raft + `[kv_t]` + object storage）；`coord agent` 二进制 + `agent.toml`（`data_plane="v2"`、`idgen_mode="segment"`、除 `mq`/`replication`/`workflow_durable` 外全开）。
- Produces: `test_v2_cluster_serves_all_builtin_services` —— Plugin.List 集合相等 + 每服务一次真实 RPC（下表）。

| 服务 | RPC 判据 |
|:--|:--|
| registry | Register → Discover 看到实例 |
| config_center | Put → Get 值一致 |
| lock | Acquire → Release |
| idgen | NextId 两次，严格递增（segment 模式 ⇒ 经 v2 Txn CAS） |
| leader_election | Campaign → GetLeader 返回自己 |
| event_notification | Publish 成功且返回 event id |
| cache | Set → Get |
| workflow | Deploy → Start → GetStatus 非空 |
| policy | PutBundle → ListBundles 含之 |
| scheduler | RegisterJob → ClaimJob 成功 |
| circuit_breaker | GetState（CLOSED）+ ReportFailure 后 GetState=OPEN |
| rate_limiter | Allow 返回 allowed/remaining |
| feature_flags | PutFlag → IsEnabled=true |
| transit | Encrypt → Decrypt 回原文 |
| pki | InitCa → IssueCert 成功 |

- [ ] **Step 1: 写失败测试**（先只做 server+agent 启动与 Plugin.List 集合相等；端口冲突 fail-closed）
- [ ] **Step 2: 跑测试确认红**（期望：测试文件内断言失败，不是启动失败）
- [ ] **Step 3: 补齐每个服务的 RPC 调用**（逐个绿，失败则记 `Ruling:` 并判断是实现缺陷还是测试假设错）
- [ ] **Step 4: 跑 3 次确认无 flake**
- [ ] **Step 5: 文档同步 + 门禁 + PR**

### Task 3: 性能/容量报告（#174 的「报告」与调参依据）

**Files:**
- Create: `docs/production/ops/performance.md`
- Modify: `docs/production/ops/runbook.md`（§5 后加一节「容量/性能调参」指向报告，不复制数字）
- Modify: `docs/adr/0015-kv-t.md`（D9 段末加一句：默认值的实测依据见报告）

- [ ] **Step 1: 跑 `PERF_GATE=1 bash scripts/bench-ci.sh`**，把 release 实测数字落 `/tmp`（不写仓库）
- [ ] **Step 2: 跑 120s 分布式 soak**（`COORD_EVIDENCE_DIR=/tmp/coord-evidence bash scripts/collect-evidence.sh soak-smoke`）
- [ ] **Step 3: 写报告**：环境/commit/命令、吞吐/延迟/写放大/watch lag 表、瓶颈与上限、D9 数值范围依据、72h soak 的**未执行**状态与执行命令
- [ ] **Step 4: 门禁 + PR**

### Task 4: v2 故障注入（#175 的 v2 证据）

**Files:**
- Create: `coord/tests/kv_t_g6_chaos_v2_test.rs`
- Modify: `.github/workflows/ci.yml`（chaos-kill9-soak job 增加该测试目标；与 v1 chaos 同参数）
- Modify: `docs/production/ops/kv-t-ga-gate.md`（G6 混沌行写清覆盖矩阵与未覆盖项）

**Interfaces:**
- Consumes: `coord` 二进制；3 节点 v2 配置（`[multi_raft] enabled` + `[kv_t] enabled` + 相同 `initial_regions`）；`coord-client` 的 v2 KV/Txn 客户端（或直接 `coord_proto::*::v2` stub）。
- Produces: `chaos_v2_mixed_writes_survive_decision_leader_loss` —— 并发事务 + 单键直写，期间 kill -9 / SIGSTOP region 0 leader，断言：已提交事务最终可见、`read_ts` 能推进、无 `commit decision apply stopped`/`passed unapplied decision` 日志锚点。

- [ ] **Step 1: 起 3 节点 v2 集群**（复用 `chaos_real.rs` 的 `RealNode` 形状，配置加 multi_raft/kv_t）
- [ ] **Step 2: 写失败测试**（先只断言「集群可提交一个 v2 txn」）
- [ ] **Step 3: 加混合写 + kill/pause 轮换**
- [ ] **Step 4: 加不变量断言**（可见性/推进/日志锚点）
- [ ] **Step 5: 跑 3 次 + CI 接线 + PR**

### Task 5: 恢复后 Watch 连续与历史边界（#169 残余）

**Files:**
- Modify: `coord/tests/backup_cli_test.rs`
- Modify: `docs/production/ops/runbook.md` §3.3（补「恢复后 Watch 从 cursor 续读」验证命令）

**Interfaces:**
- Consumes: `build_data_dir`（现有夹具，含 Commit Log shard 1 的两条记录 + region 0 shard 表）、`WatchCoordinator`（`WatchStart { start_revision, start_cursor }`）、`HISTORY_UNAVAILABLE` 错误码。
- Produces: `test_backup_restore_keeps_watch_stream_continuous`、`test_backup_restore_before_retention_is_history_unavailable`。

- [ ] **Step 1: 写失败测试**（恢复后端到端读 Watch，而不是只断言文件在）
- [ ] **Step 2: 跑测试确认红**
- [ ] **Step 3: 实现必要的夹具/断言**（不改备份实现，除非测试暴露真缺陷）
- [ ] **Step 4: 门禁 + PR**

### Task 6: 关闭 issue 与总表

**Files:**
- Modify: `docs/production/ops/kv-t-ga-gate.md`（签字记录 + 未关闭项）
- GitHub: #169/#170/#171/#172/#174/#175 证据评论 + 状态；#177 checklist；#127 表状态

- [ ] **Step 1: 逐 issue 写证据评论**（测试名/命令/CI 链接），达标才关；不达标写清残余
- [ ] **Step 2: #177 填 checklist 与签字状态**
- [ ] **Step 3: #127 只改顺序/状态引用，不复制验收细节**
